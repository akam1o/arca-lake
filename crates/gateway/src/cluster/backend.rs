//! `S3` implementation over the cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arca_lake_common::EcProfile;
use arca_lake_common::timing::{Histogram, HistogramSnapshot};
use arca_lake_metadata::ops::object::Precondition;
use arca_lake_metadata::ops::{bucket, list, object};
use arca_lake_metadata::records::{GenId, Versioning};
use arca_lake_metadata::{KvStore, MetaError};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::cache::CacheTierConfig;
use arca_lake_tiering::durable::Coders;
use s3s::dto::{
    Bucket, CommonPrefix, CreateBucketInput, CreateBucketOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
    PutObjectInput, PutObjectOutput, Timestamp,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

use super::map::ClusterMap;
use super::popqueue::{PopQueueStatsSnapshot, PopulationQueue};

/// Gateway-side cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// EC profile used for new objects (per-bucket profiles are Phase 2).
    pub profile: EcProfile,
    /// Tier for new objects.
    pub tier: String,
    /// Lease for pending generations.
    pub lease: Duration,
    /// Chunk records per `append_chunks` call.
    pub chunk_batch: usize,
    /// Frame size for shard streaming.
    pub shard_frame_bytes: usize,
    /// Per-RPC timeout to storage nodes (used to derive the RPC policy when
    /// `rpc_policy` is `None`).
    pub node_timeout: Duration,
    /// Deadlines and retries of node RPCs (R-47); `None` derives them from
    /// `node_timeout`.
    pub rpc_policy: Option<arca_lake_protocol::client::RpcPolicy>,
    /// Read-through cache tier (RFC-006); `None` disables caching.
    pub cache: Option<CacheTierConfig>,
    /// Chunks read ahead of the one being streamed to the client (1 =
    /// sequential). Bounds the gateway's buffered bytes per GET to about
    /// `read_parallelism` chunks (RFC-009 §9).
    pub read_parallelism: usize,
    /// Cache populations waiting in the gateway's queue at most; beyond
    /// that a miss is served without populating (RFC-006 §5.3).
    pub population_queue_depth: usize,
    /// Cache populations one node receives from this gateway at once.
    pub populations_per_node: usize,
}

/// Fault-injection points in the write path (tests only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Crash after every shard is durable but before any chunk record was
    /// appended (shards are orphans).
    AfterShardsBeforeManifest,
    /// Crash after the manifest is appended but before the commit.
    BeforeCommit,
    /// Crash after the commit but before the client is answered.
    AfterCommit,
    /// The gateway process dies before the commit: the error is returned
    /// without aborting the generation, which stays pending until its lease
    /// expires (unlike [`FaultPoint::BeforeCommit`], which models a handled
    /// error).
    DieBeforeCommit,
}

/// The distributed backend. Cloning shares all state (the `s3s` service
/// takes ownership of its backend, while tests and tooling keep a handle
/// for fault injection and map refresh).
pub struct ClusterBackend<S> {
    pub(super) meta: Arc<S>,
    pub(super) clients: NodeClients,
    pub(super) map: Arc<RwLock<Arc<ClusterMap>>>,
    pub(super) cfg: Arc<ClusterConfig>,
    pub(super) coders: Arc<Coders>,
    fault: Arc<Mutex<Option<FaultPoint>>>,
    pub(super) cache_stats: Arc<CacheStats>,
    pub(super) cache_state: Arc<CacheState>,
    pub(super) put_stats: Arc<PutStats>,
    pub(super) pop_queue: Arc<PopulationQueue>,
}

/// Where a PUT spends its time in this gateway (R-48 profiling). Chunk
/// phases are observed per chunk, `commit` and `total` per object.
#[derive(Debug, Default)]
pub struct PutStats {
    /// Waiting for the client's body bytes of one chunk.
    pub body_read: Histogram,
    /// Erasure coding and checksum of one chunk.
    pub encode: Histogram,
    /// Writing one chunk's shards (all attempts, including map refreshes).
    pub shard_write: Histogram,
    /// One `append_chunks` metadata transaction batch.
    pub append: Histogram,
    /// One lease renewal transaction.
    pub lease_renew: Histogram,
    /// The commit transaction (including precondition resolution).
    pub commit: Histogram,
    /// Whole PUT, from begin to commit.
    pub total: Histogram,
    /// PUTs currently in progress.
    pub in_flight: std::sync::atomic::AtomicU64,
}

/// A copy of [`PutStats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PutStatsSnapshot {
    /// See [`PutStats::body_read`].
    pub body_read: HistogramSnapshot,
    /// See [`PutStats::encode`].
    pub encode: HistogramSnapshot,
    /// See [`PutStats::shard_write`].
    pub shard_write: HistogramSnapshot,
    /// See [`PutStats::append`].
    pub append: HistogramSnapshot,
    /// See [`PutStats::lease_renew`].
    pub lease_renew: HistogramSnapshot,
    /// See [`PutStats::commit`].
    pub commit: HistogramSnapshot,
    /// See [`PutStats::total`].
    pub total: HistogramSnapshot,
    /// See [`PutStats::in_flight`].
    pub in_flight: u64,
}

impl PutStats {
    /// Copy the counters.
    #[must_use]
    pub fn snapshot(&self) -> PutStatsSnapshot {
        PutStatsSnapshot {
            body_read: self.body_read.snapshot(),
            encode: self.encode.snapshot(),
            shard_write: self.shard_write.snapshot(),
            append: self.append.snapshot(),
            lease_renew: self.lease_renew.snapshot(),
            commit: self.commit.snapshot(),
            total: self.total.snapshot(),
            in_flight: self.in_flight.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Counters of the read-through cache (per gateway process).
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Chunks served from a validated cache copy.
    pub hits: std::sync::atomic::AtomicU64,
    /// Chunks with no cache record (served from durable placement).
    pub misses: std::sync::atomic::AtomicU64,
    /// Chunks whose cache record existed but whose copy could not be used.
    pub fallbacks: std::sync::atomic::AtomicU64,
    /// Cache records removed after a failed hit.
    pub invalidations: std::sync::atomic::AtomicU64,
    /// Successful asynchronous populations.
    pub populated: std::sync::atomic::AtomicU64,
    /// Populations that failed (node or metadata error).
    pub population_failures: std::sync::atomic::AtomicU64,
    /// Populations skipped because one was already in flight here.
    pub populations_deduplicated: std::sync::atomic::AtomicU64,
    /// Chunks not admitted (size, miss threshold, capacity, generation gone).
    pub admissions_rejected: std::sync::atomic::AtomicU64,
    /// Hits served with at least one shard missing (RFC-006 §5.9).
    pub degraded_hits: std::sync::atomic::AtomicU64,
    /// Cache shards rewritten after degraded hits.
    pub shards_repaired: std::sync::atomic::AtomicU64,
    /// Read-triggered cache repairs that failed.
    pub shard_repair_failures: std::sync::atomic::AtomicU64,
    /// Read-triggered cache repairs skipped for lack of a permit.
    pub shard_repairs_skipped: std::sync::atomic::AtomicU64,
    /// Populations skipped because `max_populations_in_flight` were running.
    pub populations_skipped: std::sync::atomic::AtomicU64,
    /// Chunks whose manifest carried a cache placement (lookup hits), before
    /// the copy was read; `hits + fallbacks` by construction.
    pub lookup_hits: std::sync::atomic::AtomicU64,
    /// Fallbacks whose copy had fewer than `k` readable shards.
    pub fallback_unreadable: std::sync::atomic::AtomicU64,
    /// Fallbacks whose assembled copy failed the length or CRC check.
    pub fallback_corrupt: std::sync::atomic::AtomicU64,
    /// Fallbacks on a node or transport error of a raw copy.
    pub fallback_node: std::sync::atomic::AtomicU64,
    /// Fallbacks because the placement's group is unknown to the map.
    pub fallback_unknown_group: std::sync::atomic::AtomicU64,
    /// Admissions refused by `max_cacheable_bytes`.
    pub admission_rejected_size: std::sync::atomic::AtomicU64,
    /// Admissions deferred by `admit_after_misses`.
    pub admission_rejected_threshold: std::sync::atomic::AtomicU64,
    /// Populations refused because the tier is at capacity.
    pub admission_rejected_capacity: std::sync::atomic::AtomicU64,
    /// Populations refused because the generation was retired meanwhile.
    pub admission_rejected_generation: std::sync::atomic::AtomicU64,
    /// Population failures on a node or transport error.
    pub population_failures_node: std::sync::atomic::AtomicU64,
    /// Population failures on a metadata error.
    pub population_failures_meta: std::sync::atomic::AtomicU64,
    /// Population failures because a node stored different bytes.
    pub population_failures_corrupt: std::sync::atomic::AtomicU64,
}

impl CacheStats {
    fn snapshot(&self) -> CacheStatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        CacheStatsSnapshot {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            fallbacks: self.fallbacks.load(Relaxed),
            invalidations: self.invalidations.load(Relaxed),
            populated: self.populated.load(Relaxed),
            population_failures: self.population_failures.load(Relaxed),
            populations_deduplicated: self.populations_deduplicated.load(Relaxed),
            admissions_rejected: self.admissions_rejected.load(Relaxed),
            degraded_hits: self.degraded_hits.load(Relaxed),
            shards_repaired: self.shards_repaired.load(Relaxed),
            shard_repair_failures: self.shard_repair_failures.load(Relaxed),
            shard_repairs_skipped: self.shard_repairs_skipped.load(Relaxed),
            populations_skipped: self.populations_skipped.load(Relaxed),
            lookup_hits: self.lookup_hits.load(Relaxed),
            fallback_unreadable: self.fallback_unreadable.load(Relaxed),
            fallback_corrupt: self.fallback_corrupt.load(Relaxed),
            fallback_node: self.fallback_node.load(Relaxed),
            fallback_unknown_group: self.fallback_unknown_group.load(Relaxed),
            admission_rejected_size: self.admission_rejected_size.load(Relaxed),
            admission_rejected_threshold: self.admission_rejected_threshold.load(Relaxed),
            admission_rejected_capacity: self.admission_rejected_capacity.load(Relaxed),
            admission_rejected_generation: self.admission_rejected_generation.load(Relaxed),
            population_failures_node: self.population_failures_node.load(Relaxed),
            population_failures_meta: self.population_failures_meta.load(Relaxed),
            population_failures_corrupt: self.population_failures_corrupt.load(Relaxed),
        }
    }
}

/// A copy of [`CacheStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    /// See [`CacheStats::hits`].
    pub hits: u64,
    /// See [`CacheStats::misses`].
    pub misses: u64,
    /// See [`CacheStats::fallbacks`].
    pub fallbacks: u64,
    /// See [`CacheStats::invalidations`].
    pub invalidations: u64,
    /// See [`CacheStats::populated`].
    pub populated: u64,
    /// See [`CacheStats::population_failures`].
    pub population_failures: u64,
    /// See [`CacheStats::populations_deduplicated`].
    pub populations_deduplicated: u64,
    /// See [`CacheStats::admissions_rejected`].
    pub admissions_rejected: u64,
    /// See [`CacheStats::degraded_hits`].
    pub degraded_hits: u64,
    /// See [`CacheStats::shards_repaired`].
    pub shards_repaired: u64,
    /// See [`CacheStats::shard_repair_failures`].
    pub shard_repair_failures: u64,
    /// See [`CacheStats::shard_repairs_skipped`].
    pub shard_repairs_skipped: u64,
    /// See [`CacheStats::populations_skipped`].
    pub populations_skipped: u64,
    /// See [`CacheStats::lookup_hits`].
    pub lookup_hits: u64,
    /// See [`CacheStats::fallback_unreadable`].
    pub fallback_unreadable: u64,
    /// See [`CacheStats::fallback_corrupt`].
    pub fallback_corrupt: u64,
    /// See [`CacheStats::fallback_node`].
    pub fallback_node: u64,
    /// See [`CacheStats::fallback_unknown_group`].
    pub fallback_unknown_group: u64,
    /// See [`CacheStats::admission_rejected_size`].
    pub admission_rejected_size: u64,
    /// See [`CacheStats::admission_rejected_threshold`].
    pub admission_rejected_threshold: u64,
    /// See [`CacheStats::admission_rejected_capacity`].
    pub admission_rejected_capacity: u64,
    /// See [`CacheStats::admission_rejected_generation`].
    pub admission_rejected_generation: u64,
    /// See [`CacheStats::population_failures_node`].
    pub population_failures_node: u64,
    /// See [`CacheStats::population_failures_meta`].
    pub population_failures_meta: u64,
    /// See [`CacheStats::population_failures_corrupt`].
    pub population_failures_corrupt: u64,
}

/// Per-gateway in-memory state of the cache: in-flight populations
/// (stampede dedupe), miss counts for admission, recency-touch and
/// access-record coalescing. Hints only; correctness never depends on it.
#[derive(Debug)]
pub struct CacheState {
    populating: Mutex<std::collections::HashSet<(GenId, u32)>>,
    misses: Mutex<HashMap<(GenId, u32), u32>>,
    touched: Mutex<HashMap<(GenId, u32), u64>>,
    access: Mutex<HashMap<GenId, (u64, u64)>>,
    /// Concurrency limit of read-triggered cache repairs (RFC-006 §5.9).
    pub(super) repair_permits: Arc<tokio::sync::Semaphore>,
}

/// Read-triggered cache repairs in flight at once, per gateway.
const CACHE_REPAIR_PERMITS: usize = 2;

impl Default for CacheState {
    fn default() -> Self {
        Self::with_population_permits(8)
    }
}

impl CacheState {
    pub(super) fn with_population_permits(_workers: usize) -> Self {
        Self {
            populating: Mutex::default(),
            misses: Mutex::default(),
            touched: Mutex::default(),
            access: Mutex::default(),
            repair_permits: Arc::new(tokio::sync::Semaphore::new(CACHE_REPAIR_PERMITS)),
        }
    }
}

/// Minimum interval between recency-index writes for one cached chunk.
const TOUCH_INTERVAL_MS: u64 = 10_000;
/// Minimum interval between access-record writes for one generation.
const ACCESS_INTERVAL_MS: u64 = 1_000;
const STATE_CAP: usize = 100_000;

impl CacheState {
    pub(super) fn begin_populate(&self, g: GenId, i: u32) -> bool {
        self.populating
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((g, i))
    }

    pub(super) fn end_populate(&self, g: GenId, i: u32) {
        self.populating
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(g, i));
    }

    /// Whether the chunk has missed often enough to be admitted.
    pub(super) fn admit(&self, g: GenId, i: u32, after: u32) -> bool {
        if after <= 1 {
            return true;
        }
        let mut m = self.misses.lock().unwrap_or_else(PoisonError::into_inner);
        if m.len() > STATE_CAP {
            m.clear();
        }
        let n = m.entry((g, i)).or_insert(0);
        *n = n.saturating_add(1);
        *n >= after
    }

    pub(super) fn should_touch(&self, g: GenId, i: u32, now: u64) -> bool {
        let mut t = self.touched.lock().unwrap_or_else(PoisonError::into_inner);
        if t.len() > STATE_CAP {
            t.clear();
        }
        match t.get(&(g, i)) {
            Some(last) if now.saturating_sub(*last) < TOUCH_INTERVAL_MS => false,
            _ => {
                t.insert((g, i), now);
                true
            }
        }
    }

    /// Count a read; returns the reads to flush when the interval passed.
    pub(super) fn flush_access(&self, g: GenId, now: u64) -> Option<u64> {
        let mut a = self.access.lock().unwrap_or_else(PoisonError::into_inner);
        if a.len() > STATE_CAP {
            a.clear();
        }
        let e = a.entry(g).or_insert((0, 0));
        e.1 += 1;
        if now.saturating_sub(e.0) >= ACCESS_INTERVAL_MS {
            let reads = e.1;
            *e = (now, 0);
            Some(reads)
        } else {
            None
        }
    }
}

impl<S> Clone for ClusterBackend<S> {
    fn clone(&self) -> Self {
        Self {
            put_stats: Arc::clone(&self.put_stats),
            pop_queue: Arc::clone(&self.pop_queue),
            meta: Arc::clone(&self.meta),
            clients: self.clients.clone(),
            map: Arc::clone(&self.map),
            cfg: Arc::clone(&self.cfg),
            coders: Arc::clone(&self.coders),
            fault: Arc::clone(&self.fault),
            cache_stats: Arc::clone(&self.cache_stats),
            cache_state: Arc::clone(&self.cache_state),
        }
    }
}

impl<S> std::fmt::Debug for ClusterBackend<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterBackend")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// Current time in Unix milliseconds.
#[must_use]
pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// Create a backend and load the cluster map.
    ///
    /// # Errors
    ///
    /// Metadata errors or an invalid profile.
    pub async fn new(meta: Arc<S>, cfg: ClusterConfig) -> Result<Self, MetaError> {
        cfg.profile
            .validate()
            .map_err(|e| MetaError::InvalidArgument(e.to_string()))?;
        let map = ClusterMap::load(meta.as_ref()).await?;
        let permits = cfg
            .cache
            .as_ref()
            .map_or(8, CacheTierConfig::population_permits);
        let clients = NodeClients::with_policy(cfg.rpc_policy.clone().unwrap_or_else(|| {
            arca_lake_protocol::client::RpcPolicy::from_timeout(cfg.node_timeout)
        }));
        let pop_queue = Arc::new(PopulationQueue::new(
            clients.clone(),
            permits,
            cfg.population_queue_depth,
            cfg.populations_per_node,
        ));
        Ok(Self {
            meta,
            clients,
            pop_queue,
            map: Arc::new(RwLock::new(Arc::new(map))),
            cfg: Arc::new(cfg),
            coders: Arc::new(Coders::default()),
            fault: Arc::new(Mutex::new(None)),
            cache_stats: Arc::new(CacheStats::default()),
            put_stats: Arc::new(PutStats::default()),
            cache_state: Arc::new(CacheState::with_population_permits(permits)),
        })
    }

    /// Install a fault point for the next write (tests).
    pub fn set_fault(&self, fault: Option<FaultPoint>) {
        *self.fault.lock().unwrap_or_else(PoisonError::into_inner) = fault;
    }

    pub(super) fn take_fault(&self, at: FaultPoint) -> bool {
        let mut f = self.fault.lock().unwrap_or_else(PoisonError::into_inner);
        if *f == Some(at) {
            *f = None;
            true
        } else {
            false
        }
    }

    /// The metadata store.
    #[must_use]
    pub fn metadata(&self) -> &Arc<S> {
        &self.meta
    }

    /// The storage node clients (tests install transport fault hooks here).
    #[must_use]
    pub fn node_clients(&self) -> &NodeClients {
        &self.clients
    }

    /// Current map snapshot.
    #[must_use]
    pub fn map(&self) -> Arc<ClusterMap> {
        Arc::clone(&self.map.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Reload the cluster map from metadata.
    ///
    /// # Errors
    ///
    /// Metadata errors.
    pub async fn refresh_map(&self) -> Result<Arc<ClusterMap>, MetaError> {
        let fresh = Arc::new(ClusterMap::load(self.meta.as_ref()).await?);
        *self.map.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&fresh);
        Ok(fresh)
    }

    /// Refresh the map only if the metadata version moved.
    pub(super) async fn map_up_to_date(&self) -> Result<Arc<ClusterMap>, MetaError> {
        let cur = self.map();
        let version = arca_lake_metadata::ops::cluster::map_version(self.meta.as_ref()).await?;
        if version == cur.version {
            Ok(cur)
        } else {
            self.refresh_map().await
        }
    }

    /// Cache counters of this gateway.
    #[must_use]
    pub fn cache_stats(&self) -> CacheStatsSnapshot {
        self.cache_stats.snapshot()
    }

    /// PUT phase timings of this gateway.
    #[must_use]
    pub fn put_stats(&self) -> PutStatsSnapshot {
        self.put_stats.snapshot()
    }

    /// Counters of the population queue.
    #[must_use]
    pub fn population_queue_stats(&self) -> PopQueueStatsSnapshot {
        self.pop_queue.stats()
    }

    /// Stop the population queue: running populations finish, queued ones
    /// are dropped and counted. Call after the HTTP server has stopped.
    pub async fn shutdown(&self) {
        self.pop_queue.shutdown().await;
    }
}

pub(super) fn meta_err(e: MetaError) -> s3s::S3Error {
    match e {
        MetaError::NoSuchBucket => s3_error!(NoSuchBucket),
        MetaError::BucketAlreadyExists => s3_error!(BucketAlreadyExists),
        MetaError::BucketNotEmpty => s3_error!(BucketNotEmpty),
        MetaError::NoSuchKey => s3_error!(NoSuchKey),
        MetaError::PreconditionFailed => s3_error!(PreconditionFailed),
        MetaError::InvalidArgument(m) => s3_error!(InvalidArgument, "{m}"),
        MetaError::Kv(arca_lake_metadata::KvError::TooLarge { .. }) => {
            s3_error!(
                InvalidArgument,
                "object metadata exceeds the store's size limit"
            )
        }
        MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(r)) => {
            s3_error!(ServiceUnavailable, "metadata commit outcome unknown: {r}")
        }
        other => s3_error!(InternalError, "metadata: {other}"),
    }
}

fn ms_to_timestamp(ms: u64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_millis(ms))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[async_trait::async_trait]
impl<S: KvStore + 'static> S3 for ClusterBackend<S> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        bucket::create_bucket(
            self.meta.as_ref(),
            &req.input.bucket,
            Versioning::Disabled,
            now_ms(),
        )
        .await
        .map_err(meta_err)?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        bucket::delete_bucket(self.meta.as_ref(), &req.input.bucket)
            .await
            .map_err(meta_err)?;
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        bucket::get_bucket(self.meta.as_ref(), &req.input.bucket)
            .await
            .map_err(meta_err)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = bucket::list_buckets(self.meta.as_ref())
            .await
            .map_err(meta_err)?;
        let list = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(ms_to_timestamp(b.created_ms)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(list),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let Some(body) = input.body else {
            return Err(s3_error!(IncompleteBody));
        };
        let mut pre = Precondition::default();
        if let Some(c) = &input.if_none_match {
            if !c.is_any() {
                return Err(s3_error!(
                    NotImplemented,
                    "If-None-Match with an ETag is not supported"
                ));
            }
            pre.if_none_match_any = true;
        }
        if let Some(c) = &input.if_match {
            match c.as_etag() {
                Some(ETag::Strong(s)) => pre.if_match = Some(s.clone()),
                Some(_) => return Err(s3_error!(PreconditionFailed)),
                None => return Err(s3_error!(NotImplemented, "If-Match: * is not supported")),
            }
        }
        let committed = self
            .write_object(
                &input.bucket,
                &input.key,
                body,
                input.content_type,
                input.metadata,
                &pre,
            )
            .await?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(committed.etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.read_object(req.input).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let info = object::head_object(self.meta.as_ref(), &input.bucket, &input.key)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(i64::try_from(info.header.size).unwrap_or(i64::MAX)),
            e_tag: Some(ETag::Strong(info.header.etag.clone())),
            last_modified: Some(ms_to_timestamp(info.pointer.last_modified_ms)),
            content_type: info.header.content_type.clone(),
            metadata: (!info.header.user_metadata.is_empty())
                .then(|| info.header.user_metadata.iter().cloned().collect()),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let mut result = object::delete_object(
            self.meta.as_ref(),
            &req.input.bucket,
            &req.input.key,
            now_ms(),
        )
        .await;
        if matches!(
            result,
            Err(MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(_)))
        ) {
            // Buckets created through this gateway are unversioned, where a
            // repeated delete is idempotent (RFC-003 §5.8). A versioned
            // bucket would need a resolution step before a second marker.
            result = object::delete_object(
                self.meta.as_ref(),
                &req.input.bucket,
                &req.input.key,
                now_ms(),
            )
            .await;
        }
        result.map_err(meta_err)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let continuation = match &input.continuation_token {
            Some(t) => {
                Some(unhex(t).ok_or_else(|| s3_error!(InvalidArgument, "bad continuation token"))?)
            }
            None => None,
        };
        let max_keys = usize::try_from(input.max_keys.unwrap_or(1000))
            .unwrap_or(1000)
            .clamp(1, 1000);
        let resp = list::list_objects(
            self.meta.as_ref(),
            &input.bucket,
            &list::ListRequest {
                prefix: input.prefix.clone().unwrap_or_default(),
                delimiter: input.delimiter.clone(),
                start_after: input.start_after.clone(),
                continuation,
                max_keys,
            },
        )
        .await
        .map_err(meta_err)?;
        let key_count =
            i32::try_from(resp.objects.len() + resp.common_prefixes.len()).unwrap_or(i32::MAX);
        let contents: Vec<Object> = resp
            .objects
            .into_iter()
            .map(|o| Object {
                key: Some(o.key),
                size: Some(i64::try_from(o.size).unwrap_or(i64::MAX)),
                e_tag: Some(ETag::Strong(o.etag)),
                last_modified: Some(ms_to_timestamp(o.last_modified_ms)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: input.max_keys,
            key_count: Some(key_count),
            is_truncated: Some(resp.is_truncated),
            contents: (!contents.is_empty()).then_some(contents),
            common_prefixes: (!resp.common_prefixes.is_empty()).then(|| {
                resp.common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect()
            }),
            continuation_token: input.continuation_token,
            next_continuation_token: resp.next_continuation.map(|t| hex(&t)),
            start_after: input.start_after,
            encoding_type: input.encoding_type,
            ..Default::default()
        }))
    }
}
