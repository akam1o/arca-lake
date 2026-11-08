//! `S3` implementation over the cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arca_lake_common::EcProfile;
use arca_lake_common::timing::{Histogram, HistogramSnapshot};
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::compat;
use arca_lake_metadata::ops::object::Precondition;
use arca_lake_metadata::ops::{bucket, list, multipart, object};
use arca_lake_metadata::records::{
    Checksum, ChecksumAlgorithm, ChecksumType, GenId, UploadId, Versioning, new_gen_id,
    upload_id_from_str, upload_id_to_string,
};
use arca_lake_metadata::records::{ClusterPhase, ClusterStateRecord};
use arca_lake_metadata::{KvStore, MetaError};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::cache::CacheTierConfig;
use arca_lake_tiering::durable::Coders;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketVersioningStatus,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
    CopyObjectOutput, CopyObjectResult, CopyPartResult, CopySource, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteBucketPolicyInput, DeleteBucketPolicyOutput, DeleteMarkerEntry,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject,
    ETag, ETagCondition, GetBucketLocationInput, GetBucketLocationOutput, GetBucketPolicyInput,
    GetBucketPolicyOutput, GetBucketVersioningInput, GetBucketVersioningOutput, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    Initiator, ListBucketsInput, ListBucketsOutput, ListMultipartUploadsInput,
    ListMultipartUploadsOutput, ListObjectVersionsInput, ListObjectVersionsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput, MetadataDirective,
    MultipartUpload, Object, ObjectVersion, Owner, Part, PutBucketPolicyInput,
    PutBucketPolicyOutput, PutBucketVersioningInput, PutBucketVersioningOutput, PutObjectInput,
    PutObjectOutput, Timestamp, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
    UploadPartOutput,
};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::map::ClusterMap;
use super::popqueue::{PopQueueStatsSnapshot, PopulationQueue};
use super::write::RequestChecksum;
use crate::auth::Principal;
use crate::checksum;
use crate::policy::Policy;

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
    /// Mutual TLS to storage nodes (ADR-0017); `None` = plaintext.
    pub tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    /// Read-through cache tier (RFC-006); `None` disables caching.
    pub cache: Option<CacheTierConfig>,
    /// Chunks read ahead of the one being streamed to the client (1 =
    /// sequential). Bounds the gateway's buffered bytes per GET to about
    /// `read_parallelism` chunks (RFC-009 §9).
    pub read_parallelism: usize,
    /// Cache populations waiting in the gateway's queue at most; beyond
    /// that a miss is served without populating (RFC-006 §5.3.1). A
    /// queued population holds its chunk's bytes: memory is bounded by
    /// this times the chunk size.
    pub population_queue_depth: usize,
    /// Cache populations one node receives from this gateway at once.
    pub populations_per_node: usize,
    /// Chunk placements of one PUT in flight at once (bounded memory:
    /// that many chunks plus their shards per PUT).
    pub write_pipeline: usize,
    /// Minimum size of every multipart part but the last (AWS: 5 MiB;
    /// smaller values are a documented non-AWS extension for labs).
    pub multipart_min_part_bytes: u64,
    /// Process id of this gateway's heartbeat record (RFC-014 §5.5);
    /// usually the listen address.
    pub process_id: String,
    /// Heartbeat interval; `None` disables the heartbeat (unit tests).
    pub heartbeat: Option<Duration>,
    /// Admit indefinitely while no admission record was ever published
    /// (test clusters that run no controller). Off in deployments: without
    /// a record, writes are throttled and then refused (Phase 3.8).
    pub admission_without_controller: bool,
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
    /// Every transaction is guarded by the cluster state (RFC-015 §5.6):
    /// client writes need `Active`.
    pub(super) meta: Arc<GuardedKv<S>>,
    /// Repair-class writes (integrity reports) admitted in `ReadOnly` too.
    pub(super) meta_repair: Arc<GuardedKv<S>>,
    /// Cluster facts refreshed every 2 s (admission checks).
    pub(super) cluster: Arc<ClusterWatch>,
    pub(super) clients: NodeClients,
    pub(super) map: Arc<RwLock<Arc<ClusterMap>>>,
    pub(super) cfg: Arc<ClusterConfig>,
    pub(super) coders: Arc<Coders>,
    fault: Arc<Mutex<Option<FaultPoint>>>,
    pub(super) cache_stats: Arc<CacheStats>,
    pub(super) cache_state: Arc<CacheState>,
    pub(super) put_stats: Arc<PutStats>,
    pub(super) pop_queue: Arc<PopulationQueue>,
    pub(super) integrity: Arc<IntegrityState>,
}

/// Tell the node clients which node serves each address: over TLS a node
/// must then present exactly its own identity (ADR-0017).
fn note_node_ids(clients: &NodeClients, map: &ClusterMap) {
    for n in map.nodes.values() {
        clients.note_node_id(&n.address, n.id);
    }
}

/// The cluster state as this gateway last saw it (RFC-015 §5.6 admission;
/// refreshed every [`CLUSTER_REFRESH`]), with the incarnation the process
/// started under.
#[derive(Debug)]
pub struct ClusterWatch {
    /// Incarnation at start.
    pub incarnation: [u8; 16],
    state: RwLock<Option<ClusterStateRecord>>,
    /// Admission refusals by phase.
    pub refused: std::sync::atomic::AtomicU64,
    /// Refreshes that failed (the last state was kept).
    pub refresh_failures: std::sync::atomic::AtomicU64,
    /// Write admission from the controller's record (Phase 3.6).
    pub admission: super::admission::WriteAdmission,
}

/// Interval of the cluster-state refresh.
pub const CLUSTER_REFRESH: Duration = Duration::from_secs(2);

impl ClusterWatch {
    /// Current phase, if known.
    #[must_use]
    pub fn phase(&self) -> Option<ClusterPhase> {
        self.state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|s| s.state)
    }

    /// The record.
    #[must_use]
    pub fn record(&self) -> Option<ClusterStateRecord> {
        self.state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replace the record (the refresher, and tests that flip the state
    /// without waiting for the refresh interval).
    pub fn set(&self, rec: Option<ClusterStateRecord>) {
        *self.state.write().unwrap_or_else(PoisonError::into_inner) = rec;
    }

    /// Admission: client writes need `Active`; reads need anything but
    /// `Restoring`; an unknown state or a changed incarnation refuses.
    fn admit(&self, write: bool) -> Result<(), s3s::S3Error> {
        let refuse = |why: String| {
            self.refused
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            s3_error!(
                ServiceUnavailable,
                "cluster not available for this request: {why}"
            )
        };
        let Some(rec) = self.record() else {
            return Err(refuse("cluster state unknown".into()));
        };
        if rec.incarnation != self.incarnation {
            return Err(refuse(
                "cluster incarnation changed since this gateway started; restart it".into(),
            ));
        }
        let ok = if write {
            rec.state.admits(OpClass::ClientWrite)
        } else {
            rec.state.admits_client_reads()
        };
        if ok {
            Ok(())
        } else {
            Err(refuse(format!("cluster is {}", rec.state.name())))
        }
    }
}

/// Read-path integrity counters (RFC-008 §8.7).
#[derive(Debug, Default)]
pub struct IntegrityStats {
    /// Durable reads that needed parity (a shard was missing or unusable).
    pub degraded_reads: std::sync::atomic::AtomicU64,
    /// Durable reads that failed for lack of `k` usable shards.
    pub unreadable_reads: std::sync::atomic::AtomicU64,
    /// Reads that found the chunk moved since their manifest was read and
    /// read it at its new placement (Phase 3.8).
    pub moved_reads: std::sync::atomic::AtomicU64,
    /// Repair tasks reported to the controller's queue.
    pub integrity_reports: std::sync::atomic::AtomicU64,
    /// Reports suppressed by deduplication or the bounded map.
    pub integrity_reports_dropped: std::sync::atomic::AtomicU64,
}

/// A copy of [`IntegrityStats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct IntegrityStatsSnapshot {
    /// See [`IntegrityStats::degraded_reads`].
    pub degraded_reads: u64,
    /// See [`IntegrityStats::unreadable_reads`].
    pub unreadable_reads: u64,
    /// See [`IntegrityStats::moved_reads`].
    pub moved_reads: u64,
    /// See [`IntegrityStats::integrity_reports`].
    pub integrity_reports: u64,
    /// See [`IntegrityStats::integrity_reports_dropped`].
    pub integrity_reports_dropped: u64,
}

/// Deduplicating report state: one report per chunk per
/// [`REPORT_INTERVAL_MS`], at most [`REPORT_CAP`] chunks remembered.
#[derive(Debug, Default)]
pub struct IntegrityState {
    /// Counters.
    pub stats: IntegrityStats,
    reported: Mutex<HashMap<[u8; 16], u64>>,
}

/// Minimum interval between two reports of the same chunk.
const REPORT_INTERVAL_MS: u64 = 10 * 60 * 1000;
/// Chunks remembered for deduplication.
const REPORT_CAP: usize = 10_000;

impl IntegrityState {
    /// Whether a report for `chunk_id` should be sent now.
    pub(super) fn should_report(&self, chunk_id: [u8; 16], now: u64) -> bool {
        let mut m = self.reported.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(&last) = m.get(&chunk_id)
            && now.saturating_sub(last) < REPORT_INTERVAL_MS
        {
            return false;
        }
        if m.len() >= REPORT_CAP && !m.contains_key(&chunk_id) {
            m.retain(|_, t| now.saturating_sub(*t) < REPORT_INTERVAL_MS);
            if m.len() >= REPORT_CAP {
                return false;
            }
        }
        m.insert(chunk_id, now);
        true
    }

    /// Snapshot of the counters.
    #[must_use]
    pub fn snapshot(&self) -> IntegrityStatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        IntegrityStatsSnapshot {
            degraded_reads: self.stats.degraded_reads.load(Relaxed),
            unreadable_reads: self.stats.unreadable_reads.load(Relaxed),
            moved_reads: self.stats.moved_reads.load(Relaxed),
            integrity_reports: self.stats.integrity_reports.load(Relaxed),
            integrity_reports_dropped: self.stats.integrity_reports_dropped.load(Relaxed),
        }
    }
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
            integrity: Arc::clone(&self.integrity),
            meta: Arc::clone(&self.meta),
            meta_repair: Arc::clone(&self.meta_repair),
            cluster: Arc::clone(&self.cluster),
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

/// Bodies of refused writes read (and dropped) before answering, at most:
/// a client that sends its whole body before reading the response (most
/// SDKs) would otherwise see the connection close instead of the `503`.
const REFUSED_BODY_DRAIN: usize = 16 << 20;

/// `err`, after reading what is left of `body` up to [`REFUSED_BODY_DRAIN`]
/// bytes within 10 s.
async fn refuse_after_body(body: Option<s3s::dto::StreamingBlob>, err: S3Error) -> S3Error {
    use futures::StreamExt as _;
    if let Some(mut body) = body {
        let drain = async {
            let mut read = 0usize;
            while let Some(Ok(b)) = body.next().await {
                read += b.len();
                if read >= REFUSED_BODY_DRAIN {
                    break;
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(10), drain).await;
    }
    err
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
        // Startup gate (RFC-014 §5.4): refuse a schema outside this build's
        // window; initialise an empty store; never stamp a store with data.
        let gate = compat::check_startup(
            meta.as_ref(),
            &crate::capabilities(),
            compat::Stamp::FreshOnly,
            &cfg.process_id,
            now_ms(),
        )
        .await?;
        let state = gate
            .cluster
            .clone()
            .ok_or_else(|| MetaError::InvalidArgument("cluster state record missing".into()))?;
        arca_lake_metadata::records::set_schema_write(gate.schema_write.unwrap_or(0));
        tracing::info!(
            schema = ?gate.schema,
            schema_write = ?gate.schema_write,
            format_write = ?gate.format_write,
            phase = state.state.name(),
            incarnation = %compat::hex(&state.incarnation),
            "gateway startup gate passed"
        );
        let watch_admission =
            super::admission::WriteAdmission::for_incarnation(cfg.tier.clone(), state.incarnation);
        watch_admission.assume_normal_without_record(cfg.admission_without_controller);
        let cluster = Arc::new(ClusterWatch {
            incarnation: state.incarnation,
            state: RwLock::new(Some(state.clone())),
            refused: std::sync::atomic::AtomicU64::new(0),
            refresh_failures: std::sync::atomic::AtomicU64::new(0),
            admission: watch_admission,
        });
        let base = Arc::clone(&meta);
        let meta_repair = Arc::new(GuardedKv::new(
            Arc::clone(&base),
            state.incarnation,
            OpClass::Repair,
        ));
        let meta = Arc::new(GuardedKv::new(
            base,
            state.incarnation,
            OpClass::ClientWrite,
        ));
        let map = ClusterMap::load(meta.as_ref()).await?;
        let permits = cfg
            .cache
            .as_ref()
            .map_or(8, CacheTierConfig::population_permits);
        let clients = NodeClients::with_policy(cfg.rpc_policy.clone().unwrap_or_else(|| {
            arca_lake_protocol::client::RpcPolicy::from_timeout(cfg.node_timeout)
        }))
        .with_tls(cfg.tls.clone());
        clients.set_incarnation(Some(state.incarnation));
        // The delete fence (ADR-0023): a gateway deletes (its own partial
        // copies) only under the generation open now; refreshed with the
        // cluster state.
        let fence = arca_lake_metadata::ops::delete_fence::current(meta.inner().as_ref()).await?;
        clients.set_delete_fence(fence.open.then_some(fence.generation));
        Self::spawn_cluster_refresh(
            Arc::clone(meta.inner()),
            Arc::downgrade(&cluster),
            clients.clone(),
        );
        note_node_ids(&clients, &map);
        let pop_queue = Arc::new(PopulationQueue::new(
            clients.clone(),
            permits,
            cfg.population_queue_depth,
            cfg.populations_per_node,
        ));
        if let Some(every) = cfg.heartbeat {
            Self::spawn_heartbeat(
                Arc::clone(meta.inner()),
                cfg.process_id.clone(),
                state.incarnation,
                every,
                Arc::downgrade(&cluster),
            );
        }
        Ok(Self {
            meta,
            meta_repair,
            cluster,
            clients,
            pop_queue,
            map: Arc::new(RwLock::new(Arc::new(map))),
            cfg: Arc::new(cfg),
            coders: Arc::new(Coders::default()),
            fault: Arc::new(Mutex::new(None)),
            cache_stats: Arc::new(CacheStats::default()),
            integrity: Arc::new(IntegrityState::default()),
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

    /// The metadata store (unguarded; authorizer and tests).
    #[must_use]
    pub fn metadata(&self) -> &Arc<S> {
        self.meta.inner()
    }

    /// The cluster state watch (phase, incarnation, refusals).
    #[must_use]
    pub fn cluster(&self) -> &Arc<ClusterWatch> {
        &self.cluster
    }

    /// Refuse a client write unless the cluster is `Active` (RFC-015 §5.6).
    pub(super) fn admit_write(&self) -> Result<(), s3s::S3Error> {
        self.cluster.admit(true)
    }

    /// Refuse a client read while the cluster is `Restoring`.
    pub(super) fn admit_read(&self) -> Result<(), s3s::S3Error> {
        self.cluster.admit(false)
    }

    /// Header-time capacity check of new client data (Phase 3.6; charged
    /// per chunk since Phase 3.7): `len` bytes, or one maximum chunk when
    /// the length is not known up front.
    pub(super) fn admit_data(&self, len: Option<i64>) -> Result<(), s3s::S3Error> {
        let bytes = len
            .and_then(|l| u64::try_from(l).ok())
            .unwrap_or(self.cfg.profile.max_chunk_bytes);
        self.cluster.admission.admit_data(bytes)
    }

    /// Refresh the cluster state and `schema_write` every
    /// [`CLUSTER_REFRESH`] while the backend lives.
    fn spawn_cluster_refresh(
        meta: Arc<S>,
        watch: std::sync::Weak<ClusterWatch>,
        clients: NodeClients,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(CLUSTER_REFRESH).await;
                let Some(w) = watch.upgrade() else {
                    return;
                };
                match compat::gate(meta.as_ref()).await {
                    Ok(g) => {
                        if let Some(sw) = g.schema_write {
                            arca_lake_metadata::records::set_schema_write(sw);
                        }
                        w.set(g.cluster);
                        // A failed read keeps the last record, which then
                        // ages into a stricter state (fail closed).
                        if let Ok(rec) =
                            arca_lake_metadata::ops::admission::current(meta.as_ref()).await
                        {
                            w.admission.observe(rec, std::time::Instant::now());
                        }
                        // A failed read closes it here (fail closed).
                        let fence =
                            arca_lake_metadata::ops::delete_fence::current(meta.as_ref()).await;
                        clients.set_delete_fence(
                            fence.ok().and_then(|f| f.open.then_some(f.generation)),
                        );
                    }
                    Err(e) => {
                        // A metadata outage keeps the last known state: the
                        // commit-time guard is the authoritative fence for
                        // writes, and reads must not be refused because the
                        // store was briefly unreachable (RFC-011 §6).
                        w.refresh_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(error = %e, "cluster state refresh failed; keeping the last state");
                    }
                }
            }
        });
    }

    /// Write this process's heartbeat (RFC-014 §5.5) every `every`.
    fn spawn_heartbeat(
        meta: Arc<S>,
        id: String,
        incarnation: [u8; 16],
        every: Duration,
        watch: std::sync::Weak<ClusterWatch>,
    ) {
        tokio::spawn(async move {
            loop {
                if watch.upgrade().is_none() {
                    let _ = compat::remove_heartbeat(meta.as_ref(), &id).await;
                    return;
                }
                let rec = arca_lake_metadata::records::ProcRecord {
                    id: id.clone(),
                    kind: arca_lake_metadata::records::ProcKind::Gateway,
                    software: arca_lake_common::compat::software_version(),
                    caps: crate::capabilities(),
                    incarnation,
                    last_ms: now_ms(),
                };
                if let Err(e) = compat::heartbeat(meta.as_ref(), &rec).await {
                    tracing::debug!(error = %e, "heartbeat failed");
                }
                tokio::time::sleep(every).await;
            }
        });
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
        note_node_ids(&self.clients, &fresh);
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

    /// Read-path integrity counters of this gateway (RFC-008 §8.7).
    #[must_use]
    pub fn integrity_stats(&self) -> IntegrityStatsSnapshot {
        self.integrity.snapshot()
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

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// DeleteObject without a client precondition. Every attempt is pinned
    /// to the version it was decided on (the pointer moved since the pin:
    /// pin again), and the delete marker's version id is chosen once, so
    /// that an unknown commit outcome is resolved without acting on a
    /// version written since and without a second marker (Phase 3.5 audit
    /// CON-1, CON-3, CON-4). When the re-run finds the pointer moved, the
    /// first attempt is proven applied by its marker on a versioning-enabled
    /// bucket; elsewhere the visible state is the same either way (the
    /// other write displaced the version); an unproven versioned delete is
    /// reported as unknown (503).
    async fn delete_unconditional(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<object::Deleted, MetaError> {
        let meta = self.meta.as_ref();
        let marker = arca_lake_metadata::records::new_version_id();
        let mut moved = 0;
        loop {
            let pin = object::current_identity(meta, bucket, key).await?;
            let pinned = object::Precondition {
                if_current: Some(pin),
                ..object::Precondition::default()
            };
            match object::delete_object_as(meta, bucket, key, &pinned, &marker, now_ms()).await {
                Err(MetaError::PreconditionFailed) if moved < 4 => moved += 1,
                Err(MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(r))) => {
                    return match object::delete_object_as(
                        meta,
                        bucket,
                        key,
                        &pinned,
                        &marker,
                        now_ms(),
                    )
                    .await
                    {
                        Err(MetaError::PreconditionFailed) => {
                            let b =
                                arca_lake_metadata::ops::bucket::get_bucket(meta, bucket).await?;
                            if object::version_exists(meta, bucket, key, &marker).await? {
                                Ok(object::Deleted {
                                    delete_marker_version: Some(marker),
                                    retired: None,
                                })
                            } else if b.versioning
                                == arca_lake_metadata::records::Versioning::Enabled
                            {
                                Err(MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(r)))
                            } else {
                                Ok(object::Deleted {
                                    delete_marker_version: None,
                                    retired: None,
                                })
                            }
                        }
                        other => other,
                    };
                }
                other => return other,
            }
        }
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
        MetaError::Kv(arca_lake_metadata::KvError::ClusterState(r)) => {
            s3_error!(ServiceUnavailable, "cluster not writable: {r}")
        }
        MetaError::NoSuchUpload => s3_error!(NoSuchUpload),
        MetaError::NoSuchVersion => s3_error!(NoSuchVersion),
        MetaError::InvalidPart(n) => s3_error!(
            InvalidPart,
            "one or more of the specified parts could not be found or its ETag or checksum did not match (part {n})"
        ),
        MetaError::InvalidPartOrder => s3_error!(InvalidPartOrder),
        MetaError::EntityTooSmall(n) => s3_error!(
            EntityTooSmall,
            "part {n} is smaller than the minimum allowed size"
        ),
        MetaError::TooManyRetries => s3_error!(SlowDown, "metadata transaction conflicts; retry"),
        // A write whose pending generation was retired under it (its lease
        // expired, Phase 3.8 finding), or a read racing a collection: no
        // partial state is visible; retrying is correct.
        MetaError::NoSuchGeneration => s3_error!(
            ServiceUnavailable,
            "the generation is gone (a write's lease expired, or it was collected); nothing partial is visible; retry"
        ),
        other => s3_error!(InternalError, "metadata: {other}"),
    }
}

pub(super) fn ms_to_timestamp(ms: u64) -> Timestamp {
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

/// Principal of a request, as recorded by the authorizer (`""` when the
/// gateway runs without one).
fn principal_of<T>(req: &S3Request<T>) -> String {
    req.extensions
        .get::<Principal>()
        .map(|p| p.0.clone())
        .unwrap_or_default()
}

/// Translate the `x-amz-checksum-*` inputs of an upload (RFC-007 §5.7).
#[allow(clippy::too_many_arguments)]
fn request_checksum(
    algorithm: Option<&s3s::dto::ChecksumAlgorithm>,
    crc32: Option<&str>,
    crc32c: Option<&str>,
    crc64nvme: Option<&str>,
    sha1: Option<&str>,
    sha256: Option<&str>,
    unsupported: bool,
    trailing: Option<s3s::TrailingHeaders>,
) -> S3Result<RequestChecksum> {
    if unsupported {
        return Err(s3_error!(
            NotImplemented,
            "only CRC32, CRC32C, CRC64NVME, SHA1 and SHA256 checksums are supported"
        ));
    }
    let headers = [
        (ChecksumAlgorithm::Crc32, crc32),
        (ChecksumAlgorithm::Crc32c, crc32c),
        (ChecksumAlgorithm::Crc64Nvme, crc64nvme),
        (ChecksumAlgorithm::Sha1, sha1),
        (ChecksumAlgorithm::Sha256, sha256),
    ];
    let present: Vec<_> = headers.iter().filter(|(_, v)| v.is_some()).collect();
    if present.len() > 1 {
        return Err(s3_error!(
            InvalidRequest,
            "expecting a single x-amz-checksum-* header"
        ));
    }
    let mut out = RequestChecksum {
        trailing,
        ..Default::default()
    };
    if let Some((algo, Some(v))) = present.first() {
        let decoded = checksum::decode_b64(*algo, v).ok_or_else(|| {
            s3_error!(
                InvalidRequest,
                "value for x-amz-checksum-{} header is invalid",
                algo.name().to_ascii_lowercase()
            )
        })?;
        out.algorithm = Some(*algo);
        out.expected = Some(decoded);
    }
    if let Some(a) = algorithm {
        let algo = ChecksumAlgorithm::parse(a.as_str()).ok_or_else(|| {
            s3_error!(
                NotImplemented,
                "checksum algorithm {} is not supported",
                a.as_str()
            )
        })?;
        if out.algorithm.is_some_and(|x| x != algo) {
            return Err(s3_error!(
                InvalidRequest,
                "x-amz-sdk-checksum-algorithm does not match the checksum header"
            ));
        }
        out.algorithm = Some(algo);
    }
    Ok(out)
}

/// Conditional-write headers of PUT, Complete and CopyObject destinations.
fn precondition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<Precondition> {
    let mut pre = Precondition::default();
    if let Some(c) = if_none_match {
        if !c.is_any() {
            return Err(s3_error!(
                NotImplemented,
                "If-None-Match with an ETag is not supported"
            ));
        }
        pre.if_none_match_any = true;
    }
    if let Some(c) = if_match {
        match c.as_etag() {
            Some(ETag::Strong(s)) => pre.if_match = Some(s.clone()),
            Some(ETag::Weak(_)) => return Err(s3_error!(PreconditionFailed)),
            None => return Err(s3_error!(NotImplemented, "If-Match: * is not supported")),
        }
    }
    Ok(pre)
}

fn etag_str(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.trim_matches('"').to_string(),
        None => String::new(),
    }
}

fn parse_upload_id(s: &str) -> S3Result<UploadId> {
    upload_id_from_str(s).ok_or_else(|| s3_error!(NoSuchUpload))
}

fn owner_of(name: &str) -> Owner {
    Owner {
        display_name: Some(name.to_string()),
        id: Some(name.to_string()),
    }
}

/// Checksum output fields of a stored checksum as a tuple for the
/// repetitive output structs.
fn cs(c: Option<&Checksum>) -> super::read::ChecksumFields {
    super::read::checksum_fields(c)
}

/// Validate the announced `(algorithm, type)` pair of a multipart upload
/// (RFC-007 §5.7).
fn multipart_checksum_spec(
    algorithm: Option<&s3s::dto::ChecksumAlgorithm>,
    kind: Option<&s3s::dto::ChecksumType>,
) -> S3Result<(Option<ChecksumAlgorithm>, Option<ChecksumType>)> {
    let algo = match algorithm {
        None => None,
        Some(a) => Some(ChecksumAlgorithm::parse(a.as_str()).ok_or_else(|| {
            s3_error!(
                NotImplemented,
                "checksum algorithm {} is not supported",
                a.as_str()
            )
        })?),
    };
    let kind = match kind {
        None => None,
        Some(k) => Some(
            ChecksumType::parse(k.as_str())
                .ok_or_else(|| s3_error!(InvalidRequest, "unknown checksum type {}", k.as_str()))?,
        ),
    };
    match (algo, kind) {
        (None, Some(_)) => Err(s3_error!(
            InvalidRequest,
            "checksum type requires a checksum algorithm"
        )),
        (Some(a), Some(ChecksumType::FullObject)) if !a.combinable() => Err(s3_error!(
            InvalidRequest,
            "{} does not support full-object checksums for multipart uploads",
            a.name()
        )),
        (Some(ChecksumAlgorithm::Crc64Nvme), Some(ChecksumType::Composite)) => Err(s3_error!(
            InvalidRequest,
            "CRC64NVME supports only full-object checksums"
        )),
        _ => Ok((algo, kind)),
    }
}

/// `multipart::complete` errors to S3 errors.
fn complete_err(e: MetaError) -> S3Error {
    match e {
        MetaError::InvalidArgument(m) if m.contains("checksum") => s3_error!(BadDigest, "{m}"),
        other => meta_err(other),
    }
}

/// Part records retired per transaction after Complete and Abort.
const DISCARD_BATCH: usize = 256;

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// Retire the parts an upload no longer needs (unlisted after Complete,
    /// all after Abort), bounded per transaction. Best effort: the
    /// controller sweep finishes whatever a dying gateway leaves.
    async fn discard_all(&self, bucket_id: u64, key: &str, upload_id: &UploadId) {
        loop {
            match multipart::discard_parts(
                self.meta.as_ref(),
                bucket_id,
                key,
                upload_id,
                DISCARD_BATCH,
                now_ms(),
            )
            .await
            {
                Ok(n) if n >= DISCARD_BATCH => {}
                _ => return,
            }
        }
    }

    /// Complete, resolving an unknown commit outcome by re-running: the
    /// recipe returns the same result once it finds `Completed { root }`
    /// for our root id (RFC-007 §5.3.2).
    async fn complete_resolving(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
        req: &multipart::CompleteRequest<'_>,
    ) -> S3Result<multipart::Completed> {
        let mut last = None;
        for _ in 0..4 {
            match multipart::complete(self.meta.as_ref(), bucket, key, upload_id, req, now_ms())
                .await
            {
                Ok(c) => return Ok(c),
                Err(MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(r))) => last = Some(r),
                Err(e) => return Err(complete_err(e)),
            }
        }
        Err(s3_error!(
            ServiceUnavailable,
            "complete outcome unknown: {}",
            last.unwrap_or_default()
        ))
    }
}

#[async_trait::async_trait]
#[allow(clippy::too_many_lines)]
impl<S: KvStore + 'static> S3 for ClusterBackend<S> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.admit_write()?;
        let owner = principal_of(&req);
        bucket::create_bucket_owned(
            self.meta.as_ref(),
            &req.input.bucket,
            Versioning::Disabled,
            &owner,
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
        self.admit_write()?;
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

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        bucket::get_bucket(self.meta.as_ref(), &req.input.bucket)
            .await
            .map_err(meta_err)?;
        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        // Owner-scoped (RFC-012 §5.3.2 rule 3): without an authorizer the
        // principal is unknown and every bucket is listed.
        let principal = req.extensions.get::<Principal>().map(|p| p.0.clone());
        let buckets = bucket::list_buckets(self.meta.as_ref())
            .await
            .map_err(meta_err)?;
        let list = buckets
            .into_iter()
            .filter(|b| principal.as_ref().is_none_or(|p| b.owner == *p))
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(ms_to_timestamp(b.created_ms)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(list),
            owner: principal.as_deref().map(owner_of),
            ..Default::default()
        }))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let b = bucket::get_bucket(self.meta.as_ref(), &req.input.bucket)
            .await
            .map_err(meta_err)?;
        let status = match b.versioning {
            Versioning::Disabled => None,
            Versioning::Enabled => Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::ENABLED,
            )),
            Versioning::Suspended => Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::SUSPENDED,
            )),
        };
        Ok(S3Response::new(GetBucketVersioningOutput {
            status,
            ..Default::default()
        }))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        self.admit_write()?;
        let cfg = req.input.versioning_configuration;
        if cfg.mfa_delete.is_some() || req.input.mfa.is_some() {
            return Err(s3_error!(NotImplemented, "MFA delete is not supported"));
        }
        let versioning = match cfg
            .status
            .as_ref()
            .map(s3s::dto::BucketVersioningStatus::as_str)
        {
            Some(BucketVersioningStatus::ENABLED) => Versioning::Enabled,
            Some(BucketVersioningStatus::SUSPENDED) => Versioning::Suspended,
            _ => {
                return Err(s3_error!(
                    MalformedXML,
                    "Status must be Enabled or Suspended"
                ));
            }
        };
        bucket::set_versioning(self.meta.as_ref(), &req.input.bucket, versioning)
            .await
            .map_err(meta_err)?;
        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }

    async fn get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let b = bucket::get_bucket(self.meta.as_ref(), &req.input.bucket)
            .await
            .map_err(meta_err)?;
        let Some(policy) = b.policy else {
            return Err(s3_error!(NoSuchBucketPolicy));
        };
        Ok(S3Response::new(GetBucketPolicyOutput {
            policy: Some(policy),
        }))
    }

    async fn put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        self.admit_write()?;
        // Rule 5: every unsupported element is rejected here, with its name.
        Policy::parse(&req.input.bucket, &req.input.policy)
            .map_err(|e| s3_error!(MalformedPolicy, "{}", e.0))?;
        bucket::set_policy(
            self.meta.as_ref(),
            &req.input.bucket,
            Some(&req.input.policy),
        )
        .await
        .map_err(meta_err)?;
        Ok(S3Response::new(PutBucketPolicyOutput::default()))
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        self.admit_write()?;
        bucket::set_policy(self.meta.as_ref(), &req.input.bucket, None)
            .await
            .map_err(meta_err)?;
        Ok(S3Response::new(DeleteBucketPolicyOutput::default()))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.admit_write()?;
        if let Err(e) = self.admit_data(req.input.content_length) {
            return Err(refuse_after_body(req.input.body, e).await);
        }
        let trailing = req.trailing_headers.clone();
        let input = req.input;
        let Some(body) = input.body else {
            return Err(s3_error!(IncompleteBody));
        };
        let pre = precondition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let checksum = request_checksum(
            input.checksum_algorithm.as_ref(),
            input.checksum_crc32.as_deref(),
            input.checksum_crc32c.as_deref(),
            input.checksum_crc64nvme.as_deref(),
            input.checksum_sha1.as_deref(),
            input.checksum_sha256.as_deref(),
            input.checksum_sha512.is_some()
                || input.checksum_md5.is_some()
                || input.checksum_xxhash64.is_some()
                || input.checksum_xxhash3.is_some()
                || input.checksum_xxhash128.is_some(),
            trailing,
        )?;
        let committed = self
            .write_object(
                &input.bucket,
                &input.key,
                body,
                input.content_type,
                input.metadata,
                &pre,
                &checksum,
            )
            .await?;
        let (crc32, crc32c, crc64nvme, sha1, sha256, checksum_type) =
            cs(committed.checksum.as_ref());
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(committed.etag)),
            version_id: (committed.version_id != "null").then_some(committed.version_id),
            checksum_crc32: crc32,
            checksum_crc32c: crc32c,
            checksum_crc64nvme: crc64nvme,
            checksum_sha1: sha1,
            checksum_sha256: sha256,
            checksum_type,
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.admit_read()?;
        self.read_object(req.input).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.admit_read()?;
        self.head_object_impl(req.input).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.admit_write()?;
        let input = req.input;
        if let Some(v) = &input.version_id {
            let d =
                object::delete_version(self.meta.as_ref(), &input.bucket, &input.key, v, now_ms())
                    .await
                    .map_err(meta_err)?;
            return Ok(S3Response::new(DeleteObjectOutput {
                delete_marker: d.was_delete_marker.then_some(true),
                version_id: Some(v.clone()),
                ..Default::default()
            }));
        }
        let pre = precondition(input.if_match.as_ref(), None)?;
        let result = if pre.is_empty() {
            self.delete_unconditional(&input.bucket, &input.key).await
        } else {
            // With a client precondition an unknown outcome is not re-run:
            // after an applied delete the re-run would answer 412 or 404
            // for a delete that succeeded; it is reported instead (503,
            // R-72).
            object::delete_object_if(
                self.meta.as_ref(),
                &input.bucket,
                &input.key,
                &pre,
                now_ms(),
            )
            .await
        };
        let d = result.map_err(meta_err)?;
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: d.delete_marker_version.is_some().then_some(true),
            version_id: d.delete_marker_version.filter(|v| v != "null"),
            ..Default::default()
        }))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.admit_write()?;
        // Each entry is authorized on its own (SEC-1): the request names no
        // key, so the authorizer left the decision per (key, version).
        let gate = req
            .extensions
            .get::<crate::auth::EntryAuthorizer>()
            .cloned();
        let input = req.input;
        if input.delete.objects.len() > 1000 {
            return Err(s3_error!(MalformedXML, "at most 1000 keys per request"));
        }
        let quiet = input.delete.quiet.unwrap_or(false);
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for o in input.delete.objects {
            let r: S3Result<DeletedObject> = async {
                if let Some(g) = &gate
                    && !(g.0)(&o.key, o.version_id.as_deref())
                {
                    return Err(s3_error!(AccessDenied));
                }
                if let Some(v) = &o.version_id {
                    let d = object::delete_version(
                        self.meta.as_ref(),
                        &input.bucket,
                        &o.key,
                        v,
                        now_ms(),
                    )
                    .await
                    .map_err(meta_err)?;
                    Ok(DeletedObject {
                        key: Some(o.key.clone()),
                        version_id: Some(v.clone()),
                        delete_marker: d.was_delete_marker.then_some(true),
                        delete_marker_version_id: None,
                    })
                } else {
                    let d =
                        object::delete_object(self.meta.as_ref(), &input.bucket, &o.key, now_ms())
                            .await
                            .map_err(meta_err)?;
                    Ok(DeletedObject {
                        key: Some(o.key.clone()),
                        version_id: None,
                        delete_marker: d.delete_marker_version.is_some().then_some(true),
                        delete_marker_version_id: d.delete_marker_version,
                    })
                }
            }
            .await;
            match r {
                Ok(d) => {
                    if !quiet {
                        deleted.push(d);
                    }
                }
                Err(e) => errors.push(s3s::dto::Error {
                    code: Some(e.code().as_str().to_string()),
                    key: Some(o.key),
                    message: e.message().map(str::to_string),
                    version_id: o.version_id,
                }),
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.admit_write()?;
        // Checked here, charged per chunk as the copy writes them.
        self.admit_data(None)?;
        let input = req.input;
        let CopySource::Bucket {
            bucket: src_bucket,
            key: src_key,
            version_id: src_version,
        } = &input.copy_source
        else {
            return Err(s3_error!(
                NotImplemented,
                "access point copy sources are not supported"
            ));
        };
        let replace = match input
            .metadata_directive
            .as_ref()
            .map(s3s::dto::MetadataDirective::as_str)
        {
            None | Some(MetadataDirective::COPY) => false,
            Some(MetadataDirective::REPLACE) => true,
            Some(_) => return Err(s3_error!(InvalidArgument, "unknown metadata directive")),
        };
        if **src_bucket == *input.bucket
            && **src_key == *input.key
            && src_version.is_none()
            && !replace
        {
            return Err(s3_error!(
                InvalidRequest,
                "this copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes"
            ));
        }
        // Source: fixed at this manifest read (RFC-007 §5.6); copy-source
        // conditions all map to 412.
        let conditions = super::read::Conditions {
            if_match: input.copy_source_if_match.as_ref(),
            if_none_match: input.copy_source_if_none_match.as_ref(),
            if_modified_since: input.copy_source_if_modified_since.as_ref(),
            if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
        };
        let src = self
            .describe(
                src_bucket,
                src_key,
                src_version.as_deref(),
                &conditions,
                None,
            )
            .await
            .map_err(|e| {
                if e.code() == &S3ErrorCode::NotModified {
                    s3_error!(PreconditionFailed)
                } else {
                    e
                }
            })?;
        let src_manifest = src.manifest;
        let pre = precondition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let (content_type, metadata) = if replace {
            (input.content_type, input.metadata)
        } else {
            (
                src_manifest.header.content_type.clone(),
                (!src_manifest.header.user_metadata.is_empty())
                    .then(|| src_manifest.header.user_metadata.iter().cloned().collect()),
            )
        };
        // The destination checksum is recomputed from the bytes copied: the
        // requested algorithm, else the source's.
        let algorithm = match input.checksum_algorithm.as_ref() {
            Some(a) => Some(ChecksumAlgorithm::parse(a.as_str()).ok_or_else(|| {
                s3_error!(
                    NotImplemented,
                    "checksum algorithm {} is not supported",
                    a.as_str()
                )
            })?),
            None => src_manifest.header.checksum.as_ref().map(|c| c.algorithm),
        };
        let checksum = RequestChecksum {
            algorithm,
            expected: None,
            trailing: None,
        };
        let map = self.map_up_to_date().await.map_err(meta_err)?;
        self.note_access(src_manifest.pointer.generation);
        let body = self.stream_range(map, &src_manifest, 0, src_manifest.header.size);
        let committed = self
            .write_object(
                &input.bucket,
                &input.key,
                body,
                content_type,
                metadata,
                &pre,
                &checksum,
            )
            .await?;
        let (crc32, crc32c, crc64nvme, sha1, sha256, checksum_type) =
            cs(committed.checksum.as_ref());
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(committed.etag)),
                last_modified: Some(ms_to_timestamp(now_ms())),
                checksum_crc32: crc32,
                checksum_crc32c: crc32c,
                checksum_crc64nvme: crc64nvme,
                checksum_sha1: sha1,
                checksum_sha256: sha256,
                checksum_type,
                ..Default::default()
            }),
            version_id: (committed.version_id != "null").then_some(committed.version_id),
            copy_source_version_id: (src_manifest.pointer.version_id != "null")
                .then(|| src_manifest.pointer.version_id.clone()),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.admit_read()?;
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

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let input = req.input;
        let max_keys = usize::try_from(input.max_keys.unwrap_or(1000))
            .unwrap_or(1000)
            .clamp(1, 1000);
        let resp = list::list_versions(
            self.meta.as_ref(),
            &input.bucket,
            &list::ListVersionsRequest {
                prefix: input.prefix.clone().unwrap_or_default(),
                delimiter: input.delimiter.clone(),
                key_marker: input.key_marker.clone().filter(|k| !k.is_empty()),
                version_id_marker: input.version_id_marker.clone().filter(|v| !v.is_empty()),
                max_keys,
            },
        )
        .await
        .map_err(meta_err)?;
        let mut versions = Vec::new();
        let mut markers = Vec::new();
        for v in resp.versions {
            if v.delete_marker {
                markers.push(DeleteMarkerEntry {
                    is_latest: Some(v.is_latest),
                    key: Some(v.key),
                    last_modified: Some(ms_to_timestamp(v.last_modified_ms)),
                    version_id: Some(v.version_id),
                    owner: None,
                });
            } else {
                versions.push(ObjectVersion {
                    e_tag: Some(ETag::Strong(v.etag)),
                    is_latest: Some(v.is_latest),
                    key: Some(v.key),
                    last_modified: Some(ms_to_timestamp(v.last_modified_ms)),
                    size: Some(i64::try_from(v.size).unwrap_or(i64::MAX)),
                    version_id: Some(v.version_id),
                    ..Default::default()
                });
            }
        }
        let (next_key, next_version) = match (&resp.is_truncated, resp.next_markers) {
            (true, Some((k, v))) => (Some(k), Some(v)),
            _ => (None, None),
        };
        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: input.max_keys,
            key_marker: input.key_marker,
            version_id_marker: input.version_id_marker,
            is_truncated: Some(resp.is_truncated),
            versions: (!versions.is_empty()).then_some(versions),
            delete_markers: (!markers.is_empty()).then_some(markers),
            common_prefixes: (!resp.common_prefixes.is_empty()).then(|| {
                resp.common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect()
            }),
            next_key_marker: next_key,
            next_version_id_marker: next_version.filter(|v| !v.is_empty()),
            encoding_type: input.encoding_type,
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.admit_write()?;
        self.cluster.admission.admit_start()?;
        let owner = principal_of(&req);
        let input = req.input;
        let (algorithm, kind) = multipart_checksum_spec(
            input.checksum_algorithm.as_ref(),
            input.checksum_type.as_ref(),
        )?;
        let rec = multipart::create_upload(
            self.meta.as_ref(),
            &input.bucket,
            &input.key,
            &multipart::UploadInfo {
                owner,
                content_type: input.content_type,
                user_metadata: input
                    .metadata
                    .map(|m| m.into_iter().collect())
                    .unwrap_or_default(),
                checksum_algorithm: algorithm,
                checksum_type: kind,
            },
            now_ms(),
        )
        .await
        .map_err(meta_err)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id_to_string(&rec.upload_id)),
            checksum_algorithm: rec
                .checksum_algorithm
                .map(|a| s3s::dto::ChecksumAlgorithm::from(a.name().to_string())),
            checksum_type: rec
                .checksum_type
                .or(rec.checksum_algorithm.map(|a| {
                    if a == ChecksumAlgorithm::Crc64Nvme {
                        ChecksumType::FullObject
                    } else {
                        ChecksumType::Composite
                    }
                }))
                .map(|k| s3s::dto::ChecksumType::from(k.name().to_string())),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.admit_write()?;
        if let Err(e) = self.admit_data(req.input.content_length) {
            return Err(refuse_after_body(req.input.body, e).await);
        }
        let trailing = req.trailing_headers.clone();
        let input = req.input;
        let Some(body) = input.body else {
            return Err(s3_error!(IncompleteBody));
        };
        let upload_id = parse_upload_id(&input.upload_id)?;
        let part_number = u32::try_from(input.part_number)
            .ok()
            .filter(|n| (1..=multipart::MAX_PART_NUMBER).contains(n))
            .ok_or_else(|| s3_error!(InvalidArgument, "part number must be between 1 and 10000"))?;
        let checksum = request_checksum(
            input.checksum_algorithm.as_ref(),
            input.checksum_crc32.as_deref(),
            input.checksum_crc32c.as_deref(),
            input.checksum_crc64nvme.as_deref(),
            input.checksum_sha1.as_deref(),
            input.checksum_sha256.as_deref(),
            input.checksum_sha512.is_some()
                || input.checksum_md5.is_some()
                || input.checksum_xxhash64.is_some()
                || input.checksum_xxhash3.is_some()
                || input.checksum_xxhash128.is_some(),
            trailing,
        )?;
        let sealed = self
            .write_part(
                &input.bucket,
                &input.key,
                &upload_id,
                part_number,
                body,
                &checksum,
            )
            .await?;
        tracing::debug!(part = part_number, size = sealed.size, "part sealed");
        let (crc32, crc32c, crc64nvme, sha1, sha256, _) = cs(sealed.checksum.as_ref());
        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(sealed.etag)),
            checksum_crc32: crc32,
            checksum_crc32c: crc32c,
            checksum_crc64nvme: crc64nvme,
            checksum_sha1: sha1,
            checksum_sha256: sha256,
            ..Default::default()
        }))
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.admit_write()?;
        self.admit_data(None)?;
        let input = req.input;
        let CopySource::Bucket {
            bucket: src_bucket,
            key: src_key,
            version_id: src_version,
        } = &input.copy_source
        else {
            return Err(s3_error!(
                NotImplemented,
                "access point copy sources are not supported"
            ));
        };
        let upload_id = parse_upload_id(&input.upload_id)?;
        let part_number = u32::try_from(input.part_number)
            .ok()
            .filter(|n| (1..=multipart::MAX_PART_NUMBER).contains(n))
            .ok_or_else(|| s3_error!(InvalidArgument, "part number must be between 1 and 10000"))?;
        let conditions = super::read::Conditions {
            if_match: input.copy_source_if_match.as_ref(),
            if_none_match: input.copy_source_if_none_match.as_ref(),
            if_modified_since: input.copy_source_if_modified_since.as_ref(),
            if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
        };
        let src = self
            .describe(
                src_bucket,
                src_key,
                src_version.as_deref(),
                &conditions,
                None,
            )
            .await
            .map_err(|e| {
                if e.code() == &S3ErrorCode::NotModified {
                    s3_error!(PreconditionFailed)
                } else {
                    e
                }
            })?;
        let m = src.manifest;
        let (start, end) = match &input.copy_source_range {
            None => (0, m.header.size),
            Some(r) => {
                let r = s3s::dto::Range::parse(r)
                    .map_err(|_| s3_error!(InvalidArgument, "invalid x-amz-copy-source-range"))?
                    .check(m.header.size)
                    .map_err(|_| {
                        s3_error!(
                            InvalidArgument,
                            "x-amz-copy-source-range is not satisfiable"
                        )
                    })?;
                (r.start, r.end)
            }
        };
        let upload =
            multipart::get_upload(self.meta.as_ref(), &input.bucket, &input.key, &upload_id)
                .await
                .map_err(meta_err)?
                .ok_or_else(|| s3_error!(NoSuchUpload))?;
        let checksum = RequestChecksum {
            algorithm: upload.checksum_algorithm,
            expected: None,
            trailing: None,
        };
        let map = self.map_up_to_date().await.map_err(meta_err)?;
        let body = self.stream_range(map, &m, start, end);
        let sealed = self
            .write_part(
                &input.bucket,
                &input.key,
                &upload_id,
                part_number,
                body,
                &checksum,
            )
            .await?;
        let (crc32, crc32c, crc64nvme, sha1, sha256, _) = cs(sealed.checksum.as_ref());
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(ETag::Strong(sealed.etag)),
                last_modified: Some(ms_to_timestamp(now_ms())),
                checksum_crc32: crc32,
                checksum_crc32c: crc32c,
                checksum_crc64nvme: crc64nvme,
                checksum_sha1: sha1,
                checksum_sha256: sha256,
                ..Default::default()
            }),
            copy_source_version_id: (m.pointer.version_id != "null")
                .then(|| m.pointer.version_id.clone()),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.admit_write()?;
        let input = req.input;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let Some(listed) = input.multipart_upload.and_then(|m| m.parts) else {
            return Err(s3_error!(
                MalformedXML,
                "the XML you provided was not well-formed"
            ));
        };
        if listed.is_empty() {
            return Err(s3_error!(MalformedXML, "at least one part is required"));
        }
        let upload =
            multipart::get_upload(self.meta.as_ref(), &input.bucket, &input.key, &upload_id)
                .await
                .map_err(meta_err)?
                .ok_or_else(|| s3_error!(NoSuchUpload))?;
        let algo = upload.checksum_algorithm;
        let mut parts = Vec::with_capacity(listed.len());
        let mut etags = Vec::with_capacity(listed.len());
        for p in &listed {
            let n = p
                .part_number
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| s3_error!(InvalidPart, "part number missing"))?;
            let etag = etag_str(p.e_tag.as_ref());
            if etag.is_empty() {
                return Err(s3_error!(InvalidPart, "ETag missing for part {n}"));
            }
            let value = match algo {
                Some(ChecksumAlgorithm::Crc32) => p.checksum_crc32.as_deref(),
                Some(ChecksumAlgorithm::Crc32c) => p.checksum_crc32c.as_deref(),
                Some(ChecksumAlgorithm::Crc64Nvme) => p.checksum_crc64nvme.as_deref(),
                Some(ChecksumAlgorithm::Sha1) => p.checksum_sha1.as_deref(),
                Some(ChecksumAlgorithm::Sha256) => p.checksum_sha256.as_deref(),
                None => None,
            };
            let checksum = match (algo, value) {
                (Some(a), Some(v)) => Some(
                    checksum::decode_b64(a, v)
                        .ok_or_else(|| s3_error!(InvalidPart, "invalid checksum for part {n}"))?,
                ),
                _ => None,
            };
            etags.push(etag.clone());
            parts.push(multipart::CompletePart {
                part_number: n,
                etag,
                checksum,
            });
        }
        let etag = checksum::composite_etag(&etags);
        let pre = precondition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        // Expected object checksum from the request, if any.
        let expected = match algo {
            Some(a) => {
                let raw = match a {
                    ChecksumAlgorithm::Crc32 => input.checksum_crc32.as_deref(),
                    ChecksumAlgorithm::Crc32c => input.checksum_crc32c.as_deref(),
                    ChecksumAlgorithm::Crc64Nvme => input.checksum_crc64nvme.as_deref(),
                    ChecksumAlgorithm::Sha1 => input.checksum_sha1.as_deref(),
                    ChecksumAlgorithm::Sha256 => input.checksum_sha256.as_deref(),
                };
                match raw {
                    Some(v) => {
                        let (value, n) = checksum::parse_header_value(a, v).ok_or_else(|| {
                            s3_error!(InvalidRequest, "invalid object checksum value")
                        })?;
                        Some(Checksum {
                            algorithm: a,
                            kind: if n.is_some() {
                                ChecksumType::Composite
                            } else {
                                ChecksumType::FullObject
                            },
                            value,
                            parts: n,
                        })
                    }
                    None => None,
                }
            }
            None => None,
        };
        if let Some(size) = input.mpu_object_size
            && size < 0
        {
            return Err(s3_error!(
                InvalidRequest,
                "x-amz-mp-object-size must be non-negative"
            ));
        }
        let root = new_gen_id();
        let creq = multipart::CompleteRequest {
            parts: &parts,
            root,
            pre: &pre,
            min_part_size: self.cfg.multipart_min_part_bytes,
            etag: &etag,
            expected_checksum: expected.as_ref(),
            derive_checksum: &checksum::derive,
        };
        let done = self
            .complete_resolving(&input.bucket, &input.key, &upload_id, &creq)
            .await?;
        if let Some(size) = input.mpu_object_size
            && u64::try_from(size).is_ok_and(|s| s != done.size)
        {
            // AWS compares the declared size after the fact; the object is
            // already published atomically, so report the mismatch only.
            tracing::warn!(
                declared = size,
                actual = done.size,
                "x-amz-mp-object-size mismatch"
            );
        }
        if !done.already_completed {
            self.discard_all(upload.bucket_id, &input.key, &upload_id)
                .await;
        }
        let (crc32, crc32c, crc64nvme, sha1, sha256, checksum_type) = cs(done.checksum.as_ref());
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            location: Some(format!("/{}/{}", input.bucket, input.key)),
            e_tag: Some(ETag::Strong(done.etag)),
            version_id: (done.version_id != "null").then_some(done.version_id),
            checksum_crc32: crc32,
            checksum_crc32c: crc32c,
            checksum_crc64nvme: crc64nvme,
            checksum_sha1: sha1,
            checksum_sha256: sha256,
            checksum_type,
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.admit_write()?;
        let input = req.input;
        let upload_id = parse_upload_id(&input.upload_id)?;
        multipart::abort_and_discard(
            self.meta.as_ref(),
            &input.bucket,
            &input.key,
            &upload_id,
            DISCARD_BATCH,
            now_ms(),
        )
        .await
        .map_err(meta_err)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let after = u32::try_from(input.part_number_marker.unwrap_or(0)).unwrap_or(0);
        let max = usize::try_from(input.max_parts.unwrap_or(1000))
            .unwrap_or(1000)
            .clamp(1, 1000);
        let page = multipart::list_parts(
            self.meta.as_ref(),
            &input.bucket,
            &input.key,
            &upload_id,
            after,
            max,
        )
        .await
        .map_err(meta_err)?;
        let next = page.parts.last().map(|(n, _)| *n);
        let parts: Vec<Part> = page
            .parts
            .into_iter()
            .map(|(n, p)| {
                let (crc32, crc32c, crc64nvme, sha1, sha256, _) = cs(p.checksum.as_ref());
                Part {
                    part_number: Some(i32::try_from(n).unwrap_or(i32::MAX)),
                    e_tag: Some(ETag::Strong(p.etag)),
                    last_modified: Some(ms_to_timestamp(p.uploaded_ms)),
                    size: Some(i64::try_from(p.size).unwrap_or(i64::MAX)),
                    checksum_crc32: crc32,
                    checksum_crc32c: crc32c,
                    checksum_crc64nvme: crc64nvme,
                    checksum_sha1: sha1,
                    checksum_sha256: sha256,
                    ..Default::default()
                }
            })
            .collect();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(input.upload_id),
            part_number_marker: input.part_number_marker,
            next_part_number_marker: page
                .is_truncated
                .then(|| next.map(|n| i32::try_from(n).unwrap_or(i32::MAX)))
                .flatten(),
            max_parts: input.max_parts,
            is_truncated: Some(page.is_truncated),
            parts: (!parts.is_empty()).then_some(parts),
            initiator: Some(Initiator {
                display_name: Some(page.upload.owner.clone()),
                id: Some(page.upload.owner.clone()),
            }),
            owner: Some(owner_of(&page.upload.owner)),
            checksum_algorithm: page
                .upload
                .checksum_algorithm
                .map(|a| s3s::dto::ChecksumAlgorithm::from(a.name().to_string())),
            checksum_type: page
                .upload
                .checksum_type
                .map(|k| s3s::dto::ChecksumType::from(k.name().to_string())),
            storage_class: Some(s3s::dto::StorageClass::from_static(
                s3s::dto::StorageClass::STANDARD,
            )),
            ..Default::default()
        }))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let max = usize::try_from(input.max_uploads.unwrap_or(1000))
            .unwrap_or(1000)
            .clamp(1, 1000);
        let upload_id_marker = match input.upload_id_marker.as_deref().filter(|s| !s.is_empty()) {
            Some(s) => Some(
                parse_upload_id(s)
                    .map_err(|_| s3_error!(InvalidArgument, "invalid upload-id-marker"))?,
            ),
            None => None,
        };
        let resp = multipart::list_uploads(
            self.meta.as_ref(),
            &input.bucket,
            &multipart::ListUploadsRequest {
                prefix: input.prefix.clone().unwrap_or_default(),
                delimiter: input.delimiter.clone(),
                key_marker: input.key_marker.clone().filter(|k| !k.is_empty()),
                upload_id_marker,
                max_uploads: max,
            },
        )
        .await
        .map_err(meta_err)?;
        let uploads: Vec<MultipartUpload> = resp
            .uploads
            .into_iter()
            .map(|u| MultipartUpload {
                key: Some(u.key),
                upload_id: Some(upload_id_to_string(&u.upload_id)),
                initiated: Some(ms_to_timestamp(u.initiated_ms)),
                initiator: Some(Initiator {
                    display_name: Some(u.owner.clone()),
                    id: Some(u.owner.clone()),
                }),
                owner: Some(owner_of(&u.owner)),
                storage_class: Some(s3s::dto::StorageClass::from_static(
                    s3s::dto::StorageClass::STANDARD,
                )),
                checksum_algorithm: u
                    .checksum_algorithm
                    .map(|a| s3s::dto::ChecksumAlgorithm::from(a.name().to_string())),
                checksum_type: u
                    .checksum_type
                    .map(|k| s3s::dto::ChecksumType::from(k.name().to_string())),
            })
            .collect();
        let (next_key, next_upload) = match (resp.is_truncated, resp.next_markers) {
            (true, Some((k, u))) => (Some(k), (u != [0xff; 16]).then(|| upload_id_to_string(&u))),
            _ => (None, None),
        };
        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: input.max_uploads,
            is_truncated: Some(resp.is_truncated),
            next_key_marker: next_key,
            next_upload_id_marker: next_upload,
            uploads: (!uploads.is_empty()).then_some(uploads),
            common_prefixes: (!resp.common_prefixes.is_empty()).then(|| {
                resp.common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect()
            }),
            encoding_type: input.encoding_type,
            ..Default::default()
        }))
    }
}
