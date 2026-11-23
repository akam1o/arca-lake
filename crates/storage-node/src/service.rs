//! gRPC service implementation.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arca_lake_common::timing::{Histogram, HistogramSnapshot};

use arca_lake_protocol::storage::v1 as pb;
use arca_lake_protocol::storage::v1::storage_node_server::{StorageNode, StorageNodeServer};
use arca_lake_storage::{ShardKey, StorageError, Store, StoreConfig};
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::epochs::EpochTable;

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Listen address; port 0 picks a free port.
    pub listen: SocketAddr,
    /// Storage engine configuration (one device).
    pub store: StoreConfig,
    /// Frame size for streamed reads.
    pub frame_bytes: usize,
    /// A `PutShard` stream whose client sends no frame for this long is
    /// aborted (slow or stalled client detection, R-47).
    pub stream_progress_timeout: Duration,
    /// Mutual TLS and per-RPC authorization (ADR-0017); `None` serves
    /// plaintext with no authorization (explicit `--insecure-plaintext`).
    pub tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    /// Background segment compaction (RFC-005 §5.6).
    pub compaction: crate::compactor::CompactionPolicy,
    /// Capacity this node advertises for placement (`Health` free and
    /// total bytes are capped by it); `None` = the file system's.
    pub capacity_bytes: Option<u64>,
    /// Bytes kept free for compaction, rollover, quarantine and repair
    /// (client writes are refused above it, Phase 3.6); `None` = the
    /// default for the device ([`crate::capacity::Reserve::default_for`]).
    pub reserve_bytes: Option<u64>,
    /// Rejoin lease of an open delete fence (Phase 3.10): deletes are
    /// admitted only this long after the last push of the open fence by a
    /// caller of the node's incarnation, measured on the node's monotonic
    /// clock; a start begins with none.
    pub fence_lease: Duration,
    /// Deferred reclamation (Phase 3.12, ADR-0028): a delete of a durable
    /// shard retires the record, which is removed only by a `ReclaimShard`
    /// whose metadata observation is at least this long after the
    /// retirement. Zero: deletes remove records at once (earlier releases'
    /// behaviour; no protection of R-94(b) on this node).
    pub reclaim_retention: Duration,
    /// Clock skew allowed between a reclaim's observation and this node's
    /// clock (an observation further ahead is refused).
    pub reclaim_skew: Duration,
}

impl NodeConfig {
    /// Defaults for one device directory.
    #[must_use]
    pub fn new(listen: SocketAddr, dir: impl Into<PathBuf>) -> Self {
        Self {
            listen,
            store: StoreConfig::new(dir),
            frame_bytes: 256 << 10,
            stream_progress_timeout: Duration::from_secs(10),
            tls: None,
            compaction: crate::compactor::CompactionPolicy::default(),
            capacity_bytes: None,
            reserve_bytes: None,
            fence_lease: Duration::from_secs(30),
            reclaim_retention: Duration::ZERO,
            reclaim_skew: Duration::from_secs(60),
        }
    }
}

/// A request's incarnation (zeros: none, or not 16 bytes).
fn incarnation_of(b: &[u8]) -> [u8; 16] {
    <[u8; 16]>::try_from(b).unwrap_or([0; 16])
}

/// The node's wall clock (ms since the epoch; 0 before it).
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The service state shared by all requests.
#[derive(Debug)]
pub struct StorageNodeService {
    store: Arc<Store>,
    epochs: Mutex<EpochTable>,
    /// Destructive RPCs hold it shared from their fence checks to the end of
    /// the store mutation; raising an epoch or adopting an incarnation holds
    /// it exclusively. Once such an update is acknowledged, no delete that
    /// passed the old fence can still run (Phase 3.5 audit SEC-6).
    ///
    /// Owned guards (Phase 3.9): the shared guard moves into the blocking
    /// store call, so that a cancelled RPC cannot release it while its
    /// physical delete still runs.
    fence: Arc<tokio::sync::RwLock<()>>,
    /// Deletes refused by the delete fence (ADR-0023).
    fence_refusals: AtomicU64,
    /// Rollovers to a raised segment format that failed (Phase 3.10).
    format_rollover_failures: AtomicU64,
    frame_bytes: usize,
    stream_progress_timeout: Duration,
    stats: Arc<NodeStats>,
    /// One deep verification (`ScrubShards`/`VerifyShards`) at a time per
    /// node (RFC-008 §8.3).
    scrub_slot: tokio::sync::Semaphore,
    /// Incarnation and `format_write` learned from the controller.
    cluster: Mutex<crate::cluster::ClusterFacts>,
    /// The identity the cluster gave this disk (Phase 3.11, R-100).
    identity: Mutex<crate::identity::NodeIdentity>,
    /// Retired records and group roles (Phase 3.12, ADR-0028).
    deferred: Arc<Mutex<crate::reclaim::Deferred>>,
    /// See [`NodeConfig::reclaim_skew`].
    reclaim_skew_ms: u64,
    /// See [`NodeConfig::fence_lease`].
    fence_lease: Duration,
    /// TLS state; `Some` makes every RPC require an authorised identity.
    tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    /// Background compaction counters.
    compaction: Arc<crate::compactor::CompactionStats>,
    /// Advertised capacity cap.
    capacity_bytes: Option<u64>,
    /// Capacity admission of shard writes (Phase 3.6).
    admission: crate::capacity::Admission,
    /// The store's capacity domain (Phase 3.7), read at start.
    domain: Option<crate::domain::Domain>,
    /// Payload bytes stored by write class (client, repair, migration,
    /// cache; Phase 3.8 capacity breakdown).
    class_bytes: [AtomicU64; 4],
    /// Side file bytes as last measured (health and compaction passes).
    side_bytes: AtomicU64,
    /// Load signals for adaptive background work (ADR-0018).
    load: crate::load::LoadSampler,
}

/// Decrements a gauge when dropped (`PutShard` streams in flight).
struct InFlightGuard<'a>(&'a std::sync::atomic::AtomicU64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Where a `PutShard` spends its time on the node (R-48 PUT profiling).
#[derive(Debug, Default)]
pub struct NodeStats {
    /// From the header frame to the last data frame.
    pub put_recv: Histogram,
    /// From the last frame until the blocking store write starts.
    pub put_queue: Histogram,
    /// The store write (append + group commit).
    pub put_store: Histogram,
    /// `GetShard` streams: from request to the last frame handed over.
    pub get_serve: Histogram,
    /// `PutShard` streams currently open.
    pub puts_in_flight: std::sync::atomic::AtomicU64,
    /// `GetShard` streams currently open.
    pub gets_in_flight: std::sync::atomic::AtomicU64,
    /// Reads (`GetShard`, `VerifyShards`, `ScrubShards`) that found a
    /// corrupt record (RFC-008 §8.2).
    pub corrupt_reads: std::sync::atomic::AtomicU64,
    /// Shards verified by `ScrubShards`.
    pub scrub_shards: std::sync::atomic::AtomicU64,
    /// Bytes read by `ScrubShards`.
    pub scrub_bytes: std::sync::atomic::AtomicU64,
    /// Shards `ScrubShards` found corrupt (and quarantined).
    pub scrub_corrupt: std::sync::atomic::AtomicU64,
}

/// A copy of [`NodeStats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeStatsSnapshot {
    /// See [`NodeStats::put_recv`].
    pub put_recv: HistogramSnapshot,
    /// See [`NodeStats::put_queue`].
    pub put_queue: HistogramSnapshot,
    /// See [`NodeStats::put_store`].
    pub put_store: HistogramSnapshot,
    /// See [`NodeStats::get_serve`].
    pub get_serve: HistogramSnapshot,
    /// See [`NodeStats::puts_in_flight`].
    pub puts_in_flight: u64,
    /// See [`NodeStats::corrupt_reads`].
    pub corrupt_reads: u64,
    /// See [`NodeStats::scrub_shards`].
    pub scrub_shards: u64,
    /// See [`NodeStats::scrub_bytes`].
    pub scrub_bytes: u64,
    /// See [`NodeStats::scrub_corrupt`].
    pub scrub_corrupt: u64,
    /// Quarantined shards on the node.
    pub quarantined_shards: u64,
    /// Payload bytes of quarantined shards.
    pub quarantined_bytes: u64,
}

impl StorageNodeService {
    /// Open the store and the epoch table.
    ///
    /// # Errors
    ///
    /// Storage or I/O errors while opening.
    pub fn open(cfg: &NodeConfig) -> Result<Self, StorageError> {
        // Before the store opens (and creates its first segment): a missing
        // facts file is a new node only while no segment exists.
        let cluster = crate::cluster::ClusterFacts::open(&cfg.store.dir);
        let identity = crate::identity::NodeIdentity::open(&cfg.store.dir);
        let mut deferred = crate::reclaim::Deferred::open(
            &cfg.store.dir,
            u64::try_from(cfg.reclaim_retention.as_millis()).unwrap_or(u64::MAX),
        );
        let (store, report) = Store::open(cfg.store.clone())?;
        // Retirements of records written again since (a crash between the
        // write and its supersede) end.
        deferred
            .prune(|k| store.locate(k).map(|l| l.sequence))
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        deferred.measure(|k| store.locate(k).map(|l| l.payload_len));
        if !report.quarantined.is_empty()
            || !report.holes.is_empty()
            || !report.corrupt_segments.is_empty()
        {
            tracing::warn!(
                quarantined = report.quarantined.len(),
                holes = report.holes.len(),
                corrupt_segments = report.corrupt_segments.len(),
                "storage open found damaged records (see quarantine.log)"
            );
        }
        if report.sequence_bound_damaged {
            tracing::warn!(
                "sequence.bound was damaged and ignored; the store took a new epoch as at every open"
            );
        }
        let total = device_space(&cfg.store.dir)
            .map_or(0, |(_, t)| cfg.capacity_bytes.map_or(t, |cap| t.min(cap)));
        let reserve = cfg.reserve_bytes.map_or_else(
            || crate::capacity::Reserve::default_for(total, cfg.store.segment_max_bytes),
            |r| crate::capacity::Reserve::new(r, cfg.store.segment_max_bytes),
        );
        let epochs = EpochTable::open(&cfg.store.dir);
        if cluster.format_write > 0 {
            store.set_format_write(cluster.format_write);
            // A rollover the raise did not finish before a restart.
            store
                .roll_to_format_write()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        tracing::info!(
            format_write = store.format_write(),
            format_max = arca_lake_storage::format::SEGMENT_FORMAT_VERSION,
            protocol = pb::PROTOCOL_VERSION,
            incarnation = cluster
                .incarnation
                .map(|i| crate::cluster::hex(&i))
                .unwrap_or_default(),
            "storage node capabilities"
        );
        Ok(Self {
            store: Arc::new(store),
            epochs: Mutex::new(epochs),
            fence: Arc::new(tokio::sync::RwLock::new(())),
            fence_refusals: AtomicU64::new(0),
            format_rollover_failures: AtomicU64::new(0),
            frame_bytes: cfg.frame_bytes,
            stream_progress_timeout: cfg.stream_progress_timeout,
            stats: Arc::default(),
            scrub_slot: tokio::sync::Semaphore::new(1),
            cluster: Mutex::new(cluster),
            identity: Mutex::new(identity),
            deferred: Arc::new(Mutex::new(deferred)),
            reclaim_skew_ms: u64::try_from(cfg.reclaim_skew.as_millis()).unwrap_or(u64::MAX),
            fence_lease: cfg.fence_lease,
            tls: cfg.tls.clone(),
            compaction: Arc::default(),
            capacity_bytes: cfg.capacity_bytes,
            admission: crate::capacity::Admission::new(reserve),
            domain: crate::domain::of(&cfg.store.dir),
            class_bytes: Default::default(),
            side_bytes: AtomicU64::new(0),
            load: crate::load::LoadSampler::new(&cfg.store.dir),
        })
    }

    /// Newest segment format on the device (`--check-downgrade`).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn newest_segment_format(&self) -> Result<u16, StorageError> {
        self.store.newest_segment_format()
    }

    /// TLS counters, when the node serves mutual TLS.
    #[must_use]
    pub fn tls_stats(&self) -> Option<Arc<arca_lake_protocol::tls::TlsStats>> {
        self.tls.as_ref().map(|t| Arc::clone(&t.stats))
    }

    /// The incarnation this node adopted, if any.
    #[must_use]
    pub fn incarnation(&self) -> Option<[u8; 16]> {
        self.cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .incarnation
    }

    /// Deferred reclamation (Phase 3.14 metrics): retention in ms, records
    /// retired and not reclaimed, whether the state is damaged (nothing is
    /// reclaimed), and their payload bytes (retained: on the device, not
    /// free).
    #[must_use]
    pub fn reclaim_state(&self) -> (u64, u64, bool, u64) {
        let d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
        (
            d.retention_ms(),
            d.len() as u64,
            d.damaged(),
            d.retained_bytes(),
        )
    }

    /// Timing counters of this node.
    #[must_use]
    pub fn stats(&self) -> NodeStatsSnapshot {
        NodeStatsSnapshot {
            put_recv: self.stats.put_recv.snapshot(),
            put_queue: self.stats.put_queue.snapshot(),
            put_store: self.stats.put_store.snapshot(),
            get_serve: self.stats.get_serve.snapshot(),
            puts_in_flight: self
                .stats
                .puts_in_flight
                .load(std::sync::atomic::Ordering::Relaxed),
            corrupt_reads: self.stats.corrupt_reads.load(Ordering::Relaxed),
            scrub_shards: self.stats.scrub_shards.load(Ordering::Relaxed),
            scrub_bytes: self.stats.scrub_bytes.load(Ordering::Relaxed),
            scrub_corrupt: self.stats.scrub_corrupt.load(Ordering::Relaxed),
            quarantined_shards: self.store.quarantine_stats().0 as u64,
            quarantined_bytes: self.store.quarantine_stats().1,
        }
    }

    /// `(free, total)` bytes the node advertises for placement: the file
    /// system's, capped by `capacity_bytes`.
    #[must_use]
    pub fn device_space(&self) -> Option<(u64, u64)> {
        let segment_bytes = self.store.stats().map_or(0, |s| s.segment_bytes);
        self.device_space_with(segment_bytes)
    }

    fn device_space_with(&self, segment_bytes: u64) -> Option<(u64, u64)> {
        // The allotment covers every file of the store, side files too.
        let used = segment_bytes.saturating_add(self.side_bytes.load(Ordering::Relaxed));
        device_space(self.store.dir()).map(|(free, total)| match self.capacity_bytes {
            Some(cap) => (free.min(cap.saturating_sub(used)), total.min(cap)),
            None => (free, total),
        })
    }

    /// The delete reserve now (Phase 3.8): tombstones for every record
    /// the store holds (live and quarantined).
    #[must_use]
    pub fn delete_reserve(&self) -> u64 {
        let s = self.store.index_sizes();
        crate::capacity::delete_reserve(s.live + s.quarantined, self.store.tombstone_bytes())
    }

    /// Where the device's bytes went (Phase 3.8).
    #[must_use]
    pub fn capacity_breakdown(&self) -> CapacityBreakdown {
        let side = self.store.side_file_bytes();
        self.side_bytes.store(side, Ordering::Relaxed);
        let fs = device_fs_used(self.store.dir());
        let segments = self.store.stats().map_or(0, |s| s.segment_bytes);
        CapacityBreakdown {
            written: self.store.written(),
            by_class: std::array::from_fn(|i| self.class_bytes[i].load(Ordering::Relaxed)),
            segment_bytes: segments,
            side_file_bytes: side,
            fs_used_bytes: fs,
            delete_reserve_bytes: self.delete_reserve(),
        }
    }

    /// Capacity of the node's device (Phase 3.6): space, what the store
    /// holds of it, the reserve and the writes admitted in flight.
    ///
    /// # Errors
    ///
    /// Store statistics errors.
    pub fn device_capacity(&self) -> Result<pb::DeviceCapacity, StorageError> {
        let stats = self.store.stats()?;
        Ok(self.device_capacity_with(&stats))
    }

    fn device_capacity_with(&self, stats: &arca_lake_storage::StoreStats) -> pb::DeviceCapacity {
        self.side_bytes
            .store(self.store.side_file_bytes(), Ordering::Relaxed);
        let space = self.device_space_with(stats.segment_bytes);
        let reserve = self.admission.reserve();
        let delete = self.delete_reserve();
        pb::DeviceCapacity {
            total_bytes: space.map_or(0, |s| s.1),
            available_bytes: space.map_or(0, |s| s.0),
            allocated_bytes: stats.segment_bytes,
            live_bytes: stats.live_bytes,
            reclaimable_bytes: stats.segment_bytes.saturating_sub(stats.live_bytes),
            quarantined_bytes: self.store.quarantine_stats().1,
            reserved_bytes: reserve.reserved.saturating_add(delete),
            inflight_bytes: self.admission.inflight(),
            repair_floor_bytes: reserve.repair_floor.saturating_add(delete),
            delete_reserve_bytes: delete,
            side_file_bytes: self.side_bytes.load(Ordering::Relaxed),
            sampled_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            domain_id: self
                .domain
                .as_ref()
                .map(|d| d.id.clone())
                .unwrap_or_default(),
            domain_certain: self.domain.as_ref().is_some_and(|d| d.certain),
            allotted: self.capacity_bytes.is_some(),
            fs_total_bytes: device_space(self.store.dir()).map_or(0, |(_, t)| t),
        }
    }

    /// Writes admitted and refused by class (client, repair, migration,
    /// cache).
    #[must_use]
    pub fn admission_counts(&self) -> (u64, [u64; 4]) {
        self.admission.counts()
    }

    /// Background compaction counters.
    #[must_use]
    pub fn compaction_stats(&self) -> &Arc<crate::compactor::CompactionStats> {
        &self.compaction
    }

    /// Direct access to the underlying store (tests and tooling).
    #[must_use]
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Per-RPC authorization (ADR-0017) before any argument is read.
    fn authorize<T>(
        &self,
        request: &tonic::Request<T>,
        rpc: arca_lake_protocol::tls::NodeRpc,
    ) -> Result<(), tonic::Status> {
        let peer = request
            .extensions()
            .get::<arca_lake_protocol::tls::PeerInfo>();
        let r = arca_lake_protocol::tls::authorize(peer, rpc, self.tls.is_some());
        if r.is_err()
            && let Some(t) = &self.tls
        {
            t.stats.rpcs_denied.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                ?rpc,
                peer = peer
                    .and_then(|p| p.identity.as_ref())
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                "RPC refused by the authorization table"
            );
        }
        r
    }

    fn check_version(v: u32) -> Result<(), tonic::Status> {
        if (pb::PROTOCOL_VERSION_MIN..=pb::PROTOCOL_VERSION).contains(&v) {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(format!(
                "protocol version {v} not supported; this node speaks [{}, {}]",
                pb::PROTOCOL_VERSION_MIN,
                pb::PROTOCOL_VERSION
            )))
        }
    }

    /// Refuse a delete not under this node's open delete fence (ADR-0023):
    /// the generation must be the one adopted and still open; a request
    /// below the fenced protocol version, without a generation or with
    /// another one is refused, and so is every delete before a fence was
    /// ever adopted (fail closed).
    fn check_delete_fence(
        &self,
        protocol_version: u32,
        generation: u64,
    ) -> Result<(), tonic::Status> {
        let c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
        // Once the cluster finalized a release with the rejoin fence, only
        // such releases delete (an earlier one may run again on restored
        // metadata, which its start check alone would admit).
        // Also once a controller of this release has led the node (it may
        // have missed the floor: absent or decommissioned at the finalize).
        let min_version = if c.schema_floor >= pb::REJOIN_FENCE_SCHEMA || c.rejoin_led {
            pb::REJOIN_FENCE_VERSION
        } else {
            pb::FENCED_DELETE_VERSION
        };
        if protocol_version >= min_version
            && c.fence_admits(generation)
            && c.lease_live(std::time::Instant::now())
        {
            return Ok(());
        }
        self.fence_refusals.fetch_add(1, Ordering::Relaxed);
        Err(tonic::Status::failed_precondition(format!(
            "delete fence: node holds generation {} ({}), request carries {generation} at protocol {protocol_version}",
            c.fence_generation,
            if c.fence_open { "open" } else { "closed" }
        )))
    }

    /// Rollovers to a raised segment format that failed so far.
    #[must_use]
    pub fn format_rollover_failures(&self) -> u64 {
        self.format_rollover_failures.load(Ordering::Relaxed)
    }

    /// Deletes refused by the delete fence so far.
    #[must_use]
    pub fn fence_refusals(&self) -> u64 {
        self.fence_refusals.load(Ordering::Relaxed)
    }

    /// Whether the delete fence is under an operator hold.
    #[must_use]
    pub fn delete_fence_held(&self) -> bool {
        self.cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fence_hold
            .is_some()
    }

    /// The incarnation the operator hold is tied to (`None`: no hold).
    pub fn delete_fence_hold_incarnation(&self) -> Option<[u8; 16]> {
        self.cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fence_hold
    }

    /// The delete fence held: generation and whether open.
    #[must_use]
    pub fn delete_fence(&self) -> (u64, bool) {
        let c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
        (c.fence_generation, c.fence_open)
    }

    /// Refuse a delete whose incarnation differs from the one this node
    /// adopted (RFC-015 §5.3): a controller that predates a metadata
    /// restore must not apply its decisions. An empty incarnation is
    /// accepted only until the node adopts one (Phase 3.5 audit SEC-5: a
    /// caller could otherwise skip the fence by omitting it).
    fn check_incarnation(&self, incarnation: &[u8]) -> Result<(), tonic::Status> {
        let held = self
            .cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .incarnation;
        if incarnation.is_empty() {
            return match held {
                Some(h) => Err(tonic::Status::failed_precondition(format!(
                    "incarnation rejected: node adopted {}, request carries none",
                    crate::cluster::hex(&h)
                ))),
                None => Ok(()),
            };
        }
        match held {
            Some(h) if h.as_slice() != incarnation => {
                Err(tonic::Status::failed_precondition(format!(
                    "incarnation rejected: node adopted {}, request carries {}",
                    crate::cluster::hex(&h),
                    crate::cluster::hex(incarnation)
                )))
            }
            _ => Ok(()),
        }
    }

    /// Refuse a write whose incarnation differs from the one this node
    /// adopted (R-117): the writer belongs to another cluster (it reached
    /// this node at an address it still maps to one of its own). Unlike a
    /// delete, an empty incarnation is accepted: a writer may hold none
    /// (tools, a store before its cluster state).
    fn check_write_incarnation(&self, incarnation: &[u8]) -> Result<(), tonic::Status> {
        if incarnation.is_empty() {
            return Ok(());
        }
        self.check_incarnation(incarnation)
    }

    fn check_epoch(&self, group: u64, epoch: u64) -> Result<(), tonic::Status> {
        let known = self
            .epochs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(group);
        if epoch < known {
            return Err(tonic::Status::failed_precondition(format!(
                "epoch rejected: group {group} is at epoch {known}, request carries {epoch}"
            )));
        }
        Ok(())
    }

    fn key(k: Option<pb::ShardKey>) -> Result<ShardKey, tonic::Status> {
        let k = k.ok_or_else(|| tonic::Status::invalid_argument("missing shard key"))?;
        let chunk_id: [u8; 16] = k
            .chunk_id
            .as_ref()
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("chunk_id must be 16 bytes"))?;
        let shard_index = u16::try_from(k.shard_index)
            .map_err(|_| tonic::Status::invalid_argument("shard_index too large"))?;
        Ok(ShardKey {
            group_id: k.group_id,
            chunk_id,
            shard_index,
        })
    }

    fn pb_key(k: &ShardKey) -> pb::ShardKey {
        pb::ShardKey {
            group_id: k.group_id,
            chunk_id: Bytes::copy_from_slice(&k.chunk_id),
            shard_index: u32::from(k.shard_index),
        }
    }

    /// Page size: the request's, clamped; 0 selects the default.
    fn page_limit(requested: u32) -> usize {
        const DEFAULT: usize = 1024;
        const MAX: usize = 16_384;
        match usize::try_from(requested).unwrap_or(MAX) {
            0 => DEFAULT,
            n => n.min(MAX),
        }
    }

    /// Status for a key that `get` reports missing: quarantined keys are
    /// `DATA_LOSS` so that readers learn the difference from a real miss.
    fn missing_status(&self, key: &ShardKey) -> tonic::Status {
        match self.store.quarantine_entry(key) {
            Some(q) => tonic::Status::data_loss(format!("shard quarantined: {}", q.reason)),
            None => tonic::Status::not_found("shard not found"),
        }
    }

    fn storage_status(e: StorageError) -> tonic::Status {
        match e {
            StorageError::NotFound => tonic::Status::not_found("shard not found"),
            StorageError::Corrupt { .. } => tonic::Status::data_loss(e.to_string()),
            StorageError::InvalidArgument(_) => tonic::Status::invalid_argument(e.to_string()),
            // The device or a quota is full (Phase 3.6): a capacity refusal,
            // not an internal error, so that the writer places elsewhere.
            StorageError::Io(ref io)
                if matches!(
                    io.kind(),
                    std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
                ) =>
            {
                tonic::Status::resource_exhausted(format!("insufficient capacity: {e}"))
            }
            other => tonic::Status::internal(other.to_string()),
        }
    }
}

#[tonic::async_trait]
#[allow(clippy::too_many_lines)]
impl StorageNode for StorageNodeService {
    type GetShardStream = ReceiverStream<Result<pb::GetShardResponse, tonic::Status>>;

    async fn put_shard(
        &self,
        request: tonic::Request<tonic::Streaming<pb::PutShardRequest>>,
    ) -> Result<tonic::Response<pb::PutShardResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::PutShard)?;
        let mut stream = request.into_inner();
        let started = Instant::now();
        self.stats
            .puts_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _in_flight = InFlightGuard(&self.stats.puts_in_flight);
        let header = match stream.next().await {
            Some(Ok(pb::PutShardRequest {
                body: Some(pb::put_shard_request::Body::Header(h)),
            })) => h,
            Some(Err(e)) => return Err(e),
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "first message must be the header",
                ));
            }
        };
        Self::check_version(header.protocol_version)?;
        self.check_write_incarnation(&header.incarnation)?;
        let key = Self::key(header.key)?;
        self.check_epoch(key.group_id, header.group_epoch)?;
        let expected = usize::try_from(header.payload_len)
            .map_err(|_| tonic::Status::invalid_argument("payload too large"))?;
        // Capacity admission (Phase 3.6): decided before the body is taken,
        // on free space sampled under the admission lock, held until the
        // write ends.
        let class = pb::WriteClass::try_from(header.write_class).unwrap_or(pb::WriteClass::Client);
        let _held = self
            .admission
            .admit_sampled(
                || {
                    self.device_space()
                        .map(|(free, _)| (free, self.delete_reserve()))
                },
                class,
                // The record's own future tombstone is held with it (the
                // delete reserve counts it once the record is indexed).
                header
                    .payload_len
                    .saturating_add(self.store.tombstone_bytes()),
            )
            .map_err(|r| tonic::Status::resource_exhausted(r.to_string()))?;
        let mut payload = Vec::with_capacity(expected);
        loop {
            let next = tokio::time::timeout(self.stream_progress_timeout, stream.next()).await;
            let Ok(msg) = next else {
                return Err(tonic::Status::aborted(format!(
                    "client sent no frame for {:?} ({} of {expected} bytes received)",
                    self.stream_progress_timeout,
                    payload.len()
                )));
            };
            let Some(msg) = msg else { break };
            match msg?.body {
                Some(pb::put_shard_request::Body::Data(d)) => {
                    if payload.len() + d.len() > expected {
                        return Err(tonic::Status::invalid_argument(
                            "payload longer than declared",
                        ));
                    }
                    payload.extend_from_slice(&d);
                }
                Some(pb::put_shard_request::Body::Header(_)) => {
                    return Err(tonic::Status::invalid_argument("duplicate header"));
                }
                None => {}
            }
        }
        if payload.len() != expected {
            return Err(tonic::Status::invalid_argument(format!(
                "payload shorter than declared: {} of {expected} bytes",
                payload.len()
            )));
        }
        // Re-check the epoch and the incarnation after receiving the body: a
        // membership change, or an incarnation adopted, may have landed while
        // the data was in flight.
        self.check_epoch(key.group_id, header.group_epoch)?;
        self.check_write_incarnation(&header.incarnation)?;
        self.stats.put_recv.observe(started.elapsed());
        let crc = crc32c_of(&payload);
        // The group's role, noted before the record exists (independent
        // review): a durable write makes its group durable first, so that
        // no delete of the new record can see the group as cache; if that
        // cannot be recorded, the write is refused.
        {
            let cache = class == pb::WriteClass::Cache;
            let (noted, syncer) = {
                let mut d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
                (d.note_write(key.group_id, cache), d.syncer())
            };
            // A role change is durable before the record exists (rare: the
            // group's first write of a class), waited for off the runtime
            // and off the lock (Phase 3.13, group commit).
            let noted = match noted {
                Ok(Some(t)) => tokio::task::spawn_blocking(move || syncer.wait(t))
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))?,
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = noted {
                // Whatever the retention (re-review: one lowered to 0 may be
                // raised again, and the role must have survived).
                if !cache {
                    return Err(tonic::Status::unavailable(format!(
                        "pending-reclaim log not writable: {e}"
                    )));
                }
                // A cache write not noted leaves the group unknown, which
                // is durable: nothing is deleted at once that should not be.
                tracing::warn!(error = %e, "pending-reclaim log: cache role not noted");
            }
        }
        let store = Arc::clone(&self.store);
        let queued = Instant::now();
        let (queue_wait, store_time, result) = tokio::task::spawn_blocking(move || {
            let queue_wait = queued.elapsed();
            let t = Instant::now();
            let r = store.put_sequenced(key, &payload);
            (queue_wait, t.elapsed(), r)
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        self.stats.put_queue.observe(queue_wait);
        self.stats.put_store.observe(store_time);
        let sequence = result.map_err(Self::storage_status)?;
        {
            // The new record is live: a retirement of the key ends
            // (ADR-0028). A failure here leaves a retirement of an older
            // sequence, which is never reclaimed (the newest record differs).
            let mut d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
            if let Err(e) = d.supersede(&key) {
                tracing::warn!(error = %e, "pending-reclaim log not updated after a write");
            }
        }
        self.class_bytes[class as usize].fetch_add(header.payload_len, Ordering::Relaxed);
        Ok(tonic::Response::new(pb::PutShardResponse {
            crc32c: crc,
            bytes_received: header.payload_len,
            sequence,
            store_epoch: self.store.store_epoch(),
        }))
    }

    async fn get_shard(
        &self,
        request: tonic::Request<pb::GetShardRequest>,
    ) -> Result<tonic::Response<Self::GetShardStream>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::GetShard)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let started = std::time::Instant::now();
        self.stats.gets_in_flight.fetch_add(1, Ordering::Relaxed);
        let in_flight = InFlightGuard(&self.stats.gets_in_flight);
        let key = Self::key(req.key)?;
        let store = Arc::clone(&self.store);
        let range = if req.len == 0 {
            None
        } else {
            Some(req.offset..req.offset + req.len)
        };
        let data = tokio::task::spawn_blocking(move || store.get(&key, range))
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .map_err(|e| match e {
                StorageError::NotFound => self.missing_status(&key),
                StorageError::Corrupt { .. } => {
                    self.stats.corrupt_reads.fetch_add(1, Ordering::Relaxed);
                    Self::storage_status(e)
                }
                other => Self::storage_status(other),
            })?;
        let data = if req.len == 0 && req.offset > 0 {
            let off = usize::try_from(req.offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            Bytes::from(data).slice(off..)
        } else {
            Bytes::from(data)
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let frame = self.frame_bytes;
        let stats = Arc::clone(&self.stats);
        tokio::spawn(async move {
            let mut off = 0;
            while off < data.len() {
                let end = (off + frame).min(data.len());
                if tx
                    .send(Ok(pb::GetShardResponse {
                        data: data.slice(off..end),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                off = end;
            }
            // From request to the last frame handed over (the load signal
            // of adaptive scrubbing, ADR-0018).
            stats.get_serve.observe(started.elapsed());
        });
        drop(in_flight);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn delete_shard(
        &self,
        request: tonic::Request<pb::DeleteShardRequest>,
    ) -> Result<tonic::Response<pb::DeleteShardResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::DeleteShard)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let key = Self::key(req.key)?;
        // Held from the checks to the end of the store's delete, moved into
        // the blocking task: a fence, incarnation or epoch change waits for
        // every delete that passed the old one (SEC-6, ADR-0023).
        let fence = Arc::clone(&self.fence).read_owned().await;
        self.check_incarnation(&req.incarnation)?;
        self.check_delete_fence(req.protocol_version, req.delete_fence)?;
        self.check_epoch(key.group_id, req.group_epoch)?;
        // A delete for another node's identity never runs here (Phase 3.11,
        // R-100): another disk at the address it was decided for. A node
        // holding an identity takes only deletes carrying it (an unstamped
        // one could have been decided for another disk at this address); a
        // damaged identity file takes none.
        {
            let (held, damaged) = {
                let id = self.identity.lock().unwrap_or_else(PoisonError::into_inner);
                (id.held(), id.damaged())
            };
            let carried = req.node_identity.len() == 16;
            let refused = damaged
                || match held {
                    Some(h) => {
                        !carried || h.node_id != req.node_id || h.token[..] != req.node_identity[..]
                    }
                    None => carried,
                };
            if refused {
                return Err(tonic::Status::failed_precondition(
                    "identity rejected: the delete was not decided for this node's disk",
                ));
            }
        }
        let store = Arc::clone(&self.store);
        let if_sequence = req.if_sequence;
        // A token of another store epoch names a sequence this store may
        // have handed out again since (Phase 3.5 audit CON-16): refused.
        if if_sequence != 0 && req.if_store_epoch != store.store_epoch() {
            return Err(tonic::Status::aborted(
                "delete token of another store epoch",
            ));
        }
        let deferred = Arc::clone(&self.deferred);
        let incarnation = incarnation_of(&req.incarnation);
        let (deleted, was_deferred) = tokio::task::spawn_blocking(move || {
            let _fence = fence;
            let mut d = deferred.lock().unwrap_or_else(PoisonError::into_inner);
            if d.defers(key.group_id) || d.protects(&key) {
                if let Some(newest) = store.locate(&key).map(|l| l.sequence) {
                    // Retired, not removed (ADR-0028): the record stays
                    // readable until a reclaim proves it unneeded long
                    // enough.
                    if if_sequence != 0 && newest != if_sequence {
                        return Ok((false, true));
                    }
                    let t = d
                        .retire(key, newest, now_unix_ms(), incarnation)
                        .map_err(StorageError::Io)?;
                    if let Some(l) = store.locate(&key) {
                        d.note_bytes(&key, l.payload_len);
                    }
                    let syncer = d.syncer();
                    drop(d);
                    // Acknowledged once durable, waited for off the lock:
                    // concurrent deletes share one sync (Phase 3.13).
                    syncer.wait_opt(t).map_err(StorageError::Io)?;
                    return Ok((true, true));
                }
                // No live record. A quarantined one (a corrupt copy is not
                // protected data) is removed at once, so that its entry is
                // released (Phase 3.12 review), but only that record: the
                // tombstone is conditioned on its sequence, so a write that
                // lands meanwhile is never deleted (re-review).
                let Some(quarantined) = store.quarantine_entry(&key).map(|q| q.location.sequence)
                else {
                    return Ok((true, false));
                };
                if if_sequence != 0 && if_sequence != quarantined {
                    return Ok((false, false));
                }
                return store.delete_if(key, quarantined).map(|r| (r, false));
            }
            drop(d);
            if if_sequence == 0 {
                store.delete(key).map(|()| (true, false))
            } else {
                store.delete_if(key, if_sequence).map(|r| (r, false))
            }
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?
        .map_err(Self::storage_status)?;
        if !deleted {
            return Err(tonic::Status::aborted(
                "shard changed since the delete was decided",
            ));
        }
        Ok(tonic::Response::new(pb::DeleteShardResponse {
            deferred: was_deferred,
        }))
    }

    async fn reclaim_shard(
        &self,
        request: tonic::Request<pb::ReclaimShardRequest>,
    ) -> Result<tonic::Response<pb::ReclaimShardResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::ReclaimShard)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let key = Self::key(req.key)?;
        // The checks of a delete (ADR-0023/24/27), under the fence guard.
        let fence = Arc::clone(&self.fence).read_owned().await;
        self.check_incarnation(&req.incarnation)?;
        self.check_delete_fence(req.protocol_version, req.delete_fence)?;
        self.check_epoch(key.group_id, req.group_epoch)?;
        {
            let (held, damaged) = {
                let id = self.identity.lock().unwrap_or_else(PoisonError::into_inner);
                (id.held(), id.damaged())
            };
            let carried = req.node_identity.len() == 16;
            let refused = damaged
                || match held {
                    Some(h) => {
                        !carried || h.node_id != req.node_id || h.token[..] != req.node_identity[..]
                    }
                    None => carried,
                };
            if refused {
                return Err(tonic::Status::failed_precondition(
                    "identity rejected: the reclaim was not decided for this node's disk",
                ));
            }
        }
        if req.store_epoch != self.store.store_epoch() {
            return Err(tonic::Status::aborted("reclaim of another store epoch"));
        }
        let store = Arc::clone(&self.store);
        let deferred = Arc::clone(&self.deferred);
        let skew = self.reclaim_skew_ms;
        let (sequence, observed) = (req.sequence, req.observed_at_ms);
        let incarnation = incarnation_of(&req.incarnation);
        let keep = req.keep;
        let compact = Arc::clone(&self.deferred);
        let reclaimed_bytes = Arc::new(Mutex::new(0u64));
        let reported = Arc::clone(&reclaimed_bytes);
        let outcome = tokio::task::spawn_blocking(move || {
            let _fence = fence;
            let mut d = deferred.lock().unwrap_or_else(PoisonError::into_inner);
            if keep {
                // Needed: the retirement of exactly this sequence ends, the
                // record stays (nothing else is checked: keeping is safe).
                let t = d.kept(&key, sequence).map_err(StorageError::Io)?;
                let syncer = d.syncer();
                drop(d);
                syncer.wait_opt(t).map_err(StorageError::Io)?;
                return Ok(Ok(()));
            }
            let newest = store.locate(&key).map(|l| l.sequence);
            // The record is gone already (a reclaim whose log append
            // failed, or a delete of an earlier release since): its
            // retirement ends, nothing else changes.
            if newest.is_none() && d.retired(&key).is_some_and(|r| r.sequence == sequence) {
                d.reclaimed(&key, sequence).map_err(StorageError::Io)?;
                return Ok(Ok(()));
            }
            match d.reclaim_check(
                &key,
                sequence,
                newest,
                incarnation,
                observed,
                now_unix_ms(),
                skew,
            ) {
                crate::reclaim::ReclaimCheck::Refused(why) => Ok(Err(why)),
                crate::reclaim::ReclaimCheck::Allowed => {
                    let bytes = store.locate(&key).map_or(0, |l| u64::from(l.payload_len));
                    let removed = store.delete_if(key, sequence)?;
                    d.reclaimed(&key, sequence).map_err(StorageError::Io)?;
                    Ok(if removed {
                        *reclaimed_bytes
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner) = bytes;
                        Ok(())
                    } else {
                        Err("the record changed".to_string())
                    })
                }
            }
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?
        .map_err(Self::storage_status)?;
        crate::reclaim::maybe_compact_in_background(&compact);
        match outcome {
            Ok(()) => Ok(tonic::Response::new(pb::ReclaimShardResponse {
                payload_bytes: *reported.lock().unwrap_or_else(PoisonError::into_inner),
            })),
            Err(why) => Err(tonic::Status::failed_precondition(format!(
                "reclaim refused: {why}"
            ))),
        }
    }

    async fn list_pending(
        &self,
        request: tonic::Request<pb::ListPendingRequest>,
    ) -> Result<tonic::Response<pb::ListPendingResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::ListPending)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let after = req.after.map(|k| Self::key(Some(k))).transpose()?;
        let limit = Self::page_limit(req.limit);
        let d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
        let (page, next) = if req.due_only {
            // Bounded per call (Phase 3.13): the lock is held for at most
            // this many records.
            let (page, next) = d.page_due(after, limit, 100_000, now_unix_ms());
            (page, next.map(|k| Self::pb_key(&k)))
        } else {
            let page = d.page(after, limit);
            let next = (page.len() == limit)
                .then(|| page.last().map(|(k, _)| Self::pb_key(k)))
                .flatten();
            (page, next)
        };
        let store_epoch = self.store.store_epoch();
        Ok(tonic::Response::new(pb::ListPendingResponse {
            shards: page
                .iter()
                .map(|(k, r)| pb::PendingShard {
                    key: Some(Self::pb_key(k)),
                    sequence: r.sequence,
                    retired_at_ms: r.retired_ms,
                    store_epoch,
                })
                .collect(),
            next,
            retention_ms: d.listed_retention_ms(),
        }))
    }

    async fn list_shards(
        &self,
        request: tonic::Request<pb::ListShardsRequest>,
    ) -> Result<tonic::Response<pb::ListShardsResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::ListShards)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let after = req.after.map(|k| Self::key(Some(k))).transpose()?;
        let limit = Self::page_limit(req.limit);
        // Live and quarantined keys from one index snapshot (OPS-6), merged
        // into one ordered, bounded page whose cursor is the last key of the
        // merge (Phase 3.5 audit OPS-5: the cursor followed the live keys
        // only, and quarantined keys beyond one page were never listed).
        let (live, quarantined) = if req.include_quarantined {
            self.store
                .listing_in_group_after(req.group_id, after, limit)
        } else {
            (
                self.store
                    .entries_in_group_after_sequenced(req.group_id, after, limit),
                Vec::new(),
            )
        };
        let mut more = live.len() == limit || quarantined.len() == limit;
        let mut page: Vec<(ShardKey, u32, bool, u64)> = live
            .into_iter()
            .map(|(k, len, seq)| (k, len, false, seq))
            .collect();
        if req.include_quarantined {
            page.extend(
                quarantined
                    .into_iter()
                    .map(|(k, e)| (k, e.location.payload_len, true, e.location.sequence)),
            );
            page.sort_by_key(|a| a.0);
            if page.len() > limit {
                page.truncate(limit);
                more = true;
            }
        }
        let next = more
            .then(|| page.last().map(|(k, _, _, _)| Self::pb_key(k)))
            .flatten();
        let store_epoch = self.store.store_epoch();
        let held = self
            .cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .incarnation;
        let d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
        let entries = page
            .into_iter()
            .map(|(k, len, quarantined, sequence)| pb::ShardEntry {
                key: Some(Self::pb_key(&k)),
                payload_len: u64::from(len),
                quarantined,
                sequence,
                store_epoch,
                retired: d
                    .retired(&k)
                    .is_some_and(|r| r.sequence == sequence && Some(r.incarnation) == held),
                retired_any: d.retired(&k).is_some_and(|r| r.sequence == sequence),
            })
            .collect();
        drop(d);
        Ok(tonic::Response::new(pb::ListShardsResponse {
            entries,
            next,
            more,
        }))
    }

    async fn verify_shards(
        &self,
        request: tonic::Request<pb::VerifyShardsRequest>,
    ) -> Result<tonic::Response<pb::VerifyShardsResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::VerifyShards)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let after = req.after.map(|k| Self::key(Some(k))).transpose()?;
        let limit = Self::page_limit(req.limit);
        // Read before the listing: every record below it is listed (or
        // gone) on this page's window.
        let indexed_below = self.store.indexed_below();
        let store_epoch = self.store.store_epoch();
        let page = self
            .store
            .entries_in_group_after(req.group_id, after, limit);
        let more = page.len() == limit;
        let next = more
            .then(|| page.last().map(|(k, _)| Self::pb_key(k)))
            .flatten();
        // Differential verification (Phase 3.14): records below the
        // caller's watermark of this store epoch were verified before (a
        // record written again has a newer sequence); not read again.
        let since_applied = req.since_sequence > 0 && req.since_store_epoch == store_epoch;
        let page: Vec<_> = if since_applied {
            page.into_iter()
                .filter(|(k, _)| {
                    self.store
                        .locate(k)
                        .is_none_or(|l| l.sequence >= req.since_sequence)
                })
                .collect()
        } else {
            page
        };
        let store = Arc::clone(&self.store);
        let read_only = req.read_only;
        let _slot = self
            .scrub_slot
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let (verified, corrupt) = tokio::task::spawn_blocking(move || {
            let mut corrupt = 0u64;
            let read = |key: &ShardKey| {
                if read_only {
                    store.get_no_quarantine(key, None)
                } else {
                    store.get(key, None)
                }
            };
            let out = page
                .into_iter()
                .map(|(key, len)| match read(&key) {
                    Ok(data) => pb::ShardVerification {
                        key: Some(Self::pb_key(&key)),
                        payload_len: data.len() as u64,
                        crc32c: crc32c_of(&data),
                        ok: true,
                    },
                    Err(e) => {
                        if matches!(e, StorageError::Corrupt { .. }) {
                            corrupt += 1;
                        }
                        pb::ShardVerification {
                            key: Some(Self::pb_key(&key)),
                            payload_len: u64::from(len),
                            crc32c: 0,
                            ok: false,
                        }
                    }
                })
                .collect::<Vec<_>>();
            (out, corrupt)
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        self.stats
            .corrupt_reads
            .fetch_add(corrupt, Ordering::Relaxed);
        Ok(tonic::Response::new(pb::VerifyShardsResponse {
            shards: verified,
            next,
            more,
            store_epoch,
            indexed_below,
            since_applied,
        }))
    }

    async fn scrub_shards(
        &self,
        request: tonic::Request<pb::ScrubShardsRequest>,
    ) -> Result<tonic::Response<pb::ScrubShardsResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::ScrubShards)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let after = req.after.map(|k| Self::key(Some(k))).transpose()?;
        let limit = Self::page_limit(req.limit);
        let max_bytes = if req.max_bytes == 0 {
            u64::MAX
        } else {
            req.max_bytes
        };
        // Live entries and quarantined entries of the window, in key order:
        // a scrub reports quarantined shards as such instead of hiding them.
        let mut keys: Vec<(ShardKey, u32, Option<arca_lake_storage::QuarantineEntry>)> = self
            .store
            .entries_in_group_after(req.group_id, after, limit)
            .into_iter()
            .map(|(k, len)| (k, len, None))
            .collect();
        let page_full = keys.len() == limit;
        let bound = keys.last().map(|(k, _, _)| *k);
        for (k, e) in self
            .store
            .quarantined_in_group_after(req.group_id, after, limit)
        {
            if page_full && bound.is_some_and(|b| k > b) {
                break;
            }
            keys.push((k, e.location.payload_len, Some(e)));
        }
        keys.sort_by_key(|a| a.0);
        let store = Arc::clone(&self.store);
        let _slot = self
            .scrub_slot
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let (shards, bytes_read, corrupt, stopped_early) = tokio::task::spawn_blocking(move || {
            let mut bytes = 0u64;
            let mut corrupt = 0u64;
            let mut out = Vec::with_capacity(keys.len());
            let mut stopped_early = false;
            let total = keys.len();
            for (n, (key, len, q)) in keys.into_iter().enumerate() {
                let pb_key = Some(Self::pb_key(&key));
                if let Some(q) = q {
                    out.push(pb::ShardScrub {
                        key: pb_key,
                        payload_len: u64::from(len),
                        crc32c: 0,
                        sequence: q.location.sequence,
                        status: pb::ShardStatus::Quarantined as i32,
                        reason: q.reason.to_string(),
                    });
                } else {
                    out.push(match store.verify(&key) {
                        Ok(v) => {
                            bytes += u64::from(v.payload_len);
                            pb::ShardScrub {
                                key: pb_key,
                                payload_len: u64::from(v.payload_len),
                                crc32c: v.crc32c,
                                sequence: v.sequence,
                                status: pb::ShardStatus::Ok as i32,
                                reason: String::new(),
                            }
                        }
                        Err(StorageError::Corrupt { reason, .. }) => {
                            corrupt += 1;
                            let status = match reason {
                                "payload block crc mismatch" => pb::ShardStatus::ChecksumMismatch,
                                r if r.contains("truncated") => pb::ShardStatus::Truncated,
                                _ => pb::ShardStatus::InvalidHeader,
                            };
                            pb::ShardScrub {
                                key: pb_key,
                                payload_len: u64::from(len),
                                crc32c: 0,
                                sequence: 0,
                                status: status as i32,
                                reason: reason.to_string(),
                            }
                        }
                        Err(StorageError::NotFound) => pb::ShardScrub {
                            key: pb_key,
                            payload_len: u64::from(len),
                            crc32c: 0,
                            sequence: 0,
                            status: pb::ShardStatus::Quarantined as i32,
                            reason: "quarantined during scrub".into(),
                        },
                        Err(e) => pb::ShardScrub {
                            key: pb_key,
                            payload_len: u64::from(len),
                            crc32c: 0,
                            sequence: 0,
                            status: pb::ShardStatus::IoError as i32,
                            reason: e.to_string(),
                        },
                    });
                }
                if bytes >= max_bytes && n + 1 < total {
                    stopped_early = true;
                    break;
                }
            }
            (out, bytes, corrupt, stopped_early)
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let more = page_full || stopped_early;
        let next = more
            .then(|| shards.last().and_then(|s| s.key.clone()))
            .flatten();
        self.stats
            .scrub_shards
            .fetch_add(shards.len() as u64, Ordering::Relaxed);
        self.stats
            .scrub_bytes
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.stats
            .scrub_corrupt
            .fetch_add(corrupt, Ordering::Relaxed);
        self.stats
            .corrupt_reads
            .fetch_add(corrupt, Ordering::Relaxed);
        Ok(tonic::Response::new(pb::ScrubShardsResponse {
            shards,
            next,
            more,
            bytes_read,
        }))
    }

    async fn update_group_epoch(
        &self,
        request: tonic::Request<pb::UpdateGroupEpochRequest>,
    ) -> Result<tonic::Response<pb::UpdateGroupEpochResponse>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::UpdateGroupEpoch)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let _fence = Arc::clone(&self.fence).write_owned().await;
        let epoch = self
            .epochs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .raise(req.group_id, req.epoch)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(pb::UpdateGroupEpochResponse { epoch }))
    }

    async fn health(
        &self,
        request: tonic::Request<pb::HealthRequest>,
    ) -> Result<tonic::Response<pb::HealthResponse>, tonic::Status> {
        // Anything that changes the node's facts, the delete fence
        // included (Phase 3.9 review), needs the controller or admin role.
        let facts = {
            let r = request.get_ref();
            r.format_write > 0
                || r.schema_floor > 0
                || r.incarnation_seq > 0
                || !r.incarnation.is_empty()
                || r.delete_fence_generation > 0
                || r.delete_fence_open
                || r.delete_fence_hold
                || r.delete_fence_release
                || r.delete_fence_release_token != 0
                // Adopting an identity changes the node's facts too (Phase
                // 3.13, R-105).
                || r.adopt_identity
        };
        self.authorize(
            &request,
            if facts {
                arca_lake_protocol::tls::NodeRpc::HealthWithFacts
            } else {
                arca_lake_protocol::tls::NodeRpc::Health
            },
        )?;
        let req = request.into_inner();
        // A request carrying another identity than this disk's changes
        // nothing here (Phase 3.11, R-100): another cluster's controller, a
        // reused node id, or a controller that registered another disk at
        // this address. It still gets the report, mismatch flagged.
        // The node's own certificate names its node id (Phase 3.13,
        // R-105): a request for another id is not this node's, whatever
        // its disk holds, and adopts nothing.
        let cert_node = self.tls.as_ref().and_then(|t| {
            let own = t.identity();
            (own.role == arca_lake_protocol::tls::Role::Node)
                .then(|| own.name.parse::<u64>().ok())
                .flatten()
        });
        let cert_conflict =
            req.node_identity.len() == 16 && cert_node.is_some_and(|c| c != req.node_id);
        let identity = if cert_conflict {
            tracing::warn!(
                node_id = req.node_id,
                certificate = ?cert_node,
                "a request carried an identity for another node id than this node's certificate: nothing adopted"
            );
            crate::identity::Check::Mismatch
        } else {
            self.identity
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .check(req.node_id, &req.node_identity, req.adopt_identity)
                .map_err(|e| tonic::Status::internal(e.to_string()))?
        };
        let identity_mismatch = identity == crate::identity::Check::Mismatch;
        if identity_mismatch {
            tracing::warn!(
                node_id = req.node_id,
                "a request carried another identity than this disk's: nothing adopted"
            );
        }
        // Adopt the cluster facts the controller carries (ADR-0016): the
        // incarnation only when not older than what is held, the format
        // monotonically; both persisted before they take effect. A change
        // of incarnation waits for deletes past the old fence (SEC-6).
        // A new incarnation or a fence change waits, under the exclusive
        // guard, for every delete past the old one; it is persisted before
        // the response acknowledges it (ADR-0023).
        let caller = <[u8; 16]>::try_from(req.incarnation.as_ref()).ok();
        // Only the node's own incarnation (after this request adopted the
        // caller's, if newer) counts: a hold is tied to it, and only a
        // caller of a later one the node adopted lifts it. A caller of an
        // older incarnation (a controller started on a backup from before
        // an earlier restore) can neither set nor lift a hold.
        // The same open generation from another opener: a close of it.
        let fence_args = |c: &crate::cluster::ClusterFacts| {
            (
                req.delete_fence_generation,
                req.delete_fence_open
                    && !c.opener_conflict(
                        req.delete_fence_generation,
                        true,
                        req.delete_fence_opener,
                    ),
                req.delete_fence_hold
                    .then(|| c.incarnation.unwrap_or([0; 16])),
                req.delete_fence_release,
                caller.filter(|inc| c.incarnation == Some(*inc)),
                req.delete_fence_release_token,
            )
        };
        let needs_exclusive = |c: &crate::cluster::ClusterFacts| {
            let new_incarnation = caller.is_some_and(|inc| c.incarnation != Some(inc));
            let (generation, open, hold, release, from, token) = fence_args(c);
            // A higher schema floor also waits for admitted deletes: once it
            // is acknowledged no previous-release delete is still running
            // (re-review).
            new_incarnation
                || req.schema_floor > c.schema_floor
                || (req.delete_fence_opener != 0 && !c.rejoin_led)
                || c.plan_fence(generation, open, hold, release, from, token)
                    .is_some()
        };
        // Decided again under the facts' lock: a change made by another
        // request between the check and the mutation takes the exclusive
        // guard on a second round instead of mutating without it (Phase 3.9
        // review).
        let mut exclusive =
            needs_exclusive(&self.cluster.lock().unwrap_or_else(PoisonError::into_inner));
        let _fence = if identity_mismatch {
            None
        } else {
            loop {
                let guard = if exclusive {
                    Some(Arc::clone(&self.fence).write_owned().await)
                } else {
                    None
                };
                let mut c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
                if guard.is_none() && needs_exclusive(&c) {
                    drop(c);
                    exclusive = true;
                    continue;
                }
                if let Ok(inc) = <[u8; 16]>::try_from(req.incarnation.as_ref())
                    && c.adopt_incarnation_seq(inc, req.incarnation_changed_ms, req.incarnation_seq)
                        .map_err(|e| tonic::Status::internal(e.to_string()))?
                {
                    tracing::info!(
                        incarnation = crate::cluster::hex(&inc),
                        "adopted cluster incarnation"
                    );
                }
                let (generation, open, hold, release, from, token) = fence_args(&c);
                let conflict = c.opener_conflict(
                    req.delete_fence_generation,
                    req.delete_fence_open,
                    req.delete_fence_opener,
                );
                if conflict {
                    tracing::warn!(
                        generation = req.delete_fence_generation,
                        "delete fence generation pushed open by another opener than the one that opened it: closed"
                    );
                }
                c.set_next_opener(req.delete_fence_opener);
                if req.delete_fence_opener != 0 {
                    c.note_rejoin_leader()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;
                }
                if c.adopt_fence_held(generation, open, hold, release, from, token)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?
                {
                    tracing::info!(
                        generation = c.fence_generation,
                        open = c.fence_open,
                        "adopted delete fence"
                    );
                }
                // The rejoin lease (Phase 3.10): a push of the open fence by a
                // caller of this node's incarnation (re)starts it; nothing else
                // does, so a node that started, was cut off, or stopped hearing
                // the leader refuses deletes until the leader reaches it.
                let lease = self.fence_lease;
                c.note_fence_push(generation, open, from, std::time::Instant::now() + lease);
                if req.schema_floor > 0 {
                    c.adopt_schema_floor(req.schema_floor)
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;
                }
                if req.format_write > 0
                    && c.adopt_format_write(req.format_write)
                        .map_err(|e| tonic::Status::internal(e.to_string()))?
                {
                    let v = self.store.set_format_write(req.format_write);
                    tracing::info!(format_write = v, "storage format write gate updated");
                }
                // At once, not at the next rollover, and again on every push
                // until it succeeded (re-review: a failed rollover was not
                // retried once the value was adopted): an earlier release
                // refuses the store from then on.
                // A failure is logged and counted, not returned: the node keeps
                // answering (re-review: failing Health made it look down), and
                // the finalize waits on its reported segment format.
                if req.format_write > 0
                    && let Err(e) = self.store.roll_to_format_write()
                {
                    self.format_rollover_failures
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "cannot roll over to the raised segment format");
                }
                break guard;
            }
        };
        // Adopted and persisted: deletes may go on while the response is
        // assembled (Phase 3.9 review).
        drop(_fence);
        let stats = self.store.stats().map_err(Self::storage_status)?;
        let (qn, qb) = self.store.quarantine_stats();
        let (fence_generation, fence_open) = self.delete_fence();
        // Read once, before the response is built (a second lock of the
        // facts inside one expression would wait on the first).
        let (unknown_token, lease_ms, incarnation_seq_held, schema_floor_held) = {
            let c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
            (
                c.unknown_token,
                u64::try_from(c.lease_left(std::time::Instant::now()).as_millis())
                    .unwrap_or(u64::MAX),
                c.incarnation_seq,
                c.schema_floor,
            )
        };
        let (incarnation, group_epochs) = {
            let c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
            let e = self.epochs.lock().unwrap_or_else(PoisonError::into_inner);
            (
                c.incarnation
                    .map(|i| Bytes::copy_from_slice(&i))
                    .unwrap_or_default(),
                e.all()
                    .into_iter()
                    .map(|(group_id, epoch)| pb::GroupEpoch { group_id, epoch })
                    .collect(),
            )
        };
        let held_identity = self
            .identity
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held();
        let space = self.device_space_with(stats.segment_bytes);
        let load = self.load.sample(
            self.stats.get_serve.snapshot(),
            self.stats.put_store.snapshot(),
        );
        // One lock, released before the response (a guard kept in the
        // expression below would deadlock the next lock).
        let (reclaim_retention_ms, reclaim_pending, reclaim_damaged, reclaim_retained_bytes) = {
            let d = self.deferred.lock().unwrap_or_else(PoisonError::into_inner);
            (
                d.retention_ms(),
                d.len() as u64,
                d.damaged(),
                d.retained_bytes(),
            )
        };
        Ok(tonic::Response::new(pb::HealthResponse {
            live_shards: stats.live_shards as u64,
            live_bytes: stats.live_bytes,
            segment_bytes: stats.segment_bytes,
            quarantined_shards: qn as u64,
            quarantined_bytes: qb,
            protocol_min: pb::PROTOCOL_VERSION_MIN,
            protocol_max: pb::PROTOCOL_VERSION,
            format_min: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION_MIN),
            format_max: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
            format_write: u64::from(self.store.format_write()),
            incarnation,
            software: arca_lake_common::compat::software_version(),
            group_epochs,
            version_bump: arca_lake_common::compat::BUMP,
            schema_max: pb::NODE_SCHEMA_MAX,
            puts_in_flight: self.stats.puts_in_flight.load(Ordering::Relaxed),
            gets_in_flight: self.stats.gets_in_flight.load(Ordering::Relaxed),
            device_free_bytes: space.map_or(0, |s| s.0),
            device_total_bytes: space.map_or(0, |s| s.1),
            verify_read_only: true,
            load_get_p99_us: load.get_p99_us,
            load_put_p99_us: load.put_p99_us,
            load_device: load.device.is_some(),
            load_device_util_permille: load.device.map_or(0, |d| d.util_permille),
            load_device_queue_depth_milli: load.device.map_or(0, |d| d.queue_depth_milli),
            load_device_read_await_us: load.device.map_or(0, |d| d.read_await_us),
            load_reported: true,
            delete_fence_generation: fence_generation,
            delete_fence_open: fence_open,
            delete_fence_hold: self.delete_fence_held(),
            reclaim_retention_ms,
            reclaim_pending,
            reclaim_damaged,
            reclaim_skew_ms: self.reclaim_skew_ms,
            reclaim_retained_bytes,
            delete_fence_hold_incarnation: self
                .delete_fence_hold_incarnation()
                .map(|h| Bytes::copy_from_slice(&h))
                .unwrap_or_default(),
            delete_fence_unknown_token: unknown_token,
            delete_fence_lease_ms: lease_ms,
            incarnation_seq: incarnation_seq_held,
            schema_floor: schema_floor_held,
            newest_segment_format: self.store.newest_segment_format().map_or(0, u64::from),
            identity_supported: true,
            identity_node_id: held_identity.map_or(0, |i| i.node_id),
            identity_token: held_identity
                .map(|i| Bytes::copy_from_slice(&i.token))
                .unwrap_or_default(),
            identity_mismatch,
            devices: vec![self.device_capacity_with(&stats)],
        }))
    }
}

/// Where a node's device bytes went (Phase 3.8): bytes written by kind
/// and class since start, and what is on the device now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CapacityBreakdown {
    /// Bytes the store wrote since it was opened, by kind.
    pub written: arca_lake_storage::WrittenBytes,
    /// Payload bytes stored by class (client, repair, migration, cache).
    pub by_class: [u64; 4],
    /// Segment files now.
    pub segment_bytes: u64,
    /// Other files of the store now.
    pub side_file_bytes: u64,
    /// Bytes used on the file system now (by everything on it).
    pub fs_used_bytes: u64,
    /// The delete reserve now.
    pub delete_reserve_bytes: u64,
}

impl CapacityBreakdown {
    /// File system bytes not in the store's files (file system metadata,
    /// journal, other users of the file system): meaningful when the store
    /// has the file system to itself.
    #[must_use]
    pub fn fs_overhead_bytes(&self) -> u64 {
        self.fs_used_bytes
            .saturating_sub(self.segment_bytes.saturating_add(self.side_file_bytes))
    }
}

/// Bytes used on the file system holding `dir` (blocks in use, including
/// those reserved for root).
fn device_fs_used(dir: &std::path::Path) -> u64 {
    let Ok(s) = rustix::fs::statvfs(dir) else {
        return 0;
    };
    let unit = if s.f_frsize > 0 {
        s.f_frsize
    } else {
        s.f_bsize
    };
    s.f_blocks.saturating_sub(s.f_bfree).saturating_mul(unit)
}

/// `(available, total)` bytes of the file system holding `dir`.
fn device_space(dir: &std::path::Path) -> Option<(u64, u64)> {
    let s = rustix::fs::statvfs(dir).ok()?;
    let unit = if s.f_frsize > 0 {
        s.f_frsize
    } else {
        s.f_bsize
    };
    Some((
        s.f_bavail.saturating_mul(unit),
        s.f_blocks.saturating_mul(unit),
    ))
}

fn crc32c_of(data: &[u8]) -> u32 {
    // The storage engine verifies per-block CRCs; this whole-shard CRC is
    // returned to the writer for an end-to-end check of the bytes received.
    arca_lake_storage::format::crc32c(data)
}

/// A running node.
#[derive(Debug)]
pub struct NodeHandle {
    /// Bound address.
    pub local_addr: SocketAddr,
    service: Arc<StorageNodeService>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    compactor_stop: tokio::sync::watch::Sender<bool>,
    compactor: JoinHandle<()>,
}

impl NodeHandle {
    /// The service (for direct store access in tests).
    #[must_use]
    pub fn service(&self) -> &Arc<StorageNodeService> {
        &self.service
    }

    /// Stop serving and wait for a compaction in progress to finish. The
    /// store stays open until the handle is dropped.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.compactor_stop.send(true);
        let _ = self.task.await;
        let _ = self.compactor.await;
    }
}

/// Open the store and serve the gRPC API.
///
/// # Errors
///
/// Storage errors while opening, or bind errors.
pub async fn serve(
    cfg: NodeConfig,
) -> Result<NodeHandle, Box<dyn std::error::Error + Send + Sync>> {
    let service = Arc::new(StorageNodeService::open(&cfg)?);
    #[cfg(feature = "fault-injection")]
    crate::fault_control::spawn(cfg.store.dir.clone());
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel::<()>();
    let svc = StorageNodeServer::from_arc(Arc::clone(&service)).max_decoding_message_size(64 << 20);
    // tonic applies `tcp_nodelay` only to listeners it binds itself; with a
    // caller-supplied listener every accepted socket keeps Nagle's algorithm,
    // and Nagle plus delayed ACK stalls each small frame that follows
    // unacknowledged data (trailers, window updates) for ~40 ms (RFC-009
    // §6.11). Set it per accepted connection.
    // Every accepted socket gets `tcp_nodelay` inside `incoming`, and with
    // TLS a completed mutual handshake carrying the peer identity.
    let incoming = arca_lake_protocol::tls::incoming(listener, cfg.tls.clone());
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .initial_stream_window_size(Some(arca_lake_protocol::client::STREAM_WINDOW))
            .initial_connection_window_size(Some(arca_lake_protocol::client::CONNECTION_WINDOW))
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await;
    });
    let (compactor_stop, stop_rx) = tokio::sync::watch::channel(false);
    let space = Arc::downgrade(&service);
    let compactor = tokio::spawn(crate::compactor::run(
        Arc::clone(service.store()),
        cfg.compaction,
        Arc::clone(service.compaction_stats()),
        Arc::new(move || {
            space
                .upgrade()
                .and_then(|s| s.device_space())
                .map(|(free, _)| free)
        }),
        stop_rx,
    ));
    Ok(NodeHandle {
        local_addr,
        service,
        shutdown: Some(tx),
        task,
        compactor_stop,
        compactor,
    })
}
