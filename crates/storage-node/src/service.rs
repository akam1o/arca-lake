//! gRPC service implementation.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
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
        }
    }
}

/// The service state shared by all requests.
#[derive(Debug)]
pub struct StorageNodeService {
    store: Arc<Store>,
    epochs: Mutex<EpochTable>,
    frame_bytes: usize,
    stream_progress_timeout: Duration,
    stats: NodeStats,
    /// One deep verification (`ScrubShards`/`VerifyShards`) at a time per
    /// node (RFC-008 §8.3).
    scrub_slot: tokio::sync::Semaphore,
    /// Incarnation and `format_write` learned from the controller.
    cluster: Mutex<crate::cluster::ClusterFacts>,
    /// TLS state; `Some` makes every RPC require an authorised identity.
    tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    /// Background compaction counters.
    compaction: Arc<crate::compactor::CompactionStats>,
    /// Advertised capacity cap.
    capacity_bytes: Option<u64>,
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
        let (store, report) = Store::open(cfg.store.clone())?;
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
        let epochs = EpochTable::open(&cfg.store.dir);
        let cluster = crate::cluster::ClusterFacts::open(&cfg.store.dir);
        if cluster.format_write > 0 {
            store.set_format_write(cluster.format_write);
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
            frame_bytes: cfg.frame_bytes,
            stream_progress_timeout: cfg.stream_progress_timeout,
            stats: NodeStats::default(),
            scrub_slot: tokio::sync::Semaphore::new(1),
            cluster: Mutex::new(cluster),
            tls: cfg.tls.clone(),
            compaction: Arc::default(),
            capacity_bytes: cfg.capacity_bytes,
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
        device_space(self.store.dir()).map(|(free, total)| match self.capacity_bytes {
            Some(cap) => (free.min(cap.saturating_sub(segment_bytes)), total.min(cap)),
            None => (free, total),
        })
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

    /// Refuse a delete whose incarnation differs from the one this node
    /// adopted (RFC-015 §5.3): a controller that predates a metadata
    /// restore must not apply its decisions. An empty incarnation (a
    /// caller that predates incarnations) is accepted.
    fn check_incarnation(&self, incarnation: &[u8]) -> Result<(), tonic::Status> {
        if incarnation.is_empty() {
            return Ok(());
        }
        let held = self
            .cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .incarnation;
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
        let key = Self::key(header.key)?;
        self.check_epoch(key.group_id, header.group_epoch)?;
        let expected = usize::try_from(header.payload_len)
            .map_err(|_| tonic::Status::invalid_argument("payload too large"))?;
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
        // Re-check the epoch after receiving the body: a membership change
        // may have landed while the data was in flight.
        self.check_epoch(key.group_id, header.group_epoch)?;
        self.stats.put_recv.observe(started.elapsed());
        let crc = crc32c_of(&payload);
        let store = Arc::clone(&self.store);
        let queued = Instant::now();
        let (queue_wait, store_time, result) = tokio::task::spawn_blocking(move || {
            let queue_wait = queued.elapsed();
            let t = Instant::now();
            let r = store.put(key, &payload);
            (queue_wait, t.elapsed(), r)
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        self.stats.put_queue.observe(queue_wait);
        self.stats.put_store.observe(store_time);
        result.map_err(Self::storage_status)?;
        Ok(tonic::Response::new(pb::PutShardResponse {
            crc32c: crc,
            bytes_received: header.payload_len,
        }))
    }

    async fn get_shard(
        &self,
        request: tonic::Request<pb::GetShardRequest>,
    ) -> Result<tonic::Response<Self::GetShardStream>, tonic::Status> {
        self.authorize(&request, arca_lake_protocol::tls::NodeRpc::GetShard)?;
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
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
        self.check_incarnation(&req.incarnation)?;
        self.check_epoch(key.group_id, req.group_epoch)?;
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.delete(key))
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .map_err(Self::storage_status)?;
        Ok(tonic::Response::new(pb::DeleteShardResponse {}))
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
        let mut page: Vec<(ShardKey, u32, bool)> = self
            .store
            .entries_in_group_after(req.group_id, after, limit)
            .into_iter()
            .map(|(k, len)| (k, len, false))
            .collect();
        let more = page.len() == limit;
        if req.include_quarantined {
            // Quarantined keys of the same window, merged in key order; the
            // cursor stays the last live key so that paging is unchanged.
            let bound = page.last().map(|(k, _, _)| *k);
            for (k, e) in self
                .store
                .quarantined_in_group_after(req.group_id, after, limit)
            {
                if more && bound.is_some_and(|b| k > b) {
                    break;
                }
                page.push((k, e.location.payload_len, true));
            }
            page.sort_by_key(|a| a.0);
        }
        let next = more
            .then(|| page.last().map(|(k, _, _)| Self::pb_key(k)))
            .flatten();
        let entries = page
            .into_iter()
            .map(|(k, len, quarantined)| pb::ShardEntry {
                key: Some(Self::pb_key(&k)),
                payload_len: u64::from(len),
                quarantined,
            })
            .collect();
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
        let page = self
            .store
            .entries_in_group_after(req.group_id, after, limit);
        let more = page.len() == limit;
        let next = more
            .then(|| page.last().map(|(k, _)| Self::pb_key(k)))
            .flatten();
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
        let facts = {
            let r = request.get_ref();
            r.format_write > 0 || !r.incarnation.is_empty()
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
        // Adopt the cluster facts the controller carries (ADR-0016): the
        // incarnation only when not older than what is held, the format
        // monotonically; both persisted before they take effect.
        {
            let mut c = self.cluster.lock().unwrap_or_else(PoisonError::into_inner);
            if let Ok(inc) = <[u8; 16]>::try_from(req.incarnation.as_ref())
                && c.adopt_incarnation(inc, req.incarnation_changed_ms)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?
            {
                tracing::info!(
                    incarnation = crate::cluster::hex(&inc),
                    "adopted cluster incarnation"
                );
            }
            if req.format_write > 0
                && c.adopt_format_write(req.format_write)
                    .map_err(|e| tonic::Status::internal(e.to_string()))?
            {
                let v = self.store.set_format_write(req.format_write);
                tracing::info!(format_write = v, "storage format write gate updated");
            }
        }
        let stats = self.store.stats().map_err(Self::storage_status)?;
        let (qn, qb) = self.store.quarantine_stats();
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
        let space = self.device_space_with(stats.segment_bytes);
        let load = self.load.sample(
            self.stats.get_serve.snapshot(),
            self.stats.put_store.snapshot(),
        );
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
        }))
    }
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
    let compactor = tokio::spawn(crate::compactor::run(
        Arc::clone(service.store()),
        cfg.compaction,
        Arc::clone(service.compaction_stats()),
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
