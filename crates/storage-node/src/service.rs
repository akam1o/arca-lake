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
        Ok(Self {
            store: Arc::new(store),
            epochs: Mutex::new(epochs),
            frame_bytes: cfg.frame_bytes,
            stream_progress_timeout: cfg.stream_progress_timeout,
            stats: NodeStats::default(),
            scrub_slot: tokio::sync::Semaphore::new(1),
        })
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

    /// Direct access to the underlying store (tests and tooling).
    #[must_use]
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    fn check_version(v: u32) -> Result<(), tonic::Status> {
        if v == pb::PROTOCOL_VERSION {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(format!(
                "protocol version {v} not supported; this node speaks {}",
                pb::PROTOCOL_VERSION
            )))
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
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
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
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn delete_shard(
        &self,
        request: tonic::Request<pb::DeleteShardRequest>,
    ) -> Result<tonic::Response<pb::DeleteShardResponse>, tonic::Status> {
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let key = Self::key(req.key)?;
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
        let _slot = self
            .scrub_slot
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let (verified, corrupt) = tokio::task::spawn_blocking(move || {
            let mut corrupt = 0u64;
            let out = page
                .into_iter()
                .map(|(key, len)| match store.get(&key, None) {
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
        let req = request.into_inner();
        Self::check_version(req.protocol_version)?;
        let after = req.after.map(|k| Self::key(Some(k))).transpose()?;
        let limit = Self::page_limit(req.limit);
        // Live entries and quarantined entries of the window, in key order:
        // a scrub reports quarantined shards as such instead of hiding them.
        let mut keys: Vec<(ShardKey, u32, Option<arca_lake_storage::QuarantineEntry>)> = self
            .store
            .entries_in_group_after(req.group_id, after, limit)
            .into_iter()
            .map(|(k, len)| (k, len, None))
            .collect();
        let more = keys.len() == limit;
        let bound = keys.last().map(|(k, _, _)| *k);
        for (k, e) in self
            .store
            .quarantined_in_group_after(req.group_id, after, limit)
        {
            if more && bound.is_some_and(|b| k > b) {
                break;
            }
            keys.push((k, e.location.payload_len, Some(e)));
        }
        keys.sort_by_key(|a| a.0);
        let next = more.then(|| bound.map(|k| Self::pb_key(&k))).flatten();
        let store = Arc::clone(&self.store);
        let _slot = self
            .scrub_slot
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let (shards, bytes_read, corrupt) = tokio::task::spawn_blocking(move || {
            let mut bytes = 0u64;
            let mut corrupt = 0u64;
            let out = keys
                .into_iter()
                .map(|(key, len, q)| {
                    let pb_key = Some(Self::pb_key(&key));
                    if let Some(q) = q {
                        return pb::ShardScrub {
                            key: pb_key,
                            payload_len: u64::from(len),
                            crc32c: 0,
                            sequence: q.location.sequence,
                            status: pb::ShardStatus::Quarantined as i32,
                            reason: q.reason.to_string(),
                        };
                    }
                    match store.verify(&key) {
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
                    }
                })
                .collect::<Vec<_>>();
            (out, bytes, corrupt)
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
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
        _request: tonic::Request<pb::HealthRequest>,
    ) -> Result<tonic::Response<pb::HealthResponse>, tonic::Status> {
        let stats = self.store.stats().map_err(Self::storage_status)?;
        let (qn, qb) = self.store.quarantine_stats();
        Ok(tonic::Response::new(pb::HealthResponse {
            live_shards: stats.live_shards as u64,
            live_bytes: stats.live_bytes,
            segment_bytes: stats.segment_bytes,
            quarantined_shards: qn as u64,
            quarantined_bytes: qb,
        }))
    }
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
}

impl NodeHandle {
    /// The service (for direct store access in tests).
    #[must_use]
    pub fn service(&self) -> &Arc<StorageNodeService> {
        &self.service
    }

    /// Stop serving. The store stays open until the handle is dropped.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
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
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel::<()>();
    let svc = StorageNodeServer::from_arc(Arc::clone(&service)).max_decoding_message_size(64 << 20);
    // tonic applies `tcp_nodelay` only to listeners it binds itself; with a
    // caller-supplied listener every accepted socket keeps Nagle's algorithm,
    // and Nagle plus delayed ACK stalls each small frame that follows
    // unacknowledged data (trailers, window updates) for ~40 ms (RFC-009
    // §6.11). Set it per accepted connection.
    let incoming = tokio_stream::StreamExt::map(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        |conn| {
            conn.inspect(|sock| {
                let _ = sock.set_nodelay(true);
            })
        },
    );
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
    Ok(NodeHandle {
        local_addr,
        service,
        shutdown: Some(tx),
        task,
    })
}
