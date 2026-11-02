//! Convenience client for the storage node API: lazily connected channels
//! per address, one method per RPC, and an optional fault-injection hook
//! for tests (timeouts, lost replies, resets, partitions, slow nodes,
//! delayed acknowledgments, duplicated requests, reordered completion).
//!
//! The hook acts at the client call boundary, not on the wire: a "lost
//! reply" performs the RPC and then reports a failure, a "partition" fails
//! before sending. It reproduces the observable effects on the protocols
//! above it; behaviour on a real network (TCP resets mid-stream, HTTP/2
//! stream errors, kernel buffering) is not covered and needs its own tests.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

use crate::storage::v1 as pb;
use crate::storage::v1::storage_node_client::StorageNodeClient;

/// Shard identity on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRef {
    /// Stripe group.
    pub group_id: u64,
    /// Chunk ID.
    pub chunk_id: [u8; 16],
    /// Shard index.
    pub shard_index: u16,
}

/// One page of a shard listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardPage {
    /// `(shard, payload length)` of live shards, in key order.
    pub entries: Vec<(ShardRef, u64)>,
    /// Quarantined shards of the page (only when requested, RFC-008 §8.2),
    /// with their payload lengths.
    pub quarantined: Vec<(ShardRef, u64)>,
    /// Cursor for the next page, if any.
    pub next: Option<ShardRef>,
}

/// Classification of one shard by the node's deep verification
/// (RFC-008 §8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardStatus {
    /// Header and every payload block verified.
    Ok,
    /// A payload block CRC failed; the node quarantined the shard.
    ChecksumMismatch,
    /// The record header did not validate; quarantined.
    InvalidHeader,
    /// The record is shorter than its header claims; quarantined.
    Truncated,
    /// Already quarantined (by an earlier read, scrub or open).
    Quarantined,
    /// A transient I/O error; nothing was quarantined.
    IoError,
}

impl ShardStatus {
    fn from_pb(v: i32) -> Self {
        match pb::ShardStatus::try_from(v) {
            Ok(pb::ShardStatus::Ok) => Self::Ok,
            Ok(pb::ShardStatus::ChecksumMismatch) => Self::ChecksumMismatch,
            Ok(pb::ShardStatus::InvalidHeader) => Self::InvalidHeader,
            Ok(pb::ShardStatus::Truncated) => Self::Truncated,
            Ok(pb::ShardStatus::Quarantined) => Self::Quarantined,
            _ => Self::IoError,
        }
    }

    /// Whether the shard may be used as a read or reconstruction source.
    #[must_use]
    pub fn is_good(self) -> bool {
        self == Self::Ok
    }
}

/// One scrubbed shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardScrub {
    /// Shard.
    pub shard: ShardRef,
    /// Payload length (the index's when not OK).
    pub payload_len: u64,
    /// CRC-32C of the payload when OK.
    pub crc32c: u32,
    /// Record sequence when OK or quarantined.
    pub sequence: u64,
    /// Classification.
    pub status: ShardStatus,
    /// Node-side reason for a non-OK status.
    pub reason: String,
}

/// One page of scrubbed shards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubPage {
    /// Shards in key order (live and quarantined).
    pub shards: Vec<ShardScrub>,
    /// Cursor for the next page, if any.
    pub next: Option<ShardRef>,
    /// Payload bytes the node read for this page.
    pub bytes_read: u64,
}

/// One verified shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardVerification {
    /// Shard.
    pub shard: ShardRef,
    /// Payload length.
    pub payload_len: u64,
    /// CRC-32C of the payload when `ok`.
    pub crc32c: u32,
    /// Whether the stored bytes verified.
    pub ok: bool,
}

/// One page of shard verifications.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyPage {
    /// Verifications in key order.
    pub shards: Vec<ShardVerification>,
    /// Cursor for the next page, if any.
    pub next: Option<ShardRef>,
}

impl ShardRef {
    fn from_pb(k: &pb::ShardKey) -> Option<Self> {
        Some(Self {
            group_id: k.group_id,
            chunk_id: k.chunk_id.as_ref().try_into().ok()?,
            shard_index: u16::try_from(k.shard_index).ok()?,
        })
    }

    fn pb(&self) -> pb::ShardKey {
        pb::ShardKey {
            group_id: self.group_id,
            chunk_id: Bytes::copy_from_slice(&self.chunk_id),
            shard_index: u32::from(self.shard_index),
        }
    }
}

/// Which RPC a fault hook is consulted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rpc {
    /// `PutShard`.
    PutShard,
    /// `GetShard`.
    GetShard,
    /// `DeleteShard`.
    DeleteShard,
    /// `ListShards`.
    ListShards,
    /// `VerifyShards`.
    VerifyShards,
    /// `ScrubShards`.
    ScrubShards,
    /// `UpdateGroupEpoch`.
    UpdateEpoch,
    /// `Health`.
    Health,
}

/// What a fault hook sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcContext<'a> {
    /// Node address.
    pub addr: &'a str,
    /// RPC.
    pub rpc: Rpc,
    /// Shard, for shard RPCs.
    pub shard: Option<ShardRef>,
    /// Group, for group RPCs.
    pub group_id: Option<u64>,
}

/// What a fault hook asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultAction {
    /// Proceed normally.
    None,
    /// Wait before sending (slow node, reordered completion).
    DelayBefore(Duration),
    /// Fail without sending (partition, request timeout, connection refused).
    FailBefore(tonic::Code, String),
    /// Send and let the node apply the request, then fail (lost reply).
    FailAfter(tonic::Code, String),
    /// Send, then wait before returning the reply (delayed acknowledgment).
    DelayAfter(Duration),
    /// Send the request twice and return the second reply (duplicate).
    Duplicate,
    /// Fail without sending and drop the cached channel (connection reset).
    Reset,
    /// Shard streams only: send the header, then never another frame (a
    /// node or link that stopped without closing the connection).
    StallStream,
    /// Shard streams only: delay every frame by this much (a slow node or
    /// link that still makes progress).
    SlowStream(Duration),
}

/// A fault hook.
pub type FaultHook = Arc<dyn Fn(&RpcContext<'_>) -> FaultAction + Send + Sync>;

/// HTTP/2 initial stream window for shard transfers.
pub const STREAM_WINDOW: u32 = 8 << 20;
/// HTTP/2 initial connection window for shard transfers.
pub const CONNECTION_WINDOW: u32 = 32 << 20;

/// Deadlines and retries of node RPCs (RFC-009 §7, R-47).
///
/// Shard streams are not bounded by one per-RPC timeout. They fail when
/// no frame has moved for `progress_timeout` (a stalled stream) or when
/// the *absolute deadline* `base_deadline + bytes / min_rate` passes (a
/// transfer that progresses but too slowly). Unary RPCs use
/// `unary_timeout`. Shard RPCs whose failure is a transport error are
/// retried up to `max_attempts` times with exponential backoff from
/// `backoff`, after dropping the cached channel; errors that the node
/// decided (epoch fence, not found, corrupt, invalid) are never retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcPolicy {
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Timeout of unary RPCs (delete, list, verify, epoch, health).
    pub unary_timeout: Duration,
    /// Maximum time without a frame moving on a shard stream.
    pub progress_timeout: Duration,
    /// Absolute deadline floor of a shard stream.
    pub base_deadline: Duration,
    /// Bytes per second added to the absolute deadline (0 = none).
    pub min_rate_bytes_per_s: u64,
    /// Attempts per shard RPC (1 = no retry).
    pub max_attempts: u32,
    /// First backoff; doubles per attempt, capped at 16× the first.
    pub backoff: Duration,
}

impl Default for RpcPolicy {
    fn default() -> Self {
        Self::from_timeout(Duration::from_secs(10))
    }
}

impl RpcPolicy {
    /// A policy derived from the former single per-RPC timeout: unary
    /// RPCs, the progress timeout and the deadline floor all take
    /// `timeout`; the minimum rate is 256 KiB/s; shard RPCs are tried
    /// three times. (The R-47 matrix in RFC-009 §6.8 chose these: a
    /// progress timeout shorter than TCP's retransmission backoff under
    /// heavy loss restarts transfers that would have finished, and a
    /// deadline without a rate term fails every transfer on a slow link.)
    #[must_use]
    pub fn from_timeout(timeout: Duration) -> Self {
        Self {
            connect_timeout: (timeout / 5).max(Duration::from_millis(200)),
            unary_timeout: timeout,
            progress_timeout: timeout.max(Duration::from_millis(100)),
            base_deadline: timeout,
            min_rate_bytes_per_s: 256 << 10,
            max_attempts: 3,
            backoff: Duration::from_millis(100),
        }
    }

    /// Absolute deadline of a stream carrying `bytes`.
    #[must_use]
    pub fn deadline_for(&self, bytes: u64) -> Duration {
        if self.min_rate_bytes_per_s == 0 {
            return self.base_deadline;
        }
        let millis = bytes.saturating_mul(1000) / self.min_rate_bytes_per_s;
        self.base_deadline + Duration::from_millis(millis)
    }

    fn backoff_for(&self, attempt: u32) -> Duration {
        let factor = 1u32 << attempt.min(4);
        self.backoff * factor
    }
}

/// Counters of the RPC policy, shared by every clone of a pool.
#[derive(Debug, Default)]
pub struct RpcStats {
    /// Shard streams aborted because no frame moved for `progress_timeout`.
    pub stalled: AtomicU64,
    /// Shard streams aborted at the absolute deadline.
    pub deadline_exceeded: AtomicU64,
    /// Unary RPCs that hit `unary_timeout`.
    pub unary_timeouts: AtomicU64,
    /// Shard RPC attempts retried after a transport error.
    pub retries: AtomicU64,
    /// Shard RPCs that failed after the last attempt.
    pub exhausted: AtomicU64,
}

/// A copy of [`RpcStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RpcStatsSnapshot {
    /// See [`RpcStats::stalled`].
    pub stalled: u64,
    /// See [`RpcStats::deadline_exceeded`].
    pub deadline_exceeded: u64,
    /// See [`RpcStats::unary_timeouts`].
    pub unary_timeouts: u64,
    /// See [`RpcStats::retries`].
    pub retries: u64,
    /// See [`RpcStats::exhausted`].
    pub exhausted: u64,
}

/// How a fault hook shapes a shard stream (see [`FaultAction`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamFault {
    None,
    /// Frames stop after the header and never resume.
    Stall,
    /// Each frame is delayed by this much.
    Slow(Duration),
}

/// Concatenate frames into one buffer (one copy; the frame form avoids it).
#[must_use]
pub fn concat_frames(frames: Vec<Bytes>) -> Bytes {
    match frames.len() {
        0 => Bytes::new(),
        1 => frames.into_iter().next().unwrap_or_default(),
        _ => {
            let total: usize = frames.iter().map(Bytes::len).sum();
            let mut out = bytes::BytesMut::with_capacity(total);
            for f in &frames {
                out.extend_from_slice(f);
            }
            out.freeze()
        }
    }
}

/// Total length of frames.
#[must_use]
pub fn frames_len(frames: &[Bytes]) -> usize {
    frames.iter().map(Bytes::len).sum()
}

/// Shared pool of channels keyed by node address.
#[derive(Clone, Default)]
pub struct NodeClients {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    policy: RpcPolicy,
    stats: Arc<RpcStats>,
    hook: Arc<Mutex<Option<FaultHook>>>,
    /// `GetShard` RPCs in flight per node address (durable-read priority
    /// for cache populations).
    reads_in_flight: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
}

impl std::fmt::Debug for NodeClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClients")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl NodeClients {
    /// Pool with [`RpcPolicy::from_timeout`].
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self::with_policy(RpcPolicy::from_timeout(timeout))
    }

    /// Pool with an explicit policy.
    #[must_use]
    pub fn with_policy(policy: RpcPolicy) -> Self {
        Self {
            channels: Arc::default(),
            policy,
            stats: Arc::default(),
            hook: Arc::default(),
            reads_in_flight: Arc::default(),
        }
    }

    /// `GetShard` RPCs currently in flight to `addr` from this pool.
    #[must_use]
    pub fn reads_in_flight(&self, addr: &str) -> u64 {
        self.reads_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(addr)
            .map_or(0, |c| c.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// `GetShard` RPCs currently in flight to any node from this pool.
    #[must_use]
    pub fn reads_in_flight_total(&self) -> u64 {
        self.reads_in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    fn read_counter(&self, addr: &str) -> Arc<AtomicU64> {
        Arc::clone(
            self.reads_in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(addr.to_string())
                .or_default(),
        )
    }

    /// The policy in force.
    #[must_use]
    pub fn policy(&self) -> &RpcPolicy {
        &self.policy
    }

    /// Counters since the pool was created.
    #[must_use]
    pub fn stats(&self) -> RpcStatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        RpcStatsSnapshot {
            stalled: self.stats.stalled.load(Relaxed),
            deadline_exceeded: self.stats.deadline_exceeded.load(Relaxed),
            unary_timeouts: self.stats.unary_timeouts.load(Relaxed),
            retries: self.stats.retries.load(Relaxed),
            exhausted: self.stats.exhausted.load(Relaxed),
        }
    }

    /// Install (or remove) the fault hook. Shared by every clone.
    pub fn set_fault_hook(&self, hook: Option<FaultHook>) {
        *self.hook.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    fn action(&self, ctx: &RpcContext<'_>) -> FaultAction {
        let hook = self
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        hook.map_or(FaultAction::None, |h| h(ctx))
    }

    /// Run `call` under the fault hook's decision. The hook is consulted
    /// exactly once per call; stream-shaping actions are passed to `call`.
    async fn with_faults<T, Fut>(
        &self,
        ctx: RpcContext<'_>,
        call: impl Fn(StreamFault) -> Fut,
    ) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        match self.action(&ctx) {
            FaultAction::None => call(StreamFault::None).await,
            FaultAction::StallStream => call(StreamFault::Stall).await,
            FaultAction::SlowStream(d) => call(StreamFault::Slow(d)).await,
            FaultAction::DelayBefore(d) => {
                tokio::time::sleep(d).await;
                call(StreamFault::None).await
            }
            FaultAction::FailBefore(code, msg) => Err(tonic::Status::new(code, msg)),
            FaultAction::FailAfter(code, msg) => {
                let _ = call(StreamFault::None).await;
                Err(tonic::Status::new(code, msg))
            }
            FaultAction::DelayAfter(d) => {
                let r = call(StreamFault::None).await;
                tokio::time::sleep(d).await;
                r
            }
            FaultAction::Duplicate => {
                let _ = call(StreamFault::None).await;
                call(StreamFault::None).await
            }
            FaultAction::Reset => {
                self.forget(ctx.addr);
                Err(tonic::Status::unavailable("connection reset"))
            }
        }
    }

    /// Whether a failure is worth another attempt: transport-level errors,
    /// timeouts and cancellations, never a decision the node made.
    #[must_use]
    pub fn is_retryable(status: &tonic::Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::Aborted
        ) && !Self::is_epoch_rejected(status)
    }

    /// Run a shard RPC under the fault hook with the policy's retries.
    async fn with_retry<T, Fut>(
        &self,
        ctx: RpcContext<'_>,
        call: impl Fn(StreamFault) -> Fut,
    ) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        use std::sync::atomic::Ordering::Relaxed;
        let attempts = self.policy.max_attempts.max(1);
        let mut attempt = 0u32;
        loop {
            match self.with_faults(ctx, &call).await {
                Ok(v) => return Ok(v),
                Err(status) if attempt + 1 < attempts && Self::is_retryable(&status) => {
                    self.forget(ctx.addr);
                    self.stats.retries.fetch_add(1, Relaxed);
                    tokio::time::sleep(self.policy.backoff_for(attempt)).await;
                    attempt += 1;
                }
                Err(status) => {
                    if attempts > 1 && Self::is_retryable(&status) {
                        self.stats.exhausted.fetch_add(1, Relaxed);
                    }
                    return Err(status);
                }
            }
        }
    }

    /// Bound a unary RPC by `unary_timeout`.
    async fn unary<T, Fut>(&self, fut: Fut) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        if let Ok(r) = tokio::time::timeout(self.policy.unary_timeout, fut).await {
            return r;
        }
        self.stats
            .unary_timeouts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(tonic::Status::deadline_exceeded(format!(
            "rpc timeout after {:?}",
            self.policy.unary_timeout
        )))
    }

    /// Run a shard stream under the progress watchdog and the absolute
    /// deadline. `progress` is bumped (to a monotonic tick) by whoever
    /// moves frames; dropping `fut` cancels the HTTP/2 stream.
    async fn guard_stream<T, Fut>(
        &self,
        fut: Fut,
        progress: &Arc<AtomicU64>,
        bytes: u64,
    ) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        use std::sync::atomic::Ordering::Relaxed;
        let start = tokio::time::Instant::now();
        let deadline = start + self.policy.deadline_for(bytes);
        let tick = (self.policy.progress_timeout / 10)
            .clamp(Duration::from_millis(10), Duration::from_millis(250));
        let mut fut = std::pin::pin!(fut);
        loop {
            tokio::select! {
                r = &mut fut => return r,
                () = tokio::time::sleep(tick) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        self.stats.deadline_exceeded.fetch_add(1, Relaxed);
                        return Err(tonic::Status::deadline_exceeded(format!(
                            "shard stream exceeded its absolute deadline ({:?} for {bytes} bytes)",
                            self.policy.deadline_for(bytes)
                        )));
                    }
                    let last = progress.load(Relaxed);
                    let idle = now.duration_since(start).saturating_sub(Duration::from_millis(last));
                    if idle >= self.policy.progress_timeout {
                        self.stats.stalled.fetch_add(1, Relaxed);
                        return Err(tonic::Status::deadline_exceeded(format!(
                            "shard stream stalled: no frame moved for {:?}",
                            self.policy.progress_timeout
                        )));
                    }
                }
            }
        }
    }

    /// Channel for `addr`, created lazily. Connection failures surface as
    /// `Unavailable` statuses on the first call.
    ///
    /// # Errors
    ///
    /// Invalid address.
    pub fn channel(&self, addr: &str) -> Result<Channel, tonic::Status> {
        let mut map = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = map.get(addr) {
            return Ok(c.clone());
        }
        // HTTP/2 flow-control windows sized for multi-MiB shards over
        // high-latency links: with the 64 KiB defaults a 4 MiB shard needs
        // 64 round trips (RFC-009 §7, namespace fault runs). No per-RPC
        // timeout here: streams are guarded by `guard_stream`, unary RPCs
        // by `unary`.
        let ep = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
            .tcp_nodelay(true)
            .initial_stream_window_size(Some(STREAM_WINDOW))
            .initial_connection_window_size(Some(CONNECTION_WINDOW))
            .connect_timeout(self.policy.connect_timeout);
        let ch = ep.connect_lazy();
        map.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    /// Drop the cached channel for `addr` (after a connection error).
    pub fn forget(&self, addr: &str) {
        self.channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(addr);
    }

    fn client(&self, addr: &str) -> Result<StorageNodeClient<Channel>, tonic::Status> {
        Ok(StorageNodeClient::new(self.channel(addr)?).max_decoding_message_size(64 << 20))
    }

    /// Whether a status is the node's epoch fence.
    #[must_use]
    pub fn is_epoch_rejected(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::FailedPrecondition
            && status.message().starts_with("epoch rejected")
    }

    /// Write one shard; returns the node's CRC-32C of the bytes it stored.
    /// Retried per [`RpcPolicy`]; a duplicate write of the same bytes is
    /// idempotent on the node.
    ///
    /// # Errors
    ///
    /// Any RPC error, including the epoch fence.
    pub async fn put_shard(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        data: Bytes,
        frame_bytes: usize,
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::PutShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        self.with_retry(ctx, |fault| {
            self.put_shard_raw(addr, shard, epoch, data.clone(), frame_bytes, fault)
        })
        .await
    }

    async fn put_shard_raw(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        data: Bytes,
        frame_bytes: usize,
        fault: StreamFault,
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let mut client = self.client(addr)?;
        // Capacity 1: a frame is handed over only when the transport has
        // taken the previous one, so `progress` reflects frames on the wire.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let header = pb::PutShardRequest {
            body: Some(pb::put_shard_request::Body::Header(pb::PutShardHeader {
                protocol_version: pb::PROTOCOL_VERSION,
                key: Some(shard.pb()),
                group_epoch: epoch,
                payload_len: data.len() as u64,
            })),
        };
        let frame = frame_bytes.max(1);
        let progress = Arc::new(AtomicU64::new(0));
        let started = tokio::time::Instant::now();
        let bump = {
            let progress = Arc::clone(&progress);
            move || {
                progress.store(
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        };
        let bytes = data.len() as u64;
        let sender = tokio::spawn(async move {
            if tx.send(header).await.is_err() {
                return;
            }
            bump();
            if fault == StreamFault::Stall {
                std::future::pending::<()>().await;
            }
            let mut off = 0;
            while off < data.len() {
                if let StreamFault::Slow(d) = fault {
                    tokio::time::sleep(d).await;
                }
                let end = (off + frame).min(data.len());
                let msg = pb::PutShardRequest {
                    body: Some(pb::put_shard_request::Body::Data(data.slice(off..end))),
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
                bump();
                off = end;
            }
        });
        let rpc = async {
            let resp = client
                .put_shard(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await?;
            Ok(resp.into_inner())
        };
        let result = self.guard_stream(rpc, &progress, bytes).await;
        sender.abort();
        result
    }

    /// Read a shard or a byte range of it (`len == 0` means to the end).
    /// Retried per [`RpcPolicy`].
    ///
    /// # Errors
    ///
    /// `NotFound`, `DataLoss` (corrupt), or transport errors.
    pub async fn get_shard(
        &self,
        addr: &str,
        shard: ShardRef,
        offset: u64,
        len: u64,
    ) -> Result<Bytes, tonic::Status> {
        let frames = self.get_shard_frames(addr, shard, offset, len).await?;
        Ok(concat_frames(frames))
    }

    /// Read a shard as the frames the node streamed, in order, without
    /// concatenating them (the zero-copy read path: each frame is a slice
    /// of the transport's receive buffer).
    ///
    /// # Errors
    ///
    /// As [`Self::get_shard`].
    pub async fn get_shard_frames(
        &self,
        addr: &str,
        shard: ShardRef,
        offset: u64,
        len: u64,
    ) -> Result<Vec<Bytes>, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::GetShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        let counter = self.read_counter(addr);
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let r = self
            .with_retry(ctx, |fault| {
                self.get_shard_raw(addr, shard, offset, len, fault)
            })
            .await;
        counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        r
    }

    async fn get_shard_raw(
        &self,
        addr: &str,
        shard: ShardRef,
        offset: u64,
        len: u64,
        fault: StreamFault,
    ) -> Result<Vec<Bytes>, tonic::Status> {
        let mut client = self.client(addr)?;
        let req = pb::GetShardRequest {
            protocol_version: pb::PROTOCOL_VERSION,
            key: Some(shard.pb()),
            offset,
            len,
        };
        let progress = Arc::new(AtomicU64::new(0));
        let started = tokio::time::Instant::now();
        // Unknown length (whole shard): budget one stream window.
        let budget = if len == 0 {
            u64::from(STREAM_WINDOW)
        } else {
            len
        };
        let rpc = {
            let progress = Arc::clone(&progress);
            async move {
                let mut stream = client.get_shard(req).await?.into_inner();
                let mut out: Vec<Bytes> = Vec::new();
                let mut first = true;
                while let Some(msg) = stream.next().await {
                    if fault == StreamFault::Stall && !first {
                        std::future::pending::<()>().await;
                    }
                    if let StreamFault::Slow(d) = fault {
                        tokio::time::sleep(d).await;
                    }
                    first = false;
                    let data = msg?.data;
                    if !data.is_empty() {
                        out.push(data);
                    }
                    progress.store(
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                Ok(out)
            }
        };
        self.guard_stream(rpc, &progress, budget).await
    }

    /// Delete a shard (idempotent).
    ///
    /// # Errors
    ///
    /// RPC errors, including the epoch fence.
    pub async fn delete_shard(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
    ) -> Result<(), tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::DeleteShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let req = pb::DeleteShardRequest {
                    protocol_version: pb::PROTOCOL_VERSION,
                    key: Some(shard.pb()),
                    group_epoch: epoch,
                };
                client.delete_shard(req).await?;
                Ok(())
            })
        })
        .await
    }

    /// One page of a group's shards after `after` (key order), with payload
    /// lengths. `limit == 0` selects the node's default page size.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn list_shards_page(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
    ) -> Result<ShardPage, tonic::Status> {
        self.list_shards_page_with(addr, group_id, after, limit, false)
            .await
    }

    /// [`Self::list_shards_page`], optionally including quarantined shards
    /// (reported in `ShardPage::quarantined`; reconciliation only).
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn list_shards_page_with(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
        include_quarantined: bool,
    ) -> Result<ShardPage, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::ListShards,
            shard: None,
            group_id: Some(group_id),
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let req = pb::ListShardsRequest {
                    protocol_version: pb::PROTOCOL_VERSION,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                    include_quarantined,
                };
                let resp = client.list_shards(req).await?.into_inner();
                let mut entries = Vec::new();
                let mut quarantined = Vec::new();
                for e in resp.entries {
                    let Some(key) = e.key.as_ref().and_then(ShardRef::from_pb) else {
                        continue;
                    };
                    if e.quarantined {
                        quarantined.push((key, e.payload_len));
                    } else {
                        entries.push((key, e.payload_len));
                    }
                }
                Ok(ShardPage {
                    entries,
                    quarantined,
                    next: resp
                        .next
                        .as_ref()
                        .and_then(ShardRef::from_pb)
                        .filter(|_| resp.more),
                })
            })
        })
        .await
    }

    /// Every shard of a group on the node (walks all pages).
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn list_shards(
        &self,
        addr: &str,
        group_id: u64,
    ) -> Result<Vec<ShardRef>, tonic::Status> {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self.list_shards_page(addr, group_id, after, 0).await?;
            out.extend(page.entries.into_iter().map(|(k, _)| k));
            match page.next {
                Some(n) => after = Some(n),
                None => return Ok(out),
            }
        }
    }

    /// One page of shard verifications (the node reads and checks each
    /// shard's block checksums).
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn verify_shards_page(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
    ) -> Result<VerifyPage, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::VerifyShards,
            shard: None,
            group_id: Some(group_id),
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let req = pb::VerifyShardsRequest {
                    protocol_version: pb::PROTOCOL_VERSION,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                };
                let resp = client.verify_shards(req).await?.into_inner();
                Ok(VerifyPage {
                    shards: resp
                        .shards
                        .into_iter()
                        .filter_map(|v| {
                            Some(ShardVerification {
                                shard: ShardRef::from_pb(&v.key?)?,
                                payload_len: v.payload_len,
                                crc32c: v.crc32c,
                                ok: v.ok,
                            })
                        })
                        .collect(),
                    next: resp
                        .next
                        .as_ref()
                        .and_then(ShardRef::from_pb)
                        .filter(|_| resp.more),
                })
            })
        })
        .await
    }

    /// One page of deep shard verification (RFC-008 §8.2): the node reads
    /// every shard in full, classifies it and quarantines corrupt ones.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn scrub_shards_page(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
        max_bytes: u64,
    ) -> Result<ScrubPage, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::ScrubShards,
            shard: None,
            group_id: Some(group_id),
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let req = pb::ScrubShardsRequest {
                    protocol_version: pb::PROTOCOL_VERSION,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                    max_bytes,
                };
                let resp = client.scrub_shards(req).await?.into_inner();
                Ok(ScrubPage {
                    shards: resp
                        .shards
                        .into_iter()
                        .filter_map(|v| {
                            Some(ShardScrub {
                                shard: ShardRef::from_pb(&v.key?)?,
                                payload_len: v.payload_len,
                                crc32c: v.crc32c,
                                sequence: v.sequence,
                                status: ShardStatus::from_pb(v.status),
                                reason: v.reason,
                            })
                        })
                        .collect(),
                    next: resp
                        .next
                        .as_ref()
                        .and_then(ShardRef::from_pb)
                        .filter(|_| resp.more),
                    bytes_read: resp.bytes_read,
                })
            })
        })
        .await
    }

    /// Tell the node the current epoch of a group; returns the enforced epoch.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn update_epoch(
        &self,
        addr: &str,
        group_id: u64,
        epoch: u64,
    ) -> Result<u64, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::UpdateEpoch,
            shard: None,
            group_id: Some(group_id),
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let req = pb::UpdateGroupEpochRequest {
                    protocol_version: pb::PROTOCOL_VERSION,
                    group_id,
                    epoch,
                };
                Ok(client.update_group_epoch(req).await?.into_inner().epoch)
            })
        })
        .await
    }

    /// Node health and capacity.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn health(&self, addr: &str) -> Result<pb::HealthResponse, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::Health,
            shard: None,
            group_id: None,
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                Ok(client.health(pb::HealthRequest {}).await?.into_inner())
            })
        })
        .await
    }
}
