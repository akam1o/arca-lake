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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

use crate::storage::v1 as pb;
use crate::storage::v1::storage_node_client::StorageNodeClient;

/// Shard identity on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardRef {
    /// Stripe group.
    pub group_id: u64,
    /// Chunk ID.
    pub chunk_id: [u8; 16],
    /// Shard index.
    pub shard_index: u16,
}

/// What a conditional delete names: the record's sequence on the node and
/// the node store's epoch (Phase 3.5 audit CON-16). `sequence == 0` (an
/// older node, or [`RecordToken::NONE`]) is no token at all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RecordToken {
    /// Store epoch.
    pub store_epoch: u64,
    /// Record sequence.
    pub sequence: u64,
}

impl RecordToken {
    /// No token: an unconditional delete.
    pub const NONE: Self = Self {
        store_epoch: 0,
        sequence: 0,
    };

    /// The token of a write, from its response.
    #[must_use]
    pub fn of_put(r: &pb::PutShardResponse) -> Self {
        Self {
            store_epoch: r.store_epoch,
            sequence: r.sequence,
        }
    }

    /// Whether the node handed out a token (newer nodes always do).
    #[must_use]
    pub fn is_some(&self) -> bool {
        self.sequence != 0
    }
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
    /// The delete token of each listed shard (absent from an older node).
    pub sequences: std::collections::HashMap<ShardRef, RecordToken>,
    /// Shards retired by a delete of the incarnation the node holds (Phase
    /// 3.12): deleting them again changes nothing.
    pub retired: std::collections::HashSet<ShardRef>,
    /// Shards retired under any incarnation (Phase 3.12 review): a member
    /// move writes them again.
    pub retired_any: std::collections::HashSet<ShardRef>,
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
    /// Whether the record is retired and kept for reclaim (ADR-0028);
    /// `None` from a node that does not report it (0.1.0).
    pub retired: Option<bool>,
}

/// A scrubbed record's retirement as the controller may use it: `None`
/// unless the node reports retirement (a 0.1.0 node sends neither field,
/// which decodes as `false`, `false`).
#[must_use]
pub fn retirement(reported: bool, retired: bool) -> Option<bool> {
    reported.then_some(retired)
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
    /// The node's store epoch (0 from an earlier release, Phase 3.14).
    pub store_epoch: u64,
    /// Every existing record below this sequence was listed in this
    /// page's window (0: unknown).
    pub indexed_below: u64,
    /// Records below the caller's `since` were not read again.
    pub since_applied: bool,
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
    /// `ReclaimShard` (Phase 3.12).
    ReclaimShard,
    /// `ListShards` (and `ListPending`).
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
#[derive(Debug, Clone)]
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
    /// Wait for a permit of this semaphore before sending (a request held
    /// until the test releases it: deterministic overlap, no timing). The
    /// permit is consumed; release with `add_permits`.
    Hold(Arc<tokio::sync::Semaphore>),
}

impl PartialEq for FaultAction {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None)
            | (Self::Duplicate, Self::Duplicate)
            | (Self::Reset, Self::Reset)
            | (Self::StallStream, Self::StallStream) => true,
            (Self::DelayBefore(a), Self::DelayBefore(b))
            | (Self::DelayAfter(a), Self::DelayAfter(b))
            | (Self::SlowStream(a), Self::SlowStream(b)) => a == b,
            (Self::FailBefore(a, x), Self::FailBefore(b, y))
            | (Self::FailAfter(a, x), Self::FailAfter(b, y)) => a == b && x == y,
            (Self::Hold(a), Self::Hold(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for FaultAction {}

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
    /// Nodes refused because their protocol window does not intersect ours
    /// (RFC-014 §5.2).
    pub incompatible: AtomicU64,
    /// Deletes of this process's own partial copies that a node refused
    /// (identity, fence, epoch): the copies stay until reconciliation
    /// removes them ([`NodeClients::note_cleanup_refused`]).
    pub cleanup_refused: AtomicU64,
}

/// What a node reported in `Health` (RFC-014 §5.1) and the protocol
/// version negotiated with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCaps {
    /// Capabilities; a node that predates them reports `(1, 1)` everywhere.
    pub caps: arca_lake_common::compat::Capabilities,
    /// Protocol version this client sends to the node, if the windows
    /// intersect.
    pub protocol: Option<u32>,
    /// Software version string.
    pub software: String,
    /// Format the node stamps on new segments.
    pub format_write: u64,
    /// Incarnation the node adopted, if any.
    pub incarnation: Option<[u8; 16]>,
    /// The node's delete fence: generation (0 = none) and whether open.
    pub delete_fence: (u64, bool),
    /// Shard write ownership (ADR-0037): `None` from an earlier release
    /// (it takes writes without an incarnation, and any write while it
    /// adopted none); otherwise whether it is strict (refuses every write
    /// not carrying its incarnation).
    pub write_ownership: Option<bool>,
    /// When the `Health` answer was received.
    pub seen: std::time::Instant,
}

/// How long the capabilities a node reported are trusted for the write
/// schema check ([`NodeClients::set_required_node_schema`]) before they are
/// asked again (ADR-0039; the gateway's cluster-state refresh interval).
pub const WRITE_CAPS_MAX_AGE: Duration = Duration::from_secs(2);

impl NodeCaps {
    /// Interpret a `Health` response (zero fields = version 1 node).
    #[must_use]
    pub fn from_health(h: &pb::HealthResponse) -> Self {
        let caps = arca_lake_common::compat::Capabilities {
            protocol_min: h.protocol_min.max(1),
            protocol_max: h.protocol_max.max(1),
            schema_min: 0,
            schema_max: h.schema_max,
            format_min: h.format_min.max(1),
            format_max: h.format_max.max(1),
        };
        let v = pb::PROTOCOL_VERSION.min(caps.protocol_max);
        let protocol = (v >= pb::PROTOCOL_VERSION_MIN && v >= caps.protocol_min).then_some(v);
        Self {
            caps,
            protocol,
            software: h.software.clone(),
            format_write: h.format_write,
            incarnation: <[u8; 16]>::try_from(h.incarnation.as_ref()).ok(),
            delete_fence: (h.delete_fence_generation, h.delete_fence_open),
            write_ownership: h
                .write_ownership_supported
                .then_some(h.write_ownership_strict),
            seen: std::time::Instant::now(),
        }
    }

    fn compatible(&self) -> Result<u32, tonic::Status> {
        self.protocol.ok_or_else(|| {
            tonic::Status::failed_precondition(format!(
                "incompatible protocol: node speaks [{}, {}], this process [{}, {}]",
                self.caps.protocol_min,
                self.caps.protocol_max,
                pb::PROTOCOL_VERSION_MIN,
                pb::PROTOCOL_VERSION
            ))
        })
    }
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
    /// See [`RpcStats::incompatible`].
    pub incompatible: u64,
    /// See [`RpcStats::cleanup_refused`].
    pub cleanup_refused: u64,
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
    /// Negotiated protocol version and capabilities per node address
    /// (RFC-014 §5.2): filled by `Health` on first use.
    negotiated: Arc<Mutex<HashMap<String, NodeCaps>>>,
    /// Cluster incarnation stamped on `DeleteShard` (RFC-015 §5.3).
    incarnation: Arc<Mutex<Option<[u8; 16]>>>,
    /// Delete fence generation stamped on `DeleteShard` (ADR-0023);
    /// `None`: this process may not delete (deletes fail locally).
    delete_fence: Arc<Mutex<Option<u64>>>,
    /// Deletes stopped for good (Phase 3.11, [`NodeClients::stop_deletes`]):
    /// no later [`NodeClients::set_delete_fence`] reopens them.
    deletes_latched: Arc<AtomicBool>,
    /// The last generation deletes carried (kept when they stop): what a
    /// latched stop closes on the nodes even after an earlier stop took it
    /// (Phase 3.11 review).
    last_delete_fence: Arc<AtomicU64>,
    /// Mutual TLS (ADR-0017); `None` = explicitly plaintext.
    tls: Option<Arc<crate::tls::TlsContext>>,
    /// Node id per address: the identity a node must present.
    node_ids: Arc<Mutex<HashMap<String, u64>>>,
    /// The identity (node id, token) a node acknowledged, per address:
    /// stamped on its deletes (Phase 3.11, R-100).
    identities: Arc<Mutex<HashMap<String, NodeIdentity>>>,
    /// `GetShard` RPCs in flight per node address (durable-read priority
    /// for cache populations).
    reads_in_flight: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    /// The metadata schema a node must enforce for a shard write to it to
    /// count (ADR-0039; 0: none required).
    required_node_schema: Arc<AtomicU64>,
}

/// A node's identity: its id and the token it acknowledged.
type NodeIdentity = (u64, [u8; 16]);

/// How a delete fence is pushed ([`NodeClients::push_delete_fence_with`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FencePush {
    /// Set the operator hold (with a closed generation).
    pub hold: bool,
    /// Release the operator hold.
    pub release: bool,
    /// The caller's cluster incarnation (the hold is tied to it).
    pub incarnation: Option<[u8; 16]>,
    /// With `release`: the node's `delete_fence_unknown_token` (needed
    /// when its facts were lost; 0 otherwise).
    pub release_token: u64,
    /// With an open generation: who opened it (Phase 3.10; 0 = unknown).
    pub opener: u64,
    /// The node's identity, carried: a disk holding another adopts
    /// nothing (Phase 3.11; `None`: not carried).
    pub identity: Option<(u64, [u8; 16])>,
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
            negotiated: Arc::default(),
            incarnation: Arc::default(),
            delete_fence: Arc::default(),
            deletes_latched: Arc::default(),
            last_delete_fence: Arc::default(),
            tls: None,
            node_ids: Arc::default(),
            identities: Arc::default(),
            required_node_schema: Arc::default(),
        }
    }

    /// Use mutual TLS for every connection (ADR-0017). Call before the
    /// first RPC: cached channels keep their transport.
    #[must_use]
    pub fn with_tls(mut self, tls: Option<Arc<crate::tls::TlsContext>>) -> Self {
        self.tls = tls;
        self
    }

    /// The TLS context, if any.
    #[must_use]
    pub fn tls(&self) -> Option<&Arc<crate::tls::TlsContext>> {
        self.tls.as_ref()
    }

    /// Record which node serves `addr`: TLS connections to it then require
    /// `spiffe://<td>/node/<id>` exactly (otherwise any node identity of
    /// the trust domain). A change drops the cached channel.
    /// The identity the node at `addr` acknowledged (stamped on deletes to
    /// it), or `None` to stamp none.
    pub fn note_identity(&self, addr: &str, identity: Option<(u64, [u8; 16])>) {
        let mut m = self
            .identities
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match identity {
            Some(i) => {
                m.insert(addr.to_string(), i);
            }
            None => {
                m.remove(addr);
            }
        }
    }

    /// Stamp deletes to every address whose node id is known
    /// ([`Self::note_node_id`]) with the identity that node acknowledged in
    /// `acknowledged` (node id and token, as recorded in metadata), and
    /// none to the others: what a leader's health pass notes, for a process
    /// that reads the records instead (a gateway's deletes of its own
    /// partial copies). A disk at such an address holding another identity
    /// still refuses them (R-100).
    pub fn note_acknowledged_identities(&self, acknowledged: &HashMap<u64, [u8; 16]>) {
        let addrs: Vec<(String, u64)> = self
            .node_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(a, id)| (a.clone(), *id))
            .collect();
        for (addr, id) in addrs {
            self.note_identity(&addr, acknowledged.get(&id).map(|t| (id, *t)));
        }
    }

    fn identity_of(&self, addr: &str) -> Option<(u64, [u8; 16])> {
        self.identities
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(addr)
            .copied()
    }

    /// The node addresses this client knows an id for.
    #[must_use]
    pub fn known_addresses(&self) -> Vec<String> {
        self.node_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    /// Remember the node id at `addr` (the identity it must present).
    pub fn note_node_id(&self, addr: &str, node_id: u64) {
        let prev = self
            .node_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(addr.to_string(), node_id);
        // A channel made before the id was known (or for another id) is
        // not pinned to this node's certificate name (Phase 3.13, R-105):
        // the next call connects again, pinned.
        if prev != Some(node_id) {
            self.forget(addr);
        }
    }

    /// Stamp `DeleteShard` and `PutShard` requests with the cluster
    /// incarnation this process started under (RFC-015 §5.3, R-117). Set once
    /// after the startup gate; a node that adopted a different incarnation
    /// then rejects the deletes and the shard writes of this process, and a
    /// strict node (ADR-0037) every shard write of a process that set none.
    pub fn set_incarnation(&self, incarnation: Option<[u8; 16]>) {
        *self
            .incarnation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = incarnation;
    }

    /// Allow deletes under fence generation `generation` (ADR-0023), or
    /// none (`None`: every delete fails locally with `FAILED_PRECONDITION`,
    /// before any RPC). Nodes enforce the generation; this is what the
    /// process believes it holds.
    pub fn set_delete_fence(&self, generation: Option<u64>) {
        let mut g = self
            .delete_fence
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *g = generation
            .filter(|g| *g > 0)
            .filter(|_| !self.deletes_latched.load(Ordering::SeqCst));
        if let Some(v) = *g {
            self.last_delete_fence.fetch_max(v, Ordering::SeqCst);
        }
    }

    /// Stop this process's deletes now and return the generation they
    /// carried. With `latch`, they stay stopped: a later
    /// [`Self::set_delete_fence`] (a health pass already under way) does
    /// not reopen them (Phase 3.11).
    pub fn stop_deletes(&self, latch: bool) -> Option<u64> {
        let mut g = self
            .delete_fence
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if latch {
            self.deletes_latched.store(true, Ordering::SeqCst);
        }
        g.take()
            .or_else(|| Some(self.last_delete_fence.load(Ordering::SeqCst)).filter(|v| *v > 0))
    }

    /// The delete fence generation stamped on deletes, if any.
    #[must_use]
    pub fn delete_fence(&self) -> Option<u64> {
        *self
            .delete_fence
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether `status` is a node's (or this client's) delete fence
    /// refusal: the delete was not applied and is retried once the fence
    /// is open again.
    #[must_use]
    pub fn is_fenced(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::FailedPrecondition
            && status.message().starts_with("delete fence")
    }

    /// The incarnation stamped on deletes and shard writes.
    #[must_use]
    pub fn incarnation(&self) -> Option<[u8; 16]> {
        *self
            .incarnation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Capabilities learned from `addr` (after any RPC to it).
    #[must_use]
    pub fn node_caps(&self, addr: &str) -> Option<NodeCaps> {
        self.negotiated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(addr)
            .cloned()
    }

    /// The protocol version to send to `addr` (RFC-014 §5.2): on first use
    /// `Health` is called and `min(P_cur, node.protocol_max)` cached; a
    /// node whose window does not intersect ours is refused with
    /// `FailedPrecondition "incompatible protocol"` and counted.
    ///
    /// # Errors
    ///
    /// The `Health` RPC's error, or the incompatibility.
    pub async fn version_for(&self, addr: &str) -> Result<u32, tonic::Status> {
        if let Some(c) = self.node_caps(addr) {
            return c.compatible();
        }
        let resp = self
            .unary(async {
                let mut client = self.client(addr)?;
                Ok(client
                    .health(pb::HealthRequest::default())
                    .await?
                    .into_inner())
            })
            .await?;
        let caps = NodeCaps::from_health(&resp);
        let v = caps.compatible();
        // Only a compatible window is kept: an incompatible one is asked
        // again on the next call, so a node replaced by a compatible binary
        // is used again without a restart (Phase 3.5 audit OPS-12).
        if v.is_ok() {
            self.negotiated
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(addr.to_string(), caps.clone());
        }
        if v.is_err() {
            self.stats
                .incompatible
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        v
    }

    /// Remember the capabilities a `Health` response carried (the
    /// controller's health pass calls `Health` anyway).
    pub fn note_health(&self, addr: &str, resp: &pb::HealthResponse) {
        self.negotiated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(addr.to_string(), NodeCaps::from_health(resp));
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
            incompatible: self.stats.incompatible.load(Relaxed),
            cleanup_refused: self.stats.cleanup_refused.load(Relaxed),
        }
    }

    /// Count a delete of this process's own partial copy that a node
    /// refused (the copy is left to reconciliation).
    pub fn note_cleanup_refused(&self) {
        self.stats
            .cleanup_refused
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        let addr = ctx.addr;
        let r = self.with_faults_inner(ctx, call).await;
        // A node that refuses the negotiated protocol version runs another
        // binary than when it was asked: negotiate again on the next call
        // (Phase 3.5 audit OPS-11).
        if let Err(status) = &r
            && status.code() == tonic::Code::FailedPrecondition
            && status.message().starts_with("protocol version")
        {
            self.negotiated
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(addr);
        }
        r
    }

    async fn with_faults_inner<T, Fut>(
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
            FaultAction::Hold(sem) => {
                sem.acquire()
                    .await
                    .map_err(|_| tonic::Status::cancelled("hold closed"))?
                    .forget();
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
        let ch = match &self.tls {
            None => ep.connect_lazy(),
            Some(ctx) => {
                let ctx = Arc::clone(ctx);
                let target = addr.to_string();
                let node_id = self
                    .node_ids
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(addr)
                    .copied();
                ep.connect_with_connector_lazy(tower::service_fn(
                    move |_uri: tonic::codegen::http::Uri| {
                        let ctx = Arc::clone(&ctx);
                        let target = target.clone();
                        async move {
                            crate::tls::connect(&ctx, &target, node_id)
                                .await
                                .map(hyper_util::rt::TokioIo::new)
                        }
                    },
                ))
            }
        };
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

    /// Require nodes to enforce metadata schema `schema` for a shard write
    /// to count (ADR-0039: the cluster's `schema_write` once it reached
    /// schema 5; 0 below it). A node that reports less, in `Health` (asked
    /// again at most [`WRITE_CAPS_MAX_AGE`] after the last answer, and
    /// after every connection failure) or in the write's acknowledgement,
    /// is a failed write: [`Self::is_node_schema_refused`].
    pub fn set_required_node_schema(&self, schema: u64) {
        self.required_node_schema
            .store(schema, std::sync::atomic::Ordering::Relaxed);
    }

    /// See [`Self::set_required_node_schema`].
    #[must_use]
    pub fn required_node_schema(&self) -> u64 {
        self.required_node_schema
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether a shard write was refused because the node runs a release
    /// below the required schema ([`Self::set_required_node_schema`]).
    #[must_use]
    pub fn is_node_schema_refused(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::FailedPrecondition
            && status.message().starts_with("node schema")
    }

    fn node_schema_refusal(addr: &str, reported: u64, required: u64, what: &str) -> tonic::Status {
        tonic::Status::failed_precondition(format!(
            "node schema: {addr} {what} schema {reported}, below the cluster's schema_write {required} (an earlier release, ADR-0039): the shard write does not count"
        ))
    }

    /// The write schema check before a shard write: the node's reported
    /// schema, from a `Health` answer at most [`WRITE_CAPS_MAX_AGE`] old.
    async fn check_write_schema(&self, addr: &str) -> Result<(), tonic::Status> {
        let required = self.required_node_schema();
        if required == 0 {
            return Ok(());
        }
        let reported = match self.node_caps(addr) {
            Some(c) if c.seen.elapsed() <= WRITE_CAPS_MAX_AGE => c.caps.schema_max,
            _ => self.health(addr).await?.schema_max,
        };
        if reported < required {
            return Err(Self::node_schema_refusal(
                addr, reported, required, "reports",
            ));
        }
        Ok(())
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
        self.put_shard_as(
            addr,
            shard,
            epoch,
            data,
            frame_bytes,
            pb::WriteClass::Client,
        )
        .await
    }

    /// [`Self::put_shard`] for a write of `class` (Phase 3.6 capacity
    /// admission: a node keeps a reserve client writes may not use and
    /// repair may). A node without room answers `RESOURCE_EXHAUSTED`
    /// ([`Self::is_capacity_refused`]), which is not retried.
    ///
    /// # Errors
    ///
    /// As [`Self::put_shard`].
    pub async fn put_shard_as(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        data: Bytes,
        frame_bytes: usize,
        class: pb::WriteClass,
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let node_id = self
            .node_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(addr)
            .copied()
            .unwrap_or(0);
        self.put_shard_to(addr, node_id, shard, epoch, data, frame_bytes, class)
            .await
    }

    /// [`Self::put_shard_as`] for the placement member `node_id` (ADR-0037):
    /// the write names that node, and the identity token it acknowledged
    /// if this pool knows it for `addr` ([`Self::note_identity`]), so that
    /// another node at `addr` (an address reused within the cluster, a
    /// stale map) refuses it instead of storing it. `node_id` 0 names none
    /// ([`Self::put_shard_as`] names the node noted for `addr`, if any).
    ///
    /// # Errors
    ///
    /// As [`Self::put_shard`]; a node of another identity answers
    /// `FAILED_PRECONDITION` (`node rejected`).
    #[allow(clippy::too_many_arguments)]
    pub async fn put_shard_to(
        &self,
        addr: &str,
        node_id: u64,
        shard: ShardRef,
        epoch: u64,
        data: Bytes,
        frame_bytes: usize,
        class: pb::WriteClass,
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::PutShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        let token = self
            .identity_of(addr)
            .filter(|i| node_id != 0 && i.0 == node_id)
            .map(|i| i.1);
        let target = (node_id, token);
        self.check_write_schema(addr).await?;
        let resp = self
            .with_retry(ctx, |fault| {
                self.put_shard_raw(
                    addr,
                    target,
                    shard,
                    epoch,
                    data.clone(),
                    frame_bytes,
                    class,
                    fault,
                )
            })
            .await;
        // The acknowledgement itself shows the node's release (ADR-0039): a
        // node replaced by an earlier release since its last `Health` stored
        // the bytes, but the write does not count (they are an unreferenced
        // record, collected as any failed write's).
        let required = self.required_node_schema();
        match resp {
            Ok(r) if required > 0 && r.schema_max < required => {
                self.forget_caps(addr);
                Err(Self::node_schema_refusal(
                    addr,
                    r.schema_max,
                    required,
                    "acknowledged the write at",
                ))
            }
            Ok(r) => Ok(r),
            Err(e) => {
                // A connection failure: the node may come back as another
                // binary; its capabilities are asked again.
                if matches!(
                    e.code(),
                    tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Cancelled
                ) {
                    self.forget_caps(addr);
                }
                Err(e)
            }
        }
    }

    /// Drop the capabilities learned from `addr` (asked again on next use).
    pub fn forget_caps(&self, addr: &str) {
        self.negotiated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(addr);
    }

    /// Whether a node refused a write for lack of capacity.
    #[must_use]
    pub fn is_capacity_refused(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::ResourceExhausted
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_shard_raw(
        &self,
        addr: &str,
        target: (u64, Option<[u8; 16]>),
        shard: ShardRef,
        epoch: u64,
        data: Bytes,
        frame_bytes: usize,
        class: pb::WriteClass,
        fault: StreamFault,
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let mut client = self.client(addr)?;
        // Capacity 1: a frame is handed over only when the transport has
        // taken the previous one, so `progress` reflects frames on the wire.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let header = pb::PutShardRequest {
            body: Some(pb::put_shard_request::Body::Header(pb::PutShardHeader {
                protocol_version: self.version_for(addr).await?,
                key: Some(shard.pb()),
                group_epoch: epoch,
                payload_len: data.len() as u64,
                write_class: class.into(),
                incarnation: self
                    .incarnation()
                    .map(|i| i.to_vec())
                    .unwrap_or_default()
                    .into(),
                node_id: target.0,
                node_identity: target
                    .1
                    .map(|t| Bytes::copy_from_slice(&t))
                    .unwrap_or_default(),
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
            protocol_version: self.version_for(addr).await?,
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
        self.delete_shard_if(addr, shard, epoch, RecordToken::NONE)
            .await
    }

    /// Delete a shard only if the node's newest record of it is the one
    /// `token` names ([`RecordToken::NONE`]: unconditionally). A changed
    /// record or a store of another epoch answers `ABORTED`
    /// ([`Self::is_changed`]).
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn delete_shard_if(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        token: RecordToken,
    ) -> Result<(), tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::DeleteShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        let Some(fence) = self.delete_fence() else {
            return Err(tonic::Status::failed_precondition(
                "delete fence closed in this process: no delete is sent",
            ));
        };
        let identity = self.identity_of(addr);
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let negotiated = self.version_for(addr).await?;
                // Every delete goes at the fenced version or not at all: a
                // node that does not enforce the fence (or was rolled back
                // since the negotiation) refuses the version instead of
                // deleting unfenced (ADR-0023).
                // Since Phase 3.10 only to a node that also keeps the rejoin
                // fence (protocol 4): a node rolled back to an earlier
                // release receives no delete.
                if negotiated < pb::REJOIN_FENCE_VERSION {
                    return Err(tonic::Status::failed_precondition(
                        "delete fence: the node does not enforce the rejoin fence (protocol version below 4): no delete is sent",
                    ));
                }
                // A conditional delete goes only to a node that enforces it,
                // and always at a version a node that does not would refuse:
                // a node rolled back since the negotiation answers
                // FAILED_PRECONDITION instead of deleting unconditionally
                // (Phase 3.5 audit CON-22).
                let protocol_version = negotiated.max(pb::REJOIN_FENCE_VERSION);
                let req = pb::DeleteShardRequest {
                    protocol_version,
                    key: Some(shard.pb()),
                    group_epoch: epoch,
                    incarnation: self
                        .incarnation()
                        .map(|i| Bytes::copy_from_slice(&i))
                        .unwrap_or_default(),
                    if_sequence: token.sequence,
                    if_store_epoch: token.store_epoch,
                    delete_fence: fence,
                    node_id: identity.map_or(0, |i| i.0),
                    node_identity: identity
                        .map(|i| Bytes::copy_from_slice(&i.1))
                        .unwrap_or_default(),
                };
                client.delete_shard(req).await?;
                Ok(())
            })
        })
        .await
    }

    /// Reclaim a retired record (Phase 3.12, ADR-0028): `observed_at_ms`
    /// is this process's clock taken before the metadata read that found
    /// the shard unneeded. Sent under the same fence, incarnation and
    /// identity as a delete.
    ///
    /// # Errors
    ///
    /// RPC errors; `FAILED_PRECONDITION` when the node refuses.
    pub async fn reclaim_shard(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        token: RecordToken,
        observed_at_ms: u64,
    ) -> Result<(), tonic::Status> {
        self.reclaim_or_keep(addr, shard, epoch, token, observed_at_ms, false)
            .await
            .map(|_| ())
    }

    /// [`Self::reclaim_shard`], returning the payload bytes the node
    /// removed (Phase 3.13; 0 from an earlier release).
    ///
    /// # Errors
    ///
    /// As [`Self::reclaim_shard`].
    pub async fn reclaim_shard_sized(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        token: RecordToken,
        observed_at_ms: u64,
    ) -> Result<u64, tonic::Status> {
        self.reclaim_or_keep(addr, shard, epoch, token, observed_at_ms, false)
            .await
    }

    /// End the retirement of a record the caller's metadata needs (Phase
    /// 3.12): the record stays; a later delete retires it anew.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn keep_shard(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        token: RecordToken,
    ) -> Result<(), tonic::Status> {
        self.reclaim_or_keep(addr, shard, epoch, token, 0, true)
            .await
            .map(|_| ())
    }

    async fn reclaim_or_keep(
        &self,
        addr: &str,
        shard: ShardRef,
        epoch: u64,
        token: RecordToken,
        observed_at_ms: u64,
        keep: bool,
    ) -> Result<u64, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::ReclaimShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        let Some(fence) = self.delete_fence() else {
            return Err(tonic::Status::failed_precondition(
                "delete fence closed in this process: no reclaim is sent",
            ));
        };
        let identity = self.identity_of(addr);
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let negotiated = self.version_for(addr).await?;
                let req = pb::ReclaimShardRequest {
                    protocol_version: negotiated.max(pb::REJOIN_FENCE_VERSION),
                    key: Some(shard.pb()),
                    group_epoch: epoch,
                    incarnation: self
                        .incarnation()
                        .map(|i| Bytes::copy_from_slice(&i))
                        .unwrap_or_default(),
                    sequence: token.sequence,
                    store_epoch: token.store_epoch,
                    delete_fence: fence,
                    node_id: identity.map_or(0, |i| i.0),
                    node_identity: identity
                        .map(|i| Bytes::copy_from_slice(&i.1))
                        .unwrap_or_default(),
                    observed_at_ms,
                    keep,
                };
                Ok(client.reclaim_shard(req).await?.into_inner().payload_bytes)
            })
        })
        .await
    }

    /// One page of the node's retired records after `after` (Phase 3.12):
    /// `(shard, token, retired_at_ms)`, the next cursor, and the node's
    /// retention (ms).
    ///
    /// # Errors
    ///
    /// RPC errors (`UNIMPLEMENTED` from an earlier release).
    #[allow(clippy::type_complexity)]
    pub async fn list_pending(
        &self,
        addr: &str,
        after: Option<ShardRef>,
        limit: u32,
    ) -> Result<(Vec<(ShardRef, RecordToken, u64)>, Option<ShardRef>, u64), tonic::Status> {
        self.list_pending_with(addr, after, limit, false).await
    }

    /// [`Self::list_pending`], with `due_only` (Phase 3.13): only records
    /// due by the node's clock; a page may be short or empty with a next
    /// cursor (each call looks at a bounded number of records). A node of
    /// an earlier phase lists every record.
    ///
    /// # Errors
    ///
    /// RPC errors.
    #[allow(clippy::type_complexity)]
    pub async fn list_pending_with(
        &self,
        addr: &str,
        after: Option<ShardRef>,
        limit: u32,
        due_only: bool,
    ) -> Result<(Vec<(ShardRef, RecordToken, u64)>, Option<ShardRef>, u64), tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::ListShards,
            shard: None,
            group_id: None,
        };
        self.with_faults(ctx, |_| {
            self.unary(async {
                let mut client = self.client(addr)?;
                let negotiated = self.version_for(addr).await?;
                let r = client
                    .list_pending(pb::ListPendingRequest {
                        protocol_version: negotiated,
                        after: after.map(|a| a.pb()),
                        limit,
                        due_only,
                    })
                    .await?
                    .into_inner();
                let page = r
                    .shards
                    .iter()
                    .filter_map(|p| {
                        let k = ShardRef::from_pb(p.key.as_ref()?)?;
                        Some((
                            k,
                            RecordToken {
                                store_epoch: p.store_epoch,
                                sequence: p.sequence,
                            },
                            p.retired_at_ms,
                        ))
                    })
                    .collect();
                let next = r.next.as_ref().and_then(ShardRef::from_pb);
                Ok((page, next, r.retention_ms))
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
                    protocol_version: self.version_for(addr).await?,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                    include_quarantined,
                };
                let resp = client.list_shards(req).await?.into_inner();
                let mut entries = Vec::new();
                let mut quarantined = Vec::new();
                let mut sequences = std::collections::HashMap::new();
                let mut retired = std::collections::HashSet::new();
                let mut retired_any = std::collections::HashSet::new();
                for e in resp.entries {
                    let Some(key) = e.key.as_ref().and_then(ShardRef::from_pb) else {
                        continue;
                    };
                    if e.sequence != 0 {
                        sequences.insert(
                            key,
                            RecordToken {
                                store_epoch: e.store_epoch,
                                sequence: e.sequence,
                            },
                        );
                    }
                    if e.retired {
                        retired.insert(key);
                    }
                    if e.retired_any {
                        retired_any.insert(key);
                    }
                    if e.quarantined {
                        quarantined.push((key, e.payload_len));
                    } else {
                        entries.push((key, e.payload_len));
                    }
                }
                Ok(ShardPage {
                    entries,
                    quarantined,
                    sequences,
                    retired,
                    retired_any,
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

    /// Whether a conditional delete was refused because the shard changed.
    #[must_use]
    pub fn is_changed(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::Aborted
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
        self.verify_shards_page_with(addr, group_id, after, limit, false)
            .await
    }

    /// [`Self::verify_shards_page`] reading again only records at or above
    /// `since` = `(store epoch, sequence)` when the node is still at that
    /// store epoch (Phase 3.14; an earlier release verifies everything).
    ///
    /// # Errors
    ///
    /// As [`Self::verify_shards_page`].
    pub async fn verify_shards_since(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
        since: Option<(u64, u64)>,
    ) -> Result<VerifyPage, tonic::Status> {
        self.verify_shards_inner(addr, group_id, after, limit, false, since)
            .await
    }

    /// [`Self::verify_shards_page`], read-only when `read_only` (a corrupt
    /// shard is reported, not quarantined; nodes whose `Health` says
    /// `verify_read_only`).
    ///
    /// # Errors
    ///
    /// As [`Self::verify_shards_page`].
    pub async fn verify_shards_page_with(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
        read_only: bool,
    ) -> Result<VerifyPage, tonic::Status> {
        self.verify_shards_inner(addr, group_id, after, limit, read_only, None)
            .await
    }

    async fn verify_shards_inner(
        &self,
        addr: &str,
        group_id: u64,
        after: Option<ShardRef>,
        limit: u32,
        read_only: bool,
        since: Option<(u64, u64)>,
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
                    protocol_version: self.version_for(addr).await?,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                    read_only,
                    since_store_epoch: since.map_or(0, |s| s.0),
                    since_sequence: since.map_or(0, |s| s.1),
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
                    store_epoch: resp.store_epoch,
                    indexed_below: resp.indexed_below,
                    since_applied: resp.since_applied,
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
                    protocol_version: self.version_for(addr).await?,
                    group_id,
                    after: after.map(|s| s.pb()),
                    limit,
                    max_bytes,
                };
                let resp = client.scrub_shards(req).await?.into_inner();
                let reported = resp.retirement_reported;
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
                                retired: retirement(reported, v.retired),
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
                    protocol_version: self.version_for(addr).await?,
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
        self.health_with(addr, pb::HealthRequest::default()).await
    }

    /// Push delete fence `generation` (open or closed) to the node at
    /// `addr` (ADR-0023); returns the fence it holds afterwards (a node
    /// keeps a higher generation it already holds). Needs the controller or
    /// admin role.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn push_delete_fence(
        &self,
        addr: &str,
        generation: u64,
        open: bool,
    ) -> Result<(u64, bool), tonic::Status> {
        self.push_delete_fence_with(addr, generation, open, FencePush::default())
            .await
            .map(|(g, o, _)| (g, o))
    }

    /// [`Self::push_delete_fence`] with the operator hold or its release
    /// and the caller's incarnation; returns `(generation, open, held)`.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn push_delete_fence_with(
        &self,
        addr: &str,
        generation: u64,
        open: bool,
        how: FencePush,
    ) -> Result<(u64, bool, bool), tonic::Status> {
        let h = self
            .health_with(
                addr,
                pb::HealthRequest {
                    delete_fence_generation: generation,
                    delete_fence_open: open,
                    delete_fence_hold: how.hold,
                    delete_fence_release: how.release,
                    delete_fence_release_token: how.release_token,
                    delete_fence_opener: how.opener,
                    incarnation: how
                        .incarnation
                        .map(|i| Bytes::copy_from_slice(&i))
                        .unwrap_or_default(),
                    node_id: how.identity.map_or(0, |i| i.0),
                    node_identity: how
                        .identity
                        .map(|i| Bytes::copy_from_slice(&i.1))
                        .unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await?;
        if h.identity_mismatch {
            return Err(tonic::Status::failed_precondition(
                "identity rejected: the disk at this address holds another identity; nothing adopted",
            ));
        }
        Ok((
            h.delete_fence_generation,
            h.delete_fence_open,
            h.delete_fence_hold,
        ))
    }

    /// `Health` carrying cluster facts for the node (`format_write`, the
    /// incarnation; RFC-014 §5.3, RFC-015 §5.3). The response's
    /// capabilities are remembered for [`Self::node_caps`].
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn health_with(
        &self,
        addr: &str,
        req: pb::HealthRequest,
    ) -> Result<pb::HealthResponse, tonic::Status> {
        let ctx = RpcContext {
            addr,
            rpc: Rpc::Health,
            shard: None,
            group_id: None,
        };
        let resp = self
            .with_faults(ctx, |_| {
                let req = req.clone();
                self.unary(async {
                    let mut client = self.client(addr)?;
                    Ok(client.health(req).await?.into_inner())
                })
            })
            .await?;
        self.note_health(addr, &resp);
        Ok(resp)
    }

    /// Whether a status is a node's protocol, incarnation or write target
    /// refusal.
    #[must_use]
    pub fn is_incompatible(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::FailedPrecondition
            && (status.message().starts_with("incompatible protocol")
                || status.message().starts_with("protocol version")
                || status.message().starts_with("incarnation rejected")
                || status.message().starts_with("node rejected"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    /// A 0.1.0 node's scrub page (fields 1 to 6 of `ShardScrub`, 1 to 4 of
    /// the response) decodes without retirement: its records are never
    /// taken as kept for reclaim.
    #[test]
    fn a_scrub_page_without_retirement_reports_none() {
        let old = pb::ScrubShardsResponse {
            shards: vec![pb::ShardScrub {
                key: None,
                payload_len: 1,
                crc32c: 2,
                sequence: 3,
                status: pb::ShardStatus::Ok as i32,
                reason: String::new(),
                retired: false,
            }],
            next: None,
            more: false,
            bytes_read: 1,
            retirement_reported: false,
        };
        let back = pb::ScrubShardsResponse::decode(old.encode_to_vec().as_slice()).unwrap();
        assert!(!back.retirement_reported);
        assert_eq!(
            retirement(back.retirement_reported, back.shards[0].retired),
            None
        );
        assert_eq!(retirement(true, false), Some(false));
        assert_eq!(retirement(true, true), Some(true));
    }
}
