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
    /// `(shard, payload length)` in key order.
    pub entries: Vec<(ShardRef, u64)>,
    /// Cursor for the next page, if any.
    pub next: Option<ShardRef>,
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
}

/// A fault hook.
pub type FaultHook = Arc<dyn Fn(&RpcContext<'_>) -> FaultAction + Send + Sync>;

/// HTTP/2 initial stream window for shard transfers.
pub const STREAM_WINDOW: u32 = 8 << 20;
/// HTTP/2 initial connection window for shard transfers.
pub const CONNECTION_WINDOW: u32 = 32 << 20;

/// Shared pool of channels keyed by node address.
#[derive(Clone, Default)]
pub struct NodeClients {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    /// Per-RPC timeout.
    timeout: Option<Duration>,
    hook: Arc<Mutex<Option<FaultHook>>>,
}

impl std::fmt::Debug for NodeClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClients")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl NodeClients {
    /// Pool with a per-RPC timeout (connect timeout is a fifth of it).
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            channels: Arc::default(),
            timeout: Some(timeout),
            hook: Arc::default(),
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

    /// Run `call` under the fault hook's decision.
    async fn with_faults<T, Fut>(
        &self,
        ctx: RpcContext<'_>,
        call: impl Fn() -> Fut,
    ) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        match self.action(&ctx) {
            FaultAction::None => call().await,
            FaultAction::DelayBefore(d) => {
                tokio::time::sleep(d).await;
                call().await
            }
            FaultAction::FailBefore(code, msg) => Err(tonic::Status::new(code, msg)),
            FaultAction::FailAfter(code, msg) => {
                let _ = call().await;
                Err(tonic::Status::new(code, msg))
            }
            FaultAction::DelayAfter(d) => {
                let r = call().await;
                tokio::time::sleep(d).await;
                r
            }
            FaultAction::Duplicate => {
                let _ = call().await;
                call().await
            }
            FaultAction::Reset => {
                self.forget(ctx.addr);
                Err(tonic::Status::unavailable("connection reset"))
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
        // 64 round trips (RFC-009 §7, namespace fault runs).
        let mut ep = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
            .tcp_nodelay(true)
            .initial_stream_window_size(Some(STREAM_WINDOW))
            .initial_connection_window_size(Some(CONNECTION_WINDOW));
        if let Some(t) = self.timeout {
            ep = ep.timeout(t).connect_timeout(t / 5);
        }
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
        self.with_faults(ctx, || {
            self.put_shard_raw(addr, shard, epoch, data.clone(), frame_bytes)
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
    ) -> Result<pb::PutShardResponse, tonic::Status> {
        let mut client = self.client(addr)?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let header = pb::PutShardRequest {
            body: Some(pb::put_shard_request::Body::Header(pb::PutShardHeader {
                protocol_version: pb::PROTOCOL_VERSION,
                key: Some(shard.pb()),
                group_epoch: epoch,
                payload_len: data.len() as u64,
            })),
        };
        let frame = frame_bytes.max(1);
        tokio::spawn(async move {
            if tx.send(header).await.is_err() {
                return;
            }
            let mut off = 0;
            while off < data.len() {
                let end = (off + frame).min(data.len());
                let msg = pb::PutShardRequest {
                    body: Some(pb::put_shard_request::Body::Data(data.slice(off..end))),
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
                off = end;
            }
        });
        let resp = client
            .put_shard(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?;
        Ok(resp.into_inner())
    }

    /// Read a shard or a byte range of it (`len == 0` means to the end).
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
        let ctx = RpcContext {
            addr,
            rpc: Rpc::GetShard,
            shard: Some(shard),
            group_id: Some(shard.group_id),
        };
        self.with_faults(ctx, || async {
            let mut client = self.client(addr)?;
            let req = pb::GetShardRequest {
                protocol_version: pb::PROTOCOL_VERSION,
                key: Some(shard.pb()),
                offset,
                len,
            };
            let mut stream = client.get_shard(req).await?.into_inner();
            let mut out = bytes::BytesMut::new();
            while let Some(msg) = stream.next().await {
                out.extend_from_slice(&msg?.data);
            }
            Ok(out.freeze())
        })
        .await
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
        self.with_faults(ctx, || async {
            let mut client = self.client(addr)?;
            let req = pb::DeleteShardRequest {
                protocol_version: pb::PROTOCOL_VERSION,
                key: Some(shard.pb()),
                group_epoch: epoch,
            };
            client.delete_shard(req).await?;
            Ok(())
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
        let ctx = RpcContext {
            addr,
            rpc: Rpc::ListShards,
            shard: None,
            group_id: Some(group_id),
        };
        self.with_faults(ctx, || async {
            let mut client = self.client(addr)?;
            let req = pb::ListShardsRequest {
                protocol_version: pb::PROTOCOL_VERSION,
                group_id,
                after: after.map(|s| s.pb()),
                limit,
            };
            let resp = client.list_shards(req).await?.into_inner();
            Ok(ShardPage {
                entries: resp
                    .entries
                    .into_iter()
                    .filter_map(|e| Some((ShardRef::from_pb(&e.key?)?, e.payload_len)))
                    .collect(),
                next: resp
                    .next
                    .as_ref()
                    .and_then(ShardRef::from_pb)
                    .filter(|_| resp.more),
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
        self.with_faults(ctx, || async {
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
        self.with_faults(ctx, || async {
            let mut client = self.client(addr)?;
            let req = pb::UpdateGroupEpochRequest {
                protocol_version: pb::PROTOCOL_VERSION,
                group_id,
                epoch,
            };
            Ok(client.update_group_epoch(req).await?.into_inner().epoch)
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
        self.with_faults(ctx, || async {
            let mut client = self.client(addr)?;
            Ok(client.health(pb::HealthRequest {}).await?.into_inner())
        })
        .await
    }
}
