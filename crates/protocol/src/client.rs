//! Convenience client for the storage node API: lazily connected channels
//! per address and one method per RPC.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

use crate::storage::v1 as pb;
use crate::storage::v1::storage_node_client::StorageNodeClient;

/// Shared pool of channels keyed by node address.
#[derive(Debug, Clone, Default)]
pub struct NodeClients {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    /// Per-RPC timeout.
    timeout: Option<Duration>,
}

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

impl ShardRef {
    fn pb(&self) -> pb::ShardKey {
        pb::ShardKey {
            group_id: self.group_id,
            chunk_id: Bytes::copy_from_slice(&self.chunk_id),
            shard_index: u32::from(self.shard_index),
        }
    }
}

impl NodeClients {
    /// Pool with a per-RPC timeout (connect timeout is a fifth of it).
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            channels: Arc::default(),
            timeout: Some(timeout),
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
        let mut ep = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
            .tcp_nodelay(true);
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
        let mut client = self.client(addr)?;
        let req = pb::DeleteShardRequest {
            protocol_version: pb::PROTOCOL_VERSION,
            key: Some(shard.pb()),
            group_epoch: epoch,
        };
        client.delete_shard(req).await?;
        Ok(())
    }

    /// All shards of a group on the node.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn list_shards(
        &self,
        addr: &str,
        group_id: u64,
    ) -> Result<Vec<ShardRef>, tonic::Status> {
        let mut client = self.client(addr)?;
        let req = pb::ListShardsRequest {
            protocol_version: pb::PROTOCOL_VERSION,
            group_id,
        };
        let resp = client.list_shards(req).await?.into_inner();
        Ok(resp
            .keys
            .into_iter()
            .filter_map(|k| {
                Some(ShardRef {
                    group_id: k.group_id,
                    chunk_id: k.chunk_id.as_ref().try_into().ok()?,
                    shard_index: u16::try_from(k.shard_index).ok()?,
                })
            })
            .collect())
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
        let mut client = self.client(addr)?;
        let req = pb::UpdateGroupEpochRequest {
            protocol_version: pb::PROTOCOL_VERSION,
            group_id,
            epoch,
        };
        Ok(client.update_group_epoch(req).await?.into_inner().epoch)
    }

    /// Node health and capacity.
    ///
    /// # Errors
    ///
    /// RPC errors.
    pub async fn health(&self, addr: &str) -> Result<pb::HealthResponse, tonic::Status> {
        let mut client = self.client(addr)?;
        Ok(client.health(pb::HealthRequest {}).await?.into_inner())
    }
}
