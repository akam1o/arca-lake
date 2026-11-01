//! gRPC service implementation.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

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
}

impl NodeConfig {
    /// Defaults for one device directory.
    #[must_use]
    pub fn new(listen: SocketAddr, dir: impl Into<PathBuf>) -> Self {
        Self {
            listen,
            store: StoreConfig::new(dir),
            frame_bytes: 256 << 10,
        }
    }
}

/// The service state shared by all requests.
#[derive(Debug)]
pub struct StorageNodeService {
    store: Arc<Store>,
    epochs: Mutex<EpochTable>,
    frame_bytes: usize,
}

impl StorageNodeService {
    /// Open the store and the epoch table.
    ///
    /// # Errors
    ///
    /// Storage or I/O errors while opening.
    pub fn open(cfg: &NodeConfig) -> Result<Self, StorageError> {
        let (store, _report) = Store::open(cfg.store.clone())?;
        let epochs = EpochTable::open(&cfg.store.dir);
        Ok(Self {
            store: Arc::new(store),
            epochs: Mutex::new(epochs),
            frame_bytes: cfg.frame_bytes,
        })
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
impl StorageNode for StorageNodeService {
    type GetShardStream = ReceiverStream<Result<pb::GetShardResponse, tonic::Status>>;

    async fn put_shard(
        &self,
        request: tonic::Request<tonic::Streaming<pb::PutShardRequest>>,
    ) -> Result<tonic::Response<pb::PutShardResponse>, tonic::Status> {
        let mut stream = request.into_inner();
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
        while let Some(msg) = stream.next().await {
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
        let crc = crc32c_of(&payload);
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.put(key, &payload))
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .map_err(Self::storage_status)?;
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
            .map_err(Self::storage_status)?;
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
        let page = self
            .store
            .entries_in_group_after(req.group_id, after, limit);
        let more = page.len() == limit;
        let next = more
            .then(|| page.last().map(|(k, _)| Self::pb_key(k)))
            .flatten();
        let entries = page
            .into_iter()
            .map(|(k, len)| pb::ShardEntry {
                key: Some(Self::pb_key(&k)),
                payload_len: u64::from(len),
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
        let verified = tokio::task::spawn_blocking(move || {
            page.into_iter()
                .map(|(key, len)| match store.get(&key, None) {
                    Ok(data) => pb::ShardVerification {
                        key: Some(Self::pb_key(&key)),
                        payload_len: data.len() as u64,
                        crc32c: crc32c_of(&data),
                        ok: true,
                    },
                    Err(_) => pb::ShardVerification {
                        key: Some(Self::pb_key(&key)),
                        payload_len: u64::from(len),
                        crc32c: 0,
                        ok: false,
                    },
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(pb::VerifyShardsResponse {
            shards: verified,
            next,
            more,
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
        Ok(tonic::Response::new(pb::HealthResponse {
            live_shards: stats.live_shards as u64,
            live_bytes: stats.live_bytes,
            segment_bytes: stats.segment_bytes,
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
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .initial_stream_window_size(Some(arca_lake_protocol::client::STREAM_WINDOW))
            .initial_connection_window_size(Some(arca_lake_protocol::client::CONNECTION_WINDOW))
            .tcp_nodelay(true)
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
