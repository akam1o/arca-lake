//! `S3` implementation over the cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arca_lake_common::EcProfile;
use arca_lake_ec::{CodeParams, ErasureCoder, SchemeId};
use arca_lake_metadata::ops::object::Precondition;
use arca_lake_metadata::ops::{bucket, list, object};
use arca_lake_metadata::records::Versioning;
use arca_lake_metadata::{KvStore, MetaError};
use arca_lake_protocol::client::NodeClients;
use s3s::dto::{
    Bucket, CommonPrefix, CreateBucketInput, CreateBucketOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
    PutObjectInput, PutObjectOutput, Timestamp,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

use super::map::ClusterMap;

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
    /// Per-RPC timeout to storage nodes.
    pub node_timeout: Duration,
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

/// Coders keyed by `(scheme, k, m)`.
type CoderCache = HashMap<(String, u16, u16), Arc<dyn ErasureCoder>>;

/// The distributed backend. Cloning shares all state (the `s3s` service
/// takes ownership of its backend, while tests and tooling keep a handle
/// for fault injection and map refresh).
pub struct ClusterBackend<S> {
    pub(super) meta: Arc<S>,
    pub(super) clients: NodeClients,
    pub(super) map: Arc<RwLock<Arc<ClusterMap>>>,
    pub(super) cfg: Arc<ClusterConfig>,
    coders: Arc<Mutex<CoderCache>>,
    fault: Arc<Mutex<Option<FaultPoint>>>,
}

impl<S> Clone for ClusterBackend<S> {
    fn clone(&self) -> Self {
        Self {
            meta: Arc::clone(&self.meta),
            clients: self.clients.clone(),
            map: Arc::clone(&self.map),
            cfg: Arc::clone(&self.cfg),
            coders: Arc::clone(&self.coders),
            fault: Arc::clone(&self.fault),
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
        Ok(Self {
            meta,
            clients: NodeClients::new(cfg.node_timeout),
            map: Arc::new(RwLock::new(Arc::new(map))),
            cfg: Arc::new(cfg),
            coders: Arc::new(Mutex::new(HashMap::new())),
            fault: Arc::new(Mutex::new(None)),
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

    pub(super) fn coder(
        &self,
        scheme: &str,
        k: u16,
        m: u16,
    ) -> Result<Arc<dyn ErasureCoder>, s3s::S3Error> {
        let key = (scheme.to_string(), k, m);
        if let Some(c) = self
            .coders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(Arc::clone(c));
        }
        let id = SchemeId::parse(scheme)
            .ok_or_else(|| s3_error!(InternalError, "unknown EC scheme {scheme}"))?;
        let params = CodeParams::new(usize::from(k), usize::from(m))
            .map_err(|e| s3_error!(InternalError, "bad EC params: {e}"))?;
        let coder: Arc<dyn ErasureCoder> = Arc::from(
            arca_lake_ec::new_coder(id, params).map_err(|e| s3_error!(InternalError, "{e}"))?,
        );
        self.coders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, Arc::clone(&coder));
        Ok(coder)
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
