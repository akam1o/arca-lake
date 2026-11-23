//! In-memory S3 backend for protocol experiments and tests.
//!
//! Implements the initial API subset: bucket create/delete/head/list,
//! `PutObject` (with `If-None-Match: *` and `If-Match`), `GetObject` with
//! ranges, `HeadObject`, `DeleteObject`, and `ListObjectsV2` with prefix,
//! delimiter, `max-keys`, `start-after`, and continuation tokens. Objects
//! are held in memory; nothing here is durable.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use s3s::crypto::{Checksum as _, Md5};
use s3s::dto::{
    Bucket, CommonPrefix, CreateBucketInput, CreateBucketOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
    PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    last_modified: SystemTime,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

#[derive(Debug)]
struct StoredBucket {
    created: SystemTime,
    objects: BTreeMap<String, StoredObject>,
}

/// In-memory backend. Cheap to clone; clones share state.
#[derive(Debug, Clone, Default)]
pub struct MemoryBackend {
    buckets: Arc<RwLock<HashMap<String, StoredBucket>>>,
}

impl MemoryBackend {
    /// Empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of objects across all buckets.
    #[must_use]
    pub fn object_count(&self) -> usize {
        self.buckets
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|b| b.objects.len())
            .sum()
    }
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

fn to_i64(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

#[async_trait::async_trait]
impl S3 for MemoryBackend {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let mut buckets = self.buckets.write().unwrap_or_else(PoisonError::into_inner);
        if buckets.contains_key(&req.input.bucket) {
            return Err(s3_error!(BucketAlreadyExists));
        }
        buckets.insert(
            req.input.bucket,
            StoredBucket {
                created: SystemTime::now(),
                objects: BTreeMap::new(),
            },
        );
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let mut buckets = self.buckets.write().unwrap_or_else(PoisonError::into_inner);
        let Some(bucket) = buckets.get(&req.input.bucket) else {
            return Err(s3_error!(NoSuchBucket));
        };
        if !bucket.objects.is_empty() {
            return Err(s3_error!(BucketNotEmpty));
        }
        buckets.remove(&req.input.bucket);
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
        if !buckets.contains_key(&req.input.bucket) {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
        let mut names: Vec<(&String, &StoredBucket)> = buckets.iter().collect();
        names.sort_by(|a, b| a.0.cmp(b.0));
        let list = names
            .into_iter()
            .map(|(name, b)| Bucket {
                name: Some(name.clone()),
                creation_date: Some(Timestamp::from(b.created)),
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
        let Some(mut body) = input.body else {
            return Err(s3_error!(IncompleteBody));
        };

        // Read the whole body (this backend is not streaming by design) and
        // compute the ETag on the way.
        let mut buf = BytesMut::new();
        let mut md5 = Md5::new();
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|e| s3_error!(InternalError, "body read failed: {e}"))?
        {
            md5.update(&chunk);
            buf.extend_from_slice(&chunk);
        }
        let digest = md5.finalize();
        let content_md5 = crate::checksum::content_md5(input.content_md5.as_deref())?;
        crate::checksum::verify_content_md5(content_md5.as_ref(), digest.as_ref())?;
        let etag = hex(digest.as_ref());

        let mut buckets = self.buckets.write().unwrap_or_else(PoisonError::into_inner);
        let Some(bucket) = buckets.get_mut(&input.bucket) else {
            return Err(s3_error!(NoSuchBucket));
        };

        // Conditional writes are evaluated under the same lock as the write,
        // which is the in-memory equivalent of the commit transaction.
        let existing = bucket.objects.get(&input.key);
        if let Some(cond) = &input.if_none_match {
            if !cond.is_any() {
                return Err(s3_error!(
                    NotImplemented,
                    "If-None-Match with an ETag is not supported"
                ));
            }
            if existing.is_some() {
                return Err(s3_error!(PreconditionFailed));
            }
        }
        if let Some(cond) = &input.if_match {
            let Some(current) = existing else {
                return Err(s3_error!(PreconditionFailed));
            };
            let matches = match cond.as_etag() {
                None => cond.is_any(),
                Some(ETag::Strong(s)) => *s == current.etag,
                Some(_) => false, // weak comparison never satisfies If-Match
            };
            if !matches {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        bucket.objects.insert(
            input.key,
            StoredObject {
                data: buf.freeze(),
                etag: etag.clone(),
                last_modified: SystemTime::now(),
                content_type: input.content_type,
                metadata: input.metadata,
            },
        );
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let object = {
            let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
            let Some(bucket) = buckets.get(&input.bucket) else {
                return Err(s3_error!(NoSuchBucket));
            };
            bucket
                .objects
                .get(&input.key)
                .cloned()
                .ok_or_else(|| s3_error!(NoSuchKey))?
        };
        let total = object.data.len() as u64;
        let (data, content_range) = match input.range {
            None => (object.data.clone(), None),
            Some(range) => {
                let r = range.check(total)?;
                let start = usize::try_from(r.start).map_err(|_| s3_error!(InvalidRange))?;
                let end = usize::try_from(r.end).map_err(|_| s3_error!(InvalidRange))?;
                (
                    object.data.slice(start..end),
                    Some(format!("bytes {}-{}/{total}", r.start, r.end - 1)),
                )
            }
        };
        let output = GetObjectOutput {
            content_length: Some(to_i64(data.len())),
            content_range,
            body: Some(StreamingBlob::from_bytes(data)),
            e_tag: Some(ETag::Strong(object.etag)),
            last_modified: Some(Timestamp::from(object.last_modified)),
            content_type: object.content_type,
            metadata: object.metadata,
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
        let Some(bucket) = buckets.get(&input.bucket) else {
            return Err(s3_error!(NoSuchBucket));
        };
        let object = bucket
            .objects
            .get(&input.key)
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let output = HeadObjectOutput {
            content_length: Some(to_i64(object.data.len())),
            e_tag: Some(ETag::Strong(object.etag.clone())),
            last_modified: Some(Timestamp::from(object.last_modified)),
            content_type: object.content_type.clone(),
            metadata: object.metadata.clone(),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let mut buckets = self.buckets.write().unwrap_or_else(PoisonError::into_inner);
        let Some(bucket) = buckets.get_mut(&input.bucket) else {
            return Err(s3_error!(NoSuchBucket));
        };
        bucket.objects.remove(&input.key);
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let buckets = self.buckets.read().unwrap_or_else(PoisonError::into_inner);
        let Some(bucket) = buckets.get(&input.bucket) else {
            return Err(s3_error!(NoSuchBucket));
        };
        let prefix = input.prefix.clone().unwrap_or_default();
        let delimiter = input.delimiter.clone().filter(|d| !d.is_empty());
        let max_keys = usize::try_from(input.max_keys.unwrap_or(1000))
            .unwrap_or(1000)
            .max(1);
        let after: Option<&str> = match (&input.continuation_token, &input.start_after) {
            (Some(ct), Some(sa)) => Some(if ct >= sa { ct } else { sa }),
            (Some(ct), None) => Some(ct),
            (None, Some(sa)) => Some(sa),
            (None, None) => None,
        };

        let mut contents = Vec::new();
        let mut common: Vec<String> = Vec::new();
        let mut is_truncated = false;
        let mut next_token = None;
        for (key, obj) in bucket.objects.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some_and(|a| key.as_str() <= a) {
                continue;
            }
            // Keys under an already emitted common prefix are skipped.
            if let Some(cp) = common.last()
                && key.starts_with(cp.as_str())
            {
                continue;
            }
            if contents.len() + common.len() >= max_keys {
                is_truncated = true;
                break;
            }
            if let Some(d) = &delimiter
                && let Some(pos) = key[prefix.len()..].find(d.as_str())
            {
                let cp = key[..prefix.len() + pos + d.len()].to_string();
                next_token = Some(cp.clone());
                common.push(cp);
                continue;
            }
            next_token = Some(key.clone());
            contents.push(Object {
                key: Some(key.clone()),
                size: Some(to_i64(obj.data.len())),
                e_tag: Some(ETag::Strong(obj.etag.clone())),
                last_modified: Some(Timestamp::from(obj.last_modified)),
                ..Default::default()
            });
        }
        let key_count = i32::try_from(contents.len() + common.len()).unwrap_or(i32::MAX);
        // A common prefix as a continuation token must skip every key under
        // it: a token ending with the delimiter is treated as "after all keys
        // with this prefix" by advancing the last byte on the next request.
        let next_continuation_token = if is_truncated {
            next_token.map(|t| match &delimiter {
                Some(d) if t.ends_with(d.as_str()) => {
                    let mut bytes = t.into_bytes();
                    if let Some(last) = bytes.last_mut() {
                        *last = last.saturating_add(1);
                    }
                    String::from_utf8_lossy(&bytes).into_owned()
                }
                _ => t,
            })
        } else {
            None
        };
        let output = ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: input.max_keys,
            key_count: Some(key_count),
            is_truncated: Some(is_truncated),
            contents: (!contents.is_empty()).then_some(contents),
            common_prefixes: (!common.is_empty()).then(|| {
                common
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect()
            }),
            continuation_token: input.continuation_token,
            next_continuation_token,
            start_after: input.start_after,
            encoding_type: input.encoding_type,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
