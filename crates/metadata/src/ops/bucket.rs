//! Bucket operations.

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{bucket_by_name, write};
use crate::records::{BucketRecord, Versioning, decode};
use crate::{MetaError, keys, transact};

/// Create a bucket.
///
/// # Errors
///
/// [`MetaError::BucketAlreadyExists`] if the name is taken.
pub async fn create_bucket<S: KvStore>(
    store: &S,
    name: &str,
    versioning: Versioning,
    now_ms: u64,
) -> Result<BucketRecord> {
    create_bucket_owned(store, name, versioning, "", now_ms).await
}

/// Create a bucket owned by `owner` (a principal name, RFC-012 §5.3; empty
/// means no owner).
///
/// # Errors
///
/// [`MetaError::BucketAlreadyExists`] if the name is taken.
pub async fn create_bucket_owned<S: KvStore>(
    store: &S,
    name: &str,
    versioning: Versioning,
    owner: &str,
    now_ms: u64,
) -> Result<BucketRecord> {
    if name.is_empty() {
        return Err(MetaError::InvalidArgument("empty bucket name".into()));
    }
    transact(store, (name, versioning, owner, now_ms), |tx, env| {
        Box::pin(async move {
            let (name, versioning, owner, now_ms) = *env;
            if tx.get(&keys::bucket_by_name(name)).await?.is_some() {
                return Err(MetaError::BucketAlreadyExists);
            }
            let next = tx
                .get(&keys::bucket_counter())
                .await?
                .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
                .map_or(1, |b| u64::from_le_bytes(b) + 1);
            tx.set(&keys::bucket_counter(), &next.to_le_bytes());
            let record = BucketRecord {
                id: next,
                name: name.to_string(),
                versioning,
                created_ms: now_ms,
                owner: owner.to_string(),
                policy: None,
            };
            write(tx, &keys::bucket_by_name(name), &record)?;
            write(tx, &keys::bucket_by_id(next), &record)?;
            Ok(record)
        })
    })
    .await
}

/// Look up a bucket.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`] if it does not exist.
pub async fn get_bucket<S: KvStore>(store: &S, name: &str) -> Result<BucketRecord> {
    transact(store, name, |tx, name| {
        Box::pin(async move { bucket_by_name(tx, name).await })
    })
    .await
}

/// Change the versioning state.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`] if it does not exist.
pub async fn set_versioning<S: KvStore>(
    store: &S,
    name: &str,
    versioning: Versioning,
) -> Result<()> {
    transact(store, (name, versioning), |tx, env| {
        Box::pin(async move {
            let (name, versioning) = *env;
            let mut record = bucket_by_name(tx, name).await?;
            record.versioning = versioning;
            write(tx, &keys::bucket_by_name(name), &record)?;
            write(tx, &keys::bucket_by_id(record.id), &record)?;
            Ok(())
        })
    })
    .await
}

/// Replace the bucket policy document (`None` deletes it). The document
/// must already be validated by the caller (RFC-012 §5.3.3).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn set_policy<S: KvStore>(store: &S, name: &str, policy: Option<&str>) -> Result<()> {
    transact(store, (name, policy), |tx, env| {
        Box::pin(async move {
            let (name, policy) = *env;
            let mut record = bucket_by_name(tx, name).await?;
            record.policy = policy.map(str::to_string);
            write(tx, &keys::bucket_by_name(name), &record)?;
            write(tx, &keys::bucket_by_id(record.id), &record)?;
            Ok(())
        })
    })
    .await
}

/// Change the bucket owner (operator tooling, RFC-012 §5.3.1).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn set_owner<S: KvStore>(store: &S, name: &str, owner: &str) -> Result<()> {
    transact(store, (name, owner), |tx, env| {
        Box::pin(async move {
            let (name, owner) = *env;
            let mut record = bucket_by_name(tx, name).await?;
            record.owner = owner.to_string();
            write(tx, &keys::bucket_by_name(name), &record)?;
            write(tx, &keys::bucket_by_id(record.id), &record)?;
            Ok(())
        })
    })
    .await
}

/// Delete an empty bucket.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`] or [`MetaError::BucketNotEmpty`]. A bucket
/// with delete markers or non-current versions still counts as non-empty.
pub async fn delete_bucket<S: KvStore>(store: &S, name: &str) -> Result<()> {
    transact(store, name, |tx, name| {
        Box::pin(async move {
            let record = bucket_by_name(tx, name).await?;
            let objects = tx
                .get_range(&keys::object_prefix_range(record.id, ""), 1, false)
                .await?;
            if !objects.is_empty() {
                return Err(MetaError::BucketNotEmpty);
            }
            let versions = tx
                .get_range(
                    &KeyRange::prefix(&foundationdb_tuple::pack(&("al", "v", record.id))),
                    1,
                    false,
                )
                .await?;
            if !versions.is_empty() {
                return Err(MetaError::BucketNotEmpty);
            }
            let uploads = tx
                .get_range(&keys::mpu_prefix_range(record.id, ""), 1, false)
                .await?;
            if !uploads.is_empty() {
                return Err(MetaError::BucketNotEmpty);
            }
            tx.clear(&keys::bucket_by_name(name));
            tx.clear(&keys::bucket_by_id(record.id));
            Ok(())
        })
    })
    .await
}

/// List bucket records.
///
/// # Errors
///
/// Backend errors only.
pub async fn list_buckets<S: KvStore>(store: &S) -> Result<Vec<BucketRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let range = KeyRange::prefix(&foundationdb_tuple::pack(&("al", "b")));
            let mut out = Vec::new();
            for (_, v) in tx.get_range(&range, usize::MAX, false).await? {
                out.push(decode::<BucketRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}
