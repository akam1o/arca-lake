//! Bucket operations.

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{bucket_by_name, read, write};
use crate::records::{BucketRecord, Versioning};
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
    if name.is_empty() {
        return Err(MetaError::InvalidArgument("empty bucket name".into()));
    }
    transact(store, async |tx| {
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
        };
        write(tx, &keys::bucket_by_name(name), &record)?;
        write(tx, &keys::bucket_by_id(next), &record)?;
        Ok(record)
    })
    .await
}

/// Look up a bucket.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`] if it does not exist.
pub async fn get_bucket<S: KvStore>(store: &S, name: &str) -> Result<BucketRecord> {
    transact(store, async |tx| bucket_by_name(tx, name).await).await
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
    transact(store, async |tx| {
        let mut record = bucket_by_name(tx, name).await?;
        record.versioning = versioning;
        write(tx, &keys::bucket_by_name(name), &record)?;
        write(tx, &keys::bucket_by_id(record.id), &record)?;
        Ok(())
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
    transact(store, async |tx| {
        let record = bucket_by_name(tx, name).await?;
        let objects = tx
            .get_range(&keys::object_prefix_range(record.id, ""), 1, false)
            .await?;
        if !objects.is_empty() {
            return Err(MetaError::BucketNotEmpty);
        }
        let versions = tx
            .get_range(
                &crate::kv::KeyRange::prefix(&foundationdb_tuple::pack(&("al", "v", record.id))),
                1,
                false,
            )
            .await?;
        if !versions.is_empty() {
            return Err(MetaError::BucketNotEmpty);
        }
        tx.clear(&keys::bucket_by_name(name));
        tx.clear(&keys::bucket_by_id(record.id));
        Ok(())
    })
    .await
}

/// List bucket records.
///
/// # Errors
///
/// Backend errors only.
pub async fn list_buckets<S: KvStore>(store: &S) -> Result<Vec<BucketRecord>> {
    transact(store, async |tx| {
        let range = crate::kv::KeyRange::prefix(&foundationdb_tuple::pack(&("al", "b")));
        let mut out = Vec::new();
        for (_, v) in tx.get_range(&range, usize::MAX, false).await? {
            out.push(crate::records::decode::<BucketRecord>(&v)?);
        }
        Ok(out)
    })
    .await
}

#[allow(dead_code)]
async fn exists<X: Txn>(tx: &mut X, name: &str) -> Result<bool> {
    Ok(read::<BucketRecord, _>(tx, &keys::bucket_by_name(name))
        .await?
        .is_some())
}
