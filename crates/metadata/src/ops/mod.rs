//! Transaction recipes: the metadata half of the protocols in the
//! architecture documents (DD-4 write protocol, DD-7 garbage collection).
//!
//! Every function here runs one or more transactions through
//! [`crate::transact`] and is safe to retry. Wall-clock inputs are explicit
//! `now_ms` parameters so that tests control time and so that the recipes
//! never read the clock themselves.

pub mod bucket;
pub mod cache;
pub mod cluster;
pub mod gc;
pub mod leader;
pub mod list;
pub mod object;

use crate::error::Result;
use crate::kv::Txn;
use crate::records::{BucketRecord, CurrentPointer, GenerationHeader, decode, encode};
use crate::{MetaError, keys};

/// Read and decode a record, if present.
async fn read<T: serde::de::DeserializeOwned, X: Txn>(tx: &mut X, key: &[u8]) -> Result<Option<T>> {
    match tx.get(key).await? {
        Some(v) => Ok(Some(decode(&v)?)),
        None => Ok(None),
    }
}

/// Encode and write a record.
fn write<T: serde::Serialize, X: Txn>(tx: &mut X, key: &[u8], value: &T) -> Result<()> {
    tx.set(key, &encode(value)?);
    Ok(())
}

async fn bucket_by_name<X: Txn>(tx: &mut X, name: &str) -> Result<BucketRecord> {
    read(tx, &keys::bucket_by_name(name))
        .await?
        .ok_or(MetaError::NoSuchBucket)
}

async fn bucket_by_id<X: Txn>(tx: &mut X, id: u64) -> Result<BucketRecord> {
    read(tx, &keys::bucket_by_id(id))
        .await?
        .ok_or(MetaError::NoSuchBucket)
}

async fn current_pointer<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
) -> Result<Option<CurrentPointer>> {
    read(tx, &keys::object_current(bucket_id, key)).await
}

async fn generation_header<X: Txn>(
    tx: &mut X,
    generation: &crate::records::GenId,
) -> Result<GenerationHeader> {
    read(tx, &keys::gen_header(generation))
        .await?
        .ok_or(MetaError::NoSuchGeneration)
}
