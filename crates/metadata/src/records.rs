//! Stored record types and their encoding.
//!
//! Encoding (prototype): one version byte followed by `postcard`. Postcard is
//! compact but not self-describing, so any field change requires a new
//! record version and a decoder for the old one. Whether the production
//! format should be protobuf instead is an RFC-003 question.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::MetaError;
use crate::kv::Value;

/// Record encoding version.
pub const RECORD_VERSION: u8 = 1;

/// Generation identifier: a UUID (version 7, time ordered) as raw bytes.
pub type GenId = [u8; 16];

/// Allocate a new generation ID.
#[must_use]
pub fn new_gen_id() -> GenId {
    *uuid::Uuid::now_v7().as_bytes()
}

/// Allocate a new S3 version ID string.
#[must_use]
pub fn new_version_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// The version ID of objects in buckets without versioning.
pub const NULL_VERSION_ID: &str = "null";

/// Encode a record.
///
/// # Errors
///
/// Returns [`MetaError::Codec`] if serialization fails.
pub fn encode<T: Serialize>(v: &T) -> Result<Value, MetaError> {
    postcard::to_extend(v, vec![RECORD_VERSION]).map_err(|e| MetaError::Codec(e.to_string()))
}

/// Decode a record.
///
/// # Errors
///
/// Returns [`MetaError::Codec`] on an unknown version or malformed bytes.
pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, MetaError> {
    match b.split_first() {
        Some((&RECORD_VERSION, rest)) => {
            postcard::from_bytes(rest).map_err(|e| MetaError::Codec(e.to_string()))
        }
        Some((v, _)) => Err(MetaError::Codec(format!("unknown record version {v}"))),
        None => Err(MetaError::Codec("empty record".into())),
    }
}

/// Bucket versioning state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Versioning {
    /// Never enabled. Overwrites replace the single `null` version.
    Disabled,
    /// Every write creates a new version; deletes create delete markers.
    Enabled,
    /// Writes replace the `null` version; earlier real versions are kept.
    Suspended,
}

/// Bucket configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRecord {
    /// Internal bucket ID.
    pub id: u64,
    /// Bucket name.
    pub name: String,
    /// Versioning state.
    pub versioning: Versioning,
    /// Creation time, Unix milliseconds.
    pub created_ms: u64,
}

/// The current version of an object key. Denormalizes what listing needs so
/// that `ListObjectsV2` does not read a header per key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentPointer {
    /// Generation holding the data (meaningless for delete markers).
    pub generation: GenId,
    /// S3 version ID (`null` when versioning is not enabled).
    pub version_id: String,
    /// Whether this current version is a delete marker.
    pub delete_marker: bool,
    /// Ordinal to use for the next version record of this key.
    pub next_ordinal: u64,
    /// Object size in bytes.
    pub size: u64,
    /// ETag as returned to clients.
    pub etag: String,
    /// Last-modified time, Unix milliseconds.
    pub last_modified_ms: u64,
}

/// One entry in a key's version history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    /// Generation (meaningless for delete markers).
    pub generation: GenId,
    /// S3 version ID.
    pub version_id: String,
    /// Whether this version is a delete marker.
    pub delete_marker: bool,
    /// Commit time, Unix milliseconds.
    pub committed_ms: u64,
    /// Object size.
    pub size: u64,
    /// ETag.
    pub etag: String,
}

/// Lifecycle state of a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenState {
    /// Being written. Reclaimable once the lease expires.
    Pending {
        /// Lease expiry, Unix milliseconds.
        lease_until_ms: u64,
    },
    /// Referenced by a current pointer or a version record.
    Committed,
    /// Unreferenced; chunks may be deleted after the grace period.
    Retiring {
        /// When it became unreferenced, Unix milliseconds.
        since_ms: u64,
    },
}

impl GenState {
    /// Short name for error messages.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Committed => "committed",
            Self::Retiring { .. } => "retiring",
        }
    }
}

/// One immutable generation of an object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationHeader {
    /// Lifecycle state.
    pub state: GenState,
    /// Owning bucket.
    pub bucket_id: u64,
    /// Object key.
    pub key: String,
    /// Total size in bytes.
    pub size: u64,
    /// ETag.
    pub etag: String,
    /// Content type, if set.
    pub content_type: Option<String>,
    /// User metadata (`x-amz-meta-*`), in request order.
    pub user_metadata: Vec<(String, String)>,
    /// Number of chunk records.
    pub chunk_count: u32,
    /// Creation time, Unix milliseconds.
    pub created_ms: u64,
}

/// Where a chunk's durable copy lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurablePlacement {
    /// Stripe group.
    pub group_id: u64,
    /// Group epoch at write time.
    pub epoch: u64,
}

/// One chunk of a generation: manifest entry plus placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRecord {
    /// Cluster-unique chunk ID.
    pub chunk_id: [u8; 16],
    /// Byte offset within the object.
    pub offset: u64,
    /// Length in bytes.
    pub len: u32,
    /// Erasure coding scheme identifier (see `arca-lake-ec`).
    pub scheme: String,
    /// Data shards.
    pub data_shards: u16,
    /// Parity shards.
    pub parity_shards: u16,
    /// CRC-32C of the chunk's bytes.
    pub crc32c: u32,
    /// Durable placement.
    pub placement: DurablePlacement,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_version_check() {
        let h = GenerationHeader {
            state: GenState::Pending { lease_until_ms: 5 },
            bucket_id: 1,
            key: "k".into(),
            size: 0,
            etag: String::new(),
            content_type: None,
            user_metadata: vec![("a".into(), "b".into())],
            chunk_count: 0,
            created_ms: 1,
        };
        let b = encode(&h).unwrap();
        assert_eq!(b[0], RECORD_VERSION);
        assert_eq!(decode::<GenerationHeader>(&b).unwrap(), h);
        assert!(decode::<GenerationHeader>(&[9, 0]).is_err());
        assert!(decode::<GenerationHeader>(&[]).is_err());
    }
}
