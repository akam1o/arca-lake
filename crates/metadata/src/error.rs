//! Error type for the metadata layer.

use crate::kv::KvError;

/// Errors from metadata operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetaError {
    /// Error from the key-value backend.
    #[error(transparent)]
    Kv(#[from] KvError),
    /// A transaction conflicted too many times in a row.
    #[error("transaction retried too many times")]
    TooManyRetries,
    /// The bucket does not exist.
    #[error("no such bucket")]
    NoSuchBucket,
    /// A bucket with that name already exists.
    #[error("bucket already exists")]
    BucketAlreadyExists,
    /// The bucket still contains objects.
    #[error("bucket is not empty")]
    BucketNotEmpty,
    /// The object key has no current version.
    #[error("no such key")]
    NoSuchKey,
    /// The generation does not exist.
    #[error("no such generation")]
    NoSuchGeneration,
    /// The stripe group does not exist.
    #[error("no such stripe group {0}")]
    NoSuchGroup(u64),
    /// The generation is not in the state the operation requires.
    #[error("generation is {0}, operation requires {1}")]
    WrongGenerationState(&'static str, &'static str),
    /// The pending generation's lease has expired; it may already be retired.
    #[error("generation lease expired")]
    LeaseExpired,
    /// A conditional request's precondition did not hold.
    #[error("precondition failed")]
    PreconditionFailed,
    /// Caller error.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A stored record could not be decoded.
    #[error("record codec error: {0}")]
    Codec(String),
    /// The multipart upload does not exist or is no longer open.
    #[error("no such upload")]
    NoSuchUpload,
    /// The version does not exist.
    #[error("no such version")]
    NoSuchVersion,
    /// A listed part does not exist or its ETag does not match.
    #[error("invalid part {0}")]
    InvalidPart(u32),
    /// Parts are not in ascending order.
    #[error("invalid part order")]
    InvalidPartOrder,
    /// A part other than the last is smaller than the minimum part size.
    #[error("part {0} too small")]
    EntityTooSmall(u32),
    /// The metadata schema version is outside this build's range.
    #[error("metadata schema {found} requires software with schema in [{min}, {max}]")]
    SchemaMismatch {
        /// Version found in the store.
        found: u64,
        /// Oldest version this build serves.
        min: u64,
        /// Newest version this build serves.
        max: u64,
    },
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, MetaError>;
