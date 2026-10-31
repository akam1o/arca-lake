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
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, MetaError>;
