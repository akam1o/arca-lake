//! Error type for the storage engine.

/// Errors returned by the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The shard is not present (never written, deleted, or lost to a torn write).
    #[error("shard not found")]
    NotFound,
    /// Data on disk failed validation. The caller must treat the shard as missing.
    #[error("corrupt data in segment {segment_id} at offset {offset}: {reason}")]
    Corrupt {
        /// Segment in which the corruption was found.
        segment_id: u64,
        /// Byte offset of the record.
        offset: u64,
        /// Human-readable reason.
        reason: &'static str,
    },
    /// The on-disk format is not one this build understands.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),
    /// The caller passed an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, StorageError>;
