//! Tiering (RFC-006): the read-through cache (population, cached reads,
//! invalidation, eviction, reconciliation, prefetch and pinning), reading a
//! chunk from its durable placement, and migration of durable placements
//! between tiers.
//!
//! Everything here is a library used by the gateway (read path) and the
//! controller (schedulers). Nothing here can change an object's version:
//! cache records are discardable and migration only moves a chunk's durable
//! placement after the target is verified.

pub mod cache;
pub mod durable;
pub mod migrate;

/// Errors of the tiering operations.
#[derive(Debug, thiserror::Error)]
pub enum TierError {
    /// Metadata error.
    #[error(transparent)]
    Meta(#[from] arca_lake_metadata::MetaError),
    /// Storage node RPC error.
    #[error("node {addr}: {status}")]
    Node {
        /// Node address.
        addr: String,
        /// Status.
        status: tonic::Status,
    },
    /// Bytes did not match the recorded checksum or length.
    #[error("integrity: {0}")]
    Corrupt(String),
    /// Fewer than `k` shards were readable.
    #[error("chunk unreadable: {have} of {need} shards")]
    Unreadable {
        /// Shards obtained.
        have: usize,
        /// Shards required.
        need: usize,
    },
    /// Anything else.
    #[error("{0}")]
    Other(String),
}
