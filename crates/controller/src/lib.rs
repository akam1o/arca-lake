//! Control-plane logic: stripe-group bootstrap, garbage collection, and
//! member repair (RFC-004 §5.7, §5.10; RFC-008).
//!
//! Phase 1 status: the functions here are driven explicitly (by tests and
//! tooling). Leader election, scheduling, and the admin API come with the
//! controller process.

pub mod gc;
pub mod groups;
pub mod repair;

/// Errors from control-plane operations.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Metadata error.
    #[error(transparent)]
    Meta(#[from] arca_lake_metadata::MetaError),
    /// Placement error.
    #[error(transparent)]
    Placement(#[from] arca_lake_placement::PlacementError),
    /// Storage node RPC error.
    #[error("node {addr}: {status}")]
    Node {
        /// Node address.
        addr: String,
        /// Status.
        status: tonic::Status,
    },
    /// Not enough shards to reconstruct a chunk.
    #[error("chunk {chunk} of group {group} unrecoverable: {have} of {need} shards")]
    Unrecoverable {
        /// Group.
        group: u64,
        /// Chunk ID, hex.
        chunk: String,
        /// Shards available.
        have: usize,
        /// Shards required.
        need: usize,
    },
    /// The group's epoch changed under a running operation (another
    /// membership change); re-issue the operation against the new record.
    #[error(
        "group {group} epoch changed: expected {expected}, found {found} (0 = learned from a node's fence)"
    )]
    EpochChanged {
        /// Group.
        group: u64,
        /// Epoch the operation was working with.
        expected: u64,
        /// Epoch found afterwards (0 when reported by a node's fence).
        found: u64,
    },
    /// Anything else.
    #[error("{0}")]
    Other(String),
}
