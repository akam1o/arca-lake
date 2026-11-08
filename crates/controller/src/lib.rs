//! Control-plane logic: stripe-group bootstrap, garbage collection, and
//! member repair (RFC-004 §5.7, §5.10; RFC-008).
//!
//! Phase 1 status: the functions here are driven explicitly (by tests and
//! tooling). Leader election, scheduling, and the admin API come with the
//! controller process.

pub mod adaptive;
pub mod admission;
pub mod backpressure;
pub mod backup;
pub mod capacity;
pub mod delete_fence;
pub mod gc;
pub mod groups;
pub mod integrity;
pub mod metrics;
pub mod objects;
pub mod plan;
pub mod process;
pub mod rebalance;
pub mod recovery;
pub mod repair;
pub mod tiering;
pub mod upgrade;
pub mod walk;

/// What this controller build reads and writes on every axis (RFC-014 §5.1).
#[must_use]
pub fn capabilities() -> arca_lake_common::compat::Capabilities {
    use arca_lake_metadata::keys::{SCHEMA_VERSION, SCHEMA_VERSION_MIN};
    use arca_lake_protocol::storage::v1::{PROTOCOL_VERSION, PROTOCOL_VERSION_MIN};
    arca_lake_common::compat::Capabilities {
        protocol_min: PROTOCOL_VERSION_MIN,
        protocol_max: PROTOCOL_VERSION,
        schema_min: SCHEMA_VERSION_MIN,
        schema_max: SCHEMA_VERSION,
        // Controllers hold no segments; the format axis is the storage
        // crate this binary was built with (what `admin init` stamps and
        // what finalize targets).
        format_min: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION_MIN),
        format_max: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
    }
}

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
    /// Reconstructed or stored bytes do not match what metadata or the
    /// writer recorded; nothing was written.
    #[error("integrity: {0}")]
    Corrupt(String),
    /// Anything else.
    #[error("{0}")]
    Other(String),
}
