//! Storage node: the gRPC service around one device's storage engine.
//!
//! The node knows nothing about objects. It stores shards keyed by
//! `(group, chunk_id, shard_index)`, persists the highest placement epoch it
//! has been told per stripe group, and rejects writes and deletes carrying
//! an older epoch (fencing, RFC-004 §5.4). Every acknowledgment means the
//! storage engine has made the record durable.

pub mod capacity;
mod cluster;
pub mod compactor;
pub mod domain;
mod epochs;
#[cfg(feature = "fault-injection")]
mod fault_control;
pub mod load;
mod service;

pub use arca_lake_storage::{ShardKey, Store, StoreTimingsSnapshot};
pub use service::{
    CapacityBreakdown, NodeConfig, NodeHandle, NodeStats, NodeStatsSnapshot, StorageNodeService,
    serve,
};
