//! Storage node: the gRPC service around one device's storage engine.
//!
//! The node knows nothing about objects. It stores shards keyed by
//! `(group, chunk_id, shard_index)`, persists the highest placement epoch it
//! has been told per stripe group, and rejects writes and deletes carrying
//! an older epoch (fencing, RFC-004 §5.4). Every acknowledgment means the
//! storage engine has made the record durable.

mod epochs;
mod service;

pub use arca_lake_storage::{ShardKey, Store, StoreTimingsSnapshot};
pub use service::{
    NodeConfig, NodeHandle, NodeStats, NodeStatsSnapshot, StorageNodeService, serve,
};
