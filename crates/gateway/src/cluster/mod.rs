//! The distributed backend: S3 operations implemented on the metadata
//! store, erasure coding, and storage nodes (RFC-003 §5.3, RFC-004 §5.5–5.6).

mod backend;
mod map;
mod read;
mod write;

pub use backend::{
    CacheStatsSnapshot, ClusterBackend, ClusterConfig, FaultPoint, PutStats, PutStatsSnapshot,
};
pub use map::ClusterMap;
pub use write::Committed;
