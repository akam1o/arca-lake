//! The distributed backend: S3 operations implemented on the metadata
//! store, erasure coding, and storage nodes (RFC-003 §5.3, RFC-004 §5.5–5.6).

pub mod access;
pub mod admission;
mod backend;
pub mod commit_queue;
#[cfg(feature = "fault-injection")]
mod fault_control;
mod map;
mod popqueue;
mod read;
mod write;

pub use backend::{
    CacheStatsSnapshot, ClusterBackend, ClusterConfig, FaultPoint, IntegrityStats,
    IntegrityStatsSnapshot, PutStats, PutStatsSnapshot,
};
pub use map::ClusterMap;
pub use popqueue::{PopQueueStats, PopQueueStatsSnapshot, PopulationQueue};
pub use write::{Committed, FAILED_WRITE_SETTLE};
