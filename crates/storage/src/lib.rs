//! Local append-only chunk storage engine for arca-lake storage nodes.
//!
//! Shards are stored as immutable, checksummed records appended to large
//! segment files. An in-memory index maps shard keys to record locations and
//! is rebuilt by scanning the segments on open, so the segments are the only
//! source of truth. The on-disk layout is described in `FORMAT.md` next to
//! this crate and is versioned.
//!
//! Phase 0 status: blocking I/O, single writer, no compaction. This crate
//! exists to measure the I/O path and to prove crash consistency before
//! RFC-005 fixes the production format.

pub mod analysis;
mod error;
pub mod faults;
pub mod format;
mod index;
mod segment;
mod store;

pub use error::StorageError;
pub use format::ShardKey;
pub use index::{Location, QuarantineEntry, ReleaseCause};
pub use store::{
    CompactionReport, CompactionStep, OpenReport, ScrubReport, SegmentStats, Store, StoreConfig,
    StoreStats, StoreTimings, StoreTimingsSnapshot, SyncPolicy, Verified, clamp_format_write,
    default_format_write,
};
