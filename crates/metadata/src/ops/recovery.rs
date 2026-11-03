//! Restore verification state (RFC-015 §5.3.2, ADR-0016): the cursor the
//! controller leader resumes from and the report per incarnation. Node
//! capability records (RFC-014 §5.1) live here too: nodes cannot write
//! metadata, so the controller records what `Health` reported.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GenId, Record, decode};
use crate::{keys, transact};

/// What the controller learned about a node from `Health`
/// (`("al","ncap",node_id)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapRecord {
    /// Node id.
    pub node_id: u64,
    /// Capabilities (schema axes are 0: nodes hold no metadata).
    pub caps: arca_lake_common::compat::Capabilities,
    /// Software version string.
    pub software: String,
    /// Format the node stamps on new segments.
    pub format_write: u64,
    /// Incarnation the node adopted, if any.
    pub incarnation: Option<[u8; 16]>,
    /// Group epochs the node enforces.
    pub group_epochs: Vec<(u64, u64)>,
    /// When this was observed, Unix milliseconds.
    pub seen_ms: u64,
}

impl Record for NodeCapRecord {}

/// Record what a node reported.
///
/// # Errors
///
/// Backend errors.
pub async fn set_node_caps<S: KvStore>(store: &S, rec: &NodeCapRecord) -> Result<()> {
    transact(store, rec, |tx, rec| {
        Box::pin(async move { write(tx, &keys::node_caps(rec.node_id), *rec) })
    })
    .await
}

/// Every node capability record.
///
/// # Errors
///
/// Backend errors.
pub async fn node_caps<S: KvStore>(store: &S) -> Result<Vec<NodeCapRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::node_caps_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<NodeCapRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Where the recovery pass is (RFC-015 §5.3.1 steps 5–6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPhase {
    /// Epoch catch-up (§5.4): groups up to and including `after_group` are
    /// done.
    CatchUp {
        /// Last group whose epoch was raised and fenced.
        after_group: Option<u64>,
    },
    /// Verifying shards of `group` on `member`, after chunk `after`.
    Shards {
        /// Group being verified.
        group: u64,
        /// Member index.
        member: u16,
        /// Last chunk id verified.
        after: Option<[u8; 16]>,
    },
    /// Presence and orphan classification of `group` after chunk `after`.
    Presence {
        /// Group.
        group: u64,
        /// Last chunk id classified.
        after: Option<[u8; 16]>,
    },
    /// Multipart manifests after root `after`.
    Manifests {
        /// Last root verified.
        after: Option<GenId>,
    },
    /// Everything was examined; the report is final.
    Done,
}

/// `("al","meta","restore","cursor")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreCursor {
    /// Incarnation this pass belongs to.
    pub incarnation: [u8; 16],
    /// Where the pass is.
    pub phase: RecoveryPhase,
    /// Restore point (`restored_at_ms`), separating pre- and post-T orphans.
    pub restore_point_ms: u64,
    /// When the pass started.
    pub started_ms: u64,
    /// Last change.
    pub updated_ms: u64,
    /// Nodes whose listing could not be completed (RFC-015 §5.3.2
    /// completion rule).
    pub unanswered_nodes: Vec<u64>,
    /// Node ids the operator excluded (`--exclude-node`).
    pub excluded_nodes: Vec<u64>,
}

impl Record for RestoreCursor {}

/// One unreadable or dangling object found by verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AffectedObject {
    /// Bucket name (empty when the bucket record is gone).
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Generation.
    pub generation: GenId,
    /// Chunk index.
    pub chunk_index: u32,
    /// Group.
    pub group: u64,
    /// Good shards found.
    pub have: u16,
    /// Shards needed.
    pub need: u16,
    /// Why (`unreadable`, `dangling-part`, …).
    pub why: String,
}

/// Verification counts (RFC-015 §5.3.2 classes). Shard counts unless noted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreCounts {
    /// Shards whose record and metadata agree.
    pub matched: u64,
    /// Orphans whose generation id is newer than the restore point.
    pub orphan_post_t: u64,
    /// Orphans older than the restore point (pre-restore garbage).
    pub orphan_pre_t: u64,
    /// Shards on former members.
    pub former_member: u64,
    /// Shards a live chunk record places on a member that lacks them.
    pub missing: u64,
    /// Shards the node reported corrupt or quarantined.
    pub corrupt: u64,
    /// Chunks with at least `k` but not all shards (repair queued).
    pub degraded_chunks: u64,
    /// Chunks with fewer than `k` good shards.
    pub unreadable_chunks: u64,
    /// Chunk records whose placement epoch is ahead of the group.
    pub placement_epoch_mismatch: u64,
    /// Node/group pairs where the node's epoch disagrees with the group
    /// (before catch-up), or a node enforces a group metadata lacks.
    pub membership_mismatch: u64,
    /// Multipart manifest findings.
    pub manifest_findings: u64,
    /// Cache copies without a placement record, or placements without a
    /// copy.
    pub cache_mismatch: u64,
    /// Groups whose epoch was raised by catch-up.
    pub epochs_raised: u64,
}

/// `("al","meta","restore","report",incarnation)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreReport {
    /// Incarnation.
    pub incarnation: [u8; 16],
    /// Restore point.
    pub restore_point_ms: u64,
    /// Counts.
    pub counts: RestoreCounts,
    /// Unreadable or dangling objects (bounded to [`MAX_AFFECTED`]).
    pub affected: Vec<AffectedObject>,
    /// Whether every node answered completely.
    pub complete: bool,
    /// Nodes that did not answer.
    pub unanswered_nodes: Vec<u64>,
    /// When verification finished (0 = still running).
    pub finished_ms: u64,
    /// Operator acceptance (`admin recovery finalize`), if any.
    pub accepted: Option<Acceptance>,
}

impl Record for RestoreReport {}

/// Operator acceptance of a report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acceptance {
    /// When.
    pub at_ms: u64,
    /// Who.
    pub by: String,
    /// Whether `--force` was needed (incomplete or unreadable).
    pub forced: bool,
}

/// Objects listed per report.
pub const MAX_AFFECTED: usize = 1000;

/// The cursor, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn cursor<S: KvStore>(store: &S) -> Result<Option<RestoreCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::restore_cursor()).await })
    })
    .await
}

/// Replace the cursor.
///
/// # Errors
///
/// Backend errors.
pub async fn set_cursor<S: KvStore>(store: &S, c: &RestoreCursor) -> Result<()> {
    transact(store, c, |tx, c| {
        Box::pin(async move { write(tx, &keys::restore_cursor(), *c) })
    })
    .await
}

/// The report of an incarnation.
///
/// # Errors
///
/// Backend errors.
pub async fn report<S: KvStore>(
    store: &S,
    incarnation: &[u8; 16],
) -> Result<Option<RestoreReport>> {
    transact(store, *incarnation, |tx, inc| {
        Box::pin(async move { read(tx, &keys::restore_report(inc)).await })
    })
    .await
}

/// Replace a report (written at the end of every page so that a crash
/// loses at most one page of counts; the cursor says where to resume).
///
/// # Errors
///
/// Backend errors.
pub async fn set_report<S: KvStore>(store: &S, r: &RestoreReport) -> Result<()> {
    transact(store, r, |tx, r| {
        Box::pin(async move { write(tx, &keys::restore_report(&r.incarnation), *r) })
    })
    .await
}

/// Every report, oldest first.
///
/// # Errors
///
/// Backend errors.
pub async fn reports<S: KvStore>(store: &S) -> Result<Vec<RestoreReport>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::restore_reports_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<RestoreReport>(&v)?);
            }
            out.sort_by_key(|r| r.restore_point_ms);
            Ok(out)
        })
    })
    .await
}

/// Keep a finalized upgrade's cursor as an audit record.
///
/// # Errors
///
/// Backend errors.
pub async fn append_upgrade_audit<S: KvStore>(
    store: &S,
    cursor: &crate::records::UpgradeCursor,
    finished_ms: u64,
) -> Result<()> {
    transact(store, (cursor, finished_ms), |tx, env| {
        Box::pin(async move {
            let (c, ms) = env;
            write(tx, &keys::upgrade_audit(*ms), *c)
        })
    })
    .await
}

/// Every upgrade audit record, oldest first.
///
/// # Errors
///
/// Backend errors.
pub async fn upgrade_audits<S: KvStore>(store: &S) -> Result<Vec<crate::records::UpgradeCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::upgrade_audits_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<crate::records::UpgradeCursor>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}
