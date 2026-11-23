//! Per-group repair status (Phase 3.13, R-104): what the leader's repair
//! of a `Repairing` group is waiting for, how often it failed, when it is
//! tried next, and which nodes left the slot being rebuilt (sources of its
//! shards when they answer). Under a key of its own
//! (`("al","repairstat",group)`): `GroupRecord`'s encoding is pinned, and
//! earlier releases never read it (a rolled-back leader ignores it; a
//! record of another epoch than the group's is stale and replaced).
//!
//! Membership changes go through epoch intents
//! ([`crate::ops::epoch_intent`]) and completion through the completeness
//! check, never through this record. The leader also reads it for which
//! nodes left the slot (sources, never chosen again as its target) and
//! which are untrusted (never copied from as they are); a lost record
//! loses those sources and exclusions (the repair goes on, reconstructing
//! from the members), a stale one is replaced.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn as _};
use crate::ops::{read, write};
use crate::records::{Record, decode};
use crate::{keys, transact};

/// Why a `Repairing` group is where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RepairReason {
    /// The last attempt made progress (chunks rebuilt or the walk went on).
    #[default]
    Progressing,
    /// The last attempt failed for a reason expected to pass (an error of
    /// the target or of a node, an epoch change); retried with backoff, the
    /// target replaced after repeated failures.
    TemporarilyBlocked,
    /// No node with the room the member needs (and the target cannot take
    /// it, or is gone).
    AwaitingCapacity,
    /// A node the repair needs does not answer (a member, or the target
    /// with no replacement available).
    AwaitingNodes,
    /// Chunks with fewer than `k` readable shards while every member
    /// answers: queued for stripe repair, retried rarely; the group stays
    /// `Repairing` (never made `Active` without them).
    DataUnrecoverable,
    /// Needs an operator (e.g. `Repairing` without a cursor).
    PermanentlyBlocked,
    /// The move the repair needs (a retarget, a second failed member's
    /// replacement) is refused only because the group has no recorded
    /// failure domain (ADR-0038): `admin group set-failure-domain`. Stored
    /// as [`Self::AwaitingNodes`] with [`AWAITING_FAILURE_DOMAIN`] before
    /// the detail, which 0.1.0 decodes (it fails on an unknown variant);
    /// [`get`] and [`list`] report it as this reason.
    AwaitingFailureDomain,
}

/// Prefix of a stored detail that marks [`RepairReason::AwaitingFailureDomain`].
pub const AWAITING_FAILURE_DOMAIN: &str = "awaiting-failure-domain: ";

impl RepairReason {
    /// Whether a group with this status still counts as repair in progress
    /// (it holds back rebalancing, tiering and cache repair; Phase 3.13: a
    /// group waiting for nodes, capacity or an operator does not).
    #[must_use]
    pub fn holds_background_work(self) -> bool {
        matches!(self, Self::Progressing | Self::TemporarilyBlocked)
    }

    /// Short name (reports).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Progressing => "progressing",
            Self::TemporarilyBlocked => "temporarily-blocked",
            Self::AwaitingCapacity => "awaiting-capacity",
            Self::AwaitingNodes => "awaiting-nodes",
            Self::DataUnrecoverable => "data-unrecoverable",
            Self::PermanentlyBlocked => "permanently-blocked",
            Self::AwaitingFailureDomain => "awaiting-failure-domain",
        }
    }
}

/// `("al","repairstat",group)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepairStatus {
    /// Group.
    pub group: u64,
    /// The group's epoch this status is about (another epoch: stale).
    pub epoch: u64,
    /// Member index being rebuilt.
    pub index: u16,
    /// Node being rebuilt onto.
    pub target: u64,
    /// Current reason.
    pub reason: RepairReason,
    /// What the last attempt found.
    pub detail: String,
    /// When the current reason began, unix ms.
    pub since_ms: u64,
    /// Last attempt that made progress, unix ms.
    pub last_progress_ms: u64,
    /// Consecutive failed attempts attributed to the target at this epoch.
    pub failures: u32,
    /// Consecutive attempts at this epoch that made no progress, for any
    /// reason (the backoff's exponent).
    pub stalls: u32,
    /// Not tried again before, unix ms (backoff).
    pub next_attempt_ms: u64,
    /// Nodes that held this slot during this repair and left it (the node
    /// the repair started from, targets replaced since): kept as sources
    /// of the slot's shards while they answer, and never chosen again as
    /// its target in this repair.
    pub former: Vec<u64>,
    /// Of `former`, the targets left after they stored a shard that did
    /// not check (never used as a direct copy source).
    pub untrusted: Vec<u64>,
    /// The walk of `index` at `epoch` finished with every chunk rebuilt:
    /// only nodes not answering keep the group incomplete (a second failed
    /// member may then be replaced; its shards are no longer needed).
    pub walked_clean: bool,
    /// Targets replaced in this repair (all epochs).
    pub retargets: u32,
    /// Nodes that left *other* slots of the group during this repair
    /// (re-review): never chosen as a target for any slot until the group
    /// is `Active` (their shards of those slots are kept as a former
    /// member's).
    pub excluded: Vec<u64>,
    /// The last try to choose another target found none (the backoff then
    /// applies to the next try).
    pub no_target: bool,
}

impl Record for RepairStatus {}

/// As stored: no reason a 0.1.0 reader cannot decode.
fn stored(s: &RepairStatus) -> RepairStatus {
    let mut s = s.clone();
    if s.reason == RepairReason::AwaitingFailureDomain {
        s.reason = RepairReason::AwaitingNodes;
        s.detail = format!("{AWAITING_FAILURE_DOMAIN}{}", s.detail);
    }
    s
}

/// As reported: the reason [`stored`] encoded.
fn reported(mut s: RepairStatus) -> RepairStatus {
    if s.reason == RepairReason::AwaitingNodes
        && let Some(d) = s.detail.strip_prefix(AWAITING_FAILURE_DOMAIN)
    {
        s.detail = d.to_string();
        s.reason = RepairReason::AwaitingFailureDomain;
    }
    s
}

/// The status of `group`, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn get<S: KvStore>(store: &S, group: u64) -> Result<Option<RepairStatus>> {
    transact(store, group, |tx, group| {
        Box::pin(async move { read(tx, &keys::repair_status(*group)).await })
    })
    .await
    .map(|s| s.map(reported))
}

/// Write the status.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn put<S: KvStore>(store: &S, status: &RepairStatus) -> Result<()> {
    transact(store, status, |tx, status| {
        Box::pin(async move { write(tx, &keys::repair_status(status.group), &stored(status)) })
    })
    .await
}

/// Remove the status of `group` (its repair completed, or it is gone).
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn clear<S: KvStore>(store: &S, group: u64) -> Result<()> {
    transact(store, group, |tx, group| {
        Box::pin(async move {
            tx.clear(&keys::repair_status(*group));
            Ok(())
        })
    })
    .await
}

/// Every status, in group order.
///
/// # Errors
///
/// Backend errors.
pub async fn list<S: KvStore>(store: &S) -> Result<Vec<RepairStatus>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(
                &KeyRange::prefix(&keys::repair_statuses_prefix()),
                usize::MAX,
                false,
            )
            .await?
            .into_iter()
            .map(|(_, v)| decode::<RepairStatus>(&v).map(reported))
            .collect()
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;

    /// Stored as a reason 0.1.0 decodes, reported as written (ADR-0038).
    #[test]
    fn awaiting_a_failure_domain_is_stored_as_awaiting_nodes() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let s = RepairStatus {
                group: 3,
                reason: RepairReason::AwaitingFailureDomain,
                detail: "group 3 has no recorded failure domain".into(),
                ..RepairStatus::default()
            };
            put(&kv, &s).await.unwrap();
            let raw = transact(&kv, (), |tx, ()| {
                Box::pin(async move { read::<RepairStatus, _>(tx, &keys::repair_status(3)).await })
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(raw.reason, RepairReason::AwaitingNodes);
            assert!(raw.detail.starts_with(AWAITING_FAILURE_DOMAIN));
            assert_eq!(get(&kv, 3).await.unwrap(), Some(s.clone()));
            assert_eq!(list(&kv).await.unwrap(), vec![s]);
            let plain = RepairStatus {
                group: 4,
                reason: RepairReason::AwaitingNodes,
                detail: "member 1 does not answer".into(),
                ..RepairStatus::default()
            };
            put(&kv, &plain).await.unwrap();
            assert_eq!(get(&kv, 4).await.unwrap(), Some(plain));
        });
    }
}
