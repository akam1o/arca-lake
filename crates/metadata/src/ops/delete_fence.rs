//! The delete fence (Phase 3.9, ADR-0023): which generation of deletes
//! storage nodes accept. Storage nodes are the enforcement point (they
//! check the generation under the lock their store's delete holds); this
//! record is what the controller leader and operators decide.
//!
//! - The leader opens a new generation for its term, and only while the
//!   cluster is `Active` in its incarnation; deletes of a former term or of
//!   a closed generation are refused by every node that adopted the new
//!   one.
//! - It is closed (a new, closed generation) before a phase change or a
//!   metadata restore: by the leader when the cluster is not `Active`, and
//!   by `admin delete-fence close` / `admin recovery begin`, which push the
//!   closed generation to every node and wait for each to acknowledge it.
//! - Generations only grow. After a restore the record may be older than
//!   what nodes hold; a new generation is then above the highest any node
//!   reported (nodes ignore lower ones, so a too-low choice fails closed).

use serde::{Deserialize, Serialize};

use crate::error::{MetaError, Result};
use crate::kv::KvStore;
use crate::ops::{compat, read, write};
use crate::records::ClusterPhase;
use crate::{keys, transact};

/// The delete fence as decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteFenceRecord {
    /// Generation (0: never opened).
    pub generation: u64,
    /// Deletes under `generation` are allowed.
    pub open: bool,
    /// Fencing token of the leader that opened it (0 when closed by an
    /// operator).
    pub leader_token: u64,
    /// Cluster incarnation it was decided in.
    pub incarnation: [u8; 16],
    /// When, unix ms.
    pub changed_ms: u64,
    /// Why (who opened or closed it).
    pub reason: String,
    /// Closed by an operator (before a restore or for maintenance): the
    /// leader does not open it again until the hold is released
    /// ([`release_hold`], or a recovery's finalize).
    pub operator_hold: bool,
}

impl crate::records::Record for DeleteFenceRecord {}

/// The record (default when never written).
///
/// # Errors
///
/// Backend errors.
pub async fn current<S: KvStore>(store: &S) -> Result<DeleteFenceRecord> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(read::<DeleteFenceRecord, _>(tx, &keys::delete_fence())
                .await?
                .unwrap_or_default())
        })
    })
    .await
}

/// Close: a new, closed generation above the record's and `at_least`
/// (the highest generation a node reported), with the operator hold set
/// when `hold`. Any cluster state.
///
/// # Errors
///
/// Backend errors.
pub async fn close<S: KvStore>(
    store: &S,
    at_least: u64,
    reason: &str,
    hold: bool,
    now_ms: u64,
) -> Result<DeleteFenceRecord> {
    transact(
        store,
        (at_least, reason.to_string(), hold, now_ms),
        |tx, env| {
            Box::pin(async move {
                let (at_least, reason, hold, now_ms) = env;
                let rec: DeleteFenceRecord =
                    read(tx, &keys::delete_fence()).await?.unwrap_or_default();
                let incarnation = compat::cluster_state_in_tx(tx)
                    .await?
                    .map_or([0; 16], |s| s.incarnation);
                let next = DeleteFenceRecord {
                    generation: rec.generation.max(*at_least) + 1,
                    open: false,
                    leader_token: 0,
                    incarnation,
                    changed_ms: *now_ms,
                    reason: reason.clone(),
                    operator_hold: *hold || rec.operator_hold,
                };
                write(tx, &keys::delete_fence(), &next)?;
                Ok(next)
            })
        },
    )
    .await
}

/// Release the operator hold (the fence stays closed; the leader opens a
/// new generation at its next pass). Only while the cluster is `Active`.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] outside `Active`; backend errors.
pub async fn release_hold<S: KvStore>(
    store: &S,
    reason: &str,
    now_ms: u64,
) -> Result<DeleteFenceRecord> {
    transact(store, (reason.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (reason, now_ms) = env;
            let active = compat::cluster_state_in_tx(tx)
                .await?
                .is_some_and(|s| s.state == ClusterPhase::Active);
            if !active {
                return Err(MetaError::InvalidArgument(
                    "the delete fence hold is released only while the cluster is Active".into(),
                ));
            }
            let mut rec: DeleteFenceRecord =
                read(tx, &keys::delete_fence()).await?.unwrap_or_default();
            rec.operator_hold = false;
            rec.changed_ms = *now_ms;
            rec.reason.clone_from(reason);
            write(tx, &keys::delete_fence(), &rec)?;
            Ok(rec)
        })
    })
    .await
}

/// What the leader of term `token` in `incarnation` holds after
/// [`sync_for_term`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermFence {
    /// The record now.
    pub record: DeleteFenceRecord,
    /// Whether this call changed it.
    pub changed: bool,
}

/// The leader's view each health pass: while the cluster is `Active` in
/// `incarnation` and no operator hold is set, a generation open for this
/// term: the record's when it is open, of this term and incarnation and at
/// least `required` (the lowest generation every node would open: a
/// node's open one, or one above a closed one); otherwise a new one above
/// both. Not `Active`: the record closed (once). Written through the leader's fenced store, so a
/// former leader cannot open one.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn sync_for_term<S: KvStore>(
    store: &S,
    token: u64,
    incarnation: [u8; 16],
    highest_reported: u64,
    now_ms: u64,
) -> Result<TermFence> {
    transact(
        store,
        (token, incarnation, highest_reported, now_ms),
        |tx, env| {
            Box::pin(async move {
                let (token, incarnation, highest_reported, now_ms) = *env;
                let rec: DeleteFenceRecord =
                    read(tx, &keys::delete_fence()).await?.unwrap_or_default();
                let state = compat::cluster_state_in_tx(tx).await?;
                let active = state.as_ref().is_some_and(|s| {
                    s.state == ClusterPhase::Active && s.incarnation == incarnation
                });
                let next = if active && !rec.operator_hold {
                    let ours = rec.open
                        && rec.leader_token == token
                        && rec.incarnation == incarnation
                        // A node above it (a close this leader did not
                        // see): reopen above that.
                        && rec.generation >= highest_reported;
                    if ours {
                        None
                    } else {
                        Some(DeleteFenceRecord {
                            generation: (rec.generation + 1).max(highest_reported),
                            open: true,
                            leader_token: token,
                            incarnation,
                            changed_ms: now_ms,
                            reason: format!("opened by the leader of term {token}"),
                            operator_hold: false,
                        })
                    }
                } else if rec.open {
                    Some(DeleteFenceRecord {
                        generation: rec.generation.max(highest_reported) + 1,
                        open: false,
                        leader_token: 0,
                        incarnation,
                        changed_ms: now_ms,
                        reason: format!(
                            "closed by the leader of term {token}: cluster {}",
                            state.map_or("state missing", |s| s.state.name())
                        ),
                        operator_hold: rec.operator_hold,
                    })
                } else {
                    None
                };
                match next {
                    Some(n) => {
                        write(tx, &keys::delete_fence(), &n)?;
                        Ok(TermFence {
                            record: n,
                            changed: true,
                        })
                    }
                    None => Ok(TermFence {
                        record: rec,
                        changed: false,
                    }),
                }
            })
        },
    )
    .await
}

/// Refuse unless the record is closed (a step that must not run with
/// deletes allowed: a metadata restore).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when open; backend errors.
pub async fn require_closed<S: KvStore>(store: &S) -> Result<DeleteFenceRecord> {
    let rec = current(store).await?;
    if rec.open {
        return Err(MetaError::InvalidArgument(format!(
            "the delete fence is open (generation {}): close it first (admin delete-fence close)",
            rec.generation
        )));
    }
    Ok(rec)
}

/// Open a new generation without a leader (single-process tools and test
/// clusters that run no controller), above the record's and `at_least`;
/// only while the cluster is `Active`. A deployment's controller leader
/// opens its own with [`sync_for_term`], which supersedes this one.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] outside `Active`; backend errors.
pub async fn open_without_leader<S: KvStore>(
    store: &S,
    at_least: u64,
    reason: &str,
    now_ms: u64,
) -> Result<DeleteFenceRecord> {
    transact(store, (at_least, reason.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (at_least, reason, now_ms) = env;
            let state = compat::cluster_state_in_tx(tx).await?;
            let Some(state) = state.filter(|s| s.state == ClusterPhase::Active) else {
                return Err(MetaError::InvalidArgument(
                    "the delete fence opens only while the cluster is Active".into(),
                ));
            };
            let rec: DeleteFenceRecord = read(tx, &keys::delete_fence()).await?.unwrap_or_default();
            let next = DeleteFenceRecord {
                generation: rec.generation.max(*at_least) + 1,
                open: true,
                leader_token: 0,
                incarnation: state.incarnation,
                changed_ms: *now_ms,
                reason: reason.clone(),
                operator_hold: false,
            };
            write(tx, &keys::delete_fence(), &next)?;
            Ok(next)
        })
    })
    .await
}
