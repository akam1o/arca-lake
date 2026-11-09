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

/// Highest generation recorded (storage nodes ignore higher ones).
pub const MAX_GENERATION: u64 = u64::MAX >> 1;

/// The generation after `g`; refused beyond [`MAX_GENERATION`] (a node
/// reported one no cluster reaches).
fn next_generation(g: u64) -> Result<u64> {
    g.checked_add(1)
        .filter(|n| *n <= MAX_GENERATION)
        .ok_or_else(|| {
            MetaError::InvalidArgument(format!(
                "delete fence generation {g} cannot advance (a node reported a generation beyond {MAX_GENERATION})"
            ))
        })
}

/// Headroom kept above every open generation for closes (nodes open
/// nothing above `MAX_GENERATION - MAX_STEP`).
pub const MAX_STEP: u64 = 1 << 32;

/// [`next_generation`] for an open: refused within the closes' headroom.
fn next_open_generation(g: u64) -> Result<u64> {
    next_generation(g)?
        .le(&(MAX_GENERATION - MAX_STEP))
        .then_some(g + 1)
        .ok_or_else(|| {
            MetaError::InvalidArgument(format!(
                "delete fence generation {g} too high to open another (a node reported a generation beyond {})",
                MAX_GENERATION - MAX_STEP
            ))
        })
}

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
                    generation: next_generation(rec.generation.max(*at_least))?,
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
    sync_for_term_gated(store, token, incarnation, highest_reported, true, now_ms).await
}

/// Which controller process opened the record's generation (Phase 3.10).
/// A record of its own, so that the fence record keeps its encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FenceOpener {
    /// The generation it opened.
    pub generation: u64,
    /// A value the process drew at its start.
    pub opener: u64,
}

impl crate::records::Record for FenceOpener {}

/// What a leader's health pass knows (Phase 3.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderView {
    /// Its fencing token.
    pub token: u64,
    /// Its cluster incarnation.
    pub incarnation: [u8; 16],
    /// A value its process drew at start: "ours" means opened by this
    /// process, not by a term of the same number (a restore brings the
    /// leadership counter back).
    pub opener: u64,
    /// The lowest generation every node would open (a node's open one, or
    /// one above a closed one).
    pub required: u64,
    /// The highest generation any node reported, open or closed.
    pub max_node_generation: u64,
    /// A whole round of node reports was collected in this term.
    pub may_open: bool,
}

/// What [`sync_leader`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderFence {
    /// The record now, and whether this call changed it.
    pub fence: TermFence,
    /// Nodes hold a generation above the metadata's: the metadata went
    /// back (restored without `recovery begin`, or by hand). The fence was
    /// closed with an operator hold.
    pub metadata_behind: bool,
}

/// The leader's health pass (Phase 3.10): as [`sync_for_term_gated`],
/// with two more rules.
///
/// - **Metadata behind the nodes.** Generations only grow and every one is
///   recorded before any node sees it, so a node above the record means
///   the metadata went back: a restore not yet marked by `recovery begin`.
///   Deletes decided on the metadata before it may still be on their way;
///   the fence is closed above every node with an operator hold, which only
///   `recovery begin` (a new incarnation) or an explicit release lifts.
/// - **Ours** means opened by this process (`opener`), not merely by a
///   term of the same number.
///
/// # Errors
///
/// As [`sync_for_term`].
pub async fn sync_leader<S: KvStore>(
    store: &S,
    view: LeaderView,
    now_ms: u64,
) -> Result<LeaderFence> {
    transact(store, (view, now_ms), |tx, env| {
        Box::pin(async move {
            let (v, now_ms) = *env;
            let rec: DeleteFenceRecord =
                read(tx, &keys::delete_fence()).await?.unwrap_or_default();
            let opener: FenceOpener = read(tx, &keys::delete_fence_opener())
                .await?
                .unwrap_or_default();
            let state = compat::cluster_state_in_tx(tx).await?;
            let active = state.as_ref().is_some_and(|s| {
                s.state == ClusterPhase::Active && s.incarnation == v.incarnation
            });
            let behind = v.max_node_generation > rec.generation;
            let next = if behind {
                Some(DeleteFenceRecord {
                    generation: next_generation(
                        rec.generation.max(v.required).max(v.max_node_generation),
                    )?,
                    open: false,
                    leader_token: 0,
                    incarnation: v.incarnation,
                    changed_ms: now_ms,
                    reason: format!(
                        "storage nodes hold generation {} above the metadata's {}: the metadata went back (restored without `admin recovery begin`?); deletes held until `admin recovery begin` or `admin delete-fence release`",
                        v.max_node_generation, rec.generation
                    ),
                    operator_hold: true,
                })
            } else if active && !rec.operator_hold {
                let ours = rec.open
                    && rec.leader_token == v.token
                    && rec.incarnation == v.incarnation
                    && opener.generation == rec.generation
                    && opener.opener == v.opener
                    && rec.generation >= v.required;
                if ours || !v.may_open {
                    None
                } else {
                    Some(DeleteFenceRecord {
                        generation: next_open_generation(rec.generation.max(v.required))?,
                        open: true,
                        leader_token: v.token,
                        incarnation: v.incarnation,
                        changed_ms: now_ms,
                        reason: format!("opened by the leader of term {}", v.token),
                        operator_hold: false,
                    })
                }
            } else if rec.open {
                Some(DeleteFenceRecord {
                    generation: next_generation(rec.generation.max(v.required))?,
                    open: false,
                    leader_token: 0,
                    incarnation: v.incarnation,
                    changed_ms: now_ms,
                    reason: format!(
                        "closed by the leader of term {}: cluster {}",
                        v.token,
                        state.map_or("state missing", |s| s.state.name())
                    ),
                    operator_hold: rec.operator_hold,
                })
            } else {
                None
            };
            Ok(match next {
                Some(n) => {
                    write(tx, &keys::delete_fence(), &n)?;
                    if n.open {
                        write(
                            tx,
                            &keys::delete_fence_opener(),
                            &FenceOpener {
                                generation: n.generation,
                                opener: v.opener,
                            },
                        )?;
                    }
                    LeaderFence {
                        fence: TermFence {
                            record: n,
                            changed: true,
                        },
                        metadata_behind: behind,
                    }
                }
                None => LeaderFence {
                    fence: TermFence {
                        record: rec,
                        changed: false,
                    },
                    metadata_behind: false,
                },
            })
        })
    })
    .await
}

/// [`sync_for_term`], opening a generation only when `may_open`: a leader
/// passes it once it has heard every node it could reach in this term, so
/// that `highest_reported` is not the empty view of a fresh process (which
/// could reopen a node's own generation, Phase 3.9 review). Closing is
/// never gated.
///
/// # Errors
///
/// As [`sync_for_term`].
pub async fn sync_for_term_gated<S: KvStore>(
    store: &S,
    token: u64,
    incarnation: [u8; 16],
    highest_reported: u64,
    may_open: bool,
    now_ms: u64,
) -> Result<TermFence> {
    transact(
        store,
        (token, incarnation, highest_reported, may_open, now_ms),
        |tx, env| {
            Box::pin(async move {
                let (token, incarnation, highest_reported, may_open, now_ms) = *env;
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
                    if ours || !may_open {
                        None
                    } else {
                        Some(DeleteFenceRecord {
                            // Above every generation a node reported (an open one
                            // included: after a restore without the close, a
                            // node's old generation is never reopened).
                            generation: next_open_generation(rec.generation.max(highest_reported))?,
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
                        generation: next_generation(rec.generation.max(highest_reported))?,
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
                generation: next_open_generation(rec.generation.max(*at_least))?,
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
