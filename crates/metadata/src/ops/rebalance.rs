//! Capacity rebalancing state (Phase 3.8): the controller leader's cursor
//! over generations, the operator's pause and dry-run switches, and what
//! the passes did. One small record; the leader writes it through its
//! fenced store, operators through `admin rebalance`.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::KvStore;
use crate::ops::{read, write};
use crate::records::GenId;
use crate::{keys, transact};

/// The rebalancer's persistent state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceState {
    /// Operator pause: no pass moves anything while set.
    pub paused: bool,
    /// Operator dry run: passes plan and report, move nothing.
    pub dry_run: bool,
    /// Generation after which the next pass continues (`None`: from the
    /// start).
    pub cursor: Option<GenId>,
    /// Tier, source and target group of the move in progress.
    pub plan: Option<RebalancePlan>,
    /// Chunks moved, all passes.
    pub moved_chunks: u64,
    /// Logical bytes moved, all passes.
    pub moved_bytes: u64,
    /// Chunks whose move was skipped or failed, all passes.
    pub skipped_chunks: u64,
    /// When the last pass ran, unix ms.
    pub last_pass_ms: u64,
    /// Why the last pass stopped or did nothing.
    pub last_reason: String,
}

/// One planned move: chunks of `source` go to `target`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalancePlan {
    /// Tier of both groups.
    pub tier: String,
    /// Fuller group.
    pub source: u64,
    /// Roomier group (same scheme and shape).
    pub target: u64,
    /// Source room when planned, bytes.
    pub source_room: u64,
    /// Target room when planned, bytes.
    pub target_room: u64,
}

impl crate::records::Record for RebalanceState {}

/// The state (default when none was written).
///
/// # Errors
///
/// Backend errors.
pub async fn state<S: KvStore>(store: &S) -> Result<RebalanceState> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(read::<RebalanceState, _>(tx, &keys::rebalance())
                .await?
                .unwrap_or_default())
        })
    })
    .await
}

/// Write the state.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn put<S: KvStore>(store: &S, state: &RebalanceState) -> Result<()> {
    transact(store, state, |tx, state| {
        Box::pin(async move { write(tx, &keys::rebalance(), *state) })
    })
    .await
}

/// Set the operator switches (read-modify-write in one transaction, so a
/// pass's cursor update is not lost and the pass does not undo them).
///
/// # Errors
///
/// Backend errors.
pub async fn set_switches<S: KvStore>(
    store: &S,
    paused: Option<bool>,
    dry_run: Option<bool>,
) -> Result<RebalanceState> {
    transact(store, (paused, dry_run), |tx, (paused, dry_run)| {
        Box::pin(async move {
            let mut s: RebalanceState = read(tx, &keys::rebalance()).await?.unwrap_or_default();
            if let Some(p) = paused {
                s.paused = *p;
            }
            if let Some(d) = dry_run {
                s.dry_run = *d;
            }
            write(tx, &keys::rebalance(), &s)?;
            Ok(s)
        })
    })
    .await
}

/// Record a pass's progress, keeping the operator switches as they are in
/// the store (an operator may have changed them during the pass).
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn record_progress<S: KvStore>(store: &S, progress: &RebalanceState) -> Result<()> {
    transact(store, progress, |tx, progress| {
        Box::pin(async move {
            let current: RebalanceState = read(tx, &keys::rebalance()).await?.unwrap_or_default();
            let next = RebalanceState {
                paused: current.paused,
                dry_run: current.dry_run,
                ..(*progress).clone()
            };
            write(tx, &keys::rebalance(), &next)
        })
    })
    .await
}

/// Pacing of the rebalancer (Phase 3.9), a record of its own so that the
/// state record keeps its encoding: what the plan in progress still needs,
/// and the tiers settling after a plan finished. A source's room comes
/// back only once reconciliation and compaction removed the moved chunks'
/// old copies, while a target's room shrinks at once: planning again from
/// the rooms before that would move about twice what is needed, then back
/// (Phase 3.9 load run on FoundationDB).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalancePacing {
    /// `(tier, source, target)` of the plan the need belongs to.
    pub plan: Option<(String, u64, u64)>,
    /// Logical bytes the plan may still move (computed once, from the
    /// rooms when it was planned).
    pub remaining_bytes: u64,
    /// Tiers that finished a plan and are not planned again before the
    /// given unix ms.
    pub settling: Vec<(String, u64)>,
}

impl crate::records::Record for RebalancePacing {}

/// The pacing record (default when none was written).
///
/// # Errors
///
/// Backend errors.
pub async fn pacing<S: KvStore>(store: &S) -> Result<RebalancePacing> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(read::<RebalancePacing, _>(tx, &keys::rebalance_pacing())
                .await?
                .unwrap_or_default())
        })
    })
    .await
}

/// Write the pacing record.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn put_pacing<S: KvStore>(store: &S, pacing: &RebalancePacing) -> Result<()> {
    transact(store, pacing, |tx, pacing| {
        Box::pin(async move { write(tx, &keys::rebalance_pacing(), *pacing) })
    })
    .await
}

/// Room the rebalancer's moves will give back to their source groups
/// (Phase 3.12): per source group, the bytes each member gets back once the
/// moved shards are reconciled, reclaimed (deferred reclamation holds them
/// for the nodes' retention, ADR-0028) and compacted. A plan counts them as
/// the source's room: without them a plan made before that sees the source
/// as full as before its move and moves again what was moved (then back,
/// once the room returns).
///
/// Each entry keeps the source's room when it was recorded (`baseline`):
/// what the room rose above it since has come back, and only the rest is
/// still counted, so that room counted here and room the source reports are
/// never counted twice whenever compaction gets to it. `until` is a
/// backstop only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceInTransit {
    /// `(source group, bytes per member, baseline room, until unix ms)`.
    pub entries: Vec<(u64, u64, u64, u64)>,
}

impl crate::records::Record for RebalanceInTransit {}

impl RebalanceInTransit {
    /// Bytes per member still coming back to `group`, whose room is `room`
    /// now, at `now_ms`.
    #[must_use]
    pub fn coming_back(&self, group: u64, room: u64, now_ms: u64) -> u64 {
        self.entries
            .iter()
            .find(|(g, _, _, until)| *g == group && *until > now_ms)
            .map_or(0, |(_, bytes, baseline, _)| {
                bytes.saturating_sub(room.saturating_sub(*baseline))
            })
    }

    /// `bytes` per member more coming back to `group`, whose room is `room`
    /// now, until `until_ms` at the latest; expired entries are dropped.
    pub fn add(&mut self, group: u64, bytes: u64, room: u64, until_ms: u64, now_ms: u64) {
        let outstanding = self.coming_back(group, room, now_ms);
        self.entries
            .retain(|(g, _, _, until)| *until > now_ms && *g != group);
        let total = outstanding.saturating_add(bytes);
        if total > 0 {
            self.entries.push((group, total, room, until_ms));
        }
    }
}

/// The in-transit record (default when none was written).
///
/// # Errors
///
/// Backend errors.
pub async fn in_transit<S: KvStore>(store: &S) -> Result<RebalanceInTransit> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(
                read::<RebalanceInTransit, _>(tx, &keys::rebalance_in_transit())
                    .await?
                    .unwrap_or_default(),
            )
        })
    })
    .await
}

/// Write the in-transit record.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn put_in_transit<S: KvStore>(store: &S, t: &RebalanceInTransit) -> Result<()> {
    transact(store, t, |tx, t| {
        Box::pin(async move { write(tx, &keys::rebalance_in_transit(), *t) })
    })
    .await
}
