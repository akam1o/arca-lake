//! Capacity reservations of member moves (Phase 3.14, ADR-0032): the
//! bytes a repair or a planned move still has to write on its target, kept
//! under `("al","capresv",node,group)` so that two moves are not started
//! onto the room of one, and so that lower-priority work (rebalancing,
//! another planned move) leaves it alone.
//!
//! A reservation counts only while it describes the group's current move:
//! the group is `Repairing` at the reservation's epoch with the node in the
//! reservation's slot ([`CapacityReservation::holds`]), or the move is
//! under way (an open epoch intent of the group from the epoch before to
//! this node in this slot: [`CapacityReservation::pending`], Phase 3.14
//! review). Anything else (the move completed, was retargeted, never
//! began, or a former leader wrote it) counts for nothing and is removed
//! by the leader's sweep: a lost release cannot leak room, and a stale
//! controller cannot make one count. Earlier releases never read these
//! keys.
//!
//! [`reserve`] checks and writes in one transaction that reads every
//! reservation of the node (and the groups they name): two leaders, or two
//! moves of one pass, that reserve the same room conflict, and the second
//! sees the first. A repair counts only other repairs' reservations (it may
//! take room a planned move reserved: repair first); a planned move counts
//! all of them.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn as _};
use crate::ops::{read, write};
use crate::records::{GroupRecord, GroupState, Record, decode};
use crate::{keys, transact};

/// What the move is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReservationKind {
    /// A failed (or failing) member's rebuild: may use the room down to
    /// the repair floor, and comes first.
    #[default]
    Repair,
    /// An operator's move off a healthy node (drain, replacement): keeps
    /// the client reserve, and yields to repairs.
    Planned,
}

impl ReservationKind {
    /// Short name (reports).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Repair => "repair",
            Self::Planned => "planned",
        }
    }
}

/// `("al","capresv",node,group)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CapacityReservation {
    /// Target node.
    pub node: u64,
    /// Group.
    pub group: u64,
    /// The group's epoch of the move (its first epoch on this target).
    pub epoch: u64,
    /// Slot being rebuilt.
    pub index: u16,
    /// Repair or planned.
    pub kind: ReservationKind,
    /// Bytes the member needs on the target (estimated when the move
    /// began).
    pub bytes: u64,
    /// Bytes the move has put on the target since (written, or found
    /// there), as of its last attempt: the room they take is in the node's
    /// own figures from then on.
    pub written: u64,
    /// When reserved, unix ms.
    pub created_ms: u64,
}

impl Record for CapacityReservation {}

impl CapacityReservation {
    /// Bytes still to come.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.bytes.saturating_sub(self.written)
    }

    /// Whether the reservation describes a move of `group` that is still
    /// on: `Repairing` since the reservation's epoch with the node in the
    /// reservation's slot.
    #[must_use]
    pub fn holds(&self, group: &GroupRecord) -> bool {
        // A later epoch of the same move (another slot's change, a settle)
        // keeps it (review): the node is still in its slot.
        group.id == self.group
            && group.state == GroupState::Repairing
            && group.epoch >= self.epoch
            && group
                .members
                .get(usize::from(self.index))
                .is_some_and(|m| m.node_id == self.node)
    }

    /// Whether the reservation's move is under way: `intent` (the group's
    /// open epoch change) takes the group from the epoch before to this
    /// node in this slot.
    #[must_use]
    pub fn pending(&self, intent: &crate::ops::epoch_intent::EpochIntent) -> bool {
        intent.group == self.group
            && intent.from_epoch + 1 == self.epoch
            && intent.index == self.index
            && intent.replacement.node_id == self.node
    }

    /// Whether this reservation counts against a new one of `kind`.
    #[must_use]
    pub fn counts_against(&self, kind: ReservationKind) -> bool {
        kind == ReservationKind::Planned || self.kind == ReservationKind::Repair
    }
}

/// Outcome of [`reserve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reserved {
    /// Written.
    Yes,
    /// The node's room minus what others hold there is less than needed.
    No {
        /// Bytes the node's valid reservations of the counted kinds hold.
        held: u64,
    },
}

async fn valid_on<X: crate::kv::Txn>(tx: &mut X, node: u64) -> Result<Vec<CapacityReservation>> {
    let rows = tx
        .get_range(
            &KeyRange::prefix(&keys::capacity_reservations_of(node)),
            usize::MAX,
            false,
        )
        .await?;
    let mut out = Vec::new();
    for (_, v) in rows {
        let r: CapacityReservation = decode(&v)?;
        if counts(tx, &r).await? {
            out.push(r);
        }
    }
    Ok(out)
}

/// Whether `r` holds or is pending, read in `tx`.
async fn counts<X: crate::kv::Txn>(tx: &mut X, r: &CapacityReservation) -> Result<bool> {
    let g: Option<GroupRecord> = read(tx, &keys::group(r.group)).await?;
    if g.is_some_and(|g| r.holds(&g)) {
        return Ok(true);
    }
    let i: Option<crate::ops::epoch_intent::EpochIntent> =
        read(tx, &keys::epoch_intent(r.group)).await?;
    Ok(i.is_some_and(|i| r.pending(&i)))
}

/// Bytes the valid reservations on `node` still need (every kind), read
/// in one transaction (rebalancing, before each chunk).
///
/// # Errors
///
/// Backend errors.
pub async fn held_on<S: KvStore>(store: &S, node: u64) -> Result<u64> {
    transact(store, node, |tx, node| {
        Box::pin(async move {
            Ok(valid_on(tx, *node)
                .await?
                .iter()
                .map(CapacityReservation::remaining)
                .sum())
        })
    })
    .await
}

/// Reserve `r.bytes` on `r.node` if `room` (the node's room for a move of
/// `r.kind`, measured now) minus what the node's other valid reservations
/// of the counted kinds still need covers it. A reservation of the same
/// group on the node is replaced. One transaction.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn reserve<S: KvStore>(
    store: &S,
    r: &CapacityReservation,
    room: u64,
) -> Result<Reserved> {
    transact(store, (r, room), |tx, (r, room)| {
        Box::pin(async move {
            let held: u64 = valid_on(tx, r.node)
                .await?
                .iter()
                .filter(|o| o.group != r.group && o.counts_against(r.kind))
                .map(CapacityReservation::remaining)
                .sum();
            if held.saturating_add(r.bytes) > *room {
                return Ok(Reserved::No { held });
            }
            write(tx, &keys::capacity_reservation(r.node, r.group), *r)?;
            Ok(Reserved::Yes)
        })
    })
    .await
}

/// Record that the move of `group` onto `node` has put `written` bytes
/// there, at `epoch` (the reservation's or a later one).
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn note_written<S: KvStore>(
    store: &S,
    node: u64,
    group: u64,
    epoch: u64,
    written: u64,
) -> Result<()> {
    transact(store, (node, group, epoch, written), |tx, a| {
        let (node, group, epoch, written) = *a;
        Box::pin(async move {
            let key = keys::capacity_reservation(node, group);
            let Some(mut r) = read::<CapacityReservation, _>(tx, &key).await? else {
                return Ok(());
            };
            if r.epoch > epoch {
                return Ok(());
            }
            r.written = r.written.saturating_add(written).min(r.bytes);
            write(tx, &key, &r)
        })
    })
    .await
}

/// Remove the reservation of `group` on `node`.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn clear<S: KvStore>(store: &S, node: u64, group: u64) -> Result<()> {
    transact(store, (node, group), |tx, a| {
        let (node, group) = *a;
        Box::pin(async move {
            tx.clear(&keys::capacity_reservation(node, group));
            Ok(())
        })
    })
    .await
}

/// Every reservation (valid or not), in node and group order.
///
/// # Errors
///
/// Backend errors.
pub async fn list<S: KvStore>(store: &S) -> Result<Vec<CapacityReservation>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(
                &KeyRange::prefix(&keys::capacity_reservations_prefix()),
                usize::MAX,
                false,
            )
            .await?
            .into_iter()
            .map(|(_, v)| decode::<CapacityReservation>(&v))
            .collect()
        })
    })
    .await
}

/// Remove the reservations that no longer hold (their group's move ended,
/// moved to another target, never began). Returns how many were removed.
/// Each one is checked again in its own transaction against the group.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn sweep<S: KvStore>(store: &S) -> Result<usize> {
    let mut removed = 0;
    for r in list(store).await? {
        let gone = transact(store, &r, |tx, r| {
            Box::pin(async move {
                let key = keys::capacity_reservation(r.node, r.group);
                let Some(now) = read::<CapacityReservation, _>(tx, &key).await? else {
                    return Ok(false);
                };
                if counts(tx, &now).await? {
                    return Ok(false);
                }
                tx.clear(&key);
                Ok(true)
            })
        })
        .await?;
        removed += usize::from(gone);
    }
    Ok(removed)
}
