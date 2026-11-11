//! Epoch change intents (Phase 3.11, M2 of the Phase 3.10 audit).
//!
//! A member move raises the group's epoch on the nodes before it commits
//! the new membership in metadata (the nodes must refuse the old epoch
//! first). A crash, or an error, between the two left nodes ahead of the
//! metadata: the group refused writes and GC deferred until the same move
//! happened to run again, which nothing ensured. The intent is recorded
//! before any node is raised, under a key of its own
//! (`("al","epochint",group)`; `GroupRecord`'s encoding is pinned):
//!
//! 1. [`begin`]: conditioned on the group's epoch, and on no other intent
//!    for that epoch;
//! 2. the caller raises the nodes (idempotent);
//! 3. [`commit`]: the membership change and the intent's removal in one
//!    transaction, conditioned on the group still at the intent's epoch
//!    (a retry after an unknown commit finds it done and changes
//!    nothing).
//!
//! A leader finding an intent ([`open`]) resumes it: the group still at
//! the intent's epoch means steps 2-3 may not have happened, so it raises
//! again and commits, or, when the move is no longer wanted, settles the
//! epoch alone ([`settle`]: metadata at the raised epoch, membership
//! unchanged, so that nodes and metadata agree again); a group past it
//! means the intent is finished, and it is removed ([`discard`]). An
//! intent never changes a group that moved on: every write is conditioned
//! on the intent's epoch.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GroupMember, GroupRecord, Record, decode};
use crate::{MetaError, keys, transact};

/// `("al","epochint",group)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochIntent {
    /// Group.
    pub group: u64,
    /// The group's epoch when the change began; it goes to `from_epoch + 1`.
    pub from_epoch: u64,
    /// Member index replaced.
    pub index: u16,
    /// The new member.
    pub replacement: GroupMember,
    /// When recorded.
    pub since_ms: u64,
}

impl Record for EpochIntent {}

/// What [`begin`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Begin {
    /// Recorded now.
    Recorded,
    /// The same change was already recorded (a resumed attempt).
    Same,
    /// Another change of this epoch is open: it is resolved first.
    Other(EpochIntent),
    /// The group is no longer at the intent's epoch.
    Stale {
        /// The group's epoch now.
        found: u64,
    },
}

/// Record the intent to raise `intent.group` from `intent.from_epoch`.
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`]; backend errors.
pub async fn begin<S: KvStore>(store: &S, intent: &EpochIntent) -> Result<Begin> {
    transact(store, intent.clone(), |tx, intent| {
        Box::pin(async move {
            let g: GroupRecord = read(tx, &keys::group(intent.group))
                .await?
                .ok_or(MetaError::NoSuchGroup(intent.group))?;
            if g.epoch != intent.from_epoch {
                return Ok(Begin::Stale { found: g.epoch });
            }
            let cur: Option<EpochIntent> = read(tx, &keys::epoch_intent(intent.group)).await?;
            match cur {
                Some(c) if c.from_epoch == g.epoch => {
                    if c.index == intent.index && c.replacement == intent.replacement {
                        Ok(Begin::Same)
                    } else {
                        Ok(Begin::Other(c))
                    }
                }
                // None, or a finished one of an earlier epoch.
                _ => {
                    write(tx, &keys::epoch_intent(intent.group), intent)?;
                    Ok(Begin::Recorded)
                }
            }
        })
    })
    .await
}

/// Commit: apply `change` to the group (which must still be at
/// `from_epoch`, with the intent open), set its epoch to `from_epoch + 1`,
/// and remove the intent, in one transaction. Returns the group as it is
/// afterwards; a group already past `from_epoch` is returned unchanged
/// (the caller compares).
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`]; backend errors.
pub async fn commit<S, F>(store: &S, group: u64, from_epoch: u64, change: F) -> Result<GroupRecord>
where
    S: KvStore,
    F: Fn(&mut GroupRecord) + Send + Sync,
{
    transact(store, (group, from_epoch, &change), |tx, env| {
        Box::pin(async move {
            let (group, from_epoch, change) = *env;
            let mut g: GroupRecord = read(tx, &keys::group(group))
                .await?
                .ok_or(MetaError::NoSuchGroup(group))?;
            let cur: Option<EpochIntent> = read(tx, &keys::epoch_intent(group)).await?;
            if g.epoch != from_epoch || cur.as_ref().is_none_or(|c| c.from_epoch != from_epoch) {
                return Ok(g);
            }
            change(&mut g);
            g.epoch = from_epoch + 1;
            write(tx, &keys::group(group), &g)?;
            tx.clear(&keys::epoch_intent(group));
            crate::ops::cluster::bump_map_version_in_tx(tx).await?;
            Ok(g)
        })
    })
    .await
}

/// Settle an intent whose move is no longer wanted: the group goes to the
/// raised epoch with its membership unchanged (the nodes may already hold
/// it), and the intent is removed. Only while the group is still at the
/// intent's epoch. Returns the group afterwards.
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`]; backend errors.
pub async fn settle<S: KvStore>(store: &S, intent: &EpochIntent) -> Result<GroupRecord> {
    commit(store, intent.group, intent.from_epoch, |_| {}).await
}

/// Remove an intent the group has moved past (finished, or overtaken).
/// Returns whether one was removed.
///
/// # Errors
///
/// Backend errors.
pub async fn discard<S: KvStore>(store: &S, group: u64) -> Result<bool> {
    transact(store, group, |tx, group| {
        Box::pin(async move {
            let cur: Option<EpochIntent> = read(tx, &keys::epoch_intent(*group)).await?;
            let Some(c) = cur else { return Ok(false) };
            let g: Option<GroupRecord> = read(tx, &keys::group(*group)).await?;
            if g.is_some_and(|g| g.epoch == c.from_epoch) {
                return Ok(false);
            }
            tx.clear(&keys::epoch_intent(*group));
            Ok(true)
        })
    })
    .await
}

/// Every open intent.
///
/// # Errors
///
/// Backend errors.
pub async fn open<S: KvStore>(store: &S) -> Result<Vec<EpochIntent>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(
                &KeyRange::prefix(&keys::epoch_intents_prefix()),
                usize::MAX,
                false,
            )
            .await?
            .into_iter()
            .map(|(_, v)| decode::<EpochIntent>(&v))
            .collect()
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;
    use crate::records::{GroupRole, GroupState};

    fn member(n: u64) -> GroupMember {
        GroupMember {
            node_id: n,
            device_id: 0,
        }
    }

    async fn group(kv: &MemKv) -> GroupRecord {
        crate::ops::cluster::create_group(
            kv,
            GroupRecord {
                id: 0,
                profile: "p".into(),
                tier: "t".into(),
                scheme: "rs-gf8-v1".into(),
                data_shards: 2,
                parity_shards: 1,
                members: vec![member(1), member(2), member(3)],
                epoch: 1,
                state: GroupState::Active,
                repair_cursor: None,
                role: GroupRole::Durable,
            },
        )
        .await
        .unwrap()
    }

    #[test]
    fn begin_commit_settle_and_discard_never_touch_a_group_that_moved_on() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let g = group(&kv).await;
            let intent = EpochIntent {
                group: g.id,
                from_epoch: g.epoch,
                index: 0,
                replacement: member(4),
                since_ms: 1,
            };
            assert_eq!(begin(&kv, &intent).await.unwrap(), Begin::Recorded);
            assert_eq!(begin(&kv, &intent).await.unwrap(), Begin::Same);
            let other = EpochIntent {
                replacement: member(5),
                ..intent.clone()
            };
            assert_eq!(
                begin(&kv, &other).await.unwrap(),
                Begin::Other(intent.clone())
            );
            assert_eq!(open(&kv).await.unwrap(), vec![intent.clone()]);
            // Not finished: nothing to discard.
            assert!(!discard(&kv, g.id).await.unwrap());
            let after = commit(&kv, g.id, g.epoch, |g| g.members[0] = member(4))
                .await
                .unwrap();
            assert_eq!((after.epoch, after.members[0]), (g.epoch + 1, member(4)));
            assert_eq!(open(&kv).await.unwrap(), Vec::new());
            // A retry (an unknown commit) changes nothing more.
            let again = commit(&kv, g.id, g.epoch, |g| g.members[0] = member(9))
                .await
                .unwrap();
            assert_eq!(again, after);
            // An old intent of an earlier epoch never changes the group.
            assert_eq!(
                begin(&kv, &intent).await.unwrap(),
                Begin::Stale { found: g.epoch + 1 }
            );
            let settled = settle(&kv, &intent).await.unwrap();
            assert_eq!(settled, after);
            // Settling an open intent: epoch raised, membership unchanged.
            let next = EpochIntent {
                from_epoch: after.epoch,
                ..intent
            };
            assert_eq!(begin(&kv, &next).await.unwrap(), Begin::Recorded);
            let s = settle(&kv, &next).await.unwrap();
            assert_eq!(
                (s.epoch, s.members.clone()),
                (after.epoch + 1, after.members.clone())
            );
            assert_eq!(open(&kv).await.unwrap(), Vec::new());
        });
    }
}
