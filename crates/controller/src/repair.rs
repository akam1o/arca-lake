//! Member replacement and reconstruction (RFC-004 §5.4, §5.7).
//!
//! Order of operations and the meaning of "complete":
//!
//! 1. **Fence.** Every reachable member of the old and new membership
//!    learns `epoch + 1`. From here on, writers holding the old epoch are
//!    rejected by every member that received the fence.
//! 2. **Commit membership.** One transaction replaces the member, sets
//!    `epoch + 1`, state `Repairing`, and clears the cursor. It fails if the
//!    group changed since it was read ([`ControlError::EpochChanged`]).
//!    Writers keep using the group (`Repairing` accepts writes); their new
//!    chunks carry the new epoch and include the replacement directly.
//! 3. **Reconstruct.** Chunk IDs are the union of what every member lists,
//!    in sorted order; for each one missing on the replacement, `k` shards
//!    are read, the missing shard is reconstructed and written with the new
//!    epoch. The cursor (last processed chunk ID) is persisted after every
//!    chunk so that a crashed controller resumes by re-issuing the same
//!    command. A shard already present on the replacement (written by a
//!    concurrent writer) is skipped: both would write identical bytes.
//! 4. **Complete.** The group becomes `Active` only when every member lists
//!    the same set of chunk IDs (nothing is missing anywhere). If another
//!    membership change happened meanwhile, the repair stops with
//!    [`ControlError::EpochChanged`] and must be re-issued; the group stays
//!    `Repairing`.
//!
//! Nothing here deletes anything: shards of the replaced member stay where
//! they are and are reclaimed by reconciliation once the group is `Active`
//! and the node is no longer a member (`gc::reconcile_node`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arca_lake_ec::{CodeParams, SchemeId};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{
    GenState, GenerationHeader, GroupMember, GroupRecord, GroupState, NodeRecord, RepairCursor,
    decode,
};
use arca_lake_metadata::{MetaError, Txn, keys, transact};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::ControlError;

/// Result of a repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairReport {
    /// Epoch of the group during this repair.
    pub epoch: u64,
    /// Chunks whose shard was reconstructed and written.
    pub reconstructed: usize,
    /// Chunks already present on the replacement (written concurrently or
    /// by an earlier, interrupted run).
    pub skipped: usize,
    /// Whether this run continued an interrupted repair.
    pub resumed: bool,
    /// Whether the group is `Active` again (every member holds every chunk).
    pub complete: bool,
}

/// Points at which a [`replace_member_with`] hook is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairStep {
    /// Reachable members were told the new epoch.
    Fenced,
    /// The new membership is committed in metadata.
    MembershipCommitted,
    /// One chunk's shard was written to the replacement (`index` counts
    /// reconstructed chunks in this run).
    Chunk {
        /// Ordinal within this run.
        index: usize,
        /// Chunk ID.
        chunk_id: [u8; 16],
    },
    /// Reconstruction finished; completeness is about to be checked.
    Reconstructed,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// Replace member `index` of `group_id` with `replacement` (see the module
/// documentation for the protocol). Re-issuing the same call after a crash
/// or an [`ControlError::EpochChanged`] resumes from the persisted cursor.
///
/// # Errors
///
/// Metadata errors, node errors on the replacement, an unrecoverable
/// chunk, or [`ControlError::EpochChanged`].
pub async fn replace_member<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
) -> Result<RepairReport, ControlError> {
    replace_member_with(
        meta,
        clients,
        group_id,
        index,
        replacement,
        async |_| Ok(()),
    )
    .await
}

/// [`replace_member`] with a hook called at every [`RepairStep`]; an error
/// from the hook stops the repair at that point (tests use it to emulate a
/// controller crash or to run concurrent operations).
///
/// # Errors
///
/// As [`replace_member`], plus whatever the hook returns.
pub async fn replace_member_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
    mut hook: impl AsyncFnMut(RepairStep) -> Result<(), ControlError>,
) -> Result<RepairReport, ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let group = cluster::get_group(meta, group_id).await?;
    if index >= group.members.len() {
        return Err(ControlError::Other(format!(
            "member index {index} out of range"
        )));
    }

    let (group, resumed) = if group.members[index] == replacement {
        match group.state {
            GroupState::Repairing => (group, true),
            _ => {
                // Already replaced and complete: nothing to do.
                return Ok(RepairReport {
                    epoch: group.epoch,
                    complete: true,
                    ..RepairReport::default()
                });
            }
        }
    } else {
        let new_epoch = group.epoch + 1;
        fence(clients, &nodes, &group, replacement, new_epoch).await;
        hook(RepairStep::Fenced).await?;
        let expected_epoch = group.epoch;
        let updated = cluster::update_group(meta, group_id, |g| {
            if g.epoch != expected_epoch {
                return false;
            }
            g.members[index] = replacement;
            g.epoch = new_epoch;
            g.state = GroupState::Repairing;
            g.repair_cursor = None;
            true
        })
        .await?;
        if updated.epoch != new_epoch || updated.members[index] != replacement {
            return Err(ControlError::EpochChanged {
                group: group_id,
                expected: expected_epoch,
                found: updated.epoch,
            });
        }
        hook(RepairStep::MembershipCommitted).await?;
        (updated, false)
    };

    let mut report = reconstruct_member(meta, clients, &nodes, &group, index, &mut hook).await?;
    report.resumed = resumed;
    hook(RepairStep::Reconstructed).await?;

    // Complete only when nothing is missing anywhere in the group.
    let complete = group_is_complete(clients, &nodes, &group).await?;
    if complete {
        let epoch = group.epoch;
        let after = cluster::update_group(meta, group_id, |g| {
            if g.epoch == epoch && g.state == GroupState::Repairing {
                g.state = GroupState::Active;
                g.repair_cursor = None;
                true
            } else {
                false
            }
        })
        .await?;
        if after.epoch != epoch {
            return Err(ControlError::EpochChanged {
                group: group_id,
                expected: epoch,
                found: after.epoch,
            });
        }
    }
    report.complete = complete;
    Ok(report)
}

/// Tell every reachable member of old ∪ new the new epoch. Unreachable
/// members are the ones being replaced; if one returns later it is no
/// longer a member and its shards are reclaimed by reconciliation.
async fn fence(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    replacement: GroupMember,
    new_epoch: u64,
) {
    let mut fenced: BTreeSet<u64> = BTreeSet::new();
    let mut targets: Vec<GroupMember> = group.members.clone();
    targets.push(replacement);
    for m in &targets {
        if !fenced.insert(m.node_id) {
            continue;
        }
        if let Some(n) = nodes.get(&m.node_id) {
            let _ = clients.update_epoch(&n.address, group.id, new_epoch).await;
        }
    }
}

/// Chunk IDs per member (shard index == member index), for the members
/// that answer.
async fn list_members(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
) -> BTreeMap<usize, Option<BTreeSet<[u8; 16]>>> {
    let mut out = BTreeMap::new();
    for (i, m) in group.members.iter().enumerate() {
        let Some(n) = nodes.get(&m.node_id) else {
            out.insert(i, None);
            continue;
        };
        let listed = clients
            .list_shards(&n.address, group.id)
            .await
            .ok()
            .map(|list| {
                list.into_iter()
                    .filter(|s| usize::from(s.shard_index) == i)
                    .map(|s| s.chunk_id)
                    .collect::<BTreeSet<_>>()
            });
        out.insert(i, listed);
    }
    out
}

/// Every member answers and lists the same chunk IDs.
async fn group_is_complete(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
) -> Result<bool, ControlError> {
    let lists = list_members(clients, nodes, group).await;
    let mut union: BTreeSet<[u8; 16]> = BTreeSet::new();
    for l in lists.values().flatten() {
        union.extend(l.iter().copied());
    }
    Ok(lists
        .values()
        .all(|l| l.as_ref().is_some_and(|l| *l == union)))
}

async fn reconstruct_member<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    hook: &mut impl AsyncFnMut(RepairStep) -> Result<(), ControlError>,
) -> Result<RepairReport, ControlError> {
    let addr_of = |m: &GroupMember| nodes.get(&m.node_id).map(|n| n.address.clone());
    let target_addr = addr_of(&group.members[index]).ok_or_else(|| {
        ControlError::Other(format!(
            "replacement node {} unknown",
            group.members[index].node_id
        ))
    })?;

    // Enumerate chunk IDs from every member (union), including the target
    // so that concurrently written chunks are known.
    let lists = list_members(clients, nodes, group).await;
    if lists.get(&index).is_none_or(Option::is_none) {
        return Err(ControlError::Other(format!(
            "replacement {target_addr} did not answer ListShards"
        )));
    }
    let mut chunk_ids: BTreeSet<[u8; 16]> = BTreeSet::new();
    for l in lists.values().flatten() {
        chunk_ids.extend(l.iter().copied());
    }
    let present_on_target = lists[&index].clone().unwrap_or_default();

    let scheme = SchemeId::parse(&group.scheme)
        .ok_or_else(|| ControlError::Other(format!("unknown scheme {}", group.scheme)))?;
    let params = CodeParams::new(
        usize::from(group.data_shards),
        usize::from(group.parity_shards),
    )
    .map_err(|e| ControlError::Other(e.to_string()))?;
    let coder =
        arca_lake_ec::new_coder(scheme, params).map_err(|e| ControlError::Other(e.to_string()))?;
    let mut report = RepairReport {
        epoch: group.epoch,
        ..RepairReport::default()
    };
    for cid in chunk_ids {
        let cursor = group
            .repair_cursor
            .filter(|c| usize::from(c.member) == index)
            .map(|c| c.chunk_id);
        if cursor.is_some_and(|c| cid <= c) {
            // Processed by an earlier run (or written since, with the new
            // epoch and therefore already on the target).
            report.skipped += 1;
            continue;
        }
        if present_on_target.contains(&cid) {
            report.skipped += 1;
            persist_cursor(meta, group, index, cid).await?;
            continue;
        }
        let Some(bytes) =
            reconstruct_one(meta, clients, nodes, group, index, cid, coder.as_ref()).await?
        else {
            // Deleted meanwhile (no live generation accounts for it).
            report.skipped += 1;
            persist_cursor(meta, group, index, cid).await?;
            continue;
        };
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: cid,
            shard_index: u16::try_from(index).unwrap_or(0),
        };
        match clients
            .put_shard(&target_addr, shard, group.epoch, bytes, 256 << 10)
            .await
        {
            Ok(_) => {}
            Err(status) if NodeClients::is_epoch_rejected(&status) => {
                return Err(ControlError::EpochChanged {
                    group: group.id,
                    expected: group.epoch,
                    found: 0,
                });
            }
            Err(status) => {
                return Err(ControlError::Node {
                    addr: target_addr.clone(),
                    status,
                });
            }
        }
        persist_cursor(meta, group, index, cid).await?;
        let index_in_run = report.reconstructed;
        report.reconstructed += 1;
        hook(RepairStep::Chunk {
            index: index_in_run,
            chunk_id: cid,
        })
        .await?;
    }
    Ok(report)
}

/// Read any `k` shards of `cid` from the members other than `index` and
/// reconstruct shard `index`. `None` when fewer than `k` shards exist and
/// metadata no longer has a live generation for the chunk (it was deleted
/// while the repair ran); fewer than `k` shards for a live generation is
/// [`ControlError::Unrecoverable`].
async fn reconstruct_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    cid: [u8; 16],
    coder: &dyn arca_lake_ec::ErasureCoder,
) -> Result<Option<Bytes>, ControlError> {
    let params = coder.params();
    let k = params.data;
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; params.total()];
    let mut have = 0;
    for (i, m) in group.members.iter().enumerate() {
        if i == index || have >= k {
            continue;
        }
        let Some(n) = nodes.get(&m.node_id) else {
            continue;
        };
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: cid,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        if let Ok(b) = clients.get_shard(&n.address, shard, 0, 0).await {
            shards[i] = Some(b.to_vec());
            have += 1;
        }
    }
    if have < k {
        if chunk_is_garbage(meta, cid).await? {
            return Ok(None);
        }
        return Err(ControlError::Unrecoverable {
            group: group.id,
            chunk: hex(&cid),
            have,
            need: k,
        });
    }
    coder
        .reconstruct(&mut shards)
        .map_err(|e| ControlError::Other(format!("reconstruct: {e}")))?;
    Ok(Some(Bytes::from(shards[index].take().unwrap_or_default())))
}

/// Whether no pending or committed generation with the chunk's prefix
/// exists (a retiring or finished generation's chunks are garbage).
async fn chunk_is_garbage<S: KvStore>(meta: &S, cid: [u8; 16]) -> Result<bool, MetaError> {
    let (prefix, _) = arca_lake_common::split_chunk_id(&cid);
    transact(meta, prefix, |tx, prefix| {
        Box::pin(async move {
            let full = keys::gen_prefix_range(prefix);
            let mut begin = full.begin.clone();
            while begin < full.end {
                let first = tx
                    .get_range(
                        &arca_lake_metadata::kv::KeyRange::new(begin.clone(), full.end.clone()),
                        1,
                        false,
                    )
                    .await?;
                let Some((k, _)) = first.into_iter().next() else {
                    break;
                };
                let Some(generation) = keys::gen_from_gen_key(&k) else {
                    break;
                };
                if let Some(h) = tx.get(&keys::gen_header(&generation)).await? {
                    let header: GenerationHeader = decode(&h)?;
                    if matches!(header.state, GenState::Pending { .. } | GenState::Committed) {
                        return Ok(false);
                    }
                }
                begin = keys::gen_range(&generation).end;
            }
            Ok(true)
        })
    })
    .await
}

/// Record progress; fails with [`ControlError::EpochChanged`] if the group
/// moved on (another membership change), so that this run stops writing.
async fn persist_cursor<S: KvStore>(
    meta: &S,
    group: &GroupRecord,
    index: usize,
    cid: [u8; 16],
) -> Result<(), ControlError> {
    let epoch = group.epoch;
    let member = u16::try_from(index).unwrap_or(u16::MAX);
    let after = cluster::update_group(meta, group.id, |g| {
        if g.epoch == epoch && g.state == GroupState::Repairing {
            g.repair_cursor = Some(RepairCursor {
                member,
                chunk_id: cid,
            });
            true
        } else {
            false
        }
    })
    .await?;
    if after.epoch != epoch {
        return Err(ControlError::EpochChanged {
            group: group.id,
            expected: epoch,
            found: after.epoch,
        });
    }
    Ok(())
}
