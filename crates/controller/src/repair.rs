//! Member replacement and reconstruction (RFC-004 §5.4, §5.7).

use std::collections::{BTreeSet, HashMap};

use arca_lake_ec::{CodeParams, SchemeId};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::ControlError;

/// Result of a repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairReport {
    /// New epoch of the group.
    pub epoch: u64,
    /// Chunks whose shard was reconstructed and written.
    pub reconstructed: usize,
    /// Chunks already present on the replacement (written concurrently).
    pub skipped: usize,
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

/// Replace member `index` of `group_id` with `replacement`: bump the epoch on
/// every reachable member (old and new), commit the new membership, then
/// reconstruct every chunk's shard `index` onto the replacement.
///
/// Idempotent: re-running after a crash skips shards that already exist.
///
/// # Errors
///
/// Metadata errors, node errors on the replacement or on all healthy
/// members, or an unrecoverable chunk.
pub async fn replace_member<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
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
    let new_epoch = group.epoch + 1;

    // 1. Fence: every reachable member of old ∪ new learns the new epoch.
    let mut fenced: BTreeSet<u64> = BTreeSet::new();
    let mut targets: Vec<GroupMember> = group.members.clone();
    targets.push(replacement);
    for m in &targets {
        if !fenced.insert(m.node_id) {
            continue;
        }
        if let Some(n) = nodes.get(&m.node_id) {
            // Unreachable members are the ones being replaced; ignore errors.
            let _ = clients.update_epoch(&n.address, group_id, new_epoch).await;
        }
    }

    // 2. Commit the membership change.
    let group = cluster::update_group(meta, group_id, |g| {
        if g.epoch >= new_epoch {
            return false; // already applied (re-run)
        }
        g.members[index] = replacement;
        g.epoch = new_epoch;
        g.state = GroupState::Repairing;
        g.repair_cursor = None;
        true
    })
    .await?;
    let epoch = group.epoch;

    // 3. Reconstruct.
    let report = reconstruct_member(meta, clients, &nodes, &group, index).await?;
    cluster::update_group(meta, group_id, |g| {
        if g.epoch == epoch && g.state == GroupState::Repairing {
            g.state = GroupState::Active;
            g.repair_cursor = None;
            true
        } else {
            false
        }
    })
    .await?;
    Ok(RepairReport { epoch, ..report })
}

async fn reconstruct_member<S: KvStore>(
    _meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
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
    let mut chunk_ids: BTreeSet<[u8; 16]> = BTreeSet::new();
    let mut present_on_target: BTreeSet<[u8; 16]> = BTreeSet::new();
    for (i, m) in group.members.iter().enumerate() {
        let Some(addr) = addr_of(m) else { continue };
        match clients.list_shards(&addr, group.id).await {
            Ok(list) => {
                for s in list {
                    chunk_ids.insert(s.chunk_id);
                    if i == index && usize::from(s.shard_index) == index {
                        present_on_target.insert(s.chunk_id);
                    }
                }
            }
            Err(status) if i == index => {
                return Err(ControlError::Node { addr, status });
            }
            Err(_) => {} // unhealthy member; others cover it
        }
    }

    let scheme = SchemeId::parse(&group.scheme)
        .ok_or_else(|| ControlError::Other(format!("unknown scheme {}", group.scheme)))?;
    let params = CodeParams::new(
        usize::from(group.data_shards),
        usize::from(group.parity_shards),
    )
    .map_err(|e| ControlError::Other(e.to_string()))?;
    let coder =
        arca_lake_ec::new_coder(scheme, params).map_err(|e| ControlError::Other(e.to_string()))?;
    let k = params.data;
    let total = params.total();

    let mut report = RepairReport::default();
    for cid in chunk_ids {
        if present_on_target.contains(&cid) {
            report.skipped += 1;
            continue;
        }
        // Fetch up to k shards from the other members.
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut have = 0;
        for (i, m) in group.members.iter().enumerate() {
            if i == index || have >= k {
                continue;
            }
            let Some(addr) = addr_of(m) else { continue };
            let shard = ShardRef {
                group_id: group.id,
                chunk_id: cid,
                shard_index: u16::try_from(i).unwrap_or(0),
            };
            if let Ok(b) = clients.get_shard(&addr, shard, 0, 0).await {
                shards[i] = Some(b.to_vec());
                have += 1;
            }
        }
        if have < k {
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
        let bytes = Bytes::from(shards[index].take().unwrap_or_default());
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: cid,
            shard_index: u16::try_from(index).unwrap_or(0),
        };
        clients
            .put_shard(&target_addr, shard, group.epoch, bytes, 256 << 10)
            .await
            .map_err(|status| ControlError::Node {
                addr: target_addr.clone(),
                status,
            })?;
        report.reconstructed += 1;
    }
    Ok(report)
}
