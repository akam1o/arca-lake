//! Capacity of member moves (Phase 3.4 §4): how many bytes a replacement
//! must write, and whether a node has room for them. A replacement whose
//! target lacks room is not started (the request or drain is paused with
//! the reason and resumes on its own once a node has room); the dry run
//! reports the same numbers.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use arca_lake_metadata::records::{GroupRecord, NodeRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};

/// Room a target must have beyond the bytes it receives: 10 % plus this.
pub const RESERVE_BYTES: u64 = 256 << 20;

/// Bytes a node needs free to receive `bytes`.
#[must_use]
pub fn needed(bytes: u64) -> u64 {
    bytes
        .saturating_add(bytes / 10)
        .saturating_add(RESERVE_BYTES)
}

/// Whether a node with `free` bytes (`None` = unknown: a node that does
/// not report it) may receive `bytes`.
#[must_use]
pub fn fits(free: Option<u64>, bytes: u64) -> bool {
    free.is_none_or(|f| f >= needed(bytes))
}

/// What a member holds, measured on one surviving member of its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberSize {
    /// Shards (one per chunk; equal across members).
    pub shards: u64,
    /// Payload bytes of those shards.
    pub bytes: u64,
    /// The member measured.
    pub measured_on: u64,
}

/// Measure member `index` of `group` by listing a surviving member's shards
/// of the group (shards of a stripe have the same length on every member).
/// `None` when no other member answers (a one-member group, or every other
/// member down).
#[allow(clippy::implicit_hasher)]
pub async fn member_size(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    page: u32,
) -> Option<MemberSize> {
    'members: for (i, m) in group.members.iter().enumerate() {
        if i == index {
            continue;
        }
        let Some(n) = nodes.get(&m.node_id) else {
            continue;
        };
        let (mut shards, mut bytes) = (0u64, 0u64);
        let mut after: Option<ShardRef> = None;
        loop {
            let Ok(p) = clients
                .list_shards_page(&n.address, group.id, after, page.max(1))
                .await
            else {
                continue 'members;
            };
            // One shard per chunk of the group on a member.
            for (_, len) in &p.entries {
                shards += 1;
                bytes += len;
            }
            match p.next {
                Some(n) => after = Some(n),
                None => {
                    return Some(MemberSize {
                        shards,
                        bytes,
                        measured_on: m.node_id,
                    });
                }
            }
        }
    }
    None
}

/// Bytes a node may take for a member move (`None`: unknown or
/// unreachable). A planned move (`keep_client_reserve`) gets the room
/// above the node's client reserve and its writes in flight (as
/// rebalancing does); a failure repair the free space down to the repair
/// floor, which the node's admission enforces (Phase 3.11, M3). Either
/// way the node's own admission check has the last word.
pub async fn node_room(
    clients: &NodeClients,
    addr: &str,
    keep_client_reserve: bool,
) -> Option<u64> {
    let h = clients.health(addr).await.ok()?;
    if h.device_total_bytes == 0 && h.devices.is_empty() {
        return None;
    }
    if keep_client_reserve {
        Some(crate::admission::device_of(0, &h, 0).headroom())
    } else {
        (h.device_total_bytes > 0).then_some(h.device_free_bytes)
    }
}

/// Measurements per `(group, epoch, index)`, reused for a while so that a
/// replacement waiting for capacity does not list its group every pass.
#[derive(Debug, Default)]
pub struct Estimates {
    cache: Mutex<HashMap<EstimateKey, (Option<MemberSize>, Instant)>>,
}

/// `(group, epoch, member index)`.
type EstimateKey = (u64, u64, usize);

impl Estimates {
    /// How long a measurement is reused.
    pub const TTL: Duration = Duration::from_mins(10);

    /// [`member_size`], cached.
    #[allow(clippy::implicit_hasher)]
    pub async fn member_size(
        &self,
        clients: &NodeClients,
        nodes: &HashMap<u64, NodeRecord>,
        group: &GroupRecord,
        index: usize,
        page: u32,
    ) -> Option<MemberSize> {
        let key = (group.id, group.epoch, index);
        if let Some((v, at)) = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .copied()
            && at.elapsed() < Self::TTL
        {
            return v;
        }
        let v = member_size(clients, nodes, group, index, page).await;
        let mut c = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        c.retain(|_, (_, at)| at.elapsed() < Self::TTL);
        c.insert(key, (v, Instant::now()));
        v
    }
}
