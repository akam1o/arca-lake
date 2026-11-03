//! Dry runs of member moves (`admin replace-member --dry-run`,
//! `admin decommission-node --dry-run`, Phase 3.4 §4.3): what the
//! controller leader would do, computed with the same target selection and
//! capacity rule, without writing metadata or touching a shard. Reads only:
//! metadata, `Health` and `ListShards`.

use std::collections::{BTreeSet, HashMap};

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::{cluster, compat, membership};
use arca_lake_metadata::records::{ClusterPhase, GroupRecord, GroupState, NodeRecord};
use arca_lake_protocol::client::NodeClients;

use crate::ControlError;
use crate::capacity::{self, MemberSize};

/// One member move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovePlan {
    /// Group.
    pub group: u64,
    /// Tier of the group.
    pub tier: String,
    /// Cache group (rebuilt from the durable tier or started empty).
    pub cache: bool,
    /// Member index.
    pub index: u16,
    /// Node leaving the group.
    pub from_node: u64,
    /// Node that would receive the member (`None`: none qualifies).
    pub to_node: Option<u64>,
    /// Free bytes the target reports (`None`: unknown).
    pub to_free: Option<u64>,
    /// Shards (chunks) to rebuild and their bytes (`None`: unmeasured).
    pub size: Option<MemberSize>,
    /// Bytes read to rebuild them (EC reconstruction reads `k` shards per
    /// chunk; replication one).
    pub read_bytes: u64,
    /// Members that can serve as sources now / needed.
    pub sources: (usize, usize),
    /// Conditions under which the move would not start or not finish.
    pub blockers: Vec<String>,
    /// Facts worth knowing that do not stop it.
    pub notes: Vec<String>,
}

/// A dry run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Cluster phase (operator moves progress only while Active).
    pub phase: Option<ClusterPhase>,
    /// Moves in the order the controller would run them.
    pub moves: Vec<MovePlan>,
    /// Conditions for the whole operation.
    pub blockers: Vec<String>,
    /// Facts about the whole operation.
    pub notes: Vec<String>,
}

impl Plan {
    /// Bytes written in total (measured moves).
    #[must_use]
    pub fn write_bytes(&self) -> u64 {
        self.moves
            .iter()
            .filter_map(|m| m.size)
            .map(|s| s.bytes)
            .sum()
    }

    /// Bytes read in total.
    #[must_use]
    pub fn read_bytes(&self) -> u64 {
        self.moves.iter().map(|m| m.read_bytes).sum()
    }

    /// Whether anything blocks the operation.
    #[must_use]
    pub fn blocked(&self) -> bool {
        !self.blockers.is_empty() || self.moves.iter().any(|m| !m.blockers.is_empty())
    }
}

struct World {
    nodes: HashMap<u64, NodeRecord>,
    groups: Vec<GroupRecord>,
    /// Reachable nodes and their free bytes.
    free: HashMap<u64, Option<u64>>,
    unavailable: BTreeSet<u64>,
    phase: Option<ClusterPhase>,
    requests: Vec<membership::ReplaceRequest>,
}

async fn world<S: KvStore>(meta: &S, clients: &NodeClients) -> Result<World, ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups = cluster::list_groups(meta).await?;
    let mut unavailable = BTreeSet::new();
    for r in membership::node_states(meta).await? {
        unavailable.insert(r.node_id);
    }
    let mut free = HashMap::new();
    for n in nodes.values() {
        clients.note_node_id(&n.address, n.id);
        if let Ok(h) = clients.health(&n.address).await {
            free.insert(
                n.id,
                (h.device_total_bytes > 0).then_some(h.device_free_bytes),
            );
        }
    }
    Ok(World {
        nodes,
        groups,
        free,
        unavailable,
        phase: compat::cluster_state(meta).await?.map(|s| s.state),
        requests: membership::replace_requests(meta).await?,
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::single_match_else
)]
async fn plan_move(
    w: &World,
    clients: &NodeClients,
    g: &GroupRecord,
    index: usize,
    to_node: Option<u64>,
    failure_domain: &str,
    page: u32,
    taken: &HashMap<u64, u64>,
) -> Result<MovePlan, ControlError> {
    let from = g.members[index].node_id;
    let mut p = MovePlan {
        group: g.id,
        tier: g.tier.clone(),
        cache: g.is_cache(),
        index: u16::try_from(index).unwrap_or(u16::MAX),
        from_node: from,
        to_node: None,
        to_free: None,
        size: None,
        read_bytes: 0,
        sources: (0, usize::from(g.data_shards.max(1))),
        blockers: Vec::new(),
        notes: Vec::new(),
    };
    if g.state != GroupState::Active {
        p.blockers.push(format!(
            "group is {:?}; the move waits until it is Active",
            g.state
        ));
    }
    if let Some(r) = w.requests.iter().find(|r| r.group == g.id) {
        p.notes.push(format!(
            "an open request already replaces member {} (node {}){}",
            r.index,
            r.from_node,
            r.last_error
                .as_ref()
                .map(|e| format!("; last error: {e}"))
                .unwrap_or_default()
        ));
    }
    let reachable = g
        .members
        .iter()
        .enumerate()
        .filter(|(i, m)| *i != index && w.free.contains_key(&m.node_id))
        .count();
    p.sources.0 = reachable;
    let one_copy_cache = g.is_cache() && g.parity_shards == 0 && g.members.len() == 1;
    if one_copy_cache {
        p.notes.push("unprotected cache group: the new member starts empty and refills from the durable tier".into());
    } else {
        if reachable < p.sources.1 {
            p.blockers.push(format!(
                "only {reachable} other members answer; rebuilding needs {}",
                p.sources.1
            ));
        }
        p.size = capacity::member_size(clients, &w.nodes, g, index, page).await;
        match p.size {
            Some(s) => {
                p.read_bytes = s.bytes.saturating_mul(u64::from(g.data_shards.max(1)));
            }
            None => p
                .blockers
                .push("no other member answers ListShards: the bytes to move are unknown".into()),
        }
        if w.free.contains_key(&from) {
            p.notes.push(
                "the leaving member answers: a direct copy would read each shard once instead of k"
                    .into(),
            );
        }
    }
    let need = p.size.map_or(0, |s| s.bytes);
    let free_after = |id: u64| {
        w.free
            .get(&id)
            .copied()
            .flatten()
            .map(|f| f.saturating_sub(taken.get(&id).copied().unwrap_or(0)))
    };
    let eligible = |id: u64, with_capacity: bool| {
        w.free.contains_key(&id)
            && !w.unavailable.contains(&id)
            && to_node.is_none_or(|t| t == id)
            && (!with_capacity || capacity::fits(free_after(id), need))
    };
    let chosen =
        crate::process::choose_for(&w.nodes, &w.groups, g, index, failure_domain, &|id| {
            eligible(id, true)
        })?;
    match chosen {
        Some(m) => {
            p.to_node = Some(m.node_id);
            p.to_free = free_after(m.node_id);
            if p.to_free.is_none() {
                p.notes.push(format!(
                    "node {} does not report free space; capacity unchecked",
                    m.node_id
                ));
            }
        }
        None => {
            let without =
                crate::process::choose_for(&w.nodes, &w.groups, g, index, failure_domain, &|id| {
                    eligible(id, false)
                })?;
            p.blockers.push(match (without, to_node) {
                (Some(m), _) => format!(
                    "insufficient capacity: node {} has {} bytes free, the move needs {} (paused until a node has room)",
                    m.node_id,
                    free_after(m.node_id).unwrap_or(0),
                    capacity::needed(need)
                ),
                (None, Some(t)) => format!(
                    "requested node {t} is unreachable, draining, of another device class, or breaks the failure domain"
                ),
                (None, None) => "no reachable node of the device class in a free failure domain".into(),
            });
        }
    }
    Ok(p)
}

fn phase_blockers(w: &World) -> Vec<String> {
    match w.phase {
        Some(ClusterPhase::Active) | None => Vec::new(),
        Some(p) => vec![format!(
            "cluster is {}: operator moves progress only while Active (paused until then)",
            p.name()
        )],
    }
}

/// Dry run of `admin replace-member`.
///
/// # Errors
///
/// Metadata errors; an unknown group or index.
pub async fn plan_replace<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group: u64,
    index: u16,
    to_node: Option<u64>,
    failure_domain: &str,
    page: u32,
) -> Result<Plan, ControlError> {
    let w = world(meta, clients).await?;
    let Some(g) = w.groups.iter().find(|g| g.id == group).cloned() else {
        return Err(ControlError::Other(format!("group {group} unknown")));
    };
    if usize::from(index) >= g.members.len() {
        return Err(ControlError::Other(format!(
            "group {group} has no member {index}"
        )));
    }
    let mut blockers = phase_blockers(&w);
    if let Some(t) = to_node
        && g.members.iter().any(|m| m.node_id == t)
    {
        blockers.push(format!("node {t} is already a member of group {group}"));
    }
    let m = plan_move(
        &w,
        clients,
        &g,
        usize::from(index),
        to_node,
        failure_domain,
        page,
        &HashMap::new(),
    )
    .await?;
    Ok(Plan {
        phase: w.phase,
        moves: vec![m],
        blockers,
        notes: Vec::new(),
    })
}

/// Dry run of `admin decommission-node`: every group listing the node,
/// durable first, each target chosen as if the earlier moves had already
/// taken their room.
///
/// # Errors
///
/// Metadata errors; an unknown node.
pub async fn plan_decommission<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node: u64,
    failure_domain: &str,
    page: u32,
) -> Result<Plan, ControlError> {
    let w = world(meta, clients).await?;
    if !w.nodes.contains_key(&node) {
        return Err(ControlError::Other(format!("node {node} unknown")));
    }
    let mut blockers = phase_blockers(&w);
    if w.unavailable.contains(&node) {
        blockers.push(format!("node {node} is already draining or decommissioned"));
    }
    let mut groups: Vec<&GroupRecord> = w
        .groups
        .iter()
        .filter(|g| g.members.iter().any(|m| m.node_id == node))
        .collect();
    groups.sort_by_key(|g| (g.is_cache(), g.id));
    let mut taken: HashMap<u64, u64> = HashMap::new();
    let mut moves = Vec::new();
    for g in groups {
        let index = g
            .members
            .iter()
            .position(|m| m.node_id == node)
            .unwrap_or(0);
        let m = plan_move(&w, clients, g, index, None, failure_domain, page, &taken).await?;
        if let (Some(t), Some(s)) = (m.to_node, m.size) {
            *taken.entry(t).or_default() += capacity::needed(s.bytes);
        }
        moves.push(m);
    }
    let mut notes = Vec::new();
    if moves.is_empty() {
        notes.push(format!(
            "node {node} is in no group: nothing to move (it is decommissioned at once)"
        ));
    } else {
        notes.push(format!(
            "node {node} is decommissioned once no group lists it and no group is rebuilding; until then it stays a source"
        ));
    }
    Ok(Plan {
        phase: w.phase,
        moves,
        blockers,
        notes,
    })
}
