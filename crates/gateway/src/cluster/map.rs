//! Cached cluster map: nodes and stripe groups, refreshed when the
//! metadata map version changes or when a node rejects an epoch.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::membership::{self, NodeLifecycle};
use arca_lake_metadata::records::{GroupRecord, GroupState, NodeRecord};

/// Snapshot of nodes and groups.
#[derive(Debug, Clone, Default)]
pub struct ClusterMap {
    /// Map version this snapshot corresponds to.
    pub version: u64,
    /// Nodes by ID.
    pub nodes: HashMap<u64, NodeRecord>,
    /// Groups by ID.
    pub groups: HashMap<u64, GroupRecord>,
    /// Per group being rebuilt, alternate readers of the member being
    /// rebuilt: `(shard index, node it left)`. The node keeps its shards
    /// until the group is `Active` again (R-65).
    pub alternates: HashMap<u64, Vec<(usize, u64)>>,
}

impl ClusterMap {
    /// Load the current map.
    ///
    /// # Errors
    ///
    /// Metadata errors.
    pub async fn load<S: KvStore>(meta: &S) -> Result<Self, arca_lake_metadata::MetaError> {
        let version = cluster::map_version(meta).await?;
        let nodes = cluster::list_nodes(meta)
            .await?
            .into_iter()
            .map(|n| (n.id, n))
            .collect();
        let groups = cluster::list_groups(meta)
            .await?
            .into_iter()
            .map(|g| (g.id, g))
            .collect();
        let alternates = alternates(meta, &groups).await?;
        Ok(Self {
            version,
            nodes,
            groups,
            alternates,
        })
    }

    /// Address of a node.
    #[must_use]
    pub fn node_addr(&self, node_id: u64) -> Option<&str> {
        self.nodes.get(&node_id).map(|n| n.address.as_str())
    }

    /// Groups of a profile and tier that accept writes (`Active`, and
    /// `Repairing`: the replacement member receives new shards directly).
    pub fn active_groups<'a>(
        &'a self,
        profile: &'a str,
        tier: &'a str,
    ) -> impl Iterator<Item = &'a GroupRecord> + 'a {
        self.groups.values().filter(move |g| {
            matches!(g.state, GroupState::Active | GroupState::Repairing)
                && g.profile == profile
                && g.tier == tier
        })
    }
}

/// The node each member being rebuilt left: a replace request's
/// `from_node` for the group, or a draining node, when it is no longer a
/// member (the controller's own sources, `RepairOptions::former`).
async fn alternates<S: KvStore>(
    meta: &S,
    groups: &HashMap<u64, GroupRecord>,
) -> Result<HashMap<u64, Vec<(usize, u64)>>, arca_lake_metadata::MetaError> {
    let rebuilding: Vec<&GroupRecord> = groups
        .values()
        .filter(|g| g.state == GroupState::Repairing && g.repair_cursor.is_some())
        .collect();
    let mut out = HashMap::new();
    if rebuilding.is_empty() {
        return Ok(out);
    }
    let requests = membership::replace_requests(meta).await?;
    let draining: Vec<u64> = membership::node_states(meta)
        .await?
        .into_iter()
        .filter(|s| matches!(s.state, NodeLifecycle::Draining { .. }))
        .map(|s| s.node_id)
        .collect();
    for g in rebuilding {
        let Some(cursor) = g.repair_cursor else {
            continue;
        };
        let index = usize::from(cursor.member);
        let nodes: Vec<(usize, u64)> = requests
            .iter()
            .filter(|r| r.group == g.id)
            .map(|r| r.from_node)
            .chain(draining.iter().copied())
            .filter(|n| !g.members.iter().any(|m| m.node_id == *n))
            .map(|n| (index, n))
            .collect();
        if !nodes.is_empty() {
            out.insert(g.id, nodes);
        }
    }
    Ok(out)
}
