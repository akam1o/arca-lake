//! Cached cluster map: nodes and stripe groups, refreshed when the
//! metadata map version changes or when a node rejects an epoch.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
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
        Ok(Self {
            version,
            nodes,
            groups,
        })
    }

    /// Address of a node.
    #[must_use]
    pub fn node_addr(&self, node_id: u64) -> Option<&str> {
        self.nodes.get(&node_id).map(|n| n.address.as_str())
    }

    /// Active groups of a profile and tier.
    pub fn active_groups<'a>(
        &'a self,
        profile: &'a str,
        tier: &'a str,
    ) -> impl Iterator<Item = &'a GroupRecord> + 'a {
        self.groups.values().filter(move |g| {
            g.state == GroupState::Active && g.profile == profile && g.tier == tier
        })
    }
}
