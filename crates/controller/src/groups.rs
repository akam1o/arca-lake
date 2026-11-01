//! Stripe-group bootstrap from registered nodes.

use std::collections::{BTreeMap, BTreeSet};

use arca_lake_common::EcProfile;
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};
use arca_lake_placement::{Device, GroupSpec, choose_group_members};

use crate::ControlError;

/// Devices of all registered nodes, with membership counts from `groups`.
#[must_use]
pub fn devices_of(nodes: &[NodeRecord], groups: &[GroupRecord]) -> Vec<Device> {
    let mut counts: BTreeMap<(u64, u32), u32> = BTreeMap::new();
    for g in groups {
        for m in &g.members {
            *counts.entry((m.node_id, m.device_id)).or_default() += 1;
        }
    }
    let counts = &counts;
    nodes
        .iter()
        .flat_map(|n| {
            n.devices.iter().map(move |&d| Device {
                node_id: n.id,
                device_id: d,
                labels: n.labels.iter().cloned().collect(),
                free_bytes: u64::MAX, // capacity reporting is Phase 2
                group_count: counts.get(&(n.id, d)).copied().unwrap_or(0),
            })
        })
        .collect()
}

/// Create `count` groups for `profile` on `tier` (plus, if the profile has a
/// small-object policy, `count` replication groups), choosing members across
/// the profile's failure domain. Returns the created groups.
///
/// # Errors
///
/// Placement or metadata errors.
pub async fn create_groups<S: KvStore>(
    meta: &S,
    profile: &EcProfile,
    tier: &str,
    count: usize,
) -> Result<Vec<GroupRecord>, ControlError> {
    let nodes = cluster::list_nodes(meta).await?;
    let mut existing = cluster::list_groups(meta).await?;
    let mut created = Vec::new();

    let mut specs = vec![(
        profile.scheme.clone(),
        profile.data_shards,
        profile.parity_shards,
    )];
    if let Some(s) = &profile.small_object {
        specs.push((
            "replication-v1".to_string(),
            1,
            s.replicas.saturating_sub(1).max(1),
        ));
    }
    for (scheme, k, m) in specs {
        for _ in 0..count {
            let devices = devices_of(&nodes, &existing);
            let spec = GroupSpec {
                total_shards: usize::from(k + m),
                failure_domain: profile.failure_domain.clone(),
            };
            let members = choose_group_members(&devices, &spec, &BTreeSet::new())?;
            let group = GroupRecord {
                id: 0,
                profile: profile.name.clone(),
                tier: tier.to_string(),
                scheme: scheme.clone(),
                data_shards: k,
                parity_shards: m,
                members: members
                    .into_iter()
                    .map(|m| GroupMember {
                        node_id: m.node_id,
                        device_id: m.device_id,
                    })
                    .collect(),
                epoch: 0,
                state: GroupState::Active,
                repair_cursor: None,
            };
            let g = cluster::create_group(meta, group).await?;
            existing.push(g.clone());
            created.push(g);
        }
    }
    Ok(created)
}

/// Create one cache group (scheme `cache-v1`, one member) per device of
/// every node whose `device_class` label equals `device_class`, tagged with
/// `tier`. Nodes that already have a cache group on this tier are skipped.
///
/// # Errors
///
/// Metadata errors.
pub async fn create_cache_groups<S: KvStore>(
    meta: &S,
    tier: &str,
    device_class: &str,
) -> Result<Vec<GroupRecord>, ControlError> {
    let nodes = cluster::list_nodes(meta).await?;
    let existing = cluster::list_groups(meta).await?;
    let mut created = Vec::new();
    for n in nodes {
        let class = n
            .labels
            .iter()
            .find(|(k, _)| k == "device_class")
            .map(|(_, v)| v.as_str());
        if class != Some(device_class) {
            continue;
        }
        for &d in &n.devices {
            let member = GroupMember {
                node_id: n.id,
                device_id: d,
            };
            if existing.iter().any(|g| {
                g.scheme == arca_lake_tiering::cache::CACHE_SCHEME
                    && g.tier == tier
                    && g.members.first() == Some(&member)
            }) {
                continue;
            }
            let g = cluster::create_group(
                meta,
                GroupRecord {
                    id: 0,
                    profile: "cache".into(),
                    tier: tier.to_string(),
                    scheme: arca_lake_tiering::cache::CACHE_SCHEME.into(),
                    data_shards: 1,
                    parity_shards: 0,
                    members: vec![member],
                    epoch: 0,
                    state: GroupState::Active,
                    repair_cursor: None,
                },
            )
            .await?;
            created.push(g);
        }
    }
    Ok(created)
}
