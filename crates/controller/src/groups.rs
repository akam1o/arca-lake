//! Stripe-group bootstrap from registered nodes.

use std::collections::{BTreeMap, BTreeSet};

use arca_lake_common::EcProfile;
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupRole, GroupState, NodeRecord};
use arca_lake_placement::{Device, GroupSpec, choose_group_members};

use crate::ControlError;

/// Registered nodes that may receive new groups: active ones only (not
/// draining, decommissioned, removed or quarantined; Phase 3.11, R-100).
///
/// # Errors
///
/// Metadata errors.
pub async fn placeable_nodes<S: KvStore>(meta: &S) -> Result<Vec<NodeRecord>, ControlError> {
    let states = arca_lake_metadata::ops::node_lifecycle::states(meta).await?;
    Ok(cluster::list_nodes(meta)
        .await?
        .into_iter()
        .filter(|n| {
            states
                .get(&n.id)
                .is_none_or(|s| *s == arca_lake_metadata::ops::node_lifecycle::State::Active)
        })
        .collect())
}

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
/// the profile's failure domain from every registered node. Returns the
/// created groups. Each group's failure domain is recorded with it
/// (ADR-0038).
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
    let nodes = placeable_nodes(meta).await?;
    create_groups_from(meta, profile, tier, count, nodes).await
}

/// [`create_groups`] restricted to nodes whose `device_class` label equals
/// `device_class` (one durable tier per device class, RFC-006 §5.1).
///
/// # Errors
///
/// Placement or metadata errors.
pub async fn create_groups_for_class<S: KvStore>(
    meta: &S,
    profile: &EcProfile,
    tier: &str,
    device_class: &str,
    count: usize,
) -> Result<Vec<GroupRecord>, ControlError> {
    let nodes = placeable_nodes(meta)
        .await?
        .into_iter()
        .filter(|n| {
            n.labels
                .iter()
                .any(|(k, v)| k == "device_class" && v == device_class)
        })
        .collect();
    create_groups_from(meta, profile, tier, count, nodes).await
}

async fn create_groups_from<S: KvStore>(
    meta: &S,
    profile: &EcProfile,
    tier: &str,
    count: usize,
    nodes: Vec<NodeRecord>,
) -> Result<Vec<GroupRecord>, ControlError> {
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
                role: GroupRole::Durable,
            };
            let g = cluster::create_group_in_domain(
                meta,
                group,
                &profile.failure_domain,
                crate::process::now_ms(),
            )
            .await?;
            existing.push(g.clone());
            created.push(g);
        }
    }
    Ok(created)
}

/// Cache protection mode of a cache tier (RFC-006 §5.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheProtection {
    /// One raw copy per chunk on one device (scheme `cache-v1`).
    None,
    /// `replicas` byte-identical copies across the failure domain
    /// (scheme `replication-v1`, `1 + (replicas - 1)`).
    Replication {
        /// Total copies (≥ 2).
        replicas: u16,
    },
    /// Erasure coding with any `ErasureCoder` scheme.
    ErasureCoding {
        /// Scheme identifier (`rs-gf8-v1`, `rs-leopard-gf16-v1`).
        scheme: String,
        /// `k`.
        data_shards: u16,
        /// `m`.
        parity_shards: u16,
    },
}

impl CacheProtection {
    /// `(scheme, k, m)` of groups in this mode.
    ///
    /// # Errors
    ///
    /// Invalid parameters (`replicas < 2`, `k == 0`, `m == 0`, unknown
    /// scheme).
    pub fn spec(&self) -> Result<(String, u16, u16), ControlError> {
        match self {
            Self::None => Ok((arca_lake_tiering::cache::CACHE_SCHEME.into(), 1, 0)),
            Self::Replication { replicas } => {
                if *replicas < 2 {
                    return Err(ControlError::Other(
                        "cache replication needs at least 2 copies".into(),
                    ));
                }
                Ok(("replication-v1".into(), 1, replicas - 1))
            }
            Self::ErasureCoding {
                scheme,
                data_shards,
                parity_shards,
            } => {
                if arca_lake_ec::SchemeId::parse(scheme).is_none() {
                    return Err(ControlError::Other(format!("unknown EC scheme {scheme}")));
                }
                if *data_shards == 0 || *parity_shards == 0 {
                    return Err(ControlError::Other(
                        "cache erasure coding needs k >= 1 and m >= 1".into(),
                    ));
                }
                Ok((scheme.clone(), *data_shards, *parity_shards))
            }
        }
    }
}

/// Create cache groups of `tier` from the devices of every node whose
/// `device_class` label equals `device_class` (RFC-006 §5.1, §5.9).
///
/// - `CacheProtection::None`: one single-member group per device that has
///   none yet on this tier (`count` is ignored).
/// - Replication and erasure coding: `count` groups of `k + m` members
///   chosen across `failure_domain`.
///
/// # Errors
///
/// Invalid protection parameters, placement, or metadata errors.
pub async fn create_cache_groups<S: KvStore>(
    meta: &S,
    tier: &str,
    device_class: &str,
    protection: &CacheProtection,
    failure_domain: &str,
    count: usize,
) -> Result<Vec<GroupRecord>, ControlError> {
    let (scheme, k, m) = protection.spec()?;
    let nodes: Vec<NodeRecord> = placeable_nodes(meta)
        .await?
        .into_iter()
        .filter(|n| {
            n.labels
                .iter()
                .any(|(key, v)| key == "device_class" && v == device_class)
        })
        .collect();
    let mut existing = cluster::list_groups(meta).await?;
    let mut created = Vec::new();
    let record = |members: Vec<GroupMember>| GroupRecord {
        id: 0,
        profile: "cache".into(),
        tier: tier.to_string(),
        scheme: scheme.clone(),
        data_shards: k,
        parity_shards: m,
        members,
        epoch: 0,
        state: GroupState::Active,
        repair_cursor: None,
        role: GroupRole::Cache,
    };
    if *protection == CacheProtection::None {
        for n in &nodes {
            for &d in &n.devices {
                let member = GroupMember {
                    node_id: n.id,
                    device_id: d,
                };
                if existing
                    .iter()
                    .any(|g| g.is_cache() && g.tier == tier && g.members.first() == Some(&member))
                {
                    continue;
                }
                let g = cluster::create_group(meta, record(vec![member])).await?;
                existing.push(g.clone());
                created.push(g);
            }
        }
        return Ok(created);
    }
    for _ in 0..count {
        let devices = devices_of(&nodes, &existing);
        let spec = GroupSpec {
            total_shards: usize::from(k + m),
            failure_domain: failure_domain.to_string(),
        };
        let members = choose_group_members(&devices, &spec, &BTreeSet::new())?
            .into_iter()
            .map(|m| GroupMember {
                node_id: m.node_id,
                device_id: m.device_id,
            })
            .collect();
        let g = cluster::create_group_in_domain(
            meta,
            record(members),
            failure_domain,
            crate::process::now_ms(),
        )
        .await?;
        existing.push(g.clone());
        created.push(g);
    }
    Ok(created)
}
