//! Cluster map: storage nodes and stripe groups (RFC-004 §5.12).
//!
//! Every mutation bumps the map version so that gateways can poll one key
//! to learn that their cached cluster map is stale.

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GroupRecord, NodeRecord, decode};
use crate::{MetaError, keys, transact};

async fn bump_map_version<X: Txn>(tx: &mut X) -> Result<u64> {
    let next = tx
        .get(&keys::map_version())
        .await?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map_or(1, |b| u64::from_le_bytes(b) + 1);
    tx.set(&keys::map_version(), &next.to_le_bytes());
    Ok(next)
}

/// [`bump_map_version`] for other modules' transactions (gateways and
/// controllers refresh their view of nodes and groups on a change).
pub(crate) async fn bump_map_version_in_tx<X: Txn>(tx: &mut X) -> Result<u64> {
    bump_map_version(tx).await
}

async fn next_id<X: Txn>(tx: &mut X, counter: &[u8]) -> Result<u64> {
    let next = tx
        .get(counter)
        .await?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map_or(1, |b| u64::from_le_bytes(b) + 1);
    tx.set(counter, &next.to_le_bytes());
    Ok(next)
}

/// Current cluster-map version (0 when nothing was ever registered).
///
/// # Errors
///
/// Backend errors.
pub async fn map_version<S: KvStore>(store: &S) -> Result<u64> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get(&keys::map_version())
                .await?
                .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
                .map_or(0, u64::from_le_bytes))
        })
    })
    .await
}

/// Register a storage node, or update its record if a node with the same
/// address exists. Returns the record with its ID.
///
/// # Errors
///
/// Backend errors.
pub async fn register_node<S: KvStore>(
    store: &S,
    address: &str,
    labels: Vec<(String, String)>,
    devices: Vec<u32>,
    now_ms: u64,
) -> Result<NodeRecord> {
    transact(store, (address, labels, devices, now_ms), |tx, env| {
        Box::pin(async move {
            crate::ops::node_lifecycle::no_restore_in_progress(tx).await?;
            let (address, labels, devices, now_ms) = env;
            let all = tx
                .get_range(&KeyRange::prefix(&keys::nodes_prefix()), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(_, v)| decode::<NodeRecord>(&v))
                .collect::<Result<Vec<_>>>()?;
            let existing = all.iter().find(|n| n.address == *address).cloned();
            // No new node while a finalize is past its cut-off: its last
            // step waits for every node to hold the new floor and format,
            // and a node added meanwhile could escape it (Phase 3.10
            // review). Re-registering a known node is allowed.
            if existing.is_none() {
                let cursor: Option<crate::records::UpgradeCursor> =
                    read(tx, &keys::upgrade_cursor()).await?;
                if cursor.is_some_and(|c| {
                    !matches!(
                        c.step,
                        crate::records::UpgradeStep::Requested
                            | crate::records::UpgradeStep::Preconditioned
                            | crate::records::UpgradeStep::Migrated
                    )
                }) {
                    return Err(crate::MetaError::InvalidArgument(
                        "an upgrade finalize is past its cut-off: register the node once it completes".into(),
                    ));
                }
            }
            // A node taken out comes back only through `admin node
            // rejoin`, after its checks (Phase 3.11, R-100).
            if let Some(n) = &existing {
                let state = crate::ops::node_lifecycle::state_in_tx(tx, n.id).await?;
                if !state.in_service() {
                    return Err(crate::MetaError::InvalidArgument(format!(
                        "node {} at {address} is {}: bring it back with `admin node rejoin`",
                        n.id,
                        state.name()
                    )));
                }
            }
            missing_domain_labels(tx, &all, existing.as_ref().map(|n| n.id), address, labels)
                .await?;
            let id = if let Some(n) = &existing {
                n.id
            } else {
                let id = next_id(tx, &keys::node_counter()).await?;
                crate::ops::node_lifecycle::new_identity_in_tx(tx, id).await?;
                id
            };
            let record = NodeRecord {
                id,
                address: (*address).to_string(),
                labels: labels.clone(),
                devices: devices.clone(),
                registered_ms: existing.as_ref().map_or(*now_ms, |n| n.registered_ms),
                last_seen_ms: *now_ms,
            };
            write(tx, &keys::node(id), &record)?;
            bump_map_version(tx).await?;
            Ok(record)
        })
    })
    .await
}

/// Refuse a node registration without a label that the recorded failure
/// domain of a group it could join requires (ADR-0038): a group whose
/// members have its `device_class` (a replacement never crosses device
/// classes), or every group when it is a member already. Such a node
/// could never be checked against the group's other members' domains.
async fn missing_domain_labels<X: crate::kv::Txn>(
    tx: &mut X,
    all: &[NodeRecord],
    id: Option<u64>,
    address: &str,
    labels: &[(String, String)],
) -> Result<()> {
    let domains = crate::ops::group_domain::list_in_tx(tx).await?;
    if domains.is_empty() {
        return Ok(());
    }
    let class_of = |l: &[(String, String)]| {
        crate::ops::group_domain::label_value(l, "device_class").map(str::to_string)
    };
    let own_class = class_of(labels);
    let groups = tx
        .get_range(&KeyRange::prefix(&keys::groups_prefix()), usize::MAX, false)
        .await?
        .into_iter()
        .map(|(_, v)| decode::<GroupRecord>(&v))
        .collect::<Result<Vec<_>>>()?;
    let mut needed: std::collections::BTreeMap<&str, Vec<u64>> = std::collections::BTreeMap::new();
    for g in groups.iter().filter(|g| g.members.len() > 1) {
        let Some(d) = domains.get(&g.id) else {
            continue;
        };
        if crate::ops::group_domain::label_value(labels, &d.label).is_some() {
            continue;
        }
        let member = id.is_some_and(|id| g.members.iter().any(|m| m.node_id == id));
        let joinable = g.members.iter().any(|m| {
            all.iter()
                .find(|n| n.id == m.node_id)
                .is_some_and(|n| class_of(&n.labels) == own_class)
        });
        if member || joinable {
            needed.entry(d.label.as_str()).or_default().push(g.id);
        }
    }
    if let Some((label, groups)) = needed.into_iter().next() {
        return Err(crate::MetaError::InvalidArgument(format!(
            "node at {address} has no '{label}' label, which the recorded failure domain of groups {groups:?} requires: register it with --label {label}=<value>"
        )));
    }
    Ok(())
}

/// All registered nodes.
///
/// # Errors
///
/// Backend errors.
pub async fn list_nodes<S: KvStore>(store: &S) -> Result<Vec<NodeRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(&KeyRange::prefix(&keys::nodes_prefix()), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(_, v)| decode::<NodeRecord>(&v))
                .collect()
        })
    })
    .await
}

/// Create a stripe group. `group.id` and `group.epoch` are assigned here
/// (epoch starts at 1).
///
/// # Errors
///
/// Backend errors.
pub async fn create_group<S: KvStore>(store: &S, group: GroupRecord) -> Result<GroupRecord> {
    transact(store, group, |tx, group| {
        Box::pin(async move {
            let mut g = group.clone();
            g.id = next_id(tx, &keys::group_counter()).await?;
            g.epoch = 1;
            write(tx, &keys::group(g.id), &g)?;
            bump_map_version(tx).await?;
            Ok(g)
        })
    })
    .await
}

/// [`create_group`] with its required failure domain `label` recorded in
/// the same transaction (ADR-0038): a group never exists without it.
///
/// # Errors
///
/// Backend errors.
pub async fn create_group_in_domain<S: KvStore>(
    store: &S,
    group: GroupRecord,
    label: &str,
    now_ms: u64,
) -> Result<GroupRecord> {
    transact(store, (group, label, now_ms), |tx, env| {
        Box::pin(async move {
            let (group, label, now_ms) = env;
            let mut g = group.clone();
            g.id = next_id(tx, &keys::group_counter()).await?;
            g.epoch = 1;
            write(tx, &keys::group(g.id), &g)?;
            crate::ops::group_domain::put_in_tx(
                tx,
                &crate::ops::group_domain::GroupDomain {
                    group: g.id,
                    label: (*label).to_string(),
                    source: crate::ops::group_domain::DomainSource::Created,
                    set_ms: *now_ms,
                },
            )?;
            bump_map_version(tx).await?;
            Ok(g)
        })
    })
    .await
}

/// All stripe groups.
///
/// # Errors
///
/// Backend errors.
pub async fn list_groups<S: KvStore>(store: &S) -> Result<Vec<GroupRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(&KeyRange::prefix(&keys::groups_prefix()), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(_, v)| decode::<GroupRecord>(&v))
                .collect()
        })
    })
    .await
}

/// One stripe group.
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`].
pub async fn get_group<S: KvStore>(store: &S, id: u64) -> Result<GroupRecord> {
    transact(store, id, |tx, id| {
        Box::pin(async move {
            read(tx, &keys::group(*id))
                .await?
                .ok_or(MetaError::NoSuchGroup(*id))
        })
    })
    .await
}

/// Apply `change` to a group record in one transaction. Returning `false`
/// from `change` leaves the record untouched. The epoch is **not** bumped
/// automatically; membership changes must do it (RFC-004 §5.4).
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`].
pub async fn update_group<S: KvStore, F>(store: &S, id: u64, change: F) -> Result<GroupRecord>
where
    F: Fn(&mut GroupRecord) -> bool + Send + Sync,
{
    transact(store, (id, change), |tx, env| {
        Box::pin(async move {
            let (id, change) = env;
            let mut group: GroupRecord = read(tx, &keys::group(*id))
                .await?
                .ok_or(MetaError::NoSuchGroup(*id))?;
            if change(&mut group) {
                write(tx, &keys::group(*id), &group)?;
                bump_map_version(tx).await?;
            }
            Ok(group)
        })
    })
    .await
}

/// Record that a node answered a health probe at `now_ms`. Does not bump
/// the map version (gateways do not care about liveness timestamps).
///
/// # Errors
///
/// Backend errors; an unknown node is ignored.
pub async fn touch_node<S: KvStore>(store: &S, node_id: u64, now_ms: u64) -> Result<()> {
    transact(store, (node_id, now_ms), |tx, env| {
        Box::pin(async move {
            let (node_id, now_ms) = *env;
            let Some(mut record) = read::<NodeRecord, _>(tx, &keys::node(node_id)).await? else {
                return Ok(());
            };
            if record.last_seen_ms < now_ms {
                record.last_seen_ms = now_ms;
                write(tx, &keys::node(node_id), &record)?;
            }
            Ok(())
        })
    })
    .await
}
