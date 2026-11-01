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
    transact(store, async |tx| {
        Ok(tx
            .get(&keys::map_version())
            .await?
            .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
            .map_or(0, u64::from_le_bytes))
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
    transact(store, async |tx| {
        let existing = tx
            .get_range(&KeyRange::prefix(&keys::nodes_prefix()), usize::MAX, false)
            .await?
            .into_iter()
            .map(|(_, v)| decode::<NodeRecord>(&v))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .find(|n| n.address == address);
        let id = match &existing {
            Some(n) => n.id,
            None => next_id(tx, &keys::node_counter()).await?,
        };
        let record = NodeRecord {
            id,
            address: address.to_string(),
            labels: labels.clone(),
            devices: devices.clone(),
            registered_ms: existing.as_ref().map_or(now_ms, |n| n.registered_ms),
            last_seen_ms: now_ms,
        };
        write(tx, &keys::node(id), &record)?;
        bump_map_version(tx).await?;
        Ok(record)
    })
    .await
}

/// All registered nodes.
///
/// # Errors
///
/// Backend errors.
pub async fn list_nodes<S: KvStore>(store: &S) -> Result<Vec<NodeRecord>> {
    transact(store, async |tx| {
        tx.get_range(&KeyRange::prefix(&keys::nodes_prefix()), usize::MAX, false)
            .await?
            .into_iter()
            .map(|(_, v)| decode::<NodeRecord>(&v))
            .collect()
    })
    .await
}

/// Create a stripe group. `group.id` and `group.epoch` are assigned here
/// (epoch starts at 1).
///
/// # Errors
///
/// Backend errors.
pub async fn create_group<S: KvStore>(store: &S, mut group: GroupRecord) -> Result<GroupRecord> {
    transact(store, async |tx| {
        group.id = next_id(tx, &keys::group_counter()).await?;
        group.epoch = 1;
        write(tx, &keys::group(group.id), &group)?;
        bump_map_version(tx).await?;
        Ok(group.clone())
    })
    .await
}

/// All stripe groups.
///
/// # Errors
///
/// Backend errors.
pub async fn list_groups<S: KvStore>(store: &S) -> Result<Vec<GroupRecord>> {
    transact(store, async |tx| {
        tx.get_range(&KeyRange::prefix(&keys::groups_prefix()), usize::MAX, false)
            .await?
            .into_iter()
            .map(|(_, v)| decode::<GroupRecord>(&v))
            .collect()
    })
    .await
}

/// One stripe group.
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`].
pub async fn get_group<S: KvStore>(store: &S, id: u64) -> Result<GroupRecord> {
    transact(store, async |tx| {
        read(tx, &keys::group(id))
            .await?
            .ok_or(MetaError::NoSuchGroup(id))
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
pub async fn update_group<S: KvStore>(
    store: &S,
    id: u64,
    change: impl Fn(&mut GroupRecord) -> bool,
) -> Result<GroupRecord> {
    transact(store, async |tx| {
        let mut group: GroupRecord = read(tx, &keys::group(id))
            .await?
            .ok_or(MetaError::NoSuchGroup(id))?;
        if change(&mut group) {
            write(tx, &keys::group(id), &group)?;
            bump_map_version(tx).await?;
        }
        Ok(group)
    })
    .await
}
