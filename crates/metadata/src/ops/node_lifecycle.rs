//! Node lifecycle beyond decommission (Phase 3.11, R-100).
//!
//! `membership` records draining and decommissioned nodes. Two states
//! complete the lifecycle, kept under a key of their own
//! (`("al","nretire",id)`) that earlier releases do not read:
//!
//! - **Removed** (`admin node unregister`): a decommissioned node taken out
//!   for good. Its record stays (its id is never reused and its address
//!   cannot register again by itself); no controller talks to it.
//! - **Quarantined**: a node whose identity does not match what the
//!   cluster gave it (another cluster's disk, a reused id, a wiped or
//!   swapped disk at a member's address), or that failed the rejoin
//!   checks. It counts as unhealthy (members on it are repaired away) and
//!   receives nothing.
//!
//! Only `admin node rejoin` brings either back, as a node without members.
//!
//! The identity (`("al","nident",id)`) is a random token the leader gives
//! a node once; the node keeps it on its disk and reports it, so that a
//! disk that is not the one the cluster registered is recognised.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::membership::{NodeLifecycle, NodeStateRecord};
use crate::ops::{read, write};
use crate::records::{GroupRecord, Record, decode};
use crate::{MetaError, keys, transact};

/// Why a node is out of the cluster beyond decommission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Retirement {
    /// Unregistered by an operator.
    Removed {
        /// When.
        since_ms: u64,
        /// Who.
        by: String,
        /// Why (operator's words).
        reason: String,
    },
    /// Isolated until an operator acts.
    Quarantined {
        /// When.
        since_ms: u64,
        /// What was found.
        reason: String,
    },
}

/// `("al","nretire",node_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRetirementRecord {
    /// Node.
    pub node_id: u64,
    /// State.
    pub state: Retirement,
}

impl Record for NodeRetirementRecord {}

impl NodeRetirementRecord {
    /// Whether the node was unregistered.
    #[must_use]
    pub fn is_removed(&self) -> bool {
        matches!(self.state, Retirement::Removed { .. })
    }
}

/// `("al","nident",node_id)`: the identity token given to the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentityRecord {
    /// Node.
    pub node_id: u64,
    /// Random, drawn once per registration of the id.
    pub token: [u8; 16],
    /// The node reported holding it on its disk.
    pub acknowledged: bool,
}

impl Record for NodeIdentityRecord {}

/// A node's lifecycle as one value (for listings and checks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Serving.
    Active,
    /// Being emptied.
    Draining,
    /// Emptied; may be unregistered.
    Decommissioned,
    /// Unregistered.
    Removed,
    /// Isolated.
    Quarantined,
}

impl State {
    /// Whether the node may hold members, be chosen as a target and take
    /// deletes.
    #[must_use]
    pub fn in_service(self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }

    /// Lower-case name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Decommissioned => "decommissioned",
            Self::Removed => "removed",
            Self::Quarantined => "quarantined",
        }
    }
}

/// The state from the two records.
#[must_use]
pub fn state_of(
    lifecycle: Option<&NodeStateRecord>,
    retirement: Option<&NodeRetirementRecord>,
) -> State {
    match (retirement.map(|r| &r.state), lifecycle.map(|l| &l.state)) {
        (Some(Retirement::Removed { .. }), _) => State::Removed,
        (Some(Retirement::Quarantined { .. }), _) => State::Quarantined,
        (None, Some(NodeLifecycle::Decommissioned { .. })) => State::Decommissioned,
        (None, Some(NodeLifecycle::Draining { .. })) => State::Draining,
        (None, None) => State::Active,
    }
}

async fn read_state<X: Txn>(tx: &mut X, node_id: u64) -> Result<State> {
    let l: Option<NodeStateRecord> = read(tx, &keys::node_state(node_id)).await?;
    let r: Option<NodeRetirementRecord> = read(tx, &keys::node_retirement(node_id)).await?;
    Ok(state_of(l.as_ref(), r.as_ref()))
}

/// The state of one node.
///
/// # Errors
///
/// Backend errors.
pub async fn state<S: KvStore>(store: &S, node_id: u64) -> Result<State> {
    transact(store, node_id, |tx, node_id| {
        Box::pin(async move { read_state(tx, *node_id).await })
    })
    .await
}

/// In a transaction: the state of one node.
///
/// # Errors
///
/// Backend errors.
pub async fn state_in_tx<X: Txn>(tx: &mut X, node_id: u64) -> Result<State> {
    read_state(tx, node_id).await
}

/// Every retirement record.
///
/// # Errors
///
/// Backend errors.
pub async fn retirements<S: KvStore>(store: &S) -> Result<Vec<NodeRetirementRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(&keys::node_retirements_range(), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(_, v)| decode::<NodeRetirementRecord>(&v))
                .collect()
        })
    })
    .await
}

/// Groups listing `node_id` as a member.
async fn groups_listing<X: Txn>(tx: &mut X, node_id: u64) -> Result<Vec<u64>> {
    Ok(tx
        .get_range(&KeyRange::prefix(&keys::groups_prefix()), usize::MAX, false)
        .await?
        .into_iter()
        .map(|(_, v)| decode::<GroupRecord>(&v))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|g| g.members.iter().any(|m| m.node_id == node_id))
        .map(|g| g.id)
        .collect())
}

/// Replacement requests naming `node_id` (from or to).
async fn requests_naming<X: Txn>(tx: &mut X, node_id: u64) -> Result<Vec<u64>> {
    Ok(tx
        .get_range(&keys::replace_requests_range(), usize::MAX, false)
        .await?
        .into_iter()
        .map(|(_, v)| decode::<crate::ops::membership::ReplaceRequest>(&v))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|r| r.from_node == node_id || r.to_node == Some(node_id))
        .map(|r| r.group)
        .collect())
}

/// Unregister a node: only a decommissioned (or quarantined) node that no
/// group lists and no replacement names. Idempotent: an already removed
/// node returns its record.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when the node is unknown, in service, or
/// still referenced.
pub async fn unregister<S: KvStore>(
    store: &S,
    node_id: u64,
    by: &str,
    reason: &str,
    now_ms: u64,
) -> Result<NodeRetirementRecord> {
    transact(
        store,
        (node_id, by.to_string(), reason.to_string(), now_ms),
        |tx, env| {
            Box::pin(async move {
            no_restore_in_progress(tx).await?;
                let (node_id, by, reason, now_ms) = env;
                let node_id = *node_id;
                if tx.get(&keys::node(node_id)).await?.is_none() {
                    return Err(MetaError::InvalidArgument(format!("node {node_id} unknown")));
                }
                let cur: Option<NodeRetirementRecord> =
                    read(tx, &keys::node_retirement(node_id)).await?;
                if let Some(r) = cur.as_ref().filter(|r| r.is_removed()) {
                    return Ok(r.clone());
                }
                match read_state(tx, node_id).await? {
                    State::Decommissioned | State::Quarantined => {}
                    s => {
                        return Err(MetaError::InvalidArgument(format!(
                            "node {node_id} is {}: decommission it first (`admin node decommission`)",
                            s.name()
                        )));
                    }
                }
                let groups = groups_listing(tx, node_id).await?;
                if !groups.is_empty() {
                    return Err(MetaError::InvalidArgument(format!(
                        "node {node_id} is still a member of groups {groups:?}"
                    )));
                }
                let requests = requests_naming(tx, node_id).await?;
                if !requests.is_empty() {
                    return Err(MetaError::InvalidArgument(format!(
                        "replacement requests of groups {requests:?} name node {node_id}"
                    )));
                }
                let r = NodeRetirementRecord {
                    node_id,
                    state: Retirement::Removed {
                        since_ms: *now_ms,
                        by: by.clone(),
                        reason: reason.clone(),
                    },
                };
                write(tx, &keys::node_retirement(node_id), &r)?;
                crate::ops::cluster::bump_map_version_in_tx(tx).await?;
                Ok(r)
            })
        },
    )
    .await
}

/// Record a node removed again on metadata restored from before its
/// removal (Phase 3.11 review: `admin restore` keeps what was unregistered
/// out). Idempotent; does nothing for an unknown node.
///
/// # Errors
///
/// Backend errors.
pub async fn keep_removed<S: KvStore>(store: &S, node_id: u64, now_ms: u64) -> Result<()> {
    transact(store, (node_id, now_ms), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, now_ms) = *env;
            if tx.get(&keys::node(node_id)).await?.is_none() {
                return Ok(());
            }
            let r = NodeRetirementRecord {
                node_id,
                state: Retirement::Removed {
                    since_ms: now_ms,
                    by: "admin restore".into(),
                    reason: "unregistered before the restore".into(),
                },
            };
            write(tx, &keys::node_retirement(node_id), &r)?;
            crate::ops::cluster::bump_map_version_in_tx(tx).await?;
            Ok(())
        })
    })
    .await
}

/// Quarantine a node. Idempotent (the first reason is kept); a removed
/// node stays removed. Returns whether the record changed.
///
/// # Errors
///
/// Backend errors.
pub async fn quarantine<S: KvStore>(
    store: &S,
    node_id: u64,
    reason: &str,
    now_ms: u64,
) -> Result<bool> {
    transact(store, (node_id, reason.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, reason, now_ms) = env;
            let cur: Option<NodeRetirementRecord> =
                read(tx, &keys::node_retirement(*node_id)).await?;
            if cur.is_some() {
                return Ok(false);
            }
            let r = NodeRetirementRecord {
                node_id: *node_id,
                state: Retirement::Quarantined {
                    since_ms: *now_ms,
                    reason: reason.clone(),
                },
            };
            write(tx, &keys::node_retirement(*node_id), &r)?;
            crate::ops::cluster::bump_map_version_in_tx(tx).await?;
            Ok(true)
        })
    })
    .await
}

/// Bring a removed, quarantined or decommissioned node back as an active
/// node without members. The caller checked the node itself (versions,
/// floors, incarnation, identity) first; `fresh_identity` drops its
/// identity so that a new one is given (an emptied disk).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when the node is unknown, in service, or
/// still a member somewhere.
pub async fn rejoin<S: KvStore>(store: &S, node_id: u64, fresh_identity: bool) -> Result<State> {
    transact(store, (node_id, fresh_identity), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, fresh) = *env;
            if tx.get(&keys::node(node_id)).await?.is_none() {
                return Err(MetaError::InvalidArgument(format!(
                    "node {node_id} unknown"
                )));
            }
            let was = read_state(tx, node_id).await?;
            if was.in_service() {
                return Err(MetaError::InvalidArgument(format!(
                    "node {node_id} is {}: nothing to rejoin",
                    was.name()
                )));
            }
            let groups = groups_listing(tx, node_id).await?;
            if !groups.is_empty() {
                return Err(MetaError::InvalidArgument(format!(
                    "node {node_id} is still a member of groups {groups:?}"
                )));
            }
            tx.clear(&keys::node_retirement(node_id));
            tx.clear(&keys::node_state(node_id));
            if fresh {
                tx.clear(&keys::node_identity(node_id));
            }
            crate::ops::cluster::bump_map_version_in_tx(tx).await?;
            Ok(was)
        })
    })
    .await
}

/// The identity given to a node, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn identity<S: KvStore>(store: &S, node_id: u64) -> Result<Option<NodeIdentityRecord>> {
    transact(store, node_id, |tx, node_id| {
        Box::pin(async move { read(tx, &keys::node_identity(*node_id)).await })
    })
    .await
}

/// The identity of a node, drawn now if it has none.
///
/// # Errors
///
/// Backend errors.
pub async fn ensure_identity<S: KvStore>(store: &S, node_id: u64) -> Result<NodeIdentityRecord> {
    transact(store, node_id, |tx, node_id| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            if let Some(r) =
                read::<NodeIdentityRecord, _>(tx, &keys::node_identity(*node_id)).await?
            {
                return Ok(r);
            }
            let r = NodeIdentityRecord {
                node_id: *node_id,
                token: crate::records::new_gen_id(),
                acknowledged: false,
            };
            write(tx, &keys::node_identity(*node_id), &r)?;
            Ok(r)
        })
    })
    .await
}

/// Take the identity a node holds into the metadata (acknowledged), when
/// the metadata has none for it (Phase 3.11 review: metadata restored
/// from before the node had one). Never replaces an existing record.
/// Returns whether it was taken.
///
/// # Errors
///
/// Backend errors.
pub async fn import_identity<S: KvStore>(store: &S, node_id: u64, token: [u8; 16]) -> Result<bool> {
    transact(store, (node_id, token), |tx, env| {
        Box::pin(async move {
            let (node_id, token) = *env;
            if tx.get(&keys::node_identity(node_id)).await?.is_some() {
                return Ok(false);
            }
            write(
                tx,
                &keys::node_identity(node_id),
                &NodeIdentityRecord {
                    node_id,
                    token,
                    acknowledged: true,
                },
            )?;
            Ok(true)
        })
    })
    .await
}

/// Set the identity of a node (acknowledged), replacing any: what an
/// operator confirmed (`admin node adopt-identity`), or what a managed
/// restore recorded before the restore (Phase 3.11 review).
///
/// # Errors
///
/// Backend errors.
pub async fn set_identity<S: KvStore>(store: &S, node_id: u64, token: [u8; 16]) -> Result<()> {
    transact(store, (node_id, token), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, token) = *env;
            write(
                tx,
                &keys::node_identity(node_id),
                &NodeIdentityRecord {
                    node_id,
                    token,
                    acknowledged: true,
                },
            )
        })
    })
    .await
}

/// In a transaction: refuse while a managed restore holds its marker in
/// the store (Phase 3.11 re-review): between the marker and the clear the
/// node lifecycle stays as `admin restore` captured it.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] while the marker is present; backend
/// errors.
pub async fn no_restore_in_progress<X: Txn>(tx: &mut X) -> Result<()> {
    if tx.get(&keys::restore_marker()).await?.is_some() {
        return Err(MetaError::InvalidArgument(
            "a managed restore is in progress (`admin restore status`): node changes wait until it finishes".into(),
        ));
    }
    Ok(())
}

/// Take `token` as the node's identity only if it has no record and is
/// not quarantined, in one transaction. Returns whether it was taken.
///
/// # Errors
///
/// Backend errors.
pub async fn adopt_if_unconfirmed<S: KvStore>(
    store: &S,
    node_id: u64,
    token: [u8; 16],
) -> Result<bool> {
    transact(store, (node_id, token), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, token) = *env;
            if tx.get(&keys::node_identity(node_id)).await?.is_some()
                || read_state(tx, node_id).await? == State::Quarantined
            {
                return Ok(false);
            }
            write(
                tx,
                &keys::node_identity(node_id),
                &NodeIdentityRecord {
                    node_id,
                    token,
                    acknowledged: true,
                },
            )?;
            Ok(true)
        })
    })
    .await
}

/// In a transaction: give a newly registered node its identity (not
/// acknowledged yet), so that a disk at its address adopts it or, holding
/// another, is quarantined (Phase 3.11 review: never imported).
///
/// # Errors
///
/// Backend errors.
pub async fn new_identity_in_tx<X: Txn>(tx: &mut X, node_id: u64) -> Result<()> {
    if tx.get(&keys::node_identity(node_id)).await?.is_none() {
        write(
            tx,
            &keys::node_identity(node_id),
            &NodeIdentityRecord {
                node_id,
                token: crate::records::new_gen_id(),
                acknowledged: false,
            },
        )?;
    }
    Ok(())
}

/// The node reported holding `token`: from now on, a node at this id
/// without it is not this node.
///
/// # Errors
///
/// Backend errors.
pub async fn acknowledge_identity<S: KvStore>(
    store: &S,
    node_id: u64,
    token: [u8; 16],
) -> Result<()> {
    transact(store, (node_id, token), |tx, env| {
        Box::pin(async move {
            no_restore_in_progress(tx).await?;
            let (node_id, token) = *env;
            let cur: Option<NodeIdentityRecord> = read(tx, &keys::node_identity(node_id)).await?;
            if let Some(mut r) = cur.filter(|r| r.token == token && !r.acknowledged) {
                r.acknowledged = true;
                write(tx, &keys::node_identity(node_id), &r)?;
            }
            Ok(())
        })
    })
    .await
}

/// Every identity record, by node.
///
/// # Errors
///
/// Backend errors.
pub async fn identities<S: KvStore>(
    store: &S,
) -> Result<std::collections::HashMap<u64, NodeIdentityRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            tx.get_range(&keys::node_identities_range(), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(_, v)| decode::<NodeIdentityRecord>(&v).map(|r| (r.node_id, r)))
                .collect()
        })
    })
    .await
}

/// Every node's state, by node (nodes without records are active).
///
/// # Errors
///
/// Backend errors.
pub async fn states<S: KvStore>(store: &S) -> Result<std::collections::HashMap<u64, State>> {
    let mut out = std::collections::HashMap::new();
    for l in crate::ops::membership::node_states(store).await? {
        out.insert(l.node_id, state_of(Some(&l), None));
    }
    for r in retirements(store).await? {
        out.insert(r.node_id, state_of(None, Some(&r)));
    }
    Ok(out)
}
