//! Operator membership intents (RFC-013 device replacement and node
//! decommission, Phase 3.3 §6): a node's lifecycle (`("al","nstate",id)`)
//! and per-group replacement requests (`("al","replace",group)`). The
//! controller leader carries them out with the existing member-replacement
//! state machine (epoch fencing, persisted repair cursor, completeness
//! check); these records only say what the operator asked for, so a
//! controller crash or failover resumes from them.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{Record, decode};
use crate::{MetaError, keys, transact};

/// Lifecycle of a node beyond plain registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeLifecycle {
    /// Being emptied: every group member on it is replaced; never chosen
    /// as a replacement.
    Draining {
        /// When requested.
        since_ms: u64,
        /// Who requested.
        by: String,
    },
    /// No group lists the node and every group was Active when this was
    /// recorded: its shards are no longer needed by any placement.
    Decommissioned {
        /// When draining started.
        since_ms: u64,
        /// When it completed.
        at_ms: u64,
    },
}

/// `("al","nstate",node_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStateRecord {
    /// Node.
    pub node_id: u64,
    /// Lifecycle.
    pub state: NodeLifecycle,
    /// Why the drain cannot progress right now, if it cannot.
    pub blocked: Option<String>,
}

impl Record for NodeStateRecord {}

/// `("al","replace",group)`: replace member `index` of `group`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceRequest {
    /// Group.
    pub group: u64,
    /// Member index.
    pub index: u16,
    /// Node at that index when requested (the request is satisfied once a
    /// different node holds the index and the group is Active).
    pub from_node: u64,
    /// Requested replacement node, if the operator named one.
    pub to_node: Option<u64>,
    /// When requested.
    pub requested_ms: u64,
    /// Who requested.
    pub by: String,
    /// Why it cannot progress right now, if it cannot.
    pub last_error: Option<String>,
}

impl Record for ReplaceRequest {}

/// Start draining a node. Idempotent; refused for a decommissioned node.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] for an unknown or decommissioned node.
pub async fn drain_node<S: KvStore>(
    store: &S,
    node_id: u64,
    by: &str,
    now_ms: u64,
) -> Result<NodeStateRecord> {
    transact(store, (node_id, by.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (node_id, by, now_ms) = env;
            if tx.get(&keys::node(*node_id)).await?.is_none() {
                return Err(MetaError::InvalidArgument(format!(
                    "node {node_id} unknown"
                )));
            }
            let cur: Option<NodeStateRecord> = read(tx, &keys::node_state(*node_id)).await?;
            match cur {
                Some(r) if matches!(r.state, NodeLifecycle::Draining { .. }) => Ok(r),
                Some(_) => Err(MetaError::InvalidArgument(format!(
                    "node {node_id} is already decommissioned"
                ))),
                None => {
                    let r = NodeStateRecord {
                        node_id: *node_id,
                        state: NodeLifecycle::Draining {
                            since_ms: *now_ms,
                            by: by.clone(),
                        },
                        blocked: None,
                    };
                    write(tx, &keys::node_state(*node_id), &r)?;
                    Ok(r)
                }
            }
        })
    })
    .await
}

/// Stop draining (members already moved stay moved). Refused once
/// decommissioned.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when decommissioned.
pub async fn cancel_drain<S: KvStore>(store: &S, node_id: u64) -> Result<bool> {
    transact(store, node_id, |tx, node_id| {
        Box::pin(async move {
            let cur: Option<NodeStateRecord> = read(tx, &keys::node_state(*node_id)).await?;
            match cur {
                Some(r) if matches!(r.state, NodeLifecycle::Draining { .. }) => {
                    tx.clear(&keys::node_state(*node_id));
                    Ok(true)
                }
                Some(_) => Err(MetaError::InvalidArgument(format!(
                    "node {node_id} is decommissioned"
                ))),
                None => Ok(false),
            }
        })
    })
    .await
}

/// Record that a draining node is decommissioned, or why it is blocked
/// (`done = None` keeps it draining with `blocked`).
///
/// # Errors
///
/// Backend errors.
pub async fn update_drain<S: KvStore>(
    store: &S,
    node_id: u64,
    done_at_ms: Option<u64>,
    blocked: Option<String>,
) -> Result<()> {
    transact(store, (node_id, done_at_ms, blocked), |tx, env| {
        Box::pin(async move {
            let (node_id, done, blocked) = env;
            let cur: Option<NodeStateRecord> = read(tx, &keys::node_state(*node_id)).await?;
            let Some(mut r) = cur else { return Ok(()) };
            let NodeLifecycle::Draining { since_ms, .. } = r.state else {
                return Ok(());
            };
            if let Some(at_ms) = done {
                r.state = NodeLifecycle::Decommissioned {
                    since_ms,
                    at_ms: *at_ms,
                };
                r.blocked = None;
            } else if r.blocked == *blocked {
                return Ok(());
            } else {
                r.blocked.clone_from(blocked);
            }
            write(tx, &keys::node_state(*node_id), &r)
        })
    })
    .await
}

/// Every node state record.
///
/// # Errors
///
/// Backend errors.
pub async fn node_states<S: KvStore>(store: &S) -> Result<Vec<NodeStateRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::node_states_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<NodeStateRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Request the replacement of member `index` of `group`. Idempotent for
/// the same index; refused while another request on the group is open.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] for an unknown group or index, or a
/// conflicting request.
pub async fn request_replace<S: KvStore>(
    store: &S,
    group: u64,
    index: u16,
    to_node: Option<u64>,
    by: &str,
    now_ms: u64,
) -> Result<ReplaceRequest> {
    transact(
        store,
        (group, index, to_node, by.to_string(), now_ms),
        |tx, env| {
            Box::pin(async move {
                let (group, index, to_node, by, now_ms) = env;
                let g: crate::records::GroupRecord = read(tx, &keys::group(*group))
                    .await?
                    .ok_or_else(|| MetaError::InvalidArgument(format!("group {group} unknown")))?;
                let m = g.members.get(usize::from(*index)).ok_or_else(|| {
                    MetaError::InvalidArgument(format!("group {group} has no member {index}"))
                })?;
                if let Some(n) = to_node {
                    if g.members.iter().any(|x| x.node_id == *n) {
                        return Err(MetaError::InvalidArgument(format!(
                            "node {n} is already a member of group {group}"
                        )));
                    }
                    if tx.get(&keys::node(*n)).await?.is_none() {
                        return Err(MetaError::InvalidArgument(format!("node {n} unknown")));
                    }
                }
                let cur: Option<ReplaceRequest> = read(tx, &keys::replace_request(*group)).await?;
                if let Some(c) = cur {
                    if c.index == *index && c.to_node == *to_node {
                        return Ok(c);
                    }
                    return Err(MetaError::InvalidArgument(format!(
                        "group {group} already has a replacement request for member {}",
                        c.index
                    )));
                }
                let r = ReplaceRequest {
                    group: *group,
                    index: *index,
                    from_node: m.node_id,
                    to_node: *to_node,
                    requested_ms: *now_ms,
                    by: by.clone(),
                    last_error: None,
                };
                write(tx, &keys::replace_request(*group), &r)?;
                Ok(r)
            })
        },
    )
    .await
}

/// Every open replacement request.
///
/// # Errors
///
/// Backend errors.
pub async fn replace_requests<S: KvStore>(store: &S) -> Result<Vec<ReplaceRequest>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::replace_requests_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<ReplaceRequest>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Remove a request (done or cancelled).
///
/// # Errors
///
/// Backend errors.
pub async fn clear_replace<S: KvStore>(store: &S, group: u64) -> Result<Option<ReplaceRequest>> {
    transact(store, group, |tx, group| {
        Box::pin(async move {
            let cur: Option<ReplaceRequest> = read(tx, &keys::replace_request(*group)).await?;
            tx.clear(&keys::replace_request(*group));
            Ok(cur)
        })
    })
    .await
}

/// Note why a request cannot progress.
///
/// # Errors
///
/// Backend errors.
pub async fn note_replace_error<S: KvStore>(
    store: &S,
    group: u64,
    error: Option<String>,
) -> Result<()> {
    transact(store, (group, error), |tx, env| {
        Box::pin(async move {
            let (group, error) = env;
            let cur: Option<ReplaceRequest> = read(tx, &keys::replace_request(*group)).await?;
            if let Some(mut r) = cur
                && r.last_error != *error
            {
                r.last_error.clone_from(error);
                write(tx, &keys::replace_request(*group), &r)?;
            }
            Ok(())
        })
    })
    .await
}
