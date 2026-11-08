//! Operator side of the delete fence (Phase 3.9, ADR-0023): close it on
//! every storage node before a metadata restore or a phase change, and
//! show where each node stands. The controller leader opens it again (a
//! new generation) once the cluster is `Active`.

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::delete_fence::{self, DeleteFenceRecord};
use arca_lake_metadata::ops::{cluster, compat};
use arca_lake_protocol::client::{FencePush, NodeClients};

use crate::ControlError;

/// One node's fence as it answered (or why it did not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFence {
    /// Node id.
    pub node_id: u64,
    /// `(generation, open)` it holds, or the error.
    pub fence: Result<(u64, bool), String>,
    /// It holds an operator hold (only known when it answered).
    pub held: bool,
}

/// What [`close_and_push`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closed {
    /// The closed record.
    pub record: DeleteFenceRecord,
    /// Nodes that held an open fence before (deletes were allowed there).
    pub were_open: Vec<u64>,
    /// Every node's fence after the push.
    pub nodes: Vec<NodeFence>,
}

impl Closed {
    /// Nodes that did not acknowledge the closed generation (unreachable,
    /// or holding another state).
    #[must_use]
    pub fn not_acknowledged(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|n| !matches!(n.fence, Ok((g, false)) if g >= self.record.generation))
            .map(|n| n.node_id)
            .collect()
    }
}

/// Every registered node's fence (status).
///
/// # Errors
///
/// Metadata errors.
pub async fn node_fences<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
) -> Result<Vec<NodeFence>, ControlError> {
    let mut out = Vec::new();
    for n in cluster::list_nodes(meta).await? {
        clients.note_node_id(&n.address, n.id);
        let h = clients.health(&n.address).await;
        let held = h.as_ref().is_ok_and(|h| h.delete_fence_hold);
        let fence = h
            .map(|h| (h.delete_fence_generation, h.delete_fence_open))
            .map_err(|e| format!("{}: {}", e.code(), e.message()));
        out.push(NodeFence {
            node_id: n.id,
            fence,
            held,
        });
    }
    Ok(out)
}

/// Close the delete fence: a new closed generation above the record's and
/// every generation a node reports, recorded, then pushed to every
/// registered node, each acknowledging it only after the deletes that
/// passed its old fence have finished. Nodes that do not answer are listed
/// (`not_acknowledged`): a step that needs deletes stopped everywhere (a
/// restore) must not proceed while any is.
///
/// # Errors
///
/// Metadata errors.
pub async fn close_and_push<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    reason: &str,
    hold: bool,
    now_ms: u64,
) -> Result<Closed, ControlError> {
    let before = node_fences(meta, clients).await?;
    let highest = before
        .iter()
        .filter_map(|n| n.fence.as_ref().ok().map(|f| f.0))
        .max()
        .unwrap_or(0);
    let were_open = before
        .iter()
        .filter(|n| matches!(n.fence, Ok((_, true))))
        .map(|n| n.node_id)
        .collect();
    let record = delete_fence::close(meta, highest, reason, hold, now_ms).await?;
    // The hold is tied to the incarnation it is set in: after a restore,
    // only a controller of the incarnation `recovery begin` creates opens
    // the fence again on the nodes (one started on the restored metadata
    // before `begin` cannot).
    let how = FencePush {
        hold: record.operator_hold,
        release: false,
        incarnation: compat::cluster_state(meta).await?.map(|s| s.incarnation),
    };
    let mut nodes = Vec::new();
    for n in cluster::list_nodes(meta).await? {
        let pushed = clients
            .push_delete_fence_with(&n.address, record.generation, false, how)
            .await;
        let is_held = pushed.as_ref().is_ok_and(|p| p.2);
        let fence = pushed
            .map(|(g, o, _)| (g, o))
            .map_err(|e| format!("{}: {}", e.code(), e.message()));
        nodes.push(NodeFence {
            node_id: n.id,
            fence,
            held: is_held,
        });
    }
    Ok(Closed {
        record,
        were_open,
        nodes,
    })
}

/// Release the operator hold (cluster `Active` only), recorded and pushed
/// to every node; returns the nodes that still hold it (unreachable: they
/// keep refusing deletes until released, a later release reaches them).
///
/// # Errors
///
/// Metadata errors, including the refusal outside `Active`.
pub async fn release_and_push<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    reason: &str,
    now_ms: u64,
) -> Result<(DeleteFenceRecord, Vec<u64>), ControlError> {
    let record = delete_fence::release_hold(meta, reason, now_ms).await?;
    let how = FencePush {
        hold: false,
        release: true,
        incarnation: compat::cluster_state(meta).await?.map(|s| s.incarnation),
    };
    let mut held = Vec::new();
    for n in cluster::list_nodes(meta).await? {
        // The release names the recorded (closed) generation: a node lifts
        // its hold only at it or above, so a delayed release never lifts a
        // later close's hold.
        match clients
            .push_delete_fence_with(&n.address, record.generation, false, how)
            .await
        {
            Ok((_, _, false)) => {}
            _ => held.push(n.id),
        }
    }
    Ok((record, held))
}
