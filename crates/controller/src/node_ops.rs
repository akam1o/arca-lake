//! Operator node lifecycle actions with their checks (Phase 3.11, R-100):
//! unregister and rejoin (`admin node`). Draining and decommission are
//! `membership`'s.

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::node_lifecycle::{self, State};
use arca_lake_metadata::ops::{cluster, compat};
use arca_lake_protocol::client::{FencePush, NodeClients};

use crate::ControlError;

/// What `unregister` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unregistered {
    /// The node was already removed (nothing changed).
    pub already: bool,
    /// The node answered and its delete fence is now closed and held.
    pub fenced: bool,
}

/// Unregister a decommissioned (or quarantined) node. Before the record
/// changes, the node itself is told: its delete fence is closed with an
/// operator hold, so that no delete of this cluster applies there again.
/// A node that does not answer is unregistered only with
/// `unreachable_ok` (the operator states that its disk will never be
/// started again unwiped). Idempotent.
///
/// # Errors
///
/// [`ControlError::Other`] when the node may not be unregistered
/// (in service, still referenced, unreachable without `unreachable_ok`);
/// metadata errors.
pub async fn unregister<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    by: &str,
    reason: &str,
    unreachable_ok: bool,
    now_ms: u64,
) -> Result<Unregistered, ControlError> {
    let node = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| ControlError::Other(format!("node {node_id} unknown")))?;
    let state = node_lifecycle::state(meta, node_id).await?;
    if state == State::Removed {
        return Ok(Unregistered {
            already: true,
            fenced: false,
        });
    }
    if !matches!(state, State::Decommissioned | State::Quarantined) {
        return Err(ControlError::Other(format!(
            "node {node_id} is {}: decommission it first (`admin node decommission`)",
            state.name()
        )));
    }
    let incarnation = compat::cluster_state(meta).await?.map(|c| c.incarnation);
    clients.note_node_id(&node.address, node.id);
    let fenced = match clients.health(&node.address).await {
        Ok(h) => clients
            .push_delete_fence_with(
                &node.address,
                h.delete_fence_generation.max(1),
                false,
                FencePush {
                    hold: true,
                    incarnation,
                    ..FencePush::default()
                },
            )
            .await
            .map_err(|status| ControlError::Node {
                addr: node.address.clone(),
                status,
            })
            .and_then(|(_, open, hold)| {
                if !open && hold {
                    Ok(true)
                } else {
                    Err(ControlError::Other(format!(
                        "node {node_id} did not take the closed, held fence (open {open}, hold {hold}): not unregistered"
                    )))
                }
            })?,
        Err(e) if unreachable_ok => {
            tracing::warn!(node = node_id, error = %e, "unregistering an unreachable node: its disk must not be started again unwiped");
            false
        }
        Err(e) => {
            return Err(ControlError::Other(format!(
                "node {node_id} does not answer ({}): its fence cannot be closed; if its disk will never be started again unwiped, re-run with --unreachable",
                e.code()
            )));
        }
    };
    node_lifecycle::unregister(meta, node_id, by, reason, now_ms).await?;
    Ok(Unregistered {
        already: false,
        fenced,
    })
}

/// The result of the rejoin checks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RejoinCheck {
    /// Why the node may not rejoin (empty: it may).
    pub problems: Vec<String>,
    /// It rejoins with a new identity (an emptied disk).
    pub fresh_identity: bool,
}

/// Check that a removed, quarantined or decommissioned node may come back
/// (`admin node rejoin`): it answers; it speaks this cluster's protocol
/// and segment format; it holds this cluster's incarnation, or none, and
/// no later one; its schema floor is not above the cluster's; it holds the
/// identity the cluster gave it, or (with `allow_fresh`) none on a disk
/// without shards.
///
/// # Errors
///
/// Metadata errors.
pub async fn check_rejoin<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    allow_fresh: bool,
) -> Result<RejoinCheck, ControlError> {
    let mut out = RejoinCheck::default();
    let Some(node) = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .find(|n| n.id == node_id)
    else {
        out.problems.push(format!("node {node_id} unknown"));
        return Ok(out);
    };
    let state = node_lifecycle::state(meta, node_id).await?;
    if state.in_service() {
        out.problems.push(format!(
            "node {node_id} is {}: nothing to rejoin",
            state.name()
        ));
        return Ok(out);
    }
    let gate = compat::gate(meta).await?;
    clients.note_node_id(&node.address, node.id);
    let h = match clients.health(&node.address).await {
        Ok(h) => h,
        Err(e) => {
            out.problems
                .push(format!("node {node_id} does not answer: {}", e.code()));
            return Ok(out);
        }
    };
    let caps = arca_lake_protocol::client::NodeCaps::from_health(&h).caps;
    if caps.protocol_max < arca_lake_protocol::storage::v1::PROTOCOL_VERSION {
        out.problems.push(format!(
            "node {node_id} runs {} (protocol {}): upgrade it to this release first",
            h.software, caps.protocol_max
        ));
    }
    if let Some(f) = gate.format_write
        && caps.format_max < f
    {
        out.problems.push(format!(
            "node {node_id} writes segment format at most {}, the cluster writes {f}",
            caps.format_max
        ));
    }
    if let Some(schema) = gate.schema
        && h.schema_floor > schema
    {
        out.problems.push(format!(
            "node {node_id} holds schema floor {} above the cluster's schema {schema}: it served a later metadata history",
            h.schema_floor
        ));
    }
    if let Some(c) = &gate.cluster {
        let held = <[u8; 16]>::try_from(h.incarnation.as_ref()).ok();
        if h.incarnation_seq > gate.incarnation_seq {
            out.problems.push(format!(
                "node {node_id} holds incarnation sequence {} above the cluster's {}: another cluster, or a later history",
                h.incarnation_seq, gate.incarnation_seq
            ));
        } else if held.is_some_and(|i| i != c.incarnation)
            && h.incarnation_seq == gate.incarnation_seq
        {
            out.problems.push(format!(
                "node {node_id} holds another incarnation at the cluster's sequence: another cluster's disk"
            ));
        }
    }
    if !h.identity_supported {
        out.problems.push(format!(
            "node {node_id} does not report an identity: upgrade it to this release first"
        ));
        return Ok(out);
    }
    let ours = node_lifecycle::identity(meta, node_id).await?;
    let held = (h.identity_node_id != 0).then_some((h.identity_node_id, h.identity_token.clone()));
    match (ours, held) {
        (Some(o), Some((id, token))) if id == node_id && token.as_ref() == o.token.as_slice() => {}
        (_, Some((id, _))) => out.problems.push(format!(
            "node {node_id} holds the identity of {} (node {id}): another cluster's or another node's disk; it is not rejoined",
            if id == node_id { "another registration" } else { "another node" }
        )),
        (_, None) if !allow_fresh => out.problems.push(format!(
            "node {node_id} holds no identity (an emptied or new disk): re-run with --fresh-identity"
        )),
        (_, None) if h.live_shards > 0 => out.problems.push(format!(
            "node {node_id} holds no identity but {} shards of unknown origin: empty its disk first",
            h.live_shards
        )),
        (_, None) => out.fresh_identity = true,
    }
    Ok(out)
}

/// Rejoin after [`check_rejoin`] found nothing: the node is active again,
/// without members; reconciliation collects its former shards and repair
/// and placement may use it from the next pass.
///
/// # Errors
///
/// [`ControlError::Other`] with the problems found; metadata
/// errors.
pub async fn rejoin<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    allow_fresh: bool,
) -> Result<(State, RejoinCheck), ControlError> {
    let check = check_rejoin(meta, clients, node_id, allow_fresh).await?;
    if !check.problems.is_empty() {
        return Err(ControlError::Other(check.problems.join("; ")));
    }
    let was = node_lifecycle::rejoin(meta, node_id, check.fresh_identity).await?;
    Ok((was, check))
}

/// Confirm the identity a node holds as this cluster's (`admin node
/// adopt-identity`): the operator states that the disk at the node's
/// address is that node's (Phase 3.11 review: after metadata restored from
/// before the node had an identity, nothing is imported automatically).
/// Returns the token taken.
///
/// # Errors
///
/// [`ControlError::Other`] when the node does not answer, holds no
/// identity or another node's; metadata errors.
pub async fn adopt_identity<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
) -> Result<[u8; 16], ControlError> {
    let node = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| ControlError::Other(format!("node {node_id} unknown")))?;
    let h = clients.health(&node.address).await.map_err(|e| {
        ControlError::Other(format!("node {node_id} does not answer: {}", e.code()))
    })?;
    // Only the unconfirmed case (third review): no record, the node holding
    // its own id; never over a record, never for a quarantined node.
    if node_lifecycle::identity(meta, node_id).await?.is_some() {
        return Err(ControlError::Other(format!(
            "node {node_id} has an identity record: nothing to adopt (a mismatch is a quarantine; `admin node rejoin` decides)"
        )));
    }
    if node_lifecycle::state(meta, node_id).await? == State::Quarantined {
        return Err(ControlError::Other(format!(
            "node {node_id} is quarantined: `admin node rejoin` decides"
        )));
    }
    let token = <[u8; 16]>::try_from(h.identity_token.as_ref()).ok();
    match (h.identity_supported, h.identity_node_id, token) {
        (true, id, Some(t)) if id == node_id => {
            node_lifecycle::set_identity(meta, node_id, t).await?;
            Ok(t)
        }
        (false, _, _) => Err(ControlError::Other(format!(
            "node {node_id} runs an earlier release: no identity to adopt"
        ))),
        (true, 0, _) | (true, _, None) => Err(ControlError::Other(format!(
            "node {node_id} holds no identity: the leader gives it one by itself"
        ))),
        (true, id, _) => Err(ControlError::Other(format!(
            "node {node_id}'s address answers with node {id}'s identity: not adopted"
        ))),
    }
}
