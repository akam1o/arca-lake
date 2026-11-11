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
    let fenced = fence_and_hold(meta, clients, &node, unreachable_ok).await?;
    node_lifecycle::unregister(meta, node_id, by, reason, now_ms).await?;
    Ok(Unregistered {
        already: false,
        fenced,
    })
}

/// The identity this cluster acknowledged for the node, once what answers
/// at its address is verifiably its disk (`h`: the node's health).
async fn verified_identity<S: KvStore>(
    meta: &S,
    node_id: u64,
    h: &arca_lake_protocol::storage::v1::HealthResponse,
) -> Result<Option<node_lifecycle::NodeIdentityRecord>, ControlError> {
    // What answers must be this node's disk before anything is pushed
    // (re-review: another cluster's node at the address would have its own
    // hold released and re-tied to this cluster). Not verifiable (no
    // record, or one never acknowledged that the node does not hold):
    // refused; a node that does not answer as this one is stopped for good
    // and removed with `--unreachable`.
    let rec = node_lifecycle::identity(meta, node_id).await?;
    let held = <[u8; 16]>::try_from(h.identity_token.as_ref()).ok();
    if h.identity_supported {
        let ours = match &rec {
            Some(r) if r.acknowledged => h.identity_node_id == node_id && held == Some(r.token),
            Some(r) => {
                h.identity_node_id == 0 || (h.identity_node_id == node_id && held == Some(r.token))
            }
            None => h.identity_node_id == 0,
        };
        if !ours {
            return Err(ControlError::Other(format!(
                "the disk at node {node_id}'s address is not verifiably this node's (it holds the identity of node {}): nothing pushed; if this node's own disk will never be started again unwiped, re-run with --unreachable",
                h.identity_node_id
            )));
        }
    }
    Ok(rec.filter(|r| r.acknowledged))
}

/// Close the node's delete fence with an operator hold on the node itself
/// (`true`), or, with `unreachable_ok`, accept that it does not answer
/// (`false`: the operator states its disk never serves again unwiped).
///
/// The hold must be one the cluster's current incarnation cannot lift
/// (confirmation review): tied to it, or unknown (facts lost). A hold
/// tied to an earlier incarnation, or set before the node had one, which
/// an open of the current one would lift, is released and set again at
/// the same generation; the node is read back, and anything that opened
/// it meanwhile stops the procedure. A node that does not report its
/// hold's incarnation (an earlier release) is refused.
async fn fence_and_hold<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node: &arca_lake_metadata::records::NodeRecord,
    unreachable_ok: bool,
) -> Result<bool, ControlError> {
    let node_id = node.id;
    let incarnation = compat::cluster_state(meta).await?.map(|c| c.incarnation);
    clients.note_node_id(&node.address, node.id);
    let h = match clients.health(&node.address).await {
        Ok(h) => h,
        Err(e) if unreachable_ok => {
            tracing::warn!(node = node_id, error = %e, "removing an unreachable node: its disk must not be started again unwiped");
            return Ok(false);
        }
        Err(e) => {
            return Err(ControlError::Other(format!(
                "node {node_id} does not answer ({}): its fence cannot be closed; if its disk will never be started again unwiped, re-run with --unreachable",
                e.code()
            )));
        }
    };
    let node_err = |status| ControlError::Node {
        addr: node.address.clone(),
        status,
    };
    let identity = verified_identity(meta, node_id, &h).await?;
    // The node first takes the cluster's incarnation (with its sequence,
    // as recovery pushes it, and the node's identity: a disk holding
    // another adopts nothing), so that the hold is tied to it.
    let gate = compat::gate(meta).await?;
    if let Some(state) = gate.cluster.as_ref() {
        let _ = clients
            .health_with(
                &node.address,
                arca_lake_protocol::storage::v1::HealthRequest {
                    format_write: gate.format_write.unwrap_or(0),
                    incarnation: bytes::Bytes::copy_from_slice(&state.incarnation),
                    incarnation_changed_ms: state.changed_ms,
                    incarnation_seq: gate.incarnation_seq,
                    schema_floor: gate.schema.unwrap_or(0),
                    node_id: identity.as_ref().map_or(0, |_| node_id),
                    node_identity: identity
                        .as_ref()
                        .map(|r| bytes::Bytes::copy_from_slice(&r.token))
                        .unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await;
    }
    let generation = h.delete_fence_generation.max(1);
    let close = FencePush {
        hold: true,
        incarnation,
        ..FencePush::default()
    };
    clients
        .push_delete_fence_with(&node.address, generation, false, close)
        .await
        .map_err(node_err)?;
    for attempt in 0..2 {
        let h = clients.health(&node.address).await.map_err(node_err)?;
        let tied = h.delete_fence_hold_incarnation.as_ref();
        if h.delete_fence_open || !h.delete_fence_hold {
            return Err(ControlError::Other(format!(
                "node {node_id} did not keep the closed, held fence (open {}, hold {}): not removed",
                h.delete_fence_open, h.delete_fence_hold
            )));
        }
        // Not reported (an earlier release): the hold cannot be checked
        // (re-review: not proven safe).
        if tied.is_empty() {
            return Err(ControlError::Other(format!(
                "node {node_id} does not report the incarnation of its hold (an earlier release): upgrade it, or stop it for good and re-run with --unreachable"
            )));
        }
        // Unliftable by the current incarnation: tied to it, or unknown
        // (only an explicit release lifts that). A hold set before the
        // node had an incarnation (zeros) is lifted by any it adopts
        // later: re-tied like an earlier one, unless the node still has
        // none (then no open can lift it; re-review).
        let unliftable = hold_is_unliftable(tied, incarnation, &h.incarnation);
        if unliftable {
            return Ok(true);
        }
        if attempt == 1 {
            break;
        }
        // An earlier incarnation's hold: released and set again at the
        // node's generation (a release never opens; an open needs a higher
        // generation, which the read-back would see).
        let g = h.delete_fence_generation;
        clients
            .push_delete_fence_with(
                &node.address,
                g,
                false,
                FencePush {
                    release: true,
                    incarnation,
                    ..FencePush::default()
                },
            )
            .await
            .map_err(node_err)?;
        clients
            .push_delete_fence_with(&node.address, g, false, close)
            .await
            .map_err(node_err)?;
    }
    Err(ControlError::Other(format!(
        "node {node_id} holds its fence under an earlier incarnation and did not take this one's: not removed"
    )))
}

/// Whether a hold tied to `tied` stays in place against opens of the
/// cluster's `current` incarnation, on a node holding `node_incarnation`:
/// unknown (only a release lifts it), or tied to the current incarnation
/// on a node holding it. A zero hold, or one of an earlier incarnation, is
/// lifted by an incarnation the node adopts later (re-reviews: also when
/// it has not adopted the current one yet).
fn hold_is_unliftable(tied: &[u8], current: Option<[u8; 16]>, node_incarnation: &[u8]) -> bool {
    tied == [0xff; 16].as_slice()
        || current.is_none_or(|i| tied == i.as_slice() && node_incarnation == i.as_slice())
}

/// `admin node keep-removed` (Phase 3.11): after a manual restore of
/// metadata from before a node was unregistered, mark it removed again,
/// its fence closed and held on the node first (confirmation review: a
/// removed node is trusted to be fenced). No state or membership check:
/// its members are repaired away.
///
/// # Errors
///
/// As [`unregister`].
pub async fn keep_removed<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    unreachable_ok: bool,
    now_ms: u64,
) -> Result<bool, ControlError> {
    let node = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| ControlError::Other(format!("node {node_id} unknown")))?;
    let fenced = fence_and_hold(meta, clients, &node, unreachable_ok).await?;
    node_lifecycle::keep_removed(meta, node_id, now_ms).await?;
    Ok(fenced)
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
    let token = <[u8; 16]>::try_from(h.identity_token.as_ref()).ok();
    match (h.identity_supported, h.identity_node_id, token) {
        // Only the unconfirmed case, checked with the write (third and
        // confirmation reviews): no record, not quarantined.
        (true, id, Some(t)) if id == node_id => {
            if node_lifecycle::adopt_if_unconfirmed(meta, node_id, t).await? {
                Ok(t)
            } else {
                Err(ControlError::Other(format!(
                    "node {node_id} has an identity record or is quarantined: nothing to adopt (`admin node rejoin` decides)"
                )))
            }
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

#[cfg(test)]
mod tests {
    use super::hold_is_unliftable;

    #[test]
    fn only_holds_the_current_incarnation_cannot_lift_are_accepted() {
        let cur = Some([5u8; 16]);
        assert!(hold_is_unliftable(&[5; 16], cur, &[5; 16]));
        assert!(hold_is_unliftable(&[0xff; 16], cur, &[5; 16]), "unknown");
        assert!(!hold_is_unliftable(&[4; 16], cur, &[5; 16]), "earlier");
        assert!(
            !hold_is_unliftable(&[0; 16], cur, &[5; 16]),
            "zeros, node adopted one"
        );
        assert!(
            !hold_is_unliftable(&[0; 16], cur, &[]),
            "zeros, node has none yet"
        );
        assert!(
            !hold_is_unliftable(&[5; 16], cur, &[]),
            "the node has not adopted it"
        );
        assert!(
            hold_is_unliftable(&[4; 16], None, &[]),
            "no cluster incarnation"
        );
    }
}
