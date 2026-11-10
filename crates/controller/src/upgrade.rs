//! `admin upgrade finalize` as executed by the controller leader (RFC-014
//! §5.5, ADR-0016). The admin command writes a request into the upgrade
//! cursor; every pass here takes at most one step, each a compare-and-set
//! on the cursor's `step` under the leader's fencing token, so a crash,
//! failover or re-issued command resumes where the last leader stopped and
//! never repeats a step. Nothing here converts data: the test bump shares
//! the record and segment layouts, and a future migration plugs into
//! [`UpgradeStep::Preconditioned`] → [`UpgradeStep::Migrated`].

use arca_lake_common::compat::Capabilities;
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::compat::{self, VersionKey};
use arca_lake_metadata::ops::{cluster, recovery};
use arca_lake_metadata::records::UpgradeStep;
use arca_lake_protocol::client::NodeClients;
use tracing::{info, warn};

use crate::ControlError;

/// Heartbeats older than this are treated as gone processes (they do not
/// block the precondition; RFC-014 §5.5 step 1 blocks on *nodes*).
pub const PROC_STALE_MS: u64 = 90_000;

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpgradePassReport {
    /// Step before.
    pub before: Option<UpgradeStep>,
    /// Step after (`None` when finished and the cursor was cleared).
    pub after: Option<UpgradeStep>,
    /// Why the step was not taken.
    pub blocked: Option<String>,
}

/// One pass: take the next step of an outstanding finalize, if any.
///
/// # Errors
///
/// Metadata errors.
#[allow(clippy::too_many_lines)]
pub async fn upgrade_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    own: &Capabilities,
    token: u64,
    now_ms: u64,
) -> Result<UpgradePassReport, ControlError> {
    let mut report = UpgradePassReport::default();
    let Some(cur) = compat::upgrade_cursor(meta).await? else {
        return Ok(report);
    };
    report.before = Some(cur.step);
    report.after = Some(cur.step);
    let target = cur.target;
    if !own.reaches(&target) {
        let why = format!(
            "this controller reads schema up to {}, protocol {}, format {}: cannot finalize to {target:?}",
            own.schema_max, own.protocol_max, own.format_max
        );
        compat::note_upgrade_error(meta, &why, now_ms).await?;
        report.blocked = Some(why);
        return Ok(report);
    }
    match cur.step {
        UpgradeStep::Requested => {
            // Step 1: every node (live Health) and every fresh heartbeat
            // reaches the target. Absent nodes block unless ignored.
            let mut blockers = Vec::new();
            for n in cluster::list_nodes(meta).await? {
                let id = n.id.to_string();
                match clients.health(&n.address).await {
                    Ok(h) => {
                        let caps = arca_lake_protocol::client::NodeCaps::from_health(&h).caps;
                        let ok = caps.protocol_max >= target.protocol
                            && caps.format_max >= target.format;
                        if !ok {
                            blockers.push(format!(
                                "node {} ({}) speaks protocol {} format {}",
                                n.id, h.software, caps.protocol_max, caps.format_max
                            ));
                        }
                    }
                    Err(e) if cur.ignore_absent.contains(&id) => {
                        warn!(node = n.id, error = %e, "finalize: absent node ignored");
                    }
                    Err(e) => blockers.push(format!("node {} unreachable: {}", n.id, e.code())),
                }
            }
            for p in compat::procs(meta).await? {
                if now_ms.saturating_sub(p.last_ms) > PROC_STALE_MS
                    || cur.ignore_absent.contains(&p.id)
                {
                    continue;
                }
                if !(p.caps.schema_max >= target.schema && p.caps.protocol_max >= target.protocol) {
                    blockers.push(format!(
                        "{:?} {} ({}) reads schema {} protocol {}",
                        p.kind, p.id, p.software, p.caps.schema_max, p.caps.protocol_max
                    ));
                }
            }
            if !blockers.is_empty() {
                let why = format!("precondition: {}", blockers.join("; "));
                compat::note_upgrade_error(meta, &why, now_ms).await?;
                report.blocked = Some(why);
                return Ok(report);
            }
            if compat::advance_upgrade(
                meta,
                UpgradeStep::Requested,
                UpgradeStep::Preconditioned,
                token,
                "every node and process reaches the target",
                now_ms,
            )
            .await?
            {
                report.after = Some(UpgradeStep::Preconditioned);
            }
        }
        UpgradeStep::Preconditioned => {
            // Step 2: migrations. None are defined; the step exists so that
            // a future one runs here under the fencing token with its own
            // cursor (RFC-014 §5.4).
            if compat::advance_upgrade(
                meta,
                UpgradeStep::Preconditioned,
                UpgradeStep::Migrated,
                token,
                "no migrations defined",
                now_ms,
            )
            .await?
            {
                report.after = Some(UpgradeStep::Migrated);
            }
        }
        UpgradeStep::Migrated => {
            // Step 3: the downgrade cut-off.
            let v = target.schema;
            if compat::advance_upgrade_raising(
                meta,
                UpgradeStep::Migrated,
                UpgradeStep::SchemaWriteRaised,
                Some((VersionKey::SchemaWrite, v)),
                token,
                &format!("schema_write = {v} (downgrade cut-off)"),
                now_ms,
            )
            .await?
            {
                info!(
                    schema_write = v,
                    "upgrade: schema_write raised; rollback is no longer possible"
                );
                report.after = Some(UpgradeStep::SchemaWriteRaised);
            }
        }
        UpgradeStep::SchemaWriteRaised => {
            // Step 4: nodes may write the new format (pushed by the health
            // pass; also pushed here so that the next segment on every
            // reachable node is already the new format).
            let v = compat::raise_version(meta, VersionKey::FormatWrite, target.format).await?;
            let state = compat::cluster_state(meta).await?;
            let seq = compat::gate(meta).await?.incarnation_seq;
            for n in cluster::list_nodes(meta).await? {
                let req = arca_lake_protocol::storage::v1::HealthRequest {
                    format_write: v,
                    incarnation_seq: seq,
                    incarnation: state
                        .as_ref()
                        .map(|s| bytes::Bytes::copy_from_slice(&s.incarnation))
                        .unwrap_or_default(),
                    incarnation_changed_ms: state.as_ref().map_or(0, |s| s.changed_ms),
                    ..Default::default()
                };
                let _ = clients.health_with(&n.address, req).await;
            }
            if compat::advance_upgrade(
                meta,
                UpgradeStep::SchemaWriteRaised,
                UpgradeStep::FormatWriteRaised,
                token,
                &format!("format_write = {v}"),
                now_ms,
            )
            .await?
            {
                report.after = Some(UpgradeStep::FormatWriteRaised);
            }
        }
        UpgradeStep::FormatWriteRaised => {
            // Step 5: the schema itself; the cursor is cleared and the
            // history kept as an audit record. First every node holds the
            // new schema as its floor (Phase 3.10 review): a node that
            // missed it would take the previous release's deletes on
            // metadata restored from before the finalize. Also nodes the
            // operator named absent (re-review: one returning on such
            // metadata would): only a decommissioned node, which holds no
            // data, is not waited for.
            let decommissioned: Vec<u64> = arca_lake_metadata::ops::membership::node_states(meta)
                .await?
                .into_iter()
                .filter(|r| {
                    matches!(
                        r.state,
                        arca_lake_metadata::ops::membership::NodeLifecycle::Decommissioned { .. }
                    )
                })
                .map(|r| r.node_id)
                .collect();
            let mut missing = Vec::new();
            for n in cluster::list_nodes(meta)
                .await?
                .into_iter()
                .filter(|n| !decommissioned.contains(&n.id))
            {
                let req = arca_lake_protocol::storage::v1::HealthRequest {
                    schema_floor: target.schema,
                    ..Default::default()
                };
                match clients.health_with(&n.address, req).await {
                    Ok(h) if h.schema_floor >= target.schema => {}
                    Ok(h) => missing.push(format!(
                        "node {} keeps schema floor {} (an earlier release?)",
                        n.id, h.schema_floor
                    )),
                    Err(e) => missing.push(format!(
                        "node {} unreachable: {} (bring it back, or decommission it)",
                        n.id,
                        e.code()
                    )),
                }
            }
            if !missing.is_empty() {
                let why = format!("schema floor not held: {}", missing.join("; "));
                compat::note_upgrade_error(meta, &why, now_ms).await?;
                report.blocked = Some(why);
                return Ok(report);
            }
            let v = compat::raise_version(meta, VersionKey::Schema, target.schema).await?;
            // A node registered while the step ran: told now (it cannot
            // block a schema already raised; re-review).
            for n in cluster::list_nodes(meta).await? {
                let req = arca_lake_protocol::storage::v1::HealthRequest {
                    schema_floor: target.schema,
                    ..Default::default()
                };
                if let Err(e) = clients.health_with(&n.address, req).await {
                    warn!(node = n.id, error = %e, "finalize: node not told the schema floor after the raise");
                }
            }
            if let Some(done) = compat::clear_upgrade(meta, false).await? {
                let mut audit = done;
                audit
                    .history
                    .push(format!("{now_ms} finalized: schema = {v} by token {token}"));
                recovery::append_upgrade_audit(meta, &audit, now_ms).await?;
                info!(schema = v, target = ?target, "upgrade finalized");
            }
            report.after = None;
        }
    }
    Ok(report)
}
