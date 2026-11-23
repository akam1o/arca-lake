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
use arca_lake_metadata::keys;
use arca_lake_metadata::ops::compat::{self, VersionKey};
use arca_lake_metadata::ops::{cluster, recovery};
use arca_lake_metadata::records::{UpgradeCursor, UpgradeStep};
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
            let blockers = precondition(meta, clients, &cur, now_ms).await?;
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
            // Step 3: the downgrade cut-off. The precondition again (ADR-0039):
            // a process of an earlier release started since step 1 (a
            // rollback within the window) holds the cut-off back.
            let blockers = precondition(meta, clients, &cur, now_ms).await?;
            if !blockers.is_empty() {
                let why = format!("precondition at the cut-off: {}", blockers.join("; "));
                compat::note_upgrade_error(meta, &why, now_ms).await?;
                report.blocked = Some(why);
                return Ok(report);
            }
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
            // metadata would). Only a decommissioned node the operator
            // also names with --ignore-absent is not waited for (an
            // explicit decision: its disk must never serve again unwiped).
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
            // Removed nodes are not waited for (Phase 3.11, R-100: an
            // unregistered node never serves again under this cluster).
            let removed: Vec<u64> = arca_lake_metadata::ops::node_lifecycle::retirements(meta)
                .await?
                .into_iter()
                .filter(arca_lake_metadata::ops::node_lifecycle::NodeRetirementRecord::is_removed)
                .map(|r| r.node_id)
                .collect();
            let waited = |n: &arca_lake_metadata::records::NodeRecord| {
                let named_away =
                    decommissioned.contains(&n.id) && cur.ignore_absent.contains(&n.id.to_string());
                !removed.contains(&n.id) && !named_away
            };
            // Processes of an earlier release started after the cut-off
            // (their gate still admits schema `schema`, ADR-0039) are
            // stopped first: once the schema is raised they would run on
            // past the protections they do not enforce.
            let mut missing = procs_below(meta, &cur, now_ms).await?;
            for n in cluster::list_nodes(meta)
                .await?
                .into_iter()
                .filter(|n| waited(n))
            {
                let req = arca_lake_protocol::storage::v1::HealthRequest {
                    schema_floor: target.schema,
                    format_write: target.format,
                    ..Default::default()
                };
                match clients.health_with(&n.address, req).await {
                    Ok(h) if node_below_schema(&h, target.schema) => {
                        missing.push(node_below_message(n.id, &h, target.schema));
                    }
                    // The floor, and a segment of the target format on the
                    // device (re-review: a node that missed the format step,
                    // or whose rollover failed).
                    Ok(h)
                        if h.schema_floor >= target.schema
                            && h.newest_segment_format >= target.format => {}
                    Ok(h) if h.schema_floor >= target.schema => {
                        missing.push(format!(
                            "node {} has no segment of format {} yet (newest {})",
                            n.id, target.format, h.newest_segment_format
                        ));
                    }
                    Ok(h) => missing.push(format!(
                        "node {} keeps schema floor {} (an earlier release?)",
                        n.id, h.schema_floor
                    )),
                    Err(e) => missing.push(format!(
                        "node {} unreachable: {} (bring it back; only if it is decommissioned, name it with --ignore-absent, and never start its disk again unwiped)",
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
            // Again after the raise: a node registered while the step ran
            // is waited for too; until then the upgrade stays open and the
            // step is retried (re-review). So is a process of an earlier
            // release that started before the raise (ADR-0039): its gate
            // admitted it, and it runs on until stopped.
            let mut missing_after = procs_below(meta, &cur, now_ms).await?;
            for n in cluster::list_nodes(meta)
                .await?
                .into_iter()
                .filter(|n| waited(n))
            {
                let req = arca_lake_protocol::storage::v1::HealthRequest {
                    schema_floor: target.schema,
                    format_write: target.format,
                    ..Default::default()
                };
                match clients.health_with(&n.address, req).await {
                    Ok(h) if node_below_schema(&h, target.schema) => {
                        missing_after.push(node_below_message(n.id, &h, target.schema));
                    }
                    // The floor, and a segment of the target format on the
                    // device (re-review: a node that missed the format step,
                    // or whose rollover failed).
                    Ok(h)
                        if h.schema_floor >= target.schema
                            && h.newest_segment_format >= target.format => {}
                    Ok(h) if h.schema_floor >= target.schema => {
                        missing_after.push(format!(
                            "node {} has no segment of format {} yet (newest {})",
                            n.id, target.format, h.newest_segment_format
                        ));
                    }
                    Ok(h) => missing_after.push(format!(
                        "node {} keeps schema floor {} (an earlier release?)",
                        n.id, h.schema_floor
                    )),
                    Err(e) => missing_after.push(format!(
                        "node {} unreachable: {} (bring it back, or decommission it)",
                        n.id,
                        e.code()
                    )),
                }
            }
            if !missing_after.is_empty() {
                let why = format!("schema floor not held: {}", missing_after.join("; "));
                compat::note_upgrade_error(meta, &why, now_ms).await?;
                report.blocked = Some(why);
                return Ok(report);
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

/// Whether node health `h` shows a release that does not enforce the
/// protections of `schema` (ADR-0039: from [`keys::SCHEMA_V5`] on, a node
/// reports the newest schema it enforces; 0.1.0 and earlier report none).
/// Below schema 5 the field is not required (the earlier finalizes predate
/// it).
#[must_use]
pub fn node_below_schema(h: &arca_lake_protocol::storage::v1::HealthResponse, schema: u64) -> bool {
    schema >= keys::SCHEMA_V5 && h.schema_max < schema
}

fn node_below_message(
    id: u64,
    h: &arca_lake_protocol::storage::v1::HealthResponse,
    schema: u64,
) -> String {
    if h.schema_max == 0 {
        format!(
            "node {id} runs {} (0.1.0 or earlier, or an unknown release: it reports no schema): upgrade it to a release of schema {schema} first",
            h.software
        )
    } else {
        format!(
            "node {id} runs {} (enforces schema up to {}): upgrade it to a release of schema {schema} first",
            h.software, h.schema_max
        )
    }
}

/// Fresh heartbeats of processes that do not read the target schema. A
/// process named absent is passed over only while its heartbeat has not
/// advanced since the request (ADR-0039): one that heartbeats after it is
/// not absent.
async fn procs_below<S: KvStore>(
    meta: &S,
    cur: &UpgradeCursor,
    now_ms: u64,
) -> Result<Vec<String>, ControlError> {
    let target = cur.target;
    let mut blockers = Vec::new();
    for p in compat::procs(meta).await? {
        if now_ms.saturating_sub(p.last_ms) > PROC_STALE_MS {
            continue;
        }
        let reaches = p.caps.schema_max >= target.schema && p.caps.protocol_max >= target.protocol;
        if cur.ignore_absent.contains(&p.id) {
            if !reaches && p.last_ms > cur.started_ms {
                blockers.push(format!(
                    "{:?} {} ({}) is named absent but heartbeated after the request (reads schema {}): stop it",
                    p.kind, p.id, p.software, p.caps.schema_max
                ));
            }
            continue;
        }
        if !(p.caps.schema_max >= target.schema && p.caps.protocol_max >= target.protocol) {
            blockers.push(format!(
                "{:?} {} ({}) reads schema {} protocol {}",
                p.kind, p.id, p.software, p.caps.schema_max, p.caps.protocol_max
            ));
        }
    }
    Ok(blockers)
}

/// Step 1 (RFC-014 §5.5): every node (live Health) and every fresh
/// heartbeat reaches the target; absent nodes block unless ignored. From
/// schema 5 on a node must also report the schema (ADR-0039).
async fn precondition<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cur: &UpgradeCursor,
    now_ms: u64,
) -> Result<Vec<String>, ControlError> {
    let target = cur.target;
    let mut blockers = Vec::new();
    // A removed node is out of the cluster (Phase 3.11, R-100);
    // only a decommissioned or removed node may be named absent.
    let states = arca_lake_metadata::ops::node_lifecycle::states(meta).await?;
    let state_of = |id: u64| {
        states
            .get(&id)
            .copied()
            .unwrap_or(arca_lake_metadata::ops::node_lifecycle::State::Active)
    };
    for n in cluster::list_nodes(meta).await? {
        let id = n.id.to_string();
        let state = state_of(n.id);
        if state == arca_lake_metadata::ops::node_lifecycle::State::Removed {
            continue;
        }
        let may_ignore = cur.ignore_absent.contains(&id)
            && matches!(
                state,
                arca_lake_metadata::ops::node_lifecycle::State::Decommissioned
            );
        if cur.ignore_absent.contains(&id) && !may_ignore {
            blockers.push(format!(
                "node {} is named absent but is {}: only a decommissioned node may be (decommission or unregister it first)",
                n.id,
                state.name()
            ));
            continue;
        }
        match clients.health(&n.address).await {
            Ok(h) => {
                let caps = arca_lake_protocol::client::NodeCaps::from_health(&h).caps;
                let ok = caps.protocol_max >= target.protocol && caps.format_max >= target.format;
                if !ok {
                    blockers.push(format!(
                        "node {} ({}) speaks protocol {} format {}",
                        n.id, h.software, caps.protocol_max, caps.format_max
                    ));
                } else if node_below_schema(&h, target.schema) {
                    blockers.push(node_below_message(n.id, &h, target.schema));
                }
            }
            Err(e) if may_ignore => {
                warn!(node = n.id, error = %e, "finalize: absent decommissioned node ignored");
            }
            Err(e) => blockers.push(format!("node {} unreachable: {}", n.id, e.code())),
        }
    }
    blockers.extend(procs_below(meta, cur, now_ms).await?);
    Ok(blockers)
}

#[cfg(test)]
mod tests {
    use arca_lake_protocol::storage::v1 as pb;

    use super::*;

    fn health(software: &str, schema_max: u64) -> pb::HealthResponse {
        pb::HealthResponse {
            software: software.into(),
            schema_max,
            ..Default::default()
        }
    }

    /// A 0.1.0 node reports no schema (the field is new in 0.2.0): it blocks
    /// a finalize to schema 5 and later; the earlier finalizes do not ask.
    #[test]
    fn a_node_without_a_schema_is_below_schema_5_only() {
        let old = health("0.1.0", 0);
        assert!(node_below_schema(&old, keys::SCHEMA_V5));
        assert!(!node_below_schema(&old, keys::SCHEMA_V5 - 1));
        assert!(node_below_message(3, &old, keys::SCHEMA_V5).contains("0.1.0 or earlier"));
        let cur = health("0.2.0", pb::NODE_SCHEMA_MAX);
        assert!(!node_below_schema(&cur, keys::SCHEMA_VERSION));
        assert!(node_below_schema(&cur, keys::SCHEMA_VERSION + 1));
    }

    /// The schema a node reports is this build's metadata schema.
    #[test]
    fn nodes_report_the_metadata_schema() {
        assert_eq!(pb::NODE_SCHEMA_MAX, keys::SCHEMA_VERSION);
        assert_eq!(keys::SCHEMA_V5, 5);
        assert_eq!(keys::SCHEMA_VERSION_MIN, 4 + arca_lake_common::compat::BUMP);
    }
}
