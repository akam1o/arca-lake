//! Capacity rebalancing between the groups of a tier (Phase 3.8, R-82).
//!
//! New chunks already go to the groups with the most room (Phase 3.7);
//! data written earlier stays where it is, and one full member stops its
//! whole group. A pass moves chunks from the group with the least room to
//! the group of the same tier, scheme and shape with the most, through the
//! tier migration path ([`arca_lake_tiering::migrate::migrate_chunk`]:
//! migration intent, copy, read back, switch conditioned on the record,
//! source shards to reconciliation). Moving between groups of one shape
//! keeps the durability and failure-domain guarantees of both: every group
//! was formed under the failure-domain constraint, and the switch names
//! the target's placement epoch.
//!
//! Safety rules of a pass:
//! - the controller leader only (a scheduled leader task, fenced store);
//! - nothing while paused, while any group is not `Active` or a repair
//!   task is open (repair first), or while a tier refuses all but repair
//!   (`recovery_only`);
//! - targets are written as `migration` class: a node refuses them at its
//!   client reserve, and the pass stops on the first refusal;
//! - at most `bytes_per_pass` per pass (the rate limit);
//! - the cursor (the last generation done) is persisted after the pass, so
//!   a new leader resumes there; a chunk moved twice is harmless (the
//!   switch is conditioned on the placement read, `AlreadyThere` and
//!   `Skipped` move nothing);
//! - dry run plans and reports without moving.
//!
//! The source space comes back once reconciliation deletes the source
//! shards and compaction reclaims their segments.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::admission::AdmissionRecord;
use arca_lake_metadata::ops::rebalance::{self, RebalancePlan, RebalanceState};
use arca_lake_metadata::ops::{cluster, integrity, object};
use arca_lake_metadata::records::{GenState, GroupRecord, GroupState, NodeRecord};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateOutcome, migrate_chunk};

use crate::ControlError;

/// When and how much to rebalance.
#[derive(Debug, Clone, PartialEq)]
pub struct RebalancePolicy {
    /// Start when a tier's least roomy group has less than `1 - start` of
    /// the roomiest one's room (0.25: a quarter less).
    pub start_imbalance: f64,
    /// Stop a move once the imbalance is below this (hysteresis).
    pub stop_imbalance: f64,
    /// Logical bytes moved per pass at most (the rate limit: per pass
    /// interval).
    pub bytes_per_pass: u64,
    /// Generations examined per pass at most.
    pub generations_per_pass: usize,
}

impl Default for RebalancePolicy {
    fn default() -> Self {
        Self {
            start_imbalance: 0.25,
            stop_imbalance: 0.10,
            bytes_per_pass: 256 << 20,
            generations_per_pass: 4096,
        }
    }
}

/// What a pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebalanceReport {
    /// The move planned (`None`: nothing to do).
    pub plan: Option<RebalancePlan>,
    /// Chunks moved.
    pub moved_chunks: u64,
    /// Logical bytes moved.
    pub moved_bytes: u64,
    /// Chunks of the source examined but not moved (changed, in flight,
    /// failed).
    pub skipped_chunks: u64,
    /// Chunks of the source a dry run would move (within the pass budget).
    pub planned_chunks: u64,
    /// Why the pass stopped or did nothing.
    pub reason: String,
}

/// (tier, scheme, data shards, parity shards): groups of one shape.
type Shape = (String, String, u16, u16);

/// Imbalance of two rooms: 0 equal, 1 one of them has none.
#[allow(clippy::cast_precision_loss)]
fn imbalance(min: u64, max: u64) -> f64 {
    if max == 0 {
        0.0
    } else {
        1.0 - min as f64 / max as f64
    }
}

/// The moves worth making, best first: per tier and shape, the least roomy
/// group to the roomiest, when their imbalance is at least
/// `start_imbalance`; ordered by imbalance, then source id. `current` (the
/// plan in progress) comes first while its imbalance is above
/// `stop_imbalance` (hysteresis).
#[must_use]
pub fn plans(
    groups: &[GroupRecord],
    record: &AdmissionRecord,
    policy: &RebalancePolicy,
    current: Option<&RebalancePlan>,
) -> Vec<RebalancePlan> {
    let room: HashMap<u64, u64> = record
        .tiers
        .iter()
        .flat_map(|t| t.group_room.iter().map(|g| (g.group, g.room)))
        .collect();
    let by_id: HashMap<u64, &GroupRecord> = groups.iter().map(|g| (g.id, g)).collect();
    let eligible = |g: &GroupRecord| g.state == GroupState::Active && !g.is_cache();
    let mut out = Vec::new();
    if let Some(p) = current
        && let (Some(s), Some(t)) = (by_id.get(&p.source), by_id.get(&p.target))
        && eligible(s)
        && eligible(t)
        && let (Some(&sr), Some(&tr)) = (room.get(&p.source), room.get(&p.target))
        && sr < tr
        && imbalance(sr, tr) > policy.stop_imbalance
    {
        out.push(RebalancePlan {
            source_room: sr,
            target_room: tr,
            ..p.clone()
        });
    }
    let mut classes: HashMap<Shape, Vec<(&GroupRecord, u64)>> = HashMap::new();
    for g in groups.iter().filter(|g| eligible(g)) {
        if let Some(&r) = room.get(&g.id) {
            classes
                .entry((
                    g.tier.clone(),
                    g.scheme.clone(),
                    g.data_shards,
                    g.parity_shards,
                ))
                .or_default()
                .push((g, r));
        }
    }
    let mut ranked: Vec<(f64, RebalancePlan)> = Vec::new();
    for ((tier, ..), mut members) in classes {
        if members.len() < 2 {
            continue;
        }
        members.sort_by_key(|(g, r)| (*r, g.id));
        let (src, sr) = members[0];
        let (dst, tr) = members[members.len() - 1];
        let i = imbalance(sr, tr);
        if i >= policy.start_imbalance {
            ranked.push((
                i,
                RebalancePlan {
                    tier,
                    source: src.id,
                    target: dst.id,
                    source_room: sr,
                    target_room: tr,
                },
            ));
        }
    }
    ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.source.cmp(&b.1.source)));
    for (_, p) in ranked {
        if !out
            .iter()
            .any(|o: &RebalancePlan| o.source == p.source && o.target == p.target)
        {
            out.push(p);
        }
    }
    out
}

/// One pass (the controller leader's scheduled task).
///
/// # Errors
///
/// Metadata errors (a former leader's state write is fenced). Errors of
/// individual chunk moves are counted, not returned.
#[allow(clippy::too_many_lines)]
pub async fn run_once<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &RebalancePolicy,
    now_ms: u64,
) -> Result<RebalanceReport, ControlError> {
    let mut state = rebalance::state(meta).await?;
    let mut report = RebalanceReport::default();
    let finish = async |meta: &S, state: &mut RebalanceState, report: &RebalanceReport| {
        state.last_pass_ms = now_ms;
        state.last_reason.clone_from(&report.reason);
        state.plan.clone_from(&report.plan);
        state.moved_chunks += report.moved_chunks;
        state.moved_bytes += report.moved_bytes;
        state.skipped_chunks += report.skipped_chunks;
        rebalance::record_progress(meta, state).await
    };
    // Intents of moves that crashed (this or a former leader's, or
    // tiering's): their chunks can move again, their targets are
    // reconciliation's.
    object::expire_migrations(meta, now_ms).await?;
    if state.paused {
        report.reason = "paused".into();
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let groups = cluster::list_groups(meta).await?;
    if let Some(g) = groups
        .iter()
        .find(|g| !g.is_cache() && g.state != GroupState::Active)
    {
        report.reason = format!("group {} is {:?}: repair first", g.id, g.state);
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let (open, unrecoverable, failed) = integrity::queue_summary(meta).await?;
    if open > unrecoverable + failed {
        report.reason = format!(
            "{} repair tasks open: repair first",
            open - unrecoverable - failed
        );
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let Some(record) = arca_lake_metadata::ops::admission::current(meta).await? else {
        report.reason = "no admission record (group rooms unknown)".into();
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    };
    let candidates = plans(&groups, &record, policy, state.plan.as_ref());
    if candidates.is_empty() {
        report.reason = "balanced".into();
        state.cursor = None;
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let by_id: HashMap<u64, &GroupRecord> = groups.iter().map(|g| (g.id, g)).collect();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let addr_of = |id: u64| nodes.get(&id).map(|n| n.address.clone());
    let coders = Coders::default();
    let mut after = state.cursor;
    let mut examined = 0usize;
    for p in candidates {
        if record.tier(&p.tier).is_some_and(|t| t.state >= 4) {
            report.reason = format!("tier {} is recovery_only", p.tier);
            continue;
        }
        let (source, target) = (by_id[&p.source], by_id[&p.target]);
        // A plan other than the one the cursor belongs to scans from the
        // start.
        if state
            .plan
            .as_ref()
            .is_none_or(|c| (c.source, c.target) != (p.source, p.target))
        {
            after = None;
        }
        report.plan = Some(p.clone());
        // Never move into the target more than it can take while staying
        // ahead of the source (half the difference of the rooms).
        let budget = policy
            .bytes_per_pass
            .min(p.target_room.saturating_sub(p.source_room) / 2);
        let mut spent = 0u64;
        let mut found = 0u64;
        let started_at_start = after.is_none();
        let mut stopped = false;
        'pages: loop {
            let page = object::list_generations(meta, after, 256).await?;
            let Some((last, _)) = page.last() else {
                after = None;
                report.reason = "cursor wrapped".into();
                break;
            };
            let last = *last;
            for (generation, header) in &page {
                if header.state != GenState::Committed {
                    after = Some(*generation);
                    continue;
                }
                examined += 1;
                let manifest = object::read_manifest_of(meta, generation)
                    .await?
                    .unwrap_or_default();
                for (index, record) in &manifest {
                    if record.placement.group_id != source.id {
                        continue;
                    }
                    found += 1;
                    let len = u64::from(record.len);
                    if spent + len > budget {
                        report.reason = "pass budget spent".into();
                        stopped = true;
                        break 'pages;
                    }
                    if state.dry_run {
                        report.planned_chunks += 1;
                        spent += len;
                        continue;
                    }
                    match migrate_chunk(
                        meta, clients, &coders, &addr_of, source, target, generation, *index,
                        record, None,
                    )
                    .await
                    {
                        Ok(MigrateOutcome::Switched { .. }) => {
                            report.moved_chunks += 1;
                            report.moved_bytes += len;
                            spent += len;
                        }
                        Ok(_) => report.skipped_chunks += 1,
                        Err(arca_lake_tiering::TierError::Node { addr, status })
                            if status.code() == tonic::Code::ResourceExhausted =>
                        {
                            // The target is at its reserve: stop here; this
                            // generation is retried next pass.
                            report.skipped_chunks += 1;
                            report.reason = format!("target node {addr} at its reserve");
                            stopped = true;
                            break 'pages;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, index, "rebalance: chunk move failed");
                            report.skipped_chunks += 1;
                        }
                    }
                }
                after = Some(*generation);
                if examined >= policy.generations_per_pass {
                    report.reason = "pass generation limit".into();
                    stopped = true;
                    break 'pages;
                }
            }
            if page.len() < 256 {
                after = None;
                report.reason = "cursor wrapped".into();
                break;
            }
            after = Some(last);
        }
        // A whole scan found nothing of the source (its room is taken by
        // other groups on the same nodes): try the next plan.
        if stopped || found > 0 || !started_at_start {
            break;
        }
        report.plan = None;
        report.reason = "nothing to move".into();
    }
    if state.dry_run {
        report.reason = format!("dry run: {}", report.reason);
    } else {
        state.cursor = after;
    }
    finish(meta, &mut state, &report).await?;
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::manual_assert, clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use arca_lake_metadata::ops::admission::{GroupRoom, TierAdmission};
    use arca_lake_metadata::records::GroupRole;

    fn group(id: u64, scheme: &str, k: u16) -> GroupRecord {
        GroupRecord {
            id,
            profile: "p".into(),
            tier: "default".into(),
            scheme: scheme.into(),
            data_shards: k,
            parity_shards: 1,
            members: Vec::new(),
            epoch: 1,
            state: GroupState::Active,
            repair_cursor: None,
            role: GroupRole::Durable,
        }
    }

    fn record(rooms: &[(u64, u64)]) -> AdmissionRecord {
        AdmissionRecord {
            tiers: vec![TierAdmission {
                tier: "default".into(),
                group_room: rooms
                    .iter()
                    .map(|&(group, room)| GroupRoom { group, room })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn plans_pair_groups_of_one_shape_with_hysteresis() {
        let p = RebalancePolicy::default();
        let groups = vec![
            group(1, "rs", 2),
            group(2, "rs", 2),
            group(3, "rs", 4),
            group(4, "replication", 1),
        ];
        // Balanced enough: nothing.
        assert_eq!(
            plans(&groups, &record(&[(1, 90), (2, 100)]), &p, None),
            Vec::new()
        );
        // 1 vs 2 at 40 %; 3 has no peer of its shape, 4 neither.
        let v = plans(
            &groups,
            &record(&[(1, 60), (2, 100), (3, 1), (4, 1000)]),
            &p,
            None,
        );
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].source, v[0].target), (1, 2));
        // In progress at 15 %: continues (above the stop), a new one would
        // not start.
        let v2 = plans(&groups, &record(&[(1, 85), (2, 100)]), &p, Some(&v[0]));
        assert_eq!(v2.len(), 1);
        assert_eq!(
            plans(&groups, &record(&[(1, 85), (2, 100)]), &p, None),
            Vec::new()
        );
        // Below the stop: done.
        assert_eq!(
            plans(&groups, &record(&[(1, 95), (2, 100)]), &p, Some(&v[0])),
            Vec::new()
        );
        // A group not Active takes no part.
        let mut sealed = groups.clone();
        sealed[1].state = GroupState::Degraded;
        assert_eq!(
            plans(&sealed, &record(&[(1, 60), (2, 100)]), &p, None),
            Vec::new()
        );
    }
}
