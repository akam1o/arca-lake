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
//! - dry run plans and reports without moving;
//! - capacity (Phase 3.10, R-93): work of a higher priority goes first
//!   (repair, client writes, GC and compaction, tiering). No pass while
//!   tiering moves chunks or the GC backlog is above
//!   `gc_backlog_pause_bytes`. No move in a tier that is throttled or
//!   worse (moving data makes no room in a tier; it only evens it out,
//!   and costs the target room until the source is reclaimed); half the
//!   budget in `warning`. Before each chunk the target's members are
//!   asked for their device now, and the chunk moves only if every one
//!   keeps the placement threshold (`group_min_shards` of the largest
//!   shards above its reserve and in-flight writes) after its shard, and
//!   none needs compaction first. A pass stops at the first that does
//!   not: client writes keep the room they are admitted on.
//!
//! The source space comes back once reconciliation deletes the source
//! shards, the nodes reclaim them (after their retention, ADR-0028) and
//! compaction reclaims their segments.
//!
//! Units (Phase 3.12): a group's room is its fullest member's headroom, so
//! a plan's need and a chunk's effect are counted per member: a chunk of
//! `len` bytes takes one shard of `len / k` on every member of the target
//! and gives as much back on every member of the source
//! ([`member_bytes`]). Until it comes back, the source's room is counted
//! with what its moves will give back (the in-transit record): a plan made
//! before that would otherwise move again what was moved, then back.

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
    /// After a plan moved what it needed, its tier is not planned again
    /// for this long (ms): the sources' old copies are reconciled and
    /// compacted away meanwhile, so that the next plan sees their room.
    pub settle_ms: u64,
    /// Largest chunk clients write (the admission's), bytes: with
    /// `group_min_shards`, the room a target member keeps (R-93).
    pub max_chunk_bytes: u64,
    /// A target member keeps room for this many of the largest shards
    /// after a move (the admission's placement threshold, R-93).
    pub group_min_shards: u64,
    /// No pass while the GC backlog is above this, logical bytes (R-93).
    pub gc_backlog_pause_bytes: u64,
    /// An admission record older than this is not planned on, ms (R-93).
    pub report_max_age_ms: u64,
}

impl Default for RebalancePolicy {
    fn default() -> Self {
        Self {
            start_imbalance: 0.25,
            stop_imbalance: 0.10,
            bytes_per_pass: 256 << 20,
            generations_per_pass: 4096,
            // Default orphan age (900 s) and two reconcile intervals.
            settle_ms: 1_500_000,
            max_chunk_bytes: 8 << 20,
            group_min_shards: 64,
            gc_backlog_pause_bytes: 4 << 30,
            report_max_age_ms: 60_000,
        }
    }
}

/// What the rest of the controller is doing (R-93): a pass yields to it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pressure {
    /// GC backlog, logical bytes (the last GC pass's).
    pub gc_backlog_bytes: u64,
    /// A tiering pass is moving chunks.
    pub tiering_active: bool,
}

/// Bytes every member of `group` takes for a chunk of `len` bytes (one
/// shard, at the store's 4 KiB alignment): what a move of it changes in the
/// room of each member of the source and the target, the unit of a group's
/// room.
#[must_use]
pub fn member_bytes(group: &GroupRecord, len: u64) -> u64 {
    len.div_ceil(u64::from(group.data_shards.max(1)))
        .next_multiple_of(4096)
}

/// Whether a move whose shards take `per_member` bytes inverts a pair
/// whose rooms differ by `difference`: the source gains what the target
/// loses, so the difference shrinks by twice that.
#[must_use]
pub fn inverts(per_member: u64, difference: u64) -> bool {
    per_member.saturating_mul(2) >= difference
}

/// Room every member of `target` must keep after a shard of a chunk of
/// `len` bytes (the placement threshold), and that shard's length.
fn target_floor(policy: &RebalancePolicy, target: &GroupRecord, len: u64) -> (u64, u64) {
    let k = u64::from(target.data_shards.max(1));
    let floor = (policy.max_chunk_bytes / k).saturating_mul(policy.group_min_shards);
    (floor, member_bytes(target, len))
}

/// Whether every member of `target` takes a shard of a `len`-byte chunk
/// now (R-93): asked for its device, it keeps the placement threshold
/// above its reserve and in-flight writes after the shard, and does not
/// need compaction first (most of its little room is dead data). `Err`:
/// why not.
async fn target_takes(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    policy: &RebalancePolicy,
    target: &GroupRecord,
    len: u64,
    now_ms: u64,
) -> Result<(), String> {
    let (floor, shard) = target_floor(policy, target, len);
    for m in &target.members {
        let Some(n) = nodes.get(&m.node_id) else {
            return Err(format!("target node {} unknown", m.node_id));
        };
        let h = clients
            .health(&n.address)
            .await
            .map_err(|_| format!("target node {} did not report its device", n.id))?;
        let d = crate::admission::device_of(n.id, &h, now_ms);
        if d.total == 0 {
            return Err(format!("target node {} reports no device", n.id));
        }
        let room = d.headroom();
        if room < floor.saturating_add(shard) {
            return Err(format!(
                "target node {} at the placement threshold ({room} bytes of room, {floor} kept for client writes)",
                n.id
            ));
        }
        if d.reclaimable >= room && room < floor.saturating_mul(4) {
            return Err(format!(
                "target node {} needs compaction first ({} bytes reclaimable, {room} of room)",
                n.id, d.reclaimable
            ));
        }
    }
    Ok(())
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
    /// Passes stopped because the target would have gone below the
    /// placement threshold or needed compaction first (R-93).
    pub capacity_stops: u64,
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
    plans_with(groups, record, policy, current, &|_, _| 0)
}

/// [`plans`] with each group's room counted with what is coming back to it
/// (`coming_back(group, room)`, per member: the in-transit record).
#[must_use]
pub fn plans_with(
    groups: &[GroupRecord],
    record: &AdmissionRecord,
    policy: &RebalancePolicy,
    current: Option<&RebalancePlan>,
    coming_back: &dyn Fn(u64, u64) -> u64,
) -> Vec<RebalancePlan> {
    let room: HashMap<u64, u64> = record
        .tiers
        .iter()
        .flat_map(|t| {
            t.group_room
                .iter()
                .map(|g| (g.group, g.room.saturating_add(coming_back(g.group, g.room))))
        })
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
pub async fn run_once<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &RebalancePolicy,
    now_ms: u64,
) -> Result<RebalanceReport, ControlError> {
    run_once_with(meta, clients, policy, Pressure::default(), now_ms).await
}

/// [`run_once`] yielding to `pressure` (R-93).
///
/// # Errors
///
/// As [`run_once`].
#[allow(clippy::too_many_lines)]
pub async fn run_once_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &RebalancePolicy,
    pressure: Pressure,
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
    if integrity::any_actionable(meta).await? {
        report.reason = "repair tasks open: repair first".to_string();
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    if pressure.tiering_active {
        report.reason = "tiering is moving chunks: tiering first".into();
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    if pressure.gc_backlog_bytes > policy.gc_backlog_pause_bytes {
        report.reason = format!(
            "GC backlog of {} bytes: GC first",
            pressure.gc_backlog_bytes
        );
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let Some(record) = arca_lake_metadata::ops::admission::current(meta).await? else {
        report.reason = "no admission record (group rooms unknown)".into();
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    };
    if now_ms.saturating_sub(record.computed_ms) > policy.report_max_age_ms {
        report.reason = format!(
            "admission record {} ms old: group rooms not current",
            now_ms.saturating_sub(record.computed_ms)
        );
        finish(meta, &mut state, &report).await?;
        return Ok(report);
    }
    let mut pacing = rebalance::pacing(meta).await?;
    pacing.settling.retain(|(_, until)| *until > now_ms);
    let mut transit = rebalance::in_transit(meta).await?;
    let reported: HashMap<u64, u64> = record
        .tiers
        .iter()
        .flat_map(|t| t.group_room.iter().map(|g| (g.group, g.room)))
        .collect();
    let all = plans_with(&groups, &record, policy, state.plan.as_ref(), &|g, room| {
        transit.coming_back(g, room, now_ms)
    });
    let settling = !all.is_empty()
        && all
            .iter()
            .all(|p| pacing.settling.iter().any(|(t, _)| *t == p.tier));
    let candidates: Vec<RebalancePlan> = all
        .into_iter()
        .filter(|p| !pacing.settling.iter().any(|(t, _)| *t == p.tier))
        .collect();
    if settling {
        report.reason = "settling: the last plan's sources are reclaiming their space".into();
        state.cursor = None;
        finish(meta, &mut state, &report).await?;
        rebalance::put_pacing(meta, &pacing).await?;
        return Ok(report);
    }
    if candidates.is_empty() {
        report.reason = "balanced".into();
        state.cursor = None;
        // A need computed for a plan that is no longer wanted is not
        // reused if the same pair comes back later.
        if pacing.plan.take().is_some() {
            rebalance::put_pacing(meta, &pacing).await?;
        }
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
    // Bytes per member moved out of the plan's source this pass.
    let mut moved_member = 0u64;
    for p in candidates {
        let tier_state = record.tier(&p.tier).map_or(0, |t| t.state);
        if tier_state >= 2 {
            // Client writes first: moving data makes no room in the tier.
            report.reason = format!(
                "tier {} is {}: client writes first",
                p.tier,
                [
                    "normal",
                    "warning",
                    "throttled",
                    "write_rejected",
                    "recovery_only"
                ][usize::from(tier_state.min(4))]
            );
            continue;
        }
        if record
            .tier(&p.tier)
            .is_some_and(|t| t.excluded_groups.contains(&p.target))
        {
            report.reason = format!("target group {} excluded from placement", p.target);
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
        // ahead of the source: half the difference of the rooms, computed
        // once when the plan starts (the rooms seen later do not show the
        // source's space until its old copies are reclaimed).
        let key = (p.tier.clone(), p.source, p.target);
        if pacing.plan.as_ref() != Some(&key) {
            pacing.plan = Some(key);
            pacing.remaining_bytes = p.target_room.saturating_sub(p.source_room) / 2;
        }
        let per_pass = if tier_state == 1 {
            policy.bytes_per_pass / 2
        } else {
            policy.bytes_per_pass
        };
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
                    // At least one chunk per pass, whatever its size (a
                    // chunk larger than the budget would otherwise stop
                    // every pass at the same place), unless moving it
                    // would make the target the fuller group (the next
                    // pass would move it back). Counted per member, the
                    // unit of the rooms (Phase 3.12).
                    let per_member = member_bytes(target, len);
                    let difference = p.target_room.saturating_sub(p.source_room);
                    if inverts(per_member, difference) {
                        // Moving it would invert the pair: leave it, and
                        // keep scanning (stopping here would stop every
                        // pass at the same chunk).
                        report.skipped_chunks += 1;
                        continue;
                    }
                    if spent > 0
                        && (spent + len > per_pass
                            || moved_member + per_member > pacing.remaining_bytes)
                    {
                        report.reason = "pass budget spent".into();
                        stopped = true;
                        break 'pages;
                    }
                    if state.dry_run {
                        report.planned_chunks += 1;
                        spent += len;
                        continue;
                    }
                    if let Err(why) =
                        target_takes(clients, &nodes, policy, target, len, now_ms).await
                    {
                        report.reason = why;
                        report.capacity_stops += 1;
                        stopped = true;
                        break 'pages;
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
                            moved_member += per_member;
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
        if report.moved_chunks > 0
            && let Some(p) = &report.plan
        {
            // What the source gets back, counted until it shows in its
            // room (at the latest the nodes' retention and a settle after
            // now: reconciliation, reclaim, compaction).
            let retention = retention_of(clients, &nodes, by_id[&p.source]).await;
            transit.add(
                p.source,
                moved_member,
                reported.get(&p.source).copied().unwrap_or(0),
                now_ms
                    .saturating_add(retention)
                    .saturating_add(policy.settle_ms.saturating_mul(2)),
                now_ms,
            );
            rebalance::put_in_transit(meta, &transit).await?;
            pacing.remaining_bytes = pacing.remaining_bytes.saturating_sub(moved_member);
            if pacing.remaining_bytes == 0
                && let Some((tier, ..)) = pacing.plan.take()
            {
                pacing.settling.push((tier, now_ms + policy.settle_ms));
                state.cursor = None;
                report.reason = format!("plan done, settling: {}", report.reason);
            }
        }
        rebalance::put_pacing(meta, &pacing).await?;
    }
    finish(meta, &mut state, &report).await?;
    Ok(report)
}

/// The longest deferred-reclamation retention of `group`'s members (ms;
/// a member not answering: a day, the default).
async fn retention_of(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
) -> u64 {
    let mut longest = 0;
    for m in &group.members {
        let r = match nodes.get(&m.node_id) {
            Some(n) => clients
                .health(&n.address)
                .await
                .map_or(86_400_000, |h| h.reclaim_retention_ms),
            None => 86_400_000,
        };
        longest = longest.max(r);
    }
    longest
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

    /// The convergence simulator (Phase 3.12 §6): the planning and pacing
    /// of a pass over disjoint groups whose members are equal, a minute a
    /// pass, with client writes placed on the roomiest group and the
    /// source's space given back only after reconciliation, the nodes'
    /// retention and compaction. `fixed`: per-member units and the
    /// in-transit record (this release); otherwise the accounting of
    /// earlier releases (logical bytes against per-member rooms, no
    /// in-transit). Deterministic.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct Sim {
        k: u16,
        groups: usize,
        retention_ms: u64,
        write_bytes_per_pass: u64,
        hours: u64,
        fixed: bool,
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq)]
    struct Outcome {
        /// Minutes until the actual imbalance stayed below the start.
        settled_min: Option<u64>,
        /// Actual imbalance at the end.
        final_imbalance: f64,
        /// Largest actual imbalance after it was first below the start
        /// (a relapse: what was evened out comes apart again).
        worst_after: f64,
        /// Plans whose source was the target of an earlier one.
        reversals: u64,
        /// Bytes per member moved, all passes.
        moved_member: u64,
        /// Moves below the placement threshold (must stay 0).
        floor_violations: u64,
    }

    #[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
    fn simulate(sim: Sim) -> Outcome {
        const GIB: u64 = 1 << 30;
        const CHUNK: u64 = 8 << 20;
        let policy = RebalancePolicy::default();
        let usable = 100 * GIB;
        let groups: Vec<GroupRecord> = (0..sim.groups)
            .map(|i| group(i as u64 + 1, "rs", sim.k))
            .collect();
        // Group 1 nearly full, the others half.
        let mut used: Vec<u64> = (0..sim.groups)
            .map(|i| if i == 0 { 90 * GIB } else { 50 * GIB })
            .collect();
        let s = member_bytes(&groups[0], CHUNK);
        let (floor, _) = target_floor(&policy, &groups[0], CHUNK);
        // Reconciliation (orphan age and an interval), then the
        // retention, then compaction.
        let delay = 1_200_000 + sim.retention_ms + 600_000;
        let mut frees: Vec<(u64, usize, u64)> = Vec::new();
        let mut transit = rebalance::RebalanceInTransit::default();
        let mut pacing = rebalance::RebalancePacing::default();
        let mut current: Option<RebalancePlan> = None;
        let mut out = Outcome::default();
        let mut targets_seen: Vec<u64> = Vec::new();
        let mut first_done = false;
        let mut below_since: Option<u64> = None;
        let mut write_carry = 0u64;
        let actual = |used: &[u64]| {
            let rooms: Vec<u64> = used.iter().map(|u| usable.saturating_sub(*u)).collect();
            imbalance(*rooms.iter().min().unwrap(), *rooms.iter().max().unwrap())
        };
        for minute in 0..sim.hours * 60 {
            let now = minute * 60_000;
            frees.retain(|&(at, g, b)| {
                if at <= now {
                    used[g] = used[g].saturating_sub(b);
                    false
                } else {
                    true
                }
            });
            // Client writes: chunks to the roomiest group (Phase 3.7).
            write_carry += sim.write_bytes_per_pass;
            while write_carry >= CHUNK {
                write_carry -= CHUNK;
                let g = (0..sim.groups)
                    .max_by_key(|&g| (usable.saturating_sub(used[g]), g))
                    .unwrap();
                used[g] += s;
            }
            let i = actual(&used);
            if i < policy.start_imbalance {
                below_since.get_or_insert(minute);
                first_done = true;
            } else {
                below_since = None;
            }
            if first_done {
                out.worst_after = out.worst_after.max(i);
            }
            let rec = record(
                &(0..sim.groups)
                    .map(|g| (g as u64 + 1, usable.saturating_sub(used[g])))
                    .collect::<Vec<_>>(),
            );
            pacing.settling.retain(|(_, until)| *until > now);
            let all = if sim.fixed {
                plans_with(&groups, &rec, &policy, current.as_ref(), &|g, room| {
                    transit.coming_back(g, room, now)
                })
            } else {
                plans(&groups, &rec, &policy, current.as_ref())
            };
            let Some(p) = all.into_iter().next() else {
                current = None;
                pacing.plan = None;
                continue;
            };
            if !pacing.settling.is_empty() {
                continue;
            }
            let key = (p.tier.clone(), p.source, p.target);
            if pacing.plan.as_ref() != Some(&key) {
                if targets_seen.contains(&p.source) {
                    out.reversals += 1;
                }
                targets_seen.push(p.target);
                pacing.plan = Some(key);
                pacing.remaining_bytes = p.target_room.saturating_sub(p.source_room) / 2;
            }
            current = Some(p.clone());
            let (src, dst) = (
                usize::try_from(p.source - 1).unwrap(),
                usize::try_from(p.target - 1).unwrap(),
            );
            let difference = p.target_room.saturating_sub(p.source_room);
            let (mut spent, mut moved) = (0u64, 0u64);
            loop {
                let inverts_now = if sim.fixed {
                    inverts(s, difference)
                } else {
                    CHUNK >= difference
                };
                let (need, unit) = if sim.fixed {
                    (moved, s)
                } else {
                    (spent, CHUNK)
                };
                if inverts_now
                    || (spent > 0
                        && (spent + CHUNK > policy.bytes_per_pass
                            || need + unit > pacing.remaining_bytes))
                {
                    break;
                }
                // target_takes: the threshold kept after the shard.
                let room = usable.saturating_sub(used[dst]);
                if room < floor + s {
                    break;
                }
                if room - s < floor {
                    out.floor_violations += 1;
                }
                used[dst] += s;
                frees.push((now + delay, src, s));
                spent += CHUNK;
                moved += s;
            }
            out.moved_member += moved;
            if moved > 0 {
                let done = if sim.fixed { moved } else { spent };
                if sim.fixed {
                    transit.add(
                        p.source,
                        moved,
                        usable.saturating_sub(used[src]),
                        now + sim.retention_ms + 2 * policy.settle_ms,
                        now,
                    );
                }
                pacing.remaining_bytes = pacing.remaining_bytes.saturating_sub(done);
                if pacing.remaining_bytes == 0 {
                    pacing.plan = None;
                    pacing
                        .settling
                        .push((p.tier.clone(), now + policy.settle_ms));
                }
            }
        }
        out.final_imbalance = actual(&used);
        out.settled_min = below_since;
        out
    }

    #[test]
    fn convergence_simulator_compares_the_accounting() {
        let mut rows = Vec::new();
        for k in [2u16, 4] {
            for retention_ms in [0u64, 86_400_000] {
                for write in [0u64, 8 << 20] {
                    let base = Sim {
                        k,
                        groups: 3,
                        retention_ms,
                        write_bytes_per_pass: write,
                        hours: 24 * 5,
                        fixed: false,
                    };
                    let old = simulate(base);
                    let new = simulate(Sim {
                        fixed: true,
                        ..base
                    });
                    eprintln!(
                        "SIM k={k} retention_h={} write_MiB/min={} | old {:?} | new {:?}",
                        retention_ms / 3_600_000,
                        write >> 20,
                        old,
                        new
                    );
                    rows.push((base, old, new));
                }
            }
        }
        for (base, old, new) in &rows {
            assert_eq!(new.floor_violations, 0, "{base:?}");
            assert_eq!(old.floor_violations, 0, "{base:?}");
            // This release converges, and never moves back what it moved.
            assert!(new.settled_min.is_some(), "{base:?}: {new:?}");
            assert_eq!(new.reversals, 0, "{base:?}: {new:?}");
            assert!(new.final_imbalance < 0.25, "{base:?}: {new:?}");
            // Once below the start, it stays within a step of it.
            assert!(new.worst_after < 0.26, "relapse: {base:?}: {new:?}");
            // ... no later, and with no more data moved, than before.
            assert!(
                new.settled_min.unwrap() <= old.settled_min.unwrap_or(u64::MAX),
                "{base:?}: {old:?} {new:?}"
            );
        }
    }
}
