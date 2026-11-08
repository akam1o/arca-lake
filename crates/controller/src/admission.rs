//! Admission pass of the controller leader (Phase 3.6, R-64): the devices
//! heard in the health pass, the GC backlog measured in the GC pass and the
//! GC reclaim rate go into one [`crate::backpressure::Machine`] per tier;
//! the decisions are published for the gateways
//! ([`arca_lake_metadata::ops::admission`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, PoisonError};

use arca_lake_metadata::ops::admission::{self, AdmissionRecord, TierAdmission};
use arca_lake_metadata::ops::{cluster, compat};
use arca_lake_metadata::records::{GroupState, ProcKind};
use arca_lake_metadata::{KvStore, MetaError};

use crate::backpressure::{Decision, Device, Group, Inputs, Machine, Policy, State};

/// How long gateways may use a record as is (several health passes).
pub const RECORD_TTL_MS: u64 = 30_000;

/// Weight of the newest sample in the moving averages.
const ALPHA: f64 = 0.3;

/// Trends (free space falling, backlog growing) are measured over at
/// least this long by default, so that a burst of a few seconds does not
/// throttle (`Policy::trend_window_ms`).
pub const TREND_WINDOW_MS: u64 = 5 * 60_000;

/// Samples kept for trends (longer than any window used).
const TREND_RETENTION_MS: u64 = 15 * 60_000;

/// Samples of one quantity for windowed slopes.
#[derive(Debug, Default)]
struct Trend {
    samples: std::collections::VecDeque<(u64, f64)>,
}

impl Trend {
    /// Add `value` at `now_ms`; returns the change per second over the
    /// default window, 0 until the window is covered.
    fn push(&mut self, now_ms: u64, value: f64) -> f64 {
        self.samples.push_back((now_ms, value));
        while self.samples.len() > 2
            && now_ms.saturating_sub(self.samples[1].0) >= TREND_RETENTION_MS
        {
            self.samples.pop_front();
        }
        self.slope(now_ms)
    }

    /// Whether a sample at least `window_ms` old exists.
    fn covers(&self, now_ms: u64, window_ms: u64) -> bool {
        self.samples
            .front()
            .is_some_and(|(t, _)| now_ms.saturating_sub(*t) >= window_ms)
    }

    fn slope(&self, now_ms: u64) -> f64 {
        self.slope_over(now_ms, TREND_WINDOW_MS)
    }

    /// Change per second between the newest sample at least `window_ms`
    /// old and the latest; 0 when no sample is that old.
    #[allow(clippy::cast_precision_loss)]
    fn slope_over(&self, now_ms: u64, window_ms: u64) -> f64 {
        let Some(&(t1, v1)) = self.samples.back() else {
            return 0.0;
        };
        let Some(&(t0, v0)) = self
            .samples
            .iter()
            .rev()
            .find(|(t, _)| now_ms.saturating_sub(*t) >= window_ms)
        else {
            return 0.0;
        };
        if t1 > t0 {
            (v1 - v0) / ((t1 - t0) as f64 / 1000.0)
        } else {
            0.0
        }
    }
}

/// What the admission pass keeps between passes.
#[derive(Debug, Default)]
pub struct Tracker {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    // Leadership term the state machines belong to (fencing token) and
    // when it started here; generation of the next record.
    term: Option<([u8; 16], u64)>,
    term_started_ms: u64,
    generation: u64,
    // Published states found when the term started, and whether a record
    // existed at all.
    floors: HashMap<String, State>,
    // Groups the published record excluded, kept excluded during warm-up
    // (a member not yet heard may be the full one).
    floor_excluded: HashMap<String, Vec<u64>>,
    devices: HashMap<u64, Device>,
    machines: HashMap<String, Machine>,
    decisions: BTreeMap<String, Decision>,
    // GC backlog (logical bytes) and when it was measured.
    backlog: Option<(u64, u64)>,
    backlog_trend: Trend,
    // Reclaimed physical bytes (cumulative) at the previous pass.
    reclaimed_prev: Option<(u64, u64)>,
    reclaim: f64,
    // Free bytes per tier.
    free_trend: HashMap<String, Trend>,
}

fn ewma(old: f64, new: f64) -> f64 {
    if old == 0.0 {
        new
    } else {
        old * (1.0 - ALPHA) + new * ALPHA
    }
}

impl Tracker {
    /// A device heard in the health pass.
    pub fn note_device(&self, d: Device) {
        self.lock().devices.insert(d.node_id, d);
    }

    /// The GC backlog measured in the GC pass (logical bytes).
    #[allow(clippy::cast_precision_loss)]
    pub fn note_backlog(&self, logical_bytes: u64, now_ms: u64) {
        let mut g = self.lock();
        g.backlog_trend.push(now_ms, logical_bytes as f64);
        g.backlog = Some((now_ms, logical_bytes));
    }

    /// Decisions of the last pass, by tier.
    #[must_use]
    pub fn decisions(&self) -> BTreeMap<String, Decision> {
        self.lock().decisions.clone()
    }

    /// The worst state of the last pass.
    #[must_use]
    pub fn worst(&self) -> State {
        self.lock()
            .decisions
            .values()
            .map(|d| d.state)
            .max()
            .unwrap_or_default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Capacity totals of the durable tiers after a pass (bytes; metrics).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    /// Device bytes.
    pub total: u64,
    /// Device bytes minus the reserves.
    pub usable: u64,
    /// Free bytes client writes may still use.
    pub headroom: u64,
    /// The devices' reserves.
    pub reserved: u64,
    /// Bytes compaction can reclaim.
    pub reclaimable: u64,
    /// Admitted writes in flight.
    pub inflight: u64,
    /// GC backlog, physical bytes (estimated from logical bytes and the
    /// profiles' overhead).
    pub gc_backlog_physical: u64,
    /// GC reclaim rate, physical bytes per second.
    pub gc_reclaim_per_s: u64,
    /// Groups excluded from placement.
    pub excluded_groups: u64,
    /// Nodes sharing a file system unsafely (R-80).
    pub domain_conflicts: u64,
    /// Nodes whose sharing cannot be checked and that have no allotment.
    pub domain_unverified: u64,
}

/// A tier's kind (cache or not), writable groups, and the sums of their
/// data and total shards.
type TierGroups = (bool, Vec<Group>, (u64, u64));

fn state_of(code: u8) -> State {
    match code {
        0 => State::Normal,
        1 => State::Warning,
        2 => State::Throttled,
        3 => State::WriteRejected,
        _ => State::RecoveryOnly,
    }
}

/// One admission pass: evaluate every tier, publish the record. Returns
/// the durable totals.
///
/// # Errors
///
/// Metadata errors (the publication is fenced: a former leader fails).
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn pass<S: KvStore>(
    meta: &S,
    tracker: &Tracker,
    policy: &Policy,
    max_chunk_bytes: u64,
    reclaimed_total: u64,
    now_ms: u64,
) -> Result<Totals, MetaError> {
    // Term 0 of the cluster's current incarnation (tools and tests; the
    // leader passes its own term).
    let incarnation = compat::cluster_state(meta)
        .await?
        .map_or([0; 16], |c| c.incarnation);
    pass_in_term(
        meta,
        tracker,
        policy,
        max_chunk_bytes,
        reclaimed_total,
        Term {
            token: 0,
            pass_started_ms: now_ms,
            incarnation,
        },
        now_ms,
    )
    .await
}

/// The leadership term a pass runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Term {
    /// The leader's fencing token.
    pub token: u64,
    /// When the health pass that heard the devices started (unix ms).
    pub pass_started_ms: u64,
    /// The cluster incarnation the leader runs in (stamped on the record).
    pub incarnation: [u8; 16],
}

/// [`pass`] in leadership term `term` (Phase 3.7, R-85). A new term (a
/// failover, or this process leading again) starts the state machines
/// afresh: nothing of an earlier term's in-memory state is trusted. Until
/// every member of a tier has reported in the new term (or the staleness
/// bound has passed), the tier's state is at least what the record already
/// published says (or `throttled` when there is none): a new leader never
/// relaxes on partial information; after warm-up the usual rules (and
/// hysteresis, from that state) apply.
///
/// # Errors
///
/// As [`pass`].
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_arguments
)]
pub async fn pass_in_term<S: KvStore>(
    meta: &S,
    tracker: &Tracker,
    policy: &Policy,
    max_chunk_bytes: u64,
    reclaimed_total: u64,
    term: Term,
    now_ms: u64,
) -> Result<Totals, MetaError> {
    let new_term = tracker.lock().term != Some((term.incarnation, term.token));
    // What was published before (by this or another leader): only a floor
    // during warm-up, never a state to relax from without evidence.
    let published = if new_term {
        admission::current(meta).await?
    } else {
        None
    };
    {
        let mut g = tracker.lock();
        if new_term {
            g.term = Some((term.incarnation, term.token));
            g.term_started_ms = term.pass_started_ms;
            g.generation = 0;
            g.machines.clear();
            g.decisions.clear();
            g.floors = published
                .iter()
                .flat_map(|r| r.tiers.iter())
                .map(|t| (t.tier.clone(), state_of(t.state)))
                .collect();
            g.floor_excluded = published
                .iter()
                .flat_map(|r| r.tiers.iter())
                .map(|t| (t.tier.clone(), t.excluded_groups.clone()))
                .collect();
        }
    }
    let groups = cluster::list_groups(meta).await?;
    let gateways = compat::procs(meta)
        .await?
        .iter()
        .filter(|p| p.kind == ProcKind::Gateway && now_ms.saturating_sub(p.last_ms) <= 60_000)
        .count();
    let mut totals = Totals::default();
    let mut counted = std::collections::HashSet::new();
    let mut record = AdmissionRecord {
        computed_ms: now_ms,
        ttl_ms: RECORD_TTL_MS,
        gateways: u32::try_from(gateways.max(1)).unwrap_or(u32::MAX),
        tiers: Vec::new(),
        leader_token: term.token,
        generation: 0,
        warming_up: false,
        capacity_conflicts: Vec::new(),
        capacity_unverified: Vec::new(),
        incarnation: term.incarnation,
    };
    {
        let mut g = tracker.lock();
        if let Some((t0, r0)) = g.reclaimed_prev
            && now_ms > t0
        {
            let rate = reclaimed_total.saturating_sub(r0) as f64 / ((now_ms - t0) as f64 / 1000.0);
            g.reclaim = ewma(g.reclaim, rate);
        }
        g.reclaimed_prev = Some((now_ms, reclaimed_total));
    }
    // Tiers and their writable groups.
    // Per tier: whether it is a cache tier, its writable groups, and the
    // sums of their data and total shards (the coding overhead).
    let mut tiers: BTreeMap<String, TierGroups> = BTreeMap::new();
    for gr in &groups {
        if !matches!(gr.state, GroupState::Active | GroupState::Repairing) {
            continue;
        }
        let e = tiers
            .entry(gr.tier.clone())
            .or_insert((gr.is_cache(), Vec::new(), (0, 0)));
        e.1.push(Group {
            id: gr.id,
            data_shards: gr.data_shards,
            members: gr.members.iter().map(|m| m.node_id).collect(),
        });
        e.2.0 += u64::from(gr.data_shards);
        e.2.1 += u64::from(gr.data_shards) + u64::from(gr.parity_shards);
    }
    // Capacity domains over every device heard (R-80).
    let domains = {
        let g = tracker.lock();
        let all: Vec<Device> = g.devices.values().copied().collect();
        check_domains(&all)
    };
    if !domains.conflicts.is_empty() {
        tracing::error!(
            nodes = ?domains.conflicts,
            allowed = policy.allow_shared_filesystem,
            "storage nodes share a file system without allotments that fit it: per-node capacity admission cannot hold their reserves (give each node its own file system, or --capacity-bytes allotments that sum to at most its size)"
        );
    }
    record.capacity_conflicts.clone_from(&domains.conflicts);
    record.capacity_unverified.clone_from(&domains.unverified);
    totals.domain_conflicts = domains.conflicts.len() as u64;
    totals.domain_unverified = domains.unverified.len() as u64;
    {
        let mut g = tracker.lock();
        let (backlog_logical, growth_logical, growth_recent_logical, reclaim) = (
            g.backlog.map_or(0, |b| b.1),
            g.backlog_trend.slope_over(now_ms, policy.trend_window_ms),
            g.backlog_trend.slope_over(now_ms, policy.recent_window_ms),
            g.reclaim,
        );
        for (tier, (is_cache, tier_groups, (k_sum, n_sum))) in tiers {
            let mut members: Vec<u64> = tier_groups
                .iter()
                .flat_map(|gr| gr.members.clone())
                .collect();
            members.sort_unstable();
            members.dedup();
            let devices: Vec<Device> = members
                .iter()
                .map(|&id| {
                    g.devices.get(&id).copied().unwrap_or(Device {
                        node_id: id,
                        ..Device::default()
                    })
                })
                .collect();
            let min_k = tier_groups
                .iter()
                .map(|gr| gr.data_shards)
                .min()
                .unwrap_or(1)
                .max(1);
            let overhead = if k_sum == 0 {
                1.0
            } else {
                n_sum as f64 / k_sum as f64
            };
            // The backlog is the cluster's; it is charged to the durable tiers.
            let (backlog_physical, growth, growth_recent) = if is_cache {
                (0.0, 0.0, 0.0)
            } else {
                (
                    backlog_logical as f64 * overhead,
                    growth_logical * overhead,
                    growth_recent_logical * overhead,
                )
            };
            let free: u64 = devices.iter().map(|d| d.available).sum();
            let trend = g.free_trend.entry(tier.clone()).or_default();
            trend.push(now_ms, free as f64);
            let free_change = trend.slope_over(now_ms, policy.trend_window_ms);
            let free_change_recent = trend.slope_over(now_ms, policy.recent_window_ms);
            // The recent trends count only once they cover their window
            // (both are sampled every pass, so one check stands for both).
            let recent_trend_known =
                policy.recent_window_ms > 0 && trend.covers(now_ms, policy.recent_window_ms);
            let inputs = Inputs {
                devices: devices.clone(),
                groups: tier_groups,
                max_shard_bytes: max_chunk_bytes / u64::from(min_k),
                gc_backlog_bytes: backlog_physical as u64,
                // Throttled writes may take in what GC gives back, as client
                // (logical) bytes.
                gc_reclaim_bytes_per_s: if is_cache { 0.0 } else { reclaim / overhead },
                gc_backlog_growth_bytes_per_s: growth,
                unsafe_nodes: if policy.allow_shared_filesystem {
                    Vec::new()
                } else {
                    // Unverified domains count as shared (R-80: a file
                    // system that cannot be checked is not assumed
                    // unshared).
                    let mut n = domains.conflicts.clone();
                    n.extend(&domains.unverified);
                    n.sort_unstable();
                    n.dedup();
                    n
                },
                free_change_bytes_per_s: free_change,
                free_change_recent_bytes_per_s: free_change_recent,
                gc_backlog_growth_recent_bytes_per_s: growth_recent,
                recent_trend_known,
                overhead,
                now_ms,
            };
            // Warm-up: every member heard in this term, or the staleness
            // bound passed (then missing reports count as stale).
            let heard_all = devices
                .iter()
                .all(|d| d.sampled_ms >= g.term_started_ms && d.sampled_ms > 0);
            let warming = !heard_all && now_ms.saturating_sub(g.term_started_ms) < policy.stale_ms;
            // A tier the published record does not name (or no record):
            // `throttled` until every member has reported.
            let floor = warming.then(|| g.floors.get(&tier).copied().unwrap_or(State::Throttled));
            let start = g.floors.get(&tier).copied().unwrap_or_default();
            let machine = g
                .machines
                .entry(tier.clone())
                .or_insert_with(|| Machine::starting_at(start));
            let mut decision = machine.step(policy, &inputs);
            if let Some(floor) = floor
                && decision.state < floor
            {
                *machine = Machine::starting_at(floor);
                decision.state = floor;
                decision.reason = format!(
                    "{} (new leader: not every member has reported yet; {})",
                    floor.name(),
                    decision.reason
                );
            }
            if warming {
                record.warming_up = true;
                if let Some(ex) = g.floor_excluded.get(&tier) {
                    decision.excluded_groups.extend(ex);
                    decision.excluded_groups.sort_unstable();
                    decision.excluded_groups.dedup();
                }
            }
            if !is_cache {
                // A device serves several tiers' groups: counted once.
                for d in devices.iter().filter(|d| counted.insert(d.node_id)) {
                    totals.total += d.total;
                    totals.usable += d.usable();
                    totals.headroom += d.headroom();
                    totals.reserved += d.reserved;
                    totals.reclaimable += d.reclaimable;
                    totals.inflight += d.inflight;
                }
                totals.gc_backlog_physical =
                    totals.gc_backlog_physical.max(backlog_physical as u64);
                totals.excluded_groups += decision.excluded_groups.len() as u64;
            }
            record.tiers.push(TierAdmission {
                tier: tier.clone(),
                state: decision.state.code() as u8,
                rate_bytes_per_s: decision.rate_bytes_per_s as u64,
                excluded_groups: decision.excluded_groups.clone(),
                free_permille: (decision.free_fraction * 1000.0) as u32,
                reason: decision.reason.clone(),
                group_room: decision
                    .group_room
                    .iter()
                    .map(
                        |&(group, room)| arca_lake_metadata::ops::admission::GroupRoom {
                            group,
                            room,
                        },
                    )
                    .collect(),
            });
            let changed = g
                .decisions
                .get(&tier)
                .is_none_or(|old| old.state != decision.state);
            if changed {
                tracing::warn!(
                    tier = %tier,
                    state = decision.state.name(),
                    reason = %decision.reason,
                    "write admission state changed"
                );
            }
            g.decisions.insert(tier, decision);
        }
        totals.gc_reclaim_per_s = reclaim as u64;
        record.generation = g.generation;
        g.generation += 1;
    }
    admission::publish(meta, &record).await?;
    Ok(totals)
}

fn domain_hash(id: &str) -> u64 {
    use std::hash::{Hash as _, Hasher as _};
    if id.is_empty() {
        return 0;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    h.finish().max(1)
}

/// Capacity domains checked (Phase 3.7, R-80).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DomainCheck {
    /// Nodes sharing a file system whose allotments do not prove the
    /// sharing safe (not every one allotted, or the allotments exceed the
    /// file system).
    pub conflicts: Vec<u64>,
    /// Nodes whose file system identity does not reveal sharing (overlay,
    /// FUSE, network, or not reported) and that have no allotment.
    pub unverified: Vec<u64>,
}

/// Check the capacity domains of `devices`.
#[must_use]
pub fn check_domains(devices: &[Device]) -> DomainCheck {
    let mut by_domain: BTreeMap<u64, Vec<&Device>> = BTreeMap::new();
    let mut out = DomainCheck::default();
    for d in devices {
        if d.domain == 0 || !d.domain_certain {
            if !d.allotted {
                out.unverified.push(d.node_id);
            }
            if d.domain == 0 {
                continue;
            }
        }
        by_domain.entry(d.domain).or_default().push(d);
    }
    for nodes in by_domain.values() {
        if nodes.len() < 2 {
            continue;
        }
        let fs_total = nodes.iter().map(|d| d.fs_total).max().unwrap_or(0);
        let allotted: u64 = nodes.iter().map(|d| d.total).sum();
        let safe = nodes.iter().all(|d| d.allotted) && fs_total > 0 && allotted <= fs_total;
        if !safe {
            out.conflicts.extend(nodes.iter().map(|d| d.node_id));
        }
    }
    out.conflicts.sort_unstable();
    out.conflicts.dedup();
    out.unverified.sort_unstable();
    out
}

/// The device a health response received at `now_ms` describes (older
/// nodes report only the file system's free and total bytes: no reserve).
#[must_use]
pub fn device_of(
    node_id: u64,
    h: &arca_lake_protocol::storage::v1::HealthResponse,
    now_ms: u64,
) -> Device {
    match h.devices.first() {
        Some(d) => Device {
            node_id,
            total: d.total_bytes,
            available: d.available_bytes,
            reserved: d.reserved_bytes,
            inflight: d.inflight_bytes,
            reclaimable: d.reclaimable_bytes,
            // Received now: staleness is judged by the controller's clock,
            // not the node's.
            sampled_ms: now_ms,
            domain: domain_hash(&d.domain_id),
            domain_certain: d.domain_certain,
            allotted: d.allotted,
            fs_total: d.fs_total_bytes,
        },
        None => Device {
            node_id,
            total: h.device_total_bytes,
            available: h.device_free_bytes,
            reserved: 0,
            inflight: 0,
            reclaimable: h.segment_bytes.saturating_sub(h.live_bytes),
            sampled_ms: if h.device_total_bytes > 0 { now_ms } else { 0 },
            ..Device::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn a_trend_needs_the_whole_window_and_ignores_a_short_burst() {
        let mut t = Trend::default();
        // A burst: 1 GiB in 10 s, then flat.
        assert_eq!(t.push(0, 10e9), 0.0);
        assert_eq!(t.push(10_000, 9e9), 0.0, "window not covered");
        for s in (12..=300).step_by(2) {
            t.push(s * 1000, 9e9);
        }
        let r = t.push(TREND_WINDOW_MS + 10_000, 9e9);
        assert!(r.abs() < 1e9 / 300.0, "{r}");
        // A sustained fall: measured over the window.
        let mut t = Trend::default();
        let mut r = 0.0;
        for s in (0..=600).step_by(2) {
            r = t.push(s * 1000, 10e9 - 1e6 * s as f64);
        }
        assert!((r + 1e6).abs() < 1.0, "{r}");
        for s in (602..=2000).step_by(2) {
            t.push(s * 1000, 0.0);
        }
        assert!(
            t.samples.len() <= usize::try_from(TREND_RETENTION_MS / 2000).unwrap() + 2,
            "samples are pruned"
        );
        // A recent window sees a change the long one does not yet.
        let mut t = Trend::default();
        for s in (0..=300).step_by(2) {
            t.push(s * 1000, if s < 250 { 0.0 } else { 6e9 });
        }
        assert!(t.slope_over(300_000, 60_000) > 0.0);
        assert_eq!(t.slope_over(300_000, 600_000), 0.0, "window not covered");
    }

    fn dev(node: u64, domain: u64, certain: bool, allot: Option<u64>, fs: u64) -> Device {
        Device {
            node_id: node,
            total: allot.unwrap_or(fs),
            domain,
            domain_certain: certain,
            allotted: allot.is_some(),
            fs_total: fs,
            ..Device::default()
        }
    }

    #[test]
    fn shared_file_systems_need_allotments_that_fit() {
        const G: u64 = 1 << 30;
        // Separate file systems: nothing to report.
        let c = check_domains(&[
            dev(1, 11, true, None, 100 * G),
            dev(2, 12, true, None, 100 * G),
        ]);
        assert_eq!(c, DomainCheck::default());
        // One file system, no allotments: both in conflict.
        let c = check_domains(&[
            dev(1, 11, true, None, 100 * G),
            dev(2, 11, true, None, 100 * G),
        ]);
        assert_eq!(c.conflicts, vec![1, 2]);
        // Allotments that fit: safe; that do not: conflict.
        let ok = [
            dev(1, 11, true, Some(40 * G), 100 * G),
            dev(2, 11, true, Some(60 * G), 100 * G),
        ];
        assert_eq!(check_domains(&ok).conflicts, Vec::<u64>::new());
        let over = [
            dev(1, 11, true, Some(60 * G), 100 * G),
            dev(2, 11, true, Some(60 * G), 100 * G),
        ];
        assert_eq!(check_domains(&over).conflicts, vec![1, 2]);
        // Identities that cannot show sharing (overlay, network, older
        // nodes) without allotments: unverified, never assumed safe.
        let c = check_domains(&[dev(1, 11, false, None, G), dev(2, 0, false, None, 0)]);
        assert_eq!(c.unverified, vec![1, 2]);
    }
}
