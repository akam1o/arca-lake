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
/// least this long, so that a burst of a few seconds does not throttle.
pub const TREND_WINDOW_MS: u64 = 5 * 60_000;

/// Samples of one quantity for a windowed slope: the newest sample at
/// least [`TREND_WINDOW_MS`] old and everything after it.
#[derive(Debug, Default)]
struct Trend {
    samples: std::collections::VecDeque<(u64, f64)>,
}

impl Trend {
    /// Add `value` at `now_ms`; returns the change per second over the
    /// window, 0 until the window is covered.
    fn push(&mut self, now_ms: u64, value: f64) -> f64 {
        self.samples.push_back((now_ms, value));
        while self.samples.len() > 2 && now_ms.saturating_sub(self.samples[1].0) >= TREND_WINDOW_MS
        {
            self.samples.pop_front();
        }
        self.slope(now_ms)
    }

    #[allow(clippy::cast_precision_loss)]
    fn slope(&self, now_ms: u64) -> f64 {
        match (self.samples.front(), self.samples.back()) {
            (Some(&(t0, v0)), Some(&(t1, v1)))
                if now_ms.saturating_sub(t0) >= TREND_WINDOW_MS && t1 > t0 =>
            {
                (v1 - v0) / ((t1 - t0) as f64 / 1000.0)
            }
            _ => 0.0,
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
}

/// A tier's kind (cache or not), writable groups, and the sums of their
/// data and total shards.
type TierGroups = (bool, Vec<Group>, (u64, u64));

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
    let groups = cluster::list_groups(meta).await?;
    let gateways = compat::procs(meta)
        .await?
        .iter()
        .filter(|p| p.kind == ProcKind::Gateway && now_ms.saturating_sub(p.last_ms) <= 60_000)
        .count();
    let mut totals = Totals::default();
    let mut record = AdmissionRecord {
        computed_ms: now_ms,
        ttl_ms: RECORD_TTL_MS,
        gateways: u32::try_from(gateways.max(1)).unwrap_or(u32::MAX),
        tiers: Vec::new(),
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
    {
        let mut g = tracker.lock();
        let (backlog_logical, growth_logical, reclaim) = (
            g.backlog.map_or(0, |b| b.1),
            g.backlog_trend.slope(now_ms),
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
            let (backlog_physical, growth) = if is_cache {
                (0.0, 0.0)
            } else {
                (backlog_logical as f64 * overhead, growth_logical * overhead)
            };
            let free: u64 = devices.iter().map(|d| d.available).sum();
            let free_change = g
                .free_trend
                .entry(tier.clone())
                .or_default()
                .push(now_ms, free as f64);
            let inputs = Inputs {
                devices: devices.clone(),
                groups: tier_groups,
                max_shard_bytes: max_chunk_bytes / u64::from(min_k),
                gc_backlog_bytes: backlog_physical as u64,
                // Throttled writes may take in what GC gives back, as client
                // (logical) bytes.
                gc_reclaim_bytes_per_s: if is_cache { 0.0 } else { reclaim / overhead },
                gc_backlog_growth_bytes_per_s: growth,
                free_change_bytes_per_s: free_change,
                now_ms,
            };
            let decision = g
                .machines
                .entry(tier.clone())
                .or_default()
                .step(policy, &inputs);
            if !is_cache {
                for d in &devices {
                    totals.total += d.total;
                    totals.usable += d.usable();
                    totals.headroom += d.headroom();
                    totals.reserved += d.reserved;
                    totals.reclaimable += d.reclaimable;
                    totals.inflight += d.inflight;
                }
                totals.gc_backlog_physical += backlog_physical as u64;
                totals.excluded_groups += decision.excluded_groups.len() as u64;
            }
            record.tiers.push(TierAdmission {
                tier: tier.clone(),
                state: decision.state.code() as u8,
                rate_bytes_per_s: decision.rate_bytes_per_s as u64,
                excluded_groups: decision.excluded_groups.clone(),
                free_permille: (decision.free_fraction * 1000.0) as u32,
                reason: decision.reason.clone(),
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
    }
    admission::publish(meta, &record).await?;
    Ok(totals)
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
        },
        None => Device {
            node_id,
            total: h.device_total_bytes,
            available: h.device_free_bytes,
            reserved: 0,
            inflight: 0,
            reclaimable: h.segment_bytes.saturating_sub(h.live_bytes),
            sampled_ms: if h.device_total_bytes > 0 { now_ms } else { 0 },
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
        assert!(t.samples.len() < 200, "samples are pruned");
    }
}
