//! Write admission state of a tier (Phase 3.6, R-64): from the devices'
//! capacity, the groups' room and the GC backlog, how much new client data
//! the tier may take. The controller leader computes it every health pass
//! and publishes it ([`arca_lake_metadata::ops::admission`]); gateways
//! enforce it. The storage nodes' own admission (a reserve client writes
//! never cross) holds whatever this state says: this is the early, soft
//! part that keeps writes from running into the hard one.
//!
//! States, worst first: `RecoveryOnly` (the reserve itself is at risk, or
//! no capacity is known: only deletes, repair and GC), `WriteRejected` (new
//! client data refused; deletes and multipart completion, which add no
//! data, still accepted), `Throttled` (client data limited to the rate GC
//! reclaims), `Warning`, `Normal`. A worse state is entered at once; a
//! better one only after its conditions held, with a margin, for the dwell
//! time (hysteresis).

/// Admission state, ordered from best to worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum State {
    /// Writes admitted.
    #[default]
    Normal,
    /// Writes admitted; capacity or GC needs attention.
    Warning,
    /// Client data limited to a rate.
    Throttled,
    /// New client data refused.
    WriteRejected,
    /// Only deletes, repair and GC.
    RecoveryOnly,
}

impl State {
    /// Gauge value (`arca_controller_backpressure_state`).
    #[must_use]
    pub fn code(self) -> u64 {
        self as u64
    }

    /// Name for logs and the admission record.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Warning => "warning",
            Self::Throttled => "throttled",
            Self::WriteRejected => "write_rejected",
            Self::RecoveryOnly => "recovery_only",
        }
    }
}

/// Capacity of one device as the controller last heard it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Device {
    /// Node.
    pub node_id: u64,
    /// File system size the node may use.
    pub total: u64,
    /// Free bytes now.
    pub available: u64,
    /// Kept free for compaction, rollover and repair.
    pub reserved: u64,
    /// Admitted writes not yet stored.
    pub inflight: u64,
    /// Bytes compaction can reclaim (non-live records in segments).
    pub reclaimable: u64,
    /// When the node sampled them (unix ms); 0 = never heard.
    pub sampled_ms: u64,
}

impl Device {
    /// Bytes client writes may still use.
    #[must_use]
    pub fn headroom(&self) -> u64 {
        self.available
            .saturating_sub(self.inflight)
            .saturating_sub(self.reserved)
    }

    /// Bytes client writes may use when empty (total minus reserve).
    #[must_use]
    pub fn usable(&self) -> u64 {
        self.total.saturating_sub(self.reserved)
    }
}

/// A stripe group that takes client writes, and its members' devices.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Group {
    /// Group id.
    pub id: u64,
    /// Data shards (`k`): a chunk of `n` bytes puts `n / k` on every member.
    pub data_shards: u16,
    /// Member nodes.
    pub members: Vec<u64>,
}

/// Inputs of one evaluation of a tier.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    /// The tier's devices.
    pub devices: Vec<Device>,
    /// The tier's writable groups.
    pub groups: Vec<Group>,
    /// Largest shard a client write puts on one member (chunk size / k),
    /// so that a group with less room than a few of them is not used.
    pub max_shard_bytes: u64,
    /// GC backlog of the cluster, physical bytes (estimated).
    pub gc_backlog_bytes: u64,
    /// Physical bytes GC reclaimed per second recently.
    pub gc_reclaim_bytes_per_s: f64,
    /// Change of the backlog per second recently (positive = growing).
    pub gc_backlog_growth_bytes_per_s: f64,
    /// Change of the tier's free bytes per second recently (negative =
    /// filling).
    pub free_change_bytes_per_s: f64,
    /// Now, unix ms.
    pub now_ms: u64,
}

/// Thresholds. Fractions are of the tier's usable bytes (total minus the
/// devices' reserves) still free for client writes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Policy {
    /// Below: `Warning`.
    pub warning: f64,
    /// Below: `Throttled`.
    pub throttled: f64,
    /// Below: `WriteRejected`.
    pub rejected: f64,
    /// Below: `RecoveryOnly` (the reserve is about to be needed).
    pub recovery: f64,
    /// A better state needs its fraction this much above the threshold.
    pub hysteresis: f64,
    /// and to hold for this long, ms.
    pub dwell_ms: u64,
    /// A device report older than this is not trusted, ms.
    pub stale_ms: u64,
    /// Throttle when the tier fills (or the backlog grows) so fast that it
    /// is full within this many seconds; warn at four times it.
    pub horizon_s: f64,
    /// A group whose fullest member has room for fewer than this many of
    /// the largest shards is not used for new writes.
    pub group_min_shards: u64,
    /// Throttled rate never below this (logical bytes/s, the tier), so that
    /// clients make progress.
    pub min_rate_bytes_per_s: f64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            warning: 0.20,
            throttled: 0.10,
            rejected: 0.05,
            recovery: 0.02,
            hysteresis: 0.03,
            dwell_ms: 60_000,
            stale_ms: 30_000,
            horizon_s: 3600.0,
            group_min_shards: 64,
            min_rate_bytes_per_s: f64::from(1u32 << 20),
        }
    }
}

/// What a tier admits.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Decision {
    /// State.
    pub state: State,
    /// In `Throttled`: client data the tier takes per second (logical
    /// bytes, split over the gateways by the gateways).
    pub rate_bytes_per_s: f64,
    /// Groups not to place new chunks in (a member lacks room).
    pub excluded_groups: Vec<u64>,
    /// Usable fraction left (0..=1).
    pub free_fraction: f64,
    /// Why (for logs and the record).
    pub reason: String,
}

/// The state machine of one tier.
#[derive(Debug, Clone, Default)]
pub struct Machine {
    state: State,
    /// Since when every condition of a better state has held.
    better_since_ms: Option<u64>,
}

impl Machine {
    /// Current state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// Evaluate `inputs` and move the state.
    pub fn step(&mut self, policy: &Policy, inputs: &Inputs) -> Decision {
        let mut d = self.step_inner(policy, inputs);
        // The rate belongs to the state applied, which may be held or
        // stepped to while the inputs alone call for another one.
        d.rate_bytes_per_s = if d.state == State::Throttled {
            throttle_rate(policy, inputs)
        } else {
            0.0
        };
        d
    }

    fn step_inner(&mut self, policy: &Policy, inputs: &Inputs) -> Decision {
        let raw = evaluate(policy, inputs, 0.0);
        if raw.state >= self.state {
            // Worse or the same: at once.
            self.state = raw.state;
            self.better_since_ms = None;
            return raw;
        }
        // Better: only with the margin, and after the dwell time.
        let with_margin = evaluate(policy, inputs, policy.hysteresis);
        if with_margin.state >= self.state {
            self.better_since_ms = None;
            return Decision {
                state: self.state,
                reason: format!("{} (holding; {})", self.state.name(), raw.reason),
                ..raw
            };
        }
        let since = *self.better_since_ms.get_or_insert(inputs.now_ms);
        if inputs.now_ms.saturating_sub(since) < policy.dwell_ms {
            return Decision {
                state: self.state,
                reason: format!(
                    "{} (better for {} ms of {}; {})",
                    self.state.name(),
                    inputs.now_ms.saturating_sub(since),
                    policy.dwell_ms,
                    raw.reason
                ),
                ..raw
            };
        }
        // One step at a time, so that a recovery ramps writes up.
        let next = match self.state {
            State::RecoveryOnly => State::WriteRejected,
            State::WriteRejected => State::Throttled,
            State::Throttled => State::Warning,
            State::Warning | State::Normal => State::Normal,
        }
        .max(with_margin.state);
        self.state = next;
        self.better_since_ms = None;
        Decision {
            state: next,
            reason: format!("{} (recovered; {})", next.name(), raw.reason),
            ..raw
        }
    }
}

/// The state `inputs` call for, with thresholds raised by `margin`.
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn evaluate(policy: &Policy, inputs: &Inputs, margin: f64) -> Decision {
    let fresh: Vec<&Device> = inputs
        .devices
        .iter()
        .filter(|d| {
            d.sampled_ms > 0 && inputs.now_ms.saturating_sub(d.sampled_ms) <= policy.stale_ms
        })
        .collect();
    let mut out = Decision::default();
    if fresh.is_empty() {
        out.state = State::RecoveryOnly;
        out.reason = "no device of the tier has reported its capacity recently".into();
        return out;
    }
    let usable: u64 = fresh.iter().map(|d| d.usable()).sum();
    let headroom: u64 = fresh.iter().map(|d| d.headroom()).sum();
    let fraction = if usable == 0 {
        0.0
    } else {
        headroom as f64 / usable as f64
    };
    out.free_fraction = fraction;
    // Groups whose fullest member lacks room. A member not heard from
    // recently is no evidence of a full device (a node that is down makes
    // its groups fail writes on their own, a placement matter): only
    // members with a fresh report count.
    let min_room = inputs
        .max_shard_bytes
        .saturating_mul(policy.group_min_shards);
    let by_node = |id: u64| fresh.iter().find(|d| d.node_id == id).copied();
    let mut writable = 0;
    for g in &inputs.groups {
        let room = g
            .members
            .iter()
            .filter_map(|&n| by_node(n).map(Device::headroom))
            .min()
            // No member heard from at all: nothing known, not used.
            .unwrap_or(0);
        if room < min_room {
            out.excluded_groups.push(g.id);
        } else {
            writable += 1;
        }
    }
    // Seconds until the free bytes run out at the recent rate: the tier's
    // own filling, or the GC backlog growing.
    let draining = (-inputs.free_change_bytes_per_s).max(inputs.gc_backlog_growth_bytes_per_s);
    let tte = if draining > 0.0 {
        headroom as f64 / draining
    } else {
        f64::INFINITY
    };
    let stale_devices = inputs.devices.len() - fresh.len();
    let pct = |f: f64| f * 100.0;
    let (state, reason) = if fraction < policy.recovery + margin {
        (
            State::RecoveryOnly,
            format!(
                "{:.1} % usable free, below {:.1} %",
                pct(fraction),
                pct(policy.recovery + margin)
            ),
        )
    } else if fraction < policy.rejected + margin || (writable == 0 && !inputs.groups.is_empty()) {
        (
            State::WriteRejected,
            if writable == 0 && !inputs.groups.is_empty() {
                format!("no group has room ({} excluded)", out.excluded_groups.len())
            } else {
                format!(
                    "{:.1} % usable free, below {:.1} %",
                    pct(fraction),
                    pct(policy.rejected + margin)
                )
            },
        )
    } else if fraction < policy.throttled + margin || tte < policy.horizon_s * (1.0 + margin) {
        (
            State::Throttled,
            if tte < policy.horizon_s * (1.0 + margin) {
                format!("full in {tte:.0} s at the recent rate")
            } else {
                format!(
                    "{:.1} % usable free, below {:.1} %",
                    pct(fraction),
                    pct(policy.throttled + margin)
                )
            },
        )
    } else if fraction < policy.warning + margin
        || tte < 4.0 * policy.horizon_s * (1.0 + margin)
        || stale_devices > 0
    {
        (
            State::Warning,
            if stale_devices > 0 {
                format!("{stale_devices} device report(s) stale")
            } else if tte < 4.0 * policy.horizon_s * (1.0 + margin) {
                format!("full in {tte:.0} s at the recent rate")
            } else {
                format!("{:.1} % usable free", pct(fraction))
            },
        )
    } else {
        (State::Normal, format!("{:.1} % usable free", pct(fraction)))
    };
    out.state = state;
    out.reason = reason;
    if state == State::Throttled {
        out.rate_bytes_per_s = throttle_rate(policy, inputs);
    }
    out
}

/// Throttled: take in what GC gives back (so that the backlog stops
/// growing), never less than the floor.
fn throttle_rate(policy: &Policy, inputs: &Inputs) -> f64 {
    inputs
        .gc_reclaim_bytes_per_s
        .max(policy.min_rate_bytes_per_s)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn tier(free_gib: u64) -> Inputs {
        let devices = (1..=4)
            .map(|n| Device {
                node_id: n,
                total: 100 * GIB,
                available: free_gib * GIB,
                reserved: 4 * GIB,
                inflight: 0,
                reclaimable: 0,
                sampled_ms: 1_000_000,
            })
            .collect();
        Inputs {
            devices,
            groups: vec![Group {
                id: 1,
                data_shards: 2,
                members: vec![1, 2, 3],
            }],
            max_shard_bytes: 512 << 10,
            now_ms: 1_000_000,
            ..Inputs::default()
        }
    }

    #[test]
    fn states_follow_the_usable_free_fraction() {
        let p = Policy::default();
        // usable = 96 GiB per device; headroom = free - 4.
        for (free, want) in [
            (60, State::Normal),
            (20, State::Warning),
            (12, State::Throttled),
            (8, State::WriteRejected),
            (5, State::RecoveryOnly),
        ] {
            let mut m = Machine::default();
            assert_eq!(m.step(&p, &tier(free)).state, want, "{free} GiB free");
        }
    }

    /// `tier(free)` reported at `now`.
    fn at(free_gib: u64, now: u64) -> Inputs {
        let mut i = tier(free_gib);
        i.now_ms = now;
        for d in &mut i.devices {
            d.sampled_ms = now;
        }
        i
    }

    #[test]
    fn worse_at_once_better_only_after_margin_and_dwell_one_step_at_a_time() {
        let p = Policy::default();
        let mut m = Machine::default();
        let mut now = 1_000_000;
        assert_eq!(m.step(&p, &at(8, now)).state, State::WriteRejected);
        // Just above the threshold: no margin, stays.
        now += 1;
        assert_eq!(m.step(&p, &at(10, now)).state, State::WriteRejected);
        // Well above: still the dwell time.
        now += 10_000;
        assert_eq!(m.step(&p, &at(60, now)).state, State::WriteRejected);
        now += p.dwell_ms;
        let d = m.step(&p, &at(60, now));
        assert_eq!(d.state, State::Throttled, "one step");
        assert!(
            d.rate_bytes_per_s >= p.min_rate_bytes_per_s,
            "a throttle entered on the way back has a rate: {d:?}"
        );
        now += 1;
        assert_eq!(
            m.step(&p, &at(60, now)).state,
            State::Throttled,
            "dwell again"
        );
        now += p.dwell_ms;
        assert_eq!(m.step(&p, &at(60, now)).state, State::Warning);
        now += p.dwell_ms;
        assert_eq!(
            m.step(&p, &at(60, now)).state,
            State::Warning,
            "dwell again"
        );
        now += p.dwell_ms;
        assert_eq!(m.step(&p, &at(60, now)).state, State::Normal);
        // Worse again: at once, however far.
        now += 1;
        assert_eq!(m.step(&p, &at(5, now)).state, State::RecoveryOnly);
    }

    #[test]
    fn a_growing_backlog_throttles_to_the_reclaim_rate_before_the_space_is_short() {
        let p = Policy::default();
        let mut i = tier(60);
        // 224 GiB headroom, backlog growing 100 MiB/s: full in ~38 min.
        i.gc_backlog_growth_bytes_per_s = 100.0 * f64::from(1u32 << 20);
        i.gc_reclaim_bytes_per_s = 30.0 * f64::from(1u32 << 20);
        let d = Machine::default().step(&p, &i);
        assert_eq!(d.state, State::Throttled, "{}", d.reason);
        assert!((d.rate_bytes_per_s - 30.0 * f64::from(1u32 << 20)).abs() < 1.0);
        // Slow GC never throttles below the floor.
        i.gc_reclaim_bytes_per_s = 0.0;
        assert_eq!(
            Machine::default().step(&p, &i).rate_bytes_per_s,
            p.min_rate_bytes_per_s
        );
    }

    #[test]
    fn unknown_or_stale_capacity_fails_closed() {
        let p = Policy::default();
        let mut i = tier(60);
        i.now_ms += p.stale_ms + 1;
        assert_eq!(Machine::default().step(&p, &i).state, State::RecoveryOnly);
        let mut i = tier(60);
        i.devices[3].sampled_ms = 0;
        assert_eq!(Machine::default().step(&p, &i).state, State::Warning);
    }

    #[test]
    fn a_full_member_takes_its_groups_out_of_placement() {
        let p = Policy::default();
        let mut i = tier(60);
        i.groups.push(Group {
            id: 2,
            data_shards: 2,
            members: vec![2, 3, 4],
        });
        // A member not heard from: no capacity evidence, not excluded.
        let mut j = i.clone();
        j.devices[3].sampled_ms = 0;
        assert_eq!(
            Machine::default().step(&p, &j).excluded_groups,
            Vec::<u64>::new()
        );
        // Node 4 at its reserve: group 2 excluded, group 1 still writable.
        i.devices[3].available = 4 * GIB;
        let d = Machine::default().step(&p, &i);
        assert_eq!(d.excluded_groups, vec![2]);
        assert_ne!(d.state, State::WriteRejected);
        // Every group excluded: writes rejected.
        i.devices[0].available = 4 * GIB;
        let d = Machine::default().step(&p, &i);
        assert_eq!(d.excluded_groups, vec![1, 2]);
        assert_eq!(d.state, State::WriteRejected, "{}", d.reason);
    }
}
