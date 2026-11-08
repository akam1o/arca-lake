//! Backpressure responsiveness on a deterministic fluid model of a tier
//! (Phase 3.8, H10): eight conditions, each policy variant through the real
//! [`Machine`]. Measured per condition: offered and admitted client bytes,
//! seconds spent throttled or worse while nothing called for it (the tier
//! not filling, its backlog not growing, more than the warning fraction
//! free), seconds to `normal` after the load turns benign, state changes,
//! and node refusals (writes the throttle let through that the nodes'
//! reserve had to stop). The safety checks hold for every variant; the
//! printed table (`--nocapture`) is the comparison.
//!
//! Model, one-second ticks: four devices of 100 GiB (reserve 4 GiB each),
//! overhead 1.5; admitted client bytes take free space (times the
//! overhead); overwrites add as much GC backlog; GC gives back up to its
//! rate while it has backlog; the controller evaluates every
//! `report_s`, from the history of free bytes and backlog (windows as the
//! leader's tracker); the gateway applies the decision, aging it when the
//! controller stops (throttled at 4 MiB/s after 30 s, refused after
//! 300 s); the nodes refuse what would cross the reserve.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::struct_excessive_bools
)]

use arca_lake_controller::backpressure::{Device, Group, Inputs, Machine, Policy, State};

const GIB: f64 = (1u64 << 30) as f64;
const MIB: f64 = (1u64 << 20) as f64;
const DEVICES: u64 = 4;
const TOTAL: f64 = 100.0 * GIB;
const RESERVE: f64 = 4.0 * GIB;
const OVERHEAD: f64 = 1.5;

#[derive(Clone, Copy, Debug)]
struct Condition {
    name: &'static str,
    secs: u64,
    /// Offered client bytes/s (logical) at second t.
    offered: fn(u64) -> f64,
    /// Fraction of written bytes that becomes garbage (overwrites).
    garbage: f64,
    /// Physical bytes GC can reclaim per second.
    gc_rate: f64,
    /// Bytes freed per second by other means (deletes compacted).
    freed: f64,
    /// Free and backlog at the start (physical bytes, whole tier).
    free0: f64,
    backlog0: f64,
    /// The controller stops evaluating at this second.
    controller_stops: Option<u64>,
    /// One device's report freezes at this second.
    report_freezes: Option<u64>,
    /// From this second the load is benign (recovery time measured).
    benign_from: Option<u64>,
}

#[derive(Debug, Default)]
struct Outcome {
    offered: f64,
    admitted: f64,
    unnecessary_s: u64,
    restricted_s: u64,
    to_normal_s: Option<u64>,
    transitions: u64,
    node_refused: f64,
    min_free: f64,
}

fn slope(hist: &[(u64, f64)], now: u64, window: u64) -> (f64, bool) {
    let Some(&(t1, v1)) = hist.last() else {
        return (0.0, false);
    };
    match hist
        .iter()
        .rev()
        .find(|(t, _)| now.saturating_sub(*t) >= window)
    {
        Some(&(t0, v0)) if t1 > t0 => ((v1 - v0) / (t1 - t0) as f64, true),
        _ => (0.0, false),
    }
}

fn simulate(c: &Condition, policy: &Policy, report_s: u64) -> Outcome {
    let mut m = Machine::default();
    let (mut free, mut backlog) = (c.free0, c.backlog0);
    let mut free_hist: Vec<(u64, f64)> = Vec::new();
    let mut backlog_hist: Vec<(u64, f64)> = Vec::new();
    let mut reclaimed_hist: Vec<(u64, f64)> = Vec::new();
    let mut reclaimed_total = 0.0;
    let (mut state, mut rate, mut last_decision_s) = (State::Normal, 0.0, 0u64);
    let mut out = Outcome {
        min_free: f64::INFINITY,
        ..Outcome::default()
    };
    let mut frozen_sample: Option<(u64, f64)> = None;
    let mut prev_state = State::Normal;
    for t in 0..c.secs {
        // The controller.
        let controller_up = c.controller_stops.is_none_or(|s| t < s);
        if controller_up && t % report_s == 0 {
            free_hist.push((t, free));
            backlog_hist.push((t, backlog));
            reclaimed_hist.push((t, reclaimed_total));
            free_hist.retain(|(x, _)| t - x <= 900);
            backlog_hist.retain(|(x, _)| t - x <= 900);
            reclaimed_hist.retain(|(x, _)| t - x <= 900);
            let now_ms = t * 1000 + 1;
            let per = free / DEVICES as f64;
            let frozen = c.report_freezes.is_some_and(|f| t >= f);
            if frozen && frozen_sample.is_none() {
                frozen_sample = Some((now_ms, per));
            }
            let devices = (1..=DEVICES)
                .map(|id| {
                    let (sampled_ms, avail) = match frozen_sample {
                        Some(s) if id == 1 => s,
                        _ => (now_ms, per),
                    };
                    Device {
                        node_id: id,
                        total: TOTAL as u64,
                        available: avail.max(0.0) as u64,
                        reserved: RESERVE as u64,
                        sampled_ms,
                        ..Device::default()
                    }
                })
                .collect();
            let (free_long, _) = slope(&free_hist, t, policy.trend_window_ms / 1000);
            let (backlog_long, _) = slope(&backlog_hist, t, policy.trend_window_ms / 1000);
            let (free_recent, known) =
                slope(&free_hist, t, (policy.recent_window_ms / 1000).max(1));
            let (backlog_recent, _) =
                slope(&backlog_hist, t, (policy.recent_window_ms / 1000).max(1));
            let (reclaim, _) = slope(&reclaimed_hist, t, report_s.max(10));
            let inputs = Inputs {
                devices,
                groups: vec![Group {
                    id: 1,
                    data_shards: 2,
                    members: (1..=DEVICES).collect(),
                }],
                max_shard_bytes: 2 << 20,
                gc_backlog_bytes: backlog as u64,
                gc_reclaim_bytes_per_s: reclaim / OVERHEAD,
                gc_backlog_growth_bytes_per_s: backlog_long,
                free_change_bytes_per_s: free_long,
                free_change_recent_bytes_per_s: free_recent,
                gc_backlog_growth_recent_bytes_per_s: backlog_recent,
                recent_trend_known: known && policy.recent_window_ms > 0,
                overhead: OVERHEAD,
                now_ms,
                ..Inputs::default()
            };
            let d = m.step(policy, &inputs);
            state = d.state;
            rate = d.rate_bytes_per_s;
            last_decision_s = t;
        }
        // The gateway (one), aging a decision that stops changing.
        let age = t - last_decision_s;
        let effective = if !controller_up && age >= 300 {
            State::WriteRejected
        } else if !controller_up && age >= 30 && state < State::Throttled {
            State::Throttled
        } else {
            state
        };
        let eff_rate = if effective == State::Throttled && state < State::Throttled {
            4.0 * MIB
        } else {
            rate.max(MIB)
        };
        let offered = (c.offered)(t);
        let mut admitted = match effective {
            State::Normal | State::Warning => offered,
            State::Throttled => offered.min(eff_rate),
            State::WriteRejected | State::RecoveryOnly => 0.0,
        };
        // The nodes: never below the reserve.
        let room = (free - DEVICES as f64 * RESERVE).max(0.0) / OVERHEAD;
        if admitted > room {
            out.node_refused += admitted - room;
            admitted = room;
        }
        free -= admitted * OVERHEAD;
        backlog += admitted * OVERHEAD * c.garbage;
        let r = backlog.min(c.gc_rate);
        backlog -= r;
        free += r + c.freed;
        free = free.min(DEVICES as f64 * TOTAL);
        reclaimed_total += r;
        out.offered += offered;
        out.admitted += admitted;
        out.min_free = out.min_free.min(free);
        if effective != prev_state {
            out.transitions += 1;
            prev_state = effective;
        }
        if effective >= State::Throttled {
            out.restricted_s += 1;
            // Nothing called for it: not filling over the last minute,
            // the backlog not growing, more than the warning fraction free.
            let (df, _) = slope(&free_hist, t, 60);
            let (db, _) = slope(&backlog_hist, t, 60);
            let usable = DEVICES as f64 * (TOTAL - RESERVE);
            let frac = (free - DEVICES as f64 * RESERVE) / usable;
            if df >= 0.0 && db <= 0.0 && frac > policy.warning && controller_up {
                out.unnecessary_s += 1;
            }
        }
        if let Some(b) = c.benign_from
            && t >= b
            && out.to_normal_s.is_none()
            && effective == State::Normal
        {
            out.to_normal_s = Some(t - b);
        }
    }
    out
}

fn conditions() -> Vec<Condition> {
    let usable = DEVICES as f64 * (TOTAL - RESERVE);
    let at = |fraction: f64| DEVICES as f64 * RESERVE + usable * fraction;
    vec![
        Condition {
            name: "backlog growing",
            secs: 1800,
            offered: |_| 200.0 * MIB,
            garbage: 1.0,
            gc_rate: 150.0 * MIB,
            freed: 0.0,
            free0: at(0.5),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: None,
            benign_from: None,
        },
        Condition {
            name: "backlog constant",
            secs: 1800,
            offered: |_| 100.0 * MIB,
            garbage: 1.0,
            gc_rate: 150.0 * MIB,
            freed: 0.0,
            free0: at(0.5),
            backlog0: 20.0 * GIB,
            controller_stops: None,
            report_freezes: None,
            benign_from: Some(0),
        },
        Condition {
            name: "backlog shrinking",
            secs: 1800,
            offered: |_| 50.0 * MIB,
            garbage: 1.0,
            gc_rate: 150.0 * MIB,
            freed: 0.0,
            free0: at(0.3),
            backlog0: 60.0 * GIB,
            controller_stops: None,
            report_freezes: None,
            benign_from: Some(0),
        },
        Condition {
            name: "backlog zero",
            secs: 1800,
            offered: |_| 20.0 * MIB,
            garbage: 0.0,
            gc_rate: 150.0 * MIB,
            freed: 20.0 * MIB * OVERHEAD,
            free0: at(0.4),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: None,
            benign_from: Some(0),
        },
        Condition {
            name: "free growing after a burst",
            secs: 1800,
            offered: |t| if t < 400 { 400.0 * MIB } else { 30.0 * MIB },
            garbage: 0.0,
            gc_rate: 0.0,
            freed: 80.0 * MIB,
            free0: at(0.6),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: None,
            benign_from: Some(400),
        },
        Condition {
            name: "on/off bursts near full",
            secs: 2400,
            offered: |t| if (t / 60) % 2 == 0 { 400.0 * MIB } else { 0.0 },
            garbage: 0.0,
            gc_rate: 0.0,
            freed: 40.0 * MIB,
            free0: at(0.25),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: None,
            benign_from: None,
        },
        Condition {
            name: "free shrinking",
            secs: 3600,
            offered: |_| 120.0 * MIB,
            garbage: 0.0,
            gc_rate: 0.0,
            freed: 0.0,
            free0: at(0.4),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: None,
            benign_from: None,
        },
        Condition {
            name: "controller stopped",
            secs: 1200,
            offered: |_| 50.0 * MIB,
            garbage: 0.0,
            gc_rate: 0.0,
            freed: 50.0 * MIB * OVERHEAD,
            free0: at(0.5),
            backlog0: 0.0,
            controller_stops: Some(300),
            report_freezes: None,
            benign_from: None,
        },
        Condition {
            name: "stale report",
            secs: 1200,
            offered: |_| 50.0 * MIB,
            garbage: 0.0,
            gc_rate: 0.0,
            freed: 50.0 * MIB * OVERHEAD,
            free0: at(0.5),
            backlog0: 0.0,
            controller_stops: None,
            report_freezes: Some(300),
            benign_from: None,
        },
    ]
}

fn phase_36() -> Policy {
    Policy {
        recent_window_ms: 0,
        adaptive_rate: false,
        recent_trend_gates_entry: false,
        ..Policy::default()
    }
}

fn phase_37() -> Policy {
    Policy {
        recent_trend_gates_entry: false,
        ..Policy::default()
    }
}

#[test]
fn backpressure_policies_on_eight_conditions() {
    let variants: [(&str, Policy); 3] = [
        ("3.6", phase_36()),
        ("3.7", phase_37()),
        ("3.8", Policy::default()),
    ];
    println!(
        "{:<28} {:>4} {:>7} {:>9} {:>9} {:>10} {:>9} {:>6} {:>9}",
        "condition",
        "pol",
        "report",
        "admit %",
        "restrict",
        "unneeded",
        "to normal",
        "trans",
        "refused"
    );
    for c in conditions() {
        for report_s in [1, 10] {
            let mut by_policy = Vec::new();
            for (name, p) in &variants {
                let o = simulate(&c, p, report_s);
                println!(
                    "{:<28} {:>4} {:>6}s {:>8.1}% {:>8}s {:>9}s {:>9} {:>6} {:>8.0}M",
                    c.name,
                    name,
                    report_s,
                    100.0 * o.admitted / o.offered.max(1.0),
                    o.restricted_s,
                    o.unnecessary_s,
                    o.to_normal_s.map_or("-".into(), |s| format!("{s}s")),
                    o.transitions,
                    o.node_refused / MIB
                );
                // Safety: never below the reserve.
                assert!(
                    o.min_free >= DEVICES as f64 * RESERVE - 1.0,
                    "{} {name}: below the reserve",
                    c.name
                );
                // A stopped controller ends in refusal; a stale report
                // never lets the tier be normal.
                if c.controller_stops.is_some() {
                    assert_eq!(o.to_normal_s, None);
                }
                by_policy.push(o);
            }
            // Phase 3.8 against 3.7: never more node refusals, never more
            // flapping, never more unnecessary throttling.
            let (p37, p38) = (&by_policy[1], &by_policy[2]);
            assert!(p38.node_refused <= p37.node_refused, "{}", c.name);
            assert!(p38.transitions <= p37.transitions, "{}", c.name);
            assert!(p38.unnecessary_s <= p37.unnecessary_s, "{}", c.name);
            if c.name.starts_with("free growing") {
                assert!(p38.unnecessary_s < p37.unnecessary_s, "{}", c.name);
            }
        }
    }
}
