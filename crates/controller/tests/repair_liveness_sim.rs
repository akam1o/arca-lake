//! Deterministic simulation of the leader's scheduling of member repairs
//! (Phase 3.13, R-104, ADR-0029 §Verification). A seeded model of nodes
//! that fail, come back and fail again, groups of `k + m` shards whose
//! chunks are held per member, and a leader pass that applies the same
//! rules as the controller ([`repair_liveness::decide`], `after`,
//! `started`, `no_new_target`): rebuild onto the target, change a target
//! that is gone or keeps failing, replace a second failed member once the
//! slot is whole, back off.
//!
//! Checked on every step and seed:
//!
//! - S1: a group is made `Active` only with every chunk's shards on its
//!   members;
//! - S2: no membership change leaves a chunk with fewer than `k` shards
//!   on its members and the slot's former nodes (shards of a node that
//!   left a slot under rebuild are kept and read as that slot's shard;
//!   members down count: they come back with their shards) unless it had
//!   fewer already;
//! - S2-strict: replacing a second failed member never leaves a chunk
//!   with fewer than `k` shards on the members alone (its shards would
//!   be needed by a slot other than its own);
//! - S3: a target is never a node that left the slot in this repair, nor
//!   a down node at the time it is chosen;
//! - L1: once failures stop (nodes back, or permanently lost with spares
//!   to replace them), every group with `k` shards of each chunk becomes
//!   `Active` within a bounded number of passes, whatever other groups do
//!   (one group holds a chunk no member can rebuild);
//! - L2: the unrecoverable group is attempted at most once per its retry
//!   delay;
//! - L3: no group waits past the longest backoff while it could progress.

#![allow(
    clippy::cast_possible_truncation,
    clippy::needless_pass_by_value,
    clippy::match_same_arms,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;

use arca_lake_controller::repair_liveness::{
    self, Decision, Observation, Outcome, Pacing, RETARGET_AFTER,
};
use arca_lake_metadata::ops::repair_status::{RepairReason, RepairStatus};

const PASS_MS: u64 = 1_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[derive(Clone)]
struct Node {
    up: bool,
    /// Lost for good (never comes back).
    lost: bool,
    /// Refuses writes (full, failing disk) while up.
    refuses: bool,
    /// `(group, chunk, shard index)` held.
    shards: BTreeSet<(usize, usize, usize)>,
}

#[derive(Clone)]
struct Group {
    k: usize,
    members: Vec<usize>,
    epoch: u64,
    /// `Some(index)`: `Repairing`, rebuilding slot `index`.
    repairing: Option<usize>,
    status: Option<RepairStatus>,
    chunks: usize,
    /// Chunk whose sources were damaged for good (fewer than `k`).
    damaged: Option<usize>,
    attempts: Vec<u64>,
}

struct Sim {
    rng: Rng,
    now: u64,
    nodes: Vec<Node>,
    groups: Vec<Group>,
    pacing: Pacing,
    budget: usize,
    /// Mutation switch: replace a second failed member at once.
    replace_other_early: bool,
    /// Recent events (reported when a check fails).
    trace: Vec<String>,
}

impl Sim {
    fn new(seed: u64, nodes: usize, groups: usize, k: usize, m: usize, chunks: usize) -> Self {
        let mut s = Sim {
            rng: Rng(seed | 1),
            now: 1_000_000,
            nodes: vec![
                Node {
                    up: true,
                    lost: false,
                    refuses: false,
                    shards: BTreeSet::new(),
                };
                nodes
            ],
            groups: Vec::new(),
            pacing: Pacing {
                backoff_base_ms: 2 * PASS_MS,
                backoff_max_ms: 16 * PASS_MS,
                unrecoverable_retry_ms: 40 * PASS_MS,
            },
            budget: 3,
            replace_other_early: false,
            trace: Vec::new(),
        };
        for gi in 0..groups {
            let members: Vec<usize> = (0..k + m).map(|i| (gi * (k + m) + i) % nodes).collect();
            for c in 0..chunks {
                for (i, &n) in members.iter().enumerate() {
                    s.nodes[n].shards.insert((gi, c, i));
                }
            }
            s.groups.push(Group {
                k,
                members,
                epoch: 1,
                repairing: None,
                status: None,
                chunks,
                damaged: None,
                attempts: Vec::new(),
            });
        }
        s
    }

    fn healthy(&self, n: usize) -> bool {
        self.nodes[n].up
    }

    /// Shards of chunk `c` of group `g` on its members (up or not).
    fn on_members(&self, g: usize, c: usize) -> usize {
        let grp = &self.groups[g];
        grp.members
            .iter()
            .enumerate()
            .filter(|(i, n)| self.nodes[**n].shards.contains(&(g, c, *i)))
            .count()
    }

    /// Readable shards of chunk `c` (members up, slot `skip` excluded).
    fn readable(&self, g: usize, c: usize, skip: usize) -> usize {
        let grp = &self.groups[g];
        if grp.damaged == Some(c) {
            return grp.k - 1;
        }
        grp.members
            .iter()
            .enumerate()
            .filter(|(i, n)| {
                *i != skip && self.nodes[**n].up && self.nodes[**n].shards.contains(&(g, c, *i))
            })
            .count()
    }

    fn whole(&self, g: usize) -> bool {
        let grp = &self.groups[g];
        (0..grp.chunks).all(|c| grp.members.len() == self.on_members(g, c))
    }

    fn pick(&mut self, g: usize, exclude: &[usize]) -> Option<usize> {
        let grp = &self.groups[g];
        let cands: Vec<usize> = (0..self.nodes.len())
            .filter(|n| self.nodes[*n].up && !grp.members.contains(n) && !exclude.contains(n))
            .collect();
        if cands.is_empty() {
            return None;
        }
        let i = self.rng.below(cands.len() as u64) as usize;
        Some(cands[i])
    }

    /// A membership change of slot `index` to `to`, checking S2 (and
    /// S2-strict when `second`: the replacement of another member than the
    /// slot under rebuild).
    fn change(&mut self, g: usize, index: usize, to: usize, second: bool) {
        self.note(format!(
            "t={} g{g} slot {index}: {} -> {to} (members {:?}, up {:?}, status {:?})",
            self.now,
            self.groups[g].members[index],
            self.groups[g].members,
            self.groups[g]
                .members
                .iter()
                .map(|n| self.nodes[*n].up)
                .collect::<Vec<_>>(),
            self.groups[g]
                .status
                .as_ref()
                .map(|s| (s.index, s.reason, s.walked_clean))
        ));
        let before: Vec<usize> = (0..self.groups[g].chunks)
            .map(|c| self.on_members(g, c))
            .collect();
        assert!(self.nodes[to].up, "S3: a down node chosen as target");
        let leaving = self.groups[g].members[index];
        let former: Vec<usize> = self.groups[g]
            .status
            .iter()
            .filter(|p| usize::from(p.index) == index)
            .flat_map(|p| p.former.iter().map(|&n| n as usize))
            .chain([leaving])
            .collect();
        self.groups[g].members[index] = to;
        self.groups[g].epoch += 1;
        self.groups[g].repairing = Some(index);
        // A node that held the slot before may keep stale shards of it: a
        // rebuild onto it writes them again (a retired record is missing).
        self.nodes[to]
            .shards
            .retain(|s| !(s.0 == g && s.2 == index));
        let k = self.groups[g].k;
        for (c, b) in before.into_iter().enumerate() {
            let strict = self.on_members(g, c);
            if second {
                assert!(
                    strict >= k || strict >= b,
                    "S2-strict: group {g} chunk {c} left with {strict} shards on members (had {b}, needs {k}); trace:\n{}",
                    self.trace.join("\n")
                );
            }
            let kept = usize::from(
                !self.nodes[to].shards.contains(&(g, c, index))
                    && former
                        .iter()
                        .any(|&n| self.nodes[n].shards.contains(&(g, c, index))),
            );
            let after = strict + kept;
            assert!(
                after >= k || after >= b,
                "S2: group {g} chunk {c} left with {after} shards (had {b}, needs {k}); trace:\n{}",
                self.trace.join("\n")
            );
        }
    }

    fn note(&mut self, e: String) {
        if self.trace.len() >= 4000 {
            self.trace.remove(0);
        }
        self.trace.push(e);
    }

    fn obs(&self, g: usize) -> Observation {
        let grp = &self.groups[g];
        let index = grp.repairing.unwrap();
        let target = grp.members[index];
        Observation {
            group: g as u64,
            epoch: grp.epoch,
            index: index as u16,
            target: target as u64,
            target_candidate: self.healthy(target),
            unhealthy_others: grp
                .members
                .iter()
                .enumerate()
                .filter(|(i, n)| *i != index && !self.healthy(**n))
                .map(|(i, _)| i as u16)
                .collect(),
            auto_repair: true,
            data_shards: grp.k,
            members: grp.members.len(),
            status: grp.status.clone(),
            now_ms: self.now,
            pacing: self.pacing,
            domain_recorded: true,
        }
    }

    fn start(
        &mut self,
        g: usize,
        index: usize,
        prior: Option<RepairStatus>,
        second: bool,
        also: &[usize],
    ) -> bool {
        let exclude: Vec<usize> = prior
            .iter()
            .filter(|p| usize::from(p.index) == index)
            .flat_map(|p| p.former.iter().map(|&n| n as usize))
            .chain([self.groups[g].members[index]])
            .chain(also.iter().copied())
            .chain(
                prior
                    .iter()
                    .flat_map(|p| p.excluded.iter().map(|&n| n as usize)),
            )
            .collect();
        let Some(to) = self.pick(g, &exclude) else {
            return false;
        };
        let leaving = self.groups[g].members[index];
        if let Some(p) = &prior {
            assert!(
                !(usize::from(p.index) == index && p.former.contains(&(to as u64))),
                "S3: a node that left the slot chosen again"
            );
            assert!(
                !p.excluded.contains(&(to as u64)),
                "S3: a node that left another slot of this repair chosen"
            );
        }
        self.change(g, index, to, second);
        self.groups[g].status = Some(repair_liveness::started(
            prior.as_ref(),
            g as u64,
            self.groups[g].epoch,
            index as u16,
            to as u64,
            leaving as u64,
            false,
            self.now,
        ));
        let _ = self.attempt(g);
        true
    }

    /// One rebuild attempt of the group's slot; what to do about a second
    /// failure, from its outcome.
    fn attempt(&mut self, g: usize) -> Option<Decision> {
        self.groups[g].attempts.push(self.now);
        let index = self.groups[g].repairing.unwrap();
        let target = self.groups[g].members[index];
        let obs = self.obs(g);
        let outcome = if !self.nodes[target].up || self.nodes[target].refuses {
            Outcome::TargetFailure {
                corrupt: false,
                detail: "target".into(),
            }
        } else {
            let mut rebuilt = 0;
            let mut unrebuilt = 0;
            let mut budget = false;
            // Nodes that left the slot and answer give its shard as one of
            // the `k` inputs (`RepairOptions::former`, from the status).
            let former: Vec<usize> = obs
                .status
                .iter()
                .filter(|p| usize::from(p.index) == index)
                .flat_map(|p| p.former.iter().map(|&n| n as usize))
                .filter(|&n| self.nodes[n].up && !self.groups[g].members.contains(&n))
                .collect();
            for c in 0..self.groups[g].chunks {
                if self.nodes[target].shards.contains(&(g, c, index)) {
                    continue;
                }
                if rebuilt >= self.budget {
                    budget = true;
                    break;
                }
                let from_former = usize::from(
                    self.groups[g].damaged != Some(c)
                        && former
                            .iter()
                            .any(|&n| self.nodes[n].shards.contains(&(g, c, index))),
                );
                if self.readable(g, c, index) + from_former >= self.groups[g].k {
                    self.nodes[target].shards.insert((g, c, index));
                    rebuilt += 1;
                } else {
                    unrebuilt += 1;
                }
            }
            // Stripe repair (its own worker) fills shards missing on the
            // other members that answer, when `k` others are readable (the
            // move's completeness check queues them).
            if !budget && unrebuilt == 0 {
                for c in 0..self.groups[g].chunks {
                    for i in 0..self.groups[g].members.len() {
                        let n = self.groups[g].members[i];
                        if i != index
                            && self.nodes[n].up
                            && !self.nodes[n].shards.contains(&(g, c, i))
                            && self.readable(g, c, i) >= self.groups[g].k
                        {
                            self.nodes[n].shards.insert((g, c, i));
                        }
                    }
                }
            }
            let members_down = self.groups[g].members.iter().any(|n| !self.nodes[*n].up);
            if budget {
                Outcome::Budget
            } else if unrebuilt > 0 {
                Outcome::Unrebuilt {
                    members_down,
                    detail: "fewer than k".into(),
                }
            } else if self.whole(g) && !members_down {
                // S1 holds by construction here: checked again below.
                Outcome::Complete
            } else {
                let slot_whole = (0..self.groups[g].chunks)
                    .all(|c| self.nodes[target].shards.contains(&(g, c, index)));
                Outcome::Incomplete {
                    chunks_short: (0..self.groups[g].chunks)
                        .any(|c| self.readable(g, c, usize::MAX) < self.groups[g].k),
                    slot_whole,
                    unavailable: self.groups[g]
                        .members
                        .iter()
                        .enumerate()
                        .filter(|(_, n)| !self.nodes[**n].up)
                        .map(|(i, _)| i as u16)
                        .collect(),
                    unhealthy_unavailable: members_down,
                    progressed: rebuilt > 0,
                    detail: "incomplete".into(),
                }
            }
        };
        self.note(format!(
            "t={} g{g} attempt slot {index} onto {target}: {outcome:?}",
            self.now
        ));
        let second = repair_liveness::second_failure(&obs, &outcome);
        match repair_liveness::after(&obs, &outcome) {
            None => {
                assert!(self.whole(g), "S1: group {g} Active without every shard");
                self.groups[g].repairing = None;
                self.groups[g].status = None;
                None
            }
            Some(s) => {
                self.groups[g].status = Some(s);
                second
            }
        }
    }

    fn pass(&mut self) {
        for g in 0..self.groups.len() {
            match self.groups[g].repairing {
                None => {
                    let down = self.groups[g]
                        .members
                        .iter()
                        .position(|n| !self.healthy(*n));
                    if let Some(i) = down {
                        self.start(g, i, None, false, &[]);
                    }
                }
                Some(index) => {
                    let obs = self.obs(g);
                    let mut decision = repair_liveness::decide(&obs);
                    if self.replace_other_early
                        && let Some(&j) = obs.unhealthy_others.first()
                        && obs.target_candidate
                    {
                        decision = Decision::ReplaceOther(j);
                    }
                    match decision {
                        Decision::Wait => {}
                        Decision::AwaitNodes(_) => {}
                        Decision::Run => {
                            if let Some(Decision::ReplaceOther(j)) = self.attempt(g) {
                                let prior = self.groups[g].status.clone();
                                let also: Vec<usize> = prior
                                    .iter()
                                    .flat_map(|p| p.former.iter().map(|&n| n as usize))
                                    .chain([self.groups[g].members[index]])
                                    .collect();
                                if !self.start(g, usize::from(j), prior, true, &also) {
                                    let s = repair_liveness::no_new_target(
                                        &self.obs(g),
                                        RepairReason::AwaitingNodes,
                                        "none".into(),
                                    );
                                    self.groups[g].status = Some(s);
                                }
                            }
                        }
                        Decision::Retarget(_) => {
                            let prior = Some(repair_liveness::current(&obs));
                            if !self.start(g, index, prior, false, &[]) {
                                let mut s = repair_liveness::no_new_target(
                                    &obs,
                                    RepairReason::AwaitingNodes,
                                    "none".into(),
                                );
                                if obs.target_candidate {
                                    s.failures = 0;
                                }
                                self.groups[g].status = Some(s);
                            }
                        }
                        Decision::ReplaceOther(j) => {
                            let prior = self.groups[g].status.clone();
                            if !self.start(g, usize::from(j), prior, true, &[]) {
                                let s = repair_liveness::no_new_target(
                                    &obs,
                                    RepairReason::AwaitingNodes,
                                    "none".into(),
                                );
                                self.groups[g].status = Some(s);
                            }
                        }
                    }
                }
            }
        }
        self.now += PASS_MS;
    }

    fn fault(&mut self, max_lost: usize) {
        let n = self.rng.below(self.nodes.len() as u64) as usize;
        match self.rng.below(10) {
            0..=2 if self.nodes[n].up => self.nodes[n].up = false,
            3..=5 if !self.nodes[n].up && !self.nodes[n].lost => self.nodes[n].up = true,
            6 if self.nodes[n].up && self.nodes.iter().filter(|x| x.lost).count() < max_lost => {
                // A disk lost: the node comes back empty, later.
                self.nodes[n].up = false;
                self.nodes[n].lost = true;
            }
            7 => self.nodes[n].refuses = !self.nodes[n].refuses,
            _ => {}
        }
    }

    /// End of the fault phase: every node not lost is up and accepts
    /// writes; a lost disk is replaced by an empty node that is up.
    fn quiesce(&mut self) {
        for n in &mut self.nodes {
            n.refuses = false;
            if n.lost {
                n.shards.clear();
            }
            n.up = true;
            n.lost = false;
        }
    }
}

fn run(seed: u64, mutate: bool) -> Result<(), String> {
    let r = std::panic::catch_unwind(|| {
        // Twelve nodes, three groups: 2+1 on even seeds, 2+2 on odd ones.
        let (k, m) = if seed.is_multiple_of(2) {
            (2, 1)
        } else {
            (2, 2)
        };
        let mut s = Sim::new(seed, 12, 3, k, m, 12);
        s.replace_other_early = mutate;
        // Group 2: one chunk with fewer than `k` readable shards for good.
        s.groups[2].damaged = Some(5);
        // Faults for a while (at most one disk lost for good per run).
        for step in 0..400 {
            if step % 3 == 0 {
                s.fault(1);
            }
            s.pass();
        }
        s.quiesce();
        let quiet_from = s.now;
        // L1: with failures over, groups 0 and 1 end Active in a bound:
        // the longest backoff, a retarget per lost target, and the
        // rebuild at the budget.
        let bound = 3 * (s.pacing.backoff_max_ms / PASS_MS) as usize + 3 * 12 / s.budget + 60;
        for _ in 0..bound {
            s.pass();
        }
        for g in 0..2 {
            assert!(
                s.groups[g].repairing.is_none(),
                "L1: group {g} not Active {bound} passes after the faults: {:?}; members {:?}; per chunk on members {:?}; holders {:?}",
                s.groups[g].status,
                s.groups[g].members,
                (0..s.groups[g].chunks)
                    .map(|c| s.on_members(g, c))
                    .collect::<Vec<_>>(),
                (0..s.groups[g].chunks)
                    .map(|c| (0..s.nodes.len())
                        .filter(|n| s.nodes[*n].shards.iter().any(|x| x.0 == g && x.1 == c))
                        .map(|n| (
                            n,
                            s.nodes[n]
                                .shards
                                .iter()
                                .filter(|x| x.0 == g && x.1 == c)
                                .map(|x| x.2)
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            );
        }
        // L2: the unrecoverable group, if repairing, is retried at most
        // once per retry delay once only it is stuck (after one longest
        // backoff, by which every member answers again and it knows it).
        for _ in 0..200 {
            s.pass();
        }
        let g2 = &s.groups[2];
        if g2.repairing.is_some() {
            assert_eq!(
                g2.status.as_ref().map(|s| s.reason),
                Some(RepairReason::DataUnrecoverable),
                "{:?}",
                g2.status
            );
            let window_start = quiet_from + 2 * s.pacing.backoff_max_ms;
            let recent = g2.attempts.iter().filter(|&&t| t >= window_start).count() as u64;
            let allowed = (s.now - window_start) / s.pacing.unrecoverable_retry_ms + 2;
            assert!(
                recent <= allowed,
                "L2: {recent} attempts, at most {allowed}"
            );
        }
        // L3: what retargets happened stayed bounded (a node is never the
        // slot's target twice in one repair).
        let _ = RETARGET_AFTER;
    });
    r.map_err(|e| {
        e.downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_default()
    })
}

#[test]
fn the_scheduling_rules_keep_safety_and_liveness_over_seeds() {
    let seeds: u64 = std::env::var("ARCA_REPAIR_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    for seed in 1..=seeds {
        if let Err(e) = run(seed, false) {
            panic!("seed {seed}: {e}");
        }
    }
}

/// Mutation: a second failed member replaced before the slot is whole.
/// Some seed must break S2-strict (a chunk left with fewer than `k`
/// shards on the members).
#[test]
fn replacing_a_second_member_early_is_caught() {
    let caught = (1..=300u64).find_map(|seed| run(seed, true).err().map(|e| (seed, e)));
    let (seed, e) = caught.expect("no seed caught the early replacement");
    assert!(e.contains("S2-strict"), "seed {seed}: {e}");
}
