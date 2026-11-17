//! Bounded, resumable background sweeps (Phase 3.14): which
//! `(group, node)` pairs a reconciliation pass handles, from the persisted
//! cursor ([`arca_lake_metadata::ops::bg_cursor`]).
//!
//! A pass takes the pairs strictly after the cursor in `(group, node)`
//! order, at most `budget` of them, from the groups and nodes as they are
//! now; the pass that reaches the end wraps (the next starts at the
//! beginning) and counts a sweep. So every pair that exists through a
//! whole sweep is handled once in it, whatever was added, removed or
//! failed meanwhile, and whichever leader ran the passes; one failing pair
//! is passed over (and retried next sweep), never retried before the
//! others.

/// The work of one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Pairs to handle, in order.
    pub pairs: Vec<(u64, u64)>,
    /// Whether the batch reaches the end of the pairs (the next pass
    /// starts at the beginning).
    pub wraps: bool,
}

/// The pairs after `after` (`None`: from the beginning), at most `budget`
/// (at least one). `groups` and `nodes` in any order.
#[must_use]
pub fn next_pairs(
    after: Option<(u64, u64)>,
    groups: &[u64],
    nodes: &[u64],
    budget: usize,
) -> Batch {
    let mut g: Vec<u64> = groups.to_vec();
    g.sort_unstable();
    g.dedup();
    let mut n: Vec<u64> = nodes.to_vec();
    n.sort_unstable();
    n.dedup();
    let budget = budget.max(1);
    let mut pairs = Vec::new();
    for &gid in &g {
        if after.is_some_and(|(ag, _)| gid < ag) {
            continue;
        }
        for &nid in &n {
            if after.is_some_and(|a| (gid, nid) <= a) {
                continue;
            }
            if pairs.len() == budget {
                return Batch {
                    pairs,
                    wraps: false,
                };
            }
            pairs.push((gid, nid));
        }
    }
    Batch { pairs, wraps: true }
}

/// Pairs at or before `after`, and all pairs: how far a sweep is, counted
/// without listing the pairs (`groups` and `nodes` in any order).
#[must_use]
pub fn progress(after: Option<(u64, u64)>, groups: &[u64], nodes: &[u64]) -> (usize, usize) {
    let mut g: Vec<u64> = groups.to_vec();
    g.sort_unstable();
    g.dedup();
    let mut n: Vec<u64> = nodes.to_vec();
    n.sort_unstable();
    n.dedup();
    let total = g.len() * n.len();
    let Some((ag, an)) = after else {
        return (0, total);
    };
    let before = g.partition_point(|&x| x < ag) * n.len();
    let within = if g.binary_search(&ag).is_ok() {
        n.partition_point(|&x| x <= an)
    } else {
        0
    };
    (before + within, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_counts_the_pairs_a_batch_would_not_list() {
        let groups = [5, 1, 9, 3];
        let nodes = [20, 10, 30];
        for ag in 0..11 {
            for an in [0, 10, 15, 30, 40] {
                let after = Some((ag, an));
                let left = next_pairs(after, &groups, &nodes, usize::MAX).pairs.len();
                assert_eq!(
                    progress(after, &groups, &nodes),
                    (12 - left, 12),
                    "{after:?}"
                );
            }
        }
        assert_eq!(progress(None, &groups, &nodes), (0, 12));
    }
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn a_batch_resumes_after_the_cursor_and_wraps_at_the_end() {
        let b = next_pairs(None, &[2, 1], &[20, 10], 3);
        assert_eq!(b.pairs, vec![(1, 10), (1, 20), (2, 10)]);
        assert!(!b.wraps);
        let b = next_pairs(Some((2, 10)), &[1, 2], &[10, 20], 3);
        assert_eq!(b.pairs, vec![(2, 20)]);
        assert!(b.wraps);
        // A cursor on a group or node gone since: the next pair after it.
        let b = next_pairs(Some((1, 15)), &[2, 1], &[20, 10], 8);
        assert_eq!(b.pairs, vec![(1, 20), (2, 10), (2, 20)]);
        let b = next_pairs(Some((5, 0)), &[1, 2], &[10], 8);
        assert!(b.pairs.is_empty() && b.wraps);
        assert_eq!(next_pairs(None, &[], &[1], 4).pairs, Vec::new());
    }

    /// xorshift64*: deterministic.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    /// Deterministic simulator: many groups and nodes, added and removed
    /// between passes, pairs failing, leaders failing over (the cursor is
    /// what was persisted; a crash loses the batch's progress after the
    /// last persisted pair), cursors left stale. Checked per seed:
    /// - no pass handles more than its budget;
    /// - every pair that existed through a whole sweep was handled (or
    ///   failed) in that sweep: none starves, failures do not stop the
    ///   others;
    /// - a sweep takes at most `ceil(pairs / budget)` passes, plus per
    ///   crash what it redoes (8 pairs, or a node row for a stale cursor).
    #[test]
    #[allow(clippy::too_many_lines)]
    fn simulated_sweeps_reach_every_pair_through_churn_failures_and_failovers() {
        for seed in 1..=100u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut groups: BTreeSet<u64> = (1..=rng.below(120) + 1).collect();
            let mut nodes: BTreeSet<u64> = (100..=100 + rng.below(12)).collect();
            let mut next_group = 10_000;
            let mut next_node = 20_000;
            let budget = usize::try_from(rng.below(61) + 4).unwrap();
            let mut cursor: Option<(u64, u64)> = None;
            // Pairs seen handled since the sweep began, and the pairs that
            // existed at its start and never went away.
            let mut handled: BTreeSet<(u64, u64)> = BTreeSet::new();
            let mut stable: BTreeSet<(u64, u64)> = groups
                .iter()
                .flat_map(|&g| nodes.iter().map(move |&n| (g, n)))
                .collect();
            let mut passes_this_sweep = 0usize;
            let mut crashes_this_sweep = 0usize;
            let mut sweeps = 0;
            let mut max_nodes = 0usize;
            let mut fail_counts: BTreeMap<(u64, u64), u32> = BTreeMap::new();
            for _pass in 0..3000 {
                // Churn between passes.
                match rng.below(10) {
                    0 => {
                        groups.insert(next_group);
                        next_group += 1;
                    }
                    1 if groups.len() > 1 => {
                        let g = *groups
                            .iter()
                            .nth(usize::try_from(rng.below(groups.len() as u64)).unwrap())
                            .unwrap();
                        groups.remove(&g);
                        stable.retain(|p| p.0 != g);
                    }
                    2 => {
                        nodes.insert(next_node);
                        next_node += 1;
                    }
                    3 if nodes.len() > 1 => {
                        let n = *nodes
                            .iter()
                            .nth(usize::try_from(rng.below(nodes.len() as u64)).unwrap())
                            .unwrap();
                        nodes.remove(&n);
                        stable.retain(|p| p.1 != n);
                    }
                    _ => {}
                }
                let gv: Vec<u64> = groups.iter().copied().collect();
                let nv: Vec<u64> = nodes.iter().copied().collect();
                max_nodes = max_nodes.max(nv.len());
                let batch = next_pairs(cursor, &gv, &nv, budget);
                assert!(batch.pairs.len() <= budget, "seed {seed}");
                passes_this_sweep += 1;
                // A failover in the middle of the batch: progress up to the
                // last persisted pair (every 8, as the pass persists).
                let crash_at = (rng.below(20) == 0 && !batch.pairs.is_empty())
                    .then(|| usize::try_from(rng.below(batch.pairs.len() as u64)).unwrap());
                let mut persisted = cursor;
                let mut crashed = false;
                for (i, &p) in batch.pairs.iter().enumerate() {
                    if crash_at == Some(i) {
                        crashed = true;
                        break;
                    }
                    // A pair fails one time in 16 (handled = attempted).
                    if rng.below(16) == 0 {
                        *fail_counts.entry(p).or_default() += 1;
                    }
                    handled.insert(p);
                    if i % 8 == 7 || i + 1 == batch.pairs.len() {
                        persisted = Some(p);
                    }
                }
                if crashed {
                    crashes_this_sweep += 1;
                    cursor = persisted;
                    // A stale cursor now and then: an earlier leader's.
                    if rng.below(4) == 0 {
                        cursor = cursor.map(|(g, n)| (g.saturating_sub(1), n));
                    }
                    continue;
                }
                if batch.wraps {
                    let missing: Vec<_> = stable.difference(&handled).collect();
                    assert!(
                        missing.is_empty(),
                        "seed {seed}: pairs starved in a sweep: {:?}",
                        &missing[..missing.len().min(5)]
                    );
                    // Pairs handled in this sweep (those added ahead of the
                    // cursor included), in passes of `budget`.
                    let redo = (max_nodes + 8).div_ceil(budget) + 1;
                    assert!(
                        passes_this_sweep
                            <= handled.len().div_ceil(budget) + crashes_this_sweep * redo + 1,
                        "seed {seed}: {passes_this_sweep} passes for {} pairs at {budget}",
                        handled.len()
                    );
                    sweeps += 1;
                    cursor = None;
                    handled.clear();
                    stable = groups
                        .iter()
                        .flat_map(|&g| nodes.iter().map(move |&n| (g, n)))
                        .collect();
                    passes_this_sweep = 0;
                    crashes_this_sweep = 0;
                    max_nodes = 0;
                } else {
                    cursor = persisted;
                }
            }
            assert!(sweeps > 0, "seed {seed}: never a full sweep");
        }
    }
}
