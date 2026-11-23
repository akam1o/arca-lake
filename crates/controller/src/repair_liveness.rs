//! Scheduling of `Repairing` groups (Phase 3.13, R-104, ADR-0029): what
//! the leader does with a group whose member is being rebuilt, and the
//! status it records afterwards. Pure functions of what a pass observed,
//! so that a deterministic simulator drives exactly the leader's choices.
//!
//! Safety does not rest on anything here: a target change is a membership
//! change through an epoch intent (fenced: the new target acknowledges the
//! new epoch before the commit, so an attempt of the old epoch can no
//! longer write to it, and every cursor or completion update of the old
//! attempt is conditioned on the old epoch), and the group becomes
//! `Active` only through the completeness check. These rules decide only
//! *when* to retry, to change the target, or to replace a second failed
//! member, and how the group is reported.

use std::collections::HashMap;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::repair_status::{self, RepairReason, RepairStatus};
use arca_lake_metadata::records::{GroupRecord, GroupState};

use crate::ControlError;

/// Consecutive failures attributed to a healthy target after which it is
/// replaced.
pub const RETARGET_AFTER: u32 = 5;
/// Consecutive attempts finding chunks with fewer than `k` shards while
/// every member is healthy after which the group is reported unrecoverable
/// and retried rarely.
pub const UNRECOVERABLE_AFTER: u32 = 3;

/// Retry delays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pacing {
    /// First retry delay after an attempt without progress, ms.
    pub backoff_base_ms: u64,
    /// Longest retry delay, ms.
    pub backoff_max_ms: u64,
    /// Retry delay of a group with chunks no answering member can
    /// rebuild, ms.
    pub unrecoverable_retry_ms: u64,
}

impl Default for Pacing {
    fn default() -> Self {
        Self {
            backoff_base_ms: 15_000,
            backoff_max_ms: 600_000,
            unrecoverable_retry_ms: 3_600_000,
        }
    }
}

/// The first durable group whose repair holds back planned background
/// work (rebalancing, tiering), with why: a group not `Active` that is not
/// `Repairing`, or a `Repairing` one whose status is progressing or
/// temporarily blocked, or has none yet (the leader records one on its
/// first attempt). A group waiting for nodes, capacity, an operator, or
/// with data no member can rebuild does not (Phase 3.13, R-104): it would
/// hold them for as long as it waits.
#[must_use]
pub fn holding_group<H: std::hash::BuildHasher>(
    groups: &[GroupRecord],
    statuses: &HashMap<u64, RepairStatus, H>,
) -> Option<(u64, String)> {
    groups
        .iter()
        .filter(|g| !g.is_cache() && g.state != GroupState::Active)
        .find_map(|g| {
            if g.state != GroupState::Repairing {
                return Some((
                    g.id,
                    format!("group {} is {:?}: repair first", g.id, g.state),
                ));
            }
            match statuses.get(&g.id) {
                Some(s) if !s.reason.holds_background_work() => None,
                Some(s) => Some((
                    g.id,
                    format!(
                        "group {} is Repairing ({}): repair first",
                        g.id,
                        s.reason.as_str()
                    ),
                )),
                None => Some((g.id, format!("group {} is Repairing: repair first", g.id))),
            }
        })
}

/// Every recorded status, by group.
///
/// # Errors
///
/// Metadata errors.
pub async fn statuses<S: KvStore>(meta: &S) -> Result<HashMap<u64, RepairStatus>, ControlError> {
    Ok(repair_status::list(meta)
        .await?
        .into_iter()
        .map(|s| (s.group, s))
        .collect())
}

/// What a pass knows about a `Repairing` group.
#[derive(Debug, Clone)]
pub struct Observation {
    /// Group.
    pub group: u64,
    /// The group's epoch.
    pub epoch: u64,
    /// Member index being rebuilt (the cursor's).
    pub index: u16,
    /// Node being rebuilt onto.
    pub target: u64,
    /// The target is healthy, not draining, not retired or quarantined.
    pub target_candidate: bool,
    /// Indexes of the other members that are not healthy.
    pub unhealthy_others: Vec<u16>,
    /// Auto repair is on (a failed member may be replaced without an
    /// operator).
    pub auto_repair: bool,
    /// The group's `k` and its member count.
    pub data_shards: usize,
    /// Members of the group.
    pub members: usize,
    /// The recorded status, if it is about this slot.
    pub status: Option<RepairStatus>,
    /// Now, unix ms.
    pub now_ms: u64,
    /// Retry delays.
    pub pacing: Pacing,
}

/// What to do with the group in this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Continue rebuilding onto the current target.
    Run,
    /// Not before the recorded next attempt.
    Wait,
    /// Rebuild the slot onto another node (the current target is gone, or
    /// failed too often).
    Retarget(String),
    /// The slot is rebuilt but for members that do not answer: replace
    /// this one (auto repair).
    ReplaceOther(u16),
    /// The slot is rebuilt but for members that do not answer, and no
    /// operator or auto repair replaces them.
    AwaitNodes(String),
}

/// The status as it applies to this slot and epoch: per-epoch counters
/// reset when the epoch moved on (a target change commits a new epoch;
/// what nodes left the slot is kept).
#[must_use]
pub fn current(o: &Observation) -> RepairStatus {
    match &o.status {
        Some(s) if s.index == o.index && s.epoch == o.epoch => s.clone(),
        Some(s) if s.index == o.index => RepairStatus {
            group: s.group,
            epoch: o.epoch,
            index: o.index,
            target: o.target,
            reason: RepairReason::Progressing,
            detail: String::new(),
            since_ms: o.now_ms,
            last_progress_ms: s.last_progress_ms,
            former: s.former.clone(),
            untrusted: s.untrusted.clone(),
            retargets: s.retargets,
            excluded: s.excluded.clone(),
            ..RepairStatus::default()
        },
        // A status of another slot (a move of another slot recorded and
        // not committed, a crash between): never dropped (re-review); what
        // left that slot stays excluded, the untrusted stay untrusted.
        Some(s) => {
            let mut excluded = s.excluded.clone();
            excluded.extend(s.former.iter().copied());
            excluded.push(s.target);
            excluded.retain(|&n| n != o.target);
            excluded.sort_unstable();
            excluded.dedup();
            RepairStatus {
                group: o.group,
                epoch: o.epoch,
                index: o.index,
                target: o.target,
                since_ms: o.now_ms,
                last_progress_ms: o.now_ms,
                untrusted: s.untrusted.clone(),
                excluded,
                ..RepairStatus::default()
            }
        }
        None => RepairStatus {
            group: o.group,
            epoch: o.epoch,
            index: o.index,
            target: o.target,
            since_ms: o.now_ms,
            last_progress_ms: o.now_ms,
            ..RepairStatus::default()
        },
    }
}

/// After an attempt ended in `outcome`: replace a second failed member,
/// or report waiting for it. Only on the outcome of the walk that just ran
/// (independent review: a whole slot found earlier may have lost shards
/// since), only when that walk found the slot whole, and only while the
/// members that answer still give every chunk `k` shards (R4).
#[must_use]
pub fn second_failure(o: &Observation, outcome: &Outcome) -> Option<Decision> {
    let Outcome::Incomplete {
        slot_whole: true,
        unhealthy_unavailable: true,
        unavailable,
        chunks_short,
        ..
    } = outcome
    else {
        return None;
    };
    if *chunks_short {
        // A chunk needs a shard of a member that does not answer (re-review:
        // nodes counted are not shards per chunk).
        return Some(Decision::AwaitNodes(
            "some chunks have fewer than k shards on the members that answer: bring the others back".into(),
        ));
    }
    let &j = o.unhealthy_others.first()?;
    // Members not answering: the unhealthy ones and those this walk could
    // not list although seen recently (re-review).
    let mut down: Vec<u16> = o.unhealthy_others.clone();
    down.extend(unavailable.iter().filter(|i| **i != o.index));
    down.sort_unstable();
    down.dedup();
    if o.members.saturating_sub(down.len()) < o.data_shards {
        return Some(Decision::AwaitNodes(format!(
            "{} members do not answer: fewer than k = {} answer; bring them back",
            down.len(),
            o.data_shards
        )));
    }
    Some(if o.auto_repair {
        Decision::ReplaceOther(j)
    } else {
        Decision::AwaitNodes(format!(
            "member {j} does not answer; auto repair is off: replace it (`admin replace`) or bring it back"
        ))
    })
}

/// Decide what to do with the group.
#[must_use]
pub fn decide(o: &Observation) -> Decision {
    let s = current(o);
    // No other node could take the slot at the last try: not before the
    // backoff (independent review: every pass would otherwise probe every
    // node's room for every such group).
    let no_target_yet = s.no_target && s.next_attempt_ms > o.now_ms;
    if !o.target_candidate && no_target_yet {
        return Decision::Wait;
    }
    if !o.target_candidate {
        return Decision::Retarget(format!(
            "target node {} is unhealthy, draining, or retired",
            o.target
        ));
    }
    if s.failures >= RETARGET_AFTER {
        return Decision::Retarget(format!(
            "target node {} failed {} attempts in a row",
            o.target, s.failures
        ));
    }
    // Waiting for nodes that are back, or retrying rarely chunks no
    // member could give while one member had not been seen long enough to
    // count as failed: tried now.
    let nodes_back = s.reason == RepairReason::AwaitingNodes && o.unhealthy_others.is_empty();
    let member_since_failed =
        s.reason == RepairReason::DataUnrecoverable && !o.unhealthy_others.is_empty();
    if s.next_attempt_ms > o.now_ms && !nodes_back && !member_since_failed {
        return Decision::Wait;
    }
    Decision::Run
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The group is `Active`.
    Complete,
    /// The per-pass budget was spent (chunks were rebuilt).
    Budget,
    /// The walk finished, the completeness check failed:
    /// `unhealthy_unavailable` when members that do not answer are why;
    /// `slot_whole` when the slot rebuilt is whole all the same;
    /// `progressed` when this attempt rebuilt chunks.
    Incomplete {
        /// The rebuilt slot is whole (see `RepairReport::slot_whole`).
        slot_whole: bool,
        /// Members (other than the slot rebuilt) whose listing failed in
        /// this walk.
        unavailable: Vec<u16>,
        /// Some chunk has fewer than `k` shards on the members that
        /// answered (or the walk's list of shards missing elsewhere was
        /// cut short): no member may be replaced.
        chunks_short: bool,
        /// Unhealthy members did not answer.
        unhealthy_unavailable: bool,
        /// Chunks were rebuilt in this attempt.
        progressed: bool,
        /// What the check found.
        detail: String,
    },
    /// The walk finished, some chunks had fewer than `k` readable shards
    /// (queued for stripe repair).
    Unrebuilt {
        /// Some member is not healthy (it may hold the shards).
        members_down: bool,
        /// What failed.
        detail: String,
    },
    /// The target failed (did not answer, refused the write, or stored
    /// bytes that do not check). `corrupt`: the last.
    TargetFailure {
        /// Stored bytes did not check.
        corrupt: bool,
        /// What failed.
        detail: String,
    },
    /// Anything else expected to pass (an epoch change, another node's
    /// error).
    Transient(String),
}

/// Backoff after `stalls` attempts without progress.
#[must_use]
pub fn backoff_ms(p: &Pacing, stalls: u32) -> u64 {
    p.backoff_base_ms
        .saturating_mul(1u64 << stalls.saturating_sub(1).min(20))
        .min(p.backoff_max_ms)
}

/// The status after an attempt (`None`: the repair completed, the record
/// goes).
#[must_use]
pub fn after(o: &Observation, outcome: &Outcome) -> Option<RepairStatus> {
    let mut s = current(o);
    s.no_target = false;
    let now = o.now_ms;
    let set = |s: &mut RepairStatus, reason: RepairReason, detail: String| {
        if s.reason != reason {
            s.since_ms = now;
        }
        s.reason = reason;
        s.detail = detail;
    };
    match outcome {
        Outcome::Complete => return None,
        Outcome::Budget => {
            s.walked_clean = false;
            set(&mut s, RepairReason::Progressing, String::new());
            s.failures = 0;
            s.stalls = 0;
            s.last_progress_ms = now;
            s.next_attempt_ms = 0;
        }
        Outcome::Incomplete {
            slot_whole,
            unhealthy_unavailable,
            progressed,
            detail,
            ..
        } => {
            s.walked_clean = *slot_whole;
            s.failures = 0;
            if *progressed {
                s.stalls = 0;
                s.last_progress_ms = now;
            } else {
                s.stalls = s.stalls.saturating_add(1);
            }
            if *unhealthy_unavailable {
                set(&mut s, RepairReason::AwaitingNodes, detail.clone());
            } else {
                set(&mut s, RepairReason::TemporarilyBlocked, detail.clone());
            }
            s.next_attempt_ms = if s.stalls == 0 {
                0
            } else {
                now + backoff_ms(&o.pacing, s.stalls)
            };
        }
        Outcome::Unrebuilt {
            members_down,
            detail,
        } => {
            s.walked_clean = false;
            s.failures = 0;
            s.stalls = s.stalls.saturating_add(1);
            if *members_down {
                set(&mut s, RepairReason::AwaitingNodes, detail.clone());
                s.next_attempt_ms = now + backoff_ms(&o.pacing, s.stalls);
            } else if s.stalls < UNRECOVERABLE_AFTER {
                // A member whose listing failed for a moment looks the same
                // (re-review): retried with the backoff first.
                set(&mut s, RepairReason::TemporarilyBlocked, detail.clone());
                s.next_attempt_ms = now + backoff_ms(&o.pacing, s.stalls);
            } else {
                set(&mut s, RepairReason::DataUnrecoverable, detail.clone());
                s.next_attempt_ms = now + o.pacing.unrecoverable_retry_ms;
            }
        }
        Outcome::TargetFailure { corrupt, detail } => {
            s.walked_clean = false;
            s.failures = s.failures.saturating_add(1);
            s.stalls = s.stalls.saturating_add(1);
            if *corrupt && !s.untrusted.contains(&o.target) {
                s.untrusted.push(o.target);
            }
            set(&mut s, RepairReason::TemporarilyBlocked, detail.clone());
            s.next_attempt_ms = now + backoff_ms(&o.pacing, s.stalls);
        }
        Outcome::Transient(detail) => {
            s.walked_clean = false;
            s.stalls = s.stalls.saturating_add(1);
            set(&mut s, RepairReason::TemporarilyBlocked, detail.clone());
            s.next_attempt_ms = now + backoff_ms(&o.pacing, s.stalls);
        }
    }
    Some(s)
}

/// Why a repair waits when the move it needs found no target:
/// `capacity` (candidates exist, none with room), `unrecorded` (refused
/// only because the group has no recorded failure domain, ADR-0038).
#[must_use]
pub fn no_target_reason(capacity: bool, unrecorded: bool) -> RepairReason {
    if unrecorded {
        RepairReason::AwaitingFailureDomain
    } else if capacity {
        RepairReason::AwaitingCapacity
    } else {
        RepairReason::AwaitingNodes
    }
}

/// The status when the slot is to be rebuilt onto another node and none
/// can take it, for `reason` ([`no_target_reason`]).
#[must_use]
pub fn no_new_target(o: &Observation, reason: RepairReason, detail: String) -> RepairStatus {
    let mut s = current(o);
    if s.reason != reason {
        s.since_ms = o.now_ms;
    }
    s.reason = reason;
    s.detail = detail;
    s.no_target = true;
    s.stalls = s.stalls.saturating_add(1);
    s.next_attempt_ms = o.now_ms + backoff_ms(&o.pacing, s.stalls);
    s
}

/// The status recorded when slot `index` starts being rebuilt onto
/// `target` at `epoch` (the epoch the change commits), the node it leaves
/// being `leaving`. Carries what left the slot so far when the slot is the
/// same.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn started(
    prior: Option<&RepairStatus>,
    group: u64,
    epoch: u64,
    index: u16,
    target: u64,
    leaving: u64,
    leaving_untrusted: bool,
    now_ms: u64,
) -> RepairStatus {
    let mut former = Vec::new();
    let mut untrusted = Vec::new();
    let mut retargets = 0;
    let mut excluded = Vec::new();
    if let Some(p) = prior {
        excluded.clone_from(&p.excluded);
        // Untrusted for the whole repair, whatever slot (re-review).
        untrusted.clone_from(&p.untrusted);
        if p.index == index {
            former.clone_from(&p.former);
            retargets = p.retargets + 1;
        } else {
            // Another slot of this repair (re-review): what left it stays
            // out of every target choice until the group is Active.
            excluded.extend(p.former.iter().copied());
            excluded.push(p.target);
        }
    }
    excluded.retain(|&n| n != target);
    excluded.sort_unstable();
    excluded.dedup();
    if !former.contains(&leaving) {
        former.push(leaving);
    }
    if leaving_untrusted && !untrusted.contains(&leaving) {
        untrusted.push(leaving);
    }
    former.retain(|&n| n != target);
    RepairStatus {
        group,
        epoch,
        index,
        target,
        reason: RepairReason::Progressing,
        detail: String::new(),
        since_ms: now_ms,
        last_progress_ms: now_ms,
        former,
        untrusted,
        retargets,
        excluded,
        ..RepairStatus::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(status: Option<RepairStatus>) -> Observation {
        Observation {
            group: 7,
            epoch: 3,
            index: 1,
            target: 9,
            target_candidate: true,
            unhealthy_others: Vec::new(),
            auto_repair: true,
            data_shards: 2,
            members: 4,
            status,
            now_ms: 1_000_000,
            pacing: Pacing::default(),
        }
    }

    #[test]
    fn a_gone_target_is_replaced_at_once_a_failing_one_after_repeated_failures() {
        let mut o = obs(None);
        assert_eq!(decide(&o), Decision::Run);
        o.target_candidate = false;
        assert!(matches!(decide(&o), Decision::Retarget(_)));
        o.target_candidate = true;
        for i in 0..RETARGET_AFTER {
            assert!(!matches!(decide(&o), Decision::Retarget(_)), "{i}");
            let s = after(
                &o,
                &Outcome::TargetFailure {
                    corrupt: false,
                    detail: "unavailable".into(),
                },
            )
            .unwrap();
            assert_eq!(s.reason, RepairReason::TemporarilyBlocked);
            o.now_ms = s.next_attempt_ms;
            o.status = Some(s);
        }
        assert!(matches!(decide(&o), Decision::Retarget(_)));
    }

    #[test]
    fn data_without_k_shards_never_moves_the_target_and_is_retried_rarely() {
        let mut o = obs(None);
        // The first attempts: retried with the backoff (a listing that
        // failed for a moment looks the same).
        for _ in 1..UNRECOVERABLE_AFTER {
            let s = after(
                &o,
                &Outcome::Unrebuilt {
                    members_down: false,
                    detail: "2 of 4".into(),
                },
            )
            .unwrap();
            assert_eq!(s.reason, RepairReason::TemporarilyBlocked);
            o.now_ms = s.next_attempt_ms;
            o.status = Some(s);
        }
        for _ in 0..20 {
            let s = after(
                &o,
                &Outcome::Unrebuilt {
                    members_down: false,
                    detail: "2 of 4".into(),
                },
            )
            .unwrap();
            assert_eq!(s.reason, RepairReason::DataUnrecoverable);
            assert_eq!(s.failures, 0);
            assert_eq!(
                s.next_attempt_ms,
                o.now_ms + o.pacing.unrecoverable_retry_ms
            );
            o.status = Some(s);
            assert_eq!(decide(&o), Decision::Wait);
            o.now_ms += o.pacing.unrecoverable_retry_ms;
            assert_eq!(decide(&o), Decision::Run);
        }
    }

    #[test]
    fn a_second_failed_member_is_replaced_only_on_a_fresh_whole_slot_with_k_answering() {
        let mut o = obs(None);
        o.unhealthy_others = vec![2];
        let incomplete = |slot_whole| Outcome::Incomplete {
            slot_whole,
            unavailable: vec![2],
            chunks_short: false,
            unhealthy_unavailable: true,
            progressed: false,
            detail: "member 2 unavailable".into(),
        };
        // The slot not whole: never (the member's shards may be needed).
        assert_eq!(second_failure(&o, &incomplete(false)), None);
        assert_eq!(
            second_failure(
                &o,
                &Outcome::Unrebuilt {
                    members_down: true,
                    detail: "1 of 2".into()
                }
            ),
            None
        );
        // Whole, and k of the members answer: replaced.
        assert_eq!(
            second_failure(&o, &incomplete(true)),
            Some(Decision::ReplaceOther(2))
        );
        // A whole slot found earlier is not enough (decide never replaces).
        o.status = after(&o, &incomplete(true));
        assert!(o.status.as_ref().unwrap().walked_clean);
        assert!(!matches!(decide(&o), Decision::ReplaceOther(_)));
        // Fewer than k members answering: waits.
        o.unhealthy_others = vec![2, 3, 0];
        assert!(matches!(
            second_failure(&o, &incomplete(true)),
            Some(Decision::AwaitNodes(_))
        ));
        // A member seen recently that the walk could not list counts.
        o.unhealthy_others = vec![2];
        let flaky = Outcome::Incomplete {
            slot_whole: true,
            unavailable: vec![2, 3, 0],
            chunks_short: false,
            unhealthy_unavailable: true,
            progressed: false,
            detail: "members 2, 3, 0 unavailable".into(),
        };
        assert!(matches!(
            second_failure(&o, &flaky),
            Some(Decision::AwaitNodes(_))
        ));
        // A chunk short of k on the members that answer.
        let short = Outcome::Incomplete {
            slot_whole: true,
            unavailable: vec![2],
            chunks_short: true,
            unhealthy_unavailable: true,
            progressed: false,
            detail: "short".into(),
        };
        assert!(matches!(
            second_failure(&o, &short),
            Some(Decision::AwaitNodes(_))
        ));
        o.auto_repair = false;
        assert!(matches!(
            second_failure(&o, &incomplete(true)),
            Some(Decision::AwaitNodes(_))
        ));
    }

    #[test]
    fn what_left_one_slot_stays_excluded_for_the_others() {
        // Slot 1 rebuilt from node 4 onto 9; then slot 2 starts onto 11.
        let s1 = started(None, 7, 3, 1, 9, 4, false, 5);
        let s2 = started(Some(&s1), 7, 4, 2, 11, 6, false, 6);
        assert_eq!(s2.former, vec![6]);
        assert_eq!(s2.excluded, vec![4, 9]);
        // A status of slot 2 seen while the cursor still names slot 1 (a
        // crash before the intent): nothing it excludes or distrusts goes.
        let mut s2u = s2.clone();
        s2u.untrusted = vec![9];
        let mut o = obs(Some(s2u.clone()));
        o.index = 1;
        o.target = 9;
        o.epoch = 3;
        let c = current(&o);
        assert_eq!(c.untrusted, vec![9]);
        assert!(c.excluded.contains(&4) && c.excluded.contains(&11));
        let s4 = started(Some(&s2u), 7, 6, 3, 13, 6, false, 8);
        assert_eq!(s4.untrusted, vec![9]);
        // A retarget of slot 2 keeps them.
        let s3 = started(Some(&s2), 7, 5, 2, 12, 11, false, 7);
        assert_eq!(s3.excluded, vec![4, 9]);
        assert_eq!(s3.former, vec![6, 11]);
    }

    #[test]
    fn a_gone_target_with_no_other_node_waits_for_the_backoff() {
        let mut o = obs(None);
        o.target_candidate = false;
        assert!(matches!(decide(&o), Decision::Retarget(_)));
        o.status = Some(no_new_target(
            &o,
            RepairReason::AwaitingNodes,
            "none".into(),
        ));
        assert_eq!(decide(&o), Decision::Wait);
        o.now_ms += o.pacing.backoff_max_ms;
        assert!(matches!(decide(&o), Decision::Retarget(_)));
    }

    #[test]
    fn backoff_is_bounded_and_progress_resets_it() {
        let p = Pacing::default();
        assert_eq!(backoff_ms(&p, 1), p.backoff_base_ms);
        assert_eq!(backoff_ms(&p, 2), 2 * p.backoff_base_ms);
        assert_eq!(backoff_ms(&p, 200), p.backoff_max_ms);
        let mut o = obs(None);
        let s = after(&o, &Outcome::Transient("epoch".into())).unwrap();
        assert_eq!(s.stalls, 1);
        o.status = Some(s);
        let s = after(&o, &Outcome::Budget).unwrap();
        assert_eq!((s.stalls, s.next_attempt_ms), (0, 0));
        assert!(after(&o, &Outcome::Complete).is_none());
    }

    #[test]
    fn a_new_epoch_resets_the_counters_and_keeps_what_left_the_slot() {
        let mut o = obs(None);
        let mut s = started(None, 7, 3, 1, 9, 4, false, 5);
        s.failures = 4;
        s.stalls = 4;
        s.next_attempt_ms = u64::MAX;
        o.status = Some(s);
        let next = started(o.status.as_ref(), 7, 4, 1, 11, 9, true, 6);
        assert_eq!(next.former, vec![4, 9]);
        assert_eq!(next.untrusted, vec![9]);
        assert_eq!(next.retargets, 1);
        o.epoch = 5;
        let c = current(&o);
        assert_eq!((c.failures, c.stalls, c.next_attempt_ms), (0, 0, 0));
        assert_eq!(c.former, vec![4]);
    }
}
