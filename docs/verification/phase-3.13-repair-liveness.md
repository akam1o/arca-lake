# Phase 3.13 repair liveness and deferred reclamation verification

Where each result comes from is stated with it:

- **in process**: the in-memory metadata store with real storage-node
  services and gateways in one test process, on a Linux validation VM;
- **host A**: a validation VM (4 vCPU, 7.7 GB) running a private
  `fdbserver` (its own port and data directory; the system FoundationDB
  untouched) and release binaries of this phase and of Phase 3.12
  (`cddd078`), every process on the loopback interface;
- **host B**: a second validation VM (4 vCPU, 7.7 GB, a virtual disk) for
  the measurements of the node's retirement state;
- **CI**: the repository's CI jobs.

All storage was the VMs' virtual disks. No power-loss or physical-disk
`fsync` test was run (process kills and file damage only). Nothing ran
longer than about two hours.

## 1. Outcome

| Item | Status |
| --- | --- |
| R-104 (repair retargeting, High) | resolved for the cases of ADR-0029; Medium remainder open (risk ledger) |
| R-106 | Medium; the node state scales to 1e7 retired records measured (§4) |
| R-105 | Low; manual closes count only verified disks |
| R-94(b) | **bounded (Medium), unchanged**: no claim of a wider guarantee |

## 2. R-104: root cause and reproduction

Root cause (`cddd078`): once a member rebuild committed, nothing chose
another target; a second failure kept the completeness check failing and
no member was replaced while `Repairing`; any `Repairing` group stopped
rebalancing, tiering, cache repair and every drain; nothing recorded why
(ADR-0029 §Context, its scenario table).

Reproduced in process (`repair_retarget.rs`; each fails on `cddd078`'s
rules, see the mutation table):

| Test | Scenario |
| --- | --- |
| `a_target_that_goes_down_mid_repair_is_replaced_and_the_old_epoch_is_fenced` | target down after the commit, with a leader change; a write of the first attempt's epoch refused by the new target; the first target back changes nothing |
| `a_target_refusing_writes_is_replaced_after_repeated_failures` | `RESOURCE_EXHAUSTED` on every shard write to the first target |
| `a_second_failure_that_leaves_fewer_than_k_shards_waits_for_the_node` | 2+1: second member down while slot 0 rebuilds; `awaiting-nodes`, member kept, never `Active` until it returns |
| `a_second_failure_is_replaced_once_the_first_slot_is_whole` | 2+2: second member replaced once slot 0 is whole |
| `an_unrecoverable_group_does_not_hold_drains_or_background_work` | chunk with one readable shard of two: `data-unrecoverable`, stays `Repairing`; another group's drain completes; rebalancing/tiering not held by it |
| `a_target_change_interrupted_by_a_leader_crash_is_resumed` | crash between fence and commit of a retarget (old target answering): resumed, not settled |
| `membership::a_group_that_cannot_finish_its_repair_does_not_stop_the_others` | the Phase 3.10 test now expects the stuck slot rebuilt elsewhere |

## 3. Design, invariants and liveness

ADR-0029 (options A–D compared; D adopted: retarget through the fenced
epoch intent, scheduling from a per-group status record). Invariants
R1–R6, L1–L3 there.

Deterministic simulator (`crates/controller/tests/repair_liveness_sim.rs`):
12 nodes, three groups (2+1 on even seeds, 2+2 on odd ones), one with a
chunk no member can rebuild; 400 passes of random faults (nodes down,
back, a disk lost, a node refusing writes), then failures stop. Checked on
every step: S1 (`Active` only with every shard), S2 (no change leaves a
chunk with fewer than `k` shards on members and former nodes), S2-strict
(a second-member replacement never does so on members alone), S3 (never a
down node, a node that left the slot, or one excluded by another slot);
after the faults: L1 (repairable groups `Active` within a bound), L2 (the
unrecoverable group retried at most once per its delay). 20,000 seeds pass
(host B, release build). The simulator found two flaws of its own model
on the way (former nodes as sources; an over-strict S2), not of the rules.

## 4. R-106 measurements (host B, release build, synthetic records)

Before (Phase 3.12's code) and after; ADR-0030 has the method.

| Records | Open (before → after) | Memory per record | Compaction | Longest lock wait during compaction (8 threads retiring) | Durable retirements/s during it |
| --- | --- | --- | --- | --- | --- |
| 1e5 | 62 ms → 72 ms | 124 → 108 B | 0.15 s under the lock → 0.16 s off it | whole compaction → 11 ms | ~1,000 → 3,069 |
| 1e6 | 0.66 s → 0.73 s | 124 → 108 B | 1.4 s under the lock → 1.5 s off it | whole compaction → 25 ms | ~1,000 → 2,009 |
| 1e7 | 7.1 s → 8.1 s | 124 → 108 B | 13.6 s under the lock → 14.1 s off it | 13.6 s → 18 ms | ~1,000 → 1,567 |

Group commit without compaction: 994 durable retirements/s with one
thread (one `fdatasync` ≈ 1 ms), 2,425 with 4, 4,106 with 8, 8,016 with
16. A due-only scan of 100k records: ~2 ms. 1e8 records was not measured
(estimated 80 s to open, 11 GB of memory). The O(N) work left and its
limits: ADR-0030 §Remaining O(N) work.

Real-process reclaim load: §8.

## 5. Capacity and admission

Terms and where each comes from: ADR-0030 §Capacity terms. Changes:
`reclaim_retained_bytes` in health; admission counts reclaimed bytes (not
GC's deletes) as room while nodes defer, and counts a node it cannot hear,
or that is damaged, as deferring; a lowered retention keeps the earlier
one for what was retired before (`pending-reclaim.retention`). Tests:
`reclaim.rs` (`a_lowered_retention_never_shortens_what_was_retired_before`,
`retention_history_survives_a_future_retirement_and_a_lowering_to_zero`,
`retained_bytes_follow_retirements`). `write_rejected` and the reserves
are unchanged (node-enforced; `capacity::` suite passes). Not measured on
a full device under load this phase.

## 6. R-94(b)

Unchanged: bounded (Medium), the same residuals as Phase 3.12 §1. What
this phase touches and how it was checked:

- A retarget keeps the old target's shards as a former member's (kept by
  reconciliation and needed for reclaim while `Repairing`), so the
  protection does not narrow (ADR-0029 R6); nodes that left any slot of a
  repair are never chosen as a target for another slot (whose
  reconciliation would treat their shards as misplaced).
- Group commit acknowledges a retirement, a durable role and a kept
  record's end only once synced; the compaction keeps one log in the
  Phase 3.12 format; crash points tested (`reclaim.rs`).
- The Phase 3.12 saved seeds and mutations still run
  (`system_model::recorded_direct_restore_seeds_agree_with_the_model`,
  `deferred_reclaim::*`).

## 7. R-105

`identity_hardening.rs`: a manual close does not count a disk holding
another node's identity, another registration, or none where one was
acknowledged; adopting an identity needs the facts role and the node's
certificate id. Remaining (Low): a node with no identity yet, or of an
earlier release, is counted and listed `identity unverified`.

## 8. Real-process runs (host A, and A with B for rebalancing)

**Repair under faults** (`benchmarks/linux/repair-faults.sh`, final build;
two controllers, eight nodes, two 2+1 groups, PUT/GET/DELETE load
throughout; one run): every scenario passed, every acknowledged object
read back verified.

| Scenario | Result |
| --- | --- |
| S1 target stopped mid-rebuild | rebuilt onto a third node; `Active` 30 s after the target stop (59 s from the first stop) |
| S2 leader killed mid-repair | completed by the other controller, 29 s after the kill |
| S3 stopped nodes return | nothing changed (stale shards ignored) |
| S4 second member stopped (2+1) | `awaiting-nodes` reached (430 s: each attempt first deep-verifies the target's shards, minutes on a large group, R-104 remainder); the needed member never replaced; `Active` 69 s after it returned |
| S5 drain while group 1 waits | the drained node decommissioned; rebalancing not held by group 1 |

PUTs refused while both groups had a member down (a write needs every
member of its group; unchanged from Phase 3.12): 821 errors over 674
iterations of four objects, none acknowledged and lost. Earlier runs of
the script found two scheduling problems, both fixed: a new leader repaired
a healthy member away on last-seen times stale by the failover gap (§11),
and a repair with a member down read it for every chunk.

**Deferred reclamation under load** (`benchmarks/linux/reclaim-load.sh`,
three nodes, one 2+1 group, retention 120 s, skew allowance 5 s, 20 min,
iterations of 16 × 1 MiB PUT, verified GET of the previous iteration,
DELETE of the one before, 2 s apart; one run per release, same s3load):

| | Phase 3.12 | Phase 3.13 |
| --- | --- | --- |
| PUT ops/s, median (IQR) | 157.8 (148.9–164.2) | 157.4 (150.6–165.1) |
| PUT p95 / p99, ms (medians) | 55.7 / 56.9 | 55.6 / 56.5 |
| GET ops/s, median (IQR) | 550.2 (515.8–576.0) | 557.8 (529.4–585.6) |
| GET p95 / p99, ms | 14.6 / 15.2 | 14.2 / 14.8 |
| DELETE ops/s, median (IQR) | 1,831 (1,709–1,938) | 1,842 (1,685–1,940) |
| DELETE p95 / p99, ms | 4.6 / 4.6 | 4.5 / 4.6 |
| Errors / body mismatches | 0 / 0 | 0 / 0 |
| Reclaimed records/s | 17.6 | 17.7 (8.9 MiB/s) |
| Node CPU s (three nodes) | 25.8, 25.6, 19.4 | 23.2, 23.2, 17.0 |
| Node RSS max, MiB | 63–67 | 55–70 |
| Retirement files, max | 286 KiB | 228 KiB |

Every difference is within the runs' spread: no regression, and no
improvement is claimed. One sample per release. Unpaced runs (no sleep
between iterations) filled the nodes' allotments with a retention's
deletes in both releases: admission went to `write_rejected`; the smallest
margin above the reserve seen was about 236 MiB (3.12) and 238 MiB (3.13),
the reserve never crossed (the capacity requirement of R-106).

**Rebalancing** (`benchmarks/linux/rebalance-load.sh`, two hosts, 30 min
per window, a leader kill and a node stop in the second window; one run
per release): both releases passed (no body mismatch, every acknowledged
object read back). Not comparable: with the default one-day retention the
script's 3 GiB allotments filled with retained deletes before any plan, so
neither release moved a byte and nearly every PUT was refused
(`write_rejected`) in both. GET medians: 81.5 and 81.7 ops/s (3.13, 3.12)
in the rebalancing window; 73.8 (IQR 54.9–79.3) and 82.0 (80.7–83.4) in
the first window, a spread too wide to call. Rebalancing on real
processes is **not verified** this phase (the script needs capacity for a
retention's deletes, or a short retention).

## 9. Upgrade and rollback (host A, FoundationDB)

Final build, `fdb-suite.sh` with the CI filters plus `previous_release::`
(binaries of 3.9, 3.10, 3.11 built from their commits, 3.12 from
`cddd078`): 70 of 70 passed; `upgrade_and_rollback_with_the_phase_3_12_release`
three more runs, all passed; the three-process failover and membership
fault suites 7 of 7; `mixed_version::rolling_upgrade_with_two_binaries`
passed after the retention-0 correction (§11).

The 3.12 test covers: retirements of 3.12 read by a 3.13 node, of 3.13
read by a node rolled back to 3.12 (process killed); a repair begun by a
3.13 controller (a repair status left) finished by a 3.12 controller,
the stale status removed by 3.13 back; the retention lowered from 3600 s
to 3 s afterwards keeping the earlier retention for what was retired
before while a new delete is reclaimed. The 3.10/3.11 matrices were
updated for that rule (a node's retirements across a lowering stay
retained).

Not covered: rollback across an `admin restore` with 3.12 (covered for
3.11 by `a_managed_restore_and_a_removed_node_with_a_previous_release_binary`);
a rollback with a `pending-reclaim.prev` left by a crash (documented:
start once on 3.13 first).

## 10. Mutation runs (in process, host B)

Each change reverted alone; the named test must fail.

| Mutation | Caught by |
| --- | --- |
| no retarget | `repair_retarget::a_target_*`, `membership::a_group_that_cannot_finish_*` |
| second member replaced without a whole slot | `a_second_failure_that_leaves_fewer_than_k_shards_waits_for_the_node`; simulator (S2-strict) |
| slot always whole | `a_second_failure_that_leaves_fewer_than_k_shards_waits_for_the_node` |
| interrupted retarget settled, not resumed | `a_target_change_interrupted_by_a_leader_crash_is_resumed` |
| any `Repairing` group holds every drain | `an_unrecoverable_group_does_not_hold_drains_or_background_work` |
| any `Repairing` group holds background work | same |
| durable role found set not waited for | `reclaim::tests::a_concurrent_caller_waits_for_the_append_it_relies_on` |
| retention history pruned by time | `reclaim::tests::retention_history_survives_*` |
| a kept record's end not waited for | `reclaim::tests::a_kept_record_retired_again_after_a_crash_restarts_its_retention` |
| the start-up merge's old order | `reclaim::tests::a_crash_in_the_merge_at_a_start_keeps_ends_and_later_retirements` |
| a disk with another identity counted by a close | `identity_hardening::a_manual_close_counts_only_the_disk_the_metadata_records` |
| identity adoption with the plain health role | `identity_hardening::adopting_an_identity_*` |
| no certificate cross-check | same |

| repair before the term's first health pass | `a_new_leader_repairs_nothing_before_its_first_health_pass` |

Survived: removing the whole-slot guard (or making the slot always whole)
no longer fails any test: the later guards (per-chunk shard count, former
nodes' listings, the parity bound) already keep those replacements safe
on the tested layouts; the guard stays as defence in depth, and the
simulator's early-replacement mutation (S2-strict) still fails. Not
mutated: the former-node listing and down-former guard (review and unit
logic only).

## 11. Independent review

Two models (one of another family), read-only, with re-reviews until no
Critical or High was left. Findings and resolution:

| Finding | Reviewer | Severity | Resolution |
| --- | --- | --- | --- |
| Concurrent durable put acknowledged before the role note was synced | other | High | fixed (pending role tickets) |
| Retarget resume could copy from an untrusted target (Direct) | other | High | fixed; later also for drain/request-derived sources |
| Error after a compaction's rename left appends on the unlinked log | other | High | fixed (fail closed) |
| Retention history pruned with a future-dated retirement | other | High | fixed (never pruned by time) |
| Duplicate delete acknowledged before the retirement was synced | other | Medium | fixed |
| Retention lowered to 0 bypassed earlier protection | other | Medium | fixed: at 0 a record retired before stays retired (a delete of it keeps it); other deletes are immediate again (a first fix kept every delete deferred for good, which CI's mixed-version test caught) |
| Failed seal disabled compaction until restart | other | Medium | fixed |
| Admission credited GC's deletes before deferral was known | other | Medium | fixed |
| A kept record's end not durable: a later delete reused the old retirement time | Claude | High | fixed (keep waits) |
| Second crash in the start-up merge brought ended retirements back | Claude | Medium | fixed (snapshot over `.prev` first) |
| Stale whole-slot flag could replace a needed member | Claude | Medium | fixed (decision only on the walk just run) |
| Second-member target could be a former node of the rebuilt slot | Claude | Medium | fixed (exclusions, carried across slots) |
| Retarget of a gone target without backoff | Claude | Medium | fixed |
| A disk with no identity counted where one was acknowledged | Claude | Medium | fixed |
| Durable writes after a failed sync accepted | other (re-review) | High | fixed (refused until restart, any retention) |
| Duplicate keep acknowledged before the first was synced | other (re-review) | High | fixed |
| First slot's exclusions lost on a second slot's move | other (re-review) | High | fixed (`excluded`) |
| Damaged history at retention 0 deleted at once | other (re-review) | High | fixed |
| `k` guard used heartbeat health, not the walk's answers; then nodes not chunks | both | Medium | fixed (union; per-chunk `chunks_short`) |
| Exclusions lost in a crash between the status write and the intent; untrusted dropped across slots | other (later rounds) | High | fixed |
| A chunk listed by no answering member (only on a former node and a member down) invisible to the walk | other (later round) | High | fixed: former nodes' listings checked before a second-member replacement; refused while members and former nodes down exceed the parity. Residual: a chunk that lost more than `m` shards on members and is listed nowhere — beyond the code's tolerance, the same assumption completion already makes |
| A transient listing failure could send a group to the hourly unrecoverable retry | Claude (final) | Medium | fixed: unrecoverable only after three attempts with every member healthy |
| A record retired before a lowered retention and retired again under another incarnation got the new, shorter retention; a first fix (keeping the older time) broke a later raise; the second left the history entry in memory on a failed write | other (final rounds) | High | fixed: the difference becomes a history entry, written before the retirement, rolled back and fail-closed on a write failure (`a_new_incarnations_retirement_never_ends_before_the_earlier_one`, fails with it removed) |
| One parity: a second failure after a lost first member never replaced automatically | Claude (final) | Medium | documented (ADR-0029 §Second failures) |
| Low items (drain fan-out, no reservation, async lock waits, upgrade with a lowered retention, damaged state at retention 0 refusing deletes) | Claude | Low | documented (risk ledger, ADRs, operations guide) |

Found by the real-process runs, not by review: a new leader's first repair
pass used node last-seen times an earlier leader recorded, stale by the
failover gap; with spares seen recently and members not, a healthy member
was repaired away (then a real failure left the group short). Fixed: no
repair pass before the term's first health pass
(`a_new_leader_repairs_nothing_before_its_first_health_pass`, fails with
the guard removed). Pre-existing since auto repair; with the default
`unhealthy_after` (60 s) it needed a failover gap that long.

## 12. CI and test adaptations

The CI runs at the final commits are in the Phase 3.13 report. Test
changes to existing tests: `membership::a_group_that_cannot_finish_*`
(expects the retarget), the 3.10/3.11 matrices (the lowered-retention
rule), `delete_fence::a_restore_with_every_procedural_safeguard_skipped_loses_nothing`
(a held delete refused because the node adopted an identity since is
counted as not applied: with this phase's timing a controller adopts
identities before the delivery in about one run of three; refused either
way).

## 13. Not verified

- Power loss and physical-disk `fsync` behaviour of the retirement log.
- Clock skew between hosts (single-host runs share a clock).
- 1e8 retired records per node (estimated only).
- A 24-hour run; one sample per release of each real-process run (no
  variance across runs).
- Rebalancing on real processes (§8: the workload filled the allotments
  with retained deletes in both releases).
- Repair liveness at scale: an attempt's time is dominated by the target's
  deep verification (R-104 remainder).
