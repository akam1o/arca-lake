# Phase 3.10 recovery safety and autonomous operations verification

Where each result comes from is stated with it:

- **in process**: the in-memory metadata store with real storage-node
  services and gateways in one test process, on a developer machine
  (macOS);
- **host A**: a validation VM running a private `fdbserver` (its own
  port and data directory; the host's system FoundationDB untouched),
  one process or three in separate zones configured `triple`;
- **hosts A+B**: two validation VMs with real processes (storage nodes,
  controllers, gateway, load generator) over the network, FoundationDB
  on host A;
- **CI**: the repository's CI jobs (Linux, FoundationDB in a container).

Nothing here was run longer than about two hours, on physical storage,
or on more than two hosts (R-79, R-86).

## 1. R-90: what remained after Phase 3.9

ADR-0023 made nodes the enforcement point of the delete fence, but it
relied on the restore runbook and on reaching every node:

1. a restore without `admin delete-fence close`;
2. a controller started on the restored metadata before `recovery
   begin`;
3. a node unreachable during that controller's first round.

The controller could then reopen the generation that node still held,
and deletes decided before the restore could apply there. The Phase 3.10
review found three more ways to the same outcome:

- a fresh controller's first pass pushed a restored open record and
  renewed the nodes' leases on it;
- a controller on restored metadata opens `record + 1`, which can be the
  generation the pre-restore cluster opened after the backup;
- a leader that keeps running through the restore matches every check
  of its own fence.

## 2. Rejoin fence and the layers around it (ADR-0024)

| Layer | Where | What it stops | Evidence (all in process unless stated) |
| --- | --- | --- | --- |
| Rejoin lease: deletes only within `--fence-lease-secs` (30 s) of the leader's last push, monotonic clock, memory only | storage node | a node that restarted, was cut off, or whose leader went silent | storage-node `delete_fence::the_open_fence_lapses_without_the_leaders_pushes`, `the_fence_survives_a_node_restart` |
| Opening and renewal need a caller of the node's incarnation | storage node | a controller of another (stale or restored) incarnation | the same tests |
| Opener bound on the node: the same open generation from another opener closes it | storage node | two clusters (one restored) opening the same number | `the_same_generation_from_another_opener_closes_it` |
| The leader pushes, and deletes under, only an open generation it opened | controller | renewal of a former or restored leader's generation | `it/delete_fence::a_restored_open_fence_is_never_renewed_by_a_new_controller` |
| Metadata rollback gate: nodes ahead of the record, or the leader's lineage counter read back lower | controller | a controller on restored metadata; a restore under a running leader | `a_restore_with_every_procedural_safeguard_skipped_loses_nothing`, `a_restore_under_a_running_leader_closes_its_fence` |

Each test fails with its check removed (mutation runs during the phase):

- without the lease, two pre-restore deletes applied;
- without the gate, the controller opened on restored metadata;
- without the "ours" rule, nothing stopped the restored leader's
  renewals (the test was written to the review's schedule);
- without the lineage check, the fence stayed open.

The normal-operation cost is one refused-delete window per node start,
until the leader's next pass (5 s by default).

Accepted residuals (R-94), each needing the runbook not followed:

- a delayed renewal can extend a stale fence by at most one lease;
- a backup restored within one health interval under a running leader
  goes unnoticed.

## 3. Restore

- `recovery begin` collects every node's incarnation sequence and
  schema floor (`delete_fence::restore_floors`). It refuses while a node
  neither answers nor is declared absent.
- It raises the restored schema and `schema_write` to the highest floor,
  then marks the new incarnation above every node's sequence.
- Tests (in process):
  - `a_clock_going_back_between_restores_does_not_reorder_incarnations`;
  - `a_finalized_upgrade_is_not_undone_by_a_rollback_or_a_restore`;
  - the system model, whose restore step goes through `restore_floors`.

## 4. Upgrade and rollback

Gate: metadata schema 3, the node schema floor, protocol 4.

- Nodes that hold schema 3 refuse protocol-3 deletes.
- The finalize waits until every node that is not decommissioned holds
  the floor.
- The floors survive a node rollback to Phase 3.9 in
  `cluster-floors.txt`.

`it/previous_release::upgrade_from_and_rollback_to_the_previous_release`
runs real Phase 3.9 binaries (commit `9674894`) and this release's on
FoundationDB, three nodes, a controller and a gateway as separate
processes. It ran on host A and runs in the CI FoundationDB job.

| Step | Configuration | Result |
| --- | --- | --- |
| 1 | node-only upgrade, one node then all; 3.9 controller | the 3.9 controller's deletes applied on 3.10 nodes (GC collected) |
| 2 | controller-only upgrade (gateway 3.9) | GC by the 3.10 controller; PUT/GET by the 3.9 gateway |
| 3 | one node rolled back to 3.9 before finalize | no delete reached it for 6 s; collected once it was current again |
| 4 | every process rolled back before finalize, then upgraded | allowed (RFC-014); reads, writes, GC on 3.9 |
| 5 | finalize; 3.9 controller and gateway started | both refused at start; a node rolled back got no delete |
| 6 | metadata restored from before the finalize; 3.9 controller and gateway started before `recovery begin` | they start (schema 2 admits them); no shard deleted (live shards 21/21/21 unchanged); a delayed protocol-3 delete with the node's open generation (lease 29 s left) refused; after `recovery begin` the 3.9 controller is refused again |

Step 6 with the node-side floor check removed deleted three shards on
every node (21 → 18). That was a dangerous downgrade path (R-88 class),
closed by `d2d0a89`.

The two-binary mixed-version test (`mixed_version`, this tree against
its version-bump build) found that schema 3 stamped record version 3 in
the bump build, which this release could not read. Record versions now
map from the schema (`4ee268f`). The test passes on host A and in CI.

Not covered with real processes:

- an absent node at finalize (in process, `it/upgrade::finalize_waits_until_every_node_holds_the_schema_floor`);
- a crash between the two facts files (unit tests).

## 5. R-93: capacity-aware rebalancing

See §11 for the load comparison. In process:

- `rebalance::rebalancing_yields_to_client_writes_gc_and_tiering`:
  - tiering, a GC backlog, and throttled, write_rejected or
    recovery_only tiers move nothing;
  - warning moves at most half the budget;
  - a target at the placement threshold stops the pass with nothing
    written to it.
- `rebalancing_waits_for_repair_and_stops_at_the_targets_reserve`
  stops at the threshold before the nodes refuse.

Both run on FoundationDB too (`rebalance_fdb`, host A and CI). Without
the tier gate one test fails; without the per-chunk check, two.

## 6. R-92: sequenced incarnations

- Unit test `incarnations_are_ordered_by_sequence_not_by_clock`.
- `it/delete_fence::a_clock_going_back_between_restores_does_not_reorder_incarnations`:
  two restores, the second marked by a clock an hour behind. It fails
  when nodes ignore the sequence.
- A node holding another incarnation at the cluster's sequence is
  reported (`arca_controller_incarnation_conflicts_total`, R-97).

## 7. The system model on FoundationDB

`system_model::random_operation_sequences_agree_with_the_model_on_foundationdb`
runs the in-memory model's operations on real FoundationDB
transactions, all 18 of them:

- client: PUT, DELETE, multipart complete and abort;
- background: GC, reconcile, compaction, shard loss with repair,
  migrations with crashes before and after the switch, cache read and
  eviction;
- control plane: a former leader's GC, snapshot, restore, delayed
  delete delivery, leader change, node restart, phase change.

Invariants hold after every step; failing seeds are saved
(`system-model-failures-fdb.txt`, replay with `ARCA_MODEL_SEED`). No
operation was left out.

| Where | Seeds × steps | Time | Result |
| --- | --- | --- | --- |
| host A, one process | 6 × 60 | 43 s | pass |
| host A, three processes, triple | 185 × 150 (time budget 40 min) | 2 401 s | pass |
| CI FoundationDB job | 3 × 40 (bounded) | — | in the job |

## 8. Background operations audit

A read-only audit of GC, reconcile, repair, rebalance, tiering, cache
eviction, compaction, member moves and admission. It found no path that
deletes a shard live metadata needs: every remote delete goes through the
node's fence, incarnation, epoch and lease checks.

| ID | Finding | Severity | Status |
| --- | --- | --- | --- |
| H1 | GC read only the head of the retired queue; generations deferred there blocked all others | High | fixed (cursor paging); `gc_safety::generations_deferred_at_the_head_of_the_queue_do_not_block_the_rest` |
| H2 | one group's repair error ended every pass for the groups after it | High | fixed (per-group isolation); `membership::a_group_that_cannot_finish_its_repair_does_not_stop_the_others` |
| H3 | an operator's move ran unbudgeted before failed members' repair | High | fixed (failure first, `--planned-move-chunks-per-pass`); `membership::a_failed_member_is_repaired_before_a_planned_move` |
| M1 | tiering's capacity gate failed open at a new leader | Medium | fixed (published record, no durable group in repair) |
| M2 | node epochs raised before the membership commit; a crash leaves nodes ahead | Medium | open (R-98) |
| M3 | a planned move's target check ignores the reserves | Medium | open (R-98) |
| M4 | stripe repair reads its whole queue in one transaction | Medium | open (R-98) |
| L1, L3–L10 | leaks, missing guards, dead helpers (see R-99) | Low | open (R-99) |

Each fixed finding has a test that fails without its fix.

Priority, as implemented: Repair > Client writes > GC and compaction >
Tiering > Rebalance > Prefetch (RFC-008 §8.10).

## 9. Capacity state × failover

`it/capacity_failover.rs`, in process: each admission state (normal,
warning, throttled, write_rejected, recovery_only), reached by real
controllers on capped nodes, under each of seven faults:

- leader kill;
- gateway restart;
- node restart;
- metadata loss (every writing commit fails for 3 s);
- report loss (a node silent past the staleness limit);
- a stale record delivered to the gateway;
- a partition (a node unreachable during a leader change).

Eight invariants are checked around every fault:

1. every acknowledged object reads back whole;
2. no node crosses its reserve;
3. no node holds in-flight capacity after the writes;
4. the published state is never less strict than before the fault;
5. new data is refused from write_rejected up, also by a restarted
   gateway and against a stale record;
6. deletes are admitted in every state;
7. reads are served in every state;
8. once space returns, normal and writes come back.

All 35 combinations pass (7 tests, about 55 s). With the gateway's
record-order check removed, the stale-record case fails.

With real processes:

- FoundationDB process and quorum loss, in the CI three-process job
  (`fdb_failover`);
- the leader kill and node stop under rebalancing load on hosts A+B
  (§11).

Write rejected and recovery only were not reached with real processes.

## 10. Crash recovery and persistence

- Node facts are persisted before they are acknowledged. A failed
  incarnation or format adoption rolls back; a raised schema floor holds
  in memory and is persisted before the next acknowledgement
  (`facts_not_persisted_are_not_acknowledged_by_a_retry`).
- The floors file keeps the incarnation pair across a rollback to 3.9,
  and a damaged or lost one fails closed
  (`the_marks_survive_a_rollback_and_a_damaged_floors_file_fails_closed`).
- A failed write's leftover shard no longer holds a member move
  `Repairing`; a pending write's chunk is never skipped
  (`membership::a_move_is_not_held_up_by_a_failed_writes_leftover_in_the_group`,
  `commit_unknown::a_pending_chunk_without_a_record_may_be_in_any_group`).
- `membership_faults::mem_replace_during_leader_change_and_traffic`
  under eight-way parallel load (in process):
  - Phase 3.9 failed 2/16;
  - Phase 3.10 before the fix failed 5/32;
  - after the fix, 32/32 passed.

## 11. Rebalancing under load (hosts A+B)

`benchmarks/linux/rebalance-load.sh`, the layout of the Phase 3.9 run:

- FoundationDB (private `fdbserver`) and four nodes on host A; two nodes,
  the gateway and the load generator on host B; two controllers, one per
  host.
- Group 1 is filled with 320 objects of 8 MiB, then group 2 is added;
  every node is allotted 3 GiB.
- Load: a 15-minute baseline window with rebalancing paused, then a
  15-minute window with the controller leader killed (restarted 30 s
  later) and a node stopped for 20 s, then a converge phase of up to
  25 minutes, then every object that must exist read back verified.
- Three configurations on the same hosts, one after the other:
  - **off**: Phase 3.10 binaries, rebalancing paused in both windows;
  - **3.9**: Phase 3.9 binaries (`9674894`);
  - **3.10**: Phase 3.10 binaries (`f2dac2d`).
- Results: `benchmarks/linux/results/phase310-rebalance-{off,p39,p310}`,
  summarized with `summarize_rebalance.py`.

Second window (rebalancing on, with the faults):

| Metric | off | 3.9 | 3.10 |
| --- | --- | --- | --- |
| PUT throughput, MiB/s | 143.6 | 108.0 | 202.2 |
| PUT p50 / worst p99, ms | 157 / 1 916 | 113 / 3 731 | 115 / 1 440 |
| PUT errors (503, out of batches of 8) | 40 | 699 | 32 |
| GET throughput, MiB/s | 487 | 626 | 640 |
| GET p50 / worst p99, ms | 55 / 193 | 42 / 363 | 42 / 191 |
| Time in write_rejected or worse (15 s samples) | 0 s | 160 s | 0 s |
| Worst admission state | throttled | write_rejected | throttled |
| Capacity stops (R-93 placement threshold) | — | — | 10 |
| Moved by the end of the window | 0 | 1 152 MiB | 1 252 MiB |
| Capacity spread between nodes at the end (used fraction of usable) | 34.2 % | 10.3 % | 24.9 % |
| Largest GC backlog | 1 024 MiB | 1 000 MiB | 1 024 MiB |
| Converge | — | settled after 212 s | still moving at the end (389 chunks moved during converge; "pass budget spent") |
| Reserve violations (node below its reserve, any sample) | 0 | 0 | 0 |
| Body mismatches, acknowledged objects missing | 0 | 0 | 0 |

How to read it:

- **Errors.** Every PUT error is a 503.
  - Off and 3.10 lose whole 8-object batches only around the injected
    faults (5 and 4 batches).
  - 3.9 loses objects across many batches, because its rebalancing took
    the target groups' room until the tier refused writes for 160 s.
    That is R-93 as seen in Phase 3.9; 3.10 never left throttled.
- **Spread.** 3.10 evens the groups out more slowly. It moved nothing
  while the tier was throttled ("client writes first") and stopped 10
  times at the placement threshold, so it did not finish within the
  windows. 3.9 finished faster, at the cost of the refused writes.
- **Throughput.** The baseline windows, identical in all three (rebalancing
  paused), differ by up to 50 % between runs (PUT 150–223 MiB/s):
  - Throughput differences of that size are not attributable to the
    code. The error and write_rejected rows are.
- **Repetitions.** A first set of runs, whose sampler stopped after its
  first probe (`*-run0`, no capacity rows), agrees: off 24 PUT errors,
  3.9 749.

## 12. Independent review

Two models (A: GPT, B: Claude) reviewed `9674894..HEAD` read-only, and
then every fix again, until a round found no new Critical or High
finding. Severity is the reviewer's.

| Round | Reviewer | Critical | High | Medium | Low | Outcome (commits) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | A | 2 | 3 | 0 | 0 | fixed (`71465d5`, `a5aea95`) |
| 1 | B | 0 | 2 | 3 | 3 | fixed (`71465d5`, `7ebdb6a`) |
| 2 | A | 1 | 3 | 3 | 0 | fixed (`ed5564e`); the lineage window bounded later |
| 2 | B | 0 | 0 | 0 | 5 | fixed (`ed5564e`) |
| 3 | A | 1 | 1 | 0 | 0 | fixed (`a694f8f`); R-94(b) kept High |
| 3 | B | 0 | 0 | 1 | 2 | fixed (`a694f8f`) |
| 4 | A | 2 | 1 | 0 | 0 | fixed (`f2dac2d`: segment format 2) |
| 4 | B | 0 | 0 | 2 | 2 | fixed (`f2dac2d`) |
| 5 | A | 2 | 1 | 0 | 0 | fixed (`7c3dc9c`, `a53aa9d`) |
| 5 | B | 0 | 0 | 0 | 3 | fixed (`a53aa9d`) |
| 6 | A | 1 | 0 | 0 | 0 | fixed (`ab9e934`: decommissioned nodes excluded only explicitly) |
| 6 | B | 0 | 0 | 1 | 1 | fixed (`ab9e934`); the 3.9-CLI registration window recorded (R-100) |
| 7 | A | 0 | 1 | 0 | 0 | migration cleanup across a restore: bounded by lineage checks on rebalance and tiering passes (`0981ccf`); within R-94(b) |

Critical and High findings, with their schedules, were:

- **Restored fence renewal.** A fresh controller pushed a restored open
  record and renewed its lease; and a restored controller could open the
  very generation number the pre-restore cluster had opened. Fixed by
  "ours" only, and by the opener bound on the nodes.
- **The finalize barrier.** The finalize did not wait for every node's
  schema floor, then for absent, registering and decommissioned nodes,
  then for the segment format. Fixed by both barriers over every node
  except explicit exclusions, checking the floor and the newest segment
  format on the device, and by refusing registration past the cut-off.
- **Node rollback.** A rollback erased the floors, and later an old node
  binary ignored every node-side rule. Fixed by the floors file, the led
  flag, and segment format 2, which the old binary refuses to open.
- **Repair.** It skipped a pending write's chunk. Fixed by
  `chunk_may_be_in_group`.
- **Persistence.** Facts were acknowledged without being persisted, and
  a rollover was not retried. Fixed by roll-back, `unpersisted`, and a
  rollover on every push and at start.
- **Lineage.** A restore under a running leader went undetected. The
  counter is now advanced and checked before every GC, reconciliation,
  rebalance and tiering batch, serialized in the process. Bounded but
  open: R-94(b), High by reviewer A.

## 13. Performance (hosts A+B)

The P2 optimizations were not attempted: R-94(b) remains a High
residual, and the plan made performance work conditional on none.
Measured instead is a regression check against Phase 3.9.

The A/B alternated the Phase 3.9 (`9674894`) and Phase 3.10 (`0981ccf`)
release binaries:

- three repetitions each, on the same hosts, right after each other;
- `repeat.sh`, durable 2+1 PUT/GET and multipart PUT, 96 × 16 MiB, four
  clients;
- results in `benchmarks/linux/results/phase310-ab`.

| Indicator | 3.9 (three runs) | median | 3.10 (three runs) | median |
| --- | --- | --- | --- | --- |
| PUT, MiB/s | 188.8, 264.4, 171.9 | 188.8 | 252.0, 324.7, 330.7 | 324.7 |
| multipart PUT, MiB/s | 149.6, 146.7, 270.4 | 149.6 | 151.1, 263.9, 189.6 | 189.6 |
| warm GET, MiB/s | 660.8, 657.2, 693.0 | 660.8 | 668.1, 677.9, 676.5 | 676.5 |
| warm GET p99, ms | 108.4, 118.4, 120.8 | 118.4 | 126.1, 105.7, 106.6 | 106.6 |
| CPU s per GiB, gateway host | 4.7, 5.0, 4.7 | 4.7 | 5.0, 4.6, 4.6 | 4.6 |
| CPU s per GiB, node host | 3.1, 3.1, 1.9 | 3.1 | 3.2, 1.9, 1.8 | 1.9 |
| gateway RSS, MiB | 155.5, 153.3, 119.5 | 153.3 | 122.8, 138.5, 128.6 | 128.6 |

- Phase 3.10 adds nothing per PUT or GET: the fence lease check and the
  floor comparison are per delete, and Health carries a few more fields.
- The spread between repetitions of either binary (up to about ±40 %,
  as in Phase 3.9, R-48) is larger than any difference between them. So
  there is no regression, and no gain either, attributable to the code.

## 14. CI and test runs

CI (`1e3a1a6`): every job green.

- fmt, clippy (pedantic, `-D warnings`), the workspace tests with all
  features;
- the FoundationDB job: suites, the system model, `mixed_version`,
  `previous_release` against `9674894`, and the restore drill;
- the three-process failover job;
- cargo-deny, gitleaks, the artifact scan, DCO, links.

Earlier runs of this phase that failed:

- the version-bump build's protocol-3 test (fixed);
- the two-binary record-version bug (fixed);
- test timing on a fast runner (fixed);
- a self-hosted runner shut down mid-job;
- the libfdb crash at process exit after every test had passed (R-68).

Final runs at `1e3a1a6`:

- **In process (macOS):** the crate suites (protocol 2, storage 50,
  storage-node 36, controller 13, metadata 65, gateway 29, CLI 5) pass;
  testkit `it` 249 passed, with 4 failed: the known macOS TLS flakes
  `request_metrics`, `s3_compat` ×2 and `refused_bodies`, which pass on
  Linux.
- **Host A:**
  - the FoundationDB suites: 115 passed;
  - `previous_release` and `mixed_version`: passed;
  - the three-process FoundationDB system model (185 × 150) passed
    earlier in the phase.
- **Host A, `cargo test --workspace --all-features`** (earlier in the
  phase): 535 passed. The one failure is environmental: the host's
  3.9 GiB `/tmp` tmpfs cannot hold `capacity::new_chunks_prefer_the_group_with_more_room`'s
  allotments; it passes with `TMPDIR` on disk, and fails the same way on
  Phase 3.9.

## Not verified

- Rebalancing, failover and capacity states for longer than the windows
  above; no 24-hour soak.
- Write rejected and recovery only with real processes.
- Physical devices; more than two hosts.
- An absent node at finalize, and crashes between the two node facts
  files, with real processes.
