# Phase 3.11 restore consistency and operational safety verification

Where each result comes from is stated with it:

- **in process**: the in-memory metadata store with real storage-node
  services and gateways in one test process, on a Linux validation VM
  (the developer machine's malware scan blocks test binaries at launch);
- **host A**: a validation VM running a private `fdbserver` (its own
  port and data directory; the host's system FoundationDB untouched),
  with `fdbbackup` / `fdbrestore` where a test says so;
- **hosts A+B**: two validation VMs with real processes over the
  network, FoundationDB on host A;
- **CI**: the repository's CI jobs (Linux, FoundationDB in a container).

Nothing here was run longer than about two hours, on physical storage,
or on more than two hosts (R-79, R-86).

## 1. Outcome

R-94(b) stays **open (High)**. A restore done outside `admin restore`
(a direct `fdbrestore`, a DR switch, a snapshot rollback of the store)
can still apply a delete decided on the metadata before the restore.
What this phase changed, kept apart:

| Measure | What it is | Status |
|---|---|---|
| `admin restore` (ADR-0026 option D) | the supported restore path; protected under the conditions it checks | implemented, verified (§3) |
| Direct `fdbrestore` and other restores | outside the guarantee | **High residual (R-94(b))** |
| Trip wire (ADR-0025) | limits damage once a running leader observes a replaced or locked store | implemented, verified (§2) |
| Rejoin lease (ADR-0024) | bounds how long a node admits deletes without its leader | Phase 3.10, unchanged |
| Option Q (ADR-0026) | history-confirmed retention; candidate for the next phase | **not implemented, not verified**; design audit recorded |

R-100 (node lifecycle and identity, ADR-0027) is fixed in code and
covered by tests (§4); it counts as resolved only with the final CI run
and the independent review (§9).

## 2. Trip wire (ADR-0025)

Tests (`it/delete_fence.rs`, in process; `fdb_restore.rs`, host A):

- a running leader's in-flight deletes are refused once it sees the
  keyspace cleared, the store locked, a takeover, or the guard's own
  refusal; GC, reconciliation, migration cleanup and held batches;
- after a trip on a replaced or locked store, the generation above is
  closed on the nodes, so another controller (or this one restarted) on
  the restored metadata finds the nodes ahead and holds the fence;
- boundary, deliberately not covered: a restore between two
  observations is not seen (6 of 6 shards lost in
  `boundary_a_restore_between_two_observations_is_not_seen`), and a
  leader cut off from the store applies what it sends within its lease
  (3 within, 0 after). These are why R-94(b) stays open.

Real FoundationDB 7.3 with `fdbrestore` (host A): with the trip, the
leader tripped 42 ms after the clear and no delete applied; with the
trip disabled, 6 of 6 shards of the test object were lost.

## 3. `admin restore` (ADR-0026 option D)

`admin restore plan | run | finish | status | abandon`
(`controller::managed_restore`), resumable from a local journal.
Scenario tests: `it/managed_restore.rs` (in process) and
`fdb_restore.rs::admin_restore_with_fdbrestore_resumes_after_a_failed_restore`
(host A, real `fdbrestore`, a failed restore resumed).

Each check below has a test that fails when the check is removed
(mutation runs on a validation host):

- the plan refuses a node not answering, an open finalize, a cluster
  not Active/ReadOnly; a wrong confirmation token changes nothing;
- running controllers or gateways (fresh heartbeats) stop the run before
  anything is restored, before every clear, and on the restored
  metadata before the mark;
- the fence is closed with a hold on every node, each acknowledging
  after its admitted deletes finished; each push carries the node's
  acknowledged identity, and every node's own disk is verified before
  the clear;
- a resume re-checks that the fence is still held (the cluster may have
  been reactivated between runs) and fences again otherwise; any clear
  of a store that is not verifiably the planned one, and every retry
  after the clear, needs every planned node still held on its own disk;
- a per-attempt marker freezes node lifecycle changes between the
  capture of identities and removed nodes and the clear; the journal's
  pending marker is its own; abandon cannot race a run (journal lock)
  and removes only its journal's marker;
- after the restore: the restored metadata must name only this
  cluster's nodes at their addresses; removed nodes stay removed; the
  identities captured before the clear are put back; a mark made by a
  crashed run is adopted, not repeated;
- deletes decided before the restore, delivered in the middle of it or
  after it, apply nowhere (system model and scenario tests).

Assumptions the guarantee states (ADR-0026): no controller or gateway
runs during the procedure (checked by heartbeats, or declared stopped);
nobody changes node fences by hand during it (the holds are verified,
not locked); one restore of a cluster at a time, from one journal;
nodes of this release (earlier-release nodes are restored by the manual
runbook).

Restore scenarios 1 to 17: see the test list in §10. Scenario 18
(mixed-version restore): see §6.

## 4. R-100: node lifecycle and identity (ADR-0027)

`admin node list | status | decommission | unregister | rejoin |
adopt-identity | keep-removed`; tests in `it/node_lifecycle.rs`:

- registration gives a node an identity in the same transaction; a disk
  holding another identity is quarantined, never imported; a damaged
  identity file refuses every delete;
- deletes carry the identity the node acknowledged; a node holding an
  identity refuses others and unstamped ones;
- removal pushes nothing to a disk that is not verifiably the node's,
  re-ties a hold of an earlier incarnation to the current one, and
  needs an acknowledged identity while the node answers
  (`--unreachable` otherwise);
- reconciliation, fence closes and floors skip removed nodes.

Upgrade rule: upgrade every controller before running a leader of this
release (earlier controllers' deletes are refused once nodes hold
identities).

## 5. M2, M3, M4 (R-98)

- M2 epoch change intents: `it/epoch_intent.rs` (fails without the
  resume or settle);
- M3 planned moves keep the target's client reserve:
  `it/capacity.rs::a_planned_move_keeps_the_targets_client_reserve_a_failure_repair_may_not`;
- M4 repair queue pagination: `it/repair_paging.rs` (fails with
  unbounded pages or without the cursor); the queue depth is counted
  every pass while it fits in one page.

## 6. Rebalancing A/B (hosts A+B)

`benchmarks/linux/rebalance-load.sh`, the Phase 3.10 layout (FoundationDB
and four nodes on host A; two nodes, the gateway and the load generator
on host B; two controllers), with a shorter window: 240 objects of 8 MiB
filled into group 1, group 2 added, every node allotted 3 GiB; an 8-minute
baseline window (rebalancing paused), an 8-minute window with the leader
killed and a node stopped for 20 s, a converge phase, then every object
that must exist read back. Four configurations, two rounds, the second in
reverse order (off, 3.9, 3.10, 3.11, then 3.11, 3.10, 3.9, off), 20 min
each; release binaries of `9674894` (3.9), `3f67cb8` (3.10) and `5414fa7`
(3.11; product code unchanged up to the final commit). Results:
`benchmarks/linux/results/phase311-rebalance/<config>-r<round>`, table by
`summarize_rebalance.py`.

Measured (second window, per round r1 / r2):

| Metric | off | 3.9 | 3.10 | 3.11 |
| --- | --- | --- | --- | --- |
| PUT, MiB/s | 47.5 / 52.1 | 26.7 / 39.5 | 44.2 / 34.0 | 50.5 / 43.3 |
| PUT worst p99, ms | 3 265 / 3 568 | 3 517 / 3 863 | 3 057 / 3 568 | 3 845 / 3 830 |
| PUT errors (refused, retried by the load) | 1 732 / 1 541 | 1 441 / 1 236 | 1 389 / 1 940 | 1 554 / 1 611 |
| GET, MiB/s | 651 / 648 | 653 / 644 | 649 / 649 | 647 / 652 |
| GET worst p99, ms | 68 / 338 | 68 / 113 | 359 / 348 | 369 / 333 |
| GET errors | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Seconds in write_rejected or worse | 720 / 592 | 624 / 300 | 555 / 690 | 592 / 608 |
| Moved, MiB | 0 / 0 | 816 / 948 | 192 / 128 | 128 / 192 |
| Spread between nodes at the end | 48.8 % / 50.2 % | 47.0 % / 10.6 % | 59.8 % / 49.8 % | 58.9 % / 54.3 % |
| Reserve violations (samples) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Body mismatches, acknowledged objects missing | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |

Interpretation (not measured):

- Safety: no run lost or corrupted an acknowledged object, no GET
  failed, no node went below its reserve, in any configuration, with
  the leader killed and a node stopped under load.
- 3.11 against 3.10: no regression beyond the spread between rounds
  (PUT 50.5 / 43.3 against 44.2 / 34.0 MiB/s; the same amounts moved).
  3.11 changes nothing on the PUT and GET paths; its delete path carries
  two more fields.
- This configuration is harder than Phase 3.10's (shorter window,
  smaller per-node allotment relative to the fill): every configuration,
  including rebalancing off, spent 5 to 12 minutes in write_rejected, so
  PUT errors here are mostly capacity refusals and are not comparable
  with the Phase 3.10 numbers.
- 3.10 and 3.11 move much less than 3.9 (the capacity-aware stop of
  R-93), and the spread between nodes stays near 50-60 % at the end;
  3.9 evened one round out to 10.6 %. Rebalancing in 3.10/3.11 does not
  close a large spread within this window; recorded as an observation,
  not a fault (the converge phase ended "balanced" by its own
  criterion).
- Two rounds per configuration; differences within the spread between
  rounds are not significant.

## 7. Results pending in this document

- Capacity state × failover with real processes;
- scenario 18 and old-binary rejoin with previous-release binaries.

## 8. Flakes seen

R-77 `parallel_read::population_retried_after_resets_records_each_chunk_once`:
3 of 80 isolated runs failed on a validation host (the test's injected
reset did not fire); neither the test nor the gateway changed in this
phase.
