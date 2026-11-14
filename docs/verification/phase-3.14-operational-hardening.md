# Phase 3.14 operational hardening verification

Where each result comes from is stated with it:

- **in process**: the in-memory metadata store with real storage-node
  services and gateways in one test process, on a Linux validation VM;
- **host A**: a validation VM (4 vCPU, 7.7 GB, virtual disks) running a
  private `fdbserver` (its own port and data directory on a separate
  virtual disk; the system FoundationDB untouched) and release binaries;
- **host B**: a second validation VM of the same size;
- **A + B**: rebalancing runs with processes on both;
- **CI**: the repository's CI jobs.

All storage was virtual disks. No power-loss or physical-disk test was
run (process kills and injected I/O faults only).

## 1. Outcome

| Item | Status |
| --- | --- |
| Repair target verification (R-104 remainder) | differential verification and parallel rebuild (ADR-0031); measured in process (§2); not measured on real processes |
| Rebalancing A/B | valid: three valid samples per release (3.12, 3.13, 3.14) in condition A; no rebalancing difference claimed; a 3.13 regression of client PUT in the failover window found and fixed (R-107); condition B one sample per release (§6) |
| Capacity reservation | Medium (re-evaluated, ADR-0032); implemented, persistent, conflict-controlled; residuals Low |
| Reconcile / tiering cursors | implemented; reconcile budget default 0 (every pair) after review |
| R-94(b) | **bounded (Medium), unchanged**: no delete, retire or reclaim path changed; no wider guarantee claimed |
| 24 h soak | **Not Verified**: stopped after 14 h 13 min 43 s (12 h 50 min 10 s with faults) by decision; capacity-pressure liveness over that time verified (§8) |

## 2. Repair verification time (ADR-0031)

Breakdown and measurements are in ADR-0031 (host B, in process, three
runs per configuration in rotated order, one configuration per process
for CPU and memory). Summary, 4,000 chunks of 128 KiB, 2+1, budget 64
chunks per attempt (63 attempts):

| | Phase 3.13 behaviour | Cache, sequential | Cache, 8 parallel |
| --- | --- | --- | --- |
| Repair wall time (median of 3) | 51.5 s | 37.7 s | 18.4 s |
| Start-of-attempt verification (summed) | 15.0 s | 0.78 s | 0.73–0.81 s |
| Shards read by verification | 128,992 | 7,968 | 7,968 |
| Shards re-verified (start passes) | 124,992 | 3,968 | 3,968 |
| Metadata transactions | 8,130 | 8,130 | 8,130 |
| Retarget at half, time to `Active` | 47.2 s | 49.1 s | 17.7 s |
| CPU (user+sys of the measurement process, 200 + 1,000 + 4,000 chunks; median) | 131 s | 121 s | 103 s |
| Peak RSS of that process | 58–59 MiB | 58–61 MiB | 63 MiB |

At 200 and 1,000 chunks: 1.62 / 1.61 / 0.87 s and 8.79 / 8.19 / 4.27 s.
Bytes moved are those of the rebuilt shards (identical across
configurations: 64 KiB per chunk written, `k` × 64 KiB read). The write
step varies up to 3× between runs on the virtual disk: the sequential
gain at 200–1,000 chunks is within that noise; the verification and
parallel gains are not. A retarget gains only from parallelism (the new
target is another disk: nothing to reuse).

Mutation runs (each change alone, the named test must fail): key without
epoch (killed), node ignoring the store epoch (killed), completeness made
differential (killed), watermark without in-flight records (killed by
`storage::indexed_below`), controller ignoring `since_applied` (killed),
`>` for `>=` at the watermark (killed by
`the_first_record_after_the_watermark_is_read_again`, added for it).
Survived and kept as documented equivalences: no restart of a pass on a
mid-pass store-epoch change; no cache drop at `Active` (ADR-0031).

## 3. Capacity reservation (ADR-0032)

Severity re-evaluated per scenario in ADR-0032: no sequence loses data or
crosses a reserve (the nodes enforce them on every write); double
booking and lower-priority work taking a waiting repair's room stalled
moves and lengthened reduced redundancy (Medium). Implemented: one
transaction per reservation against every valid reservation of the node,
repair first, validity derived (group state or open epoch intent),
sweep, written-byte accounting, rebalancing per chunk.

Tests (in process; host A): 16 concurrent reservations of 1 GiB on
4 GiB grant exactly 4; validity and repair-first; a second repair is
never started onto the room the first holds, across a leader failover,
and the reservation is released at completion; rebalancing stops before
reserved room; a move under way (intent open) keeps its reservation; a
walk started again releases nothing twice; a later epoch keeps the
reservation. Mutations, all killed: in-transaction check removed, target
choice ignoring reservations, repair counting planned moves, validity
without epoch or state, written ignored, sweep not clearing, rebalancing
ignoring reservations, pending moves not counted, found shards counted,
exact-epoch validity.

## 4. Reconcile and tiering cursors

`("al","bgcursor",name)`; `sweep::next_pairs` with a deterministic
simulator (100 seeds; up to 120 groups × 12 nodes; group and node churn,
1/16 pair failures, failovers mid-batch, stale cursors): no pass over
its budget, no pair that existed through a sweep left out, passes per
sweep bounded. Process tests: a new leader's cursor never goes back
before the sweep wraps; a stale cursor resumes after it; a pair with
undecodable metadata is passed over and counted (review M1); tiering
passes bounded, resumed, an undecodable generation counted once and
passed over. Budgets: reconcile pairs per pass default 0 (every pair:
review M2, rebalancing's settle time assumes a whole sweep per
interval), cursor persisted every 8 pairs; tiering 4,096 generations per
pass, pages of 256. Repair priority unchanged (tiering and rebalancing
still yield to progressing repairs; reconciliation never deletes what a
`Repairing` group may need). Mutations, all killed: reconcile cursor
ignored, tiering cursor ignored, Codec stopping tiering, Codec stopping
reconciliation, sweep never wrapping.

## 5. Repair Medium classification and diagnostics

| Case | Behaviour | Operator view |
| --- | --- | --- |
| One parity, second failure after a lost first member | waits (EC limit: the shards are needed) | `awaiting-nodes`; `admin status` action text |
| More than `m` shards lost | chunks queued for stripe repair as unrecoverable; group stays `Repairing`; nothing deleted for it; other groups go on | `data-unrecoverable`, `arca_controller_repair_groups{reason="data-unrecoverable"}`, "WARNING: possible data loss" in `admin status`; `admin integrity list` |
| Chunk listed by no member | invisible to completeness (beyond the code's tolerance) | not detected by repair; scrub and the recovery pass report missing data |
| Former-shard dependence | sources while they answer (ADR-0029) | status `former` |
| No room | `awaiting-capacity` with room and reserved bytes | reservations listed |

`an_unrecoverable_group_does_not_hold_drains_or_background_work` now also
checks the per-reason metric and that the unrecoverable chunk's object
does not read back as whole.

## 6. Rebalancing A/B (A + B)

`benchmarks/linux/rebalance-load.sh` (artifacts and tables in
`benchmarks/linux/results/phase314-rebalance/`). Every run: FoundationDB
and four durable nodes on A, two nodes, the gateway and the load on B;
group 1 (2+1) filled with `BASE` 8 MiB objects, group 2 added; a 15 min
window with rebalancing paused, then 15 min with it resumed (the
controller leader killed at 5 min, node d1 stopped for 20 s at 10 min),
then up to 20 min without load to settle, then every acknowledged object
read back verified. Same harness, load generator (one `s3load` build),
placement, faults and settings for every release: release builds of
3.12 (`cddd078`), 3.13 (`54c468c`) and this phase. Total load per run:
30 minutes.

**Conditions.**
- **A, convergence**: `RETENTION=0` (nodes delete at once) and 6 GiB
  allotments, `BASE=640`. This is not the default retention: nothing in
  A says how rebalancing behaves with it.
- **B, capacity pressure**: the default retention (one day), 5 GiB
  allotments: the deletes of the load stay retained and the tier fills.

**Validity rules** (`summarize_rebalance.py --condition`), fixed before
the samples. A: initial spread of the nodes' used fractions at least 30
points; at least 512 MiB moved; no capacity constraint (never
`write_rejected`, no capacity stop, every node's headroom above its
reserve at least 512 MiB); settled (`balanced`) within the deadline; no
failure (body mismatch or acknowledged object missing). B: retained
bytes grew, no reserve violation, no failure.

**How the rules and the set-up came to be, and every run discarded.**
Three pilot runs (not samples) fixed the set-up: at 4 GiB allotments the
reserve and compaction lag constrained moves (capacity stops, headroom
428 MiB); at 8 GiB the initial spread was 25 points; 6 GiB with 480
objects gave 29. A first campaign attempt (`attempt1/`, 560 objects)
also required a final spread of at most 15 points: one of five runs met
it; the final spread was dominated by space the node stopped by the
fault had not compacted yet (1.3 GiB reclaimable against 0.5 GiB on the
others), which rebalancing does not act on. The rule was dropped (the
final spread is reported), `BASE` raised to 640 for margin on the
initial spread, and the campaign started again from scratch; the
attempt's runs are kept and not used. The rule "no `SlowDown`" turned
out not to be checked (the load generator prints the HTTP status only):
the gateways' logs show `SlowDown` ("write rate limited", the admission
trend) in the failover window in every release; space never
constrained (headroom at least 1.5 GiB in every valid run, never
`write_rejected`). Discarded samples: 3.14 run 2 (the harness script was
edited while it ran: a shell syntax error ended it), 3.14 run 3
(a capacity stop: a target node did not report its device, the node
stopped by the fault; by the rule invalid), the first 3.12 B run (the
host's disk was smaller than the sum of the allotments: FoundationDB
stalled when 3.12, which counts deferred deletes as room, filled it;
the disk was then cleared and the run repeated).

**Order.** Three rounds 3.12 → 3.13 → 3.14, 3.13 → 3.14 → 3.12,
3.14 → 3.12 → 3.13, then B (3.13, 3.14, 3.12). The first round's
results showed a regression (below); fixed, 3.14 was measured again
(five more runs, two of them invalid as listed): the 3.14 samples were
taken about four hours after the others, under the same set-up.

**Condition A: medians (range) of three valid samples each.**

| | 3.12 | 3.13 | 3.14 before the fix | 3.14 |
| --- | --- | --- | --- | --- |
| PUT MiB/s, first window | 198.4 (198.2–201.3) | 201.1 (198.2–205.5) | 196.8 (194.7–201.5) | 193.2 (192.8–195.2) |
| GET MiB/s, first window | 635.5 (633.6–636.2) | 633.7 (632.2–638.6) | 634.7 (634.4–635.6) | 629.3 (627.5–631.3) |
| PUT p95 / p99 ms (8 MiB), first window | 151.2 / 151.2 | 149.8 / 149.8 | 151.9 / 151.9 | 151.8 / 151.8 |
| GET p95 / p99 ms, first window | 52.7 / 55.8 | 52.7 / 55.8 | 52.6 / 55.6 | 53.3 / 56.3 |
| PUT MiB/s, rebalancing window | 170.4 (163.3–179.2) | 139.8 (128.6–145.1) | 140.8 (140.5–142.6) | 173.5 (169.9–181.7) |
| PUT error rate, rebalancing window | 0.63 % | 3.41 % | 3.02 % | 0.62 % |
| GET MiB/s, rebalancing window | 621.8 | 624.8 | 621.9 | 618.7 |
| GET p95 / p99 ms, rebalancing window | 53.8 / 57.3 | 54.3 / 57.4 | 54.2 / 57.5 | 54.5 / 57.9 |
| GET errors | 0 | 0 | 0 | 0 |
| Moved MiB | 2,368 (2,368–3,276) | 2,624 (2,240–3,536) | 3,400 (3,032–3,724) | 2,544 (2,496–3,096) |
| Rebalance MiB/s (moved / time to the last move) | 2.18 | 2.41 | 2.66 | 2.37 |
| Last move after resume, s | 1,089 | 1,087 | 1,194 | 1,075 |
| Settled after the load, s | 0 (0–454) | 1 (0–487) | 304 (273–548) | 182 (0–394) |
| Spread at resume / at the end, points | 38.2 / 16.1 | 34.7 / 20.6 | 37.5 / 9.8 | 32.0 / 23.0 |
| Repair and rebalancing overlapping, s | 0 | 0 | 0 | 0 |
| `write_rejected`, s; reserve violations | 0; 0 | 0; 0 | 0; 0 | 0; 0 |
| Smallest headroom above the reserve, MiB | 1,824 | 1,671 | 1,676 | 1,860 |
| Largest reclaimable (uncompacted) MiB, all nodes | 5,781 | 5,632 | 5,906 | 5,308 |
| Failures (mismatch, lost acknowledged object) | 0 | 0 | 0 | 0 |

Retained bytes were 0 by construction (retention 0); reserved bytes per
node 1,545 MiB in every run. PUT p95 and p99 coincide because each PUT
iteration is eight objects.

**Finding: a 3.13 regression in the failover window, fixed (R-107).**
PUT errors in the rebalancing window were 0.6 % with 3.12 and 3–3.5 %
with 3.13 and 3.14 before the fix, all `503 SlowDown` from the admission
trend in the 30 s after the leader kill. Cause: since 3.13, GC's deletes
count as room given back only while no node defers deletes, and which
nodes defer was set by the reclaim pass, which a new leader runs at
once, before its fence is open: it failed on every node and counted them
all as deferring, until the next pass a reclaim interval later. Fixed:
deferral from every health pass. After the fix the window's PUT
throughput and error rate are those of 3.12.

**Conclusions.** Valid comparison: yes, three valid samples per release
in condition A. Rebalancing itself: no difference between releases is
claimed (moved bytes, rate and settle times overlap). Client traffic:
no regression of 3.14 against 3.13; against 3.12, the failover window
is back to 3.12's figures; the first window's GET was 1 % lower in the
later 3.14 runs (ranges do not overlap) although the read path did not
change between the 3.14 builds measured before and after: the samples
are hours apart, and no regression is concluded, nor excluded below
about 1 %.

**Condition B (one run per release).** The tier filled during the first
window in every release: rebalancing found nothing to move (`balanced`,
the new data had gone to the roomier group), and every PUT of the
second window was refused with `SlowDown`; no reserve violation, no GET
error, no acknowledged object lost, every read verified.

| | 3.12 | 3.13 | 3.14 before the fix | 3.14 |
| --- | --- | --- | --- | --- |
| PUT MiB/s / error rate, first window | 159.7 / 72 % | 123.7 / 75 % | 78.2 / 74 % | 47.9 / 72 % |
| `write_rejected`, s | 3,740 | 1,575 | 1,365 | 720 |
| Smallest headroom above the reserve, MiB | 164 | 232 | 234 | 254 |
| Retained MiB (largest, all nodes) | not exported | not exported | not exported | 13,640 |
| Moved MiB | 0 | 0 | 0 | 0 |
| Reserve violations; failures | 0; 0 | 0; 0 | 0; 0 | 0; 0 |

3.12 admitted more writes (it counts deferred deletes as room: ADR-0030)
and came closest to the reserve. B shows that capacity pressure stops
client writes safely and observably (`write_rejected`, the reason in the
gateway's answer) with the reserves kept; it does not show rebalancing
moves under pressure (none were needed): Not Verified on real processes
(in process: `rebalance::rebalancing_yields_to_client_writes_gc_and_tiering`,
`rebalancing_leaves_room_reserved_by_a_waiting_repair`). One sample per
release: no variance across runs.

## 7. Restore, upgrade and rollback (host A, FoundationDB, real binaries)

`rollback_residuals_with_the_phase_3_13_and_3_12_releases` (passed;
also in CI's previous-release job from this phase) and
`upgrade_and_rollback_with_the_phase_3_12_release` (passed):

| Case | Result | Class |
| --- | --- | --- |
| 3.12 `admin restore` interleaved with rollback: managed restore (plan, run, finish) with 3.12 and 3.13 nodes, then forward | completed and verified; repair and reclaim afterwards as usual | safe |
| Rollback with `.prev` left: to 3.13 | merged, retirements kept | safe |
| Rollback with `.prev` left: to 3.12 | 0 of 3 retirements known while on 3.12; nothing deleted; merged back on this release | conditional (clean stop first) |
| Old binary on a node with retired shards (3.12 ↔ 3.13 ↔ 3.14) | retirements kept both ways | safe |
| Controller downgrade with a repair status and a reservation left | 3.13 completes the repair; this release sweeps both afterwards | safe |
| Pending reservation at rollback | ignored by 3.13; swept when back | safe (Low residual: ADR-0032) |
| Identity and fence with 3.12, 3.13 and 3.14 nodes under one leader | all acknowledged, fences open, deletes retained on every member | safe |
| Restore, then repair and reclaim | member repaired; a new delete reclaimed at the lowered retention, earlier retirements kept | safe |
| Rollback to before 3.12 with deferred retirements | outside this phase's tests (3.10/3.11 matrices of Phase 3.12/3.13 cover their rules) | unverified here |

## 8. Long-running and crash consistency

`retirement_crash::retirements_survive_sigkill_during_group_commit_and_compaction`
(host A, real processes, FoundationDB): 30 rounds of PUT and DELETE of
120 objects, a member killed with SIGKILL 0–1.5 s after its first
retirement of the round, restarted: no record lost within the retention,
no damaged state, every kept object read back; 14 of the 30 kills landed
while that round's retirements were still being written (14–114 of 120
known after the restart: the rest were not acknowledged and were retried
by GC). None of the kills landed during a log compaction (compactions
take milliseconds at this size); compaction crash steps are covered by
fault injection in process (`reclaim::tests::a_crash_at_either_step_of_a_compaction_reopens_the_same_state`,
`a_crash_in_the_merge_at_a_start_keeps_ends_and_later_retirements`,
`an_unterminated_last_line_is_kept_if_whole_else_cut_with_cache_roles`).

| Property | How covered | Not covered |
| --- | --- | --- |
| Durability after a successful sync | SIGKILL rounds (process death; page cache survives) | power loss |
| Group commit | SIGKILL rounds; `a_concurrent_caller_waits_for_the_append_it_relies_on` | — |
| Crash during compaction | fault injection at each step (in process) | a real kill during one |
| Crash after rename | fault injection (merge stopped after its first rename) | power loss between rename and directory sync |
| Directory sync | code review: every rename followed by `sync_dir` | device behaviour |
| Partial / torn write | torn-line tests (in process) | torn sectors on a device |
| Recovery from `.prev` | in process; real binaries (§7) | — |

### Soak (host A, `benchmarks/linux/soak24.sh`; artifacts in `benchmarks/linux/results/phase314-soak/`)

| Item | Value |
| --- | --- |
| Status of the 24-hour soak | **Not Verified** |
| Actual duration | workload 2026-11-13T11:34:06Z – 2026-11-14T01:47:49Z = **14 h 13 min 43 s** (51,223 s); faults until 00:24:16Z (**12 h 50 min 10 s**), then 1 h 23 min 33 s without faults ending with a full verification; then `finish` (600 s settle, final checks) |
| Reason it was stopped | not needed for the Phase 4 decision; the shared virtual disks of the validation host cannot show production durability (R-79), and with the settings below the workload was mostly refused for capacity, so a longer run would not have tested sustained write load either |
| Verified scope | liveness under long capacity pressure (below) |
| Limitations | 24 hours under sustained write load not verified; see "Not shown" |
| Follow-up | Phase 4: a 24-hour (or longer) soak on dedicated or physical storage with allotments that keep writes flowing (readiness report, priority 1) |

Set-up: one host; FoundationDB of three processes over TLS on a second
virtual disk; four durable nodes and two cache nodes with 3 GiB
allotments each, retention 2 h (skew 60 s); two controllers (auto
repair, tiering, rebalancing on), two gateways; mutual TLS; eight
workers of mixed S3 operations (PUT, versioned PUT, conditional PUT,
multipart, copy, GET, range GET, HEAD, versioned GET, DELETE, versioned
DELETE) with an acknowledged-write ledger verified in full every two
hours; one fault every 15 minutes, round robin: node restart and kill,
gateway kill, leader kill, standby controller restart, FoundationDB
process restart, network delay, blackhole and reset (proxy), shard
corruption, injected read and write EIO, cache node kill, certificate
rotation (51 faults, each type three or four times).

Shown:

| | Result |
| --- | --- |
| Acknowledged-write ledger | 7 full verifications, the last one after the faults stopped; 0 violations (no acknowledged object lost or wrong) |
| Final checks | scrub of every group: no missing, corrupt, truncated, mismatching or unrecoverable shard (the only findings are unreferenced shards: retired records kept for the retention); repair queue empty, nothing unrecoverable or failed; multipart manifests: 0 findings; every group `Active` |
| Capacity pressure | the allotments filled with the retention's deletes 9 minutes in; `write_rejected` 9.6 h of the 14.2 h in 47 episodes: 6 long ones (0.65–2.05 h), each ended by reclaims after the retention, and 41 of 1–5 minutes between `throttled` and `write_rejected` (the state flapped at the boundary) |
| Writes resumed | after every long episode (90,283 writes acknowledged in total, 593,616 refused with `SlowDown`, never a write lost) |
| Read availability | 905,582 reads served, 275 failed (0.03 %): every failure within a minute of a gateway kill (the client used both gateways, without a balancer) or of a network blackhole of a node; reads went on throughout `write_rejected` |
| Deletes | 216,613 served, 63 failed (same fault windows) |
| Reserves | 5,156 node samples (every minute, every node): none below its reserve or its repair floor; the smallest headroom of a durable node above its reserve was 446 MiB (the gateways' admission refused writes first) |
| Background work | GC backlog at most 1.7 GiB (first hour), then 0.1–0.7 GiB per hour; 32 GiB reclaimed; stripe repair queue up to 41 tasks after shard corruption, back to 0 within the hour every time; compaction backlog at most 5 MiB; retained bytes 5.0–5.7 GiB (all nodes) throughout |
| FoundationDB | 860 status samples, none unavailable, 3 not fully recovered (around its process restarts), at least two of three processes always up |
| Not exercised | no member move happened (no node stayed down past `--unhealthy-after-secs` 120), so no repair, retarget or capacity reservation ran (reservations 0 throughout); rebalancing moved nothing (the tier was full) |

Long-run figures (hourly medians; processes restarted by faults keep
their name):

| | First hour | Last hour | Note |
| --- | --- | --- | --- |
| Durable node RSS, MiB | 49–52 | 61–89 | rising, with restarts in between; not separable from state growth in 14 h |
| Durable node heap in use, MiB | 2.9–3.7 | 5.2–6.0 | rising steadily (R-108) |
| Cache node RSS, MiB | 44–51 | 59–86 | |
| Controller RSS, MiB | 38–71 | 39–63 | no trend (leadership moves) |
| Gateway RSS, MiB | 55–66 | 68–72 | flat after the second hour |
| FoundationDB process RSS, MiB | 105–120 | 115–170 | grows with the data and its cache |
| File descriptors | nodes 23–27, others 14–27 | nodes 39–57, others 17–29 | nodes' grow with open segments; no unbounded growth seen |
| GET p95 / p99, ms | 42.6 / 43.9 | 44.3 / 45.3 | +4 % over 14 h |
| PUT p95 / p99, ms (successful writes) | 1,000 / 2,209 | 2,096 / 2,588 | varies with how much of the hour was refused; no trend separable |
| FoundationDB commit latency | not sampled | | |
| CPU | not sampled per interval; cumulative after 12.9 h: FoundationDB processes up to 5.5 % of a core, gateways 3.6 %, nodes below 0.5 %, the workload 2.5 % | | the host was mostly idle: the workload was refused |

Not shown: 24 hours under sustained write load; behaviour on physical
disks or production FoundationDB; memory or latency trends beyond 14
hours (the node heap's rise is recorded, not explained: R-108); member
moves, reservations and rebalancing under a long run.

## 9. Observability

New: per-reason repair gauges, longest stall (seconds and group), longest
attempt, reservation gauges and counters, sweep error counters, node
reclaim gauges, verification counters; `admin status` stall times,
operator actions, reservations, sweep cursors. Existing metrics answer
the rest (retargets, backlogs, admission, retained bytes in health,
fence state). The metric registry test checks every new family against
the processes' output.

## 10. Independent review

Two reviewers (one of another model family), read-only, with a
re-review after the fixes:

| Finding | Reviewer | Severity | Resolution |
| --- | --- | --- | --- |
| A reservation written before its membership change commits did not count for a concurrent move | other | Medium | fixed: open epoch intents make it count (`a_move_under_way_keeps_its_reservation`) |
| Shards found present counted as written again after a cursor reset | other | Medium | fixed: only fresh writes count (`a_walk_started_again_releases_nothing_twice`) |
| Rebalancing used a per-pass reservation snapshot | other | Medium | fixed: read per chunk; one in-flight chunk documented |
| An undecodable pair stopped reconciliation forever | Claude | Medium | fixed (`a_pair_with_undecodable_metadata_does_not_stop_reconciliation`) |
| A reconcile budget breaks rebalancing's settle timing | Claude | Medium | default 0; documented |
| A later epoch dropped a running move's reservation | Claude | Low/Medium | fixed (`a_later_epoch_of_the_group_keeps_the_reservation`) |
| Unbounded verification restarts | Claude | Low | fixed (three, then a target error) |
| Tiering cursor lost on error; a full target starving the sweep | both | Low/Medium | fixed (persisted before the failing generation; a full target passed over) |
| Reserve-to-intent window; failed attempt's writes not subtracted; rewritten shard counted twice; rollback can wake an old reservation; one rebalancing chunk in flight | both | Low | documented (ADR-0032) |
| R-107 fix (deferral from health): a node lowered to retention 0 that still keeps earlier retirements counted as not deferring, so GC's deletes of such records could be credited as room (over-admission; the node's reserves still enforced) | other (review of the fix) | Medium | fixed: a node counts as not deferring only at retention 0 with no retirement left (`a_node_lowered_to_zero_with_retirements_left_still_defers`, fails without the check) |

No Critical or High was found. Re-reviews: the fixes verified; the
review of the R-107 fix found the Medium above, fixed after it (the A/B
samples of 3.14 ran with retention 0, where nodes keep no retirement:
the fix does not change them).

## 11. Completion conditions X1–X16

| ID | Condition | Status | Evidence | Not verified |
| --- | --- | --- | --- | --- |
| X1 | Repair verification bottleneck analysed | Verified with limitations | step breakdown (ADR-0031), in process, three runs | real processes, large shards |
| X2 | Safe verification reuse | Verified | ADR-0031 guarantees, 4 + 1 tests, 6 of 8 mutants killed (2 survived: documented equivalences), two reviews | — |
| X3 | Repair time reduction measured | Verified with limitations | 51.5 → 18.4 s (4,000 chunks), verify 15.0 → 0.78 s, retarget 47.2 → 17.7 s; three runs each, rotated | in process only; sequential gain within the disk's noise at small sizes |
| X4 | Valid rebalancing A/B | Verified with limitations | three valid samples per release, rules fixed before the samples, discarded runs listed (§6) | the 3.14 samples taken hours after the others; no rebalancing difference claimed; the `SlowDown` rule was not effective |
| X5 | Liveness under capacity pressure | Verified with limitations | condition B (one run per release), the soak's 9.6 h of `write_rejected` with resumption, reserves kept; in-process reservation and rebalancing tests | rebalancing and repair moves under pressure on real processes |
| X6 | Reservation safety and persistence | Verified with limitations | ADR-0032; 7 tests, 11 mutants killed; FoundationDB suite (`rebalance_fdb::rebalancing_leaves_room_reserved_by_a_waiting_repair`) | real processes (the soak made no member move); Low residuals listed |
| X7 | Reservation restored after a leader failover | Verified with limitations | `a_second_repair_is_not_started_onto_room_the_first_holds` (in process, failover in the middle); rollback test (3.13 controller and back) on FoundationDB | real-process failover with reservations |
| X8 | Reconcile and tiering cursors | Verified | simulator (100 seeds), 4 process tests, 5 mutants killed, review fixes | reconcile budget other than 0 under rebalancing (default 0) |
| X9 | Repair Medium classified, operator means | Verified | §5; metric and `admin status` actions; test extended | chunks listed by no member stay invisible to repair (beyond tolerance) |
| X10 | Restore / rollback residuals | Verified with limitations | §7 on FoundationDB with 3.12 and 3.13 binaries; CI previous-release 6 of 6 | rollback below 3.12 with this phase's state; `.prev` + 3.12 conditional |
| X11 | R-94(b) guarantee kept | Verified | no delete, retire or reclaim path changed; existing saved seeds, deferred-reclaim, restore and previous-release suites pass (§12) | no wider guarantee claimed |
| X12 | Long-running / crash consistency | Verified with limitations | 30 SIGKILL rounds (real processes); the 14 h soak | **24 h soak Not Verified**; power loss and physical disks Not Verified |
| X13 | Observability | Verified | §9; registry test | alert thresholds not tuned |
| X14 | Independent review and mutations | Verified | two reviewers with re-reviews, no Critical or High; 24 mutants killed, 2 survived as documented equivalences | — |
| X15 | Final CI green | see §12 | | |
| X16 | Phase 4 readiness assessment | Verified | `phase-4-readiness.md` | — |

## 12. Gates

Phase 3.14 commits `cd0a51b` and `25d93d6`, CI run with every job green
on its second attempt (the first failed only the known flaky test R-77
`parallel_read::population_retried_after_resets_records_each_chunk_once`,
704 of 705 passed); the final commit's CI is in the Phase 3.14 report.
Locally at the final code: `cargo fmt --check`; `cargo clippy --workspace
--all-targets --all-features -- -D warnings` (host B) clean; workspace
tests 617 passed, 0 failed, 8 ignored (host B); FoundationDB suites on
host A: the CI filters plus the system model, `previous_release` with
3.12 and 3.13, `retirement_crash` (20 rounds), 69 of 69; `fdb_restore`
with a backup agent 4 of 4.
The three-process failover suite needs the CI's container harness: it
ran in CI only. Secrets and identifiers: `sanitize.py --check` with a
deny list of this environment's identifiers, 0 findings.

## 13. Phase 4 decision

| Condition | Met? | Basis |
| --- | --- | --- |
| 1. No open Critical or High | yes | risk ledger: none open; reviews: none found |
| 2. Main failure paths of repair, rebalancing and reclaim verified | yes, with the limits stated | in process and FoundationDB suites, real-process runs of Phase 3.13 and this phase, SIGKILL rounds, soak faults |
| 3. No capacity-management defect that breaks safety | yes | reserves node-enforced on every write; reservations; no reserve crossed in any run; R-107 fixed (it over-throttled, never over-admitted) |
| 4. Valid rebalancing comparison | yes | §6 |
| 5. Main Mediums: impact, mitigation, plan stated | yes | R-94(b), R-104 remainder, R-106 with their mitigations; Phase 4 roadmap |
| 6. CI and previous-release tests pass | yes (final commit: Phase 3.14 report) | §12 |
| 7. S3 / performance / scalability gaps listed | yes | `phase-4-readiness.md` |

Phase 4 is recommended. This is **not** a judgement that the system is
ready for production or for a release: the 24 h soak, power loss and
physical disks, scale beyond two hosts, and sustained long write load
are not verified, and R-94(b) stays bounded (Medium).

## 14. Not verified

- Power loss, physical-disk `fsync` and torn sectors (the retirement
  log's durability is shown under SIGKILL and injected faults only).
- The 24-hour soak (stopped at 14 h 13 min; §8); sustained write load
  over a long run; trends beyond 14 hours.
- Repair verification gains on real processes and large shards (the
  measurements are in process).
- Rebalancing moves under capacity pressure on real processes
  (condition B needed none); one B sample per release.
- Capacity reservations on real processes (in process and FoundationDB
  suites only; the soak made no member move).
- Clock skew between hosts; clusters larger than two hosts.
- The FoundationDB failover suite locally (it needs the CI's container
  harness; it passed in CI).
