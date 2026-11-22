# R-110: capacity-pressure retest plan

Status: **version 3** (2026-11-22: stage 7 reads the manifests and the
scrub only after the background passes settled, §7; the scrub's findings
are recorded by class, §8 C5). Version 2 (C1 split into C1a and C1b,
§8.3; stage 4 goes on after a natural recovery, §7) was run once on
2026-11-22: Failed on C5 only, every other criterion Passed (results
report); its scrub ran inside the reclaim retention and counted the
retained records as unexpected shards. Version 1 was run twice on
2026-11-21, both Inconclusive ([results](phase-4.1-r110-retest-results.md));
those runs stay judged under version 1. The three test hosts are
dedicated to arca-lake (corrected 2026-11-22, §4.1); runs are approved
by the maintainer's delegation, recorded in each run's approval file
(§10). A passed run
of this plan would verify the small-allotment step only (§3); it would
not by itself close R-110.

Background: the reclaim-at-capacity run of
[ADR-0033's report](phase-4.1-adr33.md) §4c is incomplete (the load host
stopped answering 24 minutes into the pressure phase; cause Unknown,
[ADR-0034's report](phase-4.1-adr34.md) §1). It left unverified, for
R-110 ([risk register](../architecture/risks.md)): a stable return from
capacity pressure; retained bytes and allocated capacity converging; the
final integrity check of that run; the separate client effects of
reconciliation, reclaim and compaction. It showed that reclaim and GC
progress under write refusal does not by itself return writes: space
returns through compaction, and admission headroom stayed flat
(574 → 573 MB) while the run lasted. Its load answered each refusal with
an immediate new request (about 1,100 refusals per second).

## 1. Criteria, each judged on its own

| # | Criterion | Not evidence of it alone |
| --- | --- | --- |
| C1a | Under write refusal, GC and reclaim make progress on an identified set of deletes (D1) | a DELETE answered 204 |
| C1b | D1 is fully reclaimed within a fixed time of its last delete, whether the refusal held or admission recovered | a DELETE answered 204; space returned |
| C2 | Physical space returns: compaction lowers the segment bytes the admission counts, and the available bytes the nodes report rise accordingly (not only records reclaimed) | GC backlog falling; reclaim counters rising |
| C3 | Admission recovers and writes resume, held over a window (judged as C3a, recovery with probes only, and C3b, sustained writes) | one PUT succeeding |
| C4 | With a constant live set and bounded churn, retained bytes and allocated capacity reach a steady state instead of growing | a level over a few minutes |
| C5 | Data integrity after the run: every acknowledged write readable and correct, every acknowledged delete gone, every unknown outcome resolved to one of its candidate states | metric totals agreeing |

The client effects of reconciliation, reclaim and compaction taken one by
one (the fourth item above) are **not** a criterion of this plan: the
churn is itself client load, and this plan only records client latency
alongside (§8.2); separating them needs its own design.

## 2. The layers and what observes them

Every name below exists in the code at the time of writing
(`crates/common/src/metrics-registry.tsv`, `crates/cli/src/main.rs`);
"additional" marks what has to be added before a run (§9).

| Layer | Observed by | Notes |
| --- | --- | --- |
| Client-visible live objects and bytes | the generator's ledger (acknowledged PUTs minus acknowledged DELETEs, per key); a listing count (`arcabench --workload list --expect-count`) | the ledger with unknown outcomes is additional (§9 P1) |
| Metadata: deletes, GC, orphans | `arca_controller_gc_backlog`, `arca_controller_gc_backlog_bytes`, `arca_controller_gc_backlog_physical_bytes`, `arca_controller_gc_backlog_oldest_age_seconds`; `arca_controller_gc_collected_total`, `arca_controller_shards_deleted_total`, `arca_controller_gc_reclaimed_bytes_total`; `arca_controller_pass_last_seconds{task=gc\|reconcile}`; `arca_controller_gc_skipped_referenced_total` (must stay 0) | orphans found by reconciliation appear in the controller's `reconcile pass deleted=… unreachable=…` log line only: parsed from the log (additional, §9 P3) |
| Nodes: retirement, retention, reclaim | `arca_node_reclaim_pending_records`, `arca_node_reclaim_retained_bytes`, `arca_node_reclaim_retention_seconds`, `arca_node_reclaim_damaged` (must stay 0); `arca_controller_reclaimed_total`, `arca_controller_reclaimed_bytes_total`, `arca_controller_reclaim_refused_total`, `arca_controller_reclaim_errors_total`; `arca_controller_pass_last_seconds{task=reclaim}` | |
| Storage engine: live and dead, compaction | `arca_node_capacity_bytes{kind=allocated\|live\|reclaimable}`, `arca_node_segment_bytes`, `arca_node_live_bytes`, `arca_node_dead_records`, `arca_node_index_entries{kind}`; `arca_node_compaction_backlog_bytes`, `arca_node_compaction_backlog_segments`, `arca_node_compaction_blocked_segments`, `arca_node_compactions_total`, `arca_node_compaction_reclaimed_bytes_total`, `arca_node_compaction_skipped_no_space_total`, `arca_node_compaction_errors_total` | `allocated` is the segment files' bytes |
| Files on the file system | `arca_node_device_usage_bytes{kind=segments\|side_files\|fs_used\|fs_overhead\|delete_reserve}`; per node directory, apparent and allocated bytes (`du`, additional §9 P3); the file system's free bytes (`statvfs`, as the watchdog reads it) | `fs_used` and `fs_overhead` are of the whole file system, shared with other users: not attributable to the test |
| Admission inputs and state | nodes: `arca_node_capacity_bytes{kind=total\|available\|reserved\|repair_floor\|inflight}`, `arca_node_device_free_bytes`, `arca_node_admission_refused_total{class}`; controller: `arca_controller_capacity_bytes{kind}`, `arca_controller_backpressure_state`, `arca_controller_admission_excluded_groups`; gateway: `arca_gateway_admission_state`, `arca_gateway_admission_rejected_total`, `arca_gateway_admission_throttled_total`, `arca_gateway_node_capacity_refusals_total`, `arca_gateway_placement_capacity_failures_total` | |

With an allotment (`--capacity-bytes`) a node reports as available
`min(file system free, allotment − (segment bytes + side files))`
(`crates/storage-node/src/service.rs`, `device_space_with`), and admits a
client write only if it keeps its reserve: the configured or default
reserve plus a delete reserve of `(records + 1024) × 4 KiB`
(`crates/storage-node/src/capacity.rs`). Compaction lowering the segment
bytes is therefore what returns admission headroom inside an allotment.

## 3. What this plan verifies, and what not

The first step bounds every node with a small allotment, so that the
product's admission boundary is reached without filling any file system.

It can verify, at a small live set: write refusal at the admission
boundary; GC and reclaim progress under refusal; compaction returning
segment space inside the allotment; admission recovering through its
states; a steady state under bounded churn; integrity.

It cannot verify: R-110's object scale (10⁶ objects, where the reclaim
pass rate, the reconcile cycle and a delete reserve proportional to the
record count matter, as in §4c); a full file system or `ENOSPC`; file
system allocation behaviour (holes, reserved blocks, other users);
physical device performance; more than one FoundationDB process; the
client effects of each background pass separately.

A second step at a larger object count, with small objects (record-bound,
where the delete reserve dominates), is a separate decision after the
first step has passed and its data have been reviewed (§8). Neither step
alone, nor a pass of the first, is a resolution of R-110.

## 4. Environment

### 4.1 Placement (proposed; to be approved)

The three test hosts of the earlier reports (numbered as in
[ADR-0035's report](phase-4.1-adr35.md) §1 and §7.1). Correction
(2026-11-22): the hosts are dedicated to arca-lake, not shared with other
users, and the single-process system FoundationDB clusters run on hosts 1
and 2 (host 3 has none); the text below as first written said hosts 1
and 3. The watchdog reads the system FoundationDB of hosts 1 and 2. A separate process on the
same host still shares the device, file system, CPU and memory with it.

- **Option 1 (proposed):** every test process on host 2, which runs no
  system FoundationDB: a dedicated FoundationDB (one process, its own
  port and directory), one controller, four storage nodes, one gateway,
  the load generator and an on-host guard (§6.2). The watchdog and the
  driver run on the operator's machine. Hosts 1 and 3 are only read
  (`status minimal` of their system FoundationDB). Cost: one host's CPU
  and memory carry everything; FoundationDB's memory must be capped
  (its memory options are to be confirmed at preflight, §10).
- **Option 2:** the layout of ADR-0033's report (FoundationDB, controller
  and generator on host 1, nodes and gateway on host 2). It shares host
  1's device and file system with a system FoundationDB, and is the
  layout during which host 1 stopped; not proposed.

Ports and directories: a fresh port not used by any earlier run and one
directory tree under the hosts' data mount, named for this run; created
by setup with a marker file and removed by teardown only if the marker is
there (as `benchmarks/linux/adr35-integration.py` does). Retained
directories of earlier runs (the retained cluster's included) are not
read, written or removed.

### 4.2 Capacity budget (host 2, option 1)

Allotments are the product's own limit, so the test's segment files
cannot outgrow them; the budget covers what is outside them.

| Item | Budget | Basis |
| --- | --- | --- |
| Node allotments | 4 × 1 GiB = 4 GiB | `--capacity-bytes 1073741824`; bounds segment files and side files |
| File system overhead of the node files | ≤ 0.2 GiB | to be measured (`du` allocated − apparent) |
| Dedicated FoundationDB data and logs | ≤ 2 GiB | ≤ 20,000 objects of metadata; stop if exceeded |
| Process logs, samples, evidence | ≤ 1 GiB | stop if exceeded |
| Watchdog floor (stop when free falls below) | 3 GiB | the existing watchdog default (`MIN_DISK_GB`) |
| Margin for teardown and evidence copy | 2 GiB | |
| **Free space required at preflight on host 2's data file system** | **≥ 12.2 GiB** | sum; also ≥ 3 GiB free on `/` |

If host 2 has less, the allotments are lowered (4 × 768 MiB lowers the
requirement by 1 GiB) or the step is not run; a different budget is a new
approval. The space other data already occupy (the retained cluster's
included) is counted as used, never reclaimed.

Inside the allotments (proposed settings, §5): segment maximum 64 MiB;
default reserve for a 1 GiB device `max(2 × 64 MiB + 64 MiB, 1 GiB / 50)`
capped at a quarter = 192 MiB; repair floor 80 MiB; delete reserve
`(records + 1024) × 4 KiB` ≈ 20 MiB at the planned record counts. Usable
for client writes ≈ 810 MiB per node; with 2+1 erasure coding
(1.5 × physical) and two groups over four nodes, about 2.1 GiB logical
in total, less where the groups overlap unevenly (admission judges a
group by its fullest member).

Payload: 256 KiB objects, one chunk each (`--chunk-bytes 1048576`,
`--small-threshold 0`). Live set at baseline L0 = 4,000 objects =
1.0 GiB logical (about 1.5 GiB of shards, under half the usable space).

## 5. Settings

Controller, gateway and nodes run with their defaults except:

| Setting | Value (default) | Why; what it limits |
| --- | --- | --- |
| `--capacity-bytes` (nodes) | 1 GiB (file system) | the admission boundary without filling a file system (§3) |
| `--segment-max-bytes` (nodes) | 64 MiB (1 GiB) | compaction needs room for a segment's live bytes; 64 MiB in 1 GiB keeps the ratio of a 1 GiB segment on a 16 GiB device; the flag is documented as for tests |
| `--reclaim-retention-secs` (nodes) | 600 s (86,400 s) | ADR-0033 §4c's value, so that retention, reclaim and compaction all fall inside a run; must exceed twice `--reclaim-skew-secs` (60 s, unchanged), as the CLI enforces; not 0 (deferred reclamation stays on, R-94(b) bounded). The result says nothing of a 24 h retention's space needs |
| `--chunk-bytes` (gateway), `--admission-max-chunk-bytes` (controller) | 1 MiB (8 MiB) | a group is excluded when its fullest member has room for fewer than `--admission-group-min-shards` (64) of the largest shards: 64 × 4 MiB = 256 MiB of a 1 GiB device with the defaults; 64 × 512 KiB = 32 MiB keeps the same rule at this scale |

Unchanged and recorded: GC every 30 s with a 600 s grace, reclaim every
60 s (4 nodes at once), reconcile every 300 s, health every 5 s,
compaction every 60 s (dead fraction 0.5, 64 segments and 512 MiB copied
per pass at most), admission dwell 60 s and horizon 3,600 s, the delete
fence, incarnation checks, deferred reclamation, the delete reserve,
capacity admission and backpressure. No background pass is paused or
spaced out; a run that needs one changed is a different run, reported as
such, and never compared with the defaults' as the same result.

## 6. Load limits, monitoring and stopping

### 6.1 The load

Closed loop, paced, and bounded by the generator itself (§9 P1):

| Limit | Value (proposed) |
| --- | --- |
| Requests (one token bucket: 20 per second, burst 5; at most 25 in any second and 205 in any 10 s) | counting every data-plane HTTP attempt of the test: the generator's operations, their retries, the GET and PUT probes, listings and the final verification all go through the one bucket; `arcabench` issues its requests with its own HTTP client (no SDK-internal retries), so each attempt is one it counts (`http_attempts`) |
| Concurrency | 4 |
| Retries of one request after a 503 | at most 2, backoff 200 ms doubling to 3.2 s with jitter, none started after 10 s from the first refusal |
| New requests after a refusal | the worker waits the refusal backoff before its next operation (growing with consecutive refusals, `--reject-pause on`), and the shared bucket caps all requests |
| Total HTTP attempts per run | 70,000 (planned at most about 51,000 before retries: fill 4,000; the early deletes 1,000; stage 4 at most 5,000 acknowledged PUTs and 3,600 refused attempts (the hold's attempts are not retried); stage 5 at most 6,000 deletes and 360 probe PUTs; churn 10,800; GET probes about 5,000; verification at most 14,400 (the live keys at the end, about 2,000, and every acknowledged delete, at most 12,400); smoke 300; retries at most 2 per refused request elsewhere, bounded by the shared bucket and this cap) |
| Total bytes written per run (logical) | 5 GiB (planned at most 3.6 GiB: fill 1.0, stage 4 at most 1.22, churn 1.32, probes and smoke under 0.1) |
| Live set | at most 8,000 objects (2.0 GiB logical): L0 4,000, less the 1,000 early deletes, plus at most 5,000 new objects in stage 4; L1 = 2,000 objects in stage 6 |
| Run time | 7.5 h in total: the stage maxima of §7 (400 min), transitions (≤ 30 min) and evidence collection (≤ 20 min) |

The limits are the whole run's, the smoke stage's sub-runs included:
`budget.allocate` gives each smoke sub-run a fixed share (1,020 attempts
and 272 MiB of request bodies in all) out of the approved totals and the
main load the rest, refusing before any start a configuration whose
smoke shares would not fit; each load's deadline is what is left of the
run's time, counted from the run's start (a stage switch or a restart
resets neither). Stage 4's fill keeps the live set's last 4 slots free (one
per worker: at most 4,995 new objects, inside the 5,000 and the 8,000
above), so that the hold's and the probes' attempts always have a slot; a
refused or unsent attempt gives its slot back.

A refusing cluster therefore sees at most 25 requests in any second and
20 per second on average over longer intervals, not the concurrency at
refusal latency (ADR-0033 §4c: about 1,100 per second).

Churn rate (stage 6) follows from retention: a deleted shard stays on its
node for the GC grace, a GC interval, the retention, a reclaim interval
and up to two compaction intervals, about 1,400 s. At 1 PUT and 1 DELETE
of 256 KiB per second (0.375 MiB/s of shards) about 0.5 GiB waits in
that pipeline, inside the space left above the stage-6 live set L1
(2,000 objects, about 0.75 GiB of shards, leaving about 2.5 GiB). A
churn that fills the pipeline beyond the free space cannot converge in
this allotment and would test something else.

### 6.2 Monitoring and stop rules

Three layers, any one of which ends the load, and a host-side keeper
that stops the test's services:

1. **The generator** (on host 2) stops at its operation, byte and time
   limits, and is started under `timeout` with the stage's maximum.
2. **An on-host guard** (on host 2, §9 P2) stops the generator by its pid
   file and start time when the watchdog's heartbeat file is older than
   120 s (monitor or SSH lost), when the data file system has less than
   3 GiB free, or when available memory is below 1 GiB. It needs no
   network to act.
3. **The watchdog** (operator's machine, `benchmarks/linux/r110/watchdog.py`,
   §9 P2) every 10 s: the rules below; it writes the run's stop file on
   host 2, which the generator and the guard act on (the guard stops the
   generator's process group, and the services on a resource rule, by
   identity: pid, start time, working directory; never by process name, as
   `benchmarks/linux/watchdog.sh`'s `pkill` does). It signals no process
   itself, and rewrites the heartbeat only after a round in which every
   required check succeeded, so a watchdog that cannot see, or a lost
   connection, stops the generator through the guard within 120 s. It also
   watches the guard's own identity (`guard-lost`). A stop that is not a
   resource danger (a limit, an anomaly such as the dedicated
   FoundationDB down) does not end its watching: it goes on, round after
   round and beating while the resources are fine, through the drain; a
   resource or monitor rule writes an alarm file of its own on host 2
   (whatever stop came first: the first reason is kept as the run's, an
   alarm is never hidden by it).
4. **The sentinel** (on host 2, from setup to teardown,
   `guard.py sentinel`): apart from any load, it watches the data file
   system's free space, the available memory, the watchdog's heartbeat
   and alarms, and on any of them, on a check it cannot make, or on its
   deadline it stops every load (the stop file) and the test's services
   by identity, and records each stop's verdict. A lost driver, ssh or
   watchdog leaves the heartbeat stale: the sentinel stops the services
   within 120 s, with no network. The smoke stage's exercises (a withheld
   heartbeat, a limit) run in sub-runs whose guards reach only their own
   load; a real danger during the smoke stage is the sentinel's. The
   loads are recorded like the services (by their guards), so the
   sentinel's stop reaches them too, and teardown checks them; the
   watchdog watches the sentinel's identity (`sentinel-lost`, a monitor
   rule) as it watches the guard's.

| Rule | Threshold | Status |
| --- | --- | --- |
| Free space on host 2's data file system and `/` | < 3 GiB | existing default |
| Test directories' allocated bytes | > allotments + 2 GiB (FoundationDB) + 1 GiB (logs) | proposed |
| Available memory, any host | < 1 GiB | existing default |
| Swap activity on host 2 | any swap-in or swap-out over 30 s | proposed; whether swap is configured to be confirmed at preflight |
| Kernel OOM or hung-task messages on host 2 | any | proposed; readability of the kernel log to be confirmed at preflight |
| One-minute load per CPU, any host | > 4 | existing default |
| CPU iowait on host 2 | > 50 % for 60 s | proposed |
| A GET probe of one baseline object (1 per 5 s; its own object, put in stage 3 and never deleted by the test; a SlowDown answer counts as a failure) | 3 consecutive failures, or > 5 s | proposed |
| System FoundationDB of hosts 1 and 2 (`status minimal`; corrected, §4.1) | not "available" once | existing |
| The dedicated FoundationDB | not available for 60 s | proposed |
| A host unreachable | 6 consecutive checks (60 s) | existing default; the on-host guard covers host 2 itself |
| Ledger violation, integrity error, `arca_controller_gc_skipped_referenced_total` > 0, `arca_node_reclaim_damaged` = 1, unexpected 5xx other than 503 | any (the counters: the driver stops the run at the first sample showing one, a protection violation) | proposed |
| Operations, bytes, run time | the limits of §6.1 | proposed |

On a stop, in this order: stop the load (generator); if no rule of
resource danger fired, keep the read-only samplers and probes up to
30 minutes to observe the drain; save every log, sample, ledger and the
generator's report; on a resource rule (space, memory, swap, OOM, load,
iowait, system FoundationDB), also stop the test's gateway, controller,
nodes and FoundationDB by their pid files and start times. Never: a
process-name kill, a shared service, a host restart, a change to the
system FoundationDB, a network or mount change. No test data are removed
automatically; teardown is a separate, reviewed step.

## 7. Stages

Each stage starts only when the previous one ended by its exit condition
and the watchdog has not fired; anything else ends the run.

| Stage | Start | Work | Exit | Max |
| --- | --- | --- | --- | --- |
| 1 Preflight (read-only) | the separate approval to connect to the hosts (§10) | §10 | every check recorded and within its requirement | 30 min |
| 2 Smoke | 1 passed | fresh cluster (marker, own port); the watchdog, guard and generator limits exercised (a heartbeat withheld stops a running load; an operation limit ends it); 100 PUT, GET, DELETE; verify | all exercised and stopped as designed; 0 errors | 30 min |
| 3 Fill and baseline | 2 passed | PUT L0 at the limits; then idle, sampling, until the smoke stage's deletes have matured | L0 acknowledged; admission normal; GC backlog 0 and `arca_node_reclaim_pending_records` 0 on every node (no deletion in progress before D1); samples complete | 60 min |
| 4 Pressure | 3 passed | first DELETE an identified set D1 of 1,000 baseline keys (≤ 20/s), then PUT new keys at ≤ 10/s until the gateway's admission state is write rejected, at most 5,000 new objects; then hold the refusal with one PUT attempt per second (refused attempts count against the limits) while admission refuses; v2: a natural recovery ends the hold (nothing more is written), not the stage; the stage then waits, with no load, until D1's chain is over (GC backlog 0, no pending record on any node, at least D1's chain time ≈ 25 min after D1) or C1b's deadline (50 min after D1) | D1's chain over (fresh samples): C1a and C1b judged; not over by C1b's deadline: the run ends (stage 5 cannot take a clean baseline) | 60 min |
| 5 Drain and recovery | 4 exit | no new data except a probe PUT every 10 s (running beside the deletes, under the same budget and ledger; its keys deleted once C2's and C3a's holds are over, apart from C2's deletes, so that stage 6 starts at L1); DELETE every key stage 4 added and 1,000 more baseline keys (live set L1 = 2,000) at ≤ 20/s; observe GC, retention, reclaim, compaction, admission | C2 and C3a judged | 60 min (about 24 min of grace, retention and pass intervals, then the 10 min holds) |
| 6 Steady churn | C3a passed | 1 PUT of a new key and 1 DELETE of the oldest per second for 90 min, the live set within ±5 % of L1 | C3b judged over the whole stage, C4 on its last 60 min | 100 min |
| 7 Integrity and evidence | 6 ended | verify (§8 C5); v3: then wait, with no load, until at least the controller's orphan age and two reconcile intervals (900 + 2 × 300 s) after stage 6's end, the GC backlog 0 and no record pending reclaim on any node (fresh samples), at most until 15 min before the stage's end; then `admin integrity verify-manifests` and a scrub of every group (findings recorded by class); evidence saved; teardown only after review | C5 judged | 60 min |

## 8. Judgement (fixed before the run)

Proposed values; the approved ones are recorded with the run and not
changed after it.

| Criterion | Pass | Window |
| --- | --- | --- |
| C1 (v1, runs of 2026-11-21; replaced by C1a and C1b, §8.3) | counted from the moment the refusal starts (a snapshot of every C1 counter and gauge then; work on D1 finished before it does not count): over the refused interval of stage 4, GC progress after the snapshot (`arca_controller_gc_collected_total` rising from its snapshot value, and D1's generations, still in the GC backlog at the snapshot, collected until it is back to 0; none of D1 left to collect at the snapshot: Inconclusive) and their shards' retired records reclaimed once past their retention (`arca_controller_reclaimed_total` up by D1's record count, 3 shards × 1,000 objects, less any reclaimed before the snapshot, which is recorded; and pending records back to 0); D1's deletes are issued just before the fill, and the fill takes minutes against D1's ≈ 25 min chain, so most of D1's GC and all of its reclaim fall after the snapshot; `arca_controller_reclaim_refused_total` and `arca_controller_reclaim_errors_total` unchanged. The attribution to D1 is conditional: stage 3 ends with no deletion in progress and D1 is stage 4's only delete, but orphans that reconciliation removes (its log counts, §2) and the leftovers of a PUT that failed after writing some shards (`arca_gateway_node_capacity_refusals_total` > 0) add to the same counters; any of them makes the attribution approximate, and the report says so. Admission recovering before D1 matured under refusal: Inconclusive | stage 4, from D1 to the end of the refusal |
| C2 | per node, `allocated − live` (reclaimable) ≤ max(2 segments, 10 % of allocated), and `available` up by at least half the deleted shards' bytes, held for 10 min; `du` within 5 % of segment plus side-file bytes | stage 5, after retention + reclaim interval + 3 compaction intervals |
| C3a | every gateway's admission state 0 and the controller's backpressure state 0, uninterrupted for 10 min; every probe PUT in that window (60) acknowledged | stage 5 |
| C3b | over stage 6: PUTs ≥ 99 % acknowledged, none refused for capacity, admission never above warning | stage 6 |
| C4 | over the last 60 min of stage 6, per node: the least-squares slope of retained bytes and of allocated bytes ≤ 1 % of the allotment per hour, their range ≤ 10 % of the allotment, admission never write rejected | stage 6 |
| C5 | every key acknowledged and not deleted reads back with its exact body; every acknowledged delete (D1, stage 5 and churn, at most 12,400) answers 404; every unknown outcome reads as one of its candidates (recorded which); manifests and scrub 0 findings, every class (v3: when the background passes did not settle in stage 7's time, findings that are all unexpected shards and no more than the records pending reclaim on the nodes just before the scrub are Inconclusive; any other class, or more unexpected shards, Failed) | stage 7 |

Admission states 3 (write rejected) and 4 (recovery only, stricter) both
refuse client writes: both are the refusal for stage 4 and C1.

C1's progress (GC collection, the backlog back to 0, reclaims, pending
records) is judged only on a refusal observed until D1 matured (its chain,
`d1_chain_s` = 1,500 s, after D1's last delete): a refusal cut short
before (a stop, a lost monitor, admission recovering) is Inconclusive, not
Failed for progress it had no time to show; a violation observed before
(`reclaim_refused` or `reclaim_errors` rising) fails C1 all the same, as
any integrity or protection violation fails the run whatever stopped it.

Time relations taken into account: a deleted shard waits the GC grace
(600 s) and a GC interval before its retirement, then the retention
(600 s) and a reclaim interval before it can be reclaimed, then a
compaction pass (60 s, dead fraction 0.5) before its segment space
returns, and admission moves one state per dwell (60 s) after the
health pass (5 s) sees it. A criterion is judged only after its chain
had the time to complete.

Outcomes: **Passed** (every criterion of the stage met); **Failed** (a
criterion evaluated and not met within its window, or any integrity or
safety violation); **Inconclusive** (a stop by the watchdog, the guard or
a limit, a lost monitor, a stage maximum reached before the judgement, or
a precondition not formed, such as no refusal in stage 4); **Not run**
(a stage never started). A later run is a new run: it does not turn an
earlier Failed or Inconclusive into a pass, and the earlier evidence is
kept.

### 8.3 Version 2: C1a and C1b

Why: in both runs of version 1 the space D1's reclaim returned ended the
refusal before D1 matured (the fill stops at the refusal, the hold adds
one refused attempt a second), so version 1's C1, "D1 fully reclaimed
while the refusal persists", could not be observed; another run would
most likely end the same way. Version 2 judges the two things separately.
What version 1's C1 required and version 2 does not: D1's *whole*
reclaim *under a persisting refusal*. That stays unverified (results
report), and no shortening of D1's chain or of any wait is made to fit a
run.

D1's reclaimed records, a lower bound (`judge.d1_reclaimed_bound`), over
an interval: the increase of `arca_controller_reclaimed_total`, less the
increase of the reconciliation log's `deleted=` count (each orphan it
removes is at most one record), less 3 per increase of
`arca_gateway_node_capacity_refusals_total` (a shard write a node refused
for capacity marks at most one aborted object, whose at most 3 shards GC
retires). The reclaims are counted from the samples inside the interval,
the two subtractions from the samples just outside it (the last at or
before its start, the first at or after its end), so that the bound stays
a lower bound at the edges; for C1a the subtractions start at D1's last
delete, not at the refusal's start (writes a node refused for capacity
during the fill are reclaimed during the refusal). Any of the three not sampled over the interval
(gaps above two intervals) or decreasing (a restart or reset): no bound.
Limits of the bound, recorded with each result: a reconciliation pass
that ends early on a store or leadership error writes no log line, so
orphans it removed are not subtracted; aborted writes for reasons other
than capacity, and capacity refusals at a gateway other than the run's
one, are not subtracted (the run has one gateway); for C1b, orphans
reconciliation removed in the minutes between stage 3's end and D1's last
delete.

| | Passed | Failed | Inconclusive |
| --- | --- | --- | --- |
| C1a, over the observed refusal (its start: the first sample of 3 or 4 after the fill began; its end: the first sample below 3, or D1's chain over, or the stage's end) | the refusal lasted at least 600 s; `arca_controller_gc_collected_total` rose; D1's reclaimed records (the bound) reached 10 % of D1's records (300) | `reclaim_refused` or `reclaim_errors` rose (whatever else); or, the refusal lasting past D1's chain (`d1_chain_s` after D1's last delete, when D1's reclaim is due) with the counters sampled, no GC collection, or fewer than 10 % reclaimed even before the bound's subtractions | a refusal under 600 s; a refusal over before D1's chain with too little progress shown (D1's first reclaims cannot come before the GC grace and the retention, 1,200 s after D1); counters not sampled; enough reclaims in the counter but not by the bound (not attributable) |
| C1b, from D1's last delete to the deadline (D1 + 3,000 s, twice D1's chain) | D1's chain seen over by the deadline, at least D1's chain time after D1 (GC backlog 0 and every node's pending records 0 in the samples at that point), and the bound from D1 to two intervals after that point (the controller counts a pass after it went over every node) ≥ D1's records (3,000) | `reclaim_refused` or `reclaim_errors` rose; observed to the deadline without D1's chain over; over after the deadline; over with fewer records reclaimed than D1's (retirements lost) | stopped before the deadline with D1's chain not over; counters (the violation counters included) not sampled; enough in the counter but not by the bound |

The values (600 s, 10 %, 3,000 s, the bound) are fixed before any run of
version 2. Stage 5 starts only after D1's chain is over, from a fresh
baseline (§8.1), so D1's reclaim never counts in C2; C2 to C5 are judged
as in version 1 whatever C1a and C1b gave.

### 8.1 C2 in detail

As `benchmarks/linux/r110/judge.py` computes it:

- **Baseline:** per node, the last `ok` sample of
  `arca_node_capacity_bytes{kind="live"}` and `{kind="available"}` at or
  before stage 5's start (its first delete), at most one sampling
  interval (15 s) earlier. A node without one: Inconclusive.
- **D1 told apart:** D1's chain must be over at the baseline: the
  controller's `arca_controller_gc_backlog` 0 and every node's
  `arca_node_reclaim_pending_records` 0 in their baseline samples.
  Otherwise D1's reclaim would count as stage 5's, and C2 is
  Inconclusive; a missing baseline sample of either is Inconclusive too.
- **Deleted shard bytes:** in total, stage 5's acknowledged deletes
  (from the ledger) × the shard bytes of one object (3 × 128 KiB); the
  sum over the nodes of the live-byte decrease from the baseline to the
  judgement must reach 90 % of it. Per node the placement is not known in
  advance, so a node's deleted shard bytes are bounded from above by its
  own live-byte decrease plus one shard (128 KiB) per probe PUT of stage 5
  acknowledged or unknown before the judgement (a node holds at most one
  shard of an object; probes add live bytes, and adding the bound back
  makes the threshold stricter, never looser).
- **Pass, per node, over the 10 min hold after the judgement time:**
  `available` up from its baseline by at least half the node's bound in
  every sample; reclaimable (`allocated − live`, reported as
  `{kind="reclaimable"}`) ≤ max(2 segments, 10 % of allocated); `du`
  (allocated bytes of the node directory) within 5 % of the segment plus
  side-file bytes the node reports.
- **Partial-PUT leftovers and orphans:** shards of a PUT that a node
  refused after others had stored theirs
  (`arca_gateway_node_capacity_refusals_total` rising during stage 5),
  and orphans that reconciliation removes (its log's `deleted=` count
  rising), also free space. They are not subtracted (their sizes are not
  observed); the result lists them as an approximate attribution, and an
  unsampled reconcile count is listed the same way.
- **Missing data:** any sample the rule above needs (baseline, judgement
  time, any point of the hold, with no gap above two intervals) missing or
  not `ok`: Inconclusive, never a pass and never a value of 0.

### 8.2 Client effects (recorded, not judged)

GET and PUT latency and outcome counts of the probe and the generator,
per stage, next to the background passes' running gauges
(`arca_controller_pass_running_since_seconds`) and compaction counters;
no attribution to a single pass is claimed.

## 9. Prerequisites before an execution approval

Tooling only, each in its own reviewed change; no product change is part
of this plan.

- **P1, the generator:** a request rate limit shared by new requests and
  retries, operation and byte limits, and a per-key outcome ledger
  (acknowledged, refused, unknown, with the candidate states of an
  unknown one) written as it goes, so that C5 can be judged after any
  stop. `arcabench` has today the retry policy, `--reject-pause`, the
  concurrency, the duration and a per-key `--state-file` written at the
  end; `soakload` has a ledger with unknown outcomes but no rate or byte
  limit.
- **P2, the stop layers:** the on-host guard with its heartbeat; the
  watchdog's generator stop by identity, its swap, kernel-log, iowait,
  test-directory and dedicated-FoundationDB checks.
- **P3, sampling:** every metric of §2 every 15 s with the host's clock;
  `du` (apparent and allocated) of each node directory; the reconcile log
  line's counts.
- **P4, the harness:** setup and teardown with a marker and identity
  checks, the stages and their exit conditions, evidence collection on
  any stop (as `adr35-integration.py`).

Implemented in `benchmarks/linux/r110/` (P1 `budget.py`, `ledger.py`,
`load.py`; P2 `guard.py` (the guard and the sentinel), `watchdog.py`,
`proc.py`; P3 `sampler.py`; P4
`driver.py`, `judge.py`, `remote.py`), tested locally without any host.
`driver.py check` is the default and contacts nothing; every other command
needs `--execute` and an approval file naming the sha256 of the run's
configuration, the scope and the time window (§10). The example
configuration there is not approved. What the local tests cannot show
(the tools' own cost on the hosts, the real stop latencies, the real
metric names and values on the binary under test) is confirmed only at
the preflight and the smoke stage of an approved run.

## 10. Before an execution approval: what the approver chooses

- Option 1 or 2 (§4.1); the date and time window (a time when the system
  FoundationDB's users accept a test on a neighbouring host).
- The budget of §4.2 against host 2's free space measured at preflight
  (≥ 12.2 GiB on the data file system), or the lowered allotment.
- The settings of §5 (retention 600 s, segment maximum 64 MiB, chunk
  1 MiB, allotments), the limits of §6.1, the stop rules of §6.2 and the
  judgement values of §8.
- The teardown scope: the run's own directories and processes only.
- The window must contain the run's end (its time and the drain's), not
  only its start: the driver refuses a run that would end after it.

Preflight (read-only; a connection to the hosts, so only after its own
approval, given before the execution approval above): reachability; each host's free
space (data file system and `/`), memory, swap configuration, load; the
system FoundationDB's `status minimal`; the chosen port free and the run
directories absent; the retained cluster's directories unchanged since
the last record; clock offsets; the binary's commit, features
(`--features fdb`, a release build without `fault-injection`) and
sha256; that FoundationDB's memory options, `du`, `iostat` or
`/proc/stat`, and the kernel log are available to the test user. The
test FoundationDB's memory limit is chosen there (`fdb_memory`, above its
cache memory, at most
what leaves the guard's 1 GiB floor free beside it), written in the
configuration (the driver refuses a run with it undetermined; the
preflight reports the most it may be) and
passed as `fdbserver --memory`; the started command line is part of the
evidence. Monitoring sources that may be missing (`optional_checks`:
the kernel log, swap) are declared in the approved configuration, never
made optional during a run.

## 11. Evidence

Clocks: the samples and the ledger are stamped on host 2's clock; the
driver's own marks (a stage's start, a window's start) are converted to
it with the offset measured at each stage's start (the midpoint of an ssh
round trip, recorded with its stage), so a mark's uncertainty is about
half that round trip; the criteria's windows are on host 2's clock and
their coverage is checked on its samples. No accuracy beyond that is
claimed.

One run identifier for every artefact: the commit, the build features,
the binary's sha256, every setting and limit, the approved values of §8,
the operation ledger, the samples, every process log, the watchdog's and
guard's logs, the stage timeline, and the outcome of each criterion with
the reason of any stop. Published data are anonymised as in
`benchmarks/linux/adr35-summary.py` (no addresses or host names); raw
evidence stays outside the repository, named by the run identifier.
