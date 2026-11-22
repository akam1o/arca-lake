# Phase 4.1: test host recovery, data integrity, and ADR-0034

Status: host investigated (first stop: cause **Unknown**; a second stop
of both hosts: a hypervisor restart, §1b), the dedicated cluster and its
10⁶ objects **Verified**; ADR-0034 implemented and tested; cluster A/B
(§6): **0.007–0.008 FoundationDB commit per GET** with the default
(target ≤ 0.1: met); GET throughput 11–16 % above the base gateway's
but 7–14 % below `--access-records off` (target "within the spread of
off": **not met**; the explanation consistent with the counters, §6.3);
HEAD unchanged.

## 1. The load host's stop (incident of `phase-4.1-adr33.md` §4c)

The host that ran FoundationDB (the dedicated test cluster's and a
shared system instance), the controller and the load generator stopped
answering during the capacity-pressure phase and was restarted by its
operator about 18 hours later. Read-only examination after the restart:

| Evidence | Finding |
| --- | --- |
| Boot history | the previous boot's journal ends at 00:13:32 UTC; the next boot at 18:01:57 UTC |
| Kernel log up to the end | no OOM, hung task, soft or hard lockup, RCU stall, watchdog, panic, I/O or filesystem error, NIC reset or timeout; only routine audit messages |
| Crash records | none (pstore empty, no crash dumps) |
| System accounting (10 min samples, last at 00:10:01) | CPU about 50 % idle, steal 0.06 %; memory 11.5 % used, no swap configured; the data disk 77 % busy at 1.9 ms await; network about 5,500 packets/s each way |
| The system FoundationDB's own traces | metrics every 5 s up to 00:13:31–00:13:33: 6.9 GB memory available, disk queue 0, nothing above routine warnings; both trace files end in zero bytes after their last event (blocks allocated, never written) |
| Restart | the root filesystem clean at boot; the journal and services started normally |

Classification: **Unknown.** Every sampled resource was healthy until the
end, and no layer of the guest recorded a problem; the abrupt end (zero-
filled trace tails, a journal that stops) is consistent with the virtual
machine being frozen, reset or powered off from outside, but the
hypervisor's side was not accessible, so no cause is asserted. Not ruled
out: an event in the last 3.5 minutes, between the last 10-minute sample
and the end, that left no trace. Whether the run's load contributed is
not known.

## 1b. A second stop: both hosts at once (hypervisor restart)

During the second A/B campaign (§6) both test hosts stopped within 16 s
of each other (their journals end mid-session, no shutdown sequence, no
kernel message before) and came back about three hours later. The
operator reports that the hypervisor was restarted. Classification:
**Hypervisor / VM** (an external restart, not the guests). The sampled
resources before it were moderate (CPU 67–82 % idle, steal below 0.1 %);
the load was a read benchmark (GET at concurrency 32). The shared system
FoundationDB was available and healthy after it, with the same data; the
dedicated cluster's files were intact and its processes were not
restarted pending a decision.

## 2. The shared system FoundationDB

*Correction (2026-11-22): the three test hosts are dedicated to
arca-lake; the system FoundationDB is not shared with other users. A
single-process system cluster runs on each of hosts 1 and 2, host 3 has
none. The findings below are unchanged.*

Read-only status after the restart (no command other than `status`):
available and healthy, fully recovered, one process, not degraded, no
cluster or process messages, its data size as before (3.7 MB), one
client connected. During the host's outage it was necessarily
unavailable; who was affected cannot be determined from here. It was
checked again before and after the loads of §4 (available), and before
every run of §6.

## 3. The dedicated test cluster

- Its FoundationDB data and log files were intact (about 800 MB). It was
  started with its existing files and cluster file (no `configure`, no
  initialisation; a copy of the files was taken first); it recovered,
  available and healthy, 270 MB of key-value data.
- arca-lake metadata: cluster `Active`, the same incarnation as before the
  stop; 4 nodes and 8 groups at epoch 1, `Active`; the delete fence
  generation 78, open, from the leader of term 40; the reconcile cursor
  as recorded.
- The storage nodes (on the other host, stopped cleanly after the
  incident) and the gateway were started again with their data (the
  original allotments, not the pressure phase's), without the controller
  until the checks below were done: no deletes ran first. The nodes'
  counters matched the state at the stop (records waiting their
  retention 54,445–66,271 per node; damage 0; quarantine 0; 4.17–4.23 GB
  allocated per node).

## 4. Integrity of the 10⁶ objects (Verified)

The expected set is what the run's records prove written: the 1,000,000
keys under `obj/` of the population runs (every PUT acknowledged, error
rate 0), version 1; no later run wrote under that prefix. Keys of the
churn, probe and reclaim runs (other prefixes, no complete record of
their final state) are left out of any loss verdict.

| Check | Result |
| --- | --- |
| LIST of `obj/` (1,000 pages) | exactly 1,000,000 keys; 0 errors |
| GET of every key, every body checked against its version | 1,000,000 read, 0 errors, 0 mismatches, 0 missing (430 s) |
| HEAD of every key, every length checked | 1,000,000, 0 errors, 0 mismatches (184 s) |
| Scrub of every group | 3,362,806 shards verified in the four replicated groups (the four erasure-coded groups hold none: every object of this data set is below the small-object threshold); missing 0, checksum 0, truncated 0, invalid header 0, stale placement 0, metadata/data mismatch 0, unrecoverable 0; 252,436 unexpected shards (no live generation references them: the churn runs' deleted data, which reconciliation removes) |

Verdict: **Verified** for the population's keys (data, metadata and
shards). The churn runs' final states are not verified (no record of
them).

## 5. ADR-0034: implementation and tests

Implementation: ADR-0034 §12. Tests (deterministic, no host):

| Test | What it shows |
| --- | --- |
| `cluster::access::tests::the_table_is_bounded_and_coalesces` | the table and the batch in flight never exceed the capacity; reads of one generation merge |
| `…a_flush_writes_the_sums_and_nothing_for_a_finished_generation` | sums and the latest time written; no record for a generation without a header |
| `…no_record_survives_or_reappears_after_gc_finished_the_generation` | GET → GC → flush, flush → GC, a pending read after GC, tiering's reset after GC: no record |
| `…an_unknown_outcome_is_never_counted_twice` | applied and unapplied unknown outcomes and commit timeouts: counted uncertain, never sent again, never counted twice |
| `…a_contended_generation_is_split_out_retried_once_then_dropped` | a batch split around a contended generation; the others written; it retried once alone, then dropped and counted |
| `…a_refused_batch_is_not_split_and_returns_once` | a refusal a smaller batch cannot avoid: one failed transaction, back once, then dropped |
| `…a_refusal_stops_the_flush_and_the_back_off_waits_the_interval` | batches taken with a refused one are not sent; the back-off ignores the high watermark |
| `…reads_older_than_the_age_bound_are_dropped_as_stale` | the age bound at the attempt |
| `…two_gateways_add_up_and_the_time_never_goes_back` | two gateways (one clock 4 s ahead): counts add, the time is the latest and never moves back |
| `…a_graceful_stop_flushes_what_is_pending` | pending reads written at a stop; reads after it counted |
| `…a_stop_that_gives_up_on_a_stalled_commit_counts_every_read_once` | a stop whose wait ends during a stalled commit: every read counted exactly once |
| `…the_high_watermark_flushes_at_once_and_a_dropped_table_ends_the_flusher` | the watermark trigger; no flusher outlives its table |
| `…random_interleavings_keep_the_records_within_the_reads` | 40 seeds × 200 steps of reads on two gateways, flushes, GC finishes, unknown outcomes and conflicts: no record of a finished generation, no count above the reads served, every read accounted for |
| `…tiering_decisions_from_the_records_match_the_reads_unless_the_table_overflows` | §7 |
| `tiering::an_object_without_an_access_record_is_not_demoted_before_demote_after` | decision 5 |
| `tiering::gets_are_recorded_exactly_in_few_batched_commits` | through a test cluster: 200 GETs recorded exactly in at most 20 commits |
| `tiering::a_gateway_stop_flushes_pending_reads_and_a_restart_records_anew` | a gateway's stop writes its pending reads; a restarted one records |

Each fix fails a test when removed (checked by removing it): the header
check (three tests fail), no retry after an unknown outcome (two), the
header check of `reset_access` (one), the in-flight batch counted
against the capacity (one), decision 5 (one), splitting only on
contention or size (one), the shutdown's ownership of a held batch
(one).

Unchanged and passing: the deferred-reclamation (R-94(b)), delete-fence,
GC, managed-restore and tiering suites; the write paths (R-112, R-114)
are not changed.

## 6. Cluster A/B (performance)

### 6.1 Campaign 3: the default (batch 128), quiet environment

After the hypervisor restart (§1b) the dedicated cluster was started
again from its files (no initialisation; the same incarnation, delete
fence generation 79 of term 41) and checked: LIST of `obj/` exactly
1,000,000 keys, every body of both A/B data sets (10⁵ × 4 KiB, 10⁴ ×
64 KiB) read and checked, 0 errors. Method as in §6.2, with arms
**base** (`4a1ed48`), **new** (the default, batches of 128) and **off**;
one discarded warm-up pass, then four rounds in rotated order (36 runs;
none had errors, refusals, integrity violations or a saturated
generator; 21–32 operations per run were cancelled at the deadline, the
ones in flight when it ended, not counted as errors). This environment
was quiet: rounds 2–4 of one arm within 2 %; with the first round up to
12 % (it was lower for some series: base GET 4 KiB by 12 %, new by 6 %;
not for off). The medians below include the first round:

| | GET 4 KiB (10⁵) base / new / off | GET 64 KiB (10⁴) base / new / off | HEAD 4 KiB base / new / off |
| --- | --- | --- | --- |
| ops/s | 2,958 / 3,440 / 3,990 | 2,463 / 2,739 / 2,944 | 6,773 / 6,692 / 6,758 |
| p50 ms | 10.7 / 8.4 / 8.0 | 13.0 / 11.2 / 10.8 | 4.7 / 4.8 / 4.8 |
| p95 ms | 14.0 / 15.4 / 9.8 | 16.2 / 16.9 / 13.4 | 5.9 / 6.0 / 5.9 |
| p99 ms | 16.1 / 18.1 / 11.0 | 18.3 / 19.4 / 15.0 | 6.7 / 6.8 / 6.7 |
| FoundationDB commits per request | 0.971–0.974 / **0.0077** / 0.0001 | 0.803–0.812 / **0.0066–0.0068** / 0.0001 | 0 / 0 / 0 |
| FoundationDB reads per request | 7.95 / 7.95 / 6.00 | 7.62 / 7.67 / 6.01 | 5.0 / 5.0 / 5.0 |
| transactions started per request | 2.97 / 2.01 / 2.00 | 2.81 / 2.01 / 2.00 | 1.0 / 1.0 / 1.0 |
| gateway CPU s per 1,000 requests | 0.61 / 0.52 / 0.48 | 0.78 / 0.71 / 0.68 | 0.23 / 0.24 / 0.23 |
| FoundationDB CPU s per 1,000 requests | 0.32 / 0.20 / 0.15 | 0.35 / 0.21 / 0.18 | 0.09 / 0.09 / 0.08 |
| gateway RSS MiB | 63 / 58 / 56 | 69 / 63 / 62 | 69 / 63 / 62 |
| access table at the end | — / empty, 0 dropped, 0 uncertain / — | — / empty, 0 dropped / — | — |

- **Commits per GET ≤ 0.1: met** (0.0066–0.0077; about 120–127 times
  fewer than the base).
- **GET throughput within the spread of `off`: not met.** GET 4 KiB 14 %
  and GET 64 KiB 7 % below `off`, against off's own spread of 1–3 %
  (first round included); 16 % and
  11 % above the base. p99 1–2 ms above the base's, 4–7 ms above
  `off`'s. HEAD (no record in any arm) is the same in all three.
- Memory and queue: the table empty at the end of every run, nothing
  dropped or uncertain; RSS not above the base's.

### 6.2 Campaigns 1 and 2 (batch 512, noisy environment)


Layout of Phase 4.0 (4 storage nodes and one gateway on one host;
FoundationDB, the load generator on the other). Gateway arms on the same
data, nodes and FoundationDB: **base** (the gateway before ADR-0034,
`4a1ed48`), **new** (ADR-0034, batches of 512), **new128** (batches of
128, now the default), **off** (`--access-records off`). Data: 10⁵
objects of 4 KiB; 10⁴ of 64 KiB (not 10⁵: the node allotments did not
leave room for 6.4 GB more without approaching capacity pressure). Each
run: 60 s after 10 s of warm-up, concurrency 32, uniform keys, one read
in ten checked, the generator's bounded retries and pacing; the
controller stopped during the runs (no background commits: 5 commits in
30 s idle), no samplers; arms in rotated order. Commits per GET: the
FoundationDB committed-transaction counter from the end of a 30 s idle
baseline to 3 s after the run (the last flush included), over the
generator's HTTP attempts. Results: `benchmarks/linux/results/phase41-adr34/`.

Validity: no run had errors, refused operations, integrity violations
or a saturated generator (20–32 operations per run were cancelled at
the deadline: those in flight when it ended); capacity was not under pressure (no
writes). The environment was noisy: in the first campaign runs of one
arm differed by up to 2× and HEAD (no access record in any arm) differed
2× between arms, so its throughput is not compared; the second campaign
(GET 4 KiB only) was quieter, and it is the one compared in this
subsection (§6.1 supersedes both). The second
campaign stopped after 15 of 24 runs (the hypervisor restart, §1b): 4
runs per arm (3 for off).

FoundationDB commits per GET (both campaigns, every run):

| Workload | base | new (512) | new128 | off |
| --- | --- | --- | --- | --- |
| GET 4 KiB, 10⁵ objects | 0.97–0.99 | 0.0021–0.0022 | 0.0077–0.0079 | 0.0001 |
| GET 64 KiB, 10⁴ objects | 0.81–0.90 | 0.0019–0.0021 | — | 0.0001 |
| HEAD 4 KiB | 0.0 | 0.0 | — | 0.0001 |

GET 4 KiB, second campaign (medians of 4 runs, 3 for off):

| | base | new (512) | new128 (default) | off |
| --- | --- | --- | --- | --- |
| ops/s | 2,746 | 3,248 | 3,263 | 3,688 |
| p50 / p95 / p99 ms | 11.6 / 15.3 / 17.4 | 8.7 / 22.7 / 36.5 | 8.9 / 16.4 / 19.2 | 8.7 / 11.0 / 12.7 |
| worst latency per second, median (runs 2–4) | 18–19 ms | 44–45 ms | 21–22 ms | 12–14 ms |
| gateway CPU s per 1,000 requests | 0.65 | 0.55 | 0.55 | 0.52 |
| FoundationDB CPU s per 1,000 requests | 0.34 | 0.20 | 0.21 | 0.16 |
| gateway RSS | 61 MiB | 63 MiB | 57 MiB | 55 MiB |
| access table at the end | — | empty, 0 dropped | empty, 0 dropped | — |

- **Commits per GET: target met** with every batch size (0.002 with 512,
  0.008 with 128; about 120–470 times fewer than the base).
- Batches of 512 delayed GETs about once a second (a second's reads
  written as back-to-back batches: 1,024 reads issued at once and a
  512-write commit each): worst latency per second near 45 ms, p99 twice
  the base's. Batches of 128 brought both back to the base's level: the
  default is now 128.
- **Throughput:** the default is about 19 % above the base gateway and
  about 12 % below `off` (runs 2–4: 3,195–3,357 against 3,688–3,874), a
  gap larger than the spread: the target "within the spread of `off`" is
  **not met** (FoundationDB CPU 0.21 against 0.16 s per 1,000
  requests; the explanation in §6.3).
- Measured since in §6.1: GET 64 KiB and HEAD in a quiet environment and
  the new default on 64 KiB. Not measured: two gateways reading one hot
  generation (contention,
  acceptance criterion 6), a FoundationDB stall under load (criterion 3,
  covered by the unit tests only).

### 6.3 The remaining gap to `off`: an explanation consistent with the counters

No arm isolates the flush's reads, so this is an explanation the
counters support, not a measured proof (fields `fdb_reads_per_req`,
`fdb_txns_started_per_req`, `access_reads`, `access_coalesced` in
`ab3-summary.json`). The batched path removed the extra transaction per GET (2.97 → 2.01
transactions started per GET; `off` 2.00) but not the reads: each flush
reads every generation's header and record (two reads), and with
uniform keys over 10⁵ objects 2.8–3.0 % of reads coalesce (16–17 %
over 10⁴), so those two reads are paid almost once per GET (FoundationDB
reads per GET 7.95, the same as the base's, against 6.00 for `off`), as
is one write per generation. FoundationDB, one process here, is the
shared resource: its CPU per GET is 30 % above `off`'s. The header read
is the GC-safety check (no record recreated after GC) and is not
optional. Ways to close the gap, none implemented: ADR-0034 decision 6
(no record where no tiering policy applies); a record kept with atomic
operations (`ADD`/`MAX` on separate keys) so that the record read goes
(a metadata format change); longer intervals, which coalesce more only
for skewed workloads. With a skewed (hot-set) workload the gap is
expected to be smaller (more coalescing); not measured.

## 7. Tiering accuracy (simulated)

Two gateways, 2,000 objects (20 hot, read about once a second through
each; 120 random cold reads a second), ten simulated minutes, a flush
every second; tiering's rules (promotion at 10 reads with the last within
300 s, demotion after 300 s idle) applied to the records and to the true
reads:

| Table capacity | Reads dropped | Decisions differing: hot | cold | Records above the truth |
| --- | --- | --- | --- | --- |
| 65,536 (default) | 0 of 165,640 | 0 / 20 | 0 / 1,980 | 0 |
| 64 (forced overflow) | 86,185 of 165,640 | 0 / 20 | 0 / 1,980 | 0 |

Safety is unaffected either way (moves are fenced migrations; ADR-0034
§6). In this workload no decision changed even with half the reads lost,
because every object is far from the thresholds; objects near a
threshold can change decision under overflow, and that case is not
measured here. The records never exceed the truth (drops only
under-count).

## 8. CI failures during this phase

| Run (commit) | Job, test | Classification | Evidence |
| --- | --- | --- | --- |
| first attempt on the ADR-0034 commit | FoundationDB backend, `previous_release::upgrade_and_rollback_with_the_phase_3_12_release` ("timed out waiting for the current controller's repair and its status", a 60 s wait) | **Unclassified, not reproduced**; no evidence of a regression | the job's second attempt passed, and so did the test in the CI runs of the next two commits; on a test host with a FoundationDB of its own, the test run 10 times alternating the tree before ADR-0034 and the current one: 10 passes (5 each, about 33 s each); the change touches neither the repair path nor the controller outside tiering, which the test does not enable; the CI log has no controller output for that step |
| first attempt on the batch-default commit | FoundationDB failover, the restore drill step | **Environment** | the compiler was killed (`SIGKILL`) while building the test binary; no test ran; the second attempt passed |
| first attempt on the publication commit; a pull-request run on GitHub-hosted runners | format, lint and tests: `cache::population_failure_never_fails_the_get` (10 s wait), then `cache_ec::erasure_coding::population_with_a_member_down_records_nothing` (30 s wait), both "timed out waiting for population failures" | **Test harness: port reuse between parallel tests** (not introduced by these changes) | the counters added to the wait showed, on a later CI failure, two populations retried twice and then **written** (`populated: 2`, no failure): a stopped test node's port, given back to the system, had been taken by a node of another test running in parallel, which accepted the writes (`PutShard` does not check the cluster incarnation). Locally (macOS) the port is not reused and the failures arrive in about 1.3 s. Fixed in the harness: a stopped node's address is held by a listener that accepts and closes connections until the node restarts. The product side is risk R-117 |

A rerun's pass is not counted as a fix: the first row stays open until a
failure is caught with the controller's log (a candidate for a timing
margin on loaded runners).
