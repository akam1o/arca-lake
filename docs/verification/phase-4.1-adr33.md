# Phase 4.1 P0-3: background passes at object scale (ADR-0033)

Status: measured. Reconciliation at 10⁶ objects: pass 2,733 s → 146 s
(A/B, 3 runs per arm at 10⁵ and 10⁶); reclaim pass: 2.74 → 0.55 ms per
record, capacity back 492 s → 114 s (3 runs per arm); retained bytes
level for an hour at 10⁶ under sustained deletes (§4b; at a rate below
the reclaim pass's capacity, see §6). The reclaim-at-capacity run (§4c)
is **incomplete**: the load host stopped answering during its pressure
phase; its partial results are recorded, no conclusion is drawn from
them. Code: commits `f632905`, `0304151`.

## 1. Change

ADR-0033 (accepted): reconciliation judges each listing page in one
read-only metadata transaction (512 shards at most, the reads issued at
once on FoundationDB, halved when a batch keeps failing) instead of one
transaction per shard; pairs run 4 at once in slices of 8 (lineage and
cursor before each slice, as every 8 pairs before); the reclaim pass
judges a page's due records together after its observation stamp, 4
nodes at once; rebalancing's settle time covers the measured cycle. The
decision rules, the deletes (one by one, record token, group epoch,
delete fence) and deferred reclamation are unchanged.

## 2. What is verified, and how

Tests (measured):

- `crates/metadata/tests/attribute_batch.rs`: the batched attribution
  answers as `attribute_shard` for every shard, 512 random metadata
  states on memory (all generation states, missing headers, two
  generations under a prefix, chunk records in either group, migration
  intents, undecodable records, prefixes with zero bytes); a deliberately
  wrong rule makes it fail.
- `crates/metadata/tests/fdb.rs`
  `batched_attribution_agrees_with_one_shard_at_a_time_on_foundationdb`:
  the same on a real FoundationDB (64 fixed-seed cases and one batch of
  512 shards), passed on a private test server; CI runs it.
- `crates/controller/src/gc.rs` (unit): a batch the store refuses for its
  size (as a backend error and as `TooLarge`) is split down to what it
  takes, with the same answers; one shard's refusal is returned as
  before. `sweep::progress` against `next_pairs`; the settle estimate.
- Unchanged and passing: R-94(b)'s deferred-reclamation tests, the
  model-based tests and their seeds, `background_cursor`, `delete_fence`,
  `gc_safety`, `managed_restore`, `rebalance`; the whole `it` binary on
  Linux (356 passed, 0 failed).

Code analysis (not tests): why reading the judgment at the batch's read
version, up to one page's deletes before the delete, keeps the delete
rules (ADR-0033, "Why batching keeps the delete rules"); dropping a
slice's running pairs on a store or leadership error is as a crash there.
Two independent reviews, two rounds each: no High finding; the Medium
findings were fixed (settle interval and in-progress sweep, prompt stop
on a fatal pair in any order, batch halving including `TooLarge`, the
FoundationDB differential test, the new-leader estimate). Left as Low,
documented: the settle estimate is linear in pairs, not a bound; one
undecodable record still stops the rest of its pair until the next sweep
(as before).

What these do not show: that no fault sequence beyond the ones tested
leads to a wrong delete; the argument for that is the code analysis and
the reviews.

## 3. Reconciliation A/B (measured)

Layout of Phase 4.0 E7c: 4 storage nodes and one gateway on host B;
FoundationDB (one process), the controller and the generator on host A;
1 KiB objects (replicated, groups of three), 4 groups per kind, 11 GiB
allotments, retention 600 s, `--admission-horizon-secs 300`. One cluster,
populated to 10⁵ and then 10⁶ objects (0 PUT errors, 0 integrity
violations). At each size three controller binaries took turns on the
same data, in rotated order: **base** (the tree before the change,
`fc6433f`), **p4** (ADR-0033, 4 pairs at once) and **p1** (ADR-0033, one
pair at once: batching alone). A controller was replaced only between
passes, so each measured pass is a whole sweep from the first pair. No
client load during the passes (Phase 4.0's 3,566 s at 10⁶ ran alongside
client samples and a scrub; the like-for-like baseline is the base arm
here).

| Objects | Arm | Pass s (3 runs) | Median s | FDB transactions started per pass (median) | FDB reads per pass (median) | Controller CPU s (median) |
| --- | --- | --- | --- | --- | --- | --- |
| 10⁵ | base | 396.6, 427.7, 280.4 | 396.6 | 483,662 | 2.60 M | 327 |
| 10⁵ | p1 | 20.3, 19.2, 18.1 | 19.2 | 14,330 | 1.26 M | 27 |
| 10⁵ | p4 | 30.0, 16.5, 14.2 | 16.5 | 5,255 | 1.20 M | 26 |
| 10⁶ | base | 2,733.2, 2,733.5, 2,732.5 | 2,733 | 3,046,471 | 18.1 M | 1,832 |
| 10⁶ | p1 | 159.8, 160.2, 159.4 | 159.8 | 8,628 | 11.95 M | 220 |
| 10⁶ | p4 | 146.2, 145.7, 145.7 | 145.7 | 8,335 | 12.02 M | 216 |

FoundationDB counters are cluster-wide from `status json` between the
controller's start and the end of its first pass: they include the
controller's other tasks and the gateway's refreshes (small: the base
arm's 3.05 M at 10⁶ is about one per stored shard). Controller CPU is the
whole process from its start.

- At 10⁶ the pass is **18.8 times shorter** (2,733 → 146 s), with about
  **365 times fewer transactions** and a third fewer reads (the same keys
  are read, in batches; the per-transaction reads of the guard and the
  fence are paid once per batch).
- Batching gives almost all of it (p1: 17.1 times); four pairs at once
  add 9 % at 10⁶ and are within the noise at 10⁵ (not analysed further:
  the metadata reads, issued at once within a batch, are the likely
  limit).
- The cycle is now 146 s + the 300 s interval, below the minimum orphan
  age (900 s) and the default settle time (1,500 s); in Phase 4.0 the
  pass alone exceeded both from about 10⁵ objects.
- Linear in stored shards in both arms (10⁵ → 10⁶: base ×6.9, p4 ×8.8;
  the 10⁵ figures carry a larger share of fixed per-pair cost).

## 4. Reclaim pass A/B (measured)

Same cluster at 10⁶ objects, after §3. Per run: 30,000 fresh 1 KiB
objects put and deleted under the ADR-0033 controller with a 60 s GC
grace (GC code unchanged), GC completed (90,000 retired records waiting
on the nodes), the controller stopped past the retention (600 s + skew),
then started as the arm under test (reconciliation held to one pair, so
that only the reclaim pass works); measured: the longest reclaim pass of
that process (the one that judged the records: a new leader's first
pass runs before its delete fence opens and judges nothing) and the time
from the start until the nodes reported no waiting record. Order base,
p4 / p4, base / base, p4.

| Arm | Records | Longest pass s | ms per record | Records gone after the start, s | FDB transactions started | FDB reads |
| --- | --- | --- | --- | --- | --- | --- |
| base | 180,000 ¹ | 492.9 | 2.74 | 548 | 188,767 | 559,545 |
| base | 90,000 | 436.1 | 4.85 | 492 | 98,486 | 289,414 |
| base | 90,000 | 192.6 | 2.14 | 252 | 94,439 | 280,141 |
| p4 | 90,000 | 132.6 | 1.47 | 196 | 4,486 | 100,227 |
| p4 | 90,000 | 49.3 | 0.55 | 114 | 3,135 | 97,152 |
| p4 | 90,000 | 48.9 | 0.54 | 114 | 3,168 | 97,226 |

¹ The first run also had the 90,000 records of an earlier attempt whose
measurement was invalid (it timed the no-op first pass); compared per
record.

Every run reclaimed every waiting record (0 errors, 0 refusals, 0 left).
Median per record: base 2.74 ms, p4 0.55 ms (**about 5 times faster**);
capacity back after the start: median 492 s → 114 s (**about 4 times
sooner**); FoundationDB transactions per record about 1.05 → 0.035. Both
arms vary from run to run (base 2.1–4.9 ms, p4 0.5–1.5 ms per record;
the first p4 run is the slow one); the cause was not analysed. Phase 4.0
estimated about 3–4 ms per record for the base code on the same layout.

## 4b. Retained bytes under sustained deletes (measured)

The same cluster at 10⁶ objects, the ADR-0033 controller with its
defaults (retention 600 s, GC grace 600 s, reclaim every 60 s): 90 min of
PUT and DELETE (half each) over 20,000 further keys of 1 KiB at
concurrency 4 (0 errors, 0 integrity violations); every 30 s the nodes'
retained bytes and waiting records, the GC backlog and the reclaim pass.

- Filling: nothing is retained for the GC grace, then records wait their
  retention; the first wave peaked at 146 MB retained (t ≈ 1,840 s).
- From t = 1,840 s to 5,444 s (60 min): retained bytes 63.5–146.0 MB,
  mean 92.6 MB; first half-hour mean 91.7 MB, second 93.4 MB; linear
  trend −19 MB/h: **level, not growing**. GC backlog 20–32 MB; longest
  reclaim pass 109 s.
- Reclaimed 149 records/s over the hour; the records waiting, about
  90,000 on average, are what that rate times the 600 s retention gives
  (89,400): a record waits its retention and about one reclaim pass.

The rate is set by the workload (concurrency 4), not by the reclaim
pass: it is below the base code's measured capacity too (206–467 records
per second, §4), so this run shows the level at 10⁶ objects, not a
difference between the arms.

## 4c. Reclaim at capacity (incomplete)

**Incomplete. The run did not finish and is not a result of the
criterion.** About 24 minutes into its capacity-pressure phase the host
running FoundationDB, the controller and the load generator stopped
answering (no ping, no ARP resolution, from the client machine and from
the storage host; between 00:13:40 and 00:14:10 UTC, from the last
FoundationDB sample and the gateway's last request). Its cause is not
known (no access to the host's console or hypervisor); whether this
run's load contributed is not known either. After the host's restart
its own records were examined (`phase-4.1-adr34.md` §1): every sampled
resource healthy to the end, no guest-side record of a problem; the cause
is still Unknown. The storage nodes and the
gateway, on the other host, were stopped cleanly afterwards with their
data, logs and metrics kept (outside the repository). The drain phase,
the recovery of admission and the final verify of every key did not run.

Plan: on the 10⁶-object cluster of §3 (after §4b), the ADR-0033
controller with its defaults; PUT/DELETE churn (half each) over 50,000
further 1 KiB keys at concurrency 8, 16 and 32 (20 min each, 11 GiB
allotments); the nodes restarted one at a time on allotments computed to
leave about 15 % usable free (10.45 GB each), then churn at 16 for 30
min; a 20 min drain; a verify of every key. Samples every 15 s: the
nodes' retained bytes, waiting records, capacity and damage, the
controller's GC and reclaim progress and passes, the gateway's admission
state and refusals, FoundationDB's counters. Sanitized aggregates and a
per-minute series: `benchmarks/linux/results/phase41-adr33/capacity-incomplete.json`.
Figures below marked ¹ come from the 15 s samples, the nodes' and the
gateway's logs and the run's settings, kept outside the repository with
the rest of the evidence; the others are in that file.

Measured while the host answered:

| Phase | Reclaimed | GC | Retained bytes | Gateway refusals |
| --- | --- | --- | --- | --- |
| churn c8 (20 min) | 1.4 records/s (records still inside their retention) | 44 generations/s | 0 → 158 MB | 0 |
| churn c16 (20 min) | 213 records/s | 69 generations/s | 158 → 151 MB (max 217) | 0 |
| churn c32 (20 min) | 126 records/s | 80 generations/s | 160 → 168 MB (max 242) | 0 |
| capacity pressure (24 of 30 min) | 107 records/s | 71 generations/s | 241 → 261 MB (max 360) | about 1.59 million |

Capacity pressure phase:

- Usable free 12.7 % → 12.0 % (allotments of 10.45 GB ¹). The gateway's
  state was write rejected in 74 of the 88 samples of 15 s and throttled
  in the other 14; reason
  "no group has room": a group's fullest member had less headroom than
  64 of the largest shards (8 MiB / k = 512 MiB for the replicated
  groups).
- Reclaim and GC kept working (GC backlog 83 → 5 MB; longest reclaim
  pass 592 s, during the node restarts); a stable return of writes was
  not observed.
- The three ways capacity comes back moved differently (per node, mean
  of the four):
  1. *logical release* (records reclaimed, live data): live 0.93 →
     0.86 GB; retained bytes 241 → 261 MB (records still inside their
     600 s retention ¹ are not reclaimable);
  2. *physical return* (compaction): allocated 3.93 → 4.20 GB; each node
     reported about 3 GB "reclaimable" ¹ (garbage in segments awaiting
     compaction); the nodes' logs show compaction running ¹ (segments of
     about 220 MB rewritten, about 50,000 live records each) until the
     host stopped;
  3. *admission headroom* (available − reserve): 574 → 573 MB, between
     567 and 591 MB; available fell (6.50 → 6.24 GB) while the reserve
     fell with the live record count (5.93 → 5.67 GB; the delete reserve
     is proportional to it, `storage-node/src/capacity.rs`), which kept
     the difference flat; the
     fullest members stayed near the 537 MB a group needs.
  Reclaim progressing is therefore not evidence that writes return:
  that needs compaction to return space faster than writes and retained
  records take it, which was not shown.
- The load: the churn ran without retries (`--retries 0`) and without a
  pause after a refusal: each of 16 workers sent its next operation as
  soon as the last was refused (answered in 20–45 µs ¹), about 1,100
  refusals per second over the phase (1.59 million in 24 min) and more
  requests than that. That offered load was set by
  refusal latency, not by the workload; the generator now paces itself
  after a refusal by default (`benchmarks/README.md`), so a rerun is a
  different load and not comparable sample for sample.

Safety, as observed (not a final check): 0 integrity violations in every
churn and probe run that finished; node damage 0; reclaim refusals 0;
855 reclaim errors, all while the storage nodes were being restarted
one at a time (none after).

Client effect (not isolated): GET at concurrency 4 went from 655 ops/s
(p99 25 ms) on the idle cluster to 81 ops/s (p99 224 ms) during churn at
16, PUT at concurrency 2 from 54 to 7 ops/s; the churn is itself client
load, so this is not the effect of reconciliation, reclaim or compaction
alone.

Reconciliation is not a source of pile-up here: a pass starts an
interval after the previous one ended, so passes do not accumulate.

## 5. Not verified

- The settle time on a cluster with rebalancing enabled (this harness
  runs without it; the computation is unit-tested).
- Interference with client traffic while reconciliation runs (passes
  measured on an idle cluster).
- 10⁷ objects and more than one FoundationDB process.
- The sustained rate the reclaim pass absorbs: §4 measures a burst of
  90,000 records (0.55 ms each, about 1,800 per second, against 4 nodes);
  §4b holds the level at 149 per second. A run at or near the capacity,
  where GC (about 330 generations per second in §4's GC stages, not
  measured as a limit) may be the first limit, is open.
- The criterion's hour at the reclaim pass's capacity (§4c incomplete):
  a stable return from capacity pressure; retained bytes and allocated
  capacity converging; the final integrity of every key after it; the
  separate effects of reconciliation, reclaim and compaction on client
  latency. A [retest plan](phase-4.1-r110-retest-plan.md) for the first
  three awaits approval (not run). *Note (2026-11-22): plan version 1 was
  since run twice on 2026-11-21, both runs Inconclusive
  ([results](phase-4.1-r110-retest-results.md)); plan version 2 (C1 split
  into C1a and C1b) is being prepared.* *Note (2026-11-23): versions 2
  and 3 were run on 2026-11-22 ([results](phase-4.1-r110-retest-results-2.md)).*
- The slice-cancellation path of the parallel pairs (a fatal error while
  other pairs of its slice run) is covered by code analysis and the
  reviews, not by a test.

## 6. Completion criteria (Phase 4.1 roadmap, P0-3)

| Criterion | State |
| --- | --- |
| Reconciliation cycle consistent with the minimum orphan age and the settle time | Met (measured): at 10⁶ the cycle is 146 s + 300 s interval, inside 900 s and 1,500 s; the settle time now also follows the measured cycle |
| Bounded pass resumes and covers every pair | Unchanged cursor and budget semantics, now per slice of 8 pairs; `background_cursor` tests (budgets of 1 and 4 pairs, leader changes) and the sweep simulator pass with 4 pairs at once; no new budget |
| Reclaim rate measured; retained bytes level ≥ 1 h at 10⁶ at that rate | **Not met**: rate measured on bursts (§4); level for an hour at 10⁶ (§4b) at a lower rate than the capacity; the run at capacity (§4c) incomplete |
| R-94(b) tests and model seeds unchanged | Met |
| Two independent reviews | Met (two rounds each; no High; Medium fixed) |
