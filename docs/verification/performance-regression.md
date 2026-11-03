# Performance regression check

Phase 2.3 §8. Functional regression tests (the workspace test suites,
run in CI on every push) and performance measurement are separate:
performance numbers depend on the hosts, so no fixed threshold is
applied in CI. Instead:

1. `benchmarks/linux/regress.sh <label> [out] [--record]` runs the fixed
   workload (`repeat.sh`: durable-only and the unprotected cache tier at
   concurrency 4, three repetitions in shuffled order with a warm-up)
   on the two-host layout and reduces it with `perf_summary.py` to:
   PUT throughput, warm GET throughput and p50 / p95 / p99, CPU-seconds
   per GiB on the gateway host and the node host, largest process RSS,
   cache hit ratio (last pass), population throughput (first pass), and
   the metadata transaction rate during PUT.
2. `compare.py` compares the summary with `benchmarks/baselines/<label>.json`
   and fails on a regression beyond the tolerances listed in its header
   (throughput −15 %, latency +25 %, CPU and RSS +20 %, hit ratio −0.05,
   population −25 %, transaction rate ±30 %). Without a baseline for the
   label it only reports; `--record` stores one (and updates it after a
   clean comparison).
3. The label is the environment, never "CI": a baseline recorded on the
   validation hosts is compared only with later runs on those hosts.

The PUT indicator is the one most exposed to the environment (R-48: it
tracks the nodes' fdatasync latency, which on shared virtual storage
depends on neighbouring load); run the check when the hosts are
otherwise idle and treat a PUT-only change as suspect until repeated.

## Phase 3.1 run (2026-11-02/03)

Results: `benchmarks/linux/results/regress-phase31` (the check),
`regress-phase31-ab-old` (the pre-Phase 3.1 binary, commit `6f8856f`, on
the same hosts right afterwards), `regress-phase31-rerun` and
`regress-phase31-rerun-mp` (the Phase 3.1 binary again, later). Label
`lab-2host`, baseline from Phase 2.3.

| metric | baseline | Phase 3.1 check | old binary, same hosts | Phase 3.1 rerun | verdict |
| --- | --- | --- | --- | --- | --- |
| PUT MiB/s (median of 3) | 340 | 243 (reps 243 / 223 / 270) | 346 (reps 199 / 352 / 346) | 338 (reps 249 / 338 / 370) | environment (R-48), see below |
| warm GET MiB/s | 695 | 689 | 681 | — | ok |
| GET p50 / p95 / p99 ms | 78 / 94 / 100 | 80 / 106 / 109 | — | — | ok (+2 / +12 / +9 %) |
| CPU-s/GiB gateway host / node host | 4.64 / 2.19 | 4.60 / 2.32 | — | — | ok (−1 / +6 %) |
| RSS MiB | 110 | 126 | — | — | ok (+15 %; the gateway now holds the AWS SDK-free credential store and audit state, and the multipart configuration ran in the same process) |
| hit ratio (last pass) | 0.93 | 0.82 | 0.82 | — | environment: identical with the old binary; populations deferred to slow reads and dropped at the queue bound (200–290 of 384 per run) |
| population chunks/s | 70 | 62 | 56 | — | ok |
| metadata txn/s during PUT | 149 | 106 | 151 | — | tracks the PUT rate (7 transactions per PUT in every run, old and new) |
| multipart PUT MiB/s (8 MiB parts, new) | — | 220 (reps 220 / 268 / 182) | — | 171 (reps 171 / 170 / 256; slow state again) | recorded: baseline 201 = median of all six |
| multipart warm GET MiB/s (new) | — | 688 | — | 682 | recorded: reads of multipart objects (lazy part manifests) match single-object GET |

**PUT investigation.** The check flagged PUT (−28 %) and the hit ratio.
Per-phase gateway histograms (`arca_gateway_put_phase_seconds`) over the
96 PUTs of each repetition show identical body-read and encode cost
(4 ms and 5 ms per chunk, old and new) and identical transaction counts
(672 per run); the whole difference is in shard writes (70–73 ms per
chunk in the slow repetitions against 49–50 ms) and FoundationDB
append/commit (19–29 ms against 8–10 ms), with 18–26 % iowait on the
node host against about 10 % in the fast repetitions. The old binary
produced the same slow profile in one of its three repetitions
(199 MiB/s, 90 ms shard writes, 26 % iowait), and the Phase 3.1 binary
reached 338–370 MiB/s in two of three later repetitions. Conclusion:
the shared virtual storage of the validation hosts alternated between a
slow and a fast state during the check (R-48); Phase 3.1 adds no
measurable CPU or transaction cost to the PUT path. The hit-ratio drop
has the same cause (populations yield to slow durable reads and the
queue bound drops them) and is identical with the old binary.

The Phase 2.3 baseline is kept; the two multipart indicators are added
to it as the median of all six multipart repetitions (four in the slow
storage state), with a note in the baseline file. Multipart PUT is
expected below single PUT at equal concurrency: `s3load` uploads the
8 MiB parts of one object sequentially (two chunks per part, so the
placement pipeline refills per part) and each object costs three extra
requests; in the same storage state it reached 90 % of single PUT.

## Phase 3.2 run (2026-11-02/03): scrubbing, repair, soak

Results: `benchmarks/linux/results/phase32-20261103-0340` (label
`lab-2host`, 3 durable nodes `2+1`, 4 MiB chunks, 16 MiB objects). Every
figure is the median of three repetitions (`repeat.sh`, `CONFIGS=
durable:4`) unless noted. Controller flags for the scrub runs:
`--scrub-interval-secs 1 --scrub-tick-secs 1 --scrub-rate-mib 32
--scrub-concurrency 4`, i.e. the group is scrubbed back to back for the
whole run at the default budget.

### GET and PUT during scrubbing

| metric | scrub off (`baseline`) | scrub, 256-key pages (`scrub`) | scrub off, later (`baseline2`) | scrub, 8 MiB pages (`scrub8`) | verdict |
| --- | --- | --- | --- | --- | --- |
| PUT MiB/s | 182 | 167 (−8 %) | 270 (reps 270 / 224 / 318) | 341 (reps 341 / 256 / 351) | no scrub cost; the spread is the storage state (R-48) |
| warm GET MiB/s | 660 | 442 (−33 %) | 671 (reps 671 / 706 / 468) | 675 (reps 675 / 673 / 690) | ok (+1 %) |
| GET p50 / p95 / p99 ms | 83 / 108 / 119 | 126 / 157 / 178 | 80 / 104 / 114 | 78 / 103 / 111 | ok (−2 / −1 / −3 %) |
| CPU-s/GiB gateway host / node host | 4.9 / 3.1 | 7.2 / 5.0 | 5.0 / 3.2 | 4.9 / 2.2 | ok |
| RSS MiB | 115 | 135 | 111 | 118 | ok (+6 %) |

The first scrub run paid a third of the warm GET throughput and half
again on p99. The cause was pacing granularity, not the budget: a
`ScrubShards` page was bounded in keys only, and 256 keys of 2 MiB
shards is a 512 MiB read at device speed followed by a 16 s sleep, so
the "32 MiB/s" average arrived as bursts that starved the concurrent
GETs. `94ef98a` bounds a page in bytes as well (`max_bytes` in the RPC,
`--scrub-page-mib`, default 8 MiB), which spreads the same budget into
8 MiB reads every quarter second. With that, GET throughput and tail
latency under continuous scrubbing are within the repetition noise of
the scrub-off runs measured right after (`baseline2`), and PUT is
unaffected. Acceptance G10 (less than 15 % cost at the default rate) is
met on this layout.

Limitation: the 1.5 GiB working set fits the hosts' page cache, so the
scrub's reads did not reach the devices (`disk_read_amplification 0.0`
in `integrity3/summary.json`); the contention measured here is CPU and
RPC contention on the node, not disk queueing. A run with a working set
larger than memory is listed as a next task.

### Scrub and repair throughput (`integrity-perf.sh`, `integrity3`)

64 objects × 16 MiB = 1 GiB of data, 1.5 GiB of shards.

| measurement | value |
| --- | --- |
| scrub, unpaced (one `admin integrity scrub --wait`, page cache warm) | 1.5 GiB in 6.4 s = 241 MiB/s; admin-side CPU 1.8 s/GiB (CRC-32C over every shard plus metadata lookups per shard), node CPU 0.7 s/GiB |
| scrub at the default budget (32 MiB/s, 8 MiB pages) | 49.1 s measured, 48.0 s expected: the pacer holds the budget to within 2 % |
| repair after a wiped device (one of three members emptied and restarted; the presence phase queued 256 stripes) | 256 of 256 repaired, 0 failed, 512 MiB rewritten in 22.6 s = 22.7 MiB/s (one worker, one task at a time, ~88 ms per 2 MiB shard: two reads, decode, re-encode, one write, one read-back) |
| repair cost | controller CPU 18 s/GiB written (decode + re-encode of the full stripe per task), node CPU 12 s/GiB; disk writes 2.1 B per repaired byte (the shard plus its read-back and the quarantine/segment bookkeeping), network 2.0 B sent per repaired byte (two source shards in, one shard out plus the read-back) |
| afterwards | every object read back and verified (`verify-get.jsonl`: 64 objects, 0 errors, 0 mismatches, 706 MiB/s); a final scrub pass was clean |

Repair throughput is bounded by the single serial worker, not by the
nodes: a parallel worker (per-task concurrency bounded by the same byte
budget as the scrub) is the next step if a wiped device must be rebuilt
faster than ~20 MiB/s. The first measurement run (`integrity/`) recorded
a wrong repair window (it waited for a metric gauge that is refreshed
only at the end of a worker pass) and is kept for the scrub figures
only.

### Bounded integrity soak (`soak.sh`, 20 minutes, `soak4`)

Layout: durable `2+1` plus a coded cache tier (256 MiB, eviction every
2 s), versioning on, two controllers (leader failover every fifth
round), continuous scrubbing (5 s interval, 32 MiB/s), GC with a 10 s
grace, stripe repair every 5 s. Each round: 16 × 8 MiB PUTs, 6 × 24 MiB
multipart PUTs, overwrite of the previous round, DELETE of the round
before, verified GETs of the current, previous and multipart objects in
random order. Faults, one per round in rotation: storage node restart,
controller leader kill, six byte flips on a stopped device, cache node
restart, controller rejoin.

| outcome | value |
| --- | --- |
| rounds | 84 (≈ 2 200 single PUTs, 500 multipart PUTs, 1 300 overwrites, 1 300 deletes, 5 400 verified GETs) |
| body mismatches on verified reads | 0 |
| unreadable reads (`arca_gateway_unreadable_reads_total`) | 0 |
| degraded reads served correctly with parity | 1 623 (1 344 reported to the repair queue, 279 deduplicated) |
| byte flips quarantined on the node (`quarantine.log`, d1) | 171 entries over 17 flip rounds; 87 released by repair (`cause=superseded`), the rest by GC tombstones of overwritten or deleted versions |
| stripe repairs | 94 succeeded (79 missing-shard, 15 checksum-mismatch), 141 attempts failed explicitly and were retried (119 `write failed: unavailable` and 22 `members unavailable`, all inside node-restart windows), 0 unrecoverable, 0 left failed |
| final scrub after settling | two passes: findings `[0, 15, 0, 0, 1137, 0, 0, 0]` then `[0, 3, …]` (checksum mismatches queued and repaired between the passes; the "unexpected" findings are shards of versions GC had retired but not yet collected, which reconciliation removes, never scrub) |
| manifest scrub | 466 roots, 0 findings; repair queue empty; `data_integrity_status 0` |
| cache | 1 770 populations, 1 736 evictions, 92 degraded cache hits repaired in the background |
| explicit request failures during fault windows | 26 labels; every one a PUT, multipart PUT or overwrite that failed with an error while a node or the leader was restarting, and never a partially visible object: a failed overwrite left the previous version readable, a failed upload left no object (the final verification of the last round counted those absent objects as errors, not mismatches; the harness now accepts 404 for rounds whose PUTs failed) |
| controller failover | 17 leader kills; the follower took over each time and resumed the persisted scrub cursor and repair tasks |

The three earlier soaks (`soak`, `soak2`, `soak3`) are kept with their
harness false positives explained: `soak` reported five "mismatch"
labels where the harness expected the new content of overwrites that had
failed explicitly (the previous version was, as designed, still served);
`soak2` and `soak3` one label each where the last round was verified
against the wrong seed (`0b3bae0`). No run reported an unreadable read,
an unrecoverable stripe, a failed task left behind or a manifest
finding.

## Phase 3.3 run (2026-11-03): parallel repair, disk-bound scrub, regression

Results: `benchmarks/linux/results/phase33-20261103` (label `lab-2host`:
two virtual machines, 4 vCPU and 7.8 GiB RAM each, one emulated disk
each; 3 durable nodes `2+1`, 4 MiB chunks). Single runs unless noted;
on these hosts run-to-run spread is ±30 % for PUT and ±20 % for GET
latency, so differences inside that band are reported as noise.

### Parallel stripe repair (`repair-sweep.sh`)

128 objects × 16 MiB; one of three members wiped and restarted; the
scrub's presence phase queues 528 stripes; the controller's worker
rebuilds 1.0 GiB. Every run: 528 of 528 repaired, 0 failed, every object
read back and verified afterwards.

Under client load (`repair-sweep-load`: GET passes over a hot 64 MiB set
at concurrency 4 and PUT passes of 4 MiB objects at concurrency 2 for
the whole repair; `baseline` is the same load with no repair):

| workers / budget | repair MiB/s | duration s | GET p50 / p95 / p99 ms | GET MiB/s | PUT MiB/s | peak controller RSS MiB | peak queue |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (no repair) | — | — | 47 / 62 / 64 | 322 | 53 | — | — |
| 1, unpaced | 7.9 | 134 | 62 / 80 / 83 | 245 | 43 | 76 | 390 |
| 2, unpaced | 14.6 | 73 | 40 / 53 / 56 | 384 | 58 | 97 | 370 |
| 4, unpaced | 22.2 | 48 | 37 / 48 / 50 | 411 | 58 | 126 | 265 |
| 8, unpaced | 22.9 | 46 | 33 / 45 / 46 | 467 | 61 | 169 | 171 |
| 4, 64 MiB/s (default) | 11.4 | 93 | 35 / 48 / 50 | 426 | 60 | 128 | 440 |

Without client load (`repair-sweep-noload`), for clean cost figures:

| workers / budget | repair MiB/s | controller CPU-s/GiB | node CPU-s/GiB | disk write B / repaired B | network B / repaired B | peak RSS MiB |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 33.3 | 15.5 | 7.8 | 2.12 | 2.01 | 74 |
| 2 | 30.1 | 17.5 | 12.6 | 2.12 | 2.01 | 97 |
| 4 | 31.1 | 17.5 | 12.6 | 2.10 | 2.01 | 130 |
| 8 | 36.5 | 16.5 | 12.6 | 2.10 | 2.01 | 170 |
| 4, 64 MiB/s (default) | 13.3 | 18.4 | 10.7 | 2.15 | 2.01 | 124 |

Reading:

- Parallelism matters when the per-stripe round trips are slow (under
  load: 7.9 → 22 MiB/s from one to four workers, flat beyond four).
  Without load the window is dominated by the scrub that finds the
  damage and the rate is about the same for every worker count.
- Amplification does not change with parallelism (2.1 B written, 2.0 B
  sent per repaired byte, as in Phase 3.2); controller memory grows by
  about 15 MiB per worker (in-flight stripes).
- The client figures during repair are within the run-to-run noise of
  the baseline in every configuration (the hot set is in the page
  cache: this is CPU and RPC contention, not disk queueing; R-59 below
  is the disk-bound case). No client error in any run.
- The byte budget counts every byte the worker moves (two source reads,
  one write, one read-back per rebuilt byte at `2+1`), so 64 MiB/s
  rebuilds at 11–13 MiB/s.

Defaults kept: 4 workers (where the throughput under load saturates),
64 MiB/s, 2 transfers per node (1 on a node with more than 4 client
streams open). The budget is conservative on purpose: repair and scrub
budgets add up on the same devices, and the disk-bound scrub run below
shows the cost of extra device reads on p95. An operator who needs a
faster rebuild raises `--stripe-repair-rate-mib` (unpaced ≈ 22 MiB/s
under load, ≈ 31–36 MiB/s idle on these hosts).

### Scrub with a working set larger than memory (`scrub-diskbound.sh`, R-59)

1024 objects × 16 MiB: 16 GiB of shards on the host with two nodes,
7.8 GiB of RAM. Client load in every 120 s window: random 1 MiB range
GETs over all objects (concurrency 4) and PUTs of 4 MiB objects
(concurrency 2). CPU iowait is about 30 % on that host even without a
scrub, so the GETs reach the device.

| window | scrub MiB/s (effective) | device read MiB/s | avg queue depth | util | iowait % | GET p50 / p95 / p99 ms | PUT MiB/s (p99 ms) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| no scrub | 0 | 754 | 6.1 | 0.63 | 30.5 | 28.7 / 47.9 / 98.9 | 51.3 (190) |
| default budget, 32 MiB/s | 32.0 | 704 | 7.6 | 0.62 | 30.7 | 30.5 / 82.3 / 107.2 | 54.5 (198) |
| unpaced | 116.7 | 752 | 8.7 | 0.64 | 31.2 | 33.1 / 93.1 / 110.8 | 49.6 (215) |

- The pacer holds the configured budget exactly (32.0 MiB/s).
- At the default budget p50 and p99 move by +6 % and +8 %, p95 by
  +72 %; unpaced (3.6× the bytes) +15 % / +94 % / +12 %. The device
  queue deepens from 6.1 to 7.6 and 8.7. PUT is unaffected (within
  noise). The cost lands on the middle of the distribution: reads that
  previously queued behind few requests now queue behind scrub pages.
- Limitation: the device is an emulated disk; ~750 MiB/s of reads is
  more than one physical disk serves, so the hypervisor's cache below the
  guest absorbs part of it. The measurement is disk-bound inside the
  guest (page cache exceeded, iowait 30 %), not verified against a
  physical device. RFC-008 G10 ("< 15 % cost at the default rate") holds
  for p50/p99 and does not hold for p95 in this layout.

### Regression check (`regress.sh lab-2host`)

Baseline from Phase 2.3. `regress` is the Phase 3.3 binary, `regress-ab-old`
the Phase 3.2 final commit (`4af4396`) on the same hosts right
afterwards, `regress-rerun` the Phase 3.3 binary again right after that.

| metric | baseline | Phase 3.3 | Phase 3.2 binary (A/B) | Phase 3.3 rerun | verdict |
| --- | --- | --- | --- | --- | --- |
| PUT MiB/s (median of 3) | 340 | 205 (reps 205 / 196 / 307) | 312 (reps 313 / 196 / 343) | 342 (reps 356 / 268 / 342) | ok: environment (R-48); the old binary shows the same slow repetition |
| warm GET MiB/s | 695 | 678 | 681 | 684 | ok |
| GET p50 / p95 / p99 ms | 78 / 94 / 100 | 77 / 106 / 112 | 79 / 102 / 112 | 80 / 97 / 109 | ok |
| CPU-s/GiB gateway host / node host | 4.64 / 2.19 | 5.06 / 2.37 | 4.87 / 2.12 | 4.83 / 2.13 | ok |
| RSS MiB | 110 | 116 | 114 | 125 | ok (+14 %: process heartbeat, cluster watch, negotiated capabilities) |
| hit ratio (last pass) | 0.93 | 0.75 | 0.88 | 0.82 | environment: the Phase 3.2 binary also falls below the tolerance, as in Phase 3.1 |
| population chunks/s | 70 | 38 | 68 | 69 | ok in the rerun; tracks the slow PUT state of the first run |
| metadata txn/s during PUT | 149 | 91 | 137 | 151 | tracks the PUT rate |
| multipart PUT MiB/s | 201 | 247 | 248 | 276 | ok |
| multipart warm GET MiB/s | 688 | 664 | 656 | 683 | ok |

The first check flagged PUT, hit ratio, population and the transaction
rate. The A/B run of the previous release on the same hosts shows the
same slow repetition (196 MiB/s), and the rerun of the Phase 3.3 binary
is at the baseline (+0.6 %): the PUT drop was the storage state of the
virtual hosts (R-48), not the per-transaction cluster-state read added
in Phase 3.3 (one extra read in the commit of each writing transaction;
read-only transactions pay nothing). The hit ratio is below the
tolerance for both binaries; it is the population queue dropping work
behind slow reads, as recorded in Phase 3.1. No regression attributable
to Phase 3.3 code is visible within the noise of these hosts.
