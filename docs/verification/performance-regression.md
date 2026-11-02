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
