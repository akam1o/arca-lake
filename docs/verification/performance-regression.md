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
