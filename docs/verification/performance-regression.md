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
