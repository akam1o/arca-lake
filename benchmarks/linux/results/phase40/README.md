# Phase 4.0 results (summaries)

Summaries behind `docs/verification/phase-4.0-scalability.md`, sanitized
(no host names or addresses). Raw per-run directories (samplers' full
output) were not committed.

- `e*-summaries.jsonl`: one line per run from `summarize_bench.py --json`
  (E1 generator, E4b/E4c gateways, E5 FoundationDB processes, E6
  concurrency, E7/E7c object count, E13 S3 metadata shapes). Runs named in
  the report as invalid are included; the report says which and why.
- `e7-object-count.log`, `e7c-object-count.log`: campaign logs (including
  the unplanned node outage and the allotment change).
- `e7-oc-765k-state-pass-metrics.txt`, `e7c-*-pass-metrics.txt`: the
  controller's pass metrics at 765,134 and 10⁶ objects;
  `e7-765k-scrub-times.txt`, `e7c-1e6-scrub-times.txt`: scrub per group.
- `e8d-nodebench.jsonl`, `e8d-marks.jsonl`: storage node sweep (E8d).
- `e9b/`: per-thread CPU (`pidstat -t`) and perf samples per thread.
- `e10-*`: failures under load (fault times, summaries).
- `e11-metaops*.jsonl`, `e11-fdb-size.jsonl`: metadata operations.
- `e12-background-scale.txt`, `e12-memory-churn.jsonl`: controller passes
  at scale (in process) and the node heap run (R-108).
