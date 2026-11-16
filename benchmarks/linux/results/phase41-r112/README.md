# Phase 4.1 P0-1 results (R-112 reproduction)

Behind `docs/verification/phase-4.1-r112.md`. One directory per trial,
`r112c-*` the campaign (3 trials per scenario), `r112d-*` the
confirmation round on the final build: `check.json` (`contend --mode
check`: PUT outcomes, violations, definite failures visible, and its exit
status), `run.json` (request counts), `settle.log` (the nodes' live and
pending records until stable, the GC backlog), `scrub-times.txt` (every
group's scrub and its exit status), `faults.log` (fault times, Unix
seconds); `summary-*.txt` one line per trial; `latency-*.txt` PUT outcome
rates and latencies. The per-request logs were not committed (they hold
no host data, but are large).
