# ADR-0018: Adaptive scrub budget

Status: accepted (Phase 3.4)

## Context

R-59 (Phase 3.3): with a working set larger than the page cache, scrubbing
at the fixed 32 MiB/s budget raised client GET p95 by about 72 % on the
same devices. A fixed rate is either too slow for idle devices or too
fast for busy ones, and the right value depends on hardware the project
does not know.

## Decision

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Signals | what the scrubbed node reports in `Health` for the recent window (at least 0.5 s): client GET serve p99 and PUT store p99 from its own histograms (portable), and device utilisation, queue depth and read latency where the platform exposes them (Linux `/proc/diskstats` of the block device holding the store; elsewhere absent). `load_reported` distinguishes "idle" from "an older node that reports nothing" | gateway-side latency (the controller has no gateway connection; node-side serve time is what scrubbing competes with); raw `iostat` scraping by the controller (not portable, and not the node's own device) |
| Control | AIMD on the byte budget: halve after `decrease_after` (2) consecutive pressured samples, add `step` (4 MiB/s) after `increase_after` (3) consecutive calm samples, sampled at most once a second per scrub page | PID (needs per-deployment tuning; noisy shared storage makes the derivative term useless and the integral winds up during long stalls); a threshold switch between two rates (oscillates at the boundary); a fixed rate (R-59) |
| Pressure | device utilisation ≥ 85 % or queue depth ≥ 4, or client GET p99 ≥ 50 ms, or PUT p99 ≥ 250 ms (targets configurable) | latency relative to a learned baseline (the baseline drifts with the scrub itself) |
| Bounds | floor `--scrub-min-rate-mib` (default 1 MiB/s: scrubbing never stops), ceiling `--scrub-max-rate-mib` (default 4 × the base rate) | an unbounded ramp; stopping scrub under load (verification would starve indefinitely on a busy cluster) |
| Missing signals | the fixed base rate (`--scrub-rate-mib`), exactly the pre-3.4 behaviour | ramping up without evidence |
| Repair | while stripe repair is moving bytes or has work queued, the ceiling halves: repair has priority over verification | a shared budget (repair and scrub would starve each other) |
| Failover | the state is per pass and in memory; a new leader starts at the base rate and resumes the persisted cursor | persisting the rate (stale after a failover; the loop converges in seconds) |
| Exported | `arca_controller_scrub_rate_bytes_per_second`, `arca_controller_scrub_throttles_total` | — |

Enabled with `--scrub-adaptive` on the controller (`--adaptive` on
`admin integrity scrub --wait`). The default stays the fixed budget until
the measurements in the Phase 3.4 performance report justify changing it.

## Consequences

- `Health` carries load fields 20–26 (additive; older nodes leave
  `load_reported` false).
- The controller samples `Health` of the node being scrubbed once a second
  during a pass: negligible next to the scrub reads.
