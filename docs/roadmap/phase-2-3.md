# Phase 2.3: Performance stabilization and scalability

Status: **in progress**. Phase 2.3 acts on the Phase 2.2 measurements:
the cause of the PUT variance (R-48), the gateway's GET CPU, a node-aware
population queue, the metadata hot keys, multi-gateway scaling, and a
performance regression check. Design records: ADR-0012; numbers:
RFC-009 §6.14–§6.19.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| CI state and public artifacts | done | GitHub-hosted runners refused for billing reasons; CI moved to self-hosted Linux runners; artifact sanitizer with a written boundary (`docs/security/public-artifacts.md`) and a CI check; the one commit that carried host names was replaced before anyone else fetched it (private repository; no credential) |
| 2.3.3 R-48 cause | done | per-phase PUT timing on gateway, node and store; per-second sampler; cause: fdatasync latency on shared virtual storage, reproduced with a write load on another VM (RFC-009 §6.14) |
| 2.3.3 PUT variance mitigation | implemented, measured | pipelined chunk placement (`--write-pipeline`), measured quiet and under noise (§6.16) |
| 2.3.4 GET CPU | done | perf profile (§6.15): memcpy 37 %, page zeroing 8 %, CRC 7 %, no EC decode; frame pass-through removes the three post-decode copies; measured before/after |
| 2.3.5 Population queue | implemented, tested | bounded FIFO, workers, per-node permits, bounded yield to reads, retries, shutdown, counters (RFC-006 §5.3.1, ADR-0012) |
| 2.3.6 Metadata hot keys | measured | `metabench` against FoundationDB: tasks 1–32, insert/remove, touch, mixed, evictor, two processes same tier / two tiers (§6.17) |
| 2.3.7 Multi-gateway | measured | `multi-gateway.sh`: 1 / 2 / 3 gateways, durable and cache (§6.18) |
| 2.3.8 Regression check | implemented | `regress.sh` + `compare.py` with per-environment baselines (`docs/verification/performance-regression.md`) |
| 2.3.9 Correctness | tests | every suite passes with the pipeline, the frame pass-through and the queue; new `population_queue.rs` |

## Results

<!-- PHASE-2-3-RESULTS -->

## Not verified

<!-- PHASE-2-3-NOT-VERIFIED -->
