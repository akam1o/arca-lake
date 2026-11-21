# Verification evidence

These reports describe specific code, environments, workloads, and failure
conditions. A completed development phase does not mean every acceptance
criterion passed. Use the [known limitations](../guide/known-limitations.md)
and [current roadmap](../roadmap/README.md) for a current summary; follow the
reports for reproduction details and the boundaries of each conclusion.

## Recent scalability work

| Report | Scope and qualification |
| --- | --- |
| [Phase 4.1: access records (ADR-0034)](phase-4.1-adr34.md) | Bounded recording and batching; the commits/GET target passed, but GET throughput remained 7–14% below recording disabled in the final campaign. Includes unresolved failure classifications and deferred work |
| [Phase 4.1: background passes (ADR-0033)](phase-4.1-adr33.md) | Batched reconciliation and reclaim, cycle measurements, and capacity-pressure work whose convergence criteria remain incomplete |
| [Phase 4.1: R-110 capacity retest plan](phase-4.1-r110-retest-plan.md) | Criteria, budgets, load limits, stop rules and judgement for a small-allotment capacity-pressure retest |
| [Phase 4.1: R-110 capacity retest results](phase-4.1-r110-retest-results.md) | The retest run once (2026-11-21): Inconclusive; GC and reclaim worked under refusal and their returned space ended it before D1 matured, so C1 could not be judged |
| [Phase 4.1: access records where tiering acts (ADR-0035)](phase-4.1-adr35.md) | Option A measured (quiet controller background passes only) and integration-verified with two controllers and two gateways; R-109 stays Medium; residual resets tracked as R-118 |
| [Phase 4.1: concurrent overwrites (R-112)](phase-4.1-r112.md) | S3 contention and commit-outcome investigation, fixes, and remaining hot-key availability limits |
| [Phase 4.1: restore-test failures (R-113)](phase-4.1-r113.md) | Test-race classification, gateway restart ordering, and the separate residual product behavior |
| [Phase 4.0 scalability](phase-4.0-scalability.md) | Baseline object-count, process, metadata, and background-work measurements; later reports above supersede individual findings |
| [Phase 4 readiness](phase-4-readiness.md) | The earlier assessment and entry conditions for scalability work |

## Recovery, integrity, and capacity

| Report | Subject |
| --- | --- |
| [Phase 3.14](phase-3.14-operational-hardening.md) | Operational hardening, differential repair verification, and capacity reservations |
| [Phase 3.13](phase-3.13-repair-liveness.md) | Repair liveness, retargeting, and deferred reclamation at scale |
| [Phase 3.12](phase-3.12-restore-architecture.md) | Restore architecture and the conditions on deferred reclamation |
| [Phase 3.11](phase-3.11-restore-safety.md) | Restore guards, managed recovery, and remaining limits at that point |
| [Phase 3.10](phase-3.10-recovery.md) | Rejoin fencing, downgrade protection, and recovery |
| [Phase 3.9](phase-3.9-data-safety.md) | Delete fencing and data-safety checks |
| [Phase 3.8](phase-3.8-hardening.md) | Capacity and operational hardening |
| [Phase 3.7](phase-3.7-admission.md) | Admission refinement and shared file-system handling |
| [Phase 3.6](phase-3.6-capacity.md) | Capacity admission and garbage-collection backpressure |

## Cross-cutting experiments

- [Network faults](network-faults.md): namespace-based network impairment
  and its distinction from cross-host physical-network validation.
- [Performance regression](performance-regression.md): measurements and
  interpretation of performance changes.
- [Benchmark harnesses and reports](../../benchmarks/README.md): how the
  preserved measurement data is produced.

Keep raw evidence, failure descriptions, and unfulfilled criteria when
summarizing reports. New measurements belong in a clearly dated follow-up;
they do not retroactively make an earlier incomplete run pass.
