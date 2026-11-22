# Roadmap and phase records

The active plan is [Phase 4](phase-4.md). arca-lake is experimental;
0.1.0 is the first experimental release (candidate; not yet published).
A delivered implementation or completed development phase does
not establish production readiness; each verification report states the
conditions it exercised and the criteria it did not meet.

## Current work

| Area | Current status | Evidence and next work |
| --- | --- | --- |
| Scalability baseline | Phase 4.0 delivered with limitations | [Report](../verification/phase-4.0-scalability.md); larger S3 object counts and dedicated-host scaling remain in Phase 4.2 |
| Concurrent overwrites and commit answers | R-112 investigated through S3; R-114 answer defects fixed; hot-key availability remains | [Report](../verification/phase-4.1-r112.md); per-key contention work remains in Phase 4.1 |
| Restore-test failures | R-113 classified and tests corrected | [Report](../verification/phase-4.1-r113.md); separate residual product behavior is R-116 in the [risk register](../architecture/risks.md) |
| Reconciliation and reclaim | ADR-0033 implemented and measured; capacity-pressure criterion incomplete | [Report](../verification/phase-4.1-adr33.md); the later [integrity check](../verification/phase-4.1-adr34.md) recovered and verified the test cluster, but did not complete the capacity retest; the [retest plan](../verification/phase-4.1-r110-retest-plan.md) (version 1) was run twice on 2026-11-21, both runs Inconclusive ([results](../verification/phase-4.1-r110-retest-results.md)); plan version 2 (C1 split into C1a and C1b) is being prepared |
| Access-record batching | ADR-0034 decisions 1–5, 7 and 8 implemented; commit-cost target met, GET-throughput target missed where tiering records reads | [Report](../verification/phase-4.1-adr34.md); decision 6 implemented as ADR-0035 option A ([report](../verification/phase-4.1-adr35.md): with tiering confirmed disabled, GET throughput within the spread of `off` in a quiet measurement; integration verified with two controllers and two gateways); two gateways contending on one hot generation and a FoundationDB stall under load remain unverified on the cluster |
| Remaining Phase 4.1 work | In progress | [Plan](phase-4.md): transactions and conflicts per request, map-version reads, gateway placement measurements, small-object capacity analysis and benchmark follow-ups |
| Scale-out and capacity efficiency | Phase 4.2 not started | [Plan](phase-4.md#phase-42--scale-out-and-capacity-efficiency): dedicated hosts, larger object counts, packing design, long-duration and physical power-loss validation |

The [known limitations](../guide/known-limitations.md) and
[risk register](../architecture/risks.md) describe current operating
boundaries. [Public repository readiness](../release/public-readiness.md)
tracks publication work separately from these implementation phases.

## Historical phase records

These pages retain their original paths for existing references. Their
status tables and unverified items describe the named phase; use later
reports and current guides before applying them to today's code.

| Phase | Scope |
| --- | --- |
| [0](phase-0.md) | Initial architecture and proof-of-concept experiments |
| [1](phase-1.md) | Distributed object-storage integration |
| [1.2](phase-1-2.md) | Controller operations and failover |
| [2](phase-2.md) | Native multi-tier storage |
| [2.1](phase-2-1.md) | Cache protection and Linux validation |
| [2.2](phase-2-2.md) | Performance and resilience optimization |
| [2.3](phase-2-3.md) | Performance stabilization and scalability |
| [3](phase-3.md) | Reliability, recovery, capacity and operational hardening |

The complete earlier implementation log is in the
[development archive](../archive/README.md). ADRs retain the design
rationale; verification reports retain the evidence and its limits.
