# Phase 3: Production hardening

Status: **3.1 and 3.2 delivered; 3.3 next**. Priorities: long-running
operation, failure recovery, S3 compatibility, security, upgradability —
over further performance work (the Phase 2.3 regression check guards
what was gained).

## Sub-phases

| Sub-phase | Scope | Completion criteria | Status |
| --- | --- | --- | --- |
| 3.0 Design | RFC-007, RFC-012 (+ threat model), RFC-013, RFC-014, RFC-015, ADR-0013 | documents reviewed; open questions resolved or tracked; every control, operation and procedure has acceptance criteria | conditionally approved; review items reflected (ADR-0014) |
| 3.1 S3 surface | record version 2 and schema key (RFC-014 §5.4 prerequisite), multipart upload (hierarchical manifest), versioning exposure, conditional requests, CopyObject, DeleteObjects, checksums, bucket policy and credential store (RFC-012 C-1, C-3, C-4, C-6), AWS CLI + SDK conformance suite in CI | conformance suite green; RFC-007 §11 and RFC-012 §11 statuses recorded; regression check within tolerances | delivered (see the Phase 3.1 report; A10, A15, A16, S-A6, S-A9 remain Not Verified / Not Implemented) |
| 3.2 Integrity | durable quarantine, `ScrubShards`, paced controller scrub with a persisted cursor, eight corruption classes, per-stripe repair state machine, multipart manifest scrub, gateway degraded-read reports, integrity metrics, admin and node tools; FoundationDB failover for multipart (A10); security tests (corrupt policy, audit scan, rotation, tenant isolation) | RFC-008 §12 G2–G10: scrub finds injected corruption of every class, repairs the recoverable, records the unrecoverable and never serves it; verified in CI and on the validation hosts (`docs/verification/performance-regression.md`, Phase 3.2 run) | delivered (`replace-member`/`decommission-node` admin commands and object-level unreadable marking carried to 3.3, see risks R-54) |
| 3.3 Upgrade and recovery | protocol negotiation, format write gate, `admin init/upgrade finalize`, mixed-version test with the previous release binary, mTLS (RFC-012 C-9), FoundationDB TLS guide, backup validation job, restore drill with the cluster state machine (RFC-015 §5.3) | RFC-014 §11 and RFC-015 §11 met on the validation hosts; runbooks exercised | not started |
| 3.4 Soak and readiness | ≥ 24 h soak with concurrent PUT/GET/DELETE/overwrite, population, eviction, tiering, GC, repair, controller failover, node and FoundationDB restarts; leak monitoring (memory, FDs, capacity, metadata growth, throughput, p99); fault injection; alert rules loaded; RFC-013 runbooks complete | soak report with no unexplained growth or degradation; alerting verified against injected faults | not started |

## Residual risks going in

- R-19 (metadata access by cluster file) stays open until 3.3.
- R-51 (restore safety) is a design until the 3.3 drill.
- The compatibility window is new; the first release that uses it has
  no "previous release" to test against until a tag exists (3.1 creates
  the first tagged release for that purpose).
- GitHub-hosted runners remain unavailable to the account; fork pull
  requests cannot run CI until that is resolved or an ephemeral runner
  exists (RFC-013 §9).
