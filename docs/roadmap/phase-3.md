# Phase 3: Production hardening

Status: **3.1–3.5 delivered; 3.6 (capacity safety and GC backpressure)
delivered with the limitations in its report and `risks.md` (R-80..R-86)**. Priorities: long-running
operation, failure recovery, S3 compatibility, security, upgradability —
over further performance work (the Phase 2.3 regression check guards
what was gained).

## Sub-phases

| Sub-phase | Scope | Completion criteria | Status |
| --- | --- | --- | --- |
| 3.0 Design | RFC-007, RFC-012 (+ threat model), RFC-013, RFC-014, RFC-015, ADR-0013 | documents reviewed; open questions resolved or tracked; every control, operation and procedure has acceptance criteria | conditionally approved; review items reflected (ADR-0014) |
| 3.1 S3 surface | record version 2 and schema key (RFC-014 §5.4 prerequisite), multipart upload (hierarchical manifest), versioning exposure, conditional requests, CopyObject, DeleteObjects, checksums, bucket policy and credential store (RFC-012 C-1, C-3, C-4, C-6), AWS CLI + SDK conformance suite in CI | conformance suite green; RFC-007 §11 and RFC-012 §11 statuses recorded; regression check within tolerances | delivered (see the Phase 3.1 report; A10, A15, A16, S-A6, S-A9 remain Not Verified / Not Implemented) |
| 3.2 Integrity | durable quarantine, `ScrubShards`, paced controller scrub with a persisted cursor, eight corruption classes, per-stripe repair state machine, multipart manifest scrub, gateway degraded-read reports, integrity metrics, admin and node tools; FoundationDB failover for multipart (A10); security tests (corrupt policy, audit scan, rotation, tenant isolation) | RFC-008 §12 G2–G10: scrub finds injected corruption of every class, repairs the recoverable, records the unrecoverable and never serves it; verified in CI and on the validation hosts (`docs/verification/performance-regression.md`, Phase 3.2 run) | delivered (`replace-member`/`decommission-node` admin commands and object-level unreadable marking carried to 3.3, see risks R-54) |
| 3.3 Upgrade and recovery | three compatibility axes with startup gates, protocol negotiation, schema/format write gates, leader-driven `admin upgrade finalize`, two-binary mixed-version test (version-bump build, ADR-0016); cluster recovery state machine with incarnation fencing, epoch catch-up, resumable verification, `admin recovery`, real `fdbbackup`/`fdbrestore` drill; storage I/O fault injection; parallel repair with budgets; object-level integrity and manifest scrub cursor; `admin replace-member` / `decommission-node` | RFC-014 §11 and RFC-015 §11 statuses recorded; O1–O16 of the Phase 3.3 plan with evidence | delivered (mTLS, the FoundationDB TLS guide and the backup validation job B-A5 moved to 3.4; U-A2 golden files per record version Not Implemented) |
| 3.4 Soak and readiness | ≥ 24 h soak with concurrent PUT/GET/DELETE/overwrite, population, eviction, tiering, GC, repair, controller failover, node and FoundationDB restarts; leak monitoring (memory, FDs, capacity, metadata growth, throughput, p99); fault injection; alert rules loaded; RFC-013 runbooks complete | soak report with no unexplained growth or degradation; alerting verified against injected faults | delivered (see the Phase 3.4 report) |
| 3.5 Reliability closure | commit outcomes by phase, fenced deletes, member moves without lost redundancy, storage-node memory, shutdown, GC cadence (ADR-0019) | regression test per finding; long soak | delivered; READY FOR EXPERIMENTAL RELEASE (conditional); R-64, R-73 open |
| 3.6 Capacity safety and GC backpressure | multipart GC root cause, capacity accounting per device/group/tier, node-side capacity admission with a reserve, tier backpressure states with hysteresis, `SlowDown` + `Retry-After`, repair/compaction reserve, cache conditional delete (R-73), metrics/alerts/runbooks, short dense load tests A–D instead of a 24 h soak (ADR-0020, RFC-008 §6.8) | C1–C18 of the Phase 3.6 plan with evidence | delivered; long-duration operation not re-verified |

## Residual risks going in

- R-19 (metadata access by cluster file) stays open (mTLS and the
  FoundationDB TLS guide moved to 3.4).
- R-51 (restore safety): implemented and drilled in 3.3; the backup
  validation job (B-A5) remains.
- The compatibility window is new; the first release that uses it has
  no "previous release" to test against until a tag exists (3.1 creates
  the first tagged release for that purpose).
- GitHub-hosted runners remain unavailable to the account; fork pull
  requests cannot run CI until that is resolved or an ephemeral runner
  exists (RFC-013 §9).
