# Architecture Decision Records

Short, immutable records of decisions. Use [the template](0000-template.md).

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-license.md) | Apache-2.0 license | accepted |
| [0002](0002-documentation-language.md) | English is the canonical documentation language | accepted |
| [0003](0003-contribution-sign-off.md) | Developer Certificate of Origin sign-off | accepted |
| [0004](0004-supported-platforms.md) | Supported platforms | accepted |
| [0005](0005-initial-architecture-decisions.md) | Initial architecture decisions confirmed at design review | accepted |
| [0006](0006-phase-1-integration-decisions.md) | Phase 1 integration decisions: gRPC for internal RPC, postcard records, no persisted index, `m_ack = m` | accepted |
| [0007](0007-commit-resolution-and-gc-safety.md) | Commit resolution, reference-checked garbage collection, and repair completion | accepted |
| [0008](0008-leader-election-and-deletion-epochs.md) | Leader election on the metadata store, deletion epochs, verified repair completion | accepted |
| [0009](0009-multi-tier-storage-decisions.md) | Multi-tier storage: cache placements, admission and eviction, migration | accepted |
| [0010](0010-cache-protection.md) | Cache protection: group roles, coded cache copies, subordinate cache repair | accepted |
| [0011](0011-rpc-policy-and-parallel-reads.md) | Progress-based shard stream deadlines, retries, and bounded parallel reads | accepted |
| [0012](0012-population-queue-write-pipeline-and-frame-passthrough.md) | Population queue, pipelined chunk placement, frame pass-through on GET, permanent phase timing | accepted |
| [0013](0013-phase-3-0-design-decisions.md) | Phase 3.0 cross-cutting decisions: authentication and authorization model, service identity, multipart, compatibility window, backup unit, post-restore safety | accepted (as amended by 0014) |
| [0014](0014-phase-3-1-multipart-manifest-and-review-decisions.md) | Phase 3.0 review outcome: hierarchical multipart manifest and part liveness invariant, normative authorization rules, three compatibility axes with idempotent finalize, cluster recovery state machine, status-labelled acceptance criteria | accepted |
| [0015](0015-durable-quarantine-scrub-and-stripe-repair.md) | Durable sidecar quarantine, resynchronising open recovery, controller-driven paced scrubbing, per-stripe repair under the current epoch, persisted unrecoverable state, gateway degraded-read reports | accepted |
| [0016](0016-compatibility-gates-cluster-recovery-and-incarnation-fencing.md) | Cargo-feature version bump, capabilities in Health and proc heartbeats, format_write/incarnation pushed by the controller, GuardedKv cluster-state check with operation classes, node-side incarnation on DeleteShard, leader-driven recovery and finalize, legacy stamping at M=2 | accepted |
| [0017](0017-service-mtls-rpc-authorization-and-metadata-tls.md) | Service mTLS with SPIFFE identities, per-RPC role authorization, certificate and trust rotation, FoundationDB TLS | accepted |
| [0018](0018-adaptive-scrub-budget.md) | Adaptive scrub budget (AIMD on device and client latency signals; opt-in) | accepted |
| [0019](0019-commit-outcomes-fencing-and-reliability-closure.md) | Commit outcomes by phase, idempotent re-runs, process-unique leadership, fenced deletes, tombstone history at open only, former-member reads during moves, per-entry batch authorization (Phase 3.5) | accepted |
| [0020](0020-capacity-admission-and-gc-backpressure.md) | Multipart parts retired as of their root, node-side capacity admission with write classes and a reserve, per-tier backpressure state machine published by the controller and enforced by gateways (`503 SlowDown` + `Retry-After`), compaction room check, cache deletes by token (Phase 3.6) | accepted |
| [0021](0021-admission-refinement.md) | Chunk-level write charging, admission terms after failover, shared file system detection and refusal, recent-trend recovery and adaptive throttle rate, capacity-aware group choice (Phase 3.7) | accepted |
| [0022](0022-reserve-layers-record-fencing-rebalancing.md) | Delete reserve under both floors and a capacity breakdown, admission records fenced by incarnation, term and generation, recent-trend entry, capacity rebalancing between groups of a tier (Phase 3.8) | accepted |
| [0023](0023-delete-fence.md) | Delete fence enforced by storage nodes: persisted generation, open or closed, under the lock of the physical delete; operator hold tied to the incarnation; leader opens per term while Active; closed before restores and phase changes; protocol version 3 (Phase 3.9) | accepted |
