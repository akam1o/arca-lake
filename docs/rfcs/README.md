# RFC index

RFCs are the unit of design review and include detailed specifications,
compatibility matrices, runbooks, and verification sections. Start with the
[current guides](../README.md) for an introduction. Draft status does not
mean that no implementation exists; accepted status does not mean every
acceptance criterion has passed. Follow each document's implementation and
verification sections and the [ADR amendments](../adr/README.md).

Each RFC follows
[the template](0000-template.md) and must contain: Problem, Goals, Non-goals,
Requirements, Proposed Design, Alternatives Considered, Failure Scenarios,
Consistency and Durability Guarantees, Performance Considerations, Open
Questions, Acceptance Criteria.

All RFCs are public documents and must comply with
[the confidentiality policy](../CONTRIBUTING-POLICY.md).

## Lifecycle

`draft` → `review` → `accepted` | `rejected` → (`superseded by RFC-nnn`)

An RFC is accepted when the open questions that block implementation are
resolved (remaining questions are moved to tracked issues) and the
acceptance criteria are testable.

## RFC documents

| RFC | Title | Depends on | Status | Primary inputs |
| --- | --- | --- | --- | --- |
| [RFC-001](RFC-001-architecture-overview.md) | Architecture Overview | — | draft | [overview](../architecture/overview.md), DD-9, DD-10, DD-14, DD-15 |
| [RFC-002](RFC-002-object-and-chunk-data-model.md) | Object and Chunk Data Model | 001 | draft | DD-1, DD-3, DD-11, DD-13 |
| [RFC-003](RFC-003-distributed-metadata-and-consistency.md) | Distributed Metadata and Consistency | 001, 002 | draft | DD-4, DD-5, Phase 0 metadata spike |
| [RFC-004](RFC-004-erasure-coding-and-placement.md) | Erasure Coding and Placement | 002, 003, 005 | draft (Phase 1 implementation, §11) | DD-1, DD-3, DD-9, Phase 0 EC benchmarks, Phase 1 E2E tests |
| [RFC-005](RFC-005-storage-engine-and-on-disk-format.md) | Storage Engine and On-disk Format | 002 | draft | DD-2, Phase 0 I/O and crash experiments |
| [RFC-006](RFC-006-multi-tier-storage.md) | Multi-Tier Storage | 002, 003, 004, 008 | draft (Phase 2 implementation, §11) | DD-6, DD-11, DD-12, Phase 2 cache and tiering tests, ADR-0009 |
| [RFC-007](RFC-007-s3-gateway-compatibility.md) | S3 Gateway Compatibility | 001, 002, 003, 008, 012 | draft (Phase 3.0 design) | Phase 0 `s3s` spike; compatibility matrix; AWS CLI tests |
| [RFC-008](RFC-008-failure-recovery-and-garbage-collection.md) | Failure Recovery and Garbage Collection | 003, 004, 005 | draft | DD-7, DD-8, Phase 1 hardening tests |
| [RFC-009](RFC-009-performance-architecture.md) | Performance Architecture | 004, 005, 006 | draft (Phase 2.1 measurements, §6–§8) | Phase 0 transfer and EC benchmarks; Phase 2.1 three-host matrix (`benchmarks/linux/`) |
| RFC-010 | Testing and Verification | 001–008 | planned | testkit design |
| [RFC-011](RFC-011-controller.md) | Controller | 003, 004, 008 | draft | Phase 1.2 controller process and failover tests |
| [RFC-012](RFC-012-security-architecture.md) | Security Architecture | 001, 003, 007, 011 | draft (Phase 3.0 design) | [threat model](../security/threat-model.md) |
| [RFC-013](RFC-013-operations-guide.md) | Operations and Observability | 001, 006, 008, 011, 012, 014, 015 | draft (Phase 3.0 design) | metric registry, runbooks, alert rules |
| [RFC-014](RFC-014-upgrade-and-compatibility.md) | Rolling Upgrade and Compatibility | 003, 005, 008, 011 | draft (Phase 3.0 design) | format and protocol versions, layout key, mixed-version tests |
| [RFC-015](RFC-015-backup-restore-and-disaster-recovery.md) | Backup, Restore and Disaster Recovery | 003, 004, 006, 008, 011, 014 | draft (Phase 3.0 design) | FoundationDB backup, restore verification, drills |

## Initial planning context

The dependencies above describe design relationships, not an implementation
schedule. The [Phase 0 plan](../roadmap/phase-0.md) and
[initial design analysis](../architecture/design-decisions.md) retain the
original experiment and authoring plan. Current work is tracked in the
[roadmap index](../roadmap/README.md).

When updating an RFC, state which behavior is implemented, which acceptance
criteria were verified and under what conditions, and which remain open.
Keep design status separate from those results; do not change a draft to
accepted solely because code exists.
