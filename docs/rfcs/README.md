# RFC index and plan

RFCs are the unit of design review. Each RFC follows
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

## Planned RFCs

| RFC | Title | Depends on | Status | Primary inputs |
| --- | --- | --- | --- | --- |
| [RFC-001](RFC-001-architecture-overview.md) | Architecture Overview | — | draft | [overview](../architecture/overview.md), DD-9, DD-10, DD-14, DD-15 |
| [RFC-002](RFC-002-object-and-chunk-data-model.md) | Object and Chunk Data Model | 001 | draft | DD-1, DD-3, DD-11, DD-13 |
| [RFC-003](RFC-003-distributed-metadata-and-consistency.md) | Distributed Metadata and Consistency | 001, 002 | draft | DD-4, DD-5, Phase 0 metadata spike |
| [RFC-004](RFC-004-erasure-coding-and-placement.md) | Erasure Coding and Placement | 002, 003, 005 | draft (Phase 1 implementation, §11) | DD-1, DD-3, DD-9, Phase 0 EC benchmarks, Phase 1 E2E tests |
| [RFC-005](RFC-005-storage-engine-and-on-disk-format.md) | Storage Engine and On-disk Format | 002 | draft | DD-2, Phase 0 I/O and crash experiments |
| RFC-006 | Multi-Tier Storage | 002, 003, 004 | planned | DD-6, DD-11, DD-12 |
| RFC-007 | S3 Gateway Compatibility | 001, 003 | planned | Phase 0 `s3s` spike; compatibility matrix |
| RFC-008 | Failure Recovery and Garbage Collection | 003, 004, 005, 006 | planned | DD-7, DD-8 |
| RFC-009 | Performance Architecture | 004, 005, 007 | planned | Phase 0 measurements |
| RFC-010 | Testing and Verification | 001–008 | planned | testkit design |
| RFC-011 | Operations and Observability | 001, 006, 008 | planned | metrics registry, admin API |
| RFC-012 | Security Architecture | 001, 007 | planned | threat model |

## Dependency graph

```text
RFC-001 Architecture
 ├─▶ RFC-002 Data Model
 │    ├─▶ RFC-003 Metadata & Consistency ─────┐
 │    ├─▶ RFC-004 EC & Placement ──────────────┤
 │    └─▶ RFC-005 Storage Engine ──────────────┤
 │         (003,004) ─▶ RFC-006 Multi-Tier ────┤
 │                     (003..006) ─▶ RFC-008 Recovery & GC
 │                     (004,005,007) ─▶ RFC-009 Performance
 ├─▶ RFC-007 S3 Gateway (needs 003)
 ├─▶ RFC-011 Operations (needs 006, 008)
 ├─▶ RFC-012 Security (needs 007)
 └─▶ RFC-010 Testing (needs 001–008)
```

## Authoring order

1. RFC-001, RFC-002 first; they fix vocabulary and invariants.
2. RFC-003, RFC-004, RFC-005 in parallel; each is informed by a Phase 0
   experiment and should be drafted before the experiment and finalized after.
3. RFC-006 and RFC-007 once 003 and 004 stabilize.
4. RFC-008 after 003–006, because recovery crosses all of them.
5. RFC-009, RFC-010, RFC-011, RFC-012 can start as drafts early but are
   accepted last.

## Relationship to Phase 0

Phase 0 experiments produce the evidence RFC-003, 004, 005, 007, and 009 need.
Those RFCs should state explicitly which claims are *Verified* by a Phase 0
result (with a link to the benchmark report) and which remain hypotheses.
