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
