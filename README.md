# arca-lake

**arca-lake** is a high-performance, horizontally scalable, S3-compatible
distributed object store with native multi-tier storage management and
configurable erasure coding. It is written in Rust and is part of the
`arca` family of infrastructure software.

> **Status: pre-release, Phase 2 first iteration.** No usable release
> exists yet. The repository contains the architecture review, design
> decisions, accepted ADRs, the RFC drafts, the phase roadmaps with their
> verified and unverified items, and the workspace. See
> [`docs/`](docs/README.md).

## Goals

- Broad Amazon S3 API compatibility, implemented incrementally and tracked in
  an explicit compatibility matrix. We do not claim compatibility for anything
  that is not implemented and verified.
- Native multi-tier storage: arbitrary, configuration-defined tiers of
  heterogeneous devices, with automatic promotion/demotion, explicit prefetch
  and pinning, and a transparent read-through cache tier.
- Erasure coding as a first-class feature, with tier-specific profiles and
  failure-domain-aware placement.
- Strong consistency for supported S3 operations. A write is acknowledged only
  after its configured durability requirement has been met.
- Horizontal scaling of aggregate throughput by adding storage nodes and
  stateless gateways.
- Explicit failure over silent data loss. Recoverable, idempotent, observable
  operations.
- Operational simplicity: a small team should be able to run it.

## Non-goals (for now)

- File system or block semantics (POSIX, NFS, iSCSI).
- Reimplementing distributed consensus or erasure-coding algorithms that
  mature, well-tested libraries already provide.
- Embedding any environment-specific capacity, bandwidth, or performance
  targets. Everything environment-specific is configuration.

## Documentation

| Document | Purpose |
| --- | --- |
| [Architecture overview](docs/architecture/overview.md) | Components, planes, data flow |
| [Design decisions](docs/architecture/design-decisions.md) | Key decisions with alternatives and trade-offs |
| [Technology selection](docs/architecture/technology-selection.md) | Libraries and systems under evaluation |
| [Repository layout](docs/architecture/repository-layout.md) | Workspace and crate structure |
| [Risks](docs/architecture/risks.md) | Correctness, durability, performance, and project risks |
| [Open decisions](docs/architecture/open-decisions.md) | Items that still need a decision |
| [RFC index](docs/rfcs/README.md) | RFC plan and dependency graph |
| [Phase 0 plan](docs/roadmap/phase-0.md) | Architecture and performance proof of concept |
| [Phase 1](docs/roadmap/phase-1.md) | Single-cluster durable object store and reliability hardening |
| [Phase 1.2](docs/roadmap/phase-1-2.md) | Distributed operations and failover |
| [Phase 2](docs/roadmap/phase-2.md) | Native multi-tier storage: cache tiers, read-through cache, automatic tiering |
| [Phase 2.1](docs/roadmap/phase-2-1.md) | Cache protection (replication / erasure coding) and Linux validation: measurements, fault injection |

## Contributing and policy

This project is developed in public. All code, documentation, commit messages,
issues, and pull requests must be suitable for public release and must not
contain credentials, internal hostnames, or non-public infrastructure details.
See [`docs/CONTRIBUTING-POLICY.md`](docs/CONTRIBUTING-POLICY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE) (see
[ADR-0001](docs/adr/0001-license.md)). Contributions require a Developer
Certificate of Origin sign-off; see [CONTRIBUTING.md](CONTRIBUTING.md).
