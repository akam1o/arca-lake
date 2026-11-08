# arca-lake

**arca-lake** is an S3-compatible distributed object store written in
Rust: stateless S3 gateways, storage nodes with an append-only engine,
FoundationDB for metadata, erasure coding or replication per profile,
native multi-tier storage (durable tiers, read-through cache tiers,
automatic tiering), integrity scrubbing and repair, and capacity
admission with backpressure.

> **Status: experimental, not released.** There is no release yet. The
> code on `main` is under active development; it has been verified by
> automated tests and short, dense load and fault-injection runs on
> virtual machines, **not** by long-duration operation or on physical
> storage. Do not store data you cannot afford to lose. What is and is
> not verified is listed per feature in the RFCs, the phase reports and
> [Known limitations](docs/guide/known-limitations.md).

## Start here

| Document | Purpose |
| --- | --- |
| [Quick start](docs/guide/quick-start.md) | A development cluster on one Linux host |
| [Architecture overview](docs/architecture/overview.md) | Components, planes, data flow |
| [Supported platforms](docs/guide/supported-platforms.md) | Operating systems, FoundationDB, toolchain |
| [S3 compatibility](docs/guide/s3-compatibility.md) | What is implemented, verified, not supported |
| [Configuration](docs/guide/configuration.md) | The settings that matter and where they are documented |
| [Operational requirements](docs/guide/operations.md) | What a deployment must provide |
| [Security model](docs/guide/security-model.md) | Identities, TLS, authorization, threat model |
| [Backup and restore](docs/guide/backup-restore.md) | Metadata backup, restore, recovery |
| [Troubleshooting](docs/guide/troubleshooting.md) | Symptoms to runbooks |
| [Known limitations](docs/guide/known-limitations.md) | What is experimental, not supported or not verified |
| [Documentation index](docs/README.md) | RFCs, ADRs, roadmaps, verification reports |

## Goals

- Broad Amazon S3 API compatibility, implemented incrementally and tracked
  in an explicit compatibility matrix. Nothing is claimed that is not
  implemented and verified.
- Erasure coding as a first-class feature, with failure-domain-aware
  placement and per-tier profiles.
- Native multi-tier storage: configuration-defined tiers, promotion and
  demotion, a read-through cache tier that never carries durability.
- Strong consistency for supported operations: a write is acknowledged
  only after its durability requirement has been met.
- Explicit failure over silent data loss; recoverable, idempotent,
  observable operations; capacity limits that fail closed.
- Nothing environment-specific in the code: capacities, bandwidths and
  performance targets are configuration.

## Non-goals (for now)

- File system or block semantics (POSIX, NFS, iSCSI).
- Reimplementing consensus or erasure-coding algorithms that mature
  libraries provide.
- Sharing one file system between storage nodes (see
  [Operational requirements](docs/guide/operations.md)).

## Contributing and policy

Contributions are welcome under the rules in [CONTRIBUTING.md](CONTRIBUTING.md)
(Developer Certificate of Origin sign-off). Everything in this repository
must be suitable for public release: no credentials, internal hostnames
or non-public infrastructure details
([policy](docs/CONTRIBUTING-POLICY.md)). Security issues: see
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE)
([ADR-0001](docs/adr/0001-license.md)).
