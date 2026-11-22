# arca-lake

**arca-lake** is an experimental S3-compatible distributed object store
written in Rust. It combines stateless S3 gateways, FoundationDB metadata,
an append-only storage engine, erasure coding or replication, and native
multi-tier storage.

> **Status: experimental, not production-ready.** 0.1.0 is the first
> experimental release (candidate; not yet published), for evaluation
> and development. The code is under active development; it has been verified by
> automated tests and short, dense load and fault-injection runs on
> virtual machines, **not** by long-duration operation or on physical
> storage. Do not store data you cannot afford to lose. What is and is
> not verified is listed per feature in the RFCs, the verification reports and
> [Known limitations](docs/guide/known-limitations.md).

## What it provides

| Area | Current implementation |
| --- | --- |
| S3 access | Core bucket and object operations, multipart uploads, versioning, conditional requests, checksums, and SigV4 authentication; see the [compatibility matrix](docs/guide/s3-compatibility.md) for the supported subset and differences from AWS S3 |
| Distributed storage | FoundationDB transactions for metadata, failure-domain-aware placement, per-profile erasure coding or replication, and storage nodes with an append-only engine |
| Storage tiers | Durable tiers, read-through cache tiers that do not carry durability, explicit migration, prefetch, pinning, and automatic tiering |
| Integrity and recovery | Scrubbing, repair, garbage collection, node lifecycle management, and metadata recovery with explicit [restore conditions](docs/guide/backup-restore.md) |
| Capacity control | Node admission and controller-published backpressure; refused writes return `503 SlowDown` with `Retry-After` |

These features have different verification boundaries. Implementation does
not imply production readiness or support for every S3 API.

## Try it

The [quick start](docs/guide/quick-start.md) walks through a local S3 smoke
test and a development cluster on one Linux host, including an AWS CLI
round trip and cleanup. Use the 0.1.0 Linux x86_64 binary archive once
published, or build from source; no container image or crates.io package
is published.

```sh
git clone https://github.com/akam1o/arca-lake.git
cd arca-lake
```

Use the Rust toolchain pinned in `rust-toolchain.toml`. The distributed
example also needs FoundationDB 7.3, and every host running the `fdb`
build (the release binary included) needs its client library. See
[supported platforms](docs/guide/supported-platforms.md) for tested
platforms and prerequisites.

## Documentation

| Document | Purpose |
| --- | --- |
| [Architecture overview](docs/architecture/overview.md) | Components, planes, data flow |
| [S3 compatibility](docs/guide/s3-compatibility.md) | What is implemented, verified, not supported |
| [Configuration](docs/guide/configuration.md) | The settings that matter and where they are documented |
| [Operational requirements](docs/guide/operations.md) | What a deployment must provide |
| [Security model](docs/guide/security-model.md) | Identities, TLS, authorization, threat model |
| [Backup and restore](docs/guide/backup-restore.md) | Metadata backup, restore, recovery |
| [Known limitations](docs/guide/known-limitations.md) | What is experimental, not supported or not verified |
| [Documentation index](docs/README.md) | All guides, design decisions, verification evidence, and historical material |

For development status, start with the [current roadmap](docs/roadmap/README.md)
and [verification index](docs/verification/README.md). The
[changelog](CHANGELOG.md) summarizes user-visible capabilities; detailed
pre-release work records are retained in the [archive](docs/archive/README.md).

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
