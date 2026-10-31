# Repository layout

Status: **proposal**. This adjusts the initial suggestion to match the
component boundaries in the [architecture overview](overview.md). The split
separates *pure, testable logic* (placement, tiering, ec, metadata recipes)
from *processes* (gateway, storage node, controller) so that the logic crates
can be exercised in deterministic tests without networking or disks.

```text
arca-lake/
├── Cargo.toml                 # workspace; shared version/edition/license/lints
├── rust-toolchain.toml
├── deny.toml                  # cargo-deny license and advisory policy
├── crates/
│   ├── common/                # arca-lake-common: IDs, checksums, errors, config loading, telemetry init
│   ├── protocol/              # arca-lake-protocol: versioned protobuf definitions and generated code
│   ├── ec/                    # arca-lake-ec: ErasureCoder trait, profiles, stripe layout, backends
│   ├── metadata/              # arca-lake-metadata: MetadataStore trait, key layout, transaction recipes,
│   │                          #   FoundationDB backend, in-memory backend
│   ├── storage/               # arca-lake-storage: local storage engine (segments, index, scrub, compaction)
│   ├── storage-node/          # arca-lake-storage-node: gRPC server around the engine, epoch enforcement
│   ├── placement/             # arca-lake-placement: topology model, stripe-group logic, balancing (pure)
│   ├── tiering/               # arca-lake-tiering: policies, admission/eviction, migration state machines (pure)
│   ├── controller/            # arca-lake-controller: leader election, control loops, repair, GC, admin API
│   ├── gateway/               # arca-lake-gateway: S3 frontend, read/write protocols, auth
│   ├── testkit/               # arca-lake-testkit: in-process cluster harness, fault injection, fixtures
│   └── cli/                   # arca-lake: single binary with `gateway`, `storage-node`, `controller`, `admin`
├── docs/
│   ├── architecture/
│   ├── adr/
│   ├── rfcs/
│   └── roadmap/
├── tests/                     # cross-crate integration and compatibility test drivers
├── benchmarks/                # criterion micro-benchmarks and end-to-end harness, with environment templates
├── deploy/                    # container images, compose files for local clusters, example configs
├── .github/workflows/         # CI: fmt, clippy, test, deny, audit, secret scan, docs build
├── README.md
├── CONTRIBUTING.md
├── CODE_OF_CONDUCT.md
├── SECURITY.md
├── CHANGELOG.md
└── LICENSE
```

## Crate naming

Each crate's `Cargo.toml` uses the `arca-lake-` prefix and inherits workspace
metadata:

```toml
[package]
name = "arca-lake-storage"
version.workspace = true
edition.workspace = true
license.workspace = true
```

In Rust code the crate is referenced with underscores:

```rust
use arca_lake_storage::StorageEngine;
```

The CLI crate publishes the binary `arca-lake`.

## Dependency direction

```text
cli ─▶ gateway ─▶ metadata, ec, protocol, common
    ─▶ storage-node ─▶ storage, protocol, common
    ─▶ controller ─▶ placement, tiering, metadata, protocol, common
placement, tiering, ec, storage ─▶ common  (no dependency on metadata or protocol)
testkit ─▶ everything (dev-dependency only)
```

Rules:

- `placement`, `tiering`, `ec`, and `storage` must not depend on networking
  or on the metadata store. They receive inputs as plain data and return
  decisions or results, which keeps them property-testable.
- `protocol` owns wire formats; `storage` owns on-disk formats. Both carry
  explicit version fields and have golden-file compatibility tests.
- Only `metadata` knows the key layout.

## Workspace conventions

- Edition 2024, MSRV pinned in `rust-toolchain.toml` and checked in CI.
- `#![forbid(unsafe_code)]` everywhere except crates that wrap FFI or
  io_uring, which must document each `unsafe` block.
- Clippy `pedantic` as warnings with an explicit allow-list in the workspace
  `[lints]` table.
- All configuration structs derive a JSON schema that is checked into
  `deploy/` and validated in CI, so that example configurations cannot drift.

## What exists today

Only `README.md` and `docs/`. The workspace skeleton is the first Phase 0
task; see [the Phase 0 plan](../roadmap/phase-0.md).
