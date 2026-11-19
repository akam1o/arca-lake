# Repository layout

This page maps the current workspace to the components in the
[architecture overview](overview.md). The workspace is implemented;
it is no longer the initial collection of stubs. The root
[Cargo.toml](../../Cargo.toml) is the authoritative list of members,
shared dependencies, versions, and lints.

## Implementation

| Path | Responsibility |
| --- | --- |
| `crates/common/` | Shared profiles, compatibility constants, metrics, and process utilities |
| `crates/protocol/` | Storage protobuf definitions, gRPC clients, and service TLS identities |
| `crates/ec/` | Erasure-coding and replication parameters, schemes, layouts, and backends |
| `crates/metadata/` | KV abstraction, FoundationDB and in-memory backends, records, transaction recipes, and fencing |
| `crates/storage/` | Local append-only engine, index, on-disk format, and crash/fault tests |
| `crates/storage-node/` | Storage RPC service, capacity controls, node identity, delete fences, and deferred reclamation |
| `crates/placement/` | Failure-domain-aware placement |
| `crates/tiering/` | Cache and durable-tier policies and migration logic |
| `crates/controller/` | Leader election and background passes for repair, GC, capacity, tiering, recovery, and node management |
| `crates/gateway/` | S3 frontend, authentication and policy evaluation, distributed reads/writes, and access recording |
| `crates/testkit/` | Cluster harnesses, fault injection, fixtures, and integration suites |
| `crates/cli/` | The `arca-lake` executable: `gateway`, `storage-node`, `controller`, and `admin` commands |

Library crates use the `arca-lake-` prefix in Cargo and underscores in Rust
imports. The CLI package and binary are named `arca-lake`.

The CLI assembles the components. Metadata transaction rules live in
`metadata`, wire formats in `protocol`, and on-disk formats in `storage`.
Changes to those boundaries need the design review and compatibility checks
described in [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Tests, deployment helpers, and documentation

| Path | Contents |
| --- | --- |
| `crates/*/tests/` | Crate and cross-component tests, golden fixtures, and model tests |
| `tests/compat/` | Python client compatibility driver |
| `benchmarks/` | Transfer, S3, metadata, and load harnesses; reports and preserved measurement data |
| `deploy/dev/` | FoundationDB development/test helpers and network-fault harness |
| `deploy/backup/` | Backup verification helper |
| `deploy/prometheus/` | Example alert rules |
| `.github/workflows/` | Build, test, dependency, secret, and public-artifact checks |
| `docs/guide/` | Current user and operator guides |
| `docs/architecture/` | Current overview and risk register, plus labelled initial design analyses |
| `docs/adr/`, `docs/rfcs/` | Decisions, detailed designs, and acceptance criteria |
| `docs/verification/` | Version- and environment-scoped verification reports |
| `docs/roadmap/` | Current roadmap index and labelled historical phase plans |
| `docs/release/`, `docs/security/` | Publication preparation, threat model, and artifact policy |
| `docs/archive/` | Historical development records and links |

Deployment helpers are evaluation and test tools. They are not a published
container distribution. Follow the [quick start](../guide/quick-start.md)
and [operational requirements](../guide/operations.md) for prerequisites.

## Workspace conventions

- Edition 2024; the toolchain is pinned in
  [rust-toolchain.toml](../../rust-toolchain.toml), and the workspace
  manifest declares the minimum Rust version.
- `unsafe_code = "deny"` is a workspace lint. An exception for a low-level
  wrapper needs a narrow allowance and a `SAFETY` explanation.
- Clippy `pedantic` is enabled with an explicit allow-list in the workspace.
- Wire, metadata, and disk compatibility have separate version checks and
  golden fixtures; see [RFC-014](../rfcs/RFC-014-upgrade-and-compatibility.md).
- Configuration and its validation are described in the
  [configuration guide](../guide/configuration.md) and each command's `--help`.
