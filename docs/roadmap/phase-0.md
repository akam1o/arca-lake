# Phase 0: Architecture and performance proof of concept

Status: **in progress**. Phase 0 exists to replace hypotheses with measurements
before Phase 1 commits to a design. It produces no user-facing product.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 0.1 skeleton and CI | done | fmt, clippy, test, doc, cargo-deny, gitleaks CLI, DCO, link check |
| 0.2 EC evaluation | done (first pass) | `arca-lake-ec`; [laptop report](../../benchmarks/reports/2026-11-01-ec-backends.md); ISA-L and server-hardware run outstanding |
| 0.3 storage engine prototype | first iteration | `arca-lake-storage`: format v1, durable put/delete, range reads, recovery, scrub, crash tests, I/O benchmark. Outstanding: group commit, `O_DIRECT`, io_uring backend (Linux), process-kill crash test in CI, `fio` comparison |
| 0.4 protocol and transfer | not started | |
| 0.5 metadata spike | backend done, measurements outstanding | `arca-lake-metadata`: KV abstraction with FoundationDB semantics, layout v1, write-protocol recipes, concurrency tests; FoundationDB backend (`fdb` feature) passing the protocol tests against a real single-process cluster in a container (`deploy/dev/fdb-test.sh`) and in CI. Outstanding: publishable latency/listing/hot-spot measurements on a described environment |
| 0.6 S3 spike | not started | |
| 0.7 single-node harness | not started | |
| 0.8 multi-node functional test | not started | |
| 0.9 RFCs and ADRs | ADR-0001..0005 done | RFC-001/002 drafts outstanding |

## Scope

In scope:

- Workspace skeleton, CI, and project hygiene.
- Erasure coding micro-benchmarks across backends and profiles.
- Local chunk I/O engine prototype with crash-consistency tests.
- Network shard transfer measurements (gRPC vs. raw framing, TLS on/off).
- Metadata store spike: key layout prototype and transaction latency on a
  local FoundationDB cluster.
- S3 protocol layer spike with `s3s` driving an in-memory backend.
- Single-node end-to-end harness that chains the above.
- Multi-node functional test in containers; a multi-node benchmark only if a
  publicly describable environment is available.
- Draft RFC-001 and RFC-002; draft outlines of RFC-003, 004, 005.

Out of scope:

- Any S3 feature beyond what the spike needs (PutObject, GetObject,
  HeadObject, ListObjectsV2 subset).
- Tiering, repair, GC, authentication beyond SigV4 parsing.
- Performance tuning. Phase 0 measures; it does not optimize.

## Dependencies

- Rust stable toolchain; Linux hosts for io_uring experiments (macOS can run
  everything else with the blocking I/O backend).
- Docker or Podman for FoundationDB and multi-node containers.
- A publicly describable benchmark environment (for example a cloud instance
  type with stated vCPU, memory, NVMe, and network class) for any published
  numbers. Numbers from undisclosed hardware are not published.

## Implementation tasks

### 0.1 Project skeleton (prerequisite for everything)

- Workspace with crates listed in [repository layout](../architecture/repository-layout.md), each with a `lib.rs` containing only the public trait or type stubs.
- CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo nextest`, `cargo deny check`, `cargo audit`, secret scanning, docs link check.
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, issue and PR templates that include the public-repository checklist.
- `deny.toml` with the permissive-license allow-list.
- Decision: license file (see [open decisions](../architecture/open-decisions.md)).

### 0.2 `arca-lake-ec`: erasure coding evaluation

- `ErasureCoder` trait: `encode(data_shards) -> parity_shards`,
  `reconstruct(available, missing_indices)`, `scheme_id()`.
- Backends: pure-Rust classic RS (existing crate), ISA-L via FFI (feature
  gated), `reed-solomon-simd` (feature gated).
- Correctness: property tests (any `k` of `k+m` reconstructs), cross-backend
  compatibility tests for backends claiming the same scheme, golden vectors
  checked into the repository for cross-version stability.
- Benchmarks (criterion): profiles `4+2`, `6+3`, `8+3`, `8+4`, `12+4`,
  `16+4`; shard sizes from 64 KiB to 4 MiB; encode; reconstruct 1 shard;
  reconstruct `m` shards; single-threaded and N-threaded.

### 0.3 `arca-lake-storage`: local chunk I/O prototype

- Segment format v1: header, framed records, per-record checksum, footer.
- Writer with group commit; reader with range reads; recovery scan.
- Index behind a trait; first implementation RocksDB; rebuild-from-segments
  path.
- `ChunkIo` backends: blocking thread pool; io_uring on the low-level crate
  (Linux only, feature gated).
- Benchmarks: sequential append throughput at several record sizes; random
  range read latency/throughput; fsync strategies (per record, group commit
  windows, `fdatasync` vs `fsync`); buffered vs `O_DIRECT`; compared against
  `fio` on the same device as a ceiling.
- Crash tests: kill the process during writes (SIGKILL loop), truncate
  segment tails, flip bytes; verify recovery never returns a corrupt record as
  valid and never loses an acknowledged record. Run under a disk-failure
  simulation layer that can drop or delay writes.

### 0.4 `arca-lake-protocol` and transfer measurements

- Protobuf definitions for `PutShard`, `GetShard(range)`, `DeleteShard`,
  `ListShards(group)`, with protocol version in every message.
- Minimal storage-node server and client over tonic.
- Comparison harness: tonic streaming vs. raw length-prefixed TCP, both via
  rustls and plaintext, at shard sizes from 64 KiB to 4 MiB, 1 to N
  concurrent streams. Measure throughput, CPU per byte, p99 latency.

### 0.5 `arca-lake-metadata`: FoundationDB spike

- `MetadataStore` trait with in-memory and FoundationDB implementations.
- Key layout v1 for buckets, objects, versions, pending generations,
  manifest/placement entries, stripe groups.
- Transaction recipes: allocate pending generation, append manifest entries,
  commit version (with conditional checks), list with prefix/delimiter,
  tombstone.
- Tests: concurrent commits to the same key (exactly one conditional create
  succeeds); manifest entries exceed one transaction's budget and still commit
  correctly; lease expiry.
- Measurements: commit latency distribution, list throughput, write hot-spot
  behavior under sequential keys, all on a local 3-process cluster. Local
  numbers are for relative comparison only and are labelled as such.

### 0.6 `arca-lake-gateway`: S3 protocol spike

- `s3s`-based service with an in-memory object map; SigV4 on.
- Verify with AWS CLI and at least two SDKs: PutObject, GetObject (with
  Range), HeadObject, ListObjectsV2, DeleteObject.
- Measure HTTP+SigV4 overhead with large bodies: throughput and CPU with and
  without TLS, with and without MD5 and SHA-256 computation.
- Record findings on streaming, backpressure, and error mapping for the
  `s3s` ADR.

### 0.7 Single-node end-to-end harness

- Wire 0.2–0.6 into a single process ("all-in-one") that executes the write
  protocol from DD-4 against local storage engines on one or more directories.
- Benchmarks: PUT and GET throughput for several object sizes and EC
  profiles; CPU breakdown (TLS, hashing, EC, I/O) via profiling.
- Publish a benchmark report template that requires the full environment
  description.

### 0.8 Multi-node functional test

- Compose file: FoundationDB, N storage-node containers, one gateway.
- Functional: write with `k+m` shards across N containers; kill a container;
  degraded read succeeds; restart; reads succeed.
- Benchmark only in a publicly describable environment; otherwise record
  "not measured".

### 0.9 Design documents

- RFC-001 and RFC-002 drafts to `review`.
- RFC-003, 004, 005 drafts with Phase 0 results linked.
- ADRs: license, metadata store, EC backend, S3 protocol layer.

## Tests (Phase 0 definition of "tested")

- Unit and property tests in every crate with logic.
- Crash-consistency tests for the storage engine run in CI on Linux.
- Cross-backend EC compatibility tests and golden vectors.
- Metadata concurrency tests.
- S3 spike verified by AWS CLI and SDK smoke tests in CI.

## Acceptance criteria

1. CI is green on every merge with fmt, clippy, tests, license and advisory
   checks, and secret scanning.
2. EC benchmark report exists for all listed profiles and backends, with a
   recommendation and an ADR draft.
3. Storage engine prototype passes crash tests: no acknowledged record is
   lost and no corrupt record is returned as valid across at least 1,000
   randomized crash iterations in CI.
4. Transfer measurement report compares gRPC and raw framing with and without
   TLS and states a recommendation.
5. Metadata spike demonstrates the write protocol end to end including the
   over-budget manifest case and the concurrent conditional create case.
6. S3 spike passes the smoke tests with AWS CLI and two SDKs.
7. Single-node end-to-end benchmark report exists with a complete environment
   description and a CPU breakdown.
8. Multi-node functional test passes in containers, including one storage
   node failure.
9. RFC-001 and RFC-002 are in review; RFC-003, 004, 005 exist as drafts.
10. The risk list is updated with Phase 0 findings; each hypothesis in the
    design documents is marked *Verified*, *Refuted*, or still *Hypothesis*
    with a reason.

## Risks specific to Phase 0

| Risk | Mitigation |
| --- | --- |
| Benchmarks on developer machines leak into documents as if general | Report template requires environment disclosure; CI rejects reports without it; local numbers labelled "relative only" |
| ISA-L FFI binding effort larger than expected | Pure-Rust backend is the baseline; ISA-L is optional for Phase 0 acceptance |
| io_uring experiments blocked by kernel availability in CI | Run io_uring tests on a self-hosted or cloud Linux runner; blocking backend remains the default |
| `s3s` does not fit streaming/backpressure needs | Spike is time-boxed; fallback is a hand-written hyper layer for the few operations Phase 1 needs |
| FoundationDB native client complicates builds | Provide a dev container; keep the in-memory backend so most tests do not need FDB |
| Scope creep into Phase 1 features | Scope list above is the gate; new items need a plan change |

## Estimated effort

Estimates are rough, in engineer-weeks for one experienced engineer working
full time, and will be revised:

| Task | Estimate |
| --- | --- |
| 0.1 skeleton and CI | 1 |
| 0.2 EC evaluation | 1–2 |
| 0.3 storage engine prototype and crash tests | 3–4 |
| 0.4 protocol and transfer measurements | 1–2 |
| 0.5 metadata spike | 2–3 |
| 0.6 S3 spike | 1–2 |
| 0.7 single-node harness and report | 2 |
| 0.8 multi-node functional test | 1 |
| 0.9 RFCs and ADRs | 2 (ongoing) |
| **Total** | **14–19** |

Tasks 0.2 through 0.6 are independent and parallelize across people.
