# Technology selection

> **Initial technology survey (2026-11-01).** Recommendations and
> alternatives below record the initial evaluation, not the current
> dependency inventory. Later choices are recorded in the
> [ADR index](../adr/README.md); the [architecture overview](overview.md)
> and [workspace manifest](../../Cargo.toml) describe the implementation.
> Version checks and proposed experiments retain their original date and scope.

Original status: **proposal**. Facts marked *Verified* were checked against the cited
primary source on 2026-11-01. Everything else is an assessment or hypothesis.
Final selections are recorded as ADRs after Phase 0 measurements.

Evaluation criteria for every item: performance, correctness, operational
complexity, long-term maintenance, community support, license compatibility
with an Apache-2.0 project, and security.

## Summary table

| Area | Recommendation | Alternatives kept | Decision point |
| --- | --- | --- | --- |
| Async runtime | Tokio | thread-per-core runtime for storage node if measurements justify | Phase 0 |
| HTTP server | hyper 1.x; axum for the admin API | — | Now |
| S3 protocol layer | Evaluate `s3s` as the request/response layer behind our own `S3` trait implementation | Hand-written parser on hyper | Phase 0 spike |
| Internal RPC | tonic (gRPC) for control messages; measure gRPC vs. raw framed TCP for shard streaming | Raw length-prefixed TCP for data | Phase 0 |
| Metadata store | FoundationDB | TiKV | Phase 0 spike, then ADR |
| Embedded KV (index only) | RocksDB via `rocksdb` crate | custom index, `redb`, `fjall` | Phase 1 |
| Erasure coding | Classic GF(2^8) Reed–Solomon via ISA-L or a Rust implementation; `reed-solomon-simd` evaluated | — | Phase 0 benchmark |
| Disk I/O | Abstraction with a blocking/`std` backend first; io_uring backend measured | SPDK deferred | Phase 0 |
| TLS | rustls | — | Now |
| Observability | OpenTelemetry (traces), Prometheus exposition, `tracing` for structured logs | — | Now |
| Hashing | MD5 (ETag), SHA-256/SHA-1/CRC32/CRC32C/CRC64NVME (S3 checksums), CRC32C or CRC64 for on-disk records | BLAKE3 for internal whole-chunk hashes | Phase 0 |
| Testing | proptest, loom, cargo-nextest, deterministic in-process cluster harness, Ceph `s3-tests`, AWS SDKs | madsim-style deterministic simulation (evaluate) | Phase 0–1 |
| Supply chain | cargo-deny (licenses, advisories), cargo-audit, secret scanning in CI | — | Now |

## Runtime and networking

### Tokio

Industry-standard async runtime in Rust with the broadest ecosystem. Used by
hyper, tonic, axum. *Assumption*: Tokio's work-stealing scheduler is
adequate for gateways. For storage nodes, a thread-per-core design may reduce
cross-core traffic for disk I/O; this is a Phase 0 hypothesis, not a decision.

### hyper / axum

*Verified*: hyper 1.12.0 (MIT), axum 0.8.9 (MIT). hyper is the HTTP engine
for the gateway. axum is convenient for the admin API where ergonomics matter
more than raw overhead.

### tonic / gRPC

*Verified*: tonic 0.14.9 (MIT). gRPC gives typed, versioned, streaming RPC
with TLS and is appropriate for control traffic and for shard transfer.
*Measured* (loopback, laptop, relative; [report](../../benchmarks/reports/2026-11-01-transfer-grpc-vs-raw.md)):
with TLS on, gRPC with one connection per stream is within about 20% of a
raw length-prefixed TCP protocol in throughput, at roughly 1.5× the CPU per
byte and somewhat more memory (HTTP/2 windows). Flow control works end to
end for both. The raw protocol's first version deadlocked under TLS because
of a missing flush at message boundaries, which gRPC handles internally.
Recommendation: gRPC for all internal RPC including shard transfer, with
several HTTP/2 connections per peer (OD-08).

### rustls

*Verified*: rustls 0.23.46 (Apache-2.0 OR ISC OR MIT). Memory-safe TLS;
avoids OpenSSL build and vulnerability surface. TLS throughput is measured in
Phase 0 because it is on the gateway's critical CPU path.

## S3 protocol layer

### `s3s`

*Verified* (crates.io and project README, 2026-09): version 0.17.0,
Apache-2.0, implements the S3 REST API as a hyper service with data types
generated from the AWS Smithy model, companion crates for SigV4/SigV2
parsing, `aws-chunked` decoding, a policy language model, and an
end-to-end test harness. The README describes the project as experimental,
states that the adapter is not a complete security boundary, and documents
that body-size limits, rate limits, and backpressure are the integrator's
responsibility.

Trade-offs: adopting it removes a very large amount of XML/DTO work and
tracks the AWS model; the cost is dependency on an externally governed
project for the protocol surface, and we must verify that its streaming and
error-mapping behavior meets our backpressure and compatibility needs.

Alternative: write the protocol layer directly on hyper. Full control, large
ongoing cost.

**Recommendation.** Spike `s3s` in Phase 0 behind our own `S3` service trait
implementation. Decide by ADR with the spike's findings on streaming,
backpressure, error fidelity, and maintenance cadence.

## Metadata store

See [DD-5](design-decisions.md#dd-5-metadata-store-selection) for the
comparison. *Verified* facts for FoundationDB come from the official "Known
Limitations" page (v8.0 documentation): value ≤ 100,000 bytes, key ≤ 10,000
bytes, transaction ≤ 10,000,000 bytes of affected data, transaction lifetime
≤ 5 seconds, rotational disks not recommended with the ssd storage engine,
testing envelope of ~500 processes and ~100 TB, and no user-level access
control (network isolation required).

*Verified*: the `foundationdb` crate is at 0.11.0 (MIT/Apache-2.0) with
features for FDB API versions through 7.4; it requires the native
`libfdb_c` client library at build and run time.

*Verified*: `tikv-client` 0.4.0 (Apache-2.0) was published 2026-02.

Security note: because FoundationDB has no access control, the metadata
cluster must live on an isolated network reachable only by arca-lake
components, with TLS enabled.

## Embedded key-value store (shard index only)

*Verified*: RocksDB is dual-licensed GPL-2.0 or Apache-2.0 (repository
`COPYING` and `LICENSE.Apache`), and the `rocksdb` crate is 0.25.0
(Apache-2.0). Use is restricted to the per-device shard index and small
local state; object bytes never go into it. Pure-Rust alternatives can be
evaluated later behind the index trait; the index is rebuildable from
segments, so this choice is reversible.

## Erasure coding

Requirements: systematic code (data shards are the original bytes, so healthy
reads need no decode), configurable `k` and `m` in the ranges common for
storage (`k` up to a few tens, `m` up to a handful), fast single-shard
reconstruction (the dominant repair and degraded-read case), stable
cross-version encoding (parity written by version N must decode with version
N+1), permissive license.

| Library | *Verified* facts | Assessment |
| --- | --- | --- |
| **Intel ISA-L** | BSD-3-Clause (repository `LICENSE`). C library with SIMD GF(2^8) RS, CRC, hashing. | Industry standard for storage EC. Needs FFI bindings and a C build; the Rust binding situation must be surveyed (*Open*). Strong candidate for the default backend on x86-64 and AArch64. |
| **`reed-solomon-simd`** | 3.1.0, MIT AND BSD-3-Clause; Leopard-RS–derived O(n log n) code over GF(2^16); SIMD on x86-64 and AArch64; README benchmarks show decode throughput essentially independent of how many shards are lost and note that another crate may be faster at decoding with about 42 or fewer recovery shards; shard size must be even; one-time table initialization. | Excellent for very wide codes. For storage-typical `m ≤ 6`, its decode cost does not shrink for single-shard loss, which is our common case. Also not wire-compatible with classic RS. *Hypothesis*: not the best fit for the default profile; worth keeping as an optional backend for wide profiles. Phase 0 measures. |
| **`reed-solomon-erasure`** | 6.0.0, MIT; classic GF(2^8) RS with SIMD; last release 2022-09 (crates.io). | Correct and widely used, but low maintenance activity is a long-term risk. Acceptable as a pure-Rust fallback or test oracle. |

**First measurement (2026-11-01, single core, developer laptop; relative
comparison only).** See
[the report](../../benchmarks/reports/2026-11-01-ec-backends.md). The classic
GF(2^8) backend reconstructed a single lost shard 6–14× faster than the
Leopard backend across all profiles, while the Leopard backend encoded
1.2–2.3× faster. This supports the hypothesis above for the common
single-shard loss case; confirmation on server hardware is still needed.

**Recommendation.** Define `ErasureCoder` as a trait; implement a pure-Rust
classic RS backend (via an existing crate) for portability and as a test
oracle, and an ISA-L backend as the performance option. The EC *profile*
records the scheme identifier so that a backend change never changes the
meaning of stored parity. Cross-backend compatibility tests (encode with one,
decode with the other) are mandatory for backends claiming the same scheme.

## Disk I/O

| Option | *Verified* facts | Assessment |
| --- | --- | --- |
| Blocking `std::fs` on a dedicated thread pool | — | Baseline; portable; adequate for a correctness-first Phase 0–1. |
| io_uring via `tokio-uring` | 0.5.0 (MIT), released 2024-05; README states the project is young and requires a recent kernel; it needs its own runtime wrapper. | Low release cadence is a maintenance risk. |
| io_uring via the low-level `io-uring` crate | 0.7.15 (MIT OR Apache-2.0), released 2026-09. | Actively maintained; we would write a small driver for the storage engine's needs (read, write, fsync, registered buffers). Linux-only. |
| SPDK | BSD-3-Clause. | Requires raw-device engine (DD-2 option C), hugepages, dedicated cores. Deferred until measurements show the kernel I/O path is the bottleneck. |

**Recommendation.** A `ChunkIo` abstraction with a blocking backend first; an
io_uring backend built on the low-level crate as a Phase 0 experiment; adopt
by measurement. macOS development uses the blocking backend.

## Hashing and checksums

*Verified*: `crc32c` 0.6.8 (Apache-2.0/MIT), `crc64fast-nvme` 1.2.1 (MIT OR
Apache-2.0), `blake3` 1.8.7 (CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH
LLVM-exception). MD5 is required for ETag compatibility; SHA-256, SHA-1,
CRC32, CRC32C, and CRC64NVME are required for the S3 checksum feature set.
Internal record checksums use CRC32C or CRC64; Phase 0 measures throughput
on the gateway path where several hashes may run concurrently over the same
stream.

## Observability

*Verified*: `opentelemetry` 0.33.1 (Apache-2.0), `prometheus-client` 0.25.2
(Apache-2.0 OR MIT). `tracing` for structured logs with span context.
Metrics are defined in a registry crate so that names are stable and
documented.

## Testing

*Verified*: `proptest` 1.12.0, `loom` 0.7.2. Ceph's `s3-tests` README states
the suite has moved into the main `ceph/ceph` repository; we will consume it
from there. Major AWS SDKs (Rust, Python, Go, Java, JavaScript) and the AWS
CLI are used for compatibility verification. A deterministic in-process
cluster harness with fault-injection hooks is a Phase 0 deliverable.

## License compatibility

All libraries recommended above are under permissive licenses (MIT,
Apache-2.0, BSD-3-Clause, ISC, CC0) compatible with distributing arca-lake
under Apache-2.0. RocksDB's dual license permits choosing Apache-2.0.
`cargo-deny` will enforce an allow-list in CI; copyleft licenses are denied
by default and any exception requires an ADR.

## Items requiring further verification

- Availability and quality of Rust bindings for ISA-L (*Open*).
- Whether `s3s` streaming bodies integrate with our backpressure model
  (*Phase 0 spike*).
- FoundationDB client library packaging for container images and the
  version-coupling constraints during upgrades (*Phase 0*).
- io_uring kernel feature requirements and the minimum supported kernel we
  will state (*Open*).
