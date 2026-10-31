# Open decisions

Status: **living document**. Items that need a decision by the maintainers.
Each has a recommendation; none is decided until recorded in an ADR.

## Needs a decision before Phase 0 starts

| ID | Decision | Options | Recommendation | Why it blocks |
| --- | --- | --- | --- | --- |
| OD-01 | **License** | Apache-2.0; MIT; dual MIT/Apache-2.0 | Apache-2.0. All shortlisted dependencies are permissive and compatible (see [technology selection](technology-selection.md#license-compatibility)); Apache-2.0 adds an explicit patent grant, which matters for storage software. Final confirmation after `cargo-deny` runs on the real dependency tree. | The `LICENSE` file and headers must exist before the first public commit of code |
| OD-02 | **Documentation language** | English only; Japanese only; English canonical with translations | English canonical. It widens the contributor base and matches the ecosystem; translations can follow. | Affects every document written from now on |
| OD-03 | **Governance basics** | DCO sign-off; CLA; neither | DCO sign-off (low friction, standard for Apache-2.0 projects) | `CONTRIBUTING.md` content |
| OD-04 | **Minimum supported Linux kernel and platforms** | State a minimum kernel for io_uring features; support macOS for development only | Linux as the only production platform; macOS and non-io_uring Linux for development with the blocking backend | CI matrix and feature gating |

## Needs a decision during or at the end of Phase 0

| ID | Decision | Options | Recommendation | Evidence needed |
| --- | --- | --- | --- | --- |
| OD-05 | Metadata store | FoundationDB; TiKV | FoundationDB | Phase 0 metadata spike results (latency, hot spots, operability) |
| OD-06 | S3 protocol layer | `s3s`; hand-written on hyper | `s3s` if the spike confirms streaming and error fidelity | Phase 0 S3 spike |
| OD-07 | Default EC backend | ISA-L via FFI; pure-Rust classic RS; `reed-solomon-simd` | ISA-L for production profiles, pure Rust as oracle and fallback | Phase 0 EC benchmarks, FFI binding survey |
| OD-08 | Shard transfer protocol | gRPC streaming; raw framed TCP | gRPC unless measured overhead is material | Phase 0 transfer measurements |
| OD-09 | Default disk I/O backend for storage nodes | blocking pool; io_uring | blocking pool for Phase 1 unless io_uring shows a clear, reproducible win | Phase 0 I/O benchmarks |
| OD-10 | On-disk record checksum | CRC32C; CRC64-NVME | CRC32C unless CRC64 reuse with S3 checksums is shown to save a hashing pass | Phase 0 hashing measurements |

## Design decisions that need maintainer confirmation

| ID | Decision | Options | Recommendation |
| --- | --- | --- | --- |
| OD-11 | CopyObject and multipart copy data handling | Physical copy (single-owner chunks); shared chunks with reference counts | Physical copy for the MVP; revisit with a dedicated RFC if copy volume matters |
| OD-12 | Where EC encode happens | Gateway; primary storage node | Gateway (DD-9) |
| OD-13 | Metadata layer deployment | Embedded library; separate service | Embedded (DD-10) |
| OD-14 | Small object handling | Replication below threshold; packing; inline in metadata | Replication below a profile threshold (DD-13) |
| OD-15 | Durable placement representation | Stripe groups; per-chunk location lists | Stripe groups (DD-3) |
| OD-16 | Grace-period measurement for GC | Metadata commit versions; wall clock; both | Commit versions primary, wall clock as a conservative bound (DD-7) |
| OD-17 | `m_ack < m` (reduced acknowledgment) support in the MVP | Support as a profile option; defer | Defer to Phase 3; keep the field in the profile schema |
| OD-18 | Versioning in the MVP | Versioned and unversioned buckets from Phase 1; unversioned only | Both from Phase 1, because the generation model already supports it and retrofitting changes the key layout |
| OD-19 | Which S3 checksum algorithms are in the initial API | All five (CRC32, CRC32C, SHA-1, SHA-256, CRC64NVME); subset | All five for PutObject/GetObject; composite checksums for multipart in Phase 3 |
| OD-20 | Metadata partitioning for very large deployments | Single cluster; per-bucket-group clusters | Single cluster now; key layout must keep a bucket's keys self-contained so partitioning remains possible |

## Items we intentionally are not deciding yet

- Write-back cache (future RFC with its own durability model).
- Raw block device storage engine and SPDK (after measurement).
- Object Lock, event notifications, cross-site replication, STS (Phase 3+).
- Thread-per-core runtime for storage nodes (after measurement).

## How to close an item

Open a pull request that adds an ADR under `docs/adr/`, links the evidence,
and removes the item from this list (or moves it to the decided section of
the ADR index).
