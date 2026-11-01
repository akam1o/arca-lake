# Open decisions

Status: **living document**. Items that need a decision by the maintainers.
Each has a recommendation; none is decided until recorded in an ADR.

## Decided (recorded in ADRs)

| ID | Decision | Outcome | ADR |
| --- | --- | --- | --- |
| OD-01 | License | Apache-2.0 | [ADR-0001](../adr/0001-license.md) |
| OD-02 | Documentation language | English canonical; translations optional | [ADR-0002](../adr/0002-documentation-language.md) |
| OD-03 | Governance basics | DCO sign-off | [ADR-0003](../adr/0003-contribution-sign-off.md) |
| OD-04 | Supported platforms | Linux production; macOS development only; minimum kernel stated in RFC-005 | [ADR-0004](../adr/0004-supported-platforms.md) |
| OD-11 | CopyObject data handling | Physical copy in the MVP | [ADR-0005](../adr/0005-initial-architecture-decisions.md) |
| OD-12 | Where EC encode happens | Gateway | ADR-0005 |
| OD-13 | Metadata layer deployment | Embedded library behind a trait | ADR-0005 |
| OD-14 | Small object handling | Replication below a profile threshold | ADR-0005 |
| OD-15 | Durable placement representation | Stripe groups | ADR-0005 |

## Needs a decision during or at the end of Phase 0

| ID | Decision | Options | Recommendation | Evidence needed |
| --- | --- | --- | --- | --- |
| OD-05 | Metadata store | FoundationDB; TiKV | FoundationDB. The write-protocol recipes pass unchanged against a real single-process cluster (`fdb` feature, containerized test and CI job); the server enforced the value and transaction limits exactly where the in-memory emulation predicted, and conflict detection resolved concurrent conditional creates to a single winner. Outstanding: publishable latency, listing, and hot-spot measurements | Measurements on a described environment |
| OD-06 | S3 protocol layer | `s3s`; hand-written on hyper | `s3s`. Spike findings so far (`arca-lake-gateway`, AWS CLI smoke test): SigV4 and `aws-chunked` bodies with checksum trailers (the CLI default) work unchanged; S3 error codes map faithfully (`PreconditionFailed`, `SignatureDoesNotMatch`, `InvalidBucketName`); the operation DTOs track the AWS model closely (e.g., `ETagCondition` for `If-Match`). Outstanding: streaming backpressure behaviour under slow clients, overhead measurements, a second SDK | Phase 0 S3 spike measurements |
| OD-07 | Default EC backend | ISA-L via FFI; pure-Rust classic RS; `reed-solomon-simd` | Classic GF(2^8) scheme as the default (ISA-L for production once bindings are evaluated, pure Rust as oracle and fallback); Leopard optional for wide profiles. First evidence: [EC backend report](../../benchmarks/reports/2026-11-01-ec-backends.md) | Server-hardware rerun of the EC benchmarks, FFI binding survey |
| OD-08 | Shard transfer protocol | gRPC streaming; raw framed TCP | gRPC (tonic) with several HTTP/2 connections per storage node and TLS on. Loopback measurements ([report](../../benchmarks/reports/2026-11-01-transfer-grpc-vs-raw.md)): under TLS both transports are within ~20% in throughput; raw framing costs ~1.5× less CPU per byte but is more code and produced a TLS flush deadlock bug in its first version. Revisit in Phase 4 only if CPU per byte is the bottleneck | Server-hardware confirmation; ADR to be written with Phase 1 |
| OD-09 | Default disk I/O backend for storage nodes | blocking pool; io_uring | blocking pool for Phase 1 unless io_uring shows a clear, reproducible win | Phase 0 I/O benchmarks |
| OD-10 | On-disk record checksum | CRC32C; CRC64-NVME | CRC32C unless CRC64 reuse with S3 checksums is shown to save a hashing pass | Phase 0 hashing measurements |

## Design decisions that need maintainer confirmation

| ID | Decision | Options | Recommendation |
| --- | --- | --- | --- |
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
