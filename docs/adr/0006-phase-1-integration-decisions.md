# ADR-0006: Phase 1 integration decisions

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-003 §5.7, RFC-004 §5.13, RFC-005 §5.5 and §9

## Context

Phase 1 integrated the gateway, metadata library, storage engine, and node
protocol into one write and read path. Three choices that Phase 0 had left
open had to be made for the integration to proceed, and each now has
evidence: a Phase 0 measurement, a Phase 1 evaluation, or a Phase 1
measurement. Decisions that still lack evidence (metadata store final
confirmation, EC backend on server hardware, I/O backend, checksum
algorithm) remain in `docs/architecture/open-decisions.md`.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Internal RPC (OD-08) | gRPC over tonic for every storage-node RPC, including shard transfer; several HTTP/2 connections per node when fan-out is needed; TLS on in production | Phase 0 loopback report (`benchmarks/reports/2026-11-01-transfer-grpc-vs-raw.md`): within ~20 % throughput under TLS, raw framing ~1.5× cheaper in CPU per byte but produced a TLS flush deadlock (R-29). Phase 1 implemented the full node API on tonic without transport issues |
| Metadata record encoding (OD-21) | postcard with a leading version byte; decoders match the version exactly; golden encodings per version in tests. protobuf remains the wire format of the node API only | RFC-003 §5.7 evaluation: compactness matters for FoundationDB transaction budgets; protobuf's silent unknown-field tolerance contradicts RFC-002's explicit-version rule; records are internal to arca-lake |
| Storage node index persistence (OD-22) | No persisted index. The in-memory index is rebuilt from segments on open by the sequence rule | `benchmarks/reports/2026-11-01-storage-index-rebuild.md` (developer laptop, relative only): rebuild processed on the order of 10^6 records in about one second and scanned ~1.4 GB of segments in about half a second. Open time grows linearly with record count and segment bytes; a persisted index becomes worth its consistency cost only when devices hold tens of millions of shards or when the scan is I/O bound on slower media. Revisit with a server-hardware measurement |
| Durability condition in Phase 1 (OD-17) | `m_ack = m`: a chunk is protected only when all `k + m` shards are durably acknowledged with a matching CRC-32C. The profile schema keeps `required_parity_acks` for a later phase | Simplest condition that gives the profile's full tolerance at commit time; under-protected tracking and prioritized repair are not implemented yet (RFC-004 §11) |

## Alternatives rejected

- Raw framed TCP for shard transfer: see OD-08 above; may be revisited in
  Phase 4 only with a CPU-per-byte measurement showing it is the bottleneck.
- protobuf for metadata records: rejected for silent schema tolerance and
  size; would also require a mapping layer between generated types and the
  recipe types.
- Persisted index with its own WAL or an embedded KV: a second source of
  truth to keep consistent with segments; the measured rebuild cost does not
  justify it at Phase 1 scale.
- `m_ack < m` in Phase 1: adds an under-protected state to track and repair
  with priority; deferred (OD-17).

## Consequences

- `arca-lake-protocol` is the only place protobuf appears; metadata record
  changes follow the version-bump rule in RFC-003 §5.7.
- `Store::open` cost is part of the node restart time; the `index_rebuild`
  example (`crates/storage/examples/index_rebuild.rs`) is the measurement
  to rerun on server hardware before Phase 4.
- Any change to a row above requires a superseding ADR.
