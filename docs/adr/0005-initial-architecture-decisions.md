# ADR-0005: Initial architecture decisions confirmed at design review

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-001 through RFC-008 (planned)

## Context

The design review in `docs/architecture/design-decisions.md` compared
alternatives for the core architecture. The maintainers confirmed the
recommendations below so that RFC authoring and Phase 0 can proceed on a
stable basis. Each item links to the design decision that holds the full
trade-off discussion.

## Decisions

| Item | Decision | Reference |
| --- | --- | --- |
| EC granularity | Fixed-maximum-size chunk per stripe; chunk size is a profile parameter | DD-1 |
| Storage engine persistence | Append-only segments on a local file system with a rebuildable shard index; object bytes never stored in an embedded KV | DD-2 |
| Durable placement representation | Stripe groups with per-group epochs; cache placements use per-chunk location lists | DD-3 |
| Write protocol | Pending generation, incremental manifest, small commit transaction as the linearization point; durability condition `k + m_ack` with default `m_ack = m` | DD-4 |
| Tier migration | Copy, verify, switch, retire state machine; identity never changes on physical movement | DD-6 |
| Garbage collection | Single-owner chunks, tombstone queue with grace period, low-frequency orphan reconciliation | DD-7 |
| Failure recovery | Per-group epochs and fencing; metadata records as the recovery journal; explicit failure on quorum loss | DD-8 |
| EC location | Gateway-side encode and decode | DD-9 |
| Metadata layer deployment | Embedded library behind a `MetadataStore` trait | DD-10 |
| Tiering granularity | Decisions per chunk; policies at bucket, prefix, and storage class | DD-11 |
| Cache write policy | Read-through first; write-through later as an option; write-back only via a future RFC | DD-12 |
| Small objects | Replication below a per-profile threshold | DD-13 |
| CopyObject data handling | Physical copy in the MVP | OD-11 |

Items that still depend on Phase 0 measurements (metadata store, S3 protocol
layer, EC backend, transfer protocol, I/O backend, checksum algorithm) remain
open and are listed in `docs/architecture/open-decisions.md`.

## Alternatives rejected

See the corresponding DD sections. The common thread is that each rejected
alternative either placed a controller on the data path, required per-chunk
metadata writes under failure, or introduced multi-owner data that
complicates garbage-collection safety.

## Consequences

- RFC-001 and RFC-002 can be written against these decisions.
- Any change to a row above requires a superseding ADR, not an edit to this
  one.
- Phase 0 experiments are designed to test the hypotheses these decisions
  rest on; a refuted hypothesis triggers a superseding ADR.
