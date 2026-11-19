# Phase 2: Native multi-tier storage

> **Historical phase plan.** The statuses and outstanding tasks below
> record this phase, not the current system. Later work may supersede them.
> See the [roadmap index](README.md) for active priorities and
> [known limitations](../guide/known-limitations.md) for current boundaries.

Original status: **first iteration done** (see "Not verified" for what remains).
Phase 2 adds operator-defined tiers, cache placements that are independent
of durable placements, a transparent read-through cache with admission,
LRU eviction, pinning and prefetch, and automatic promotion and demotion
between durable tiers as a crash-safe migration. Design: RFC-006;
decisions: ADR-0009.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 2.1 RFC-006 | done | Tier model, placement granularity, cache design, admission/eviction policies compared on performance, metadata load, capacity, recovery, and complexity; §11 lists what is verified |
| 2.2 Tier abstraction | done | Tiers are configuration (name, device class, role, profile); nodes carry a `device_class` label; groups carry `tier`; `create_groups_for_class`, `create_cache_groups`; CLI `--tier`, `--device-class`, `create-cache-groups` |
| 2.3 Durable vs cache placement | done | `CachePlacement` records in the generation subspace, recency index and usage per tier; never read by commit, GC verification, or repair; `read_manifest` returns both in one transaction; model test C1–C4 |
| 2.4 Read-through cache | done | Per-chunk validated hits (length + CRC-32C), durable fallback and asynchronous invalidation on any failure, asynchronous deduplicated population that re-checks the committed generation; `cache.rs` (12) on the in-memory backend, `cache_fdb.rs` (12) on FoundationDB |
| 2.5 Admission and eviction | done | Size cap, optional miss threshold, capacity enforced in the insert transaction; LRU to the low watermark, pinned skipped, `over_watermark` reported; controller eviction scheduler and metrics |
| 2.6 Prefetch and pinning | done | `prefetch`, `pin`, `unpin` on the gateway backend (per current generation) |
| 2.7 Cache crash recovery | done | Orphan copies by cache reconciliation, dangling records by reader invalidation, total loss of cache metadata refilled by read-through; idempotent |
| 2.8 Automatic tiering | done | `controller::tiering` policy over access records; `migrate_chunk` copy → verify → conditional switch → retire; `tiering.rs` (5) incl. crashes before and after the switch |
| 2.9 Real network impairment (Phase 1 carry-over) | not run | Unchanged from Phase 1.2: the development container runtime lacks `sch_netem`; R-35 and R-40 stay High |
| 2.10 Performance | not measured | No figures are recorded; RFC-006 §9 states hypotheses only |

## What the new tests verify

| Suite | Cases | Verified behaviour |
| --- | --- | --- |
| `cache.rs` / `cache_fdb.rs` | 12 each | Miss then hit; partial hits and range GETs; 12 concurrent GETs populate each chunk once; population failure (cache nodes down) never fails the GET and leaves no record; cache node failure after population falls back per chunk and invalidates; overwrite never serves the stale generation and GC collects the old cache copies and usage; a corrupt copy falls back, is invalidated, and is repopulated; LRU eviction to the watermark keeps pinned and most recent entries, `over_watermark` when everything is pinned, zero after unpin; admission rejects over capacity and before the miss threshold; prefetch/pin/unpin; crash states (copy without record, record without copy, all cache metadata lost) recovered idempotently; cache under durable repair, GC, reconciliation, and a fenced cache epoch |
| `tiering.rs` | 5 | Migration switches the placement without changing identity and retires the source; crash before the switch leaves the source authoritative and the target reclaimable; crash after the switch leaves the target authoritative and the source reclaimable; the switch is refused for a retired generation and the target copy is cleaned up; the policy promotes a hot generation and later demotes it with byte-identical reads |
| `metadata/tests/cache_model.rs` | C1–C4 | Usage equals the sum of placement bytes; recency entries and placements correspond one to one in ascending recency; placements only for committed generations, insert refuses retired generations and over-capacity copies; finishing a generation removes exactly its cache state |
| Phase 1 suites | unchanged | All Phase 0/1/1.2 suites run unchanged; `total_shards` counts durable groups only |

## Not verified

- Real network impairment, multi-host FoundationDB, leader change with an
  in-flight RPC on a real network (Phase 1.2 "Not verified" applies
  unchanged; R-35, R-40).
- Performance: hit latency, population cost, recency-index write load,
  and the tiering walk over many generations are not measured.
- Write-through population on PUT (policy flag described in RFC-006 §5.3,
  not implemented).
- Per-bucket tier policies and storage classes (RFC-007 mapping).
- Cache groups with more than one member, or a cache tier spanning more
  than one device class.
- Eviction and tiering under sustained load concurrent with GC and repair
  on a real cluster (only the single-pass interleavings in the suites).
- The tiering pass walks every committed generation each run; its cost on
  a large namespace is not measured (R-44).

## Acceptance criteria for closing Phase 2

1. `cache_fdb` and the Phase 1 FoundationDB jobs pass in CI on every push.
2. A measured report (RFC-009 format) of hit/miss latency and population
   cost on a describable environment.
3. The Phase 1.2 real-network run (its criterion 2).
4. Per-bucket tier policy once RFC-007 fixes storage-class semantics.
