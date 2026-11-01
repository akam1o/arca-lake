# ADR-0009: Multi-tier storage: cache placements, admission and eviction, migration

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-006, RFC-008 §6, RFC-004 §5.6

## Context

Phase 2 added cache tiers and automatic tiering to a system whose
durability and visibility rules (RFC-003, RFC-004, RFC-008) were already
verified. Every decision below was made so that none of those rules
acquires a new dependency: a cache copy is never the reason an object
exists, and a migration never changes what a generation is. Each decision
is backed by tests on the in-memory backend; the cache suite also runs on
a FoundationDB cluster in CI.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Tier model | A tier is configuration: name, device class, role (`durable` or `cache`), profile for durable tiers. Nodes carry a `device_class` label; groups carry `tier`. No tier name or count appears in code. The same device class may back several tiers. | `groups::create_groups_for_class`, `groups::create_cache_groups`; `tiering.rs` creates a second durable tier from relabelled nodes; RFC-006 §5.1 |
| Cache groups | Scheme `cache-v1`: one member, the whole chunk as shard 0, no coding. Cache groups reuse the storage node, its epoch fence, `ListShards`, and `DeleteShard`; repair and auto-repair skip them. | `cache.rs::cache_survives_durable_repair_gc_and_fenced_cache_epoch`; RFC-006 §5.1 |
| Cache placement record | `("al","g",gen,"x",index)` inside the generation's subspace, with a recency index `("al","cidx",tier,last_access_ms,gen,index)` and a usage counter per tier. The record never takes part in commit, GC verification, or repair; `read_manifest` returns it in the same transaction as the chunk records. Clearing a generation clears its cache keys. | `metadata/tests/cache_model.rs` (C1–C4); `cache.rs::overwrite_never_serves_a_stale_generation`; RFC-006 §5.2 |
| Hit validation | A cache hit is served only if the copy's length and CRC-32C equal the chunk record's. Anything else (unreachable node, missing copy, short or corrupt copy, unknown group) is a per-chunk fallback to the durable read plus an asynchronous invalidation. | `cache.rs::cache_node_failure_…`, `corrupt_cache_copy_…`, `crash_states_…` (b); RFC-006 §5.3 |
| Population | Asynchronous, off the GET's critical path: write the copy, then one transaction that requires the generation to be `Committed` and the tier usage to stay at or below the high watermark before recording the placement; otherwise delete the copy. Per-gateway in-flight set dedupes a stampede. A population failure only increments a counter. | `cache.rs::population_failure_never_fails_the_get`, `concurrent_gets_populate_each_chunk_once`; RFC-006 §5.3 |
| Admission | Size-aware (`max_cacheable_bytes`) plus an optional per-gateway miss-count threshold (`admit_after_misses`); capacity enforced in the insert transaction. TinyLFU and CLOCK deferred until a workload shows LRU's weakness. | `cache.rs::admission_rejects_over_capacity_and_large_chunks`; RFC-006 §5.4 |
| Eviction | LRU by the metadata recency index, from the high watermark down to the low watermark, record removed before the copy. Pinned entries are skipped. If pins keep usage above the low watermark the pass reports `over_watermark` and admissions are rejected while above the high watermark; reads are never affected. | `cache.rs::eviction_respects_watermarks_lru_and_pins`; RFC-006 §5.4 |
| Pinning and prefetch | Both act on the current generation and go through the same populator synchronously; an overwrite creates an unpinned generation and the old one is collected with its cache entries. | `cache.rs::prefetch_and_pin_populate_synchronously`; RFC-006 §5.5 |
| Cache recovery | Copies without a record are orphans deleted by cache reconciliation (same page walk as durable reconciliation, classified by cache placement); records without a copy are removed by the reader's invalidation. Losing every cache record loses no data and refills by read-through. All steps idempotent. | `cache.rs::crash_states_are_recovered_idempotently`; RFC-006 §5.6 |
| GC interaction | A retiring generation's cache copies are deleted after its durable shards; `finish` clears the cache keys, recency entries, usage, and access record with the generation. | `cache.rs::overwrite_never_serves_a_stale_generation`; model test C4 |
| Migration | Copy → verify by read-back → one conditional transaction (`switch_placement`: generation `Committed`, placement unchanged, same chunk ID) → retire. The source is referenced until the switch commits; a crash before leaves target orphans, a crash after leaves source orphans, both reclaimed by reconciliation. Identity (generation, chunk ID) never changes. | `tiering.rs` (5 cases incl. crashes before and after the switch); RFC-006 §5.8 |
| Automatic tiering vs cache | Tiering moves the durable placement by policy (reads within a window promote, idle time demotes, tiers ordered by configuration); the cache copies on read. The two never share records. | `tiering.rs::controller_policy_promotes_hot_and_demotes_cold_generations` |

## Alternatives rejected

- Node-local cache index without metadata records (hit/miss would need a
  probe per cache node on every read).
- Object-level cache and migration (range reads and large objects, DD-11).
- Fixed hot/cold tiers or tier roles derived from device class.
- Delete-and-rewrite migration (changes identity, RFC-004 §6).
- Write-back cache (separate durability model; not in scope).

## Consequences

- `read_manifest` grows by one key per cached chunk of the generation.
- Every cache hit may write one recency entry (coalesced per gateway and
  chunk); eviction order is only as fresh as that coalescing.
- A cache tier's capacity accounting lives in one key per tier, written by
  every insert and eviction; this is a known hot key (R-42).
- Admission counters and the stampede set are per gateway; they are hints
  and never affect correctness.
