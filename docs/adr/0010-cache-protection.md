# ADR-0010: Cache protection: group roles, coded cache copies, subordinate cache repair

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-006 §5.9, RFC-008 §7, RFC-009

## Context

Phase 2 cache tiers stored one raw copy per chunk on one device. Phase 2.1
had to let an operator choose, per cache tier, between no redundancy,
replication, and erasure coding, reusing the durable `ErasureCoder` path,
without letting any of it count as durability and without letting cache
repair compete with durable repair.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Group role | `GroupRecord.role ∈ {Durable, Cache}` decides what a group's shards mean; the scheme no longer does. Every consumer that must skip or specially treat cache groups (GC, reconciliation, repair scheduling, tiering, test helpers) tests the role. | `cache_ec.rs`, `cache.rs` unchanged behaviour with `none` |
| Protection modes | `none` = scheme `cache-v1`, one member, raw copy; `replication r` = `replication-v1` with `1 + (r-1)`; `ec k+m` = any `ErasureCoder` scheme. Mode is per tier at group creation; `k`, `m`, `r` are parameters, never constants. | `groups::CacheProtection`, CLI `create-cache-groups --protection` |
| Shared coded path | `durable::read_coded` / `write_coded` serve durable reads, migration, and coded cache groups; the cache adds only the placement record and the fallback. | `tiering/src/durable.rs` |
| Population atomicity | Every shard of a copy is acknowledged with its CRC before the placement is recorded; any failure deletes what was written. There is no partially recorded cache copy. | `population_with_a_member_down_records_nothing` |
| Degraded hits | A hit with `≥ k` readable shards is served (validated by the chunk record's length and CRC) and the missing shards are rewritten asynchronously by the gateway under a small per-gateway permit count; a hit with `< k` shards is a durable fallback plus invalidation. | `shard_loss_is_a_degraded_hit_repaired_on_read`, `member_down_degrades_then_unreadable_falls_back`, `corrupt_shards_are_missing_and_too_many_fall_back` |
| Deletion | Eviction, invalidation and GC delete shard `i` on member `i` for every member; a failed delete is an orphan for per-member cache reconciliation. | `eviction_with_a_member_down_leaves_orphans_for_reconciliation`, `durable_repair_and_gc_leave_the_coded_cache_alone` |
| Cache repair | Same state machine as durable repair (fence, membership commit, walk, reconstruct, verified completion) with two cache-specific rules: a shard is rebuilt only if a live generation's cache placement names the group, and an unreadable cached chunk is dropped (record removed) instead of failing the repair. | `controller_cache_repair_rebuilds_a_lost_member_within_budget`; `repair.rs::reconstruct_one` |
| Priority and budget | In a repair pass durable groups come first; cache groups are considered only when no durable group was `Repairing` or needed a replacement in that pass, and each cache repair reconstructs at most `cache_repair_chunks_per_pass` chunks before persisting its cursor and yielding (`RepairOptions.max_chunks`). | same test (one chunk per pass until complete); `process.rs::repair_pass` |
| Not durability | No change to commit, GC verification, or durable repair; the durable placement remains the sole basis for T3. | `coded_hit_reads_shards_and_leaves_durable_alone` (durable shard count unchanged) |
| Default | `none`, for the reasons in RFC-006 §5.9: capacity and population cost usually outweigh surviving a rare cache fault. | — |

## Alternatives rejected

- Keeping `scheme == "cache-v1"` as the marker of cache groups (would
  forbid coded cache groups or require a parallel scheme namespace).
- A separate cache repair implementation (would duplicate fencing,
  cursors and completeness without adding safety).
- Synchronous (on the GET path) rewriting of missing cache shards.
- Counting cache redundancy toward the durability condition.

## Consequences

- Record format: `GroupRecord` gained a field; the prototype encoding has
  no migration path (RECORD_VERSION unchanged, pre-release).
- `RepairReport.budget_exhausted` and `RepairOptions.max_chunks` are part
  of the repair API; durable repairs use an unlimited budget.
- A coded cache hit fans out to `k` nodes; tail latency of a hit is the
  tail of `k` RPCs (measured in RFC-009).
