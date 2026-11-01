# RFC-006: Multi-Tier Storage

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-002, RFC-003, RFC-004, RFC-008
- Related ADRs: ADR-0005 (tiering granularity, read-through first),
  ADR-0009 (Phase 2 tiering decisions)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

Deployments mix device classes with very different cost and speed. Objects
must be durable on the cheapest class that meets their policy, hot data
must be served from the fastest class, and neither the cache nor the
movement between classes may ever weaken the durability or the consistency
that RFC-003 and RFC-004 guarantee: a cache copy is never the reason an
object exists, and a reader never sees a different generation than the one
metadata names.

## 2. Goals

- Any number of operator-defined tiers; a tier is a name, a device class,
  a role (`durable` or `cache`), and for durable tiers an EC profile.
- Durable placement and cache placement of a chunk are independent
  records; durability is decided by the durable placement alone.
- A transparent read-through cache on cache tiers, with admission,
  eviction, pinning, and explicit prefetch.
- Automatic promotion and demotion between durable tiers as a resumable,
  crash-safe migration that never changes object identity.
- Everything above expressed in the existing manifest, placement-epoch,
  GC, and repair machinery.

## 3. Non-goals

- Write-back caching (its own durability model; future RFC).
- Learning-based or workload-adaptive policies (Phase 4 at the earliest).
- Cross-site replication and remote tiers (object stores as a tier) are a
  later extension of the same `Tier` abstraction.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| T1 | Tier names and counts are configuration; no tier is special in code except by its role. |
| T2 | Device class and tier role are separate concepts; one device class may back several tiers. |
| T3 | The durable placement of a chunk is the only basis for its durability; deleting every cache placement changes no object version. |
| T4 | A cache placement is keyed by generation; a stale generation's cache is never served for a newer version. |
| T5 | Any cache failure (node down, corrupt copy, missing copy, half-written copy) degrades to a durable read, never to an error, as long as the durable read succeeds. |
| T6 | Cache population and eviction are idempotent and resumable; losing all cache metadata loses no data and is recovered by reconciliation against durable data. |
| T7 | Migration between durable tiers deletes the source only after the target is verified durable and the metadata switch is committed. |
| T8 | Cache, GC, repair, and migration may run concurrently without violating RFC-008 invariants. |

## 5. Design

### 5.1 Tiers, device classes, roles

```yaml
storage:
  tiers:
    - name: fast
      deviceClass: nvme
      role: cache
      capacityBytes: 2Ti          # cache tiers: eviction target
      highWatermark: 0.95
      lowWatermark: 0.85
    - name: capacity
      deviceClass: hdd
      role: durable
      ecProfile: standard
```

- Nodes advertise a `device_class` label per device (`NodeRecord.labels`,
  one device per node in Phase 2 tests). Stripe groups are created per
  tier from devices of that tier's class; a group record already carries
  `tier`. Groups of a cache tier use the `cache-v1` scheme: one member,
  the whole chunk as "shard 0", no coding.
- The same device class can back a cache tier and a durable tier; the
  controller creates separate groups for each.
- Writes target the bucket's durable tier (Phase 2: one configured default
  durable tier; per-bucket policies reuse the same records).

Alternatives for the tier model:

| Option | Performance | Metadata load | Capacity efficiency | Recovery | Complexity | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| Fixed two tiers (hot/cold) in code | same | same | same | same | lowest | rejected: violates T1 |
| Arbitrary tiers, groups tagged by tier (**chosen**) | — | none beyond `GroupRecord.tier` | — | — | low | chosen |
| Tier as a property of the device, groups span tiers | mixed-speed stripes | none | — | repair must respect tiers | high | rejected |

### 5.2 Durable placement vs cache placement

| Record | Key | Content | Owner |
| --- | --- | --- | --- |
| Chunk record (RFC-002) | `("al","g",gen,"c",index)` | `DurablePlacement { group, epoch }`, scheme, `k+m`, CRC | write protocol, migration |
| Cache placement | `("al","cache",gen,index)` | `CachePlacement { tier, group, epoch, state, bytes, pinned, last_access_ms, created_ms }` | cache population, eviction, pinning |
| Cache recency index | `("al","cidx",tier,last_access_ms,gen,index)` | empty | maintained with the placement |
| Cache usage | `("al","cusage",tier)` | bytes (Ready + Populating entries) | maintained with the placement |

Object-level versus chunk-level placement:

| Granularity | Performance | Metadata load | Capacity | Recovery | Complexity | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| Object (whole generation cached or moved) | simple hit/miss | one record per object | caches cold chunks of hot objects | one state per object | lowest | rejected for cache (range reads, large objects) |
| Chunk (**chosen**, DD-11) | partial hits, range GET only touches needed chunks | one record per cached chunk | exact | per-chunk states, all idempotent | moderate | chosen |

Independence: the cache record never appears in the durable path (commit,
GC verification, repair completeness). Deleting all `("al","cache",…)` keys
changes no pointer, header, or chunk record (T3). The cache key includes
the generation, so a GET that resolved generation `g` can only find
`g`'s cache entries (T4); an overwrite commits a new generation, whose
cache starts empty, and the old generation's cache entries are collected
with the old generation (§5.7).

### 5.3 Read-through cache

```text
GET ─▶ read_manifest (pointer, header, chunk records, cache placements: one tx)
   for each chunk in range:
     cache Ready? ─▶ read whole chunk from the cache member ─▶ CRC ok? ─▶ serve
            │                                      └─ miss/corrupt/unreachable ──┐
            └─ no ──────────────────────────────────────────────────────────────▶ durable read (RFC-004 §5.6) ─▶ serve
                                                                                   └─▶ admission? ─▶ async populate
```

- The manifest read includes the cache placements of the resolved
  generation, so hit/miss is decided on the same snapshot as the chunk
  records (no second lookup that could see another generation).
- A cache hit reads the whole chunk from the cache member and verifies the
  chunk CRC recorded in the chunk record. Any failure (RPC error, missing,
  short, corrupt) falls back to the durable read of that chunk and marks
  the entry for removal (`invalidate`). Partial hits are per chunk.
- Population is asynchronous and never on the GET's critical path: after
  the durable read, the chunk bytes are handed to the cache populator,
  which (1) writes the chunk to the chosen cache group member with the
  group's epoch, (2) in one transaction re-checks that the generation is
  still `Committed`, inserts the placement as `Ready`, the recency index
  entry, and adds `bytes` to the tier usage. If the generation was
  retired meanwhile, the copy is deleted; if the transaction fails, the
  copy becomes an orphan on the cache node that cache reconciliation
  removes (§5.7).
- Stampede: a per-gateway in-flight set dedupes populations of the same
  `(gen, index)`; across gateways, duplicate populations write identical
  bytes and the second transaction finds the entry and does nothing.
- Cache nodes are storage nodes; the cache group's placement epoch fences
  writes exactly as for durable groups.

Write-through (populate on PUT) is a policy flag on the same mechanism
(populate from the chunk bytes the gateway already holds). Phase 2 ships
read-through; write-through is listed as a Phase 2.1 option and is not
needed for correctness.

| Cache design | Performance | Metadata load | Capacity | Recovery | Complexity |
| --- | --- | --- | --- | --- | --- |
| Read-through, chunk level, metadata-recorded placements (**chosen**) | hit avoids EC decode and `k` RPCs | +1 key per cached chunk, read with the manifest | exact | reconciliation rebuilds from durable | moderate |
| Read-through with node-local cache index only (no metadata) | same hits | none | exact | a gateway cannot know what is cached without probing every cache node | low, but hit/miss needs probes |
| Whole-object cache | simplest | 1 key per object | wastes space on cold chunks | — | low | 

### 5.4 Admission and eviction

| Policy | Hit ratio | State | Complexity | Decision |
| --- | --- | --- | --- | --- |
| LRU (recency index in metadata) | good for recency workloads | index entry per chunk | low | **chosen** for eviction |
| CLOCK | similar, cheaper updates | one bit per entry on the node | low, but node-local state | later |
| TinyLFU / frequency-aware admission | best for scan-resistant workloads | frequency sketch | moderate | partial: admission after `n` misses |
| Size-aware admission | protects small hot chunks | none | trivial | **chosen** (`max_cacheable_bytes`) |

Phase 2: admission is size-aware with an optional miss-count threshold
(per-gateway in-memory sketch, so it is a hint, not a guarantee); eviction
is LRU by the metadata recency index, driven by the controller when tier
usage exceeds the high watermark, down to the low watermark, skipping
pinned entries. Evicting an entry: delete the placement and index entry
and subtract usage in one transaction, then delete the copy on the node
(a failed delete leaves an orphan for reconciliation).

**Pinning and capacity.** Pinned entries are never evicted. If pinned
bytes exceed the low watermark, eviction cannot reach its target and the
tier reports `cache_over_watermark`; admissions are rejected while usage
is above the high watermark (misses are still served from durable data),
and the condition is exposed as a metric. Pinning never fails a read.

### 5.5 Prefetch and pinning

- `prefetch(bucket, key)`: populate every chunk of the current generation
  through the same populator (synchronously, for operators and tests).
- `pin(bucket, key)` = prefetch + mark pinned; `unpin` clears the flag.
  Pins are per generation: an overwrite creates a new, unpinned
  generation (the pin is a request about current data, and the old
  generation is collected with its cache entries).

### 5.6 Crash safety of the cache

| Intermediate state | How it arises | Recovery |
| --- | --- | --- |
| Copy on cache node, no placement | populate crashed between the write and the transaction; transaction failed; generation retired during population | cache reconciliation: a cache-group shard without a `Ready` placement for a live generation is deleted (same classification as durable reconciliation, against the cache key space) |
| Placement, no copy | node lost the copy, eviction's delete raced a crash, node reset | GET falls back and invalidates; cache reconciliation deletes placements whose copy the node does not list |
| Placement for a retired generation | overwrite/delete before eviction | collected with the generation: `finish` clears the cache keys of the generation after the copies are deleted (GC) |
| Cache node restart | — | copies persist (storage engine); nothing to do |
| Crash during eviction | after the transaction, before the delete RPC | orphan copy → reconciliation |
| Crash during prefetch | same as populate | same |
| Durable repair concurrent with cache | independent key spaces and groups | no interaction; a cache hit is validated by CRC regardless of durable placement |
| All cache metadata lost | operator error | every copy becomes an orphan and is deleted by reconciliation; readers fall back; the cache refills by read-through (T6) |

All recovery steps are idempotent (delete `NotFound` is success; inserting
an existing placement is a no-op).

### 5.7 GC interaction

Generation-driven GC (RFC-008 §6.2) deletes the cache copies of a
retiring generation after the durable shards, then `finish` clears the
generation's cache keys together with its chunk keys (same range), and
subtracts usage. Cache reconciliation runs per cache node and group with
the same page walk as durable reconciliation.

### 5.8 Automatic tiering (durable tiers)

Distinct from the cache: promotion and demotion move the **durable**
placement between durable tiers.

- Access tracking: the gateway records `("al","acc",gen) = { last_ms,
  count }` for GETs, coalesced to at most one write per generation per
  interval; the controller reads it.
- Policy (Phase 2): demote a chunk whose generation was not read for
  `demote_after` from a tier marked `promotable` to the tier below;
  promote a chunk whose generation was read at least `promote_reads`
  times within `promote_window` to the tier above. Tier order is the
  configuration order of durable tiers.
- Migration (DD-6 state machine, per chunk):
  1. Read the chunk from the current durable placement (decode if needed)
     and verify its CRC.
  2. Encode with the target tier's profile and write all shards to a
     target group with the target group's epoch; require every
     acknowledgment with matching CRC (`m_ack = m`).
  3. Verify: read the shards back from the target (`VerifyShards` of the
     chunk's keys) before switching.
  4. Switch: one transaction that re-reads the chunk record, requires the
     generation to be `Committed` and the record's placement to be the
     one read in step 1, and writes the new placement, scheme, and `k+m`.
  5. Retire: the old shards are now unreferenced (their chunk record is
     placed elsewhere) and are deleted by reconciliation, or eagerly by
     the migrator after the switch (deletes carry the old group's epoch).
  A crash before step 4 leaves target shards that reconciliation deletes
  (unreferenced); a crash after step 4 leaves source shards that
  reconciliation deletes. The chunk is readable throughout from whichever
  placement the record names.

## 6. Alternatives considered

- Cache index only on the node (no metadata): rejected because hit/miss
  would need probes to every cache node and the gateway could not avoid
  the RPC on misses.
- Object-level cache and migration: rejected (DD-11), see §5.2.
- Write-back cache: out of scope; its durability model is a separate RFC.
- Migration by delete-and-rewrite: rejected (RFC-004 §6); identity must
  not change.

## 7. Failure scenarios

See §5.6 and RFC-008. Additionally:

| Scenario | Behaviour |
| --- | --- |
| Cache node down | hits fail over to durable per chunk; populations to that group fail and are retried later; no error to the client |
| Corrupt cache copy | node reports `DataLoss` or the CRC mismatches; fallback; entry invalidated |
| Overwrite while a population is in flight | the population's transaction sees the generation `Retiring` and deletes the copy |
| Durable node down during a cache miss | degraded durable read (RFC-004 §5.6); population proceeds from the decoded chunk |
| Migration target node fails mid-copy | no switch; target shards are orphans; retry later |

## 8. Consistency and durability guarantees

- T3, T4, T5 hold by construction and are verified by the cache test
  suite on the in-memory backend and on FoundationDB.
- A cache hit returns bytes whose CRC equals the chunk record's CRC; a
  mismatch is a fallback, never a response.
- Migration never reduces the number of durable, verified copies below
  the profile's tolerance at any point (the source is kept until after the
  switch).
- Not guaranteed: that a population happens after a miss (admission,
  capacity, or failures may prevent it); any hit ratio.

## 9. Performance considerations

*Hypotheses* (not measured in this phase): a hit saves the `k` shard RPCs
and the decode of a miss and reads one contiguous chunk; the manifest read
grows by one key per cached chunk; the recency index write on every hit is
the main metadata cost and is coalesced per generation and interval.
Measurements belong to RFC-009.

## 10. Open questions

- Per-bucket tier policies and storage classes (RFC-007 mapping).
- CLOCK or TinyLFU once a workload shows LRU's weakness.
- Write-through as default for small objects.
- Recency-index write coalescing interval.

## 11. Acceptance criteria

Phase 2 tests (`crates/testkit/tests/cache*.rs`, `tiering.rs`, FoundationDB
instantiation): hit/miss/partial hit/range GET; concurrent GETs and
stampede dedupe; population failure does not fail the GET; cache node
failure and corruption fall back; stale generation never served after an
overwrite; eviction to watermark with pinned entries kept; prefetch and
pin; crash states of §5.6 recovered idempotently; migration
copy-verify-switch-retire with crashes before and after the switch; cache
concurrent with GC and repair.
