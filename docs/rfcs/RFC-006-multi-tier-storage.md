# RFC-006: Multi-Tier Storage

- Status: draft (Phase 2 implementation, §11)
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

**Hit accounting (Phase 2.2).** "Hit ratio" is not one number. The gateway
counts, per chunk read: *lookup hits* (the manifest carried a cache
placement), *served hits* (the copy was read and validated), *degraded
hits* (served with at least one shard missing), and *fallbacks* by cause
(fewer than `k` readable shards, length or CRC mismatch, node error,
unknown group). On the population side it counts admissions refused by
size, by the miss threshold, at capacity, or because the generation was
retired; populations skipped for lack of an in-flight permit; populations
deduplicated; and population failures by cause (node, metadata,
checksum). `served hits / (served hits + misses + fallbacks)` is the
readable hit ratio; `lookup hits − served hits` is the fallback count.
RFC-009 §6 uses these to explain the differences between protection
modes.

Write-through (populate on PUT) is a policy flag on the same mechanism
(populate from the chunk bytes the gateway already holds). Phase 2 ships
read-through; write-through is listed as a Phase 2.1 option and is not
needed for correctness.

| Cache design | Performance | Metadata load | Capacity | Recovery | Complexity |
| --- | --- | --- | --- | --- | --- |
| Read-through, chunk level, metadata-recorded placements (**chosen**) | hit avoids EC decode and `k` RPCs | +1 key per cached chunk, read with the manifest | exact | reconciliation rebuilds from durable | moderate |
| Read-through with node-local cache index only (no metadata) | same hits | none | exact | a gateway cannot know what is cached without probing every cache node | low, but hit/miss needs probes |
| Whole-object cache | simplest | 1 key per object | wastes space on cold chunks | — | low | 

#### 5.3.1 Population queue (Phase 2.3)

A miss that passes admission hands the chunk to the gateway's
population queue and returns; the GET never waits for it and a failed
population only counts. The queue bounds the work three ways:

- **bounded FIFO** (`population_queue_depth`, CLI
  `--cache-population-queue-depth`, default 8): when full, the
  population is dropped and counted as skipped, which is the
  backpressure signal to the miss path. A queued population holds its
  chunk's bytes, so the queue's memory is bounded by its depth times the
  chunk size (32 MiB at the defaults; a depth of 32 showed as +80 MiB
  of gateway RSS in RFC-009 §6.19);
- **workers** (`max_populations_in_flight`): at most that many
  populations run at once per gateway;
- **per-node permits** (`populations_per_node`, CLI
  `--cache-populations-per-node`, default 2): a population holds one
  permit on every member node of its cache group while it writes, taken
  in node order so that two populations never deadlock.

Durable reads come first: before taking its permits a population yields
while the gateway has shard reads in flight to any of its nodes (up to
200 ms) or to any node (up to 50 ms), then proceeds, so a read burst is
served before the cache is written and a busy node cannot starve the
cache. Node errors are retried up to three times with exponential
backoff; metadata and corruption errors are not retried (the next miss
tries again). At shutdown the queue stops accepting, running attempts
finish, queued populations are dropped and counted. Counters:
`arca_gateway_population_queue_{enqueued,dropped_full,dropped_shutdown,completed,retries,deferred_for_reads}_total`
and the gauges `queued` and `running`. The dedup marker of a chunk is
released on every exit path, so a dropped population is retried by a
later miss. See ADR-0012.

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

**Conditional cache deletes (Phase 3.6, R-73).** The record tokens (store
epoch, sequence) the nodes return for a copy's shard writes are kept next
to its placement (`("al","g",gen,"xt",index)`, removed with it); eviction,
invalidation and a failed population's cleanup delete each member's shard
only if it is still that record. A newer copy written under the same key
since (a population after the eviction removed the record, or a
population that won the insert) is kept; a repair of a degraded copy
records the tokens of the shards it rewrote (if the placement still names
its group). A node that restarted has a new store epoch and refuses older
tokens: the copy is then an orphan that reconciliation (which lists fresh
tokens) removes once no placement names it. Placements recorded before
Phase 3.6, and nodes that hand out no tokens, get no delete: their
copies are left to reconciliation (an unconditional delete could remove a
copy populated meanwhile that its new placement names). On a node that
predates tokens reconciliation keeps them too (R-74) until it is
upgraded.

**Cache capacity (Phase 3.6).** The cache tier is bounded by its capacity
and watermarks as above; its nodes also keep the storage node's reserve
against cache population (write class `cache`), so a cache device never
fills past it whatever the watermarks say. The controller publishes the
cache tier's admission state, but it does not gate client writes (they
go to durable tiers).

Admission is refused only when a copy would exceed `capacityBytes`;
eviction starts when usage exceeds the high watermark and stops at the
low watermark, so under a read-through workload usage oscillates between
the two and LRU turnover happens. The capacity check reads the tier's
usage as a *snapshot* and the usage is updated by an atomic addition
(Phase 2.2, R-42): concurrent populations do not conflict on the
counter, at the price that up to `max_populations_in_flight` copies per
gateway may be admitted past the limit before the counter reflects them;
eviction reclaims the overshoot, which is bounded by that many chunks. (Phase 2 refused admissions at the high
watermark, which left eviction nothing to do and froze the cache content;
corrected in Phase 2.1 after the three-host measurements.)

**Pinning and capacity.** Pinned entries are never evicted. If pinned
bytes exceed the low watermark, eviction cannot reach its target and the
tier reports `cache_over_watermark`; admissions are rejected while usage
is at capacity (misses are still served from durable data), and the
condition is exposed as a metric. Pinning never fails a read.

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
  0. Take the chunk's migration intent `("al","mig",chunk) = { target
     group, attempt, since }` (Phase 3.5): refused while another attempt's
     intent is younger than one hour. While it is held, reconciliation
     keeps the target group's shards of the chunk.
  1. Read the chunk from the current durable placement (decode if needed)
     and verify its CRC.
  2. Encode with the target tier's profile and write all shards to a
     target group with the target group's epoch; require every
     acknowledgment with matching CRC (`m_ack = m`).
  3. Verify: read the shards back from the target (`VerifyShards` of the
     chunk's keys) before switching.
  4. Switch: one transaction that re-reads the chunk record, requires the
     generation to be `Committed`, the record's placement to be the one
     read in step 1 and the intent to be this attempt's, writes the new
     placement, scheme, and `k+m`, and drops the intent.
  5. Retire: the old shards are now unreferenced and are deleted by
     reconciliation with conditional deletes (RFC-008 §6.6 amendment). The
     migrator does not delete them itself: a delete it sent could arrive
     after a later migration back into the source group.
  A switch refused while the attempt still holds the intent deletes the
  target shards it wrote, each only if the node still holds the record it
  wrote, then drops the intent. Other failures (a read or write error part
  way) drop the intent and leave what was written to reconciliation. A
  crash before step 4 leaves target shards kept by the intent until it is
  stale, then reclaimed by reconciliation; a crash after step 4 leaves
  source shards that reconciliation deletes. The chunk is readable
  throughout from whichever placement the record names.

Phase 3.10 (RFC-008 §8.10): a tiering pass needs the published
admission record. That record must be younger than 60 s (or three
health intervals) with no tier throttled or worse, and no durable group
may be repaired; the leader's own tracker, empty at a new leader's first
pass, no longer decides alone.

Capacity rebalancing between groups of one tier (Phase 3.8, R-82; R-93
in Phase 3.10) moves chunks through the same migration path. Since Phase
3.10 it yields to the work above it:

- no pass while tiering moves chunks, while the GC backlog is above its
  pause, or on an admission record that is not current;
- no move in a tier that is throttled or worse, and half the budget in
  `warning`;
- before every chunk, each target member is asked for its device, and
  the chunk moves only if every member keeps the placement threshold
  (`group_min_shards` of the largest shards above its reserve and
  in-flight writes) after it, and none needs compaction first. Otherwise
  the pass stops (`arca_controller_rebalance_capacity_stops_total`).

Phase 3.11 (M3): a planned member move (a drain, a replacement request,
a move whose former member still answers) checks the target's room above
its client reserve and writes with the `Migration` class, as tiering and
rebalancing do; only a failure repair may use the space down to the
repair floor.

### 5.9 Cache protection (Phase 2.1)

A cache tier chooses one *protection mode* for its groups. The mode is
configuration per tier; nothing in the read path depends on it beyond the
group record, which already carries scheme, `k`, `m`, and members.

| Mode | Group | Scheme | Shards per chunk |
| --- | --- | --- | --- |
| `none` | one member per device | `cache-v1` (raw copy, no coder) | 1 |
| `replication` (`r` copies) | `r` members across the failure domain | `replication-v1` | `1 + (r-1)` |
| `ec` (`k+m`) | `k+m` members across the failure domain | any `ErasureCoder` scheme (`rs-gf8-v1`, `rs-leopard-gf16-v1`) | `k + m` |

The same `ErasureCoder`, `StripeLayout`, and shard RPCs are used for
durable groups and coded cache groups (`durable::read_coded`,
`durable::write_coded`). Group records carry a `role` (`durable` or
`cache`), so the controller, GC, reconciliation and repair never confuse
the two whatever the scheme. **Cache protection is not durability**: a
cache group's redundancy only decides how often a hit survives a cache
fault before falling back; the durable placement is still the only basis
for T3, and nothing in commit, GC verification, or durable repair reads a
cache group.

Comparison (per chunk of `L` bytes; `R` the hit ratio; "fallback" means a
durable read of `k_d` shards plus decode):

| Criterion | `none` | `replication r` | `ec k+m` |
| --- | --- | --- | --- |
| Capacity efficiency | `L` | `r·L` | `L·(k+m)/k` |
| Read amplification (hit) | 1 shard, `L` bytes | 1 shard, `L` bytes (any copy) | `k` shards, `L` bytes total (+ parity on loss) |
| Write amplification (population) | `L`, 1 RPC | `r·L`, `r` RPCs | `L·(k+m)/k`, `k+m` RPCs |
| Encode / decode CPU | none | memcpy | encode on population; decode only on degraded hits |
| Network transfer (hit) | `L` from one node | `L` from one node | `L` from `k` nodes in parallel (lower per-node load, more connections) |
| Hit latency | one RPC | one RPC | max of `k` parallel RPCs (tail of `k`) |
| Degraded read | impossible: fallback | any surviving copy | any `k` of `k+m`; fewer → fallback |
| Cache repair | none needed (refill by read-through) | copy from a survivor | reconstruct from `k` survivors |
| Eviction | 1 delete | `r` deletes | `k+m` deletes (any failure leaves an orphan for reconciliation) |
| Population | 1 write, record | `r` writes, all acknowledged, record | `k+m` writes, all acknowledged, record; a partial write is deleted and never recorded |
| Failure-domain awareness | none (a device loss loses its copies) | members in distinct domains | members in distinct domains |
| Complexity | lowest | low | moderate (shared with durable path) |

Choice and reasons:

- `none` is the default. A cache miss costs one durable read; for a cache
  whose purpose is latency, paying `r×` or `(k+m)/k×` capacity to avoid
  that read after a *rare* cache fault is usually not worth it, and
  population already competes with reads for the cache device.
- `replication` suits small cache tiers with few devices where hit latency
  matters more than capacity and a device loss would otherwise empty the
  cache; `r = 2` is the sensible maximum.
- `ec` suits large cache tiers on many devices where capacity is the
  constraint and the parallel `k`-shard read is itself a throughput
  benefit; it also keeps a hot working set hot across a device loss. Its
  costs are CPU per population and the tail latency of `k` RPCs; a
  mis-sized `k` turns every hit into a fan-out. Measurements are in
  RFC-009.

Failure behaviour (all modes): a hit is served only if the assembled
chunk's length and CRC-32C match the chunk record; a degraded hit (some
shards missing or corrupt but `≥ k` readable) is served and the missing
shards are rewritten asynchronously by the gateway (read-triggered cache
repair, bounded by a per-gateway concurrency limit); an unreadable copy
(`< k` shards) is a fallback to the durable read plus invalidation, as in
§5.3. A population that cannot write every shard deletes what it wrote and
records nothing. Eviction and GC delete every member's shard; a failed
delete becomes an orphan for cache reconciliation, which runs per member.
A membership change bumps the cache group's epoch exactly as for durable
groups: stale populations are rejected by the node; existing placements
stay valid because readers do not pass an epoch.

Cache repair in the controller is **subordinate to durable repair**: a
cache group with an unhealthy member is rebuilt (replace member, fence,
reconstruct from `k` survivors, verified completion, same state machine as
RFC-008 §7) only in a repair pass in which no durable group is `Repairing`
or needs a replacement, and each pass reconstructs at most
`cache_repair_chunks_per_pass` chunks before persisting its cursor and
yielding. Chunks of a cache group are validated against the chunk record
of a live generation whose cache placement names the group; a cache shard
with no such placement is skipped (reconciliation deletes it). Losing a
cache repair entirely costs hit ratio, never data.

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
- Cache and tiering deletes (eviction, cache reconciliation, the clean-up
  of a migration attempt's target copies) carry the delete fence like
  every node delete (Phase 3.9, ADR-0023, RFC-008 §6.11). While it is
  closed nothing is evicted from the nodes: the record goes and the
  copy stays an orphan for a later reconciliation, so the cache nodes'
  space is returned only once the fence opens again. Populations and
  hits are unaffected.

**Cache groups and deferred reclamation (Phase 3.12, ADR-0028).** Cache
copies are discardable: their deletes stay immediate. A node learns a
group's role from the write class: a group written with the cache class
is cache until a durable write, which makes it durable for good (noted
before that write is stored); an unknown group is durable.

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

## 11. Acceptance criteria and Phase 2 status

Phase 2 tests: `crates/testkit/tests/it/cache.rs` (in-memory) and
`cache_fdb.rs` (FoundationDB, CI job `fdb` and `deploy/dev/fdb-test.sh`),
`tiering.rs`, and `crates/metadata/tests/cache_model.rs`. Decisions are
recorded in ADR-0009; the roadmap page is `docs/roadmap/phase-2.md`.

| Requirement / criterion | Status | Evidence |
| --- | --- | --- |
| T1, T2 tiers as configuration, device class separate from role | verified | `create_groups_for_class`, `create_cache_groups`; `tiering.rs` builds a second durable tier from relabelled nodes; CLI `--tier`, `--device-class`, `create-cache-groups` |
| T3 cache deletion changes no version; durable placement untouched by caching | verified | `miss_then_hit_serves_from_cache` (durable shard count unchanged), `crash_states_are_recovered_idempotently` (c), model test C4 |
| T4 generation-keyed cache; stale generation never served | verified | `overwrite_never_serves_a_stale_generation` |
| T5 any cache failure degrades to a durable read | verified | `population_failure_never_fails_the_get`, `cache_node_failure_falls_back_and_invalidates`, `corrupt_cache_copy_falls_back_and_is_invalidated`, fenced epoch case |
| Hit, miss, partial hit, range GET, concurrent GET, stampede | verified | `miss_then_hit_…`, `partial_hits_and_range_gets`, `concurrent_gets_populate_each_chunk_once` |
| Admission (size, miss threshold, capacity) | verified | `admission_rejects_over_capacity_and_large_chunks` |
| LRU eviction, watermarks, pins, `over_watermark` | verified | `eviction_respects_watermarks_lru_and_pins` |
| Prefetch, pin, unpin | verified | `prefetch_and_pin_populate_synchronously` |
| T6 crash states of §5.6 recovered idempotently; total cache metadata loss | verified | `crash_states_are_recovered_idempotently` (a)–(c) |
| T7 migration copy → verify → switch → retire; source kept until the switch | verified | `tiering.rs`: switch, crash before, crash after, refused switch |
| T8 cache with GC, repair, reconciliation, epoch change | verified (single-pass interleavings) | `cache_survives_durable_repair_gc_and_fenced_cache_epoch`, `overwrite_…` (GC); sustained concurrent load not run |
| Record invariants C1–C4 | verified (model) | `cache_model.rs` |
| FoundationDB instantiation | verified | `cache_fdb.rs` 12/12 in CI and in the local container runner |
| Cache protection `none` / `replication` / `ec` selectable per tier; `k`, `m`, `r` as parameters (§5.9) | verified | `cache_ec.rs` (replication 2 and RS 2+1 modules), `cache_ec_fdb.rs`; `create-cache-groups --protection` |
| Coded cache hit from `k` shards; durable placement untouched; members on distinct failure domains | verified | `coded_hit_reads_shards_and_leaves_durable_alone` |
| Degraded cache read on shard loss, node down, or corruption; read-triggered cache repair | verified | `shard_loss_is_a_degraded_hit_repaired_on_read`, `member_down_degrades_then_unreadable_falls_back`, `corrupt_shards_are_missing_and_too_many_fall_back` |
| Unrecoverable cache copy falls back to the durable tier and is invalidated | verified | same cases (fewer than `k` members, all shards corrupt) |
| Population or eviction with a member down: nothing partial recorded; orphans reconciled | verified | `population_with_a_member_down_records_nothing`, `eviction_with_a_member_down_leaves_orphans_for_reconciliation` |
| Controller cache repair: verified completion, per-pass budget, subordinate to durable repair | verified (budget and completion); priority by construction in `repair_pass`, no dedicated test | `controller_cache_repair_rebuilds_a_lost_member_within_budget` |
| Epoch change fences stale cache populations; placements stay readable | verified | `epoch_change_fences_stale_population_and_keeps_placements` |
| Durable repair and GC with a coded cache | verified | `durable_repair_and_gc_leave_the_coded_cache_alone` |
| Write-through population | not implemented | §5.3, Phase 2.1 option |
| Per-bucket tier policies | not implemented | §10 |
| Performance (§9) | not measured | hypotheses only; RFC-009 |
| Real network faults during cache use | not verified | Phase 1.2 carry-over; R-35, R-40 remain High |
