# RFC-004: Erasure Coding and Placement

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-002, RFC-003, RFC-005
- Related ADRs: ADR-0005 (stripe groups, gateway-side EC, small-object replication)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

Object data must survive the loss of devices, hosts, and racks with a
configurable, documented tolerance; a chunk must stay readable while
members fail and are replaced; stale writers must never corrupt a stripe
after a membership change; repair must not race with concurrent writes;
and all of this must be expressed in metadata that does not grow with the
number of chunks on a failed device.

## 2. Goals

- One abstraction (stripe = chunk, shard `i` on member `i` of a stripe
  group) that expresses both replication and Reed–Solomon coding.
- Per-profile fault tolerance stated explicitly and enforced by placement
  across operator-defined failure domains.
- Per-group placement epochs with fencing on storage nodes.
- Degraded reads from any `k` shards; background repair that reconstructs a
  replaced member while writes continue.
- Checksums at shard, chunk, and object level.
- Profile changes and rebalancing as data movement that never changes
  object identity.

## 3. Non-goals

- Locally repairable or hierarchical codes (future profile types).
- Cache placement policy (RFC-006); this RFC only reserves the separation
  between durable and cache placement.
- Capacity-planning formulas; the controller balances by measured free
  capacity.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| R1 | A committed chunk tolerates the loss of any `m` shards for its profile and remains readable. |
| R2 | No two shards of a chunk are placed in the same failure domain at the level the profile names (default: host). |
| R3 | A writer holding a stale membership cannot write a shard that the current membership would not account for. |
| R4 | Repair of a member replaces exactly the shards of that member and converges even while new chunks are written to the group. |
| R5 | A device failure changes O(1) metadata records, not one per chunk. |
| R6 | Mixed profiles coexist indefinitely; a profile change is per chunk and crash-resumable. |
| R7 | Every shard, chunk, and object carries a checksum verified before use. |

## 5. Proposed design

### 5.1 Stripe, chunk, shard, group

```text
object bytes ──chunking──▶ chunk c (≤ max_chunk_size)
chunk c ──StripeLayout(k)──▶ data shards d0..d(k-1)   (equal length, zero padded)
                 ──encode──▶ parity shards p0..p(m-1)
shard i of chunk c ──▶ stored on member i of stripe group g (device), key (g, chunk_id, i)
```

- `chunk_id = generation[0..12] ‖ index_be_u32`. The generation's UUID v7
  prefix carries its creation time and enough randomness to make the
  128-bit chunk ID unique in practice, and lets a storage node or the
  controller map any shard back to its generation and age without extra
  metadata (used by orphan GC, §5.10).
- A stripe group is an ordered list of `k + m` `(node, device)` members with
  one EC profile and one tier, a monotonically increasing **epoch**, and a
  state: `active` (accepts new chunks), `sealed` (no new chunks; used for
  draining), `degraded` (a member is unhealthy), `repairing`.
- The chunk record (RFC-002) stores the durable placement as
  `(group_id, epoch_at_write)` plus the scheme and `(k, m)` used, so a
  reader never needs the profile table to decode.

### 5.2 EC profiles

```yaml
ec_profiles:
  - name: standard
    scheme: rs-gf8-v1        # arca-lake-ec SchemeId
    data_shards: 8
    parity_shards: 4
    required_parity_acks: 4  # m_ack; default = parity_shards
    max_chunk_bytes: 8Mi
    small_object:            # below this size, replicate instead
      threshold_bytes: 256Ki
      replicas: 3
    failure_domain: host     # topology level that must differ between shards
    checksum: crc32c
```

| Property | Replication (`replication-v1`, k = 1, m = r − 1) | Reed–Solomon (`rs-gf8-v1`, k + m) |
| --- | --- | --- |
| Storage overhead | `r` | `(k + m) / k` |
| Tolerated losses | `r − 1` | `m` |
| Healthy read | any one copy, no compute | `k` data shards, no compute |
| Degraded read | another copy | any `k` of `k + m`, one decode (*Verified* laptop: classic RS reconstructs one shard at memory speed) |
| Repair traffic per lost shard | 1 × shard | `k` × shard |
| Write fan-out | `r` | `k + m` (each shard `1/k` of the chunk) |
| Small objects | natural | wasteful below `threshold_bytes` |

Both are expressed by the same `ErasureCoder` trait and stripe layout, so
the write and read paths do not branch on the scheme. The default profile
for Phase 1 development is `rs-gf8-v1` with small-object replication; the
production default is a deployment decision.

**Fault tolerance statement** (to be printed by `--check-config`): profile
`P` tolerates the simultaneous loss of `m` shards; with
`failure_domain: host` and `k + m` distinct hosts that means any `m` hosts;
with `failure_domain: rack` and `k + m` distinct racks, any `m` racks. If the
topology has fewer domains than `k + m`, group creation fails explicitly.

### 5.3 Topology and failure-domain-aware placement

Topology is operator configuration: devices belong to nodes, nodes carry
labels for arbitrary levels (`host`, `rack`, `zone`, …). The placement
module (`arca-lake-placement`, pure logic) creates groups by choosing
`k + m` devices such that the values of the profile's `failure_domain`
label are pairwise distinct, preferring devices with the most free capacity
and the fewest existing group memberships. Alternatives considered:

| Option | Trade-off |
| --- | --- |
| Hash-based device selection (CRUSH-like) | No controller state, but cannot express capacity- and tier-aware policy (DD-14). |
| Controller-chosen groups published in a cluster map (**chosen**) | O(1) metadata per device change; gateways choose among active groups locally. |
| Per-chunk device selection at write time | Maximal flexibility; O(chunks) metadata on device failure (DD-3). |

Gateways cache the cluster map (groups, members, epochs, node addresses)
and refresh it on any epoch-mismatch error or on a timer. A write chooses
an `active` group of the bucket's profile and tier, weighted by free
capacity.

### 5.4 Placement epoch and fencing

- Every group record carries `epoch`. Storage nodes persist, per group they
  belong to, the highest epoch they have been told (`UpdateGroupEpoch`
  RPC), and reject `PutShard`/`DeleteShard` whose epoch is lower.
- Membership change protocol (controller):
  1. Choose the new member set `M'` and `epoch' = epoch + 1`.
  2. Send `UpdateGroupEpoch(g, epoch')` to every reachable member of `M ∪ M'`.
     Unreachable *old* members are the ones being replaced; if they return,
     they learn the epoch from the cluster map before serving writes.
  3. Commit the new group record (one metadata transaction).
  4. Start repair (§5.6).
- A gateway with a stale map writes with the old epoch; any member that
  already learned `epoch'` rejects it. Because a chunk needs `k + m_ack`
  acknowledgments, the write fails, the gateway refreshes the map and
  rewrites the chunk's shards with `epoch'` (same `chunk_id`, same bytes:
  `PutShard` is idempotent on the storage engine).
- Reads carry no epoch: a shard's bytes are self-validating (checksummed
  and keyed by chunk ID), so reading from a former member is harmless; it
  just may be missing.

### 5.5 Write path (gateway)

For each chunk, in parallel across shards:

1. `StripeLayout::split` → `encode` → `k + m` shards.
2. `PutShard(group, epoch, chunk_id, i, bytes)` to member `i`; the storage
   node acknowledges only after its durability policy (group commit) has
   synced the record (RFC-005).
3. The chunk is **protected** when all `k` data shards and at least `m_ack`
   parity shards are acknowledged. With `m_ack = m` (default) every shard
   must be acknowledged; otherwise the chunk record is marked
   `under_protected` and queued for repair.
4. The chunk record (`crc32c` of the chunk, scheme, `k`, `m`, placement)
   is appended to the pending generation (RFC-003 §5.3).

If any member rejects with an epoch error, refresh and retry the chunk; if
a member is unreachable or returns an I/O error, retry a bounded number of
times, then **fail the chunk**: the gateway may select another active group
for this chunk and retry, and the shards already written to the first group
become orphans handled by §5.10. The commit transaction never runs unless
every chunk reached protection.

### 5.6 Read path and degraded reads

For each chunk covering the requested range:

1. Resolve `(group, epoch)` → current members from the cluster map.
2. Request the data shards covering the range from their members. Each
   storage node verifies its block checksums; a corrupt shard is reported as
   missing (and quarantined locally).
3. If any needed data shard is missing, request parity shards and any other
   data shards until `k` shards are available, then `reconstruct`.
4. Verify the chunk's `crc32c` (full-chunk reads) or rely on shard block
   checksums (range reads), then stream.
5. If fewer than `k` shards are obtainable: fail the request with an
   explicit integrity error (HTTP 500 with a distinct code) and flag the
   chunk for repair. Never return partial or wrong bytes.

### 5.7 Background repair

Trigger: a member is unhealthy (missed heartbeats beyond a configurable
window, or a device reports failure, or scrub/read reports corruption for
many chunks). Controller:

1. Membership change (§5.4) replacing member `i` with a device in a
   different failure domain from the remaining members.
2. Enumerate chunk IDs of the group by `ListShards(group)` on healthy
   members (union); the replacement is included so already-written new
   chunks are skipped.
3. For each chunk whose shard `i` is missing on the replacement: fetch any
   `k` shards, reconstruct shard `i`, `PutShard` it with `epoch'`. Idempotent
   and resumable: progress is a cursor over chunk IDs stored in the group
   record (`repair_cursor`), so a controller failover continues.
4. When the enumeration completes, set the group `active` (or `sealed` if
   it was sealed) and clear `degraded`.

**Repair concurrent with writes.** New chunks written during repair already
carry shard `i` on the replacement (they use `epoch'` and the new member
set), so repair cannot "fix" them wrongly: step 3 skips shards that exist.
A chunk whose write is in flight when the enumeration runs is either
complete on the replacement (skipped) or will be completed by its writer;
in both cases the end state is one shard `i` with identical bytes. Repair
and writers never write different bytes under the same key because shard
bytes are a deterministic function of the chunk.

Repair bandwidth is rate-limited by configuration per node and per cluster.

### 5.8 Rebalancing and node addition

Adding devices creates new groups that include them; the controller weights
new writes toward groups with more free capacity. Moving existing chunks
off full devices is the same operation as tier migration (RFC-006): copy
the chunk to a new group, verify, switch the chunk record's placement, retire
the old shards after the grace period. Removing a node = mark its groups
`sealed`, repair each group onto other devices (§5.7, treating the removed
member as failed), then drain remaining sealed groups by migration.

### 5.9 EC profile change

A new profile means new groups. Existing chunks are re-encoded by migration
(RFC-006 state machine): read the chunk (decode if needed), encode with the
new profile, write to a group of the new profile, verify, switch, retire.
Because every chunk record stores its own scheme and `(k, m)`, chunks of
different profiles coexist within one object and across the system
indefinitely; no global cut-over is needed.

### 5.10 Checksums and orphan handling

| Level | Checksum | Verified by |
| --- | --- | --- |
| Shard blocks | CRC-32C per block (RFC-005) | storage node on every read and scrub |
| Chunk | CRC-32C of the chunk bytes in the chunk record | gateway after assembling or decoding a full chunk; controller after reconstruction before writing |
| Object | ETag (MD5 or multipart composite) and optional S3 checksums | gateway on PUT; returned on GET/HEAD |

**Orphaned shards** arise when a gateway crashes after writing shards but
before appending the chunk record or committing. Two mechanisms reclaim
them (RFC-008 owns scheduling):

1. Generation-driven: an expired pending generation is retired; its
   recorded chunks' shards are deleted on their members; then its metadata
   is removed.
2. Node-driven reconciliation: a node lists its shards per group; for each
   `chunk_id` the controller derives the generation prefix and age from the
   UUID v7 bytes; if no generation with that prefix exists in metadata and
   the age exceeds `lease + grace`, the shard is deleted. Shards younger
   than that are left alone because their generation may still be pending.

### 5.11 Incomplete stripes and coexistence windows

- A chunk whose write did not reach protection is never referenced by a
  committed generation; it is garbage (§5.10).
- A committed chunk with `m_ack < m` acknowledged parity shards is
  `under_protected`; repair fills the missing parity. Its tolerance until
  then is `m_ack` losses, which the profile author accepted explicitly.
- During a membership change, chunk records keep `epoch_at_write`; readers
  use the *current* members. Shards on a replaced member are not deleted
  until repair completes and the member is dropped from the record; if the
  old member returns, its shards are harmless duplicates until GC.
- During migration between groups (profile change, rebalancing, tiering),
  the chunk record lists both placements with a state (RFC-006); both are
  readable; the source is retired only after the target is verified.

### 5.12 Metadata additions (to RFC-003 layout)

| Tuple | Value |
| --- | --- |
| `("al","node",node_id)` | `NodeRecord { address, labels, devices, state, last_seen }` |
| `("al","grp",group_id)` | `GroupRecord { profile, tier, members: [(node_id, device_id)], epoch, state, repair_cursor }` |
| `("al","map","version")` | cluster-map version bumped on any node/group change (gateways poll it) |

### 5.13 Storage node API additions (`arca_lake.storage.v1`)

`DeleteShard`, `ListShards(group, cursor)`, `UpdateGroupEpoch(group, epoch)`,
`Health`. `PutShard` returns `EpochRejected` when the node knows a higher
epoch. All are idempotent.

## 6. Alternatives considered

- Per-chunk explicit member lists (DD-3): rejected for O(chunks) metadata
  on failure.
- Node-side encoding (DD-9): rejected; gateway CPU scales statelessly.
- Epoch per chunk instead of per group: rejected (same reason as DD-3).
- Delete-and-rewrite for profile change: rejected; migration keeps the
  object identity and is resumable.
- Random chunk IDs with a separate chunk→generation index: rejected in
  favour of generation-prefixed IDs, which make orphan attribution and
  aging possible without an index.

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Member down during write | Chunk fails protection; gateway retries, then chooses another group; leftover shards are orphans (§5.10). |
| Member down during read | Degraded read from `k` shards; chunk flagged. |
| More than `m` members down | Read fails explicitly; writes avoid the group; data unavailable until members return (no silent loss). |
| Controller fails mid-membership-change | Either the new record is committed (repair resumes from `repair_cursor`) or not (nodes may hold `epoch'` already; the next attempt reuses or increments it; writers with the old epoch are rejected and refresh; the map still shows the old members, which still serve reads). |
| Stale gateway after membership change | Epoch rejected → refresh → rewrite chunk (idempotent). |
| Corrupt shard | Reported as missing by the node; decode from others; repair rewrites it. |
| Network partition between gateway and some members | Writes to those groups fail protection; gateway uses other groups; reads degrade. |
| Repair and writer race | Both produce identical bytes for the same key; storage engine put is idempotent. |

## 8. Consistency and durability guarantees

- A committed chunk has at least `k + m_ack` durably acknowledged shards on
  distinct failure domains of the profile's level at commit time.
- Chunk and shard checksums are verified before any byte reaches a client.
- Membership changes never change chunk records; repair never changes bytes.
- Not guaranteed: tolerance beyond `m` (or `m_ack` for under-protected
  chunks); availability when fewer than `k` members are reachable.

## 9. Performance considerations

- Write fan-out `k + m` connections per chunk; gateway CPU for encoding
  (*Verified* laptop: classic RS encodes at several GiB/s per core).
- Degraded reads cost one reconstruction per missing shard (memory-speed
  for classic RS on a single missing shard).
- Repair traffic `k × shard` per lost shard; rate-limited.
- *Hypothesis*: `ListShards` on large groups needs pagination and a per-group
  index on the node (the storage engine's ordered index gives it for free).

## 10. Open questions

- Health detection parameters and whether gateways report member errors to
  the controller (likely yes, as hints).
- `m_ack < m` support timing (OD-17).
- Whether to store `under_protected` per chunk or per group.
- Repair prioritization when several groups are degraded.

## 11. Acceptance criteria and Phase 1 status

Implemented in Phase 1 (`arca-lake-gateway::cluster`, `arca-lake-controller`,
`arca-lake-storage-node`, `arca-lake-placement`) and exercised by
`crates/testkit/tests/e2e.rs` on an in-process cluster (in-memory metadata
backend, real storage engines and gRPC):

| Criterion | Status |
| --- | --- |
| Write and read through ≥ 3 storage nodes with an RS profile (2+1, multi-chunk objects, range reads) | *Verified* |
| Small objects placed on `replication-v1` groups; readable with two of three members stopped | *Verified* |
| GET with one node stopped decodes from `k` shards; with fewer than `k` reachable the request fails explicitly | *Verified* |
| Corrupt shard reported by the node as `DataLoss`, decoded around; two corrupt shards of a 2+1 chunk fail explicitly | *Verified* |
| Repair of a replaced member: epoch bump, reconstruction of exactly the missing shards, byte-identical reads with another member down, new writes carry the new epoch, re-run is a no-op | *Verified* |
| Stale-epoch writes rejected by nodes and by the gateway path until the membership commits | *Verified* |
| Orphan shards reclaimed: generation-driven (abort, expiry, overwrite, delete) and node-driven reconciliation by generation prefix and age; young orphans kept | *Verified* |
| `m_ack < m` (under-protected chunks) | Not implemented; `m_ack = m` enforced (OD-17) |
| Failure-domain-aware group creation | *Verified* at unit level (`arca-lake-placement`); E2E clusters use one device per host |
| Capacity-weighted group choice | Not implemented (free capacity is not reported yet; groups are rotated) |
| Health detection, automatic repair triggering, rate limiting | Not implemented; repair and GC are driven explicitly |
| `ListShards` pagination | Not implemented (whole list per group) |
| Rebalancing and profile change (migration, RFC-006) | Not implemented |
| `--check-config` fault-tolerance statement | `EcProfile::fault_tolerance_statement` exists; not wired to a CLI flag |

Behaviour exercised by the E2E tests but outside this RFC's scope is listed
in RFC-003 (concurrent and conditional PUT, GET during PUT, DELETE during
GET, crashes around the commit).
