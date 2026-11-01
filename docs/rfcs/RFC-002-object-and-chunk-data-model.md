# RFC-002: Object and Chunk Data Model

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-001
- Related ADRs: ADR-0005

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

S3 exposes buckets, keys, and versions. arca-lake stores erasure-coded
shards on many devices and moves them between tiers. The model in between
must let physical data move without changing anything a client can observe,
make overwrites and deletes atomic, keep garbage collection simple enough
to be provably safe, and keep metadata size proportional to data size with
a small constant.

## 2. Goals

- Separate logical identity (bucket, key, version) from physical placement.
- Make object data immutable once committed; overwrites create new internal
  generations.
- Allow per-chunk placement, caching, and migration.
- Keep every record small enough for the metadata store's value limit and
  every write small enough for its transaction limit.
- Define the invariants that RFC-003 (metadata), RFC-004 (placement),
  RFC-006 (tiering), and RFC-008 (GC) rely on.

## 3. Non-goals

- The exact byte encoding of metadata records (RFC-003) and of on-disk
  shard records (RFC-005).
- Erasure-coding mathematics (RFC-004).
- Access control and tenancy (RFC-012).

## 4. Requirements

- R1 Physical movement must never change an object's version ID, ETag, or
  generation.
- R2 An overwrite on an unversioned bucket must make the previous data
  unreferenced atomically with publishing the new data.
- R3 Any chunk must be independently readable, cacheable, and migratable.
- R4 Each chunk record must fit comfortably under the metadata store's value
  limit (*Verified* for FoundationDB: 100,000 bytes); a generation's
  records must be writable in several transactions (10,000,000 bytes each).
- R5 Garbage collection must never need to count references across
  generations in the MVP (DD-7, ADR-0005: single-owner chunks).

## 5. Proposed design

### 5.1 Entities

```text
Bucket ──< ObjectKey ──< Version ──1 Generation ──< Chunk ──1 ChunkPlacement
                                                     │
                                                     └──< CacheLocation (0..n)
StripeGroup ──< (device, shard index)      EC profile, tier, epoch, state
```

| Entity | Identity | Mutability | Definition |
| --- | --- | --- | --- |
| **Bucket** | name; internal `bucket_id: u64` | configuration mutable | Namespace with versioning state, placement and tiering policy references, default EC profile, quotas. Deleting and recreating a bucket yields a new `bucket_id`; old metadata is unreachable. |
| **Object key** | `(bucket_id, key)` | — | UTF-8 key, 1..=1024 bytes. |
| **Current pointer** | `(bucket_id, key)` | replaced atomically | The current version of a key: `generation`, `version_id`, `delete_marker`, `next_ordinal`, denormalized `size`, `etag`, `last_modified`. |
| **Version** | `(bucket_id, key, ordinal)` | immutable | One entry of a key's history when versioning is enabled: `version_id`, `generation`, `delete_marker`, `committed_at`, `size`, `etag`. Newest first in key order. |
| **Generation** | `gen_id: [u8;16]` (UUID v7) | state machine | One immutable byte sequence plus user metadata: `state`, `bucket_id`, `key`, `size`, `etag`, `content_type`, `user_metadata`, `chunk_count`, `created_at`. |
| **Chunk** | `(gen_id, index: u32)`; `chunk_id: [u8;16]` cluster-unique | record mutable (placement), bytes immutable | A contiguous slice of the generation's data: `offset`, `len`, `crc32c`, EC scheme and `(k, m)`, **durable placement** `(group_id, epoch)`, cache locations, migration state, pin state. |
| **Stripe group** | `group_id: u64` | membership mutable with epoch | Ordered list of `k + m` `(node, device)` members satisfying the failure-domain rule of its EC profile and tier; `epoch`; `state ∈ {active, sealed, degraded, repairing}`. |
| **EC profile** | name | configuration | `scheme_id`, `k`, `m`, `m_ack` (default `m`), `max_chunk_size`, `min_ec_object_size`, replication factor below it, failure-domain rule. |
| **Tier** | name | configuration | Device class, policy, allowed profiles, capacity targets, role (durable, cache, or both). |

### 5.2 Generation lifecycle

```text
          begin_put               commit_put
  (none) ──────────▶ Pending ───────────────▶ Committed
                       │  lease expiry /                │ displaced / deleted /
                       │  abort_put                     │ version expired
                       ▼                                ▼
                    Retiring ◀──────────────────────────┘
                       │  grace period, shards deleted
                       ▼
                    (removed)
```

- `Pending{lease_until}`: being written. Chunks may be added. Reclaimable
  once the lease expires (RFC-003 §5.6).
- `Committed`: referenced by a current pointer and/or a version record.
- `Retiring{since}`: unreferenced. Shards may be deleted after the grace
  period; the metadata is removed last (RFC-008).

A generation never returns from `Committed` to `Pending`, and never from
`Retiring` to anything. Invariant **G1**: a chunk belongs to exactly one
generation. Invariant **G2**: a generation referenced by any current pointer
or version record is `Committed`.

### 5.3 Versions

| Versioning | PUT | DELETE |
| --- | --- | --- |
| Disabled | Replaces the current pointer; `version_id = "null"`; the displaced generation (if any, not a marker) becomes `Retiring`. | Clears the pointer; displaced generation `Retiring`. |
| Enabled | New `version_id` (UUID v7, hex); new version record; pointer replaced; displaced generation stays `Committed` (it is in the history). | Delete marker becomes current and is recorded in the history. |
| Suspended | Replaces the `null` version only: pointer replaced with `version_id = "null"`; a displaced `null` generation retires; a displaced real version stays. | `null` delete marker becomes current. |

Rule **V1**: a generation is retired by a pointer change only when its
`version_id` is `"null"` (nothing else references it). This single rule
covers all three states.

### 5.4 Chunks and stripes

Object data is cut into chunks of at most `max_chunk_size` (profile
parameter; DD-1). Each chunk is one erasure-coding stripe: the chunk's bytes
are split into `k` contiguous data shards of equal length (zero padded to
the coder's alignment, 64 bytes in the current backends), `m` parity shards
are computed, and shard `i` is stored on member `i` of the chunk's stripe
group. A byte range of a chunk touches only the data shards covering it
(`arca-lake-ec::StripeLayout`).

Objects smaller than `min_ec_object_size` use replication (`k = 1`,
`m = replicas − 1`), expressed as the same stripe abstraction (DD-13).

The shard key on a storage node is `(group_id, chunk_id, shard_index)`
(RFC-005). The chunk record's `(group_id, epoch)` plus the group's member
list is all a reader needs to find every shard.

### 5.5 Placement representation

Durable placement is `(group_id, epoch_at_write)` (DD-3). Repair and
membership change update the group record and bump the epoch; chunk records
are untouched. A reader with an older cached group record may contact a
former member, which rejects the epoch; the reader refreshes the group and
retries.

Cache locations are explicit per-chunk `(node, device)` entries keyed by
generation (RFC-006). They are discardable and never counted toward
durability.

### 5.6 Manifest layout

The generation header and its chunk records share a key prefix
(`("al","g",gen,"h")`, `("al","g",gen,"c",index)` in the current layout,
RFC-003 §5.2) so one range read fetches everything a GET needs, and a
subrange of chunk indices can be read for a ranged GET. The manifest is
written incrementally during the upload (RFC-003 §5.3), which is what keeps
arbitrarily large objects within the transaction limit.

### 5.7 Multipart uploads

A multipart upload owns one pending generation. Each part is a run of chunks
with its own part ETag recorded in a part record. `CompleteMultipartUpload`
validates the part list, assigns final chunk indices in part order (chunk
records written during upload use `(part_number, chunk_in_part)` ordering so
no data is rewritten), computes the multipart ETag, and commits. Abort or
expiry retires the generation. Part size limits and the 10,000-part limit
follow S3.

### 5.8 Concurrency and linearization points

| Operation | Linearization point | Notes |
| --- | --- | --- |
| PutObject / CompleteMultipartUpload | Commit transaction writing the pointer | Before: old pointer; after: new. |
| Overwrite PUT (unversioned) | Same transaction | Also sets the old generation `Retiring`. |
| Concurrent PUTs | Commit order | Loser's generation is retired at the winner's commit (if the loser committed first) or when the loser's commit runs (it displaces the winner, which is correct: last commit wins). Conditional PUTs evaluate `If-Match`/`If-None-Match` inside the transaction. |
| GET during PUT | Pointer read | Never observes `Pending`. |
| DELETE during GET | Delete transaction | Reader keeps its generation; data is retained for the grace period (RFC-008). |
| Multipart completion | Commit transaction | Parts arriving afterwards are rejected by the generation state check. |
| Tier migration | Switch transaction | Chunk record's durable placement changes; nothing else. |
| EC repair | Group epoch update | Chunk records unchanged. |
| GC | Removal of the generation record | Shard deletion is unobservable (unreferenced). |

## 6. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Whole-object stripes | Unbounded shard size, no streaming, whole-object repair and migration (DD-1). |
| Multi-object aggregated stripes | Multi-owner GC and re-encoding compaction (DD-1). |
| Placement list inside the manifest | Migration and repair would rewrite manifests; manifests should be immutable (DD-3, DD-6). |
| Per-chunk explicit `k+m` device lists for durable placement | Device failure rewrites every chunk record on the device (DD-3). |
| Reference-counted shared chunks (free CopyObject) | Refcount correctness becomes a durability dependency (DD-7); deferred to a dedicated RFC. |
| Generation IDs as counters | Needs a hot counter key; UUID v7 gives time ordering without coordination. |

## 7. Failure scenarios

- **Crash after begin, before commit**: `Pending` with a lease; expiry sweep
  retires it; its chunks are reclaimed (RFC-003 §5.6, RFC-008).
- **Crash during commit with unknown outcome**: the gateway re-reads the
  pointer; if its generation is current, success; otherwise it may retry
  the commit (idempotent on generation) or fail.
- **Two gateways complete the same multipart upload**: second completion sees
  `Committed` and fails explicitly.
- **Group membership changes under a reader**: epoch mismatch → refresh →
  retry; no stale data is read because shards are checksummed and keyed by
  chunk ID.
- **Metadata record decode failure**: surfaced as an explicit codec error;
  never treated as "not found".

## 8. Consistency and durability guarantees

- Versions and generations are immutable once committed; ETag and version
  ID are fixed at commit.
- Physical movement (repair, migration, caching, compaction) changes only
  placement records or storage-node-local state.
- Exactly one generation owns a chunk (G1); unreferenced generations are the
  only GC input.
- Not guaranteed: visibility of a generation whose commit transaction has
  not succeeded; durability of cache copies.

## 9. Performance considerations

- Metadata per object: one pointer, one header, `ceil(size / max_chunk_size)`
  chunk records, plus a version record when versioning is enabled.
  *Hypothesis*: chunk records of ~100 bytes keep metadata under 0.01% of
  data for chunk sizes in the single-digit MiB range. To be measured.
- Listing reads only pointers (denormalized size and ETag): no per-key
  header read.
- Ranged reads fetch only the covering chunk records and only the covering
  data shards.

## 10. Open questions

- Chunk-record encoding: postcard (current prototype) is compact but not
  schema-evolvable; protobuf is self-describing but larger. Decide in
  RFC-003 before Phase 1.
- Whether to store the part-level ETags in the generation header or only in
  part records (affects header size for 10,000-part uploads).
- Inline storage of very small objects in metadata (DD-13 option C).

## 11. Acceptance criteria

- `arca-lake-metadata` records and recipes implement §5.1–§5.3 and §5.8;
  the concurrency tests in `crates/metadata/tests/protocol.rs` pass against
  the in-memory store and the FoundationDB backend (currently *Verified*
  for the subset implemented: put/overwrite/delete, conditional creates,
  versioning states, lease expiry, GC queue).
- A property test shows that for any interleaving of PUT, DELETE, and GC on
  one key, the visible state is always a committed generation or absent, and
  no chunk of a visible generation is ever in `Retiring`.
- Multipart upload recipes (§5.7) exist and are tested (Phase 1).
