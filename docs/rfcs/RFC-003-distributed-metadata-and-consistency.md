# RFC-003: Distributed Metadata and Consistency

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-001, RFC-002
- Related ADRs: ADR-0005; ADR for OD-05 pending

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

Every consistency property arca-lake offers reduces to what its metadata
store can do atomically and what the protocols around it guarantee. The
store has hard limits (value size, transaction size, transaction duration)
and an optimistic concurrency model, so the write protocol must be shaped
to those limits while still giving strong consistency, atomic visibility,
safe conditional writes, and crash recovery for every intermediate state.

## 2. Goals

- Strict serializability for the metadata operations behind supported S3
  operations.
- Atomic visibility of objects, with the commit transaction as the single
  linearization point.
- Conditional writes evaluated inside the commit transaction.
- Objects of any size without ever exceeding the store's transaction limit.
- A recovery path for every intermediate write state, driven only by durable
  metadata records (no separate journal).
- A backend abstraction so that the store can be swapped without touching
  the protocols.

## 3. Non-goals

- Deciding tiering and cache semantics (RFC-006) or the GC schedule
  (RFC-008); this RFC defines the records and state transitions they use.
- Multi-region metadata.
- Metadata partitioning across clusters (OD-20); the key layout keeps a
  bucket's keys self-contained so that it remains possible.

## 4. Requirements

| ID | Requirement | Status |
| --- | --- | --- |
| R1 | Multi-key transactions with strict serializability | *Verified* for FoundationDB (official documentation) |
| R2 | Value ≤ 100,000 bytes; key ≤ 10,000 bytes; transaction ≤ 10,000,000 bytes; transaction ≤ 5 s | *Verified* for FoundationDB ("Known Limitations"); enforced by the in-memory emulation and observed on a real cluster in Phase 0 |
| R3 | A write never returns success before its data met the durability condition on storage nodes and its commit succeeded | DD-4 |
| R4 | Concurrent conditional creates of the same key: exactly one succeeds | *Verified* on the in-memory store and on FoundationDB (`tests/protocol.rs`, `tests/fdb.rs`) |
| R5 | Listing is a range read with cost proportional to the output | DD-5; *Verified* functionally |
| R6 | Crashed writers leave discoverable state that GC can reclaim | DD-4 |

## 5. Proposed design

### 5.1 Key-value abstraction

`arca-lake-metadata::kv` defines:

- `KvStore::begin() -> Tx`, `limits()`.
- `Tx::get`, `get_range(range, limit, reverse)`, `set`, `clear`,
  `clear_range`, `commit() -> commit_version`.
- `transact(store, body)`: runs `body` in a new transaction, retrying on
  `Conflict` up to a bound. `body` must be safe to re-run.

Semantics are FoundationDB's: snapshot reads, buffered writes, read-write
conflict detection on keys and ranges, size errors surfaced at commit. The
in-memory backend (`mem::MemKv`) implements exactly these semantics,
including the limits, so that recipes are tested against them without a
database; the FoundationDB backend (`fdb::FdbKv`) maps onto the client.

Unknown commit outcome (`commit_unknown_result`) is **not** retried inside
`transact`; it is returned as a backend error and the caller resolves it by
re-reading (see §7).

### 5.2 Key layout (version 1)

All keys are FoundationDB tuples rooted at `"al"`:

| Tuple | Value | Purpose |
| --- | --- | --- |
| `("al","meta","schema")` | u64 LE | metadata schema version (RFC-014 §5.4; 2 since Phase 3.1) |
| `("al","meta","cluster")` | `ClusterState` | recovery state and incarnation (RFC-015 §5.3) |
| `("al","g",root,"m",part_number)` | `PartRef` | part manifest reference of a multipart root (RFC-007 §5.3) |
| `("al","mpu",bucket_id,key,upload_id)` | `MultipartRecord` | multipart upload (`Open`, `Aborting`, `Completed`) |
| `("al","mpp",upload_id,part_number)` | `PartRecord` | sealed part of an upload |
| `("al","ctr","bucket")` | u64 | bucket ID allocator |
| `("al","b",name)` / `("al","bi",id)` | `BucketRecord` | bucket by name / by ID |
| `("al","o",bucket_id,key)` | `CurrentPointer` | current version of a key |
| `("al","v",bucket_id,key,MAX−ordinal)` | `VersionRecord` | version history, newest first |
| `("al","g",gen,"h")` | `GenerationHeader` | generation |
| `("al","g",gen,"c",index)` | `ChunkRecord` | one key per chunk |
| `("al","pend",lease_until_ms,gen)` | — | pending generations by lease expiry |
| `("al","gc",since_ms,gen)` | — | retired generations by retirement time |

Properties: a bucket's object keys are one range (listing); a generation's
header and chunks are one range (GET); the pending and retired indexes are
time ordered (sweeps read a prefix). Object-key prefixes become key ranges
by dropping the tuple string terminator (`keys::object_prefix_range`).

Planned additions (RFC-004/006/008): stripe groups `("al","grp",id)`,
cluster map and topology, multipart uploads `("al","mpu",bucket_id,upload_id,…)`,
leases `("al","lease",name)`, and cache locations.

### 5.3 Write protocol

```text
gateway                               metadata store                    storage nodes
  │ begin_put(bucket,key)  ─────────▶  tx: header=Pending{lease}, pend index
  │ for each chunk:
  │    encode, write k+m shards  ───────────────────────────────────────▶  fdatasync, ack
  │    (k + m_ack acks)          
  │ append_chunks(batch) ───────────▶  tx: require Pending & leased; set chunk records
  │                                      (batches sized to a quarter of the tx limit)
  │ commit_put(info, preconditions) ─▶  tx: require Pending & leased
  │                                        read bucket, current pointer
  │                                        evaluate If-Match / If-None-Match
  │                                        header=Committed, clear pend index
  │                                        pointer=new; version record if versioning
  │                                        displaced null generation → Retiring + gc index
  │ ◀── version id ────────────────────  commit   ← LINEARIZATION POINT
```

- `append_chunks` re-checks `Pending` and the lease in every batch, so a
  writer that lost its lease cannot keep adding chunks to a generation that
  the sweep has retired.
- Batch sizing charges `3 × key + value + 128` bytes per mutation against a
  quarter of the transaction limit. *Verified*: the raw key+value size was
  rejected by a real server (`transaction_too_large`); the conservative
  estimate passed with 120,000 chunk records.
- The commit transaction touches one bucket record, one pointer, two headers
  at most, one version record, and two index entries: well under limits and
  independent of object size.
- `m_ack`: the number of parity shards that must be acknowledged before a
  chunk counts as protected; default `m`. Values below `m` require the
  profile to say so, mark the chunk under-protected, and are deferred to
  Phase 3 (OD-17).

### 5.4 Read protocol

`read_manifest(bucket, key, chunk_range)`: one transaction reads the pointer,
the header, and the chunk records in the requested index range. A delete
marker or missing pointer yields "not found". The gateway then reads shards
according to RFC-004 and verifies checksums. Readers never see a `Pending`
generation because pointers only ever reference `Committed` ones (G2).

### 5.5 Listing

`list_objects(prefix, delimiter, start_after, continuation, max_keys)`
scans the pointer range in pages of read-only transactions. When a key
contains the delimiter after the prefix, the common prefix is emitted once
and the scan jumps to the end of that prefix's range, so hidden keys are not
read. Delete markers are skipped. The continuation token is the raw key
position of the next page; after a common prefix it is the end of that
prefix's range, so a prefix is never emitted twice. *Verified* functionally
with pagination tests. Each page is its own snapshot: a listing is a
sequence of consistent pages, not one global snapshot.

### 5.6 Leases, expiry, and GC input

- `begin_put` sets `lease_until = now + lease` (lease is configuration).
- `expire_pending(now)` scans `("al","pend")` up to `now`, and for each entry
  re-reads the header in its own transaction: if still `Pending` and
  expired, sets `Retiring{since: now}` and queues it. A commit racing the
  sweep is resolved by the store: exactly one of them wins. *Verified* in
  `tests/protocol.rs::lease_expiry_and_abort`.
- `collectable(now, grace)` returns retired generations older than the
  grace period; `finish(gen)` removes a generation's keys after its shards
  are gone (RFC-008 owns the schedule).

Time is a parameter of every recipe, never read inside them, so tests
control it; production passes wall-clock time. Grace-period measurement by
store commit versions instead of wall clock is OD-16.

### 5.7 Record encoding

Records are `postcard` with a leading version byte. Phase 1 evaluated the
alternatives and keeps this choice (ADR-0006):

| Criterion | postcard + version byte (**kept**) | protobuf |
| --- | --- | --- |
| Size | Minimal: varints, no field tags, no length prefixes for fixed arrays. Matters because chunk records are the bulk of metadata and are charged against FoundationDB's transaction budget (§5.3). | Field tags and length prefixes on every field and nested message; larger per record (not measured in Phase 1). |
| Schema evolution | Any change is a new record version with an explicit decoder for every older version. Adding a field is never silent. | Unknown fields are skipped and missing fields take defaults, which is exactly what RFC-002 forbids for metadata (an unknown version must be an explicit error). The flexibility would have to be disabled by convention. |
| Self-description | None; tooling needs the schema of the version it reads. Acceptable: records are read only by arca-lake code, and `arca-lake-cli` ships every decoder. | Readable with generic tooling and `.proto` files. |
| Cross-language | Rust only (serde). Metadata is internal; the external surfaces are S3 and the gRPC node API, which already uses protobuf. | Yes. |
| Toolchain | serde derive; no code generation. | protoc (already vendored for the node API); generated structs would need a mapping layer to the record types used by the recipes. |

Rules that make the choice safe: every record starts with a version byte;
decoders match the version exactly and return `Codec` errors otherwise;
golden encodings of every record version are kept in tests
(`records.rs::roundtrip_and_version_check`) so that a change to a struct
fails CI unless a new version is introduced. The gRPC node API stays
protobuf because it crosses processes and releases (RFC-004 §5.13).

### 5.8 Commit outcome unknown

A commit whose reply is lost (timeout, dropped connection, FoundationDB's
`commit_unknown_result`) surfaces as `KvError::CommitUnknown`. `transact`
never retries it. Resolution (ADR-0007, RFC-008 §5.2):

1. `resolve_commit(generation)` reads the header: `Committed` → the PUT
   succeeded; `Pending` with a live lease → the commit may be retried (the
   commit transaction re-checks the state and the preconditions, so a
   retry can never publish twice); `PendingExpired` → do not retry;
   `Retiring`/`Missing` → the write failed.
2. After a bounded number of still-unknown retries the gateway fences the
   generation with `abort_put`. The abort reads and writes the header, so
   it conflicts with a late commit: after a successful abort no commit can
   land; an abort rejected with "committed" proves the commit landed and
   the PUT is reported as success.
3. If even the fence fails, the generation is left `Pending`; lease expiry
   (`expire_pending`, same transaction shape) decides later with the same
   exclusion.

Idempotent recipes (`begin_put` with its pre-chosen generation ID,
`append_chunks`) use `transact_idempotent`, which re-runs on an unknown
outcome. `delete_object` on an unversioned bucket is retried once by the
gateway (a repeated delete is a no-op there); on a versioned bucket it
would need a resolution step before a second marker, which is not
implemented. *Verified* on both backends by `tests/commit_unknown.rs` and
the testkit fault suite (`faults.rs`, `faults_fdb.rs`).

### 5.9 Linearization points

Every state transition is one transaction; the table names it so that the
recovery rules in RFC-008 can refer to "before" and "after" unambiguously.

| Transition | Linearization point (transaction) | Visible effect |
| --- | --- | --- |
| Object version becomes visible | `commit_put` | pointer and header `Committed` written together |
| Object version disappears | `delete_object` / `commit_put` of a successor | pointer removed or replaced; displaced generation `Retiring` |
| Pending write abandoned | `abort_put` or `expire_pending` (per generation) | header `Retiring` |
| Generation metadata gone | `gc::finish` | all keys of the generation cleared (shards must already be deleted) |
| Membership changes | `cluster::update_group` in `repair::replace_member` | new members, `epoch + 1`, `Repairing`, cursor |
| Group healthy again | `cluster::update_group` at repair completion | `Active` |
| Controller leadership changes | `leader::try_acquire` | holder, token + 1, lease |
| Leader keeps leadership | `leader::renew` | lease extended, token unchanged |

Node-side effects (shard bytes durable, shard deleted) happen outside these
transactions and are made safe by the epoch rules in RFC-008 §6.4.

### 5.10 Leader election

The controller's leader record and the fenced store wrapper live in this
crate (`ops::leader`, `fence::FencedKv`) because they are plain
transactions on the same store: see RFC-011 §5.2–5.3.

## 6. Alternatives considered

| Alternative | Why not |
| --- | --- |
| One commit transaction containing the whole manifest | Breaks the 10 MB limit for large objects; aborted uploads are undiscoverable (DD-4). |
| Two-phase commit across storage nodes | Storage nodes would need transactional state; the store already provides one atomic commit point (DD-4). |
| TiKV | Viable; fewer size limits, more processes, weaker simulation-testing story. Kept behind the trait (DD-5). |
| Separate metadata service | Extra hop and component (DD-10). |
| Retrying unknown commit outcomes automatically | Recipes such as `commit_put` are not idempotent under re-execution after an actual commit (the displaced generation would be retired twice or a second version created); resolution re-reads first (§5.8). Only `begin_put` and `append_chunks` are retried. |

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Gateway crash before commit | `Pending` with lease; expiry sweep retires; chunks reclaimed. |
| Gateway crash after commit, before responding | Object is visible; client retries get the committed object (and, for `If-None-Match: *`, a `PreconditionFailed`, which is the S3-visible truth). |
| Commit outcome unknown | §5.8: re-read, bounded retry while pending, then fence with `abort_put`; an abort that finds `Committed` means success. *Verified* on both backends. |
| Conflict | `transact` retries the whole body; recipes are written to be re-runnable (IDs chosen before the loop). |
| Store unavailable | All operations fail with retryable errors; no stale data is served as authoritative. |
| Oversized record (e.g., huge user metadata) | Explicit `TooLarge` before anything is published. *Verified* on both backends. |
| Lease expired during a long upload | Next `append_chunks` or the commit fails with `LeaseExpired`; the client restarts the upload. Lease length is configuration and must exceed the longest expected chunk write. |

## 8. Consistency and durability guarantees

- Operations on one key are linearized by the commit transaction; across
  keys, transactions are strictly serializable.
- Invariants I1–I5 of RFC-008 §4 are maintained by the recipes and checked
  by a model-based test (`tests/model.rs`) on random operation sequences.
- A committed version's generation is immutable and `Committed`.
- A `Retiring` generation is never referenced by a pointer or version record
  (V1 in RFC-002).
- Not guaranteed: a single global snapshot across listing pages; visibility
  of anything before its commit.

## 9. Performance considerations

- Metadata operations per PUT: 1 + ⌈chunks / batch⌉ + 1 transactions.
- Per GET: one read-only transaction (no commit).
- *Hypotheses*: FoundationDB commit latency in the low milliseconds on a
  properly provisioned cluster; sequential object keys within one bucket
  create write hot spots that FoundationDB's shard splitting mitigates.
  Local numbers from the Phase 0 spike are relative only and not published.

## 10. Open questions

- OD-05 final confirmation (measurements on a described environment).
- OD-16 grace-period measurement.
- Whether `append_chunks` should use FoundationDB's `get_approximate_size`
  instead of the static estimate.
- Versioned-bucket DELETE after an unknown outcome needs a resolution step
  before a second delete marker (§5.8).

## 11. Acceptance criteria

- `tests/protocol.rs` and `tests/fdb.rs` pass against the in-memory and
  FoundationDB backends (currently *Verified*).
- A randomized test interleaves `commit_put`, `expire_pending`,
  `delete_object`, and `finish` on one key and checks the invariants of
  RFC-002 §8 after every step (Phase 1).
- A documented procedure for resolving unknown commit outcomes exists and is
  exercised by a fault-injection test (Phase 1).
