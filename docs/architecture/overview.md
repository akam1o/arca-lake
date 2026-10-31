# Architecture overview

Status: **draft for design review** (pre-RFC). This document summarizes the
recommended architecture. Each major area will be specified in an RFC; see
[the RFC index](../rfcs/README.md). Labels: *Verified*, *Assumption*,
*Hypothesis*, *Open* are defined in [docs/README.md](../README.md).

## 1. Requirements analysis

### 1.1 What the requirements ask for

arca-lake must be an S3-compatible object store that is:

- strongly consistent for supported operations,
- erasure coded with configurable, tier-specific profiles,
- natively multi-tiered, including a transparent read-through cache tier,
- horizontally scalable in throughput and capacity,
- recoverable from the failure classes listed in the requirements,
- operable by a small team, and
- fully configurable; nothing about scale, topology, or workload is hard-coded.

### 1.2 Ambiguities and tensions found in the requirements

These are not contradictions, but places where a naive reading would lead to
conflicting designs. Each is resolved below or recorded as *Open*.

1. **"Strong consistency" appears in Phase 1 (design) and Phase 3 (hardening).**
   Atomic object visibility cannot be retrofitted. We treat strong consistency
   as a Phase 1 *design* requirement and Phase 3 as the point where it is
   *verified under fault injection*. (Resolved.)
2. **Metadata transactions vs. distributed atomicity.** The requirements ask
   for a transactional metadata store *and* state that metadata transactions
   alone do not provide atomicity across storage nodes. The resolution is a
   two-phase write protocol in which the metadata commit is the
   linearization point but is only attempted after storage nodes have durably
   acknowledged the data. See [design decision DD-4](design-decisions.md#dd-4-atomic-object-visibility-and-the-write-protocol).
3. **Immutable data vs. CopyObject / Multipart Copy.** Sharing chunks between
   generations would require reference counting, which complicates garbage
   collection safety. *Open*: physical copy (simple GC) versus shared chunks
   with reference counts. Recommendation: physical copy for the MVP.
   See [open decisions](open-decisions.md).
4. **Object Version vs. Object Generation.** The external S3 version ID and
   the internal generation are distinct. On an unversioned bucket an overwrite
   creates a new generation and the previous generation becomes garbage after
   commit; on a versioned bucket both generations remain referenced by
   different version IDs. Physical data movement never changes a version ID
   or a generation. (Resolved by the data model.)
5. **Tier-specific EC profiles vs. the cache tier.** A cache copy is
   discardable and should not pay erasure-coding overhead. A tier's profile
   may therefore be *replication (n=1..k)* rather than EC, and the cache tier
   will normally use single-copy or low-replication. Durability accounting
   never counts cache copies. (Resolved.)
6. **Failure-domain awareness without hard-coded topology.** "Host" and
   "rack" are examples. Topology is an operator-defined hierarchy of labelled
   levels; placement rules reference level names from configuration.
   (Resolved.)
7. **Placement Controller as a central component vs. horizontal scalability.**
   If every write asked a controller where to place data, the controller
   would be on the data path. The resolution is *stripe groups*: the
   controller pre-computes and publishes validated placement sets; gateways
   select among them locally. See [DD-3](design-decisions.md#dd-3-placement-representation-stripe-groups-vs-per-chunk-location-lists).
8. **Listing consistency with delimiters.** `ListObjectsV2` with `delimiter`
   over a sorted key space requires skip-scanning past common prefixes. This
   is a performance concern, not a consistency one, and is addressed in the
   metadata key layout.
9. **ETag semantics.** The S3 ETag for a single PUT is the MD5 of the body and
   for multipart uploads is derived from per-part MD5s. This forces MD5
   computation on the write path even though MD5 has no integrity role
   internally. Compatibility tests depend on this. (Accepted cost; measured in
   Phase 0.)

## 2. Planes and components

```text
                 ┌────────────────────────────────────────────────────┐
   S3 clients ──▶│  S3 Gateway (stateless, N instances)               │
                 │  SigV4 · S3 REST · streaming · EC encode/decode    │
                 └──────┬───────────────────────────┬─────────────────┘
                        │ metadata txns             │ shard I/O (gRPC)
                        ▼                           ▼
        ┌──────────────────────────┐   ┌──────────────────────────────┐
        │ Metadata Plane           │   │ Data Plane                   │
        │ metadata layer (library) │   │ Storage Nodes (M instances)  │
        │ over a distributed       │   │  local storage engine        │
        │ transactional KV store   │   │  append-only segments        │
        └──────────▲───────────────┘   │  checksums · scrub · repair  │
                   │                   └──────────────▲───────────────┘
                   │ leases, cluster map, work queues │ repair / migrate / GC
        ┌──────────┴──────────────────────────────────┴───────────────┐
        │ Control Plane: Controller (leader-elected, 1 active)        │
        │  placement · tiering · repair · scrubbing · GC · admin API  │
        └─────────────────────────────────────────────────────────────┘
```

### 2.1 S3 Gateway (`arca-lake-gateway`)

Stateless. Terminates HTTP/HTTPS, authenticates (AWS SigV4), authorizes,
parses S3 requests, and executes the read and write protocols. The gateway
splits object data into chunks, erasure-codes each chunk into shards, writes
shards directly to storage nodes, and commits metadata. On read it fetches
the manifest, reads the minimum set of shards (preferring data shards, falling
back to decode), and streams the result.

Gateway state is limited to in-flight requests and caches of the cluster map
and bucket configuration. Any gateway can serve any request; failure of a
gateway only aborts in-flight requests, which are recoverable through the
write protocol.

### 2.2 Metadata plane (`arca-lake-metadata`)

A library crate that defines the `MetadataStore` trait, the key layout, and
the transaction recipes (allocate generation, commit version, list, etc.). The
recommended backing store is **FoundationDB** (see [DD-5](design-decisions.md#dd-5-metadata-store-selection)).
An in-memory implementation exists for tests.

The metadata layer is embedded in gateways and the controller rather than
deployed as a separate network service in the first phases. See
[DD-10](design-decisions.md#dd-10-metadata-layer-embedded-library-vs-separate-service).

Owned concepts: buckets, object keys, versions, generations, chunk manifests,
chunk placement records, multipart upload state, stripe groups, cluster map,
placement epochs, leases, GC queues, migration state.

### 2.3 Storage node (`arca-lake-storage-node`, engine in `arca-lake-storage`)

Owns a set of local devices. Each device hosts an independent instance of the
storage engine: an append-only, immutable-record segment store with a
per-device shard index, per-record checksums, crash recovery by segment
replay, scrubbing, and compaction. Exposes a versioned gRPC API for shard put,
get (with byte ranges), delete, list-by-group, and scrub reporting. Enforces
placement epochs (fencing).

The storage node does not know about objects, buckets, or S3. It stores
shards addressed by `(stripe_group, chunk_id, shard_index)`.

### 2.4 Controller (`arca-lake-controller`)

One active leader (leased through the metadata store), hot standbys. Hosts
several control loops that are separate crates with pure, testable logic:

- **Placement** (`arca-lake-placement`): topology model, stripe-group
  creation and validation against failure-domain rules, capacity balancing,
  node add/remove, epoch management, rebalancing plans.
- **Tiering** (`arca-lake-tiering`): policy evaluation, promotion/demotion
  decisions, cache admission and eviction, migration state machines.
- **Repair and scrub orchestration**: reacts to degraded stripe groups and
  scrub findings.
- **Garbage collection**: drives tombstoned generations and orphaned shards
  through grace periods to deletion.
- **Admin API**: a separate HTTP API for placement inspection, prefetch,
  promotion, demotion, pinning, migration status, and policy management. It
  is not mixed into the S3 API surface.

The controller is never on the data path. If it is down, reads and writes
continue using the last published cluster map; only rebalancing, repair,
tiering, and GC pause.

### 2.5 Erasure coding (`arca-lake-ec`)

Defines `ErasureCoder` (encode `k` data shards into `m` parity shards; decode
any `k` of `k+m`), EC profiles, and stripe layout. Backed by mature
libraries; see [technology selection](technology-selection.md#erasure-coding).

### 2.6 Internal protocol (`arca-lake-protocol`)

Versioned protobuf definitions for storage node and controller APIs, and the
versioned on-the-wire framing of shards. Every message carries a protocol
version; every persistent record carries a format version.

## 3. Data model (summary; detailed in RFC-002)

| Concept | Definition |
| --- | --- |
| **Bucket** | Namespace with configuration: versioning state, placement policy, tiering policy, default EC profile, quotas. |
| **Object key** | UTF-8 key within a bucket. |
| **Object version** | Externally visible S3 version identity. `null` on unversioned buckets. Delete markers are versions. |
| **Object generation** | Internal, monotonically unique identifier for one immutable byte sequence plus user metadata. Allocated at the start of a write; committed or garbage collected. A version points to exactly one generation. |
| **Chunk** | A contiguous, fixed-maximum-size slice of a generation's data. The unit of erasure coding, placement, caching, and migration. Immutable. Identified by a cluster-unique chunk ID. |
| **Chunk manifest** | Ordered list of a generation's chunks with sizes, offsets, and checksums. Immutable once the generation commits. |
| **Chunk placement record** | Mutable record per chunk: durable location (stripe group + epoch), optional cache locations, migration state, pin state. Separate from the manifest so that physical movement never rewrites the manifest. |
| **Stripe group** | A controller-validated ordered set of `k+m` devices across failure domains for one EC profile and tier, with a membership epoch. Shards of a chunk placed in a group occupy shard index `i` on device `i`. |
| **EC profile** | `(scheme, k, m, shard_size_policy, min_ec_object_size, failure_domain_rule)`. `scheme` may be replication. |
| **Physical location** | `(node_id, device_id)`; appears only inside stripe-group membership and cache records. |
| **Tier** | Operator-defined name with device class, policy, allowed EC profiles, capacity targets, and role (durable and/or cache). |
| **Placement epoch** | Monotonic counter per stripe group; incremented on membership change. Storage nodes reject operations tagged with an older epoch. |
| **Migration state** | Per chunk: `none`, `copying(target)`, `verifying(target)`, `switching`, `retiring(source)`. Crash-resumable. |

Objects are immutable once committed. Overwrites create a new generation.
Physical movement changes placement records only.

## 4. Core protocols (summary; detailed in RFC-003 and RFC-008)

### 4.1 Write (PutObject, single part)

1. Gateway allocates a generation ID and writes a *pending generation* record
   with a lease expiry (one small metadata transaction).
2. Gateway streams the body, computing MD5 (ETag) and any requested S3
   checksums, cutting it into chunks. For each chunk: encode into `k+m`
   shards, select a stripe group from the cached cluster map according to the
   bucket's policy, write shards in parallel to the group's devices with the
   group epoch. Each storage node acknowledges only after the shard is durable
   on its device (fsync or equivalent, configurable but defaulting to safe).
3. A chunk is *protected* when at least `k + m_required_ack` shards are
   acknowledged, where `m_required_ack` is a profile parameter (default: all
   `m`; lower values trade write latency for a temporarily reduced fault
   tolerance that repair must restore). The gateway records the chunk's
   manifest entry and placement record under the pending generation.
4. After the last chunk, the gateway runs the *commit transaction*: verify the
   pending record still exists and its lease is held, check conditional
   headers against the current version, write the version record pointing to
   the generation, update the current pointer, mark any superseded generation
   as *retiring* (unversioned bucket). **This transaction commit is the
   linearization point.** Only after it succeeds does the gateway return 200.
5. Retiring generations and shards of aborted generations are reclaimed
   asynchronously after a grace period.

Large objects therefore do not need a single large metadata transaction; the
manifest is built incrementally under the pending prefix and the commit is
small. This is required by the metadata store's transaction size limits
(*Verified* for FoundationDB: 10 MB per transaction, 100 KB per value, 5 s
transaction lifetime).

### 4.2 Read (GetObject, Range)

1. One metadata read fetches the version, the generation's manifest entries
   and placement records for the requested byte range (laid out as one key
   range so a single range read suffices).
2. For each chunk, the gateway prefers cache locations (if the generation
   matches), then data shards from the durable stripe group, then decodes
   from any `k` available shards (degraded read). Shard checksums are
   verified before use; a corrupt shard is treated as missing and reported.
3. On a cache miss, and if the bucket's tiering policy admits the chunk, the
   gateway (or the controller, by policy) schedules a cache fill. Stampede
   suppression is per chunk.

### 4.3 Multipart upload

Each part is written like a single-part object body under the upload's
pending generation, producing a manifest fragment and a part ETag.
`CompleteMultipartUpload` is a metadata-only operation: it validates the part
list, concatenates fragments into the manifest, computes the multipart ETag,
and commits. Chunks are not re-copied. Abort and expiry reclaim parts.

### 4.4 Tier migration (durable to durable)

`copying` → write chunk shards to the target group; `verifying` → read back
and compare checksums; `switching` → one metadata transaction swaps the
durable location and marks the source `retiring`; `retiring` → source shards
are deleted after the grace period. Reads are served from whichever durable
location is current at every step. Each step is idempotent and resumable
from the recorded state.

### 4.5 Cache (read-through)

Cache locations are additional, discardable placements keyed by chunk and
generation. Admission is policy driven (frequency, recency, size, free
capacity). Eviction is driven by tier capacity. Loss of a cache device
affects only latency. Cache copies are never counted toward durability.

## 5. Failure handling principles

- **Durability before acknowledgment.** No success response before the
  profile's durability condition is met.
- **End-to-end checksums.** Per shard on disk, per chunk in the manifest,
  per object as ETag and optional S3 checksums.
- **Epochs and fencing.** Membership changes bump epochs; stale writers are
  rejected by storage nodes.
- **Recovery journal is the metadata store.** Pending generations, migration
  states, and GC queues are durable records with leases; any controller or
  gateway can resume them.
- **Fail explicitly.** If a stripe group cannot meet its profile (too few
  healthy members for the required acknowledgments), writes to that group are
  refused and other groups are chosen; if no group qualifies, the write fails
  with a retryable error. Reads with fewer than `k` readable shards fail with
  an explicit integrity error, never with silently wrong data.

## 6. What this architecture intentionally does not do

- It does not route data through the controller.
- It does not store object bytes in an embedded key-value database.
- It does not implement consensus; it relies on the metadata store.
- It does not hard-code tier names, tier count, topology levels, chunk sizes,
  EC parameters, or any throughput or capacity figures.

## 7. Where to go next

- [Design decisions and alternatives](design-decisions.md)
- [Technology selection](technology-selection.md)
- [Phase 0 plan](../roadmap/phase-0.md)
- [Risks](risks.md) and [open decisions](open-decisions.md)
