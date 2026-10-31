# Key design decisions and alternatives

Status: **draft for design review**. Each decision below lists at least two
options, the trade-offs, a recommendation, and what would change our mind.
Recommendations become ADRs once reviewed. Labels (*Verified*, *Assumption*,
*Hypothesis*, *Open*) are defined in [docs/README.md](../README.md).

Index:

- [DD-1 Erasure coding granularity](#dd-1-erasure-coding-granularity)
- [DD-2 Chunk layout and the storage engine's persistence model](#dd-2-chunk-layout-and-the-storage-engines-persistence-model)
- [DD-3 Placement representation: stripe groups vs. per-chunk location lists](#dd-3-placement-representation-stripe-groups-vs-per-chunk-location-lists)
- [DD-4 Atomic object visibility and the write protocol](#dd-4-atomic-object-visibility-and-the-write-protocol)
- [DD-5 Metadata store selection](#dd-5-metadata-store-selection)
- [DD-6 Tier migration semantics](#dd-6-tier-migration-semantics)
- [DD-7 Garbage collection safety](#dd-7-garbage-collection-safety)
- [DD-8 Failure recovery: epochs, fencing, and repair](#dd-8-failure-recovery-epochs-fencing-and-repair)
- [DD-9 Where erasure coding happens](#dd-9-where-erasure-coding-happens)
- [DD-10 Metadata layer: embedded library vs. separate service](#dd-10-metadata-layer-embedded-library-vs-separate-service)
- [DD-11 Tiering placement granularity and policy attachment](#dd-11-tiering-placement-granularity-and-policy-attachment)
- [DD-12 Cache write policy](#dd-12-cache-write-policy)
- [DD-13 Small objects](#dd-13-small-objects)
- [DD-14 Horizontal scalability model](#dd-14-horizontal-scalability-model)
- [DD-15 Operational simplicity](#dd-15-operational-simplicity)

---

## DD-1 Erasure coding granularity

**Question.** What is the unit that gets erasure coded?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Whole object as one stripe | Object split into `k` equal shards | Minimal metadata; one placement decision per object | Shard size varies with object size: tiny shards for small objects (per-shard I/O overhead dominates), huge shards for large objects (no streaming, no parallelism across groups, repair granularity is the whole object). Range reads of large objects touch all `k` devices. |
| B. Fixed-maximum-size chunk as one stripe (**recommended**) | Object cut into chunks of at most `C` bytes; each chunk is a `k+m` stripe placed in one stripe group | Bounded shard size; streaming encode/decode; different chunks can go to different groups (parallelism, balancing); chunk is a natural unit for caching, migration, range reads, repair | Manifest grows with object size (one entry per chunk); small objects still need a policy (DD-13). |
| C. Multi-object aggregated stripes (segment-level EC) | Storage nodes pack many objects' bytes into large segments; segments (not objects) are erasure coded | Excellent small-object efficiency; large sequential I/O | Object deletion leaves holes requiring compaction that must re-encode; a lost device requires reconstructing whole segments; GC and placement become much more complex; conflicts with "gateway writes directly to `k+m` nodes" and makes degraded reads of one small object expensive. |

**Trade-offs discussed.**

- *Read amplification.* With B, a range read touches only the chunks covering
  the range, and within a chunk only the data shards covering the range (for
  systematic codes, each data shard is a contiguous slice of the chunk if the
  stripe is laid out as `k` contiguous pieces). Reading a byte range of
  length `L` inside one chunk costs about `L` bytes plus per-shard overhead
  when all data shards are healthy.
- *Write amplification.* `(k+m)/k` for any option; B additionally pays
  per-chunk metadata.
- *Repair.* B repairs per stripe group (DD-3), reading `k` shards per lost
  shard. Per-shard repair traffic is `k × shard_size` for each lost shard; this
  is the standard RS repair cost and is a reason to keep `k` moderate.
- *Metadata overhead.* B costs one manifest entry and one placement record
  per chunk. With chunk sizes in the single-digit to tens of megabytes range,
  a very large object has thousands of entries, which is well within range
  reads of the metadata store but must be written incrementally (DD-4).

**Recommendation.** B. Chunk maximum size is a per-EC-profile parameter, not a
constant. Default values will be chosen from Phase 0 measurements, and the
on-disk and metadata formats must accept any chunk size so defaults can
change without migration.

**Would change our mind.** If Phase 0 shows that the metadata cost per chunk
dominates for the object size distributions we care about, we would raise the
default chunk size rather than switch options.

---

## DD-2 Chunk layout and the storage engine's persistence model

**Question.** How does a storage node persist shards on a device?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. One file per shard on a local file system | Shard = file; directory fan-out | Simplest; repair/scrub with ordinary tools | Metadata-heavy for many small shards (inode churn, directory lookups); fsync cost per file; file system journaling overhead; poor control over layout; deletion cost. |
| B. Append-only segments on a local file system with a shard index (**recommended for Phase 0–1**) | Large preallocated segment files; shards appended as framed, checksummed records; index maps `(group, chunk, shard)` → `(segment, offset, len)`; dead space reclaimed by compaction | Sequential writes; group commit of fsync; small number of open files; immutable records make checksumming and crash recovery simple (replay tail); deletion is a tombstone plus later compaction | Needs an index (embedded KV or custom); compaction adds background I/O; must manage segment lifecycle. |
| C. Raw block device with a custom extent allocator | Bypass the file system; manage extents and a write-ahead log directly | Maximum control; no double journaling; prerequisite for SPDK | Highest implementation and testing cost; must build allocator, WAL, and recovery; harder to operate (no ordinary tooling). |
| D. Store shards in an embedded KV database (e.g., RocksDB) | Shard bytes as values | Reuses a mature engine | LSM write amplification on large values; compaction rewrites object data repeatedly; memory and CPU cost; violates the requirement not to assume an embedded KV for data. |

**Recommendation.** B now, with an internal `BlockDevice`-like abstraction so
that C can be added later without changing the segment format. Use the
embedded KV only for the shard index (and even that behind a trait so a
custom index can replace it if measurements justify it). D is rejected for
data.

**Design points for B.**

- Segment header with format version, device UUID, segment ID. Each record:
  header (version, group, chunk ID, shard index, length, flags), payload,
  checksum of header + payload. A record is valid only if its checksum
  verifies; recovery scans the last segment and truncates at the first
  invalid record.
- Checksum algorithm is a format field; CRC32C and CRC64-NVME are candidates.
  *Hypothesis*: hardware-accelerated CRC32C is sufficient and cheapest; CRC64
  may be preferable where S3 CRC64NVME checksums could be reused end to end.
  Phase 0 measures both.
- Durability: `fdatasync` (or `fsync` where directory entries change)
  before acknowledging. Group commit amortizes fsync across concurrent
  writes. Write barriers and `O_DIRECT` versus buffered I/O are measured in
  Phase 0, not assumed.
- Index durability: the index is a cache of the segments. If the index is
  lost or inconsistent, it is rebuilt by scanning segments. This keeps the
  segments as the single source of truth and tolerates index engine crashes.
- Compaction copies live records to a new segment and atomically retargets
  index entries; the old segment is deleted only after the new one is
  durable.

---

## DD-3 Placement representation: stripe groups vs. per-chunk location lists

**Question.** How does metadata express where a chunk's shards live, and what
happens on device failure?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Per-chunk explicit location list | Each chunk record lists `k+m` `(node, device)` pairs | Maximal flexibility; any chunk anywhere; simple to understand | Device failure requires updating the placement record of **every** chunk on that device (potentially millions of metadata writes under failure conditions); repair must enumerate affected chunks via a secondary index; placement validation per chunk. |
| B. Stripe groups (**recommended**) | Controller creates groups: ordered `k+m` device sets validated against failure-domain rules, with an epoch. A chunk's durable placement is `(group_id, epoch_at_write)`. Shard `i` lives on member `i`. | Device failure updates one group record (new member, epoch+1) regardless of chunk count; repair enumerates shards via the storage node's local per-group index; placement rules validated once per group; gateways choose groups locally from a cached cluster map, keeping the controller off the data path; natural unit for capacity balancing (weight groups). | Less flexibility (chunk cannot be partially relocated without a group change); requires a group lifecycle (create, fill, seal, drain); hot groups need weighting. Conceptually similar to placement groups in other systems, so it is well understood. |

**Recommendation.** B for durable placements. Cache placements use simple
per-chunk location lists (option A) because they are discardable and
single-device.

**Details.**

- The number of groups is a function of device count and a configurable
  target groups-per-device; operators do not hand-manage groups.
- A group has states: `active` (accepts new chunks), `sealed` (read-only,
  used for draining), `degraded` (member unhealthy; reads decode; writes
  refused or allowed with reduced acknowledgment per policy), `repairing`.
- Each group record carries the profile, the tier, the member list with
  device IDs, and the epoch. Storage nodes keep a copy of the groups they
  belong to.

---

## DD-4 Atomic object visibility and the write protocol

**Question.** How does an object become visible atomically, with durability
verified first, given transaction size and time limits in the metadata store?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Single commit transaction containing manifest and version | Buffer manifest in memory; write everything at the end | Simple reasoning | Breaks for large objects: *Verified* FoundationDB limits (10 MB per transaction, 100 KB per value); an aborted upload leaves no metadata trail, so orphaned shards can be found only by scanning storage nodes. |
| B. Pending generation with incremental manifest and a small commit (**recommended**) | Write a pending record first; append manifest/placement entries as chunks are protected; final commit writes the version and flips the current pointer | Works for any object size; aborted uploads are discoverable (pending record with expired lease) so GC is targeted; commit is tiny and fast | More metadata writes during upload (amortized per chunk, which is cheap relative to chunk I/O); needs lease handling. |
| C. Two-phase commit across storage nodes | Prepare on all nodes, then commit | Textbook atomicity | Storage nodes would need transactional state; latency; the metadata store already gives us a single atomic commit point, so this is unnecessary. |

**Recommendation.** B.

**Linearization points** (what a concurrent observer sees before/after):

| Operation | Linearization point |
| --- | --- |
| PutObject / CompleteMultipartUpload | Commit of the transaction that writes the version record and current pointer. Before: old version (or none). After: new version. |
| Overwrite PUT (unversioned) | Same transaction also marks the old generation `retiring`. Readers that already fetched the old manifest continue to read it within the grace period (DD-7). |
| Concurrent PUTs to the same key | Both proceed through data writes independently; commits serialize on the key's current pointer. Last committer wins by commit order (not by request start). The loser's generation becomes `retiring` immediately. Conditional writes (`If-None-Match: *`, `If-Match`) are evaluated inside the commit transaction, so exactly one of two concurrent conditional creates succeeds. |
| GET during PUT | GET reads the committed current pointer; it never observes a pending generation. |
| DELETE during GET | DELETE's commit (tombstone/delete marker) is the point. In-flight GETs hold a manifest reference and are served from retained data within the grace period; a GET that outlives the grace period may fail with a retryable error, which it handles by re-reading metadata and returning `NoSuchKey` if appropriate. |
| Multipart completion | Commit of the manifest assembled from parts. Parts uploaded after completion begins but before commit are rejected by the state check in the same transaction. |
| Tier migration | The `switching` transaction (DD-6). |
| EC repair | Group epoch update. Repaired shards become authoritative when the group record names the new member. |
| Garbage collection | Deletion of the generation record after all shards are deleted (deletion of shards is not observable, since nothing references them). |
| Metadata failure mid-write | If the commit transaction's outcome is unknown (timeout), the gateway re-reads the version; if its generation is current, report success, otherwise retry the commit (idempotent on generation ID) or fail. |

**Durability condition.** A chunk is protected when `k + m_ack` shards are
durably acknowledged, where `m_ack` is a profile parameter with default `m`.
Lower `m_ack` is allowed only when the profile declares it; the chunk is then
recorded as `under-protected` and repair must bring it to full redundancy
before the group is considered healthy. The commit transaction fails if any
chunk did not reach `k + m_ack`.

---

## DD-5 Metadata store selection

Evaluated against the requirements: distributed transactions, strong
consistency, scale to the metadata volume of multi-petabyte clusters, mature
operations, Rust client, permissive license.

| Candidate | Consistency model | Known constraints (*Verified* where cited) | Rust client | Assessment |
| --- | --- | --- | --- | --- |
| **FoundationDB** | Strict serializable, optimistic, multi-key transactions | Values ≤ 100,000 B; transactions ≤ 10,000,000 B; transaction lifetime ≤ 5 s; HDD not recommended for the ssd engine; tested up to ~500 processes and ~100 TB of KV data; no built-in access control (see FoundationDB "Known Limitations", v8.0 docs) | `foundationdb` 0.11.0, MIT/Apache-2.0, supports API through 7.4 (crates.io) | Strongest correctness story (deterministic simulation testing); constraints are all addressable by the key layout (DD-4). Operational dependency on a native client library (`libfdb_c`) that must match the server version. **Recommended.** |
| **TiKV** | Snapshot isolation with Percolator-style transactions, Raft replication | Needs PD plus TiKV processes; heavier footprint; large values acceptable | `tikv-client` 0.4.0, Apache-2.0 (crates.io, 2026-02) | Viable; fewer size constraints but more moving parts and a weaker simulation-testing story. Keep as the fallback behind the `MetadataStore` trait. |
| **etcd** | Linearizable KV with mini-transactions | Designed for configuration-scale data, not per-object metadata | gRPC | Rejected for object metadata; acceptable only for small coordination data, and we do not need a second store for that. |
| **Own Raft-based store** | Whatever we build | Must build, test, and operate a consensus system | n/a | Rejected: no clear technical reason outweighs the risk, per the development principles. |

**Recommendation.** FoundationDB behind a `MetadataStore` trait with an
in-memory implementation for tests. *Hypothesis to verify in Phase 0*:
commit latency and list throughput of the proposed key layout are acceptable
on a small local cluster; sequential key insertion hot spots are manageable
with key design.

**Key-layout principles** (detailed in RFC-003):

- One subspace per bucket for object keys so that `ListObjectsV2` is a range
  read; delimiter handling uses key-selector skips.
- Manifest and placement entries for a generation live under one prefix so a
  single range read fetches everything needed for a GET.
- No single hot key: counters are sharded; sequence numbers use versionstamps.
- Every value carries a format version.

---

## DD-6 Tier migration semantics

**Question.** How does a chunk move between durable tiers without a window of
unavailability or data loss, and resumably after a crash?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Move (delete source as soon as target write returns) | Minimal transient capacity | Window where only the target holds data before verification; a crash between target write and metadata update can orphan or lose. |
| B. Copy, verify, switch, retire (**recommended**) | Target written and verified first; one transaction switches the durable location; source retired after a grace period | Reads work from a durable location at every step; crash-resumable from the recorded state; idempotent retries | Transient capacity of two copies per migrating chunk (bounded by concurrency limits). |
| C. Manifest rewrite (new generation) | Treat migration as a re-PUT | Reuses the write path | Changes the generation; breaks "physical movement does not change identity"; would invalidate caches and complicate versioning. |

**Recommendation.** B, recorded per chunk in the placement record as a state
machine: `none → copying(target) → verifying(target) → switching → retiring(source) → none`.
Each transition is one metadata transaction with a lease held by the
migrating worker; a worker that finds an expired lease may take over at the
recorded step. GC may not delete a source location while the state is not
`retiring` and the grace period has not elapsed. The durability requirement
of the *target* profile must be met before `switching`.

Explicit prefetch, promotion, demotion, pinning, and unpinning are
expressed as requested target states on the same records and executed by the
same state machine, so there is one migration implementation.

---

## DD-7 Garbage collection safety

**Question.** How do we reclaim shards without deleting data that is still
referenced or being read?

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Reference counts on chunks | Each chunk has a refcount; delete at zero | Supports chunk sharing (CopyObject without copying) | Refcount updates are metadata writes on hot paths; a lost or duplicated decrement is a correctness bug; crash between data delete and refcount update needs reconciliation. |
| B. Single-owner chunks with tombstone queue and grace period (**recommended for MVP**) | A chunk belongs to exactly one generation. When a generation becomes unreferenced (`retiring`), its chunks are enqueued; after a configurable grace period they are deleted; the generation record is removed last. | Simple invariant; no counters; targeted deletion; GC progress is observable | Requires physical copy for CopyObject (open decision); readers longer than the grace period must handle retry. |
| C. Mark-and-sweep from storage nodes | Nodes periodically list shards and ask metadata whether each is live | Finds true orphans | Expensive at scale; must not treat in-flight uploads as orphans. |

**Recommendation.** B as the primary mechanism, with a low-frequency C as a
*reconciliation* pass for orphans caused by crashes between shard write and
manifest entry. To make C safe, every shard carries its generation ID, and a
shard is an orphan candidate only if its generation is neither committed nor
pending-with-valid-lease. Candidates are deleted only after an additional
grace period.

**Reader safety.** Readers pin a generation when they fetch the manifest. The
grace period is configurable and should exceed the maximum expected read
duration for one chunk fetch plus clock skew tolerance; long object reads
re-validate per chunk, so a read that outlives the grace period of a deleted
object fails explicitly rather than returning partial data silently. We do
not rely on synchronized clocks for correctness: grace periods are measured
by the metadata store's commit versions where possible, with wall-clock as a
conservative secondary bound. *Open*: exact mechanism for grace measurement.

---

## DD-8 Failure recovery: epochs, fencing, and repair

| Mechanism | Design |
| --- | --- |
| **Placement epoch** | Per stripe group. Incremented by the controller on any membership change. Storage nodes persist the groups they serve with the current epoch and reject puts/deletes carrying an older epoch. A gateway with a stale cluster map gets an explicit error, refreshes, and retries. |
| **Controller fencing** | Leader lease in the metadata store; all controller writes are conditioned on the lease; a demoted leader cannot commit. |
| **Recovery journal** | Pending generations, migration states, repair tasks, and GC queues are metadata records with leases. Any worker can resume. No separate journal file. |
| **Degraded reads** | Any `k` of `k+m` shards; corrupt shards (checksum mismatch) count as missing and are reported for repair. |
| **Repair** | Controller detects an unhealthy member (heartbeat loss beyond a configurable threshold, or scrub reports), chooses a replacement device satisfying failure-domain rules, bumps the epoch, and schedules reconstruction of every shard of that member. The storage node's local per-group index enumerates work; progress is recorded per group. Repair bandwidth is rate-limited by configuration. |
| **Scrubbing** | Each device verifies record checksums in the background at a configurable rate; mismatches trigger repair of that shard. Periodic full-stripe verification (decode and compare) is a slower, optional mode. |
| **Quorum loss behavior** | If fewer than `k` shards of a chunk are readable: GET returns an explicit integrity error (HTTP 5xx with a distinct code); the chunk is flagged. If a group cannot meet `k + m_ack`: it is excluded from new writes. If the metadata store is unavailable: all S3 operations fail fast with retryable errors; nothing is served from stale caches as authoritative. |
| **Rolling upgrade** | Protocol and format versions negotiated; a node must be able to read one format version back and refuse to write a format newer than the cluster's agreed minimum. Upgrade tests live in CI. |

**Alternative considered:** per-chunk epochs instead of per-group. Rejected
because repair would again require per-chunk metadata writes (DD-3).

---

## DD-9 Where erasure coding happens

| Option | Description | Pros | Cons |
| --- | --- | --- | --- |
| A. Gateway-side encode/decode (**recommended**) | Gateway encodes and writes `k+m` shards directly | No extra hop; fan-out parallelism; CPU scales with gateway count (stateless); storage nodes stay simple | Gateway egress for a write is `(k+m)/k` × body; gateway CPU carries EC plus TLS plus MD5. |
| B. Primary-node encode | Gateway sends the chunk to one storage node, which encodes and forwards parity | Gateway egress is 1× | Extra hop and latency; primary node network egress is `(k+m)/k`; storage node needs placement awareness; failure of the primary mid-write complicates recovery. |

**Recommendation.** A. *Hypothesis*: EC encode cost is small compared with
TLS and hashing on the gateway. Phase 0 measures all three.

---

## DD-10 Metadata layer: embedded library vs. separate service

| Option | Pros | Cons |
| --- | --- | --- |
| A. Library embedded in gateway and controller (**recommended for Phase 1**) | One fewer process type; no extra hop; direct use of the store's transactions | Every gateway needs the native client library; changing the store means redeploying gateways; metadata access control relies on network isolation of the store. |
| B. Separate gRPC metadata service | Stable internal API; central place for caching and admission control; store can be swapped | Extra hop on every request; another stateful-looking component to operate; must re-expose transactional semantics over RPC. |

**Recommendation.** A, behind the `MetadataStore` trait so B remains possible.
Revisit if client-library operational constraints prove painful.

---

## DD-11 Tiering placement granularity and policy attachment

| Granularity | Metadata overhead | Performance | Migration cost | Failure recovery | Operational complexity | Storage efficiency |
| --- | --- | --- | --- | --- | --- | --- |
| Object | One record per object | Whole-object moves; range reads of partly-hot large objects cannot benefit | Whole object moves even if 1% is hot | Simple | Simple | Wasteful for large objects |
| **Chunk** (**recommended for placement records**) | One record per chunk (already exists for EC) | Partial caching and partial promotion of large objects | Fine-grained | Per chunk, uniform with EC repair | Moderate | Good |
| Dataset (bucket/prefix) | Negligible | Coarse | Bulk | Simple | Simplest to express | Depends on access skew within the dataset |
| Storage class | Negligible | Client-controlled | Client-controlled | Simple | Standard S3 concept | Depends on client discipline |

**Recommendation.** *Decisions* are recorded at chunk granularity (the
placement record). *Policies* are attached at bucket, optional prefix, and
storage class level, and evaluated per chunk using chunk-level access
statistics (frequency, recency), object size, tier free capacity, and
administrative overrides (pins). Access statistics are sampled and
aggregated by gateways and flushed to the controller; they are advisory and
lossy, never a correctness dependency.

---

## DD-12 Cache write policy

| Policy | Performance | Durability | Consistency | Implementation complexity |
| --- | --- | --- | --- | --- |
| **Read-through (write-around)** (**recommended first**) | Writes go to durable tier only; reads populate cache on miss | Cache never holds the only copy | Generation-keyed cache entries cannot serve stale data: a new generation has a new key | Lowest |
| Write-through | Write to durable tier and cache synchronously | Same as above | Same | Low; costs write latency and cache write bandwidth for data that may never be read |
| Write-back | Acknowledge after cache write; flush later | Cache must itself meet the durability requirement, which contradicts "discardable" | Requires flush ordering and crash protocol | Highest |

**Recommendation.** Read-through for the initial implementation; write-through
as a per-bucket option later if measurements show benefit; write-back only as
a future RFC with its own durability model. Cache consistency is by
construction: cache records are keyed by `(chunk_id, generation)` and a
generation is immutable.

---

## DD-13 Small objects

Erasure coding a small object into `k` tiny shards wastes I/O operations and
inflates per-shard overhead. Options:

| Option | Pros | Cons |
| --- | --- | --- |
| A. Replicate below a threshold (**recommended for MVP**) | Simple; profile-driven (`min_ec_object_size`, replication factor); one read I/O | Storage efficiency of replication for small objects |
| B. Pack small objects into shared stripes | Efficient | Shared stripes reintroduce multi-owner GC (DD-7 option A problems) and compaction with re-encoding |
| C. Inline very small objects in metadata | Fastest reads | Must respect value-size limits of the store; bloats the metadata store; only for tiny objects |

**Recommendation.** A, with the threshold and replication factor as profile
parameters. C is an *Open* optimization for objects far below the metadata
value limit. B is deferred.

---

## DD-14 Horizontal scalability model

- **Gateways** are stateless and scale linearly for CPU-bound work (TLS, hashing,
  EC). *Hypothesis*: aggregate throughput scales with gateway count until the
  storage nodes or network saturate.
- **Storage nodes** add capacity and throughput; the controller creates new
  stripe groups including new devices and rebalances by draining sealed groups
  at a configurable rate.
- **Metadata** scales with the chosen store; FoundationDB's documented testing
  envelope (*Verified*: ~500 processes) bounds a single cluster; a very large
  deployment may require metadata partitioning by bucket, which the key layout
  must not preclude (*Open*).
- **Controller** is a single active leader but performs only control work; its
  throughput requirements grow with change rate, not request rate.

Alternative considered: client-computed placement (hash-based) with no
controller. Rejected because capacity-aware and tier-aware placement with
operator policies is a core requirement and is poorly served by pure hashing.

---

## DD-15 Operational simplicity

Process types to operate: gateway, storage node, controller, metadata store.
All arca-lake components ship as one binary with subcommands and share one
configuration format with schema validation and `--check-config`.

The metadata store is the main operational cost. FoundationDB has its own
deployment and upgrade procedures and requires SSD-class storage (*Verified*).
We accept this because the alternative (building consensus and transactions)
is worse, but it must be documented honestly in the operations guide along
with a recommended minimal topology for small clusters.

Alternative considered: a single-process "all-in-one" mode for development and
tiny deployments. Recommended as a development convenience only; it must use
the same code paths and never relax durability checks.
