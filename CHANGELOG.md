# Changelog

0.2.0 is the second experimental release (in preparation; to be marked
pre-release). 0.1.0, the first, was published on 2026-11-23.
This page summarizes what each release contains and the changes that affect users
and operators. Detailed implementation history is preserved in the
[development archive](docs/archive/README.md). The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) (0.x: any minor version may
break compatibility).

## [0.2.0] - TBD

Second experimental release, for evaluation and development; **not
production-ready**. It closes the misdirected-write and failure-domain
gaps found in the 0.1.0 validation (R-117, R-119), makes scrub tell
retained, in-flight and unclassified shards apart (ADR-0036), fixes the
tiering access reset (R-118) and a repair task dropped during a
migration (R-120), makes concurrent writes to one key take turns
(R-112), verifies `Content-MD5` and the checksums of XML request bodies,
and gives body verification failures S3 client errors. It is the first
release with a supported upgrade predecessor: 0.1.0.

- Artifacts: as for 0.1.0, a Linux x86_64 (glibc) binary built with
  `cargo build --release --locked -p arca-lake --features fdb`, and the
  source; it needs the FoundationDB 7.3 client library on every host
  that runs it ([supported platforms](docs/guide/supported-platforms.md)).
- Compatibility axes: metadata schema 5 (reads 4–5; a new store starts
  at 5, an upgraded one stays at 4 until `admin upgrade finalize`); wire
  protocol 4 (accepts 3), metadata record version 2, segment format 2
  (reads 1–2) and storage record version 1, all unchanged from 0.1.0.
  New protobuf fields are additive and ignored by 0.1.0
  ([versions and compatibility](docs/guide/operations.md#versions-and-compatibility)).
- Upgrade: from 0.1.0 only (stores of schema 3 or below go through 0.1.0
  first); storage nodes, then controllers, then the groups' failure
  domains recorded, then gateways, then the one-way finalize
  ([upgrading from 0.1.0](docs/guide/operations.md#upgrading-from-010-to-020)).
- Contents: the Changed, Security, Fixed and Limitations entries below.

### Changed

- Concurrent writes to one key through a gateway take turns (R-112,
  [ADR-0041](docs/adr/0041-hot-key-commit-queue.md)): PUT, DELETE and
  CompleteMultipartUpload on a key wait for the commit ahead of them
  instead of conflicting until refused, and conflicts with other gateways
  back off with jitter. A wait beyond `--commit-queue-wait-ms` (2 s) or
  more than `--commit-queue-max-waiters` (256) waiting writes is answered
  `503 SlowDown` before any commit attempt (nothing applied).
  `--commit-queue off` restores the previous behaviour. Measured with 64
  writers on one key through four gateways: SlowDown 55 % → under 1 %,
  acknowledged PUT/s 1.6–1.7 ×, acknowledged PUT latency higher (p50
  about 200 → 450 ms, p99 about 0.8 → 1.7 s); 16 keys within 3–6 %
  ([results](docs/verification/0.2.0-hot-key-results.md); measured
  against the 0.1.0 tag, so the gain is of all 0.2.0 changes, the queue
  being its cause by mechanism; an on/off control in one binary confirms
  it: SlowDown 54 % → 0.5–2.6 %, acknowledged PUT/s 1.34–1.59 ×). Every
  DELETE form (single, version-specific, DeleteObjects entries) takes
  the same turn and backoff. No format change.
- Gateway deletes of its own partial cache copies (a population that
  could not write every member) now carry the identity token each node
  acknowledged, read with the cluster state every 2 s, so nodes holding
  an identity no longer refuse them (R-100, R-117 follow-up); gateway
  shard writes carry it too, and so do its invalidations and evictions,
  which delete copies other gateways may have populated (after their
  placement was removed). A cache delete a node still refuses (fence,
  epoch, a token not read yet) is logged and counted in the new
  `arca_gateway_cache_cleanup_refused_total`; the copy is left to
  reconciliation, as before. A repair task's recorded error now names
  the refusal (node schema, incarnation, node, epoch, capacity) after
  the status code; other messages are recorded without addresses or
  paths and at most 256 bytes.
- Scrub: each listed shard is classified from its own evidence (chunk
  record in the group, the node's retirement flag, generation state,
  migration intent; ADR-0036). Records deleted and kept for reclaim are
  reported as `retained`, no longer as unexpected shards; damaged ones
  are still findings of their damage class (`retained_damaged`), not
  queued; shards held by an operation in progress are `in_flight`;
  shards that cannot be classified (0.1.0 storage nodes, which do not
  report retirement, or records changing during the pass) are
  `unclassified` and counted as scrub errors. `admin integrity scrub`
  appends `retained … retained_damaged … in_flight … unclassified …` to
  its line (the `findings [8]` array keeps its length and order); new
  metric `arca_controller_scrub_shard_classes_total{class}`. Additive
  protobuf fields `ShardScrub.retired` and
  `ScrubShardsResponse.retirement_reported`; protocol and metadata
  versions unchanged, no metadata record changed.
- Repair (R-120): a task for a damaged shard of a migration's target
  found before the placement switch is kept while the migration intent
  targets the group and repaired after the switch (it was dropped, and
  the stripe stayed one shard short with an empty queue until a degraded
  read or the next scrub pass). Such a waiting task does not hold
  tiering or rebalancing.
- R-110 harness: the scrub parser reads both line formats; the judge's
  count rule for unexpected shards applies only to timelines of earlier
  binaries; unclassified shards make C5 Inconclusive. A run's files on
  the host (logs, guard and sentinel records, the ledger; not the node
  stores) are saved, checked by sha256 and listed in a manifest before
  any teardown may remove them; an incomplete collection is an evidence
  failure, never a pass, and a process lost to a host reboot (boot id
  changed) is recorded as such, never as stopped. The teardown removes
  nothing unless every stop is confirmed and the collection is complete.
- Metadata schema 5
  ([ADR-0039](docs/adr/0039-schema-5-and-the-0.1.0-upgrade.md)). 0.2.0
  reads schemas 4 and 5 and initialises new stores at 5. A cluster
  upgraded from 0.1.0 stays at schema 4, with the 0.2.0 protections
  degraded, until `admin upgrade finalize` raises it to 5. After that,
  0.1.0 controllers and gateways refuse to start. The leader quarantines
  0.1.0 storage nodes before pushing them anything, and gateways and the
  controller never count a shard write on them (the node must report
  schema 5 in `Health` and in the write's acknowledgement). Restoring a
  pre-finalize backup does not bring 0.1.0 back. The finalize is refused while any
  0.1.0 (or unknown) process or node takes part.
  `admin status --versions` shows each process's and node's software
  and schema, and whether protections are degraded or mandatory.
  Compatibility impact: no record layout, record version byte, wire
  protocol version or segment format change. Two additive fields,
  `HealthResponse.schema_max` (50) and `PutShardResponse.schema_max`
  (5), are ignored by 0.1.0. Stores of
  schema 3 or below must be upgraded through 0.1.0 first
  ([upgrading from 0.1.0](docs/guide/operations.md#upgrading-from-010-to-020)).

### Security

- Shard write ownership (R-117, ADR-0037): a storage node that has
  adopted no incarnation yet (a new disk its controller has not reached)
  refuses every shard write, so another cluster's gateway at a reused
  address can no longer have its shards stored there and counted
  written. Every shard write now names the node it is for (the
  placement member; `PutShardHeader` fields 7–8, the node id and, from
  the controller, the identity token), and a node holding another
  identity refuses it: a write reaching another node of the same cluster
  is never stored or counted written. Once the cluster is finalized to
  metadata schema 5, nodes also refuse shard writes carrying no
  incarnation or naming no node (a cluster 0.2.0 initialises, at schema 5
  (ADR-0039), is strict from its first write; one upgraded from 0.1.0
  accepts such writes and counts them until the finalize). Nodes turn strict when `upgrade finalize`
  pushes schema 5, before the schema is raised: stop or upgrade tools
  that write shards without an incarnation (0.1.0 `nodebench`) first,
  or their writes fail. Accepted unproven writes are counted and
  reported: new metrics `arca_node_writes_unproven_total`,
  `arca_node_write_ownership_strict` and
  `arca_controller_nodes_write_ownership_lenient`, `HealthResponse`
  fields 47–49 (a capability; no protocol version change), and
  `admin node show` reports the mode. A new incarnation (a recovery or a
  restore) is acknowledged by a node only once no write of the previous
  one can still be stored. `arcabench nodebench` writes with the
  target node's incarnation.

### Fixed

- A write torn by a crash or a failed write (a full device, an I/O error)
  could make the storage node report a segment as corrupt
  (`corrupt_segments`, "record truncated", at every open from then on)
  instead of discarding the torn bytes; affects 0.1.0. It happened when
  a compaction had published its output under a number above the segment
  being written, or when the store went on after the failed write in a new
  segment. No acknowledged record was lost and the node started; the
  segment was no longer appended to. The segment being written now
  always has the highest number, and a failed write is cut off before its
  segment is sealed. A segment 0.1.0 already reported this way stays
  reported until it is compacted (recovery cannot tell it from a damaged
  segment); its records before the torn bytes are intact. No format
  change (RFC-005 §5.3, §5.5).
- `x-amz-checksum-*` (CRC32, CRC32C, CRC64NVME, SHA1, SHA256) of the XML
  bodies of DeleteObjects, PutBucketPolicy, PutBucketVersioning and
  CreateBucket is verified. Before, it was ignored: a corrupted body was
  applied and answered `200` (current SDKs send CRC32 on DeleteObjects
  instead of `Content-MD5`). A mismatch is now `400 BadDigest`; a
  malformed value, several or repeated checksum headers, an algorithm
  header without its value or naming another algorithm, or an
  `aws-chunked` body on these operations `400 InvalidRequest`; the
  operation is not applied (RFC-007 §5.7).
- A PUT or UploadPart that fails while its body is streamed (a refused
  body, a failed chunk) waits for its chunk placements in flight before
  it is answered, instead of cancelling them: a shard already sent could
  be stored after the error reply, after the write's abort and GC. Such
  shards are still reclaimed by node reconciliation; none appears after
  the reply now. The wait is bounded (5 s, or a third of the lease if
  smaller); past it the placements are aborted as before and counted in
  `arca_gateway_refused_write_settle_timeouts_total`. A request dropped
  by its client now also aborts the placement it was waiting for, which
  kept running before.
- `Content-MD5` is verified. Before, a body that did not match it was
  stored and answered `200` (a false success for a client that asked for
  the check). PutObject and UploadPart (plain and `aws-chunked`, any
  size) and the XML bodies of DeleteObjects, CompleteMultipartUpload,
  PutBucketPolicy, PutBucketVersioning and CreateBucket now answer `400
  BadDigest` on a mismatch and `400 InvalidDigest` for a malformed value;
  nothing is committed (RFC-007 §5.7).
- A signed PUT or UploadPart whose body does not match its
  `x-amz-content-sha256` is answered `400 XAmzContentSHA256Mismatch`, as
  AWS, instead of `500 InternalError`, which SDKs retried as a server
  fault; other body verification failures are client errors too
  (`IncompleteBody`, `SignatureDoesNotMatch` for an `aws-chunked` chunk
  signature, `EntityTooLarge`). Nothing was or is stored. Re-creating a
  bucket one owns is `409 BucketAlreadyOwnedByYou` instead of `409
  BucketAlreadyExists` (still answered for another principal's bucket);
  us-east-1's legacy `200 OK` is not emulated (RFC-007 §5.1.1).
- A group's failure domain is recorded and kept by every member move
  (R-119, [ADR-0038](docs/adr/0038-group-failure-domain-record.md)):
  new groups record their label at creation (new metadata key
  `("al","gdomain",group)`; no version change); auto repair,
  `replace-member`, drains, retargets, the dry-run planner, resumed
  moves and the membership commit use the recorded label, not the
  controller's `--failure-domain`. A group created by 0.1.0 moves no
  member until `admin group set-failure-domain` records its label
  (never inferred). `admin group check` and `admin status` report groups
  with two members in one domain, groups without a record and members
  whose node lacks the label (gauges
  `arca_controller_groups_failure_domain_violations`,
  `arca_controller_groups_failure_domain_unrecorded`); a member without
  the label no longer stops cache-group repair for the whole pass, and
  the reason is shown on the replacement request or drain;
  `register-node` refuses a node without a label its groups need.
  `admin upgrade finalize` to schema 5 waits until every group's domain
  is recorded. **Upgrading from 0.1.0:** once the controllers run 0.2.0,
  run `admin group set-failure-domain --all-unrecorded --label <label>`
  (first with `--dry-run`, which lists the groups) with the label the
  groups were created with; until then no member of those groups is
  moved, including the repair of a failed one (repair reason
  `awaiting-failure-domain`; `admin status` warning and gauge
  `arca_controller_groups_failure_domain_unrecorded_at_risk` for such
  groups with a member down). `set-failure-domain` refuses a label no
  member's node carries (`--force`) and exits 2 when it prints warnings.
  See [operations](docs/guide/operations.md#upgrading-from-010-record-the-groups-failure-domains).

- Automatic tiering no longer consumes a generation's access record
  after a move that did not happen (R-118,
  [ADR-0040](docs/adr/0040-tiering-access-reset-completion.md)): the
  record is reset only once every chunk of the generation is in the
  target tier, a generation moved in part is finished by a later pass,
  and reads recorded while the pass ran are kept. No format change.
- Tests: races and environment assumptions fixed in the restore, node
  lifecycle, delete-fence, capacity (allotments that fit on tmpfs),
  S3 error, hot-key and R-110 baseline-probe tests; no assertion was
  removed. The CI upgrade job runs against the published 0.1.0 archive
  (its sha256 and `BUILD-INFO.txt` commit pinned).
- Documentation: corrections to the 0.1.0 record (its readiness tracker,
  the roadmap and an ADR-0033 note); the 0.1.0 Release text was
  corrected to match it.

### Limitations

This remains experimental. Not verified for 0.2.0 at the time of
writing: the final three-host validation, an R-110 run, and the hot-key
gain attributed to the commit queue alone (the on/off control is
pending). See the [0.2.0 readiness](docs/release/0.2.0-readiness.md),
[known limitations](docs/guide/known-limitations.md) and the
[risk register](docs/architecture/risks.md).

## [0.1.0] - 2026-11-23

First experimental release (tag `v0.1.0`; [0.1.0 release](https://github.com/akam1o/arca-lake/releases/tag/v0.1.0)), for
evaluation and development; **not production-ready**.

- Artifacts: a Linux x86_64 (glibc) binary built with
  `cargo build --release --locked -p arca-lake --features fdb`, and the
  source. It needs the FoundationDB 7.3 client library (`libfdb_c`) on
  every host that runs it. No container image, no crates.io package, no
  macOS, aarch64 or Windows binary
  ([supported platforms](docs/guide/supported-platforms.md)).
- Compatibility axes (separate from the software version): wire
  protocol 4 (accepts 3), metadata schema 4 (reads 3), metadata record
  version 2, segment format 2 (reads 1–2), storage record version 1. A new
  store initialises at schema 4 and segment format 2. 0.1.0 has no
  supported upgrade predecessor: the earlier "releases" named below and
  in CI are pre-release development commits used by the upgrade tests
  ([versions and compatibility](docs/guide/operations.md#versions-and-compatibility)).
- Contents: the Added, Changed, Fixed and Limitations entries below.

### Added

- Distributed object storage with stateless S3 gateways, FoundationDB
  metadata, append-only storage nodes, per-profile erasure coding or
  replication, and failure-domain-aware placement.
- Core bucket and object operations, multipart uploads, versioning,
  conditional requests, range reads, copy operations, and checksums.
  Supported operations and differences from Amazon S3 are tracked in the
  [compatibility guide](docs/guide/s3-compatibility.md).
- Durable storage tiers, automatic promotion and demotion, and independent
  read-through cache tiers with replication or erasure coding, prefetch
  and pinning.
- Controller election, integrity scrubbing, repair, garbage collection,
  capacity admission and backpressure, and fenced recovery with deferred
  reclamation. See [operational requirements](docs/guide/operations.md)
  and [backup and restore](docs/guide/backup-restore.md) for their conditions.
- SigV4 authentication, a credential store and bucket-policy subset,
  service mTLS and role authorization, and FoundationDB TLS guidance.
  See the [security model](docs/guide/security-model.md) for the boundary.
- Operator commands, metrics, alerts, fault-injection and compatibility
  tests, and reproducible benchmark tooling.

### Changed

- Access records use a bounded per-gateway table and one batched flusher.
  The transaction checks generation liveness, unknown commit outcomes are
  not retried, and observable losses are counted. The default mode is
  `--access-records on` (the batched path) with a batch of 128 generations;
  `legacy` retains the earlier path. In the reported VM A/B,
  the FoundationDB commits-per-GET target was met, while GET throughput
  remained 7–14 % below `off` and missed its target. See the
  [ADR-0034 verification report](docs/verification/phase-4.1-adr34.md).
  Recording only where tiering acts (ADR-0034 decision 6) is implemented
  as `admin tiering enable | disable` (ADR-0035, below).
- Metadata schema 4 (ADR-0035): no record layout changes; it gates
  disabling tiering. This build reads schemas 3 and 4, so a store of
  schema 2 is upgraded through a release of schema 3 first, and a
  metadata backup taken at schema 2 is restored with a release of schema
  3 (and finalized there) before this one. After `admin upgrade
  finalize` writes schema 4, earlier releases refuse to start and the
  cluster cannot be downgraded below it.
- A tiering pass that finds another migration of a chunk in flight (for
  example a deposed leader's move) no longer resets the generation's
  access record: it moves nothing, keeps the reads and logs the skip.
  Found by the ADR-0035 integration verification
  ([report](docs/verification/phase-4.1-adr35.md) §7).
- `admin tiering objects --bucket B`: each object's tier and access
  record (read-only). Gateways log each change of their tiering
  observation and of their recording decision (every refresh at debug);
  controllers log who holds the lease when they stand by. The test-only
  `fault-injection` feature also holds a gateway's tiering refresh or a
  controller's tiering move (`ARCA_TEST_FAULT_FILE`; never in a release
  build).
- `admin tiering status | enable | disable | set`: the tiering record,
  the cluster's source of truth for whether tiering acts and its policy
  (ADR-0035). Controllers run tiering from it (their `--tiering-*` flags
  only seed an absent record) once the leader has acknowledged it; moves
  a tiering pass started switch only under the policy that decided them;
  demotion waits for a grace after tiering is enabled
  (`--tiering-clock-skew-secs`). Disabling is refused below schema 4.
  Gateways read the tiering and leader records in their two-second
  refresh and stop recording reads only while tiering is confirmed
  disabled by the current leader (unknown, unacknowledged or stale:
  recorded); `--tiering-lease-skew-ms`, `arca_gateway_access_recording`,
  `arca_gateway_access_unrecorded_total`. The flush applies the record's
  promotion cutoff (`arca_gateway_access_dropped_total{reason="cutoff"}`),
  so read counts from before tiering was last enabled never promote
  (`--access-records legacy` does not apply it). Measured with tiering
  disabled, GET throughput is within the spread of `--access-records off`
  ([ADR-0035 report](docs/verification/phase-4.1-adr35.md) §6). On an upgraded
  cluster with `--tiering-*` flags the first pass seeds the record and
  the next acknowledges it: tiering pauses one interval, then nothing is
  demoted for `demote_after` plus the grace margin and earlier read
  counts do not promote until new reads arrive.
- The gateways' `--access-max-age-ms` is at most 30 s (the flush delay
  the tiering grace allows for); 0 or larger values are lowered, with a
  warning.
- Reconciliation and reclaim judge a listing page in one read-only
  metadata transaction and use bounded parallelism. Rebalancing's settle
  time accounts for the measured reconcile cycle. The capacity-pressure
  criterion remains incomplete; see the
  [ADR-0033 verification report](docs/verification/phase-4.1-adr33.md).
- Benchmark retries use bounded exponential backoff and jitter; rejected
  operations pause before the next attempt. Comparisons must account for
  this change in offered load.
- The dependency advisory exception for indirect `chrono` usage is scoped
  to `RUSTSEC-2026-0543`; its rationale and removal conditions are tracked
  as R-115 in the [risk register](docs/architecture/risks.md).

### Fixed

- A repair task for a chunk whose committed object lives in another
  group (a stray copy left by a write diverted while a member was down)
  is dropped instead of being retried every minute forever with the
  repair queue never empty (R-120; found in the 0.1.0 three-host
  validation).
- Dependency `url` 2.5.9 (with `form_urlencoded` 1.2.3), used through
  the S3 request layer (`s3s`): `url` 2.5.8 and earlier overflow the
  stack in `Url::origin()` on nested `blob:` URLs (RUSTSEC-2026-0592).
- A replacement member (`replace-member`, automatic repair, drain, the
  planner) is never placed in a failure domain the group already uses
  (R-119; found in the 0.1.0 three-host validation, where a 2+1 group on
  three hosts had two members put on one host). Groups changed by an
  earlier replacement are not reported: compare each group's members with
  the nodes' labels in `admin status`. Replacements use the controller's
  `--failure-domain`: keep it the label the groups were created with.
- A storage node that has adopted a cluster incarnation refuses shard
  writes carrying another one (R-117): a gateway or controller of another
  cluster reaching it at a reused address is refused instead of having
  its write stored and counted. A write carrying no incarnation is still
  accepted (unlike a delete), as is any write to a node that has adopted
  none. A node holding another incarnation at
  the cluster's sequence (R-97) now refuses the cluster's shard writes
  too, until the next `recovery begin`.
- PUT, UploadPart, CompleteMultipartUpload and DELETE no longer report a
  later attempt's error as a definite refusal when an earlier commit may
  still apply. The outcome is fenced or reported as unknown; resolved
  versioned PUTs return their own version id, and expired unsealed parts
  are not reported as stored. See the
  [commit-outcome verification](docs/verification/phase-4.1-r112.md).
- Restore tests now restart gateways after restoration and verify that
  old gateways are fenced. The intermittent failure was a test race;
  product fencing behavior was unchanged. See the
  [restore-test investigation](docs/verification/phase-4.1-r113.md).

### Limitations

This remains experimental. Long-duration operation under the current
validation criteria and physical-device power-loss behavior are not
verified. Implementation, measured results and unverified conditions are
separate; see [known limitations](docs/guide/known-limitations.md), the
[risk register](docs/architecture/risks.md) and the
[current roadmap](docs/roadmap/README.md).
