# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/) once it has releases.

## [Unreleased]

### Added (Phase 3.9)

- Delete fence enforced by storage nodes (ADR-0023, R-88): every
  `DeleteShard` carries a fence generation; nodes persist the generation,
  whether it is open, and an operator hold, and apply a delete only under
  their open generation, checked under the lock their physical delete
  holds. `admin delete-fence status|close|release`; `--absent-node` for
  `close` and `recovery begin`; metrics `arca_controller_delete_fence_*`
  and `arca_node_delete_fence_*`.
- Tests: delete fence rules on the node (including a delete cancelled
  while the fence closes), cluster scenarios of deletes delivered after
  a restore, a phase change and a leader change, and a system model
  with backups, restores, phase changes, leader terms, node restarts and
  late, reordered deletes.

### Changed (Phase 3.9)

- Protocol version 3. A version-3 node refuses deletes below version 3;
  version-3 clients send no delete to an older node. Deletes pause on a
  node during a rolling upgrade until both sides speak version 3.
- `admin recovery begin` closes the delete fence on every registered
  node and refuses unless each acknowledges; `admin cluster
  set-read-only` closes it; `admin recovery finalize` releases the hold.
  Both commands now take the node RPC and TLS options.
- The controller leader opens a fence generation for its term while the
  cluster is `Active` and closes it otherwise.
- GC defers, and reconciliation and cache reconciliation keep, shards
  whose delete the fence refused.

### Fixed (Phase 3.9)

- A node's fence (incarnation, epoch, delete fence) no longer releases
  its shared guard when a delete's RPC is cancelled while the store is
  still deleting: the guard lives in the blocking store call.

### Added (Phase 3.8)

- Delete reserve under both capacity floors (one tombstone per record
  held); node capacity breakdown `arca_node_written_bytes_total`,
  `arca_node_device_usage_bytes`.
- Capacity rebalancing between the groups of a tier (`--rebalance`,
  `admin rebalance status|pause|resume|dry-run`,
  `arca_controller_rebalance_*`).
- Reads follow chunks moved since their manifest was read
  (`arca_gateway_moved_reads_total`).
- Tests: system model over the whole data path, backpressure simulator,
  rebalancing, the Phase 3.7 reserve finding, fencing of admission
  records.

### Changed (Phase 3.8)

- Gateways take admission records only of their incarnation and never
  of a lower term or generation (no time rule); without any record they
  fail closed after 60 s; unnamed tiers are throttled.
- A worse backpressure state on time to full needs the recent trend to
  agree once it is covered.
- Delete-class reference checks are refused outside `Active`; migration
  switches check the target group's epoch; deletes of absent keys write
  no tombstone; a write whose generation was retired under it answers
  503 instead of 500.

### Added (Phase 3.7)

- Every client write path (PUT with or without a length, UploadPart,
  CopyObject, UploadPartCopy) is charged to write admission chunk by
  chunk as it writes, with a bounded wait (ADR-0021, R-81);
  `arca_gateway_admission_charged_bytes_total`.
- Admission leadership terms: records carry the fencing token, a
  generation and a warm-up flag; a new leader keeps the published state
  until every member has reported (R-85).
- Storage nodes report their capacity domain; nodes sharing a file
  system without fitting `--capacity-bytes` allotments leave placement
  unless `--allow-shared-filesystem` (R-80);
  `arca_controller_capacity_domain_conflicts`, `_unverified`.
- `admin check-config` (capacity configuration, admission record, group
  rooms and imbalance).
- Capacity-aware group choice from published group rooms (R-82).
- Tests: multipart under backpressure (in-memory and FoundationDB),
  refused writes through the AWS SDK for Rust, AWS CLI v2 and boto3,
  failover terms, shared file systems; `capacity_bench`.
- Documentation: README, `docs/guide/` (quick start, platforms, S3
  compatibility, configuration, operations, security model,
  backup/restore, troubleshooting, known limitations), `docs/release/`
  (public repository audit, public CI review, release checklist).

### Changed (Phase 3.7)

- Backpressure leaves a state on the recent trend (1 min) and adapts the
  throttled rate (R-83); the trend windows and the adaptive rate are
  policy settings.
- Test clusters give every node a 512 MiB allotment; `soak24.sh` runs its
  controllers with `--allow-shared-filesystem`.

### Added (Phase 3.6)

- Capacity admission on storage nodes (ADR-0020): every shard write is
  admitted against the device's free space minus the writes in flight;
  client, migration and cache writes stop at a reserve (`--reserve-bytes`;
  default two segments and 64 MiB, or 2 % of the device), repair at the
  repair floor, deletes always pass. `PutShardHeader.write_class`;
  `RESOURCE_EXHAUSTED` for capacity refusals and ENOSPC/EDQUOT;
  `Health.devices` (per-device capacity: total, available, allocated,
  live, reclaimable, quarantined, reserved, in flight, repair floor).
- Write backpressure per tier: the controller leader evaluates `normal`,
  `warning`, `throttled`, `write_rejected`, `recovery_only` with
  hysteresis (`--admission-dwell-secs`, `--admission-max-chunk-bytes`,
  `--admission-group-min-shards`) and publishes an admission record;
  gateways throttle new data to the GC reclaim rate, refuse it when
  rejected (`503 SlowDown` with `Retry-After`), skip groups without room,
  and keep reads, deletes and completions open.
- GC stage timing and volume (`arca_controller_gc_stage_seconds`,
  `_gc_transactions_total`, `_gc_reclaimed_bytes_total`,
  `_gc_roots_collected_total`, `_gc_parts_retired_total`), the backlog in
  bytes, capacity and admission gauges on every process, six alerts and
  runbook RFC-013 §10.11.
- `gc_bench` (GC by workload, in-memory and FoundationDB metadata),
  `benchmarks/linux/capacity-tests.sh` (GC saturation, multipart
  saturation, capacity exhaustion, failure injection), `gcload --verify`
  and `--mode drain`.

### Changed (Phase 3.6)

- A capacity refusal is `503 SlowDown` (was `503 ServiceUnavailable` with
  an internal node error); a refused PUT or UploadPart body up to 16 MiB
  is read before the answer, so that clients see the `503`.
- Compaction skips a segment whose live bytes do not fit in the free
  space.

### Fixed (Phase 3.6)

- A completed multipart object's parts served a second grace period
  after their root was collected, holding every multipart object's data
  for two grace periods (R-64); they are now retired as of the root and
  collected in the same pass.
- Cache eviction, invalidation and a failed population's cleanup could
  delete a newer cache copy under the same key (R-73); cache copies are
  now deleted by the tokens their writes returned (copies without tokens
  are left to reconciliation).

### Added

- Phase 3.4 (security hardening and production readiness; ADR-0017,
  ADR-0018): mutual TLS for every node RPC with SPIFFE identities
  (`spiffe://<trust-domain>/<role>/<name>`), per-RPC role authorization
  on the storage node, certificate/trust bundle/CRL hot reload that
  closes connections whose trust was withdrawn, `--insecure-plaintext`
  required for plaintext; FoundationDB client TLS (`--fdb-tls-*`) with a
  fail-closed cluster-file check and `--fdb-insecure-plaintext`;
  `arca-lake tls init-ca|issue|crl|show` (development PKI); TLS metrics.
- Hardened `admin recovery finalize` (one transaction; refused while
  verification is unfinished or incomplete even with `--force`; critical
  findings need `--force --reason --operator`; persistent data-loss
  records and an audit of every attempt; `admin recovery losses`).
- `admin replace-member|decommission-node --dry-run`; capacity-aware
  member moves (nodes report device free/total bytes, `--capacity-bytes`;
  a move without room pauses and resumes by itself); the leaving member
  as a validated source and an opt-in direct copy
  (`--repair-source direct`).
- Background segment compaction on storage nodes
  (`--compaction-interval-secs`, `--compaction-min-dead-fraction`,
  `--compaction-max-segments`, `--compaction-max-copy-mib`).
- Adaptive scrub budget (`--scrub-adaptive`, ADR-0018): nodes report
  window load in `Health`; AIMD with a floor, a ceiling, hysteresis,
  repair priority, and the fixed rate without signals.
- Continuous metadata backup verification: `admin backup
  verify-restored|key-range`, `deploy/backup/verify-backup.sh`, text-file
  metrics.
- `admin integrity verify-object --decode` (end-to-end decode and
  checksum/ETag/layout verification) and `--quarantine`.
- Metric registry (`crates/common/src/metrics-registry.tsv`) checked in
  CI against every process; HELP lines from it; Prometheus alert rules
  (`deploy/prometheus/arca-lake-alerts.yml`); S3 request, auth refusal,
  device space, groups-repairing, compaction and adaptive-scrub series.
- Golden files for every metadata record and the quarantine log format.
- Soak tooling: `soakload` (mixed S3 workload with an acknowledged-write
  ledger), `faultproxy` (userspace network faults), test-only storage EIO
  control (feature `fault-injection`), `benchmarks/linux/soak24.sh`.

### Changed (Phase 3.4)

- Node RPCs and metadata connections refuse to start without TLS
  configuration or the explicit plaintext flags.
- `admin integrity verify-object` is read-only by default.
- `admin cluster activate` refuses while a restore report is unaccepted;
  `admin recovery verify|resume` are conditional transactions.
- `arca_controller_nodes_total` is renamed `arca_controller_nodes_registered`
  (a gauge); histogram maxima are separate `…_max_seconds` gauge families.
- CI runs only on pushes to `main` (no pull-request trigger) with actions
  pinned to commit SHAs.

### Fixed (Phase 3.4)

- A segment creation that failed after the file existed made every later
  write of the node fail with `EEXIST`.
- A failed completeness check left a group `Repairing` without a cursor,
  waiting for an operator forever; shards missing on other members are
  now handed to stripe repair (a stale unrecoverable verdict is
  re-checked), and a member move can read the leaving member's shard.
- Deleted space was never reclaimed by a running node; GC fell behind a
  sustained write rate (now batches until drained, concurrently).
- Audit records of finalize attempts in the same millisecond overwrote
  each other (found before release).
- The recovery pass now pushes the new incarnation to every node at its
  start (a delayed pre-restore delete was accepted until `Active`).
- A chunk stamped in the restore point's millisecond is a post-T orphan.
- `arca_node_phase_seconds{phase="get_serve"}` was never observed (the
  series stayed empty, and adaptive scrubbing had no GET signal).
- Compaction rewrote segments whose tombstones were all still needed into
  identical new segments every pass; that churn used up every pass and the
  mostly dead large segments were never compacted (a soak node grew 4 GB
  an hour). Such a segment is now left alone (`no_progress`, `blocked` in
  `segment_stats`) until older records are gone.
- Compaction kept a tombstone while any other segment had a lower
  sequence, which a single long-lived record satisfies forever: every
  deleted or garbage-collected shard left a 4 KiB tombstone record that
  was never reclaimed (found by the Phase 3.4 convergence test). A
  tombstone is now kept exactly while a non-live record of its key is
  still on disk in another segment (tracked in the index, rebuilt on open,
  released only after the segment's unlink is durable); a failed unlink
  no longer leaves an untracked segment file behind. New node gauges for
  compaction backlog, blocked segments, dead records and the active
  segment.
- Write placement put the first chunk of every object (every chunk of a
  single-chunk object) into one group per gateway process: the group was
  chosen by the chunk index from candidates in a per-process hash-map
  order, and a gateway restart moved all new writes to another group
  (present since Phase 1; found by the Phase 3.4 soak as one node draining
  to 0.2 GB while another doubled). Chunks are now spread by their id over
  the eligible groups in group-id order.
- Automatic tiering moved every generation of a controller process into
  the first group of the target tier in hash-map order (a leader change
  switched all later moves to another group). Targets are now spread by
  generation id over the tier's Active groups of the source's scheme.
- `arca_gateway_s3_requests_total` and `_s3_request_seconds` counted every
  request as `op="unknown"` on gateways without an authorizer (the
  operation was named only by the owner-policy authorizer); the allow-all
  access hook now names it and keeps refusing unsigned requests.
- `admin integrity scrub --wait --adaptive` ignored the adaptive policy
  and ran at the fixed rate (found by the Phase 3.4 measurement).

- Phase 3.3 (operational safety, rolling upgrade, disaster recovery;
  ADR-0016): independent wire-protocol / metadata-schema / storage-format
  versions with startup gates, protocol window negotiation, `schema_write`
  and `format_write` gates, process heartbeats and node capability
  records, leader-driven `admin upgrade status|finalize|cancel|
  check-downgrade` with an audit record, `admin init`, `admin status
  --versions`, `storage-node check-downgrade`, and a `version-bump` build
  for two-binary mixed-version tests.
- Cluster recovery state (`Active`, `ReadOnly`, `Restoring`,
  `VerificationPending`, `RecoveryRequired`) with an incarnation checked at
  commit of every writing transaction and on node `DeleteShard`; epoch
  catch-up; resumable restore verification with eleven finding classes
  and a per-object report; `admin recovery status|begin|verify|resume|
  finalize` and `admin cluster set-read-only|activate`; metadata restore
  drill with `fdbbackup`/`fdbrestore`.
- Storage I/O fault injection (`storage::faults`); a failed append or
  fsync is never acknowledged and seals the segment.
- Parallel stripe repair with a byte budget, per-node transfer slots that
  yield to client load, and priorities (`--stripe-repair-workers`,
  `--stripe-repair-rate-mib`, `--stripe-repair-per-node`).
- `admin integrity status --objects`, `admin integrity verify-object`;
  manifest scrub cursor; `arca_controller_integrity_findings_open`.
- `admin replace-member` and `admin decommission-node` carried out by the
  controller leader with the existing member-replacement state machine.

- Architecture review documents, design decisions, technology selection,
  risk register, and open decisions.
- RFC and ADR templates and index; ADR-0001 through ADR-0005.
- Phase 0 proof-of-concept plan.
- Rust workspace skeleton with crate stubs, CI, license and advisory policy.
- `arca-lake-ec`: `ErasureCoder` trait, `SchemeId`, `CodeParams`,
  `StripeLayout`, replication / classic GF(2^8) / Leopard GF(2^16)
  backends, property tests, golden parity vectors, and criterion
  benchmarks (Phase 0 task 0.2).
- `arca-lake-storage`: append-only segment format v1 (`FORMAT.md`), store
  with durable put/delete, range reads with per-block CRC verification,
  recovery scan distinguishing torn tails from corruption, scrub, and
  crash/corruption tests (Phase 0 task 0.3, first iteration).
- Benchmark policy, report template, and the first EC backend report.
- `arca-lake-metadata`: transactional key-value abstraction with
  FoundationDB-like semantics and limits, an in-memory backend with
  optimistic conflict detection, key layout v1, record types, and the
  write-protocol recipes (begin/append/commit/abort, head, manifest read,
  delete, listing with delimiter skips and continuation tokens, bucket
  operations, lease expiry and GC queue), with concurrency tests (Phase 0
  task 0.5, in-memory part).
- `arca-lake-metadata` `fdb` feature: FoundationDB backend, protocol tests
  against a real cluster, containerized runner (`deploy/dev/fdb-test.sh`)
  and a CI job.
- `arca-lake-gateway`: S3 protocol layer spike on `s3s` with an in-memory
  backend (buckets, PutObject with conditional headers, GetObject with
  ranges, HeadObject, DeleteObject, ListObjectsV2), server wiring on
  hyper, and an AWS CLI smoke test; `arca-lake gateway` subcommand
  (Phase 0 task 0.6, functional part).
- `arca-lake-storage`: group commit (`SyncPolicy::Group`, now the default),
  compaction with a fault-injection hook, order-independent index rebuild
  (highest sequence wins across data records and tombstones), per-segment
  live accounting and compaction candidates, SIGKILL crash test with a
  deterministic writer process, interrupted-compaction tests at every step,
  concurrent-read-during-compaction test, golden v1 segment for format
  compatibility, concurrent-writer benchmark (Phase 0 task 0.3).
- `arca-lake-protocol`: storage node API v1 (`PutShard` client streaming,
  `GetShard` server streaming) compiled with a vendored `protoc`.
- `benchmarks/transfer`: shard transfer measurement tool comparing tonic
  gRPC and raw framed TCP, with and without TLS, with backpressure, CPU, and
  RSS accounting, plus a driver script and summarizer (Phase 0 task 0.4).
- RFC-001, RFC-002, RFC-003, RFC-005 drafts; storage group-commit report.
- RFC-004 Erasure Coding and Placement draft (Phase 1).
- `arca-lake-protocol`: `DeleteShard`, `ListShards`, `UpdateGroupEpoch`,
  `Health` RPCs with documented epoch-rejection semantics; `NodeClients`
  convenience client.
- `arca-lake-metadata`: node and stripe-group records, cluster-map version,
  generation-prefix ranges for orphan attribution; `transact` takes an
  explicit environment and boxed `Send` futures.
- `arca-lake-common`: `EcProfile` with validation and fault-tolerance
  statement; chunk ID = generation prefix + index.
- `arca-lake-placement`: failure-domain-aware group member and replacement
  choice, write-group selection.
- `arca-lake-storage-node`: gRPC service over the storage engine with
  persisted per-group epochs and fencing.
- `arca-lake-gateway`: `ClusterBackend` implementing the distributed write
  protocol (generation → erasure coding → parallel durable shard writes →
  batched manifest → atomic commit) and the read protocol (manifest →
  shards → decode → verify → stream) with small-object replication and
  fault-injection points.
- `arca-lake-controller`: stripe-group bootstrap, garbage collection
  (lease expiry, retired generations, node-driven orphan reconciliation),
  and member replacement with reconstruction.
- `arca-lake-testkit`: in-process cluster harness and 18 end-to-end tests
  covering concurrent and conditional writes, degraded reads, corruption,
  repair, fencing, crashes around the commit, and orphan GC.
- `arca-lake-storage`: `index_rebuild` example measuring index rebuild on
  open; `format::crc32c` helper.
- ADR-0006 (Phase 1 integration decisions); Phase 1 roadmap page.
- Phase 1 reliability hardening:
  - `arca-lake-testkit`: `TestCluster` generic over the metadata backend;
    the E2E suite and a commit-fault suite run on FoundationDB
    (`e2e_fdb.rs`, `faults_fdb.rs`) in CI and via `deploy/dev/fdb-test.sh`.
  - `arca-lake-metadata`: `KvError::CommitUnknown`, `transact_idempotent`,
    `resolve_commit`, `fault::FaultKv` (commit fault injection for any
    backend), model-based test of the generation state machine.
  - `arca-lake-gateway`: commit resolution and fencing on unknown
    outcomes; writes to `Repairing` groups; `FaultPoint::DieBeforeCommit`.
  - `arca-lake-controller`: GC verifies generation state and references
    before deleting; reconciliation decides by metadata reference
    (pending generation, chunk placement, membership) with age as a guard
    only; repair with per-member cursor, resume, completeness check,
    `EpochChanged` detection, and step hooks. `GroupRecord.repair_cursor`
    layout changed (pre-release).
  - `arca-lake-protocol`: `NodeClients` fault hook (delay, fail before /
    after, duplicate, reset).
  - Tests: `gc_safety.rs`, `repair.rs`, `transport.rs`,
    `metadata/tests/commit_unknown.rs`, `metadata/tests/model.rs`.
  - RFC-008 draft; RFC-003 §5.8; RFC-004 §5.7 rewritten; ADR-0007.
- Phase 1.2 distributed operations and failover:
  - `arca-lake-protocol` / `arca-lake-storage-node`: paged `ListShards`
    with payload lengths and continuation cursor; `VerifyShards`.
  - `arca-lake-metadata`: shard attribution recipes (`attribute_shard`,
    `generation_is_live`, `chunk_record_in_group`), leader election
    (`ops::leader`), `fence::FencedKv`, `KvError::Fenced`,
    `cluster::touch_node`, FoundationDB transaction timeout; model test
    extended with shard attribution (I6).
  - `arca-lake-controller`: bounded-memory group walker; repair with
    acknowledged fence, per-member cursor from the membership commit,
    chunk validation against recorded scheme/placement/CRC, verified
    completion (existence, lengths, `VerifyShards`); reconciliation by
    pages and metadata reference with deletion epochs; controller
    process (election, lease, fencing, GC/reconcile/health/repair
    schedulers, auto repair, backoff, graceful shutdown, metrics and
    health endpoint, tracing).
  - `arca-lake` CLI: `storage-node`, `controller`, `admin` subcommands and
    `gateway --backend cluster` (feature `fdb`).
  - Tests: `paging.rs`, `repair_completion.rs`, `controller.rs`,
    `fdb_failover.rs` (three-process FoundationDB: process, coordinator,
    and quorum loss; leader failover; repair and GC across process loss),
    `deploy/dev/fdb-failover.sh`, CI job `fdb-failover`.
  - RFC-011 Controller draft; RFC-003 §5.9–5.10; RFC-004 §5.13; RFC-008
    §6.4–6.7, §7; ADR-0008.
- Phase 2 native multi-tier storage (first iteration):
  - `arca-lake-metadata`: `CachePlacement` records in the generation
    subspace, per-tier recency index and usage counter, access records,
    `ops::cache` recipes, `read_manifest` returning cache placements,
    `switch_placement`, `list_generations`, `read_manifest_of`,
    `cache_placement_in_group`; model test `cache_model.rs` (C1–C4).
  - `arca-lake-tiering`: cache groups (`cache-v1`), population with a
    committed-generation and capacity check, validated cached reads,
    invalidation, LRU eviction with watermarks and pins, cache
    reconciliation; durable chunk read/encode; `migrate_chunk`
    (copy, verify, conditional switch, retire) with fault points.
  - `arca-lake-gateway`: read-through cache on the read path with
    per-chunk fallback, asynchronous deduplicated population, admission
    control, coalesced recency and access records; `prefetch`, `pin`,
    `unpin`, `evict`, `cache_stats`.
  - `arca-lake-controller`: cache groups per device class, durable groups
    per device class, GC of cache copies, cache-group reconciliation,
    eviction scheduler, automatic tiering policy and scheduler, metrics.
  - `arca-lake` CLI: `--cache-*` options for gateway and controller,
    `--tiering-tier` policy options, `admin create-cache-groups`,
    `create-groups --tier --device-class`.
  - Tests: `cache.rs` and `cache_fdb.rs` (12 cases each, FoundationDB in
    CI and `deploy/dev/fdb-test.sh`), `tiering.rs` (5).
  - RFC-006 Multi-Tier Storage draft with Phase 2 status; ADR-0009;
    Phase 2 roadmap page; risks R-41–R-45.
- Phase 3.2 data integrity and repair hardening (RFC-008 §8, ADR-0015):
  - Storage engine: durable sidecar quarantine (`quarantine.log`, append
    + fsync, replayed by re-scan); records whose payload fails CRC or that
    extend past the segment are quarantined on read and at open instead
    of being dropped or hiding the rest of the segment; recovery
    resynchronises after a corrupt header; `Store::verify`; compaction
    purges quarantined records.
  - Storage node / protocol: `ScrubShards` (deep verification with
    per-shard classification OK / checksum mismatch / invalid header /
    truncated / quarantined / I/O error), `ListShards.include_quarantined`,
    `DATA_LOSS` for reads of quarantined shards, one scrub in flight per
    node, quarantine and scrub counters in `Health` and `/metrics`.
  - Metadata: scrub cursors per group and stripe repair tasks per
    `(group, chunk)` with the eight corruption classes; multipart
    manifest verification (roots, references, part generations, chunk
    coverage, checksums, layout).
  - Controller: paced background scrub (`--scrub-interval-secs`,
    `--scrub-rate-mib`, `--scrub-page`, `--scrub-page-mib`,
    `--scrub-concurrency`, `--scrub-tick-secs`) with byte-bounded pages
    and the cursor persisted after every page, stopping promptly on
    shutdown;
    stripe repair worker (`--stripe-repair-*`): Classify, Plan,
    Reconstruct from a verifying `k`-subset, write under the planned
    epoch, read-back Verify, Commit, with back-off and `Failed` /
    `Unrecoverable` states, never deleting; reconciliation treats
    quarantined and wrong-index shards as orphans; integrity metrics
    (`arca_controller_scrub_*`, `_integrity_findings_total{class}`,
    `_repair_*`, `_stripe_repairs_*`, `_data_integrity_status`,
    `_manifest_*`); `admin integrity status|scrub|enqueue|list|
    verify-manifests`; `storage-node quarantine-list` and the hidden
    `corrupt-shard` drill tool.
  - Gateway: degraded and unreadable durable reads are counted and
    reported to the repair queue (deduplicated per chunk);
    `arca_gateway_degraded_reads_total` and friends; audit records carry
    the claimed access key id for refused keys and `anonymous` for
    unsigned requests.
  - Tests: `storage/tests/quarantine.rs`; testkit `integrity.rs` (13
    scenarios: corruption classes, quarantine over restart and GC,
    single and multiple missing shards, unrecoverable stripes, node down
    as a retry, concurrent detections and workers, repair under
    GET/PUT/DELETE, a fenced member rejecting the repair write, scrub
    cursor resume, controller processes with leader failover and
    metrics), `multipart_integrity.rs`, `security.rs` (corrupt stored
    policy, unsupported elements, audit-log secret scan, credential
    rotation under concurrent requests, tenant isolation); the
    FoundationDB failover test covers multipart (RFC-007 A10) and waits
    for a committed write after a process loss (R-50).
  - Benchmarks: `integrity-perf.sh` (scrub and repair throughput, CPU
    per GiB, disk and network amplification) and `soak.sh` (bounded
    integrity soak with fault injection); results and analysis in
    `docs/verification/performance-regression.md` (Phase 3.2 run).
- Phase 3.1 S3 surface, metadata foundation and security controls:
  - Metadata: record version 2 with version-1 decoders (bucket owner and
    policy, generation kind and checksum), schema version constant and
    key, multipart records and recipes (hierarchical manifest: parts are
    immutable generations, Complete publishes a root with one reference
    key per part in one bounded transaction, Abort and root finish are
    batched, GC enforces part liveness), versioned reads and deletes,
    null-version preservation when versioning is enabled, suspended-bucket
    null semantics, ListObjectVersions, DELETE with `If-Match`.
  - Gateway: multipart upload (Create/UploadPart/UploadPartCopy/Complete/
    Abort/ListParts/ListMultipartUploads), GET/HEAD by version and part
    number, conditional requests (RFC 9110 order), DeleteObjects,
    ListObjectVersions, bucket versioning and policy operations,
    CopyObject (physical copy, fixed source version, copy-source
    conditions), checksums CRC32/CRC32C/CRC64NVME/SHA1/SHA256 from headers
    or `aws-chunked` trailers with composite and full-object derivation,
    `GetBucketLocation`; TOML credential store with key rotation and
    reload; bucket owner + policy-subset authorizer (RFC-012 rules 1–7,
    copy sources authorized on the source bucket); audit record per
    request with `x-amz-request-id`; CLI flags `--credentials-file`,
    `--authorize`, `--bucket-creation`, `--no-audit`,
    `--multipart-min-part-bytes`.
  - Controller: multipart sweep (aborting uploads, discarded parts, expiry
    `--multipart-expiry`), batched multipart root finish.
  - Tests: metadata multipart (10 000 parts in one transaction, races,
    liveness) and versions suites, a model-based test of the versioned key
    state machine, a 10 000-part Complete on FoundationDB,
    gateway unit tests (checksums, policy grammar, credentials), testkit
    `phase31.rs` (multipart with GC shard accounting, gateway death,
    unknown commit outcome, versioning, conditional matrix, CopyObject,
    checksums) and `s3_compat.rs` (AWS SDK for Rust, AWS CLI, boto3,
    authorization over HTTP). `s3load --op multipart-put` and a multipart
    configuration in the regression check.
- Phase 3.0 review reflected (conditional approval): RFC-007 multipart
  redesigned as a hierarchical manifest (root + per-part reference keys,
  Complete in one bounded transaction, batched abort and root finish,
  invariants I-MP1–I-MP4, linearization points, failure matrix),
  versioning linearization points and AWS differences, per-operation
  conditional-request table, checksum composite/full-object rules;
  RFC-012 nine normative authorization rules and the supported policy
  JSON subset; RFC-014 three independent compatibility axes, write gates,
  idempotent crash-safe finalize, rollback/downgrade policy, mixed-version
  test strategy; RFC-015 cluster recovery state machine with incarnation
  id, verification pass and completion criteria; status-labelled
  acceptance criteria in RFC-007/012/013/014/015; ADR-0014.
- Phase 3.0 production-hardening design (documents only, under review):
  - `docs/security/threat-model.md`; RFC-012 Security Architecture
    (SigV4 via s3s with a reloadable multi-key store, bucket ownership and
    a bucket-policy subset, mTLS service identity with role-scoped RPC
    authorization, audit records, limits and budgets, supply-chain and
    runner rules); RFC-007 S3 Gateway Compatibility (compatibility
    matrix, multipart by part-generation adoption, versioning exposure,
    conditional requests, CopyObject, checksums, DeleteObjects,
    lifecycle subset); RFC-013 Operations and Observability (deployment,
    node add/remove, device replacement, failover, metric registry,
    alert rules, runbooks, public-repository CI checklist); RFC-014
    Rolling Upgrade and Compatibility (adjacent-version window, protocol
    negotiation, format write gate, layout version and migrations,
    finalize and downgrade policy); RFC-015 Backup, Restore and Disaster
    Recovery (FoundationDB backup, restore classification of orphan /
    missing / stale-epoch state, `verification_pending` gate, epoch
    catch-up, recovery objectives); ADR-0013.
  - CI: DCO check on direct pushes; FoundationDB failover job waits for a
    healthy cluster (R-50).
- Phase 2.3 performance stabilization and scalability:
  - `benchmarks/linux/sanitize.py` and `docs/security/public-artifacts.md`:
    a policy for what committed measurement artifacts may contain, a
    sanitizer that rewrites and verifies, a CI check (`secret scan`).
  - CI on self-hosted Linux runners (GitHub-hosted runners refused for
    billing reasons): self-sufficient jobs, GitHub Actions cache for
    build artifacts, tools and the advisory database; macOS leg removed.
  - PUT phase timing (`arca_gateway_put_phase_seconds{phase}`), storage
    node and store timing (`arca_node_phase_seconds{phase}`, storage node
    `--metrics-listen`), metadata transaction counters
    (`arca_gateway_meta_txn_*`), `s3load --progress`.
  - `arca-lake-gateway`: pipelined chunk placement within a PUT
    (`--write-pipeline`, default 2); frame pass-through on GET (no
    concatenation of shard frames; CRC over frames); population queue
    (`--cache-population-queue-depth`, `--cache-populations-per-node`)
    with per-node permits, bounded yield to reads in flight, retries with
    backoff, graceful shutdown and counters.
  - `arca-lake-protocol`: `get_shard_frames`, `concat_frames`,
    per-node reads-in-flight accounting.
  - Tests: `population_queue.rs` (5 cases); every suite runs with the
    write pipeline and frame pass-through.
  - Measurement tooling: `put-profile.sh` + `sampler.sh` +
    `summarize_putprofile.py` (per-second alignment of throughput with
    sar, pidstat, TCP counters, every metrics endpoint and FoundationDB
    status), `netns-faults.sh cpuprofile` (perf), `metabench` (metadata
    hot-key micro-benchmark), `multi-gateway.sh` + `summarize_multigw.py`,
    `regress.sh` + `perf_summary.py` + `compare.py` with per-environment
    baselines.
  - RFC-009 §6.14–§6.19, RFC-006 §5.3.1, ADR-0012, risks R-48 (explained,
    mitigated) and R-49, Phase 2.3 roadmap page.
- Phase 2.2 performance and resilience optimization:
  - `arca-lake-protocol`: `RpcPolicy` for node RPCs (progress timeout for
    stalled shard streams, absolute deadline `base + bytes / min_rate`,
    bounded retries with backoff for transport errors, unary timeout),
    `RpcStats` counters, `StallStream` / `SlowStream` fault actions;
    frames handed to the transport one at a time.
  - `arca-lake-storage-node`: `PutShard` streams whose client sends no
    frame for `stream_progress_timeout` are aborted; `TCP_NODELAY` is set
    on every accepted connection (tonic ignores `tcp_nodelay` for a
    caller-supplied listener, so accepted sockets had Nagle's algorithm
    and a ~40 ms stall on small frames after unacknowledged data).
  - `arca-lake-metadata`: `Txn::atomic_add` and `Txn::get_snapshot`
    (FoundationDB atomic ADD and snapshot reads; in-memory equivalents);
    cache usage kept by atomic addition with a snapshot capacity check
    (R-42); `object::renew_lease` and `append_chunks_renewing`.
  - `arca-lake-gateway`: bounded parallel chunk fetch with ordered
    delivery (`read_parallelism`); pending-generation lease renewal
    while the client's bytes arrive and while a chunk's shards upload; hit accounting (lookup / served / degraded hits,
    fallbacks by cause, admission rejections by cause, population
    failures by cause); RPC counters exported with the cache counters.
  - `arca-lake` CLI: `--node-timeout-secs`, `--node-progress-timeout-secs`,
    `--node-min-rate-kib`, `--node-max-attempts`, `--read-parallelism`.
  - Tests: `transport.rs` (retry after reset commits once, stalled stream
    detected and retried while a slow one is not, absolute deadline,
    repair stops after exhausted retries, slow upload renews its lease),
    `parallel_read.rs` (parallelism 1/4/16 equivalence with cached,
    uncached and degraded chunks, error behind prefetched chunks, dropped
    body, retried populations under resets); every suite runs with
    parallel reads; storage sigkill harness waits for the first ack.
  - Measurement tooling: `repeat.sh` + `aggregate.py` (repetitions,
    shuffled order, warm-up, median / min / max / sd), `transport-matrix.sh`
    + `summarize_transport.py`, `diskbound.sh`, `summarize_hits.py`,
    `summarize_faults.py`, `netns-faults.sh r47` (ten conditions against
    a gateway policy; netem queue limit, two-way slow-node shaping),
    `s3load --order random --first`, `two-host.sh` `GW_EXTRA_ARGS`,
    `S3LOAD_EXTRA`, `SKIP_PUT`, `MIXED_PUT_OBJECTS`.
  - RFC-009 §6.8–§6.13 (R-47 policy matrix, hit-ratio analysis, parallel
    fetch, transport matrix, repeated comparison, disk-bound); RFC-006
    §5.3 hit accounting and §5.4 snapshot capacity check; ADR-0011;
    Phase 2.2 roadmap page.
- Phase 2.1 cache protection and Linux validation:
  - `arca-lake-metadata`: `GroupRecord.role` (durable or cache),
    `chunk_record_of`, `live_generation_with_prefix`.
  - `arca-lake-tiering`: cache protection `none` / `replication` /
    erasure coding on the shared `read_coded` / `write_coded` path,
    atomic population, degraded hits with read-triggered shard repair,
    per-member deletion; populations bounded per gateway; admission at
    capacity (eviction between the watermarks).
  - `arca-lake-controller`: `CacheProtection` and failure-domain-aware
    cache groups; cache repair on the durable repair state machine with
    a per-pass budget, subordinate to durable repair; metrics.
  - `arca-lake-protocol` / `arca-lake-storage-node`: 8 MiB HTTP/2 stream
    window, 32 MiB connection window.
  - `arca-lake-gateway`: degraded-hit and repair counters, population
    skip counter, shard write failures logged with their reason.
  - `arca-lake` CLI: `create-cache-groups --protection`, `--cache-max-populations`,
    `--cache-repair-chunks-per-pass`, `--metrics-listen`, `admin cache-usage`;
    fixed `--cache-tier` overriding `--tier`.
  - `benchmarks/s3load`, `benchmarks/linux/` (two- or three-host driver,
    matrix, summarizer, sanitizer) and `deploy/dev/netns-faults.sh`
    (namespace-pair fault injection) with committed, sanitized results.
  - Tests: `cache_ec.rs` / `cache_ec_fdb.rs` (9 cases × replication, EC).
  - RFC-006 §5.9 and §11; RFC-009 Performance Architecture draft with the
    three-host measurements; ADR-0010; Phase 2.1 roadmap page;
    `docs/verification/network-faults.md`; risks R-46, R-47, R-40 narrowed.
