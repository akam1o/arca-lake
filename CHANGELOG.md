# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
will follow [Semantic Versioning](https://semver.org/) once it has releases.

## [Unreleased]

### Added

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
