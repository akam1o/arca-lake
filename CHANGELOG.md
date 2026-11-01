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
