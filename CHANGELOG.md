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
