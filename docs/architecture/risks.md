# Risk register

Status: **living document**. Severity is the product of likelihood and impact
on correctness, durability, or the project; it is a judgment, not a
measurement. Each risk names an owner area and a mitigation. Phase 0 must
update this list with what it learned.

## Correctness and durability

| ID | Risk | Severity | Mitigation |
| --- | --- | --- | --- |
| R-01 | A write is acknowledged before its durability condition is met (bug in acknowledgment accounting or in fsync handling on the storage node) | Critical | Durability condition evaluated in one place; storage node acknowledges only after `fdatasync` returns; fault-injection tests that drop writes after acknowledgment and check for loss; crash tests in CI |
| R-02 | Garbage collection deletes shards still referenced by a pending or committed generation | Critical | Single-owner chunk invariant; GC acts only on `retiring` generations after grace; orphan reconciliation requires generation to be neither committed nor pending-with-lease; model-based tests of the GC state machine |
| R-03 | Stale writer after membership change overwrites or adds shards to a group with an old layout | High | Per-group epochs persisted on storage nodes; writes carry epoch; rejected if stale; tests with delayed messages |
| R-04 | Metadata commit outcome unknown (timeout) leads to double-publish or false failure | High | Commit idempotent on generation ID; gateway re-reads after unknown outcome; test with injected commit timeouts |
| R-05 | Silent data corruption passes through because a checksum is skipped on some path | High | Checksums verified on every shard read before use; corrupt shard treated as missing; scrubbing; end-to-end ETag/checksum comparison in tests with bit flips |
| R-06 | Interrupted migration leaves no durable copy or two copies with inconsistent metadata | Medium (was High) | Copy-verify-switch-retire with a conditional switch transaction; the source stays referenced until the switch commits; crashes before and after the switch leave only unreferenced shards that reconciliation deletes (`tiering.rs`, RFC-006 §5.8, ADR-0009); not yet exercised on a real network |
| R-07 | On-disk or wire format change breaks rolling upgrade or old data | High | Format and protocol versions in every record; golden-file tests per version; upgrade tests reading data written by the previous release |
| R-08 | Long reads cross the GC grace period and return partial data | Medium | Per-chunk re-validation; explicit error rather than partial success; grace period configurable; documented behavior |
| R-09 | Clock skew used for correctness (leases, grace periods) | Medium | Prefer metadata-store commit versions for ordering; wall clock only as a conservative bound; document assumptions |
| R-10 | `m_ack < m` profiles leave chunks under-protected longer than expected | Medium | Under-protected chunks tracked and repaired with priority; metric and alert; default `m_ack = m` |

## Performance and scalability

| ID | Risk | Severity | Mitigation |
| --- | --- | --- | --- |
| R-11 | Gateway CPU (TLS + MD5 + S3 checksums + EC) caps throughput below storage capability | High | Measure each component in Phase 0; stateless gateways scale out; consider hardware-accelerated hashing; avoid redundant hashing passes |
| R-12 | Metadata store becomes the bottleneck for small-object workloads or listing | High | Key layout designed for range reads; incremental manifests; measure in Phase 0; keep the `MetadataStore` trait so partitioning or a different store remains possible |
| R-13 | Sequential key insertion creates write hot spots in the metadata store | Medium | Spread by bucket subspace; measure hot-spot behavior in the spike; sharded counters |
| R-14 | EC backend decode cost for single-shard loss is high (e.g., O(n log n) codes) | Medium | Benchmark single-shard reconstruction specifically; choose classic RS for small-`m` profiles. First measurement (laptop, relative) confirms the concern for the Leopard code: see [report](../../benchmarks/reports/2026-11-01-ec-backends.md) |
| R-15 | Small objects dominate and EC overhead per object is unacceptable | Medium | Replication below a profile threshold; inline-in-metadata as a later optimization |
| R-16 | io_uring or SPDK adopted without evidence | Low | Policy: measurement before low-level optimization; abstraction keeps the blocking backend as reference |
| R-17 | Repair traffic after a device failure saturates the network | Medium | Rate-limited repair with configurable budget; per-group parallelism caps; metrics |

## Operational

| ID | Risk | Severity | Mitigation |
| --- | --- | --- | --- |
| R-18 | FoundationDB operations (deployment, upgrades, client library coupling) are a burden for a small team | High | Operations guide with a minimal recommended topology; containerized client library; tests for client/server version coupling; TiKV kept as a fallback behind the trait |
| R-19 | Metadata store has no access control; exposure equals full compromise | High | Documented network isolation requirement; TLS; deployment examples that isolate it by default |
| R-20 | Controller leader failover stalls repair and tiering | Medium | Hot standbys; leases; all control work resumable from metadata records |
| R-21 | Misconfiguration (e.g., profile that cannot be satisfied by the topology) | Medium | Policy validation at configuration load and at group creation; `--check-config`; explicit refusal with reasons |

## Project and ecosystem

| ID | Risk | Severity | Mitigation |
| --- | --- | --- | --- |
| R-22 | Dependency on `s3s` for the protocol surface; project self-describes as experimental | Medium | Spike before adoption; isolate behind our trait; be prepared to vendor or replace |
| R-23 | `reed-solomon-erasure` maintenance has been inactive since 2022 | Medium | Use as oracle/fallback only; ISA-L or another maintained backend for production; scheme ID decouples stored parity from backend |
| R-24 | `tokio-uring` release cadence is low | Low | Prefer the low-level `io-uring` crate with our own small driver, or stay on the blocking backend |
| R-25 | A dependency with an incompatible license enters the tree | Medium | `cargo-deny` allow-list in CI; exceptions require an ADR |
| R-26 | Non-public information leaks into the public repository | High | Policy document; PR checklist; secret scanning; benchmark report template requiring public environment description; reviewers explicitly check |
| R-27 | Claims of S3 compatibility outrun verification | Medium | Compatibility matrix maintained from test results only; `s3-tests` and SDK suites in CI |
| R-28 | Multi-node benchmarks require infrastructure that may not be publicly describable | Medium | Use cloud instance types that can be named; otherwise publish only functional results and label performance as "not measured" |
| R-29 | A hand-written data protocol accumulates subtle transport bugs that mature RPC stacks already handle (observed in Phase 0: a missing `flush()` at message boundaries deadlocked raw TLS connections randomly under concurrency while plaintext worked) | Medium | Prefer gRPC unless measurements show a material difference (OD-08); if a raw protocol is adopted, require flush-at-boundary and timeout rules in the protocol spec and fuzz/soak tests over TLS |
| R-30 | FoundationDB differences (transaction size accounting, conflict ranges, latency under retries) surface only in production | Medium (was High) | The E2E and commit-fault suites now run against a real FoundationDB cluster in CI (`e2e_fdb.rs`, `faults_fdb.rs`); remaining gap: multi-process FoundationDB clusters and failover during a PUT |
| R-31 | Reconciliation deletes a shard that metadata still accounts for | Low (was High) | Decisions are by reference in one transaction (RFC-008 §6.3): pending generations and placed chunk records keep shards regardless of age; age applies only when no generation exists, as a guard for `begin_put` retries; `gc_safety.rs` |
| R-32 | Repair and GC enumerate a group with one unpaginated `ListShards` response; a large device exhausts memory or the gRPC message limit | Closed | `ListShards`/`VerifyShards` are paged; walkers use O(page × members) memory (`paging.rs`) |
| R-33 | A PUT that fails on one group retries on another and leaves durable shards on the first group; until reconciliation runs they consume capacity invisibly | Low | Reconciliation classifies them as unreferenced (chunk record placed elsewhere) and deletes them regardless of age (`transport.rs::lost_reply_…`); scheduling is Phase 2 |
| R-35 | Transport faults are injected at the client call boundary; real TCP/HTTP2 failure modes (resets mid-stream, half-open connections, kernel buffering) behave differently | Medium | Documented as unverified in RFC-008 §9; build a two-host test with real network impairment before Phase 3 |
| R-36 | Repair completion verification reads every shard of the rebuilt member (and of every member with `verify_all_members`); cost is O(data) per repair | Medium | Paged, bounded memory; default verifies only the replacement; rate limiting is Phase 2 |
| R-37 | Two repairs on the same group share one cursor field; a concurrent repair of another member resets it | Low | Detected: the running repair stops with `EpochChanged` and resumes from scratch (skipping present shards); leader election makes concurrent controllers impossible |
| R-38 | The leader fence conflicts with every renewal (`lease / 3`); under heavy controller write load retries could starve long transactions | Low | Renewal writes one key; `transact` retries; measure under load before Phase 4 |
| R-39 | The three-process FoundationDB configuration in CI (`double`, three coordinators) is a test setup; deployments need their own redundancy and coordinator placement, and client-side timeouts must match | Medium | RFC-011 §6 and ADR-0008 state that it is not an availability claim; operations guide (RFC-013) to specify |
| R-40 | Network namespace tests could not be run on the development container runtime (no `sch_netem`); real network impairment remains unverified | High | Two-host or Linux-host run with `tc netem`/`iptables` before Phase 3 (R-35) |
| R-34 | The gateway reports an error after the commit succeeded (crash or injected fault after commit); clients that do not retry may believe the object is absent while it is visible | Low | Documented S3 behaviour (the commit is the linearization point); clients retry PUT, which is harmless; verified by `crash_after_commit_means_object_is_visible_despite_error` |
| R-41 | A cache copy is served although it does not match the committed generation (stale, corrupt, or half-written copy) | Low | Cache placements are keyed by generation and read in the manifest's transaction; every hit is validated by length and CRC-32C against the chunk record; any failure is a durable fallback plus invalidation (`cache.rs`, RFC-006 §5.3, ADR-0009) |
| R-42 | The per-tier cache usage key and the recency index are written by every insert, eviction, and (coalesced) hit; under many gateways they become hot keys and a source of transaction conflicts | Medium | Usage is one key per tier; recency touches are coalesced per gateway and chunk (10 s); not measured (RFC-006 §9); sharded counters if measurements show conflicts |
| R-43 | Pinned entries exceed the cache capacity: eviction cannot reach the low watermark, admissions are rejected, and the cache stops absorbing new hot data without an operator noticing | Medium | `over_watermark` is reported by every eviction pass and exported as `arca_controller_cache_over_watermark`; reads are never affected (RFC-006 §5.4); operations guide to alert on it |
| R-44 | The automatic tiering pass reads every committed generation's header, access record, and manifest on each run; on a large namespace it is O(generations) metadata reads per interval | Medium | Generation walk is paged and bounded per transaction; the interval is configurable; an access-ordered index is the planned fix if measurements require it; not measured |
| R-45 | A cache node is a storage node with no redundancy; a cache tier on a single device class with one member per group loses all its copies when that device fails, causing a hit-ratio collapse (never data loss) | Low | By design (RFC-006 §5.1): copies are discardable and refill by read-through; verified fallback in `cache_node_failure_falls_back_and_invalidates` |

## Review cadence

Risks are reviewed at the end of each phase and whenever an RFC is accepted.
Closed risks are moved to a "retired" section with the evidence that closed
them.
