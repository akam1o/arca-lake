# Phase 1: Distributed object storage

Status: **first iteration done**. Phase 1 turns the Phase 0 components into
one system: S3 PUT and GET through a gateway that erasure codes objects
across storage nodes and publishes them atomically in the metadata store.
Durability and consistency come first; performance work is Phase 4.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 1.1 RFC-004 Erasure Coding and Placement | drafted | [RFC-004](../rfcs/RFC-004-erasure-coding-and-placement.md); §11 lists what Phase 1 verified |
| 1.2 Storage node service | done | `arca-lake-storage-node`: gRPC API v1 over the storage engine, persisted per-group epochs, fencing on put/delete, durable acknowledgment via group commit |
| 1.3 Cluster map, EC profiles, placement | done | `arca-lake-metadata::ops::cluster` (nodes, groups, map version), `arca-lake-common::EcProfile`, `arca-lake-placement` (failure-domain-aware member choice) |
| 1.4 Gateway cluster backend | done | `arca-lake-gateway::cluster`: write protocol (generation → EC → parallel shard writes with CRC check → batched manifest → commit), read protocol (manifest → shards → decode → verify → stream), small-object replication, fault injection |
| 1.5 Controller operations | done as library functions | `arca-lake-controller`: group bootstrap, GC (expiry, collection, orphan reconciliation), member replacement with reconstruction. No process, scheduler, leader election, or admin API yet |
| 1.6 Integration tests | done | `arca-lake-testkit` harness and `tests/e2e.rs` (18 tests, see below) |
| 1.7 Design notes | done | Record encoding evaluation (RFC-003 §5.7), index persistence measurement (RFC-005 §5.6), ADR-0006 |
| 1.8 FoundationDB-backed end-to-end run | not started | E2E tests use the in-memory backend; FoundationDB runs the recipe-level protocol tests only |
| 1.9 Controller process and admin API | not started | |
| 1.10 Multipart upload, versioned-bucket GET/LIST of versions through the gateway | not started | Metadata recipes support versioning; the gateway exposes unversioned semantics |

## What the integration tests verify

Run with `cargo test -p arca-lake-testkit --test e2e` (the AWS CLI test
skips when `aws` is not installed).

| Area | Test | Verified behaviour |
| --- | --- | --- |
| Write/read | `put_get_across_three_nodes_with_multiple_chunks` | 3 chunks × (2+1) shards, one shard per node per chunk; byte-identical GET; range read across a chunk boundary; HEAD size and ETag |
| Write/read | `small_object_uses_replication_and_survives_two_node_loss` | Objects below the threshold use `replication-v1` (1+2); readable with two nodes stopped |
| Gateway | `aws_cli_put_get_delete_through_gateway` | AWS CLI `s3 cp` up and down, `head-object`, range `get-object`, delimiter listing, `delete-object` |
| Concurrency | `concurrent_writers_same_key_leave_exactly_one_committed_version` | 8 writers; GET returns one writer's complete bytes with its ETag; after GC only the winner's shards remain |
| Concurrency | `concurrent_writers_distinct_keys_all_readable` | 16 concurrent keys |
| Conditional | `conditional_put_if_none_match_and_if_match` | `If-None-Match: *` and `If-Match` succeed/fail as S3 specifies; rejected PUTs leave no visible state and their shards are collected |
| Conditional | `concurrent_conditional_create_has_exactly_one_winner` | 8 concurrent `If-None-Match: *` creators, exactly one success |
| Visibility | `get_during_put_sees_only_complete_versions` | 40 GETs during 6 overwrites: every response is one complete version with the matching ETag |
| Visibility | `delete_during_get_never_yields_wrong_bytes` | Body started before DELETE+GC completes with the original bytes or fails; delivered bytes are always a prefix of the original |
| Node failure | `get_with_one_node_stopped_decodes_and_two_stopped_fails_explicitly` | Degraded read and range read with one member down; explicit error with two down; PUT with two members down fails and is invisible; reads recover after restart |
| Node failure | `node_restart_preserves_acknowledged_shards` | Restarted node serves its shards while another member is down |
| Corruption | `corrupt_shard_is_detected_and_decoded_around` | On-disk bit flip → node returns `DataLoss`; gateway decodes from the others; second corrupt shard → explicit error; other objects unaffected |
| Repair | `repair_onto_new_node_yields_identical_data` | Epoch 1→2, 3 shards reconstructed, reads identical with another member down, new writes use epoch 2, re-run reconstructs nothing |
| Fencing | `stale_epoch_writes_are_fenced_until_membership_commits` | Node rejects epoch 1 after learning 5; gateway PUT fails and is invisible; after the membership commit, PUT succeeds with epoch 5 |
| Crash | `crash_after_shards_before_manifest_leaves_orphans_that_reconcile_reclaims` | Old version stays visible; retry succeeds; orphans unknown to GC are reclaimed by reconciliation only once older than the minimum age |
| Crash | `crash_before_commit_is_invisible_and_collected` | Uncommitted generation invisible; GC deletes all 6 shards |
| Crash | `crash_after_commit_means_object_is_visible_despite_error` | Commit is the linearization point; client retry is harmless |
| Lease | `expired_pending_generation_is_retired_and_reclaimed` | Abandoned pending generation collected after the lease |

## Not verified in Phase 1

Listed so that nobody documents them as guaranteed:

- Behaviour against FoundationDB end to end (only the in-memory backend in
  E2E tests; FoundationDB runs the recipe-level tests).
- Real network partitions, delayed or reordered messages, slow nodes; tests
  stop processes cleanly or flip bytes on disk.
- Metadata store unavailability or timeouts during a PUT (commit outcome
  unknown, RFC-003 §7).
- Power loss on hardware; `SIGKILL` tests exist for the storage engine
  (Phase 0), not for the full node.
- Repair concurrent with writes to the same group; the race analysis in
  RFC-004 §5.7 is a design argument, not a test.
- Controller crash in the middle of a membership change (the fenced-but-not-
  committed state is tested; a crash during reconstruction is not).
- Profiles other than 2+1 and 1+2 end to end (the EC crate has property
  tests for many profiles).
- Any performance property.

## Acceptance criteria for closing Phase 1

1. All of the above plus a FoundationDB-backed end-to-end run in CI.
2. A controller process that runs GC, reconciliation, and repair on a
   schedule with leader election.
3. Multipart upload through the gateway.
4. RFC-003, RFC-004, RFC-005 moved to `review`.
