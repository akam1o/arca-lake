# ADR-0019: Commit outcomes by phase, idempotent re-runs, process-unique leadership, fenced deletes

Status: accepted (Phase 3.5)

## Context

Phase 3.4 left six risks open (R-64, R-65, R-68, R-70, R-71, R-72). The
Phase 3.5 reliability work and an independent cross-model review found
the decisions below; each is backed by a regression test that fails on
the previous code. The full mutation inventory is in
[commit-outcome-audit](../architecture/commit-outcome-audit.md).

## Decision

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Commit-time errors | Classified by phase and code from the FoundationDB 7.3 table (`fdb_errors`): an error ending a commit is `CommitUnknown` unless the code proves rejection (`RETRYABLE_NOT_COMMITTED`, client errors); the same codes before the commit are definite. Timeout and cancellation are unknown at commit (R-72) | trusting `is_maybe_committed` alone (misses 1031/1025/1101, which the C API documents as "may or may not commit"); treating every timeout as unknown (reads that time out sent nothing) |
| Re-running after an unknown outcome | `transact_idempotent` only for bodies whose re-run after an applied commit has no effect *and* cannot act on state written by others since. Ids that are reused (`null` versions) are pinned to the generation first; unconditional DELETE re-runs with an internal `if_current` precondition on the pointer it was decided on | re-resolving a version id on each attempt (CON-1: deletes a version written since); not re-running (an applied abort reported as failed left multipart parts until the sweep, R-71) |
| Conditional DELETE after an unknown outcome | 503, not re-run | re-running into 412/404 for a delete that succeeded |
| Leadership identity | The leader record holder is the configured id plus a random per-process instance suffix: a second process with the same configured id neither inherits a live lease nor shares a token (SEC-4) | the configured id alone (two processes pass each other's fence) |
| Storage-node delete fence | A node that adopted an incarnation refuses deletes carrying none (SEC-5); deletes hold the node's fence shared from their checks to the end of the store mutation and epoch raises / incarnation adoption hold it exclusively (SEC-6). Admin commands stamp the current incarnation | accepting empty incarnations for old callers (any caller could skip the restore fence); checks outside the lock (an acknowledged raise could race a delete past the old fence) |
| Tombstone history in the index | Kept only while the index is rebuilt at open; a runtime quarantine applies only to the location still live (R-70: one entry per delete for the life of the process) | bounding the history by size (the rebuild needs all of it, runtime needs none) |
| Reads during a member move | The rebuilt shard index is also read from the node the member left (replace request or drain), which keeps its shards until the group is `Active`; verified by the chunk checksum like any shard. Rebuilds without a former node are counted (`arca_controller_groups_reduced_redundancy`) (R-65) | copy-before-switch with dual writes to an incoming member (a second state machine next to repair, and writes during the copy need catching up); remove-then-rebuild as before (one failure during the move made objects unreadable) |
| S3 authorization in batches | DeleteObjects authorizes each `(key, version)` against `s3:DeleteObject(Version)` and the bucket policy in the backend through an `EntryAuthorizer` the authorizer inserts (SEC-1) | authorizing the envelope (no key: object-scoped denies and the version action did not apply) |
| `aws:SecureTransport` | `x-forwarded-proto` believed only from `--trusted-proxy` peers; the listener itself is plaintext (SEC-2) | believing the header from anyone |
| Authorization without authentication | refused at startup and in `serve_with_access`; owner-policy with static keys shares one credential store (SEC-3, SEC-8) | s3s skips the access hook without authentication |
| Credential rotation | reload when the credentials file or any referenced secret file changes, stamped from the bytes parsed (SEC-7, OPS-2) | the credentials file's mtime |
| Multipart sweep | one bounded page of the upload space per transaction (OPS-1) | one transaction over every upload |
| Deletes of reusable shard keys | Nodes return record sequences (PutShard, ListShards) and honour `DeleteShard.if_sequence`; reconciliation and migration cleanup delete only the records they saw or wrote; a migration leaves its source to reconciliation (CON-5, CON-6) | unconditional deletes fenced by the group epoch only (the epoch does not change between two placements of a chunk in one group: a stale delete removed a later placement) |
| Tier migration concurrency | a per-chunk intent taken before the target is written, kept by reconciliation, required by the switch, expired after one hour (CON-1, CON-2) | protecting targets by age (a committed generation is old); a `Deleting` state (does not stop RPCs already sent) |
| Version identity on re-runs | generation, publication time and ordinal, the ordinal taken from the current version's record (CON-3, CON-7, CON-11: a pointer restored to an older version keeps the newer `next_ordinal`) | the generation alone (all delete markers share the zero generation) |
| Record sequences as delete tokens | a persisted bound above every sequence handed out (CON-10); runtime tombstones kept until every older in-flight record has reached the index, then pruned (CON-9; R-70 stays bounded by the in-flight window); nodes without sequences get no reconciliation or cleanup deletes (CON-12) | sequences rebuilt from surviving records (compaction can drop the highest); no runtime history (an older put applied after a newer delete resurrected the key); unconditional deletes on older nodes (the race CON-5/CON-6 stays open during a rolling upgrade) |
| Paged listings with quarantine | live and quarantined keys merged into one bounded page whose cursor is the last merged key (OPS-5) | the live keys' cursor (quarantined keys beyond one page were never listed) |
| Upgrade cut-off | the `schema_write` raise and the cursor advance in one transaction (CON-8) | two transactions (a cancellation in between left the gate raised without a cursor) |
| Process exit with the FoundationDB client | `_exit` after the runtime has stopped and the standard streams are flushed (R-68: 13 of 80 signalled shutdowns crashed in the client library's static destructors) | stopping the network at exit (handles still alive: every test binary crashed) |
| GC cadence | a pass runs batches for at least the GC interval when the backlog is full; 32 generations at once (R-64) | a 30 s budget every interval, 8 at once (about 20 generations/s on the reference VM) |

## Consequences

- `FaultKv` gains commit timeouts (after apply, without apply, delayed)
  and `interleave_next`; new mutations are expected to be tested with them.
- A node adopts an incarnation through the controller's health facts; a
  pre-incarnation caller can no longer delete on such a node (no
  deployments exist that need one).
- New gauges: `arca_node_index_entries`, `arca_node_process_memory_bytes`,
  `arca_controller_gc_backlog`, `_gc_backlog_oldest_age_seconds`,
  `arca_controller_groups_reduced_redundancy`; alerts
  `ArcaNodeHeapGrowing`, `ArcaNodeMemoryHigh`.
