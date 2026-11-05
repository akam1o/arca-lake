# Commit outcome audit (Phase 3.5, R-72)

Every metadata mutation and what happens when its commit's outcome is not
known. Normative for new mutations: a change that adds one adds its row.

## 1. What FoundationDB tells us (7.3)

Sources: `flow/include/flow/error_definitions.h`, `fdb_error_predicate`
in `bindings/c/fdb_c.cpp`, the C API documentation of
`fdb_transaction_commit` and `fdb_transaction_cancel`, and the `timeout`
transaction option in `fdb.options`.

- The `MAYBE_COMMITTED` predicate covers only `commit_unknown_result`
  (1021) and `cluster_version_changed` (1039).
- The `timeout` option is implemented as cancellation, and a transaction
  cancelled after `fdb_transaction_commit` was called "may or may not"
  commit, "even if the call appears to return a `transaction_cancelled`
  error". `transaction_timed_out` (1031) returned by a commit is therefore
  an unknown outcome, as are `transaction_cancelled` (1025) and
  `operation_cancelled` (1101).
- `RETRYABLE_NOT_COMMITTED` (1007, 1009, 1020, 1037, 1038, 1042, 1051,
  1078, 1213, 1223): the commit was rejected; retrying is safe.
- 2000–2999 (client usage errors, size limits): raised by the client before
  anything is sent.
- A late commit is bounded by the MVCC window (five seconds) and is
  rejected if anything it read changed meanwhile.

## 2. The backend contract

`crates/metadata/src/fdb_errors.rs` (`classify(code, message, phase)`),
shared by the FoundationDB backend and the fault injector:

| Phase | Codes | `KvError` | Meaning |
|---|---|---|---|
| any | `RETRYABLE_NOT_COMMITTED` | `Conflict` | definitely not committed; `transact` retries |
| any | 2101–2103 | `TooLarge` | definitely not committed (client) |
| any | other 2000–2999 | `Backend` | definitely not committed (client) |
| any | 1021, 1022, 1039 | `CommitUnknown` | outcome unknown |
| commit | anything else (1031, 1025, 1101, 1100, 15xx, …) | `CommitUnknown` | outcome unknown |
| before commit | anything else | `Backend` | definitely not committed (nothing was sent) |

Before Phase 3.5 a commit-time 1031 was `Backend`, a definite failure
(R-72). `Fenced` and `ClusterState` are raised by the wrappers before the
inner commit and are definite. A successful commit is definitely
committed.

Resolution primitives: `transact` never re-runs after `CommitUnknown`;
`transact_idempotent` re-runs up to three times and is allowed only for a
body whose re-run after an applied commit has no further effect *and*
cannot act on state written by others since (Phase 3.5 audit CON-1:
version ids such as `null` are reused).

## 3. Mutations

"Unknown" says how an unknown outcome is resolved and why that is safe.

### Objects and S3 operations (gateway)

| Mutation | Unknown outcome | Why safe | Tests |
|---|---|---|---|
| `begin_put`, `begin_part` | re-run (`transact_idempotent`): generation id chosen before the first attempt | same keys, same values | `commit_unknown.rs::begin_and_append_*`, `commit_timeouts.rs` |
| `append_chunks` | re-run: chunk records are a function of the chunk; the lease only moves later | reads the header: a late commit after an abort conflicts | `commit_timeouts.rs` (Append × every outcome) |
| `commit_put` (PUT, Copy) | `commit_resolving`: re-read the generation; committed = success; pending = retry (bounded); then `abort_put` fences a late commit; abort finding it committed = success | the abort writes the header the commit read | `commit_unknown.rs`, `testkit/src/suite_faults.rs`, FDB failover |
| `abort_put` (gateway after a failure) | best effort; lease expiry retires it otherwise | refuses a committed generation | `commit_timeouts.rs` (Abort) |
| `seal_part` (UploadPart) | `seal_resolving`: like `commit_put`; retiring + upload open = superseded = success | as above | `commit_timeouts.rs` (Seal) |
| `complete` | `complete_resolving` re-runs with the root id chosen before the first attempt; `Completed{root}` for that root = same answer | idempotent by root id | `phase31.rs::complete_with_unknown_*`, `commit_timeouts.rs` (Complete) |
| `abort` (multipart, Open→Aborting) | re-run: `Aborting` is accepted again; a Complete that won answers `NoSuchUpload` | R-71: an applied abort reported as failed left the parts until the sweep | `commit_timeouts.rs::abort_whose_commit_times_out_*` |
| `discard_parts` | re-run: a part record is cleared in the transaction that retires its generation | nothing retired twice | as above |
| `delete_object_as` (unconditional DeleteObject) | every attempt carries the internal precondition `if_current` = the version identity (generation, publication time, ordinal) it was decided on and the same pre-chosen marker id; a moved pointer before the commit = pin again; after an unknown outcome the re-run's moved pointer is resolved by the marker (versioning enabled: present = done, absent = 503) or, unversioned/suspended, as done (the other write displaced the version) | never acts on a version written since, never a second marker, never claims a versioned delete it cannot prove (CON-1, CON-3, CON-4, CON-7) | `commit_timeouts.rs::a_delete_rerun_*`, `::a_versioned_delete_*`, `commit_unknown.rs::markers_of_the_same_millisecond_*` |
| `delete_object_if` (If-Match) | not re-run; 503 (unknown) | a re-run would answer 412/404 for a delete that succeeded | `commit_timeouts.rs::deletes_*` |
| `delete_version` (DeleteObject/DeleteObjects with a version) | version pinned to its identity (generation, publication time, ordinal) first; re-run deletes only while the id still names it | a reused `null` id or marker cannot make the re-run delete a newer version (CON-1, CON-3) | `commit_unknown.rs::delete_version_rerun_*`, `commit_timeouts.rs::deletes_*` |
| `create_bucket_owned`, `delete_bucket` | error to the client (503); the client's retry answers `BucketAlreadyExists`/`NoSuchBucket` | no data effect; wrong code only after an applied commit | — (Low, documented) |
| `set_versioning`, `set_policy`, `set_owner` | error to the client; a retry writes the same value | idempotent | — |
| `record_access`, `reset_access` | best effort, counters for tiering | affects tiering timing only | — |

### GC, reconciliation, multipart sweep (controller)

| Mutation | Unknown outcome | Why safe | Tests |
|---|---|---|---|
| `expire_pending` | the next pass re-reads the index entry and the header | one transaction per generation, state-checked | `gc_safety.rs` |
| `finish` (after shard deletes) | the next pass lists the generation again, deletes again (idempotent), finishes (`NoSuchGeneration` = done) | shard deletes are idempotent; metadata goes only after them | `gc_safety.rs`, failover |
| `finish_root_step` | the next pass resumes from the remaining part references | state-driven | `multipart.rs` |
| reconcile | no metadata mutation; a shard is deleted only on a single-transaction attribution | — | `gc_safety.rs`, `transport.rs` |
| `sweep_multipart` (`abort`, `discard_parts`) | as above, re-run | idempotent | `multipart.rs::sweep_*` |

### Repair, integrity, placement, membership (controller and admin)

| Mutation | Unknown outcome | Why safe | Tests |
|---|---|---|---|
| `update_group` (membership commit: members, epoch, Repairing, cursor) | the caller re-reads the group: same members = resumed repair, other epoch = `EpochChanged` | the fence precedes the commit; the commit is conditional on the epoch read | `repair.rs`, `membership_faults.rs` (FDB) |
| repair cursor (`persist_cursor`) | the next chunk or a resumed run rewrites it | progress only | `repair.rs` |
| `enqueue`/`enqueue_or_wake`, `set_task`, `remove_task` | the next scrub or repair pass re-derives tasks from shard state | tasks are hints; a missing or extra task is re-checked against the shards | `integrity.rs` |
| `set_scrub_cursor`, `set_manifest_cursor` | progress only | — | — |
| `create_group`, `register_node`, `touch_node` | error to the operator; `touch_node` is a liveness timestamp | `create_group` is not idempotent: an operator's retry after an applied commit creates another empty group on the same nodes (Low: no data effect, visible in the group list); `register_node` and `touch_node` are | — |
| `drain_node`, `cancel_drain`, `update_drain`, `request_replace`, `clear_replace`, `note_replace_error` | operator retry; the controller re-reads intents each pass | records of intent, state-checked | `membership_faults.rs` |
| `begin_migration`, `end_migration`, `expire_migrations` | error: the next pass re-reads the intent; a held intent of a crashed attempt expires after one hour | intents protect targets and serialize migrations of a chunk; deletes are conditional on node record sequences | `tiering.rs::a_paused_migration_*`, `::deletes_decided_before_*`, `commit_unknown.rs::stale_migration_*` |
| `switch_placement` (tiering) | error: the copy is *not* removed; the next pass re-reads the chunk record; only the attempt holding the intent switches | applied: the source is unreferenced and reconciled conditionally; not applied: the intent keeps the target until cleaned up or stale | `tiering.rs` |
| cache `insert`/`remove`/`touch`/`set_pinned`/`finish_cache` | error: no copy deleted on error; cache reconciliation removes copies no placement names | reads verify against the chunk record and fall back to the durable tier | `cache*.rs` |

### Leadership, recovery, upgrade

| Mutation | Unknown outcome | Why safe | Tests |
|---|---|---|---|
| `try_acquire` | the loop retries; the same holder (this process: configured id plus an instance suffix) gets the same token back | holder is per process since SEC-4 | `controller.rs` |
| `renew` | keeps acting until the lease would have expired; every controller transaction is fenced on the token | fencing, not the renewal, protects | `fence.rs`, `controller.rs` |
| `release` | best effort; the lease expires | — | — |
| `finalize` (recovery) | the operator re-runs; an applied finalize leaves the cluster `Active` and the re-run is refused by state | single transaction; audit record per attempt | `recovery_finalize.rs` |
| `activate_read_only`, `restart_verification`, `set_cursor`, `set_progress`, `set_report`, `set_node_caps`, `append_upgrade_audit` | state-checked re-run by the operator or the next pass | — | `recovery*.rs`, `fdb_restore.rs` |
| `advance_upgrade_raising` (`schema_write` cut-off) | the next pass re-reads the cursor; the raise happens only with the cursor advance | a cancellation cannot leave the gate raised without a cursor (CON-8) | `upgrade.rs::a_cancelled_upgrade_*` |
| `compat::init`, `set_phase`, `transition`, `mark_restoring`, `raise_version`, `request_finalize`, `advance_upgrade`, `note_upgrade_error`, `clear_upgrade`, `heartbeat`, `remove_heartbeat` | operator or the next pass re-reads the gate state; version raises are monotonic | — | `upgrade.rs`, `mixed_version.rs` |

## 4. Fault injection

`crates/metadata/src/fault.rs` (`CommitFault`): reply lost after apply,
request lost before apply, store unavailable, and since Phase 3.5 commit
timeouts after apply, without apply and delayed (`apply_delayed` commits a
held transaction later, against whatever changed meanwhile), plus
`interleave_next` (a writer between an applied commit and its lost reply).
`crates/testkit/tests/commit_timeouts.rs` hits every commit of UploadPart,
Complete, the gateway's abort, multipart Abort and DELETE with each outcome
and requires nothing lost, nothing leaked, nothing reported failed that
succeeded. Real FoundationDB: `fdb_failover.rs` (process loss, quorum
loss, coordinator loss, leader failover, repair under process loss).

## 5. Not covered

- A late commit landing after the MVCC window is impossible in
  FoundationDB; the in-memory backend accepts it (`apply_delayed` is used
  only right after the failure or the retry).
- Client-visible wrong codes after an applied bucket create/delete (Low).
- An unconditional delete on an unversioned or suspended bucket whose
  first attempt was not applied and whose pointer moved before the re-run
  reports success (linearized before the other write; the visible state is
  the same). On a versioning-enabled bucket the marker decides: absent =
  503.
- Cache copies deleted by population, repair, invalidation and eviction
  are not conditional on record sequences (INT-1/INT-2, Medium): a newer
  copy under the same key can be lost, which costs a cache miss (reads
  verify against the chunk record and fall back to the durable tier).
- Delete tokens are (store epoch, sequence) with a new random 64-bit
  epoch at every store open: two epochs collide with probability 2^-64
  (CON-20); a token from before a node restart is refused, and the delete
  is redone after the next listing.
- Storage nodes without record sequences (before Phase 3.5) get no
  reconciliation or migration-cleanup deletes; their orphans wait for the
  upgrade (CON-12).
- Regression tests not shown to fail on the previous code: CON-4 (an
  unproven versioned delete, the previous code had no such path), CON-8
  (the test calls the new combined operation) and OPS-3 (the previous code
  had no paging to break), OPS-6 (one index snapshot per listing page;
  the race needs a pause between two reads that no longer exist).
