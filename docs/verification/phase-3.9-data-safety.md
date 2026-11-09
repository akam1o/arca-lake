# Phase 3.9 data safety closure verification

Deterministic in-process tests with fault injection and a system model
carry most of the evidence. Tests on real FoundationDB run in CI (single
process, container), and one load run per configuration ran on two
validation VMs (shared virtual storage). Nothing here verifies behaviour
beyond about 30 minutes, on physical storage, or with more than two
hosts (R-79, R-86).

## R-88: cause

A delete decided on the metadata (GC verified a generation unreferenced;
reconciliation found an orphan) is sent to the storage nodes as
`DeleteShard` RPCs. Before Phase 3.9 a node checked the caller's cluster
incarnation, the group epoch and, for conditional deletes, the record
token. None of these changes when the decision becomes wrong:

- **Restore.** `fdbrestore` brings back metadata in which the generation
  is referenced again. The nodes learn the new incarnation only at
  `recovery begin` and the leader's next push, so a delete delayed past
  the restore carried the incarnation the node still held.
- **Same epoch.** The group epoch the delete was decided under is the
  one the restore brings back.
- **Same record.** The record token names exactly the record the
  restored metadata needs.
- **Phase.** The phase (`ReadOnly`, `Restoring`) was not visible to
  nodes at all.

`it/delete_fence.rs::without_closing_the_fence_first_the_restored_object_loses_its_shards`
reproduces the race on the current code by skipping the close: every
delayed delete is applied and the restored object loses its shards. With
the close (the runbook's first step, also performed again by `recovery
begin`) the same deliveries are refused by the fence itself (checked by
reason, not by incarnation).

The design and the argument for each invariant are in ADR-0023; RFC-008
§6.11 places it in the GC rules.

## Delete paths (§2 audit)

Every path that removes shard bytes on a storage node, found by
following every caller of `NodeClients::delete_shard_if` (the only
client function that sends `DeleteShard`; `delete_shard` calls it) and
every file removal in the storage engine. "Fence" lists what the node
checks:

- **I**: incarnation
- **E**: group epoch
- **T**: record token (sequence and store epoch)
- **F**: delete fence generation, open

| # | Path | Removes | Decided by | Basis | Fence | Physical point | Races considered | Protection | Verification | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | S3 DELETE (unversioned) | metadata only: the generation goes `Retiring` | gateway | current version in metadata | metadata guard (`ClientDelete` class, cluster state and incarnation) | none (GC, row 5) | phase change, restore | refused outside `Active`; shards only through GC | `e2e`, `gc_safety`, `system_model` | Safe |
| 2 | Versioning: delete markers, noncurrent versions | metadata only | gateway | version records | as row 1 | none (GC) | as row 1 | as row 1 | `versioning` suites, `system_model` (versioned keys) | Safe |
| 3 | Multipart abort | metadata (upload and parts retired) | gateway | upload record | as row 1 | none (GC `sweep_multipart`) | complete racing abort | parts retired as of their root (I-MP2, ADR-0020) | `multipart*`, `system_model` (`MultipartAbort`) | Safe |
| 4 | Parts unused after multipart complete | metadata (parts retired) | gateway | manifest of the root | as row 1 | none (GC) | as row 3 | as row 3 | `multipart*`, `system_model` (`MultipartComplete`) | Safe |
| 5 | GC | every shard of a retired generation, cache copies of it | controller leader | `verify_unreferenced` (Retiring, no reference), Delete-class guard | I, E, F (no token: chunk ids are never reused) | node `delete_shard` → `Store::delete` | restore, phase change, leader change, delayed or duplicated RPC | fence (ADR-0023), leader token on the metadata, guard refused outside `Active` | `it/delete_fence.rs` (all), `gc_safety`, `system_model` (`Gc`, `FormerLeaderGc`, `Restore`, `PhaseFlip`, `DeliverDelayed`) | Safe |
| 6 | Reconciliation | orphan shards (no chunk record, a former member's copy, a superseded copy) | controller leader | listing token + reference check + `min_orphan_age` | I, E, T, F | `Store::delete_if` | rewrite under the same key, restore, phase change | token (a newer record is kept), fence; fenced deletes counted `kept_fenced` | `system_model` (`Reconcile`), `gc_safety`, `recovery_finalize` | Safe |
| 7 | Old shards after a repair | the replaced member's copy | reconciliation (row 6) | epoch raised by the repair | I, E, T, F | as row 6 | delete decided under the old epoch, delivered after the raise | epoch fence (Phase 3.5 SEC-6, owned guard since 3.9) | `repair_completion::delete_decided_under_an_old_epoch_is_rejected_after_a_fence`, `storage-node delete_fence::an_epoch_raise_is_acknowledged_only_after_deletes_past_the_old_fence` | Safe |
| 8 | Rebalancing: source after the switch | the source group's copy | reconciliation (row 6) | the chunk record names the target | I, E, T, F | as row 6 | delete delivered right after the switch, leader change mid-move | the source copy becomes an orphan only after the switch commits; token | `suite_rebalance` (8 cases, in-memory and FoundationDB), load run below | Safe |
| 9 | Rebalancing/tiering: a failed attempt's target copies | target shards the attempt wrote | the attempt (controller) | its own `PutShard` tokens, intent held with 10 min to spare | I, E, T, F | `Store::delete_if` | a successor wrote the same keys | token; expired attempts never delete (R-89) | `an_expired_move_never_deletes_its_successors_shards`, `a_move_whose_target_changed_epoch_is_abandoned` | Safe |
| 10 | Tiering: the old tier's copy | as row 8 | as row 8 | as row 8 | as row 8 | as row 8 | as row 8 | as row 8 | `system_model` (`Migrate`), tiering suites | Safe |
| 11 | Cache eviction | cache copies | controller leader | cache record removed first, tokens of the copy | I, E, T, F | `Store::delete_if` | repopulation under the same key | token; a refused delete leaves an orphan for row 12 | `cache_tokens`, `cache_ec::*eviction*`, `system_model` (`CacheReadAndEvict`) | Safe |
| 12 | Cache reconciliation | cache orphans | controller leader | listing token, no cache record | I, E, T, F | `Store::delete_if` | as row 11 | token; fenced counted as kept | `cache_ec::*`, `cache` suites | Safe |
| 13 | Gateway clean-up of a failed write | shards the PUT itself wrote | gateway | its own `PutShard` tokens | I, E, T, F (gateway takes the recorded generation, closed on a failed read) | `Store::delete_if` | phase change during the PUT | token; a refused delete leaves an orphan for row 6 | `faults` suites (partial writes) | Safe |
| 14 | Compaction | superseded records, tombstones no longer needed, whole sealed segments | storage node | its own index (live location, dead-elsewhere set) | none needed (no external decision) | segment unlink after the index swap | crash mid-compaction, readers holding the file | index swap before unlink; since 3.9 the output is a temporary file until synced | storage `io_faults` (incl. `a_compaction_output_torn_before_its_sync_is_never_a_corrupt_segment`), `sigkill`, `compaction` | Safe |
| 15 | Recovery: a segment shorter than its header | an empty file of a crashed creation | storage node at open | file length | none needed | `remove_file` at open | none (holds no record) | | `crash.rs` | Safe |
| 16 | Recovery: leftover compaction output | `seg-*.arca.tmp` | storage node at open | file name (never in an index) | none needed | `remove_file` at open | none | | `a_compaction_output_torn_before_its_sync_is_never_a_corrupt_segment` | Safe |
| 17 | Quarantine | corrupt records set aside (bytes kept until compaction) | storage node | CRC failure | none needed | compaction (row 14) | repair racing quarantine | quarantine entry released only when the record goes | storage `quarantine` tests | Safe |
| 18 | Node decommission | nothing on the node: the node leaves the groups in metadata | operator + leader | no group lists it | n/a | none (the operator wipes the device outside arca-lake) | | | `membership::decommission*` | Safe |
| 19 | Group member replacement | the old member's shards through reconciliation | leader | epoch raise | as row 7 | as row 7 | as row 7 | as row 7 | `membership_faults`, `repair_completion` | Safe |
| 20 | Restore clean-up | nothing during recovery; post-restore orphans through reconciliation after finalize | leader of the new incarnation | report, `min_orphan_age` | I, T, F (new incarnation, a generation above every earlier one) | as row 6 | deletes decided before the restore | fence closed with a hold before the restore; the hold is lifted only for a later incarnation | `it/delete_fence.rs::a_delete_decided_before_a_restore_never_removes_what_the_restored_metadata_needs`, `recovery*`, `system_model` (`Restore`) | Safe |
| 21 | Former leader's GC | as row 5 | a controller whose lease expired | its last view | metadata: leader token; node: F | as row 5 | new leader's term | the new leader's generation is above every node's | `it/delete_fence.rs::a_former_leaders_late_deletes_are_refused`, `a_leader_killed_while_pushing_its_fence_is_superseded_cleanly` | Safe |

No admin command sends `DeleteShard` (the CLI has no shard delete).
Only `DeleteShard` removes data on request; `PutShard`, `UpdateGroupEpoch`,
`VerifyShards`, `ScrubShards` and `Health` never remove a record.

## Phase and restore transitions (§3)

| Transition | What happens to deletes | Verification |
| --- | --- | --- |
| Active → ReadOnly (maintenance) | `cluster set-read-only` closes the fence on every node it reaches; the leader closes the rest at its next pass; the Delete-class guard refuses new decisions | `deletes_decided_before_a_phase_change_are_refused_after_it`, `the_leader_opens_the_fence_for_its_term_and_closes_it_outside_active`, `system_model` (`PhaseFlip`) |
| Active → Restoring (restore start) | `admin delete-fence close` (hold), then `recovery begin` closes again and refuses unless every node acknowledged | `a_delete_decided_before_a_restore_never_removes_what_the_restored_metadata_needs`, `a_node_that_missed_the_close_is_reported_and_closed_once_back` |
| Restoring → VerificationPending → ReadOnly / RecoveryRequired | fence stays closed (the leader closes outside `Active`), recovery pushes the closed generation with the hold | recovery suites, `system_model` (`Restore`) |
| Recovery → Active (finalize) | the hold is released in the finalize transaction; the leader opens a new generation | `a_delete_decided_before_a_restore_…` (GC resumes) |
| Incarnation change | nodes adopt it under the exclusive guard; the hold follows the node's incarnation | `only_a_later_incarnation_or_a_release_lifts_the_hold` |
| Leader change | new generation above every node's | `a_former_leaders_late_deletes_are_refused`, `a_leader_killed_while_pushing_its_fence_is_superseded_cleanly`, `system_model` (`LeaderChange`) |
| Node restart | fence persisted before acknowledged; a node refuses until it holds the leader's generation | `the_fence_survives_a_node_restart`, `the_delete_fence_only_moves_forward_and_survives_a_restart`, `system_model` (`NodeRestart`) |
| Stale node rejoining | never goes back to a lower generation or reopens a closed one | `deletes_need_the_open_fence_generation_the_node_holds` |
| Rollback | a version-2 node receives no delete from version-3 clients; a version-3 node refuses version-2 deletes | `an_older_protocol_delete_is_refused`; `mixed_version` (CI) |
| Mixed-version upgrade | deletes pause per node until both sides speak version 3 | `mixed_version::*` (CI, version-bump build) |
| A restore step fails | deletes stay stopped (closed, held); every step can be re-run | `a_node_that_missed_the_close_…` (re-run), `a_leader_killed_while_pushing_…` (interrupted close completed by a re-run) |

## Storage node guarantees (S3, S4)

| Test | What it shows |
| --- | --- |
| `storage-node delete_fence::deletes_need_the_open_fence_generation_the_node_holds` | nothing adopted: refused; another generation, a closed one, none: refused; generations never go back, a closed one never reopens; refusals counted |
| `storage-node delete_fence::an_older_protocol_delete_is_refused` | protocol 2 with or without a generation, protocol 3 without one: refused (in a version-bump build by the version check) |
| `storage-node delete_fence::closing_the_fence_waits_for_deletes_in_progress_even_cancelled_ones` | a close is acknowledged only after a delete held 800 ms inside the store's tombstone write finished, also when its RPC was cancelled; **fails with the Phase 3.5 guard placement** (acknowledged after 10 ms) |
| `storage-node delete_fence::an_epoch_raise_is_acknowledged_only_after_deletes_past_the_old_fence` | the same for an epoch raise (the end of the tombstone write before the acknowledgement) |
| `storage-node delete_fence::the_fence_survives_a_node_restart` | closed stays closed, open stays open across restarts |
| `storage-node delete_fence::only_a_later_incarnation_or_a_release_lifts_the_hold` | an older incarnation (not adopted) cannot lift a hold; the held one needs the release; a release names its generation (a delayed one does not lift a later close's hold), never opens; a generation no cluster reaches is ignored; **fails with the first hold rule** |
| `storage-node delete_fence::quarantined_shards_beyond_one_page_are_listed` | a store written without the node (no facts): held; a release without the start's token or with a wrong one does not lift it, with it does |
| `storage-node cluster::tests` (5) | persistence and monotonicity; damaged lines, lost file with data, unknown keys; a close whose persistence failed is not acknowledged until it is on disk; the version-2 file checked whole (every truncation, an edit, header loss after migration), headroom and jumps, the token, an unknown hold persisted and restarted |
| `storage-node mtls::every_rpc_is_authorised_by_role` | a gateway or node identity cannot open, close, hold or release the fence; controller and admin can |

## Fault injection (§4)

| Scenario | Test |
| --- | --- |
| Phase change right after the delete decision / after the RPC was sent | `deletes_decided_before_a_phase_change_are_refused_after_it` (decided while Active, delivered after `set-read-only`, and after Active again under a new generation: refused by the fence; deletes decided under the new one apply) |
| Fence change right after the RPC is accepted / before the physical delete | `closing_the_fence_waits_for_deletes_in_progress_even_cancelled_ones` (held inside the tombstone write); an RPC accepted but not yet past its checks waits for the shared guard and sees the new fence |
| Controller killed while updating the fence | `a_leader_killed_while_pushing_its_fence_is_superseded_cleanly` (one node took the new generation; the next leader opens above every node; an interrupted close completes when re-run) |
| Storage node killed while updating the fence | the facts file is replaced atomically (temporary file, fsync, rename, directory fsync) and checked whole at start; `the_fence_survives_a_node_restart`, `a_close_that_failed_to_persist_is_not_acknowledged_until_it_is` |
| Pre-restore RPC delivered after the restore | `a_delete_decided_before_a_restore_never_removes_what_the_restored_metadata_needs` (refused by the fence, checked by reason), `without_closing_the_fence_first_the_restored_object_loses_its_shards` (the race, reproduced), `system_model` (`Restore`) |
| Former leader's request after the new leader | `a_former_leaders_late_deletes_are_refused`, `system_model` (`FormerLeaderGc`, `LeaderChange`) |
| New write and old delete of the same shard id | record tokens (Phase 3.5): `system_model` reconcile and cache paths; chunk ids are never reused for GC deletes (RFC-008 §6.6) |
| Old delete right after a rebalancing switch | `suite_rebalance::an_expired_move_never_deletes_its_successors_shards`, `a_read_started_before_a_move_follows_the_chunk`, load run below |
| Old delete right after a repair | `repair_completion::delete_decided_under_an_old_epoch_is_rejected_after_a_fence` |
| Partition and return | `a_node_that_missed_the_close_is_reported_and_closed_once_back` (a stopped node misses the close, is reported, closes once back), `system_model` (`NodeRestart`) |
| Duplicated and reordered RPCs | `a_former_leaders_late_deletes_are_refused` (duplicate), `system_model` (`DeliverDelayed`: shuffled batches) |
| Timeout / commit unknown | a push that times out is not acknowledged (listed by `close`); fence records are written in one transaction each and re-runnable; `capacity::failover_while_writes_are_refused_with_gateway_and_node_restarts` (no capacity left held) |
| Partial fence update | `a_node_that_missed_the_close_is_reported_and_closed_once_back`, `a_leader_killed_while_pushing_…` |
| New leader on restored metadata, restore without the close | `a_new_leader_never_reopens_a_nodes_own_generation_after_a_restore` (**fails without the first-round rule**) |

Mutation checks (protection removed, test expected to fail): the node's
fence check removed → 4 of the 6 cluster scenarios and the system model
fail; guard moved back into the handler → the cancelled-delete test
fails; the first hold rule → the hold test fails; the first-round rule
removed → the restored-leader test fails; compaction output under its
segment name → the torn-output test fails; the gateway's admission read
at start removed → the S12 test fails.

## Model-based testing (§7, S13)

`system_model` adds to the Phase 3.8 operations: a metadata backup and
its restore through the recovery procedure (fence closed everywhere with
the hold, restore, every held delete delivered, `recovery begin`,
verification, finalize, a new leader term reopens), a phase flip
(ReadOnly, fence closed, held deletes delivered, Active, new term), a
leader change, a node restart, and deletes decided at DELETE time, held,
and delivered later in shuffled order. While a backup is pending nothing
removes data and every delete is held; the model checks every key after
every step (invariants A–C during the run, not only at the end), and at
the end that GC and reconciliation converged (F: durable shards equal
what live objects need, no capacity held).

Runs at the final code: 3 × 20 seeds × 100 steps, all passing (one seed
takes 2–6 s); through the phase about 160 seed runs in all. With the node's
fence check removed the model fails (a restored key reads nothing) and
records the seed in `target/tmp/system-model-failures.txt` for replay
(`ARCA_MODEL_SEED`). Not done: the reduced run on FoundationDB (the
model drives an in-process cluster; on FoundationDB only the dedicated
suites run).

## Rebalancing on real FoundationDB (§5, S11)

- **CI:** the eight `suite_rebalance` cases (moves under concurrent
  GET/PUT/DELETE and GC, dry run and pause, cursor resume after a leader
  change and a crashed move, repair first and the target's reserve, a
  read following a moved chunk, a target epoch change, a chunk larger
  than the budget, an expired attempt never deleting its successor's
  shards, the pacing) run on FoundationDB in the CI job
  (`rebalance_fdb::`).
- **Load runs** (`benchmarks/linux/rebalance-load.sh`): two VMs,
  private `fdbserver`, six storage nodes in two 2+1 groups of one tier
  (3 GiB allotment each), 320 × 8 MiB objects filled into group 1, then
  group 2 added. Then 12 minutes of verified PUT (8 × 8 MiB per
  iteration), verified GET (64 objects) and DELETE (the iteration before
  last) with rebalancing paused, then 12 minutes with it running: the
  leader is killed at 4 minutes (restarted after 30 s) and a node is
  stopped for 20 s at 8 minutes. Convergence follows, then a verified
  read of every object that must exist.

| | run 1 (before the pacing fix) | run 2 (after) |
| --- | --- | --- |
| iterations, failures | 601, 0 | 596, 0 |
| body mismatches, acknowledged objects missing | 0, 0 | 0, 0 |
| verify of the 320 base objects | 320/320, 0 errors | 320/320, 0 errors |
| chunks moved | 1 093 (4.27 GiB) | 309 (1.21 GiB) |
| plan direction | reversed (2 → 1) at the end of the window | never reversed |
| settled | `balanced` 1.5 min into convergence | at once |
| per-node live bytes, group 1 / group 2, after | 638 / 642 MiB | 666 / 622 MiB |
| reserve crossed | never (lowest available 989 MiB, reserve 774) | never (lowest 1 023, reserve 773) |
| leader change | ctl-b continued the cursor, fence generation 2 opened | the same |
| PUT throughput, paused → rebalancing | 134.9 → 89.0 MiB/s | 125.7 → 69.0 MiB/s |
| PUT runs with refusals (`SlowDown`), paused → rebalancing | 0 → 65 of 311 | 0 → 126 of 314 |
| GET throughput, paused → rebalancing | 519.5 → 633.5 MiB/s | 492.9 → 647.2 MiB/s |

The overshoot of run 1 was a defect of the planner (each pass planned
from rooms that do not show a source's space until its old copies are
reconciled and compacted); fixed in 15a9952 (a plan's need is computed
once, then the tier settles). Writes were refused for about a minute
twice in run 2 (`refusals.txt`): from 18 s before the leader kill (the
former leader's decision, inherited by the new term at 32.7 % usable
free), and while a node was stopped and the target had been filling
(both groups below the placement threshold). That is R-93: rebalancing
does not keep the target above the placement threshold. Refusals are
`SlowDown`, retried by clients; no data was lost and no reserve crossed.

Not covered by the load run: tiering and repair in competition with
rebalancing (in-process only), a destination at its reserve (in-process
only), an intent expiring under load (in-process only), more than two
hosts, physical storage. The throughput comparison mixes the cost of the
moves with the throttling they cause; it is one run per configuration.

## Capacity pressure and leader failover (§6, S12)

| State | Leader kill | FDB outage | Controller restart | Gateway restart | Node restart | Delayed record | Reports missing / back |
| --- | --- | --- | --- | --- | --- | --- | --- |
| normal | `a_new_term_keeps_the_published_state_until_every_member_reported` (round 0) | `stale_or_missing_capacity_reports_fail_closed`; R-85 tests (Phase 3.8) | as leader kill | — | — | `only_newer_records_of_this_incarnation_lift_a_refusal` | `a_new_term_…` |
| warning | `a_new_term_…` (round 1, added) | as above | as leader kill | — | — | as above | `a_new_term_…` |
| throttled | `a_new_term_…` (round 2) | as above | as leader kill | — | — | as above | `a_new_term_…` |
| write_rejected | `a_new_leader_rebuilds_the_admission_state_from_fresh_reports`, `failover_while_writes_are_refused_with_gateway_and_node_restarts` (new) | `fdb_failover` (CI) | as leader kill | **new test** | **new test** | **new test** (the leader's own earlier `normal` record delivered late is ignored) | `a_new_term_…` (round 3) |
| recovery_only | `a_new_term_…` (round 4) | as above | as leader kill | — | — | as above | `a_new_term_…` |

The new test found a defect: a gateway read the admission record only at
its first refresh, 2 s after start, so a gateway restarted while new
data was refused admitted writes until then (the nodes' reserves still
refused what would cross them). Fixed in 80887b7; the test failed before
the fix (the PUT was accepted). It also showed that `fdb_failover` had
always been in `write_rejected` at the point where it writes after a
gateway restart (the default placement threshold exceeds that test
cluster's allotments; fixed in the test's configuration).

Checked in every case: a new leader rebuilds the state from fresh
reports and never relaxes on partial ones; no write crosses a reserve
and no capacity stays held after the writes end (`assert_reserves_kept`);
writes resume once the space is back. Not verified: a leader kill during
`warning`, `throttled` or `recovery_only` with real processes (the
in-process term tests cover the state machine), and an FDB outage
combined with every state on real FoundationDB.

## Crash recovery (S9)

- Delete fence on the node: persisted before acknowledged (atomic
  replacement), checked whole at start (version 2 file with CRC-32C and
  a migration marker), damaged or lost facts on a node with data hold
  the fence closed until an operator releases it with the start's
  token. Tests above.
- Controller crash during a fence update:
  `a_leader_killed_while_pushing_its_fence_is_superseded_cleanly`.
- Storage engine, found by CI: a compaction killed while writing its
  output after the active segment had rolled over left a torn segment
  that the next open reported as corrupt (no data lost). Fixed in
  cdf1e8f (temporary output until synced);
  `io_faults::a_compaction_output_torn_before_its_sync_is_never_a_corrupt_segment`
  reported `(2, 64, "record truncated")` before the fix. `sigkill.rs`
  (SIGKILL during writes and compaction, CI) passes since.

## Operability (§8)

- `admin delete-fence status`: the record (generation, open or closed,
  operator hold, leader token, when, why), every node's fence and hold,
  how many nodes are at the record, and why deletes are stopped.
- `admin delete-fence close --reason R --yes [--absent-node ID]`,
  `admin delete-fence release --reason R`; no command forces deletes
  open.
- Metrics: `arca_controller_delete_fence_{generation,open,nodes_current,nodes_stale,refusals_total}`,
  `arca_node_delete_fence_{generation,open,hold,refusals_total}`; GC
  backlog as before; deferred and kept deletes (`kept_fenced`) in the
  reconciliation report.
- Runbook: RFC-013 §10.12 (normal operation, before, during and after a
  restore, failed updates, an unresponsive node, a stale node rejoining,
  GC backlog, damaged facts), §5.9 (restore, step 0), §5.8 (upgrade).

## Security and compatibility (§11)

| Question | Answer | Evidence |
| --- | --- | --- |
| Who may change the fence | controller and admin roles only (any fence field makes `Health` a fact push) | `mtls::every_rpc_is_authorised_by_role` |
| Who may delete | gateway, controller, admin (unchanged), each only under the node's open generation | `delete_fence` tests |
| An older controller (protocol 2) | its deletes are refused by every version-3 node | `an_older_protocol_delete_is_refused` |
| An older node | receives no delete from a version-3 client | `client.rs` (`FENCED_DELETE_VERSION`); `mixed_version` (CI) |
| Mixed-version upgrade | deletes pause per node until both sides speak version 3; never unfenced | RFC-013 §5.8; `mixed_version`, `upgrade` (CI) |
| Rollback | node to version 2: no deletes reach it; nodes and controllers to version 2: the pre-3.9 behaviour (R-88) returns | ADR-0023 Consequences |
| Missing token / defaults | an absent `delete_fence` decodes as 0, refused; absent Health fence fields change nothing | `deletes_need_…`, `an_older_protocol_…` |
| A wrong generation | refused | `deletes_need_…` |
| Restore privileges | closing and releasing need the admin role on the nodes and metadata access; `recovery begin` refuses unless every node acknowledged | CLI, `close_and_push` |
| A hostile or buggy node reporting a huge generation | ignored by controllers; nodes refuse jumps beyond 2^32 and opens in the closes' headroom | `the_facts_file_is_checked_whole_and_the_fence_keeps_headroom` |

## Independent review (§12, S14)

Two reviewers on different models read the changes from 439ca7a,
read-only, then re-read each set of fixes.

| Round | Reviewer A | Reviewer B |
| --- | --- | --- |
| 1 (at 15a9952) | 1 Critical (a close whose persistence failed acknowledged on retry), 3 High (phase change before the close; fence fields not authorized; damaged facts lose the floor), 4 Medium | 1 High (fence fields not authorized, plus generation overflow), 3 Medium (persistence; lost facts; a delayed release lifting a later hold), 6 Low, Info |
| 2 (at ff69929) | 1 Critical (a release delayed across a loss of facts), 2 High (a truncated file that parses; a fresh leader reopening a node's own generation), 2 Medium | no Critical or High; 1 Medium (generation space exhaustion), 1 Low (a new node restarted before its first push) |
| 3 (at a89a7f8) | no new Critical; 2 High (a node unreachable during the leader's first round; header loss looking like an earlier release's file), 3 Medium | — |
| 4 (at 04c7a4e) | no Critical or High apart from the accepted residual | — |

All Critical and High findings were fixed and confirmed in the next
round (ff69929, a89a7f8, 04c7a4e), except one that both the reviewer and
the maintainers classify as an accepted residual (R-90): it needs a
restore without the close **and** a controller started on the restored
metadata before `recovery begin` (two departures from the runbook)
**and** a node unreachable during that controller's first round of
reports. Where the reviewers rated a finding differently, the deciding
question was whether an execution order reaching data loss exists:
reviewer A's phase-change finding has none (a phase change does not
make a decision wrong; a restore needs its own acknowledged close), so
it was fixed as a reporting problem (`set-read-only` exits 1). Accepted
without change (Medium/Low, no data loss): a release that misses a node
is retried by the operator (shown in status); temporary compaction
output whose removal fails is removed at the next open; R-92 (clock
skew between restores).
