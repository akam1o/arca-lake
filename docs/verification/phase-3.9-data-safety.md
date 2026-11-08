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
