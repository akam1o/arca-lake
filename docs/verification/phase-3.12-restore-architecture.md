# Phase 3.12 restore safety architecture verification

Where each result comes from is stated with it:

- **in process**: the in-memory metadata store with real storage-node
  services and gateways in one test process, on a Linux validation VM
  (the developer machine's malware scan blocks test binaries at launch);
- **host A**: a validation VM running a private `fdbserver` (its own
  port and data directory; the host's system FoundationDB untouched),
  with `fdbbackup` / `fdbrestore` where a test says so, and real release
  binaries of Phase 3.10 (`3f67cb8`) and Phase 3.11 (`04e6fe4`);
- **CI**: the repository's CI jobs (Linux, FoundationDB in a container,
  the Phase 3.9, 3.10 and 3.11 releases built from their commits).

Nothing here was run longer than about two hours, on physical storage
other than the validation VMs' virtual disks, or on more than two hosts
(R-79, R-86). No 24-hour soak was run.

## 1. Outcome

R-94(b) is **bounded, not closed**. ADR-0028 (node-enforced deferred
reclamation) protects every restore, whatever path it takes (a direct
`fdbrestore`, a DR switch, a snapshot rollback, `admin restore`), of a
backup younger than the nodes' retention `R` less twice the clock-skew
allowance `ε`, under the hypotheses H1–H5 of the ADR. What remains is
explicit and observable:

| Residual | Why it cannot be closed here | What shows it |
| --- | --- | --- |
| Backups older than `R − 2ε` | any finite retention; reclaimed data is gone | `admin restore` refuses them unless accepted; the recovery pass reports what is missing |
| Clock skew beyond `ε` | the bound uses wall clocks (RFC-008 §6.7 departs here, for this mechanism only) | `--reclaim-skew-secs` is reported in health |
| Nodes of earlier releases | they delete physically | `arca_controller_nodes_not_deferring`; `admin restore` refuses to plan |
| H5 exception: a leader that keeps running through a direct restore, unobserved, operating on the restored metadata under the old incarnation, followed by a second restore of a backup of that restored history | no component observes the first restore in that window | none; run `recovery begin` (or use `admin restore`) after any restore |
| A total, unobserved rollback of metadata and every node disk together | indistinguishable from the past state | none (it restores a consistent past, losing nothing of it) |

## 2. Failure model and invariants

ADR-0028 §Failure model and §Invariants (A–F), §What cannot be
guaranteed. Each fault of the plan's §1.1 and where it is exercised:

| Fault | Exercised by |
| --- | --- |
| FoundationDB restore (direct `fdbrestore`) | host A `fdb_restore::a_direct_fdbrestore_after_deletes_and_a_move_finds_both_objects_whole`; in process `deferred_reclaim::*`, system model `DirectRestore` |
| Namespace clear | the same (the keyspace is cleared before `fdbrestore`) |
| Snapshot rollback, DR failover | as a restore of a snapshot: system model `DirectRestore`, `deferred_reclaim` (metadata replaced in one transaction) |
| Leader running through the restore | system model `DirectRestore` (GC and reconciliation on the metadata of before, applied unfenced); `deferred_reclaim::no_reclaim_removes_what_the_restored_metadata_needs` |
| Leader cut off from the store | it cannot read: it issues no reclaim; its deletes are retained (`deferred_reclaim::*`) |
| Nodes partly unreachable | reclaim acts per node only (invariant E); `reclaim_node` isolates errors per record and node |
| Delete RPCs delayed, duplicated, reordered | system model held deletes delivered after the restore; `retire` is idempotent per (sequence, incarnation) |
| Controller or node crash | `deferred_reclaim::rewrite_restart_and_damage`, `the_controller_reclaims_after_the_retention_across_a_restart` |
| Consecutive restores | `deferred_reclaim::a_retirement_of_an_earlier_incarnation_waits_for_a_new_delete` |
| Deletes after the backup point | every restore test above |
| Mixed binaries | host A and CI `previous_release::same_schema_matrix_with_the_phase_3_10_release` / `_3_11_release` |

## 3. Options compared

ADR-0028 §Options: A (fence and lineage), B (restore epoch outside the
store), C (revalidation before delete) and D (deferred reclamation)
across safety, liveness, fault tolerance, operations, capacity,
performance, protocol, format, migration and testability. A, B and C
are rejected as closures (A and B roll back with the store or depend on
a raise nobody makes on a direct restore; C has a check-to-use window
and stalls when the store is down); D is adopted in a token-free form.

## 4. The adopted design and what was found in review

The node retires a durable record on delete and removes it only on a
reclaim whose observation (stamped before the metadata reads that found
the record unneeded) is at least `R` after the retirement, of the same
sequence, by the same cluster incarnation that retired it. Details in
ADR-0028 §Decision.

Two corrections came out of this phase's own reviews:

1. **Consecutive restores** (self-review of the safety sketch). The first
   sketch let a retirement of one incarnation be reclaimed under another.
   A record retired, needed again through a first restore at the same
   sequence and unneeded again later in the restored history could be
   reclaimed on its first retirement's age before a second restore of a
   backup taken in between. Retirements are now bound to the incarnation
   of the delete; a delete under another incarnation restarts the clock.
   Test: `a_retirement_of_an_earlier_incarnation_waits_for_a_new_delete`
   (fails with the check removed).
2. **Observation stamped after the group read** (independent review,
   Critical). The reclaim pass read the groups once per pass and stamped
   each observation later, so a restore during the pass could be judged
   on the map of before. The pass now stamps once per page, then reads
   the groups and the attributions.

## 5. Implementation

| Part | Where |
| --- | --- |
| Retirement state, log, checks | `crates/storage-node/src/reclaim.rs` |
| Delete retires, write supersedes, `ReclaimShard`, `ListPending`, health 42–45, `ShardEntry.retired` | `crates/storage-node/src/service.rs`, `storage.proto` |
| Reclaim pass | `crates/controller/src/gc.rs` (`reclaim_node`), `process.rs` (`reclaim_pass`, `--reclaim-interval-secs`) |
| Node flags | `--reclaim-retention-secs` (86 400), `--reclaim-skew-secs` (60) |
| Metrics | `arca_controller_reclaimed_total`, `_reclaim_refused_total`, `_reclaim_errors_total`, `_nodes_not_deferring`, `_identity_unverifiable_total` |
| `admin restore` | destination probe, cluster uid, protection guard, `amend` (`managed_restore.rs`, CLI) |

No segment-format or schema change; protocol additions are additive
(earlier releases answer `UNIMPLEMENTED` or leave the fields unset).

## 6. Restore identity and path (plan §4.1)

| Case | Before | Now |
| --- | --- | --- |
| Symlink, relative path, `..`, bind mount, mount namespace, a container's own file, the same database through another path | the path string compared: an equal string could name another database, an unequal one the same | the destination is identified by content: a random probe is written through `admin restore`'s own connection right before every clear, and the restore tool must read it back through its own (`fdbcli`); nothing is cleared otherwise (`a_restore_tool_reaching_another_database_clears_nothing`) |
| The cluster file replaced at the same path | not seen | the journal records the cluster file's connection id (`description:id`); `run` compares it |
| A file replaced between the probe and `fdbrestore` | — | run locally, both read a private copy (owner-only) taken at the start of `run`; with `--fdbrestore-cmd` (another file system) the tool reads its own file, which the probe checks: the boundary is that file's owner. If the restore lands elsewhere anyway, the store `admin restore` uses stays empty and the run refuses before the mark; nodes keep their fence; a re-run resumes (availability, not data) |
| Another cluster's backup | refused only by node addresses | refused by the cluster uid right after the restore, before anything is pushed to the nodes; `admin restore amend` replaces the backup of the cleared store (`another_clusters_backup_is_refused_and_amended`); a backup without a uid (an earlier release's) by the node addresses as before |
| A backup outside deferred reclamation's protection | not checked | refused at plan and again right before the clear, as the nodes are then, unless `--accept-unprotected-backup` (recorded in the journal) (`plan_refuses_a_backup_outside_the_retention_unless_accepted`, `a_node_that_stopped_deferring_since_the_plan_stops_the_run`) |

Path normalisation alone was not enough and is not relied on.

## 7. Node identity (plan §4.2)

Audit of fence close, delete and reclaim against identity missing,
damaged, an earlier release, node or disk replacement, address reuse,
another cluster, certificate rotation. Fail-safe in every case with nodes
of this release. Changes made:

- **High, fixed**: an earlier release (which applies deletes whatever
  identity they carry) answering where a node acknowledged an identity
  was pushed the open fence. The health pass now asks first, facts-free,
  before every push to such a node; an earlier release gets no push, so
  no rejoin lease and no delete, until this release answers again. Reads
  and writes go on (`same_schema_matrix_with_the_phase_3_10_release`).
- **Medium, fixed**: `release_and_push` (operator hold release) went to
  removed nodes and carried no identity; it now covers active nodes and
  stamps acknowledged identities, like the close.
- **Medium, fixed**: `admin node adopt-identity` now refuses a disk of
  another incarnation or a later schema floor (as `rejoin`).
- **Medium, open**: the manual `admin delete-fence close` / `recovery
  begin` count acknowledgements without the managed restore's identity
  verification. `admin restore` verifies; the runbook directs to it.
- **Low, open**: health facts authorization does not cover
  `adopt_identity`; a channel created before its node id is noted is not
  pinned; no cross-check of a node's certificate name against its
  identity file; a damaged identity file has no repair path that keeps
  the data (`rejoin --fresh-identity` needs an empty disk).

Isolate, repair, re-register: `admin node unregister [--unreachable]`,
`admin node rejoin [--fresh-identity]`, `admin node adopt-identity`.

## 8. Previous-release CI (plan §5)

The CI job builds the Phase 3.9, 3.10 and 3.11 releases from their
commits. The 3.9 matrix (finalize, schema 2→3) is kept. A same-schema
matrix runs against 3.10 and against 3.11 (`same_schema_matrix`):
node-only upgrade (partial, with deferral on the current node and the
previous controller's deletes retained, never reclaimed by it),
controller-only upgrade (it reclaims what was retired), every node
current with the gateway previous, gateway upgrade and gateway-only
rollback, a node rolled back with retirements pending (3.11 shows its
identity and deletes at once; 3.10 gets no fence and deletes nothing),
`admin restore` refused while a node does not defer, back on this release
its retirements kept and reclaimed, every process rolled back and forward.
Removed-node rejoin and the managed restore with a previous-release node:
`a_managed_restore_and_a_removed_node_with_a_previous_release_binary`.
Delete fence, schema floor and segment format are covered by the 3.9
matrix (finalize raises them) and the same-schema matrix (no change).

Host A results: every `previous_release::` test passed with the real
3.10 and 3.11 binaries (the 3.9 one runs in CI).

## 9. Rebalancing convergence (plan §6)

Causes found:

1. **Units.** A group's room is its fullest member's headroom; a plan's
   need was half the room difference but was spent in logical bytes. A
   chunk of `len` bytes changes each member's room by one shard
   (`len / k`), so a plan moved `1/k` of what it needed, then settled for
   25 minutes; the skip test `len ≥ difference` also skipped chunks that
   would not invert the pair.
2. **Deferred reclamation.** The source's room returns `R` after a move
   (plus reconciliation and compaction). A plan made after the settle saw
   the source as full as before and moved the same room again, then back
   once it returned.
3. **Group versus node skew.** Groups sharing nodes report the same
   member headroom; a group with no data of its own can be the plan's
   source ("nothing to move", next plan). Rebalancing moves chunks
   between groups; it cannot even out nodes inside one group (placement
   and member moves do).

Changes: needs and moves counted per member (`member_bytes`, `inverts`);
what a move will give back to the source is counted as its room until it
shows (`RebalanceInTransit`, its own key, which earlier releases never
read; each entry keeps the room it was recorded against, so room that
came back is not counted twice; a time limit as a backstop). Client
writes keep their admission and reserve; targets keep the placement
threshold (unchanged `target_takes`).

Deterministic simulator (`rebalance::tests::convergence_simulator_compares_the_accounting`,
three groups, one at 90 % and two at 50 %, 120 simulated hours, the
same `plans` code):

| k | Retention | Writes | Earlier accounting: settled / reversals / moved | This release: settled / reversals / moved |
| --- | --- | --- | --- | --- |
| 2 | 0 | none | 371 min / 0 / 27.9 GiB | 213 min / 0 / 27.2 GiB |
| 2 | 0 | 8 MiB/min | 335 min / 0 / 27.0 GiB | 206 min / 0 / 26.9 GiB |
| 2 | 24 h | none | never / 127 / 344 GiB | 1637 min / 0 / 27.2 GiB |
| 2 | 24 h | 8 MiB/min | never / 125 / 289 GiB | 1602 min / 0 / 26.9 GiB |
| 4 | 0 | none | 756 min / 0 / 27.5 GiB | 422 min / 0 / 27.2 GiB |
| 4 | 0 | 8 MiB/min | 707 min / 0 / 26.7 GiB | 409 min / 0 / 26.8 GiB |
| 4 | 24 h | none | never / 106 / 234 GiB | 1805 min / 0 / 27.2 GiB |
| 4 | 24 h | 8 MiB/min | never / 115 / 215 GiB | 1768 min / 0 / 26.8 GiB |

"Settled": the actual imbalance below the start threshold for the rest
of the run; "moved": bytes per member. No run went below the placement
threshold. With a retention, convergence takes about the retention: the
source's room returns no sooner. Limitation (independent review,
Medium): the in-transit record infers what came back from the source's
net room; writes landing on the source at the same time can hide it
until the backstop expires. Not measured on real hardware this phase.

## 10. Background operations liveness (plan §7)

Audit of GC, reclaim, reconciliation, repair, stripe repair and scrub,
rebalancing, tiering, cache eviction, compaction, health and node
lifecycle against head-of-line blocking, starvation, unbounded retry,
cursor stagnation, intent leakage, capacity deadlock, failover, partial
failure, fairness and backlog convergence. Findings and status:

| # | Finding | Severity | Status |
| --- | --- | --- | --- |
| 1 | Reclaim: one metadata error stopped the pass for every later node | High | fixed: per-record and per-node isolation |
| 2 | Reclaim: one failing record stopped its node every pass | High | fixed: skipped and counted; a node is left only after 8 consecutive errors |
| 3 | Reclaim throughput (group list per record, sequential) | High | fixed: groups once per page, 16 records at a time |
| 4 | Node `ListPending` sorted every entry per page under the write path's lock | High | fixed: ordered map, range page |
| 5 | A failed replacement target or a second failure leaves a group `Repairing`, blocking rebalancing and drains | High | **open**: needs a re-targeting design (risk R-104) |
| 6 | An unrebuildable chunk pinned the member repair's cursor | High | fixed: queued for stripe repair, the walk goes on, the group stays `Repairing` |
| 6b | Found fixing 6: after the shards of a lost chunk were quarantined, a later walk and the completeness check did not see the chunk and the group became `Active` without it | High (pre-existing) | fixed: a queued task for the member's missing shard keeps it incomplete |
| 7 | Rebalancing settle shorter than the retention | High | fixed: in-transit accounting (§9) |
| 8 | A dangling pending-generation index entry stopped GC | High | fixed: cleared or skipped |
| 9 | Failure repairs unbudgeted, one group after another | High | fixed: `--failure-repair-chunks-per-pass` (4096) |
| 10 | Reconciliation re-deleted retired records every pass | Medium | fixed: `ShardEntry.retired` (this incarnation's only) |
| 12 | Reconciliation gave up a (group, node) at the first refused delete | Medium | open (R-104) |
| 14 | Tiering went on writing to a target at its reserve | Medium | fixed: the pass stops |
| 15 | Tiering forgot a generation whose moves all failed | Medium | fixed |
| 16 | Admission credits GC's logical reclaim rate though space returns `R` later | Medium | open (R-104) |
| 11, 13, 17–23 | reconciliation cursor, tiering cursor, GC backlog of unreachable nodes, health pass error isolation, planned-move budget on failover, drains waiting on unrelated repairs, rebalancing stopped by any non-Active group, epoch memo, stripe-repair task errors | Medium | open, documented (R-104) |
| 24–32 | scrub coder errors, cache repair waiting, local fence check before reads (fixed), stale pending entry (fixed), needed verdicts re-judged, intents expiring only in optional passes, compaction `break`, eviction paging, per-task failure gauge | Low | two fixed, the rest open |

## 11. Model-based testing and fault injection (plan §8)

- System model (in process and on FoundationDB): deferred reclamation on
  (`ARCA_MODEL_RETENTION_MS`, default 3 s), new operations
  `DirectSnapshot`, `DirectRestore` (the leader's GC and reconciliation
  on the metadata of before applied, the metadata replaced unfenced, held
  deletes delivered after it, then the recovery), `Reclaim`; at the end,
  after the retention, exactly the live objects' shards remain.
- Failing seeds saved: six seeds that lose a backup's object in a direct
  restore with deferral removed, replayed on every run
  (`recorded_direct_restore_seeds_agree_with_the_model`).
- Each mechanism removed fails a test (mutation runs, in process):

| Mutation | Caught by |
| --- | --- |
| no retention check (`observed ≥ retired + R`) | `deferred_reclaim::no_reclaim_removes_what_the_restored_metadata_needs` |
| deletes not deferred | every `deferred_reclaim` test; system model; recorded seeds |
| no sequence check | `reclaim::tests` |
| damaged log not failing closed | `reclaim::tests` |
| no metadata check before reclaim | `no_reclaim_removes_what_the_restored_metadata_needs` |
| no identity / incarnation / fence check on `ReclaimShard` | `reclaims_are_refused_on_identity_incarnation_and_fence` |
| no supersede on write | `rewrite_restart_and_damage` |
| cache role not learned / durable not sticky | `cache_class_deletes_stay_immediate_durable_is_sticky` |
| retirement not bound to its incarnation | `a_retirement_of_an_earlier_incarnation_waits_for_a_new_delete` |
| no destination probe | `a_restore_tool_reaching_another_database_clears_nothing` |
| no cluster uid check | `another_clusters_backup_is_refused_*` |
| no protection guard at plan / at run | `plan_refuses_a_backup_outside_the_retention_unless_accepted` |
| no queued-task completeness check | `repair_reports_corruption_instead_of_writing_mismatched_bytes` |

Not covered by a mutation run: the rebalancing in-transit record (the
simulator compares both accountings instead), the identity probe in the
health pass (covered by the 3.10 matrix, not mutated).

## 12. Independent review

Two reviews of the whole change by different models (one of another
family), read-only. Findings and resolution are in §4 and the table
below.

| Finding | Severity | Resolution |
| --- | --- | --- |
| Reclaim judged with a group map read before its observation stamp | Critical | fixed (§4) |
| A durable write's group role noted after the write | High | fixed: noted before, the write refused if it cannot be |
| Restore protection not re-checked against the nodes before the clear | High | fixed |
| Probe and restore can reach different files | High (rated Medium here: availability, not data) | pinned copy when run locally; boundary documented |
| Log file creation not synced at the directory | Medium | fixed |
| In-transit estimate inferred from net room | Medium | documented (§9) |
| A member moved away and back took its retired record as present (H4 broken within one incarnation; reclaimable on its first retirement's age) | High | fixed: member moves count a retired record as absent and write the shard again (`a_member_moved_away_and_back_writes_its_shard_again`, fails with it removed) |
| Group map read before the observation stamp (same as the first review's Critical) | Medium | fixed (§4) |
| Quarantined records never deleted on a deferring node | Medium | fixed: no live record, deleted at once |
| A torn or failed append marked the whole log damaged | Medium–High | fixed: an unterminated last line is cut on open; a failed append is truncated back |
| Retirements of an earlier incarnation in groups the metadata no longer has never reclaimed | Medium | fixed: the reclaim pass deletes them again under the current incarnation |
| Cache-group repair marked the group durable on the target | Medium | fixed: written with the cache class |
| Durable write's role noted after the write | Medium | fixed (first review) |
| Memory and lock scope of the retirement state on a busy node | Medium | open (R-106) |
| Full re-walk every pass while a chunk cannot be rebuilt | Medium | the extra cursor resets removed; the existing re-walk of an incomplete member remains (R-104) |
| Retention and skew not checked against each other | Low | fixed: a node refuses to start unless twice the skew is below the retention |
| Cluster uid lost on a restore of a backup without one | Low | fixed: put back at the mark |
| The backup point is the operator's statement | Low | documented |
| A put racing a delete can drop a retirement | Low | open (liveness only: reconciliation deletes it again) |

### 12.1 Re-review

Both reviewers re-reviewed the fixes (the same sessions, read-only).
Their earlier findings were confirmed fixed or partly fixed as stated
above. New findings in the fixes, all fixed:

| Finding | Severity | Resolution |
| --- | --- | --- |
| Member moves counting every member's retired records as absent: after a restore, a chunk retired on all members was not found, and a move completed without rebuilding it (both reviewers) | High | only the member being rebuilt: the others' retired records are still theirs and are sources (`after_a_restore_a_member_move_rebuilds_from_retired_copies`) |
| A needed record retired under an earlier incarnation stayed retired for good, re-judged every pass | Medium | the reclaim pass ends the retirement of a record it finds needed (`ReclaimShardRequest.keep`) |
| The quarantined-record delete could remove a record written meanwhile | High | the tombstone is conditioned on the quarantined record's sequence |
| The pinned cluster file could take the journal's own name | High | a unique name, created new, nothing replaced |
| An unterminated last line dropped without regard to an acknowledged durable role | High | kept if it checks; otherwise cut, and every cache role forgotten (a lost durable role is the one unsafe loss) |
| Directory sync skipped on a retry after its failure | Medium | retried until it succeeds |
| The protection re-check cannot see a node that was briefly at a shorter retention since the backup | Medium | documented (residual of H3) |
| A record retired then quarantined loses the retention | Low | accepted: quarantine follows a checksum failure; the copy is corrupt |
| An unknown group's first cache write marks it cache although it holds durable records (group id reused after a restore, or a log removed) | Low | open (R-106) |

A second re-review of these fixes is in §12.2.

## 13. Performance

One fsynced append per durable delete (and per role change); one listing
per node per reclaim interval and one attribution read per due record;
one extra health probe per push to a node with an acknowledged identity.
No change on PUT or GET beyond the role note on a group's first write of
a class. Freed space returns `R` later: capacity planning must hold `R`
of deletes (`reclaim_pending` in health). Not measured under load this
phase.

## 14. Not verified

- Physical storage and real fsync behaviour of `pending-reclaim.log`
  under power loss (only process crashes and file damage were tested).
- Clock skew between hosts (single-host tests share a clock).
- A DR failover with a real second cluster.
- Rebalancing convergence on real hardware.
- A 24-hour run.
