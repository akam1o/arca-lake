# ADR-0026: Restore-safe deletes (proposal to close R-94(b))

Status: option D **accepted and implemented** (Phase 3.11, `admin
restore`); option Q **proposed** for the next phase, not implemented and
not verified. R-94(b) stays open (High). Option D does not close it.

## Problem

R-94(b) is closed only by a mechanism under which a delete decided
before a restore cannot become the physical loss of data that the
restored metadata needs. ADR-0025 detects a restore when the leader can
observe it; it cannot cover:

1. a restore that is atomic for the leader (one-step replacement, a disk
   or storage snapshot rollback), which restores the same holder, token
   and incarnation;
2. a leader cut off from FoundationDB for the whole restore;
3. a restore done outside arca-lake, which nothing announces to the
   nodes;
4. deletes admitted before anyone observes anything.

Each of these was reproduced (ADR-0025 `boundary_*` tests). Every
existing identifier (term, token, incarnation, fence generation,
lineage counter value) is restored with the metadata, so none of them
can tell the storage nodes, at delete time, that the metadata they act
on has been replaced.

## Options

### D. Managed restore

`admin restore` (prepare, run, complete) as the only supported path:

1. close the fence with an operator hold on every registered node and
   require each node's durable acknowledgement; refuse to continue
   otherwise (an absent node must be unregistered first, R-100);
2. only then clear and run `fdbrestore` (or tell the operator to);
3. keep deletes forbidden until `recovery begin` and finalize established
   a new incarnation and a leader opened a new generation;
4. a restore done any other way is outside the guarantee, and the
   recovery procedure says how to handle it.

Most pieces exist (`admin delete-fence close` with a hold, `recovery
begin`'s node checks, ADR-0024's gates). With D used, the boundary of
ADR-0025 is empty.

D cannot be enforced. FoundationDB has no permission that lets
arca-lake refuse an `fdbrestore`, a DR switch or a volume snapshot
rollback by whoever administers the cluster. Tenants and token
authorization (7.1+) restrict clients, not the cluster's own backup and
restore tooling. Keeping the restore tooling away from day-to-day
operators (`backup_agent` and cluster-file access only for the arca-lake
admin role) is an operational control, not a technical guarantee. D
alone therefore does not meet the goal of tolerating a runbook not
followed.

### Q. History-confirmed retention on the nodes (recommended to close R-94(b))

The node already deletes by writing a tombstone; the data record stays
on disk until compaction reclaims it. Q makes that interval the safety
margin:

- **Tagged deletes.** Every delete carries the lineage step its batch
  was decided under (`L`, already advanced before every batch,
  ADR-0024) and that step's random history token `H` (new: drawn when
  the step is written, kept in a metadata log of recent steps). The node
  keeps the superseded data record, tagged `(L, H)`.
- **Purge only on confirmation.** Compaction may reclaim a retained
  record only when both hold:
  1. a leader push confirmed that the metadata history still contains
     step `L` with token `H` (the leader reads the step from the
     metadata log and sends it);
  2. the record has been retained for the retention window `Q`
     (node monotonic clock; configurable, e.g. 24 h).
- **Fork detection on the node.** A restored metadata store lacks the
  steps after its backup point. When its leader advances the lineage
  again, it writes those step numbers with new random tokens. A node
  holding `(L, H)` sees a different token for `L` (or no step) and
  knows its history was replaced. It closes its fence with an unknown
  hold and stops all purging. No observation by any leader is needed,
  and an atomic restore, a disk rollback or an external `fdbrestore`
  is detected the same way.
- **Undelete on recovery.** `recovery begin` / `recovery pass` restores
  the retained records that the restored metadata references (an index
  update, no data copy). Reconciliation later collects whatever is
  really garbage.

Guarantee: restoring a metadata backup loses no shard to a delete
decided after the backup point, provided the restore completes within
`Q` of that delete. This holds whatever path the restore took and
whether or not any leader observed it. Beyond `Q` the loss is
explicit, metered, metadata-backup RPO, the same as soft-delete
retention in object stores.

Costs and work:

- Capacity: retained bytes equal the delete rate times `Q`. They are
  counted as reclaimable. Under capacity pressure a node may reclaim
  confirmed records older than a lower bound `Q_min`. That shortens the
  guarantee and must be metered and alarmed, never silent.
- Protocol 5: `DeleteShard` carries `(L, H)`, and fence pushes carry
  confirmations.
- Storage format 3: tombstones carry `(L, H)`, and retention state is
  persisted.
- Schema 4: the metadata lineage log.
- Upgrade: after finalize, nodes refuse deletes without a history tag
  (ADR-0024 downgrade gate).
- Compaction rule change. Recovery undelete. Tests for atomic restore,
  disk rollback, external `fdbrestore`, a cut-off leader, and capacity
  pressure.

### Compared

| | D: managed restore | Q: history-confirmed retention | ADR-0025 alone |
| --- | --- | --- | --- |
| Restore through arca-lake | safe | safe | safe once the leader observes it |
| External `fdbrestore`, DR switch, disk rollback | not covered | safe within `Q` | not covered |
| Atomic restore, cut-off leader | safe if D was used | safe within `Q` | not covered |
| Enforceable | no (operational) | yes (node-side) | n/a |
| Cost | small (CLI, checks) | protocol 5, format 3, schema 4, capacity `Q` | done |
| Closes R-94(b) | only for restores that follow it | yes, bounded by `Q` (stated and configurable) | no |

## Option D as implemented (Phase 3.11)

`admin restore plan | run | finish | status`
(`controller::managed_restore`) reuses the runbook's functions:
`close_and_push`, `restore_floors`, `mark_restoring_above`, the
verification pass and `finalize`.

1. **plan.** Checks the cluster:
   - cluster state `Active` or `ReadOnly`;
   - no open upgrade finalize;
   - every node answers, except removed nodes, which were fenced when
     they were unregistered.

   It refuses to start otherwise. It shows the target cluster
   (incarnation, nodes), the source (backup URL, version) and the
   destination. It writes a journal outside the metadata store, with a
   confirmation token. Nothing in the cluster changes.
2. **run --confirm TOKEN.** Refuses a wrong token, or a store that is not
   the planned cluster. Then:
   - freezes: `ReadOnly`, and no controller or gateway heartbeat fresh
     (or the process is declared stopped);
   - closes the fence with an operator hold on every node; each node
     acknowledges after the deletes it admitted finished, and each
     node's state is read back. Any node missing stops the procedure
     before anything is restored;
   - checks for processes again, then clears arca-lake's keys and
     restores;
   - runs `recovery begin`: the fence is closed again on the restored
     metadata, a new incarnation is marked above every node's sequence,
     and the schema floor is raised. A mark made by a run that crashed
     before recording it is adopted, not made twice;
   - runs the verification pass.

   Each step is recorded in the journal once done. A failed or
   interrupted run resumes from the first step not recorded. A failed
   restore resumes from the clear.
3. **finish.** Finalizes on the report (the operator's decision;
   critical findings need `--force --reason`), then waits until a leader
   opened a generation above the closed one on every node.

Review fixes (Phase 3.11):

- **Clearing step.** A step is recorded before the clear commits, so a
  crash between the two resumes. Processes are checked before every
  clear, and again on the restored metadata before the mark, where a
  running process refreshes its heartbeat.
- **Node check.** Before the mark, the restored metadata must name only
  this cluster's nodes, at their addresses. Another cluster's backup is
  refused.
- **Removed nodes.** Nodes removed before the restore are marked removed
  again.
- **Declared absences.** The mark and `finish` take `--absent-node`
  (the operator's statement, as for `recovery begin`).
- **What `finish` waits for.** The restored metadata's nodes in service.
- **Identities.** The plan records the identities the nodes
  acknowledged, and the mark puts them back on the restored metadata.
  Nothing is imported from the nodes.
- **When identities are taken.** Right before the clear, after the
  freeze, and again on a resume while the planned store is still there
  (the clear did not commit), so that a fresh rejoin since the plan is
  not undone (confirmation review).
- **Removed nodes' addresses.** The plan records them. A restored record
  of a removed id at another address is refused, and the restore's fence
  and floor RPCs skip removed nodes.
- **Accepted (re-review).** A process running against the empty store
  between the clear and the restore writes nothing: a controller cannot
  take leadership, guarded writes are refused, and heartbeats are
  skipped. It becomes visible on the restored metadata at its next
  heartbeat, and the check before the mark stops the run. A process
  started less than one heartbeat interval before the mark is not seen.
  It runs under the restored incarnation, which the mark replaces, so it
  is refused afterwards. A backup taken less than 90 s before the
  restore holds heartbeats that look fresh: name those processes with
  `--stopped`.
- **Destination.** It is compared as the cluster file's path, not its
  contents. A command prefix (`--fdbrestore-cmd`) that resolves the same
  path to another cluster is not detected; the nodes stay held, so
  nothing is deleted, but the planned cluster stays cleared until the
  run is repeated correctly. This is a known limitation.

Guaranteed under these conditions (tests: `it/managed_restore.rs`,
`it/fdb_restore.rs::admin_restore_with_fdbrestore_resumes_after_a_failed_restore`
on real FoundationDB, each check failing its test when removed):

- No delete decided on the metadata before the restore applies after the
  fence step.
- No delete applies while the restore runs, or before finalize and a new
  generation.
- No step starts while a node's safety is unconfirmed or a process runs.
- A failed or interrupted run resumes without reopening deletes and
  without a second incarnation.

Not guaranteed by option D:

- **A restore done any other way.** A direct `fdbrestore`, a DR switch,
  or a disk or snapshot rollback of the store remains R-94(b) (High). The
  trip (ADR-0025) and the rejoin lease (ADR-0024) only mitigate it.
- **Processes declared stopped that are not.** Their writes make the
  restore fail ("non-empty destination"), which is safe, but their
  deletes are already refused only by the fence step.
- **Lost metadata.** When the current metadata is gone, the procedure
  refuses to plan, and the manual runbook applies with its weaker
  guarantee.

## Option Q: design audit (Phase 3.11; not implemented)

What Q relies on, and where that can fail:

1. **History tokens roll back with FoundationDB.** The metadata log of
   `(L, H)` is restored with the metadata. Steps up to the backup's
   lineage `L_r` come back with their original tokens, so a node finds
   them consistent. Only steps after `L_r` are written again with new
   tokens, and only those reveal the rollback. A delete decided under a
   step `L <= L_r` whose reference removal committed after the backup
   point is therefore not detected by the tokens at all. Only the
   retention window `Q` protects it. Consequence: the history check
   cannot replace `Q`. It can only stop purges early once a fork is
   seen.
2. **The same token reappearing.** Tokens must be drawn from the OS's
   random source at the moment the step is written, and never derived
   from metadata, from a counter, or from a seed stored in the store. A
   derived token is recreated identically by the restored history, and
   the fork is invisible. Two restores of the same backup then write
   different tokens for the same step numbers, with probability
   1 - 2^-128 per step.
3. **A restore later than `Q` after the delete.** The record may already
   be reclaimed, and the restored metadata's reference is then lost.
   This is explicit, bounded RPO and is reported by recovery. It is not
   prevented.
4. **An RPC delayed beyond `Q`.**
   - Retention counts from when the node applies the delete, so a late
     delete still gets `Q`.
   - A delete whose step the node already knows to be forked is refused.
   - A step that has aged out of the metadata log can never be
     confirmed, so its record would stay forever. An explicit rule
     (alarm, operator action, or reclaim after a longer `Q_max`) must
     bound this. Otherwise the invariant "old data does not linger
     needlessly" fails.
5. **Storage node crash and restart.** The tag must sit in the
   tombstone itself (one record, atomic). The node's map of seen
   `(L, H)` must be persisted. Loss or damage of either must fail
   closed: no reclaim until the tag is confirmed again.
6. **Snapshot rollback of a node's disk.** The node's retained records
   and its memory of steps roll back together. Deletes after its
   snapshot point are undone, so the data is back, which is garbage,
   not loss. Its memory of later steps is gone, so it may accept the
   restored history as consistent. That is safe for its own data,
   because nothing it reclaims was deleted after its snapshot point.
   A disk rollback also undoes writes, which is a durability matter
   (erasure coding across nodes), outside Q.
7. **Nodes not reached.** They get no confirmation, so they reclaim
   nothing; that costs capacity, not safety. One unreachable through a
   restore sees the fork when it returns.
8. **Leader change during confirmation.** A confirmation must be read
   from the metadata in the same act. A push built before a restore and
   delivered after it confirms the old history, and the node cannot tell.
   This matters only for records already older than `Q`, which are
   beyond the guarantee anyway. A leader cut off from the store cannot
   build a confirmation; replaying an earlier one must be impossible by
   construction (confirmations carry the confirming step).
9. **GC backlog and the capacity reserve.**
   - Retained bytes equal the delete rate times `Q`. They are counted as
     reclaimable but not free.
   - Admission sees less free space and throttles writes earlier.
     Compaction cannot reclaim segments that hold retained records, so
     write amplification rises.
   - Under pressure the choice is to shorten `Q` (a smaller guarantee,
     metered and alarmed) or to refuse writes. A silent shortening is
     not acceptable.
10. **Segment format and schema.** Tagged tombstones need segment
    format 3, the lineage log needs schema 4, and the tags need
    protocol 5.
    - Until finalize, deletes from earlier controllers are untagged and
      unprotected.
    - After finalize, untagged deletes must be refused.
    - Rolling nodes back past format 3 becomes impossible.
11. **Restores from outside the cluster.** Q needs no observer: reclaim
    needs a confirmation, and a restored history cannot produce one for
    the forked steps. The guarantee still holds only if:
    - the restore completes within `Q` of the delete;
    - the node still holds the record (no disk loss or replacement);
    - nobody shortened `Q`.

    A restore of the metadata together with every node's disk (a whole
    cluster snapshot) is consistent and needs no protection.

**When every source of history rolls back.** If the metadata and a
node's disk both roll back, the node's deletes after its snapshot point
are undone with them, so nothing is lost. The fork is undetectable only
when the node's memory of steps is lost but its deletes stand (damage
of that memory). Q then rests on the retention time alone, which is
why item 5 must fail closed.

**Conditions Q cannot guarantee:**
- a restore later than `Q`;
- a lost or replaced node disk;
- a shortened `Q`;
- untagged deletes before finalize;
- a damaged step memory without fail-closed handling.

Q must therefore be presented as "restore-safe within `Q`". It is not an
unconditional closure of R-94(b). Whether that bound is sufficient is a
decision for the next phase. Proofs and model tests for items 1, 4, 5
and 8 are needed before implementation.

## Recommendation

Do both:

- D now. It is cheap and makes the supported path safe.
- Q evaluated again in the next phase, against the audit above. It is
  implemented only if its bounded guarantee is accepted and its proofs
  hold. It changes the protocol, the storage format and the schema, and
  it adds capacity cost. Until then R-94(b) stays High. The supported
  restore path is D, and its guarantee is stated as "restores done
  through `admin restore`".
