# ADR-0026: Restore-safe deletes (proposal to close R-94(b))

Status: **proposed** (Phase 3.11). Not implemented. Needs a decision on
scope and phase.

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

## Recommendation

Do both:

- D now. It is cheap and makes the supported path safe.
- Q to close R-94(b), as its own phase. It changes the protocol, the
  storage format and the schema, and it adds capacity cost. Until Q
  ships, R-94(b) stays High. The supported restore path is D, and the
  guarantee is stated as "restores done through `admin restore`".
