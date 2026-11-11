# ADR-0028: Node-enforced deferred reclamation (R-94(b))

Status: **accepted** (Phase 3.12). Supersedes ADR-0026 option Q as the
candidate for R-94(b). ADR-0025 (trip wire), ADR-0024 (rejoin lease,
metadata-behind gate) and ADR-0026 option D (`admin restore`) stay in
force.

## Context

R-94(b): a delete decided on metadata that a restore (or any rollback of
the metadata store) later replaces can still be applied to a storage
node, and destroy a shard that the restored metadata needs. Phase 3.11
showed that no observer closes it: a restore between two observations is
not seen, and a leader cut off from the store applies what it already
decided (`boundary_*` tests).

Facts the design rests on (Phase 3.12 survey):

- A physical shard is the record `(group, chunk, index)` at a
  `sequence` on one node. The key is reused (a chunk moved A→B→A is
  written to A again), but every write gets a new sequence.
- The shards whose loss matters are not only those of objects the user
  deleted after the backup point (documented RPO: reported unreadable).
  Relocations are worse: a migration, a rebalance move or a member move
  leaves the former placement to be deleted by reconciliation, with no
  time recorded; restored metadata from before the move needs exactly
  those shards, for objects that were never deleted.
- Only GC records when something became unneeded (`since_ms`).
  Migration, rebalance and reconciliation record nothing.
- Nodes cannot read the metadata store, and do not know a group's role.

## Failure model

| Fault | What changes | What a node can observe |
| --- | --- | --- |
| FoundationDB restore (direct `fdbrestore`) | metadata back to the backup point `b` | nothing until a leader of the restored metadata pushes |
| Namespace clear | metadata gone (then restored, or not) | nothing |
| Snapshot rollback of the store, DR failover | as a restore, to the snapshot or replica point | nothing |
| Leader running through the restore | it keeps deleting what it decided before | its pushes and deletes, unchanged |
| Leader cut off from the store | it cannot read or commit; what it decided it may still send | as above |
| Storage nodes partly unreachable | some nodes miss pushes | only its own view |
| Delete RPC delayed, duplicated, reordered | an old decision arrives late | the RPC |
| Controller or node crash | in-memory state lost | its persisted state |
| Consecutive restores | as above, repeatedly | as above |
| Delete after the backup point | the backup still references the shard | — |
| Mixed versions | an earlier release's deletes, or nodes | the RPC's fields |

## Invariants

- **A.** No shard needed by metadata that is current at any time is
  physically removed because of a decision made on metadata before that
  metadata became current (a restore included).
- **B.** A delete RPC of an earlier term, incarnation or metadata history
  is not *physically* applied on the strength of that decision alone.
- **C.** A node physically removes a durable shard only when the
  reclaim condition (below) holds for that exact record.
- **D.** Safety does not depend on any counter, token or generation kept
  in the metadata store (all of them roll back with it).
- **E.** A node acts only on its own records; an unreachable node, or a
  reachable one, never loses data because another did not answer.
- **F.** After transient faults, deletion and reclamation resume and
  converge (no record retained forever while it stays unneeded and the
  controllers run this release).

## What cannot be guaranteed

1. **Arbitrarily old restores.** A restore of a backup older than the
   retention `R` can need shards reclaimed long ago. No finite retention
   protects it; such a restore loses what was reclaimed (reported by the
   recovery pass, never silently).
2. **A total, unobserved rollback.** If the metadata *and* every node's
   disk roll back together, nothing distinguishes before from after. That
   restores a consistent past state, so no data of that past is lost; but
   nothing in this design can detect it.
3. **Clocks beyond the stated skew.** The bound uses wall clocks.
4. **Nodes of earlier releases.** They delete physically on `DeleteShard`.

## Options

**A. Extend fence and lineage.** Every generation, term, incarnation,
opener and lineage value is stored in the metadata store or derived from
it. After a restore they come back with their old values, so a node
cannot tell a restored history from the one it saw, and an old leader's
deletes carry values that are valid for the node. The extensions already
made (ADR-0023/24/25) stop *new* leaders on restored metadata and a leader
that *observes* the restore. They cannot stop a decision already made or
a leader that does not observe it. Rejected as a closure; kept as layers.

**B. Rollback-resistant restore epoch.** A counter outside the metadata
store (on the nodes, or on a separate service) that a restore must raise.
A direct `fdbrestore` raises nothing, since the tools do not know of it.
The epoch then only helps when an operator or a tool raises it, which is
what `admin restore` already does with fences and holds. Nodes already
keep the highest generation and sequence they saw, so restored metadata
is detected when its leader pushes; the old leader's decisions are not
detected. A separate service would also add a new dependency whose own
loss, rollback or partition needs the same analysis. Rejected.

**C. Revalidation before the physical delete.** The node (or the
controller just before the RPC) checks the current metadata. Nodes have
no metadata access by design. A check by the controller has a window
between the check and the delete in which a restore can happen
(time-of-check to time-of-use); when the store is down nothing can be
checked, and the delete is either refused (no progress) or allowed
(unsafe). Rejected alone; used inside D, where the window is made
harmless by time.

**D. Deferred reclamation (chosen, in the form below).** ADR-0026's
option Q confirmed reclaims with history tokens, which roll back with the
store (its audit, item 1). The form chosen here uses no token: the node
keeps the record readable for a retention `R` after the delete, and
reclaims only on a confirmation whose metadata observation is at least
`R` after the node retired the record.

| | A | B | C | D (chosen) |
| --- | --- | --- | --- | --- |
| Safety against a direct restore | no | no | no (TOCTOU) | yes, for backups younger than `R` |
| Liveness | unchanged | unchanged | stalls when the store is down | reclaim delayed by `R` |
| Fault tolerance | — | new dependency | store availability | none added |
| Operational requirements | runbook | raise the epoch | — | `R` ≥ the oldest backup restored, clock skew ≤ `ε` |
| Capacity overhead | none | none | none | deletes over `R` stay on disk |
| Performance overhead | none | small | a read per delete | one sidecar append per delete; a reclaim pass |
| Protocol | — | new field | — | new RPC, additive fields |
| Storage format | — | — | — | none (sidecar log; segment format 2 unchanged) |
| Migration | — | yes | — | none: nodes defer by themselves |
| Testability | yes | partial | hard | deterministic (time is injected) |

## Decision

### Mechanism

1. **Retire (node).** On a 3.12 node with retention `R > 0`, a
   `DeleteShard` of a durable shard (all checks of today: incarnation,
   fence, epoch, identity, sequence token) does **not** remove the record.
   The node appends `(key, sequence, retired_at = node clock,
   incarnation of the delete)` to a sidecar log (`pending-reclaim.log`,
   fsynced before the reply) and keeps the record readable. This holds
   whatever sent the delete: an earlier release's controller, a stale
   leader, a gateway. A record already retired at that sequence keeps its
   first retirement time if the new delete carries the same incarnation;
   under another incarnation its retirement starts again (time and
   incarnation of the new delete).
2. **Reclaim (controller → node).** A reclaim pass of the leader lists
   the pending records of every node. For each, it stamps `t_c` (its
   clock, *before* any metadata read), then checks in the metadata that
   the shard is unneeded (the same attribution reconciliation uses: no
   live chunk places this chunk on this group at this index). If so it
   sends `ReclaimShard(key, sequence, observed_at = t_c)` under the same
   fence, incarnation and identity checks as a delete. A record it finds
   needed has its retirement ended (`keep`; the record stays, a later
   delete retires it anew), so that a record needed again through a
   restore does not stay retired for good.
3. **Reclaim (node).** The node removes the record only if a pending entry
   with exactly that sequence exists, the newest record of the key is
   still that sequence, the reclaim carries the incarnation the entry was
   retired under (and the node's, as for any delete), and
   `observed_at ≥ retired_at + R` with `observed_at ≤ now + ε`. Otherwise
   nothing changes.
4. **A new write** of the key (a new sequence) supersedes the pending
   entry. A node restart replays the log and drops entries whose
   sequence is no longer the newest.
5. **Cache groups.** A cache copy is discardable (the durable placement
   holds the data), so cache deletes stay immediate. The node learns a
   group's role from writes: a group written with the `Cache` class is
   cache until a non-cache write; a non-cache write makes it durable for
   good (persisted, sticky). An unknown group is durable.

### Safety argument

Assumptions:

- **H1.** Every restore, of any kind, is of a backup (or snapshot, or DR
  replica) whose point `b` satisfies `b ≥ t_x − (R − 2ε)`, where `t_x` is
  when the restored metadata becomes current.
- **H2.** Clocks of controllers, nodes and the backup system differ by
  at most `ε`.
- **H3.** Every node holding durable shards runs this release with a
  retention of at least `R`.
- **H4.** Within one cluster incarnation, a key needed again after it
  became unneeded is written again (a new sequence). (Migration,
  rebalance, repair and gateway retries all write; only a restore brings
  back a need for the same record.) A member move that makes a former
  member a member again counts its retired record as absent and writes
  the shard again (`ShardEntry.retired_any`; Phase 3.12 review: it took
  the retired record as present, needed it unchanged, and a later move
  away let it be reclaimed on its first retirement's age).
- **H5.** Restored metadata is used for deletes and reclaims only under a
  new cluster incarnation (`recovery begin`, `admin restore`). New
  leaders on restored metadata are held back until then by the
  metadata-behind gate (ADR-0024), and a leader that observes the
  restore stops by the trip wire (ADR-0025). A leader that has not yet
  observed it is the exception (below).

Claim: a reclaimed record `(k, s)` is not needed by any metadata that is
current at or after the reclaim.

Sketch. A reclaim of `(k, s)` under incarnation `I` needs a retirement of
`(k, s)` by a delete of `I` (step 3). Let `t_p` be that retirement's time
(that of `I`'s first delete of it), `t_I` the time `I` began,
and `t_u ≤ t_p` the time `k` stopped being needed in `I`'s history before
the delete that retired it. The reclaim's observation at `t_c ≥ t_p + R`
(within `ε`) saw `k` unneeded in `I`'s history.

- *Within `I`.* From `t_I` on, `I`'s history is one history (H5: a
  restore starts a new incarnation). If `k` were needed again in it
  between `t_u` and `t_c`, that would be through a write (H4), which
  supersedes `(k, s)`; so `k` at `s` is unneeded in `I`'s history from
  `t_u` on, and after `t_c` as well.
- *A restore after the reclaim, of a backup of `I`'s history.* By H1,
  `b ≥ t_x − R + 2ε > t_c − R + 2ε ≥ t_p + ε ≥ t_u`: the backup is of a
  point where `k` at `s` was already unneeded.
- *A restore after the reclaim, of a backup of an earlier incarnation's
  history* (taken before `t_I`): `t_p ≥ t_I > b`, and
  `t_x > t_c ≥ t_p + R > b + R`, so H1 does not hold: outside the
  protection.

The earlier form of this sketch (before the Phase 3.12 review) let a
retirement of one incarnation be reclaimed under another. It failed for
consecutive restores: a record retired, needed again through a first
restore at the same sequence, unneeded again later in the restored
history, could be reclaimed on its first retirement's age before a
second restore of a backup taken in between. Binding the retirement to
its incarnation makes every reclaimable retirement start after the
incarnation began. A delete decided on the restored metadata (GC,
reconciliation) retires the record again under the new incarnation;
reconciliation re-deletes what is retired under an earlier one
(`ShardEntry.retired` flags only this incarnation's).

A stale confirmation (decided before a restore, delivered after it)
carries the earlier incarnation and is refused by the node like any
delete of it. A leader that cannot read the store issues none. An
earlier release's controller cannot reclaim (no RPC). Nothing in the
argument reads a counter of the metadata store (invariant D).

### Not covered

- Backups older than `R − 2ε` at the time they are restored (H1), and
  clock skew beyond `ε` (H2): the residual of R-94(b), now **bounded and
  explicit**. `admin restore` refuses such a backup unless accepted.
- Nodes of earlier releases (H3): they still delete physically. The
  leader reports whether every node defers
  (`arca_controller_nodes_not_deferring`); `admin restore` refuses to plan
  while one does not, unless accepted.
- H5's exception: a leader that keeps running through a direct restore
  without observing it, and operates on the restored metadata under the
  old incarnation, followed by a second restore of a backup taken of that
  restored history in between. A single direct restore is covered (its
  shards are retained, whoever deletes them); the consecutive case needs
  that leader's reclaim to fall between the two restores before its next
  health pass observes the first. Run `recovery begin` (or use
  `admin restore`) after any restore.
- `admin node keep-removed` / `unregister` do not change this: a removed
  node gets no reclaims.

### Persistent state

- Node: `pending-reclaim.log` next to the store, append-only lines each
  with its CRC-32C, fsynced before the change is acknowledged: `p` (a
  record retired, with its sequence and retire time), `x` (a retirement
  ended: reclaimed or superseded), `c` / `d` (a group written with the
  cache class / a durable class; durable is sticky). Compacted by a
  rewrite through a temporary file, fsync and rename. On replay a `p` of
  another sequence replaces the earlier one (a supersede that was not
  logged does not hide the newer retirement); a start drops retirements
  whose record is no longer the newest of its key.
  Damage fails closed: a line that does not check makes the state
  damaged, in which every group is durable, deletes are still retained
  and nothing is reclaimed (`reclaim_damaged` in health) until the
  operator removes the file; the retained records are then deleted again
  by reconciliation and GC.
- Metadata: none for the mechanism. `admin restore` adds a cluster uid
  key and a probe key (see the Phase 3.12 verification report).

### Protocol (additive)

- `DeleteShardResponse.deferred` (the record is retained).
- `ReclaimShard(key, sequence, store_epoch, observed_at_ms, group_epoch,
  delete_fence, incarnation, node identity)` and `ListPending(after,
  limit)` returning `(key, sequence, retired_at_ms, store_epoch)` and the
  node's retention. Both are authorized for the controller and admin roles
  only (ADR-0017 table), not gateways.
- `HealthResponse.reclaim_retention_ms` (42; 0: an earlier release, or
  retention off), `reclaim_pending` (43), `reclaim_damaged` (44),
  `reclaim_skew_ms` (45).
- An earlier release answers `UNIMPLEMENTED` to `ListPending`: the
  reclaim pass counts it in `arca_controller_nodes_not_deferring`.

### Upgrade, rollback

Nodes defer as soon as they run this release, with no finalize step and
no format change. An earlier controller's deletes are retained and wait
for a controller of this release to reclaim them. Rolling a node back to
an earlier release leaves its pending records live: the earlier release
deletes them again physically on its next delete (its own semantics).

### Capacity

Retained bytes are the bytes deleted over `R`; they stay counted as used
by admission (they are on disk). The operator shortens `R` only by
restarting nodes with a smaller retention, which narrows H1 explicitly
(logged at start, reported in health).

### Performance

One fsynced sidecar append per durable delete; one reclaim pass per
interval, a listing per node and an attribution read per pending record.
No change on PUT or GET.

### Tests

Deterministic, in process, with the retention short and time controlled
by the observation stamps: a restore to a point before a relocation with
the old leader's deletes and its confirmations delivered afterwards
(nothing lost); a confirmation younger than `R` refused; a rewrite
superseding a pending entry; a node restart replaying the log; a damaged
log failing closed; an earlier release's delete retained; cache deletes
immediate; each check removed makes its test fail.

## Consequences

- R-94(b) becomes bounded: protected for restores of backups younger
  than `R − 2ε` under H2 and H3, by the nodes themselves, whatever path
  the restore takes. Older restores and skew beyond `ε` remain the
  residual.
- Freed space arrives `R` later.
- `admin restore` keeps its own checks; it additionally refuses (at plan,
  and again right before the clear) a backup older than `R − 2ε`, of
  unknown age, or with nodes that do not defer, unless the operator
  accepts the loss explicitly (`--accept-unprotected-backup`, recorded in
  the journal).
- The node clock decides `retired_at` and bounds `observed_at`; RFC-008
  §6.7 ("safety must not depend on clocks") no longer holds for this
  mechanism. The dependence is explicit (H2) and confined to it: every
  other delete check stays clock-free.
