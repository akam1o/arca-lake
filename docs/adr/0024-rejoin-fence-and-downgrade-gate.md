# ADR-0024: Rejoin fence, metadata rollback gate, sequenced incarnations, downgrade gate

Status: accepted (Phase 3.10)

## Context

ADR-0023 made storage nodes the enforcement point of the delete fence.
Three gaps remained after Phase 3.9.

- **R-90.** The fence relied on the runbook and on reaching every node:
  1. A restore was done without `admin delete-fence close`.
  2. A controller was started on the restored metadata before `recovery
     begin`.
  3. A node was unreachable during that controller's first round.

  The controller could then reopen the generation that node still held,
  and deletes decided before the restore could be applied there.
- **R-92.** Nodes ordered incarnations by the cluster state's
  `changed_ms`, a wall clock of whichever host marked the restore.
- **Rollback.** A release without the delete fence (protocol 2) could run
  again on a store that had used it. Nothing such a release checks at
  start differed between a fenced and an unfenced cluster.

The design rule for this phase: safety must not depend on the runbook
being followed, on every node answering, or on a node already knowing
the new cluster state.

## Decision

### Rejoin fence (storage node)

An open fence admits deletes only while a lease runs on the node's
**monotonic** clock (`--fence-lease-secs`, default 30 s, kept in memory
only). The lease is (re)started by each push of that open generation
from a caller of the node's own incarnation, or from any caller while
the node has none.

- **A start** begins without a lease. The open generation persisted on
  disk is kept, but no delete is admitted until the leader's next push.
- **A partition, or a leader that stopped pushing** (crashed, fenced, or
  a cluster whose metadata was replaced) closes the node after the lease
  ends, without the node learning anything.
- **Opening** needs a caller of the node's incarnation, or a node that
  has none. A controller of another incarnation neither opens nor renews.
- **Normal operation** costs nothing extra. The leader pushes every
  health pass (5 s by default) and the lease is six passes long. A
  restart costs one pass of refused deletes, which GC defers and
  reconciliation keeps.

### Opener (metadata and nodes)

A leader treats an open generation as its own only if *this process*
opened it. The process draws an opener value at start, recorded with the
generation in `delete_fence_opener`. Matching only the term is not
enough: a restore brings the leadership counter back, so a new leader
could hold the same term number as the record and adopt a pre-restore
generation.

- **Pushed only when ours.** The leader pushes an open fence, and its
  clients delete under it, only when the record is ours. An open record
  of anyone else (a former leader's, or one restored with the metadata)
  is not pushed at all, so the nodes' leases on it lapse (review: a
  fresh process's first pass pushed it and renewed them).
- **Bound on the node.** Pushes carry the opener (`delete_fence_opener`,
  HealthRequest field 11), and a node persists the opener of the
  generation it holds open. A push of that open generation from another
  opener is a conflict: the node closes the generation. Two clusters, one
  restored from the other, that opened the same generation number never
  renew each other's lease (review: a restored controller opens
  `record + 1`, which may be the generation the pre-restore cluster
  opened after the backup). An unknown opener (lost, or an earlier
  release's, 0) conflicts with any other, which fails safe: the leader
  opens one generation higher.

### Metadata rollback gate (controller)

Generations only grow, and each is recorded before any node sees it. A
node reporting a generation above the record therefore means the
metadata went back: a restore that `recovery begin` has not marked yet,
or a hand edit. The leader then:

- closes the fence above every node,
- sets an operator hold, which only `recovery begin` (a new incarnation)
  or an explicit release lifts,
- counts the event in `arca_controller_delete_fence_metadata_behind_total`.

It never opens on such metadata.

The leader also advances a lineage counter in metadata
(`delete_fence_lineage`) on every health pass and remembers the value it
wrote. Reading back less than that means the metadata went back under a
leader that kept running; it is handled like a node ahead (review: such a
leader matched every check of its own fence).

### Sequenced incarnations (R-92)

Every incarnation has a sequence, stored under the metadata key
`incarnation_seq`, carried in `Health`, and persisted by nodes. The
rules:

- A node holding a sequence adopts only a higher one.
- The same sequence is adopted only with the same incarnation; a
  different incarnation at that sequence is a conflict, refused.
- A push without a sequence changes nothing once the node knows one.
- Clocks never order sequenced incarnations.

A restore brings the metadata's counter back too, so `recovery begin`
collects the highest sequence every node reports and marks the new
incarnation above it. It refuses while a node neither answers nor is
declared absent.

### Downgrade gate

Deletes come from controllers and gateways, and both check the metadata
schema at start.

- **Schema 3.** The new schema means "a cluster whose deletes are
  fenced". The layout is unchanged from schema 2. This release reads
  schemas 2 and 3. Once `upgrade finalize` raised the schema, no release
  without the fence starts a controller or gateway on the store. Before
  finalize, rolling back to the previous release (which has the fence)
  stays possible, as RFC-014 requires.
- **Schema floor.** Controllers push the schema to nodes
  (`schema_floor`), and nodes keep the highest they saw. `recovery begin`
  raises restored metadata (schema and `schema_write`) back to that floor,
  so restoring a backup from before the finalize does not reopen the
  downgrade. It refuses when the nodes saw a schema newer than its own
  binary.
  - The finalize raises the schema only once every registered node that
    is not decommissioned reports the floor; a node that does not answer
    blocks it, also one the operator named absent (review).
  - A floor increase waits, like a fence change, for the deletes the node
    already admitted.
  - A node that a controller of this release has led (its pushes carry
    an opener) takes only protocol-4 deletes from then on, whatever its
    floor. It may have missed the floor (decommissioned while
    unreachable, then named again by metadata restored from before its
    replacement); third review round.
  - Nodes keep the floor and the incarnation with its sequence also in
    `cluster-floors.txt`, which earlier releases never touch: a node
    rolled back rewrites `cluster.txt` without the keys it does not know.
    The floors file is authoritative for the incarnation pair; a damaged
    or lost one (after its marker exists) fails closed.
- **Protocol 4.** Nodes of protocol 4 keep the rejoin fence and the
  sequence. Clients send deletes only to such nodes: a node rolled back
  to the previous release receives none. Protocol-4 nodes still accept
  the previous release's version-3 deletes, because the node applies the
  lease to every delete. The previous release's controllers push the
  fence every pass, which renews the lease, so deletes go on during a
  rolling upgrade.
- **Nodes refuse the previous release's deletes after finalize.** A node
  whose schema floor is 3 or more (`REJOIN_FENCE_SCHEMA`) accepts only
  protocol-4 deletes. Without this, metadata restored from before the
  finalize (schema 2) would admit a previous-release controller started
  before `recovery begin`. That controller pushes the nodes' own
  incarnation, which renews their rejoin lease, and it has no rollback
  gate, so its deletes would apply. The two-binary test
  `previous_release::upgrade_from_and_rollback_to_the_previous_release`
  showed exactly that with the node check removed: three shards deleted
  on every node.

## Alternatives considered

| Alternative | Rejected because |
| --- | --- |
| A lease alone | A late renewal can extend a stale fence by one lease. The lease is one layer; the opener and the rollback gate keep a restored cluster from producing a valid renewal at all. |
| A node rejoin fence keyed on node restart only | A node that never restarts but is cut off keeps its fence open. The monotonic lease covers both cases. |
| Wait for every registered node before opening | One node down would stop deletes everywhere. The rejoin lease makes an unreachable node close on its own instead. |
| Order incarnations by a metadata counter alone | A restore brings the counter back. Nodes are the only place the history survives, and `recovery begin` already contacts them all. |
| Mark the store so that old node binaries refuse it | Nothing an old node checks at start can tell a fenced store from an unfenced one without also refusing the previous (fenced) release. The gate is applied where the deletes come from (controllers and gateways) and in the delete protocol. |

## Consequences

- A node restart, or a partition longer than the lease, stops deletes
  on that node until the leader's next push.
- A controller started on restored metadata deletes nothing until
  `recovery begin` (deletes then stay held until finalize) whenever any
  node it reaches is ahead of that metadata. That is the case as soon as
  the cluster opened or closed a generation after the backup point.
  Otherwise it opens a generation of its own above every node's, which
  refuses every delete decided before the restore (ADR-0023).
- `recovery begin` needs every node to answer (or to be declared absent)
  for the sequence and the schema floor.
- After `upgrade finalize` to this release, rolling controllers and
  gateways back to an earlier release is refused at their start.
  Rolling nodes back stops deletes to them.
- `upgrade finalize` needs every node that is not decommissioned to
  answer at its last step.
- Residuals (R-90 narrowed), each needing the restore runbook not
  followed:
  - a renewal delayed past the lease can extend a stale open fence on an
    unreachable node by at most one lease (30 s by default), when the
    previous leader stopped less than one lease before the restore
    completed;
  - a leader that keeps running through a restore is detected by the
    lineage counter, which it advances at every health pass and before
    every GC and reconciliation batch, and which it checks before every
    such batch. Deletes it decided after the backup point and still sends
    after the restore can apply until its next pass detects the restore:
    at most one batch and one health pass (plus the pass's own duration).
    Reviewer A rates the impact (shards that restored objects need) as
    High; it needs the runbook not followed and a leader left running.
