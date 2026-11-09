# ADR-0023: A delete fence enforced by storage nodes

Status: accepted (Phase 3.9)

## Context

R-88: a shard delete decided on the metadata before a phase change or a
metadata restore could be applied by a storage node afterwards.

1. GC verifies a generation unreferenced and decides to delete its
   shards.
2. The `DeleteShard` RPCs are sent and delayed (network, a queue, a
   paused process).
3. The metadata is restored to a backup in which the generation is
   referenced again, or the cluster leaves `Active`.
4. The delayed RPCs arrive and remove shards the restored metadata
   needs.

None of the protections that existed before stops step 4.

| Protection | Why it does not stop the race |
| --- | --- |
| Cluster incarnation on `DeleteShard` (ADR-0016) | Nodes learn the new incarnation only after the restore (`recovery begin`, then a controller push). A delete that arrives between `fdbrestore` and that push carries the incarnation the node still holds. |
| Group epoch | A restore brings back the epoch the delete was decided under. Epochs fence membership changes, not time. |
| Record token (`if_sequence` / `if_store_epoch`, Phase 3.5) | The delete names exactly the record the restored metadata references. The token protects a newer record under the same key, not the record itself. |
| Metadata guard (`GuardedKv`) and leader token | They fence metadata writes. A delete RPC already sent writes nothing to the metadata. |
| Controller-side checks | They stop new decisions, not decisions already in flight. |

So the safety of a delete must be decided where it is applied: on the
storage node, at the moment its store removes the record.

## Decision

Every `DeleteShard` carries a **delete fence generation**. A node
applies a delete only while it holds that generation **open**. The
check runs under a lock that the node's physical delete holds until it
ends, so a fence change waits for every delete that passed the old
fence.

### Node state and rules

The node persists `(generation, open, hold)` in `cluster.txt`. It
writes the file atomically and syncs it before the RPC that carried the
change is acknowledged.

| Rule | Purpose |
| --- | --- |
| A delete is applied only if the request's protocol is ≥ 3 and `delete_fence` equals the node's generation while it is open. Anything else is refused with `FAILED_PRECONDITION` "delete fence …": generation 0 or missing, another generation, a closed fence, or no fence adopted yet. | Invariants D and E; fail closed |
| The incarnation is checked first, then the fence. Both run under the shared guard, which is an owned guard moved into the blocking store call. | A cancelled RPC cannot release the guard while its delete still runs. Before this change the guard lived in the async handler, a latent flaw of the Phase 3.5 incarnation fence as well. |
| A fence change takes the exclusive guard, and only when it changes something (`plan_fence`). | A change is acknowledged only after every delete past the old fence has finished. |
| Generations only grow. A closed generation never reopens. A lower generation is ignored. | A delayed or reordered push cannot reopen deletes. |
| The facts file (`facts 2`, ending in a CRC-32C of its content) is written by a new node at its first start, after a marker file (`cluster.v2`) that makes a later file without the version line damage rather than an earlier release's. A file that cannot be read, fails its checksum, is truncated, or has a damaged line, or a missing one while the store holds segments, keeps the fence closed under an *unknown* hold (the generation floor, the hold and the incarnation it knew are gone). Only a release carrying the token the node drew at this start (reported in `Health`; drawn at every start while the hold lasts) lifts it, so no release sent before the loss can. An open fence that cannot be persisted is not adopted. A close that cannot be persisted still applies in memory, and no later request is acknowledged until it is on disk. | Fail closed; a lost file cannot reopen a node to a delayed push (review) |
| Any fence field in a `Health` request makes it a fact push, which only the controller and admin roles may send. | A gateway or node identity cannot open, close, hold or release (review) |
| A release names the closed generation it releases. It lifts the hold only at that generation or above, adopting it closed, and never opens anything. | A delayed release cannot lift a later close's hold (review) |
| Nodes ignore generations above 2^63 − 1, open nothing above 2^63 − 1 − 2^32, and take no jump of more than 2^32; metadata records the same bounds, and controllers ignore reports beyond them. | A close always has room above any open; nothing can push the fence where it cannot advance (review) |
| The operator hold is tied to the node's incarnation when the hold was set, and keeps that incarnation when a later push repeats it. While held, an open generation is adopted only from a caller whose incarnation is a later one that the node adopted; a release lifts the hold without opening (the leader opens afterwards). A caller of an incarnation the node does not adopt can neither set nor lift a hold. | A controller started on restored metadata before `recovery begin` (same incarnation), or on a backup from before an earlier restore (older incarnation), cannot reopen deletes. |

### Who opens and closes it

- **The controller leader**, in each health pass
  (`delete_fence::sync_for_term_gated`). It opens a generation only
  after a whole round of node reports in its term: a fresh process knows
  no node's generation and could otherwise reopen one (review). A
  failure to advance the fence stops this process's deletes without
  stopping the pass.
  - While the cluster is `Active` in its incarnation and no operator
    hold is set, it keeps a generation open for its term. The record's
    generation is reused if it is open, of this term and incarnation,
    and at least the required one. Otherwise the leader opens
    `max(record, required) + 1`, where `required` is the lowest
    generation every node would accept: a node's open generation, or its
    closed generation plus one. So a node's own open generation is never
    reopened by a new term (after a restore without the close, it may be
    a generation decided before the restore).
  - Outside `Active` it closes the fence.
  - It pushes the fence with the other cluster facts and repeats a
    recorded hold of its own incarnation to nodes that missed it (not
    the hold `recovery begin` set before a restore's new incarnation:
    repeated after it, the hold would be tied to the new incarnation and
    could not be lifted after finalize). It never releases a hold.
    Recovery's own push never carries the hold.
  - The record is written through the leader's fenced metadata store, so
    a former leader cannot open a generation.
- **Gateways** delete only their own partial writes. They take the
  recorded generation at start and on every 2 s refresh. A failed read
  closes the fence in the gateway.
- **Operators**:
  - `admin delete-fence close --reason R --yes` writes a new closed
    generation above every generation a node holds, sets the hold and
    pushes the close to every registered node. It exits 1 unless every
    node acknowledged the closed generation *and* the hold; a node may be left out only with an explicit
    `--absent-node ID` (its process stopped).
  - `admin recovery begin` performs the same close (with the hold)
    before it marks the restored metadata. It refuses unless every node
    acknowledged, and warns about every node whose fence was still
    open: the runbook's first step was skipped.
  - `admin cluster set-read-only` closes the fence without a hold
    (best effort; the leader closes it on the remaining nodes at its
    next pass).
  - `admin recovery finalize` clears the recorded hold in its own
    transaction.
  - `admin delete-fence release` clears the hold in the record and on
    the nodes, only while the cluster is `Active`.
  - There is no command that opens a fence. Only a leader of an `Active`
    cluster does.

### Clients

`NodeClients` sends a delete only at protocol version ≥ 3 with its
current generation. Without an open generation it fails locally,
before any RPC. A node that negotiated an older version receives no
delete at all, so the client never sends an unfenced delete for
compatibility. Callers keep what was refused:

- GC defers the generation.
- Reconciliation and cache reconciliation count the shard as kept
  (`kept_fenced`).
- Cache eviction and migration clean-up leave orphans, which a later
  reconciliation removes.

### Wire

- `DeleteShardRequest.delete_fence = 7`
- `HealthRequest`: `delete_fence_generation = 4`, `delete_fence_open = 5`,
  `delete_fence_hold = 6`, `delete_fence_release = 7`,
  `delete_fence_release_token = 8`
- `HealthResponse`: `delete_fence_generation = 28`,
  `delete_fence_open = 29`, `delete_fence_hold = 30`,
  `delete_fence_unknown_token = 31`
- `PROTOCOL_VERSION = 3` (`FENCED_DELETE_VERSION = 3`)

The metadata record is `DeleteFenceRecord` (golden-encoded) at
`keys::delete_fence()`.

## Why the guarantees hold

- **D: after a fence change, no delete of the old generation completes.**
  A delete reads the fence and finishes its physical delete under the
  shared guard. A change is adopted and persisted under the exclusive
  guard, and the push is answered only after that. Once a node has
  acknowledged a change, every delete that passed the old fence has
  finished, and every later delete sees the new fence. This also holds
  when the delete's RPC was cancelled.
- **C: a delete decided before a restore cannot remove a shard the
  restored metadata references.** The runbook closes the fence on every
  node, each acknowledging (D), before `fdbrestore`. `recovery begin`
  refuses to mark the restored metadata unless every node acknowledges a
  close. The hold keeps the fence closed until a controller of the new
  incarnation (after `begin`) or an explicit release opens it. A
  generation opened then is above every generation decided before the
  restore, because generations never reopen and only grow. So no delete
  decided before the restore is ever admitted again.
- **Leader change (S7).** A new leader's generation is above every
  generation a node holds. A node that adopted it refuses the former
  leader's deletes. This is defense in depth: GC decisions of a former
  leader concern generations verified unreferenced, which stay
  unreferenced in the same metadata (ADR-0019). The fence makes the
  cut-off explicit and visible.
- **Phase change.** Outside `Active` no generation is open, and
  `set-read-only` and the leader close the generation that was. Deletes
  decided while `Active` are refused afterwards, also after the cluster
  is `Active` again under a new generation.
- **E: fence unknown means nothing is deleted.** A node without a fence,
  a damaged file, a gateway that cannot read the record, a client
  without a generation, an older protocol: in each case the delete is
  refused.
- **F: deletes resume.** Once `Active` without a hold, the leader opens a
  generation on its next pass (`health_interval`, default 5 s). GC, reconciliation and
  cache reconciliation retry what they deferred or kept, so the backlog
  converges. PUT and GET never check the fence.

## Alternatives considered

| Alternative | Rejected because |
| --- | --- |
| Incarnation alone, bumped before the restore | The incarnation identifies a metadata history. Minting one before a restore means writing it to metadata that is about to be replaced, and pushing it to the nodes, which is a fence close under another name. It is also not monotonic per node across restores. A generation separates the two concerns. |
| A delete epoch in metadata only, checked by controllers | RPCs already sent are not checked again (the race itself). |
| Nodes reading the metadata before each delete | Storage nodes hold no metadata connection by design (RFC-001: they store shards and know only group epochs and cluster facts pushed to them). It would make every delete depend on FoundationDB availability and add a round trip per shard. |
| Time leases on delete permission | Safety would depend on clocks and pause bounds, and RFC-008 §6.7 says safety does not depend on clocks. |
| Deferred physical deletes (trash with a grace period) | It costs space, and cancelling a deferred delete still needs a fence. Kept as possible defense in depth (Phase 4). |
| Fencing every delete path the same way, including compaction | Compaction and recovery-time segment removal are node-local. They remove records already superseded or tombstoned in the node's own index, so no external decision is involved (see the audit in `docs/verification/phase-3.9-data-safety.md`). |
| A force-open command | It would undo the protection exactly when it matters (a confused restore). Opening is left to a leader of an `Active` cluster; the hold is lifted by finalize or an explicit release, and only while `Active`. |

## Consequences

- Deletes stop while the cluster is not `Active`, while a hold is set,
  and on a node until it has adopted the leader's generation (after a
  start, within one health pass). GC backlog grows meanwhile and
  converges afterwards. Writes and reads are unaffected. Space is not
  returned while deletes are stopped, so capacity admission can throttle
  or refuse writes earlier during a long hold (the reserves themselves
  are unaffected: deletes are never needed to keep them).
- Mixed versions: a version-2 node receives no deletes from a version-3
  client, and a version-3 node refuses every delete from a version-2
  client. During a rolling upgrade, deletes on a node resume once both
  the node and the controllers speak version 3. Rolling back a node to
  version 2 stops its deletes. Rolling back both nodes and controllers
  to version 2 brings back the pre-3.9 behaviour, including R-88.
- A restore needs every registered node to acknowledge the close, or an
  explicit `--absent-node` for a node whose process is stopped. Such a
  node keeps the fence it had on disk and must stay stopped until the
  restored cluster's leader has pushed it a fence. Its old generation is
  open, and a still-running controller of the old incarnation could
  otherwise delete there.
- Residual: if the runbook's first step is skipped, a delete in flight
  can still land between `fdbrestore` and `recovery begin`. `begin`
  warns, and verification reports the shards that are missing.
