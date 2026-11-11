# ADR-0027: Node lifecycle, identity, unregister and rejoin (R-100)

Status: accepted (Phase 3.11)

## Context

Before this phase a node had two lifecycle states beyond plain
registration, `Draining` and `Decommissioned` (`membership`). Nothing ever
removed a node, and a node never learned its own id. The Phase 3.11
survey confirmed in code:

1. A wiped disk restarted at a member's address answered Health and
   counted as that healthy member. It held nothing, no repair started,
   and the group ran with one redundancy less.
2. A decommissioned node kept its former shards and every leader kept
   pushing it the open fence, so this cluster's deletes applied there:
   late ones, and ones decided on metadata restored from before its
   replacement (R-100).
3. `create-groups` placed groups on draining, decommissioned or dead
   nodes.
4. `--ignore-absent` accepted any node. An active node named there let
   the finalize pass its cut-off without that node being checked.
5. Re-registering a decommissioned node's address silently brought it
   back with its old id.

## Decision

### States

| State | Record | Holds members | Placement target | Leader pushes | Finalize waits |
| --- | --- | --- | --- | --- | --- |
| Active | none | yes | yes | facts, fence, identity | yes |
| Draining (decommissioning) | `nstate` | being moved | no | yes | yes |
| Decommissioned | `nstate` | no | no | yes (its former shards are reconciled) | yes, unless named in `--ignore-absent` |
| Removed | `nretire` (new key) | no | no | nothing | no |
| Quarantined | `nretire` | repaired away | no | a probe only | yes |
| Unreachable | derived (`last_seen_ms`) | — | no | retried | yes |
| Rejoining | not persisted: the checks of `admin node rejoin` | — | — | — | — |

The new states live under a key of their own, which earlier releases do
not read, so no existing record changes its encoding. A removed or
quarantined node is never healthy for repair, and no target is chosen on
it.

### Identity

The leader gives every node a random 128-bit token, kept in metadata
(`nident`). It sends `(node_id, token)` with its health push:

- **Adopt.** The request asks the node to adopt the identity
  (`adopt_identity`) only until the node has acknowledged it once. The
  node writes it to `node-identity.txt`, a file that earlier releases
  neither read nor rewrite.
- **Compare.** After that, the identity is only compared. A request
  carrying another identity changes nothing on the node (no facts, no
  fence), and the reply flags `identity_mismatch`.
- **Quarantine.** The leader quarantines a node that holds:
  - no identity after acknowledging one (an emptied or replaced disk);
  - another token for its id (another cluster's disk, a re-registered
    id);
  - another node's id (a disk moved).
- **Earlier releases.** Nodes of earlier releases report no identity
  support. They are not judged by it (they keep the gates they have).

Recovery pushes carry no identity: restored metadata may predate a
node's identity.

The identity is a check that a disk is the one registered. It is not
authentication: a request carrying no identity still changes the node's
facts as before. Such requests come from earlier controllers, admin
pushes and recovery, and are authorized by mutual TLS.

Review fixes (Phase 3.11):

- **Restored metadata without a record.** When the metadata has no
  record for a node, as after restoring a backup from before identities,
  the leader carries none. If the node holds an identity for its own id
  and its incarnation sequence is not above the cluster's, the leader
  takes it into the metadata. If the node holds none, it is given one.
  A node holding another node's identity is quarantined. Before this
  fix, every node would have been quarantined after such a restore.
- **Damaged file.** A damaged identity file is never replaced by an
  adoption. An operator removes it; `rejoin --fresh-identity` then gives
  a new one.
- **Deletes bound to the disk.** Deletes carry the identity the node
  acknowledged (`DeleteShardRequest.node_id`, `node_identity`, additive
  fields). A node holding another identity, or none, refuses them: a
  delete decided for one node never runs on another disk at its
  address.
- **Reconciliation.** It skips removed and quarantined nodes.
- **Removed addresses.** A removed node's address cannot be registered
  by itself again. A new machine there comes in through `admin node
  rejoin --fresh-identity`, on an empty disk.

### Unregister (`admin node unregister --node N --reason R --yes`)

Only a decommissioned or quarantined node that no group lists and no
replacement names may be unregistered. The node is told first: its
delete fence is closed with an operator hold. The command refuses if
the node does not take it. A node that does not answer is unregistered
only with `--unreachable`, which states that its disk will never be
started again unwiped. Unregistering an already removed node changes
nothing. The record stays, so the id is never reused. Registering the
address again is refused and points to `rejoin`.

### Rejoin (`admin node rejoin --node N [--fresh-identity] [--check] --yes`)

Refused unless the node:

- answers;
- speaks this release's protocol;
- writes at least the cluster's segment format;
- holds no schema floor above the cluster's schema;
- holds no incarnation sequence above the cluster's, and not another
  incarnation at the cluster's sequence;
- reports an identity, and that identity is the one the cluster gave
  it.

An emptied disk needs `--fresh-identity`, which is refused if the disk
holds shards. A disk with another identity never rejoins. The node comes
back active without members. Its hold stays until an operator releases
it (`admin delete-fence release`). Reconciliation then collects its
former shards, and placement and repair may use it.

### Upgrade gate

`--ignore-absent` naming a node that is not decommissioned blocks the
finalize's first step, with the reason. Removed nodes are not waited for
at any step.

## Alternatives considered

| Alternative | Rejected because |
| --- | --- |
| Delete the node record on unregister | The id would be reused by the counter, and the address could register again silently. |
| Store the node id in the existing facts file | Earlier releases rewrite that file with known keys only; a rollback and roll-forward would lose the identity and quarantine every node. |
| New `NodeLifecycle` variants | Earlier releases fail to decode the record in a mixed-version cluster. |
| Let a node adopt any identity it is sent | A wiped disk would take the member's identity silently, which is case 1 again. |

## Consequences

- An emptied disk at a member's address is repaired away instead of
  serving nothing as a member.
- An unregistered node refuses this cluster's deletes, including a
  restored metadata's leader's, as long as its hold stands. Only a
  later incarnation that it adopts would lift the hold, and no leader
  of this release reaches it.
- Residuals, recorded in R-100:
  - a node unregistered with `--unreachable` relies on the operator's
    statement;
  - nodes of earlier releases are not checked by identity;
  - a decommissioned node that was never unregistered, restarted under
    an earlier release's binary with metadata restored from before its
    replacement, and driven by an earlier release's controller, still
    has only that release's protections (needs a restore, old binaries,
    and the runbook not followed).
- Tests: `it/node_lifecycle.rs`, five tests. Each fails with its check
  removed: no quarantine, unregister without the node push,
  registration accepting retired nodes, `--ignore-absent` accepting
  active nodes. Also `storage-node identity` and `metadata leader` unit
  tests.
