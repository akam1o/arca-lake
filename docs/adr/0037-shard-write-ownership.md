# ADR-0037: Shard write ownership

Status: **accepted** (0.2.0 preparation, R-117). Concerns which `PutShard`
a storage node takes. Reuses the cluster incarnation (RFC-015 §5.3,
ADR-0016), the schema floor nodes already learn from their controller
(ADR-0024) and the node's fence guard (Phase 3.5 audit SEC-6); no new
signing, token or certificate; the target node is named with the
node identity deletes already carry (Phase 3.11, R-100). No protocol
version, metadata schema, record or segment format change: two fields
are added to `PutShardHeader` (7 `node_id`, 8 `node_identity`) and three
to `HealthResponse` (47–49, a capability); proto3, ignored by 0.1.0
readers.

## 1. Problem

R-117 (found on CI, 2026-11-19): a gateway that still maps a node to an
address now served by a node of another arca-lake cluster wrote shards
there and counted them written. 0.1.0 stamps every `PutShard` with the
writer's incarnation and a node that adopted another one refuses it
(`check_write_incarnation`). Four paths were left open:

1. a write carrying no incarnation was accepted;
2. a node that had adopted no incarnation accepted any writer;
3. hence another cluster's node at a reused address, if it had adopted
   nothing yet (a new disk its controller had not reached);
4. clusters sharing a CA and trust domain authenticate each other, and
   node IDs (small integers) repeat across clusters, so mutual TLS does
   not tell them apart.

5. a write reaching **another node of the same cluster** (an address
   reused within the cluster, a node moved, a gateway's stale map) was
   stored and counted written: writes were not bound to the target node,
   as deletes are.

A sixth, smaller gap: the re-check after the body was not ordered with
the store write, so an incarnation adopted between the two could be
acknowledged while a write of the previous one was still being stored.

## 2. Who writes shards, and with what

Every `PutShard` goes through `NodeClients::put_shard_raw`
(`crates/protocol/src/client.rs`), which stamps the pool's incarnation
(`set_incarnation`) and, since this ADR, the target node. Every product
writer now calls `put_shard_to` with the placement member it writes
(`member.node_id`: the gateway's group member, repair's replacement
member, the integrity task's target member, the tiering and migration
target group's member); the pool adds the identity token that node
acknowledged when it knows it for that address (`note_identity`, which
the controller's health pass maintains for deletes; the gateway reads
the acknowledged identity records with its cluster state, every
`CLUSTER_REFRESH` (2 s), for its deletes of its own partial cache copies
and its writes alike). `put_shard`/`put_shard_as` name
the node noted for the address (`note_node_id`), if any. The pools:

| Writer | Pool | Incarnation |
| --- | --- | --- |
| Gateway PUT, multipart parts, cache populations (`gateway/src/cluster/write.rs`) | the backend's | the cluster state read at the startup gate (`backend.rs`, set before serving) |
| Controller leader: repair, integrity repair, tiering, migration, rebalancing (`controller/src/repair.rs`, `integrity.rs`, `tiering/src/{durable,migrate}.rs`) | the leader's (`process.rs` `start_leader_tasks`) | the incarnation the leader started under |
| Managed restore (`controller/src/managed_restore.rs`) | its own | the marked incarnation |
| `admin integrity scrub-now`, `admin verify-object --quarantine` (CLI) | admin | the cluster state, if the store is initialised (it always is once a cluster ran) |
| Other admin commands (replace-member, rebalance, recovery, restore plan) | admin | they write no shard (they record intents for the leader) |
| `arcabench nodebench` | its own | none in 0.1.0; now the target node's own (it benchmarks one node by address) |
| `benchmarks/transfer` | its own server | not a storage node |
| testkit | the harness's and the backend's | as the gateway; the harness now pushes the incarnation to its nodes as a controller does |

All 0.1.0 writers of the product stamp the incarnation (checked on tag
`v0.1.0`: the same `put_shard_raw` and the same `set_incarnation` call
sites). The only 0.1.0 writer sending none is `nodebench`.

A node adopts its incarnation from the controller: every health pass
pushes the incarnation with its sequence and change time and the schema
floor (`gate.schema`); `recovery begin` and a managed restore push a new
incarnation of a higher sequence. Adoption is persisted (`cluster.txt`
and the floors file) before it is acknowledged and survives restarts and
rollbacks; a push carrying another node identity than the disk's adopts
nothing (R-100). Deletes already require the adopted incarnation (an
empty one refused once adopted, SEC-5), the open fence generation, and
the node's identity token (R-100).

## 3. Decision

A node takes a `PutShard` only as follows (`service.rs`
`check_write_incarnation`, before the body and again after it):

| Node state | Carries the node's incarnation | Carries another | Carries none |
| --- | --- | --- | --- |
| Adopted one, **strict** | taken | refused | refused |
| Adopted one, not strict | taken | refused | taken, counted |
| Adopted none, held no data at start (a new disk) | refused | refused | refused |
| Adopted none, held data at start, not strict (an earlier release's disk never reached by a controller) | taken, counted | taken, counted | taken, counted |
| Adopted none, strict | refused | refused | refused |

**Target node** (`check_write_target`, before and after the body, after
the incarnation check). A write names a node (`node_id`, and the token
if the writer knows it):

| Node holds | Names this node (token absent or equal) | Names another node, or this id with another token | Names none |
| --- | --- | --- | --- |
| its identity | taken | refused | strict: refused; else taken, counted |
| no identity, a node certificate (mTLS) | taken if the certificate names that node | refused | as above |
| no identity (or a damaged one) and no node certificate naming a node | strict: refused; else taken, counted | strict: refused; else taken, counted | as above |

The controller's health pass gives a node that holds no identity one in
the same request as the incarnation and the floor, but not every node
holds one: a disk whose identity file was removed keeps its facts and
incarnation, and the controller then pushes no adoption (it reports the
disk and waits for `admin node rejoin --fresh-identity`); a damaged
file is not replaced either. Nor does every node serve a usable node
certificate: plaintext clusters have none, and a node certificate whose
SPIFFE name is not a numeric node id (`spiffe://<td>/node/<name>`) names
no node. Such a node cannot check the name, so it refuses named writes
once strict, and before that takes them, counted (`writes_unproven`).
The testkit harness gives strict clusters' nodes their identity with the
facts, as a controller does; tests of nodes without identities build
them explicitly (`ClusterOptions::node_identities`).

Refusals are `FAILED_PRECONDITION` with a message starting
`incarnation rejected` or `node rejected`, which writers treat as a
failed write of that member (`NodeClients::is_incompatible`; the gateway places
elsewhere or answers 503, repair and migration retry).

**Strict** means the node was told the schema
`WRITE_OWNERSHIP_SCHEMA` (5) or later: the schema floor it adopts from
its controller, persisted and monotonic. `upgrade finalize` pushes the
target schema to every node before raising it, and a 0.1.0 process
cannot start on a schema-5 store, so from then on no peer sends a write
without the incarnation. A cluster initialised at schema 5 pushes it at
its controller's first health pass, together with the incarnation, and
its nodes hold no data before that: they are strict from their first
write. The schema itself is raised by the coordinated upgrade change
(its ADR); this ADR only names the value (`pb::WRITE_OWNERSHIP_SCHEMA`,
to be reconciled with that change's constant).

**Not strict** is the mixed window of an upgrade from 0.1.0. What a
0.1.0 peer relies on is kept and made visible: a write carrying no
incarnation or naming no node (every 0.1.0 writer names none), writes
to an earlier release's disk that never adopted an incarnation, and
writes to a node that cannot check the node named, are taken and
counted (`writes_unproven`). A write naming another node than the one
the node is, is refused in every mode: only 0.2.0 writers name one. The bound and
the end condition: these exceptions exist only below schema 5, and end
for good on each node when it is told schema 5 (the floor never goes
down, also after a restore).

**A new disk** (no live record when the node started) takes no write
before it adopted an incarnation, in every mode. This closes paths 2
and 3 without waiting for schema 5 and breaks no 0.1.0 peer: every
controller, 0.1.0 included, pushes the incarnation at each health pass,
and nothing places data on a node before its controller reached it;
a write arriving earlier (a gateway on a stale map, a bootstrap race) is
refused and placed elsewhere.

**Mid-write change.** The check after the body and the store write hold
the node's fence guard shared (moved into the blocking store call); an
adoption of a new incarnation, a fence change and an epoch raise take it
exclusive. An adoption is therefore acknowledged only once no write that
passed the previous incarnation can still be stored, and every later
check sees the new one (`a_new_incarnation_is_acknowledged_only_after_writes_past_the_old_one`).
A body still in flight when the incarnation changes is refused by the
second check.

### Bootstrap order

1. `admin register-node`, `admin create-groups` (metadata only).
2. Storage nodes start; a new disk holds no facts and takes no write.
3. The controller's first health pass gives every node its identity,
   the incarnation and the schema floor. Gateways admit writes only
   under the controller's admission record (Phase 3.8), so normal
   startup does not reach a node before this; one that does is refused.
4. Writes. A restarted node keeps its incarnation and floor and takes
   writes at once. A node added later, or a disk replaced (`node rejoin
   --fresh-identity`), takes writes from its first health pass. After a
   restore or `recovery begin`, nodes take writes of the new incarnation
   once it was pushed to them (as 0.1.0; R-97 for a node that missed it).

### Update order (0.1.0 to 0.2.0)

Any order, as for 0.1.0 releases: 0.2.0 writers stamp the incarnation as
0.1.0 writers do and also name the target node, which 0.1.0 nodes
ignore; 0.2.0 nodes keep taking what 0.1.0 writers send (the
incarnation, no node: counted). Upgraded nodes hold the incarnation they adopted under
0.1.0. Replace `nodebench` binaries of 0.1.0 (they send none: counted in
the window, refused once strict). `upgrade finalize` to schema 5 makes
every node strict. A 0.1.0 node in the window behaves as 0.1.0 (it is
reported, §4).

Nodes turn strict **before** the schema is raised: the finalize pushes
the target schema to every node first and raises the stored schema only
once all hold it (`controller/src/upgrade.rs`). From that push on, a
writer that still sends no incarnation or names no node (a 0.1.0 CLI
tool or `nodebench`, any process the finalize's heartbeat check did not
see because it does not heartbeat or started after the check) has every
shard write refused, which its users see as failed writes (503 through a
gateway). This costs availability, not safety: nothing unproven is
stored. Stop or upgrade such tools before `upgrade finalize`.

Restoring onto strict nodes: the schema floor survives a restore. A
backup taken at schema 4 restored onto nodes already told schema 5 keeps
them strict; 0.1.0 writers started on the restored metadata then have
their shard writes refused (as their deletes are refused by the rejoin
fence floor), and 0.1.0 processes cannot be relied on after a restore
that crosses the finalize. Restore with 0.2.0 processes only.

### Peers refused

- Another cluster's writer, whatever the CA: its incarnation differs
  (incarnations are random UUIDv7s; a restore marks a new one).
- Any writer to a new disk before its controller reached it, and to
  any node before an incarnation once strict.
- A writer holding no incarnation, once strict.
- A write for another node of this cluster (named by the writer, from
  the placement member), whatever the address, once the node holds its
  identity or serves a node certificate; and once strict, a write
  naming no node.
- An earlier-release node of this cluster is not protected (it ignores
  the fields and takes what 0.1.0 takes; reported by the capability
  bit).

## 4. Reporting

- `HealthResponse.write_ownership_supported` (capability bit: the node
  enforces the incarnation and target-node rules; false from 0.1.0, which
  ignores the target node), `write_ownership_strict`, `writes_unproven` (since start);
  `NodeCaps::write_ownership`.
- Node metrics `arca_node_write_ownership_strict`,
  `arca_node_writes_unproven_total`; controller gauge
  `arca_controller_nodes_write_ownership_lenient` (nodes that answered
  the last health pass and are not strict, earlier releases included).
- `admin node show` prints the node's write ownership mode and count.

## 5. What it guarantees, and what still relies on one trust domain

**When it takes effect.** The checks that do not depend on the schema
(another incarnation, a new disk without one, another node named, the
fence guard) apply as soon as a node runs 0.2.0. Strict mode needs
schema 5, which 0.2.0 has with the schema-5 change (ADR-0039): a cluster
0.2.0 initialises is at schema 5 and strict from its first write; a
cluster upgraded from 0.1.0 is in the mixed window, unproven writes taken
and counted, until `upgrade finalize` to schema 5. The testkit suite runs
strict clusters (every test cluster is initialised at schema 5, or 6 in
CI's all-features `version-bump` build), including `write_ownership`.

Guaranteed by the node, whatever the CA and trust domain: a write
misdirected from another cluster (a reused address, a repeated node id,
a shared CA) is never stored on a node of this cluster that adopted an
incarnation, nor on a new disk; a write for another node of the same
cluster is never stored on a node that holds its identity (or serves a
node certificate naming its node id), so it is never counted written;
once strict, a write that does not carry the node's incarnation and name
the node, or that the node cannot check against an identity or a
certificate, is never stored.

Not guaranteed (still relies on mutual TLS with one CA or trust domain
per cluster): the incarnation is not a secret (a node reports it in
`Health` to any authorised caller), so a malicious process holding a
valid certificate of the same CA and trust domain can learn it and
write. A gateway's tokens are at most one cluster refresh (2 s) older
than the identity records: after `node rejoin --fresh-identity`, its
writes and cleanup deletes to that node are refused until the new
identity is acknowledged and read (writes go elsewhere; a refused
cleanup is counted, `arca_gateway_cache_cleanup_refused_total`, and the
copy left to reconciliation). A node the gateway knows no acknowledged
identity for gets the node id only. Before strict mode, a node without an identity and without a
node certificate naming its node (a removed or damaged identity file, a
plaintext cluster's node before its controller gave one, a non-numeric
SPIFFE node name) takes writes naming any node, counted.

Smaller residuals (review):

- Identity adoption and incarnation adoption are taken under the fence
  guard; the `writes_unproven` counter counts a write when it passes the
  check after its body, also if the store write then fails, so it can
  over-count (it is a signal, not an exact tally).
- `put_shard` and `put_shard_as` (tests, tools) name the node the pool
  noted for the address, not a placement member; only `put_shard_to`
  names the member, and every product writer uses it. A tool writing
  with no node noted names none (counted, refused once strict).
- Store writes hold the fence guard shared from the check after the
  body to the end of the store write (L2): an epoch raise, a fence change
  and the adoption of an incarnation or an identity wait for the slowest
  PUT then in flight to finish storing, and new PUTs and deletes queue
  behind the waiting change (the lock is fair). A slow device therefore
  delays these pushes, and every write and delete behind them, by up to
  one store write (not by the network transfer, which happens before the
  guard is taken).
- After `admin node rejoin --fresh-identity` the controller keeps
  stamping the old identity token for that address until its next
  health pass acknowledges the new one (L4): repair, migration and
  integrity writes to the node are refused (`node rejected`) and retried
  by their tasks meanwhile; gateway writes, which name the node id only,
  are taken.
- A disk counts as holding earlier data by its live records at start: a
  0.1.0 disk whose records were all deleted before the upgrade is taken
  for a new one and refuses writes until its controller reaches it
  (availability only).

## 6. Alternatives rejected

- **Refuse writes to any node without an incarnation and every write
  without one at once** (no mixed window): a 0.1.0 tool's writes, and
  an earlier disk never reached by a controller, would stop before the
  cluster could finish its upgrade. Kept for schema 5 instead.
- **Bind writes to the node identity token only** (no incarnation
  check): tokens are per node, the incarnation is what separates
  clusters; both are used.
- **The gateway loading identity tokens with its cluster map**: an
  acknowledgement does not move the map version, so the tokens would
  stay stale until the next map change. They are read with the cluster
  state instead (one range read per refresh), which bounds the staleness
  to a refresh; before that follow-up the gateway sent no token, and
  nodes holding an identity refused its cleanup deletes.
- **Adopt the first writer's incarnation** (trust on first use): the
  first writer of a new disk at a reused address may be the other
  cluster's.
- **A new protocol version**: nothing a 0.1.0 peer sends changes
  meaning; the capability bit is enough to report the mode.

## 7. Tests

Real RPC (`crates/storage-node/tests/write_ownership.rs`, including
`a_write_for_another_node_is_refused`,
`write_ownership_mixed.rs`, `delete_fence.rs`, `mtls.rs`) and end to end
through the gateway (`crates/testkit/tests/it/write_ownership.rs`,
including another node of the same cluster at a member's address); the
before/after runs are in the PR. Normal paths (first writes, restart,
a node added, repair, migration, tiering, restore then rejoin) are the
existing testkit suite, whose harness now gives nodes the incarnation as
a controller does.
