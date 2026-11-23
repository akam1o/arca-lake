# ADR-0037: Shard write ownership

Status: **accepted** (0.2.0 preparation, R-117). Concerns which `PutShard`
a storage node takes. Reuses the cluster incarnation (RFC-015 §5.3,
ADR-0016), the schema floor nodes already learn from their controller
(ADR-0024) and the node's fence guard (Phase 3.5 audit SEC-6); no new
signing, token or certificate. No protocol version, metadata schema,
record or segment format change: three fields are added to
`HealthResponse` (a capability, proto3, ignored by 0.1.0 readers).

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

A fifth, smaller gap: the re-check after the body was not ordered with
the store write, so an incarnation adopted between the two could be
acknowledged while a write of the previous one was still being stored.

## 2. Who writes shards, and with what

Every `PutShard` goes through `NodeClients::put_shard_raw`
(`crates/protocol/src/client.rs`), which stamps the pool's incarnation
(`set_incarnation`). The pools:

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

Refusals are `FAILED_PRECONDITION` with a message starting
`incarnation rejected`, which writers already treat as a failed write of
that member (`NodeClients::is_incompatible`; the gateway places
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
incarnation, and writes to an earlier release's disk that never adopted
an incarnation, are taken and counted (`writes_unproven`). The bound and
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
0.1.0 writers do; 0.2.0 nodes keep taking what 0.1.0 writers send (they
always carry it). Upgraded nodes hold the incarnation they adopted under
0.1.0. Replace `nodebench` binaries of 0.1.0 (they send none: counted in
the window, refused once strict). `upgrade finalize` to schema 5 makes
every node strict. A 0.1.0 node in the window behaves as 0.1.0 (it is
reported, §4).

### Peers refused

- Another cluster's writer, whatever the CA: its incarnation differs
  (incarnations are random UUIDv7s; a restore marks a new one).
- Any writer to a new disk before its controller reached it, and to
  any node before an incarnation once strict.
- A writer holding no incarnation, once strict.
- An earlier-release node of this cluster is not protected (it takes
  what 0.1.0 takes).

## 4. Reporting

- `HealthResponse.write_ownership_supported` (capability bit; false
  from 0.1.0), `write_ownership_strict`, `writes_unproven` (since start);
  `NodeCaps::write_ownership`.
- Node metrics `arca_node_write_ownership_strict`,
  `arca_node_writes_unproven_total`; controller gauge
  `arca_controller_nodes_write_ownership_lenient` (nodes that answered
  the last health pass and are not strict, earlier releases included).
- `admin node show` prints the node's write ownership mode and count.

## 5. What it guarantees, and what still relies on one trust domain

Guaranteed by the node, whatever the CA and trust domain: a write
misdirected from another cluster (a reused address, a repeated node id,
a shared CA) is never stored on a node of this cluster that adopted an
incarnation, nor on a new disk; once strict, a write that does not
carry the node's incarnation is never stored.

Not guaranteed (still relies on mutual TLS with one CA or trust domain
per cluster): the incarnation is not a secret (a node reports it in
`Health` to any authorised caller), so a malicious process holding a
valid certificate of the same CA and trust domain can learn it and
write. Writes within one cluster to the wrong node of that cluster (two
of its nodes' addresses swapped) are not bound to the node identity, as
deletes are; that needs the identity on `PutShard` (a new header field)
and is left to R-117's open part. Plaintext deployments have only the
incarnation check.

## 6. Alternatives rejected

- **Refuse writes to any node without an incarnation and every write
  without one at once** (no mixed window): a 0.1.0 tool's writes, and
  an earlier disk never reached by a controller, would stop before the
  cluster could finish its upgrade. Kept for schema 5 instead.
- **Bind writes to the node identity token** (as deletes): needs a new
  `PutShardHeader` field that 0.1.0 nodes drop silently, and does not
  separate clusters (tokens are per node); the incarnation does.
- **Adopt the first writer's incarnation** (trust on first use): the
  first writer of a new disk at a reused address may be the other
  cluster's.
- **A new protocol version**: nothing a 0.1.0 peer sends changes
  meaning; the capability bit is enough to report the mode.

## 7. Tests

Real RPC (`crates/storage-node/tests/write_ownership.rs`,
`write_ownership_mixed.rs`, `delete_fence.rs`, `mtls.rs`) and end to end
through the gateway (`crates/testkit/tests/it/write_ownership.rs`); the
before/after runs are in the PR. Normal paths (first writes, restart,
a node added, repair, migration, tiering, restore then rejoin) are the
existing testkit suite, whose harness now gives nodes the incarnation as
a controller does.
