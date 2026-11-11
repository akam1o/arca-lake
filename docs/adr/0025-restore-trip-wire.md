# ADR-0025: Restore trip wire for a leader left running (R-94(b))

Status: accepted (Phase 3.11) as a **mitigation** of R-94(b). It limits
the damage once a leader observes a restore; it is not a fence that
guarantees that deletes decided before a restore never apply to the
restored data. R-94(b) stays open (High); the permanent mechanism is a
separate decision (see "Not guaranteed" and "Next").

## Context

ADR-0023 and ADR-0024 refuse deletes decided before a restore once the
fence changed or the leader saw its lineage counter go back. R-94(b)
remained: a controller leader left running through a restore done
without the runbook (no `admin delete-fence close`, controllers not
stopped) could still apply deletes it decided on pre-restore metadata.

The race was reproduced deterministically in Phase 3.11 (test hooks that
hold a GC batch between its reference check and its deletes, or hold the
`DeleteShard` RPCs themselves; no timing). Before this change the
restored object lost every shard (6 of 6).

### When each step happens

| Step | Where | What it sees |
| --- | --- | --- |
| Delete decided | GC `verify_unreferenced`, reconciliation `attribute_shard`, the former-placement check after a migration | one transaction, at its read version |
| RPC sent | after that transaction, with no further metadata access | the process's fence generation (`NodeClients`) |
| Fence checked | storage node, on admission of the delete | the node's open generation and rejoin lease |
| Shard removed | storage node, after admission | nothing more |
| Metadata rolled back | operator clears the keyspace, then `fdbrestore` writes the backup under a database lock | — |
| Leader can notice | its next metadata operation (any task) | the clear, the lock, or the restored records |

### Why the earlier layers do not stop it

- **Fence generation and opener.** The node holds the generation the
  leader opened; the leader's process keeps it in memory. Nothing told the
  process to stop using it when its transactions started failing.
- **Leader term.** A backup taken during the current term restores the
  leader record with the same holder and token: the restored store
  accepts the leader's next commits as its own.
- **Incarnation.** Only `recovery begin` changes it, after the restore.
- **Rejoin lease.** It is renewed by the leader's pushes, which resume as
  soon as the restored metadata is readable.
- **Lineage counter.** It detects the rollback at the next health pass or
  batch, after the restore. A batch in flight at that point was decided
  and passed its checks before.
- **`database_locked` (1038)** was classified as a retryable conflict:
  a leader retried through the restore without noticing it.

FoundationDB 7.3 was checked on a real cluster (private `fdbserver` and
`backup_agent`): during `fdbrestore` every read and write of another
client fails with 1038; the documented procedure clears the keyspace
before the restore.

## Options compared

| Criterion | A: extend fence and lineage (chosen) | B: independent restore epoch | C: bind each delete to the lineage |
| --- | --- | --- | --- |
| Data safety | Stops every delete sent, or landing on a node, after the leader first observes the clear, the lock or a replaced record. Does not cover what the leader cannot observe (boundary below): a mitigation, not a guarantee. | Needs a source of truth that a restore cannot roll back and that the old leader consults; outside the metadata store that is the nodes, which only learn a restore from `recovery begin`, i.e. too late for the same race. | Reads the lineage in each delete's decision transaction; does not cover a delete already sent, and a restore between decision and send matches the restored counter (the R-94(b) case itself). |
| Faults (node unreachable, leader kill, partition) | Local stop needs no node; the close push is best effort, an unreached node closes when its rejoin lease ends; a killed leader stops sending. | Epoch must reach every node before any delete; unreachable nodes stall deletes. | As today. |
| Restore resilience | Works while the store is locked or empty (no metadata write). | Needs an epoch store outside FoundationDB. | Depends on the store being readable. |
| Compatibility | No protocol, schema or format change. The close push is the existing fence push. | New RPC field or protocol 5, mixed-version handling. | Schema change for per-batch lineage records. |
| Performance | One read-only transaction per leader every 250 ms; no change per delete. | One epoch check per delete on the nodes. | One extra read (and conflict range) per decision transaction. |
| Implementation | One wrapper store, one probe task, two error variants. | New durable state, RPCs and an operator procedure. | Changes in GC, reconciliation, migration. |
| Operations | No manual step. | New operator step to advance the epoch. | No manual step. |
| Verifiability | Deterministic tests with held batches and held RPCs; each part removed makes a test fail. | Hard: needs an external store in tests. | Deterministic, but does not cover in-flight deletes. |

B fails its own requirement as stated: a counter that a restore cannot
roll back and that the running leader would consult does not exist
inside FoundationDB, and the storage nodes learn of a restore only
through `recovery begin`. C narrows the same window as A's
per-transaction check but leaves deletes already sent. A is the
smallest change that covers deletes already sent once the leader can
observe the restore. None of the three closes R-94(b): each depends on
someone observing the restore before the delete lands.

## Decision

### Errors

- `KvError::Locked`: FoundationDB 1038 (`database_locked`), no longer
  retried as a conflict. A restore or DR switch holds the lock.
- `KvError::Replaced`: a record that exists for as long as the store
  serves this cluster is missing: the leader record under a leader
  (`FencedTx::commit`) or the cluster state (`GuardedKv`). Previously
  these were `Fenced` / `ClusterState`.

### Trip wire (`controller::trip`)

Every store a leader's tasks use is wrapped (`TripKv`), and every error
of every operation goes through `Trip::observe`:

| Error | Action |
| --- | --- |
| `ClusterState` (phase or incarnation refuses the work) | deletes stop locally until the next successful health pass |
| `Fenced` (another leader or term) | deletes stop for the rest of the term (latched), the generation is closed on the nodes, the leader steps down |
| `Locked`, `Replaced` | as `Fenced`, and the process remembers it (`restore_suspected`, sticky across terms) |
| anything else (conflict, unknown commit, backend error) | nothing |

- **Latch.** `NodeClients::stop_deletes(latch)` takes the generation the
  process's deletes carry and, latched, makes every later
  `set_delete_fence` a no-op for that client set. A health pass already
  under way cannot reopen deletes after the trip. A new term creates new
  clients.
- **Close on the nodes.** The taken generation is pushed closed to every
  node the process knows (an existing fence push; no metadata needed). A
  node acknowledges a close only after the deletes it already admitted
  finished and refuses every later one of that generation, so a delete
  sent before the trip and still in flight is refused when it lands.
- **Step down.** The lease loop ends the term. The next term, started on
  whatever the store holds then, sees `restore_suspected` and closes the
  fence with an operator hold (`sync_leader`, reason "this leader saw the
  metadata store locked or replaced"); only `admin recovery begin` or an
  explicit release lifts it.
- **Store watch.** A probe every `store_watch_interval` (default 250 ms)
  reads the leader record and the cluster state in one read-only
  transaction: either missing → `Replaced`; a leader record of another
  holder or term → `Fenced`; a locked store fails the reads (1038). This
  bounds detection when every other task is idle or busy sending.
- **Renewal failure.** Any lease renewal error stops deletes locally (not
  latched) until a health pass succeeds: a leader that cannot show its
  lease does not delete. A lost lease stops and closes as `Fenced`.

Every controller delete goes through `NodeClients::delete_shard_if`,
which checks the process's fence before sending: GC, reconciliation
(which also removes the former placement after a migration), a
migration's cleanup of its own target copies, cache eviction and repair
cleanups. The trip covers all of them. Gateways delete only the partial
copies of a write they just made, under the fence they read; they have no
trip, and the nodes refuse those deletes once the fence is closed
(review correction).

Review fixes (Phase 3.11):

- The trip wraps the cluster-state guard: the guard's own refusals (a
  missing or changed cluster state) trip too. Before, the guard answered
  above the trip; the schedulers' bookkeeping through the fenced store
  still tripped at the end of each pass, so the exposure was the rest of
  a pass.
- A trip on a locked or replaced store closes the generation above the
  one in use on the nodes. Any later leader on restored metadata (another
  controller, or this one restarted, neither remembering the trip) then
  finds the nodes ahead of its record and holds the fence. A takeover
  (`Fenced`) closes the term's own generation, so that ordinary failover
  is unchanged.
- A lost lease probes the store and reports a replaced one; the
  generation a latched trip closes is the last one deletes carried, even
  after an earlier stop took it. The close above starts from the highest
  generation the leader knows (its record, the nodes' reports), also when
  its deletes never carried one (re-review).
- A transient database lock (a DR switch, a lock taken by hand) is
  treated as a restore: the fence is held until an operator releases it
  (`admin delete-fence release`). This is a deliberate fail-closed
  choice; nothing tells a lock without a rollback from one with.
- Process heartbeats are not written into a store without a cluster
  state: a cleared keyspace stays empty under processes left running.

## What is and is not guaranteed

Guaranteed (by the implementation, given the controller runs this
release):

1. No delete is sent by the leader process after it first observed the
   store cleared, locked or replaced.
2. No delete of that term's generation is applied by a node after the
   close push reached it, including deletes sent before the trip.
3. The next term on the restored metadata, by this controller or another
   one, deletes nothing until an operator runs `recovery begin` (or
   releases the hold), provided the trip's close of the generation above
   reached the nodes.

Not guaranteed (no information exists, or the guarantee is bounded):

1. **Deletes applied before the leader observes anything.** Between the
   operator's clear (or the restore's lock) and the leader's next metadata
   operation, at most one store-watch interval (250 ms by default) plus
   one round trip, a delete already admitted by a node completes. Such a
   delete was decided on metadata newer than the backup point; it is
   equivalent to the same delete completing just before the restore, and
   the recovery report classifies the affected objects (RPO of a metadata
   backup: ADR-0024, RFC-015 §5.3.1).
2. **A restore that completes entirely between two observations.** The
   watch looks for a missing record, another term, or the lock. A
   restore that is atomic for the leader (the clear and the backup's
   records in one step, or a rollback of the store's disks or of a
   storage snapshot), or a clear and a full `fdbrestore` within one
   watch interval, restores the same holder, token and incarnation and
   shows none of them: the deletes in flight apply. Only the lineage
   check of the next health pass or batch detects it, afterwards
   (`boundary_a_restore_between_two_observations_is_not_seen`: 6 of 6
   in-flight deletes applied). Shortening the interval does not close
   this.
3. **A node the close push does not reach** still applies the term's
   deletes that were already in flight to it, until its rejoin lease ends
   (30 s by default). No delete is sent after the local stop.
4. **A leader cut off from FoundationDB for the whole restore** observes
   nothing (its operations fail with transient errors). Deletes stop
   locally at the first failed renewal; deletes already in flight are
   not closed on the nodes, and those landing while the nodes' rejoin
   lease runs apply
   (`boundary_a_leader_cut_off_from_the_store_applies_deletes_until_the_rejoin_lease_ends`:
   3 of 3 within the lease applied, 0 of 3 after it).
6. **A restore done outside arca-lake** (`fdbrestore`, a DR switch, a
   disk or volume snapshot rollback) is not announced to the storage
   nodes by anything. Nothing in this design detects it except a leader
   that happens to observe it.
5. **Controllers of an earlier release** do not have the trip.

The runbook step `admin delete-fence close` before a restore remains the
only way to make the boundary empty: it closes every node before any
metadata changes.

## Next

Closing R-94(b) needs a mechanism under which a delete decided before a
restore cannot become a physical delete of data the restored metadata
needs, whoever observes the restore and however it was done. That is a
separate decision (ADR-0026, proposed).

## Consequences

- A transient FoundationDB error at renewal pauses deletes until the next
  successful health pass.
- A controller takes no leadership on a store without a cluster state
  record (`leader::try_acquire_initialized`): a keyspace cleared for a
  restore stays empty under controllers left running, as `fdbrestore`
  requires ("Attempted to restore into a non-empty destination
  database" otherwise, seen on a real cluster).
- A leader that sees another leader's record steps down at once instead
  of at its next renewal.
- One read-only transaction per leader every 250 ms.
- After any locked or replaced store, deletes stay held until an operator
  acts, even if the store comes back unchanged (a DR switch, a lock taken
  by hand).
- Tests: `it/delete_fence.rs` (`a_restore_under_a_running_leaders_held_gc_batch_loses_nothing`,
  `a_held_gc_batch_released_after_the_restore_loses_nothing`,
  `a_running_leaders_in_flight_deletes_are_refused_once_it_sees_the_keyspace_cleared`,
  `a_running_leaders_in_flight_reconcile_deletes_are_refused_once_it_sees_the_keyspace_cleared`,
  `a_running_leaders_in_flight_migration_cleanup_is_refused_once_it_sees_the_keyspace_cleared`,
  `a_running_leader_that_sees_the_store_locked_stops_its_in_flight_deletes`,
  `an_idle_leaders_store_watch_sees_the_keyspace_cleared`),
  `it/fdb_restore.rs::a_leader_left_running_through_fdbrestore_deletes_nothing_the_restore_needs`
  (real FoundationDB, `fdbbackup`/`fdbrestore`), the two `boundary_*`
  tests that pin the residuals, `leader` and `controller::trip` unit
  tests. With the trip disabled every one of the
  integration tests loses shards; without the close push the four
  in-flight tests do; without the store watch the idle-leader test does.
