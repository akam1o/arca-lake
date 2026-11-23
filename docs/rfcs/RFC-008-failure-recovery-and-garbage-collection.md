# RFC-008: Failure Recovery and Garbage Collection

- Status: draft (Phase 3.2 adds §8: scrubbing, classification, quarantine, stripe repair; Phase 3.9 adds §6.11: the delete fence)
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-003, RFC-004, RFC-005, RFC-007
- Related ADRs: ADR-0005 (single-owner chunks, tombstone queue with grace),
  ADR-0007 (commit resolution, reference-checked GC, repair completion),
  ADR-0015 (durable quarantine, controller-driven scrub, stripe repair)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

Shards live on storage nodes; the facts that make them meaningful (which
object, which generation, which placement) live in the metadata store. The
two are never updated in one transaction (RFC-003 §5.3). Every failure
between them leaves an intermediate state: shards without a committed
generation, a generation whose commit outcome nobody knows, a group whose
membership changed while shards were still being copied. Recovery must
drive every such state to one of two ends, "visible and fully placed" or
"invisible and reclaimed", without ever deleting a shard that metadata
still accounts for.

## 2. Goals

- Name the intermediate states and the recovery rule for each.
- State the invariants that the recipes maintain and the tests check.
- Define garbage collection so that time is never the criterion for
  deleting a shard; it is at most a guard.
- Define when a repair is complete and in which order metadata changes.

## 3. Non-goals

- Scheduling, rate limiting, and leader election of the controller
  process (Phase 2).
- Tiering and cache placement recovery (RFC-006).

## 4. Invariants

| ID | Invariant | Maintained by | Checked by |
| --- | --- | --- | --- |
| I1 | The current pointer of a key names the generation of the last successful commit for that key, or nothing. | `commit_put`, `delete_object` | model test, E2E visibility tests |
| I2 | A `Retiring` generation is referenced by no current pointer and no version record. | `commit_put` retires only the displaced `null` version; `delete_object` likewise; `abort_put` and expiry act on `Pending` only | model test; GC re-verifies before deleting (§6.2) |
| I3 | A `Committed` generation is never retired by GC and never removed. | `finish` requires `Retiring`; `expire_pending` requires `Pending` | `commit_unknown.rs`, model test, `gc_safety.rs` |
| I4 | Expiry retires only generations that were `Pending` with `lease_until <= now`. | `expire_pending` re-reads the header in its own transaction | model test |
| I5 | Once a generation leaves `Pending`, its commit fails; once it is `Committed`, abort and a second commit fail. | state checks inside each transaction | `commit_unknown.rs` |
| I6 | A shard is deleted only when metadata, read in one transaction, does not account for it (§6). | controller GC and reconciliation | `gc_safety.rs`, `transport.rs` |
| I7 | A stripe group is `Active` only when every current member holds a shard for every chunk ID known to any member, with equal lengths, and the rebuilt member's shards verify (§7). | repair completion check | `repair.rs`, `repair_completion.rs` |
| I9 | Every shard delete or repair write carries the group epoch of the fenced transaction that decided it; nodes reject lower epochs (§6.4). | controller GC and repair; node fencing | `repair_completion.rs::delete_decided_under_an_old_epoch_…` |
| I10 | At most one controller commits metadata changes; a controller that lost its lease cannot commit after its successor's first transaction (RFC-011). | `FencedKv` | `controller.rs` |
| I8 | A writer holding an epoch older than a member's enforced epoch cannot write to that member. | node fencing (RFC-004 §5.4) | `e2e.rs`, `repair.rs` |
| I11 | A repair writes a shard only if the chunk reconstructed from the sources decodes to bytes whose CRC-32C equals the chunk record's `crc32c` and whose length equals `len` (§8.5). | `stripe_repair::reconstruct` | `integrity.rs`, `repair_completion.rs` |
| I12 | Every repair write carries the epoch of the group record read in the task's planning transaction; a node with a higher enforced epoch rejects it and the task re-plans (I9 for repairs). | stripe repair worker; node fencing | `integrity.rs::epoch_change_during_stripe_repair_…` |
| I13 | Quarantine changes what a node *serves*, never what metadata *references*: a quarantined shard is reported as absent to readers and repair, its bytes stay on the device until the record is superseded or tombstoned and its segment compacted, and no metadata is written by the node. | storage engine quarantine log (§8.2) | `storage/tests/quarantine.rs` |
| I14 | A shard that is quarantined, missing, or fails verification is never a reconstruction source and never a read source; only shards whose block CRCs verified on the node and whose length matches the stripe layout are. | `read_coded`, `stripe_repair::classify` | `integrity.rs`, `suite.rs::corrupt_shard_…` |
| I15 | A stripe with fewer than `k` verifiable shards of a live chunk is recorded as `Unrecoverable`; readers return an integrity error and never bytes for it; the record is cleared only by a verification that finds `k` good shards again. | repair task state; `read_durable` | `integrity.rs::unrecoverable_stripe_…` |
| I16 | Scrubbing and stripe repair never delete a shard; deletion remains the GC and reconciliation path under I6. | code structure (no delete RPC in the scrub/repair modules) | review; `integrity.rs` asserts shard counts |

## 5. Intermediate states and recovery

### 5.1 Write path (gateway)

| State | How it arises | Recovery | Verified by |
| --- | --- | --- | --- |
| Shards written, no manifest entry | Gateway died after `PutShard` acks, before `append_chunks` | Generation stays `Pending` until its lease ends, then expiry retires it and `finish` removes it. Its shards have no chunk record: reconciliation deletes them (no generation with that prefix) once older than the minimum age. | `crash_after_shards_before_manifest_…` |
| Manifest entries, no commit | Gateway died before `commit_put` | Expiry, then collection deletes the recorded shards, then `finish`. | `crash_before_commit_…`, `pending_generation_is_never_collected_before_its_lease_…` |
| Handled error before commit | Node failure, body error, injected fault | Gateway aborts (`abort_put`): generation `Retiring` at once, collected after grace. | `crash_before_commit_…`, `get_with_one_node_stopped_…` |
| Commit outcome unknown | Lost reply, timeout, `commit_unknown_result` | §5.2 | `commit_unknown.rs`, `faults.rs`, `faults_fdb.rs` |
| Committed, client not answered | Gateway died after commit | Object is visible; client retry is harmless (`If-None-Match: *` returns `PreconditionFailed`, which is the truth). | `crash_after_commit_…` |
| Chunk diverted to another group | Node of the first group failed or lost its reply after applying | Shards on the first group have a chunk record placed elsewhere: reconciliation deletes them by reference. | `lost_reply_on_put_shard_…` |

### 5.2 Commit outcome unknown (RFC-003 §5.8)

```text
commit_put ──ok──▶ visible
    │
    └─unknown─▶ resolve_commit ─Committed─▶ visible (success)
                      │
                      ├─Pending────▶ retry commit (bounded) ──┐
                      │                                       │ still unknown
                      │                                       ▼
                      │                              abort_put (fence)
                      │                                 ├─ok──────────▶ invisible, retiring
                      │                                 ├─"committed"─▶ visible (success)
                      │                                 └─error───────▶ pending; lease expiry decides
                      ├─PendingExpired─▶ abort_put (same as above; never retry the commit)
                      ├─Retiring/Missing─▶ failure (nothing visible)
                      └─error────────▶ failure reported as retryable; generation stays pending
```

Why the fence works: `abort_put` reads the generation header and writes
it. A commit that is still in flight also reads and writes the header.
Under the store's serializable isolation at most one of them commits; the
other fails with a conflict. So after a successful abort no late commit can
publish, and an abort that finds `Committed` proves the commit landed.
Expiry (`expire_pending`) is the same transaction shape and gives the same
exclusion for a gateway that died before deciding.

Idempotent steps (`begin_put` with a pre-chosen generation ID,
`append_chunks`) are simply retried on an unknown outcome
(`transact_idempotent`). `commit_put`, `abort_put`, `delete_object`,
`expire_pending`, and `finish` are never retried blindly; the gateway
resolves as above, GC defers to the next pass (every GC step re-reads the
state it changes). `DELETE` on an unversioned bucket is retried once
because a repeated delete is a no-op there.

### 5.3 Garbage collection and the controller

| State | How it arises | Recovery |
| --- | --- | --- |
| Retired generation, grace not elapsed | Overwrite, delete, abort, expiry | Wait; readers that already resolved a manifest can still fetch its shards (`delete_during_get_…`). |
| Shards deleted on some members, `finish` not reached | Controller died or a member was unreachable | Next pass re-verifies (§6.2) and re-issues deletes (NotFound counts as done) before `finish`. |
| `finish` outcome unknown | Lost reply | Next pass: `finish` is idempotent (a missing generation is success). |
| Expiry outcome unknown | Lost reply | Next pass re-reads: either already retired or still pending and expired. |
| Shards of a finished generation on a former member | Member was replaced before GC ran | Reconciliation on that node: no generation → delete (if old enough) or not a member → delete. |

### 5.4 Membership change and repair (RFC-004 §5.7)

| State | How it arises | Recovery |
| --- | --- | --- |
| Members fenced, membership not committed | Controller died between steps 1 and 2 | Writers are rejected by fenced members, refresh the map, find the old record, and keep failing until the controller re-issues the change (RFC-004 §7). Reads are unaffected. Tested in `stale_epoch_writes_are_fenced_…`. |
| Membership committed, reconstruction partial | Controller died, replacement reset, operator stopped it | Group stays `Repairing` with a per-member cursor; re-issuing the same command resumes; reads decode from the remaining members meanwhile. `controller_crash_mid_repair_…`, `connection_reset_during_repair_…`. |
| Second membership change during repair | Operator or health system acts on another member | The running repair stops with `EpochChanged` at its next metadata or node write; the second change keeps the group `Repairing` because its completeness check fails; re-issuing the first repair completes under the new epoch. `membership_change_during_repair_…`. |
| Chunk deleted during repair | DELETE + GC while rebuilding | Repair finds fewer than `k` shards, checks metadata, finds no live generation, skips. Any shard it wrote before GC ran is reclaimed by reconciliation. `delete_and_gc_during_repair_…`. |
| Old member returns after replacement | It missed the fence | It still accepts the old epoch but is not a member: readers never consult it; reconciliation deletes its copies once the group is `Active`. `stale_epoch_writer_and_returning_old_member_…`. |

## 6. Garbage collection rules

### 6.1 Sources

1. `Pending` generations whose lease ended → retired by `expire_pending`.
2. `Retiring` generations past the grace period → shards deleted on the
   current members of their groups, then `finish`.
3. Shards that metadata does not account for → deleted by reconciliation.

### 6.2 Reference check before deletion

For every generation in the retired queue, one transaction re-reads the
header and requires: state `Retiring`; the current pointer of
`(bucket, key)` does not name the generation; no version record of the key
names it. Anything else is skipped and counted (`skipped_referenced`). A
non-zero count is an invariant violation to investigate, not something to
act on. Because no recipe moves a generation out of `Retiring`, the check
does not need to be in the same transaction as the deletes.

### 6.3 Reconciliation (node-driven)

For each shard a node lists for a group, one transaction classifies it:

| Metadata says | Decision |
| --- | --- |
| A generation with the shard's prefix is `Pending` | keep (the writer may still append or commit) |
| A generation with the prefix has a chunk record for the shard's index placed in this group | keep |
| Generations with the prefix exist, none pending, none places this chunk here | delete (diverted or failed write) |
| No generation with the prefix exists, shard older than `min_age` | delete (finished generation, or `begin_put` never committed) |
| No generation with the prefix exists, shard younger than `min_age` | keep (a `begin_put` may be in flight or retrying) |
| The node is not a current member of the group and the group is not `Repairing` | delete (never read) |

`min_age` applies to the no-generation case only and must be at least the
pending lease. It guards against the writer's clock (which stamps the chunk
ID) being ahead of the controller's; it is never the reason a shard with
metadata is deleted.

### 6.4 Deletion epochs

Metadata transactions and node RPCs are not atomic together, so every
decision to delete or write a shard is made in a (fenced) transaction and
then carried out by an RPC that may arrive arbitrarily late. The RPC
carries the group epoch the decision was made under, and a node rejects
any `DeleteShard`/`PutShard` whose epoch is lower than the one it was last
fenced to (RFC-004 §5.4). A membership change fences the replacement
before committing the new membership (`repair::fence` fails otherwise).
Hence a reconciliation that decided "this node is not a member" under
epoch `E` cannot delete a shard of a node re-added at epoch `E+1`: the
node learned `E+1` before it became a member and rejects the `E` delete.
*Verified*: `repair_completion.rs::delete_decided_under_an_old_epoch_is_rejected_after_a_fence`.

### 6.5 Paged listings

`ListShards` and `VerifyShards` return pages in key order after a cursor.
A page is a snapshot of the node's index when it was served; across pages
the walk is not a snapshot:

| Event during a walk | Effect | Why it is harmless |
| --- | --- | --- |
| Shard inserted with a key before the cursor (a writer with a slower clock) | Missed by this walk | Every consumer re-runs: GC and reconciliation on their schedule, repair until the completeness check passes |
| Shard inserted after the cursor | Seen | Normal |
| Shard deleted after being listed | Stale entry | Repair reads it and finds it missing (treated as absent); reconciliation's delete gets `NotFound` |
| Node restarts between pages | Continues from the cursor; the index is rebuilt deterministically | Keys are totally ordered and stable |
| Membership epoch changes | Repair stops with `EpochChanged` at its next write; GC deletes with the old epoch are rejected (§6.4) | Re-run under the new record |

Duplicates are impossible within one member's stream (strictly increasing
keys) and across members because the walker merges by chunk ID. Memory is
O(page size × members). *Verified*: `paging.rs`.

### 6.6 Why verify-then-delete has no TOCTOU hole

The question is whether a shard that passed the check in §6.2 or §6.3 can
become needed between the check and the delete RPC. Three facts about the
metadata state machine answer it:

1. **No transition leaves `Retiring`.** A generation that is `Retiring`
   stays so until `finish` removes it; nothing re-references it (I2, model
   test). So §6.2's check holds at delete time.
2. **Chunk IDs are never reused.** A chunk ID is the generation's UUIDv7
   prefix plus an index; a new generation always has a new prefix. A
   delete can therefore never hit a shard written later by a different
   generation under the same key. A retried write of the *same* generation
   writes identical bytes.
3. **`begin_put` commits the header before any shard is written.** A shard
   whose prefix has no generation (§6.3, "no generation") can only mean the
   generation was finished, or `begin_put`'s own retry is about to write
   the same header; the age guard covers the latter.

What these facts do not cover is membership: a node that is not a member
when the decision is made and becomes one before the RPC arrives. That is
the case §6.4 closes with the deletion epoch. Alternatives compared:

| Option | Guarantee | Cost | Decision |
| --- | --- | --- | --- |
| Verify-then-delete with chunk-ID uniqueness and deletion epochs | Equivalent to atomicity for the cases above | One extra epoch check on the node (already present for writers) | **Chosen** |
| `Deleting` reservation state in metadata before the RPC | Same, since `Retiring` is already terminal; would add a state and a transaction per generation | One more transaction per collected generation | Not needed |
| Node-side tombstones with a deletion epoch | Lets a node refuse deletes older than its last membership change even without the fence | Per-shard tombstone state on nodes | Subsumed by epoch fencing |
| Generation tombstone kept after `finish` | Would let reconciliation distinguish "finished" from "never existed" | Unbounded tombstone growth | Not needed: both cases are garbage |

A long-stopped controller resuming is the same case as a stale leader: its
decisions were made in fenced transactions that fail once a successor
exists (RFC-011 §5.3), and RPCs it issued before losing leadership carry
epochs that current members check.

**Phase 3.5 amendment (audit CON-5, CON-6).** Fact 2 holds for
generations but not for *placements*: tier migration writes a committed
chunk's shards into a group under the same keys, and a chunk migrated
A→B→A has its keys in A twice. A delete decided before the return (by
reconciliation, or by the first migration's eager source cleanup) could
arrive after it and remove the shards the record now names; the group
epoch does not change between the two placements. Two mechanisms close it:

- **Conditional deletes.** A node returns each record's sequence from
  `PutShard` and `ListShards`; `DeleteShard.if_sequence` deletes only if
  the key's newest record (appended, live or quarantined) still has that
  sequence, checked and written under the store's writer lock, and
  answers `ABORTED` otherwise. Reconciliation (durable and cache) deletes
  only the records it listed; a migration cleans up only what it wrote and
  leaves its source to reconciliation. GC of retired generations stays
  unconditional (fact 2 holds for them).
- **Migration intents** (RFC-006 §5.8): reconciliation keeps a group's
  shards of a chunk while a migration into that group holds the chunk's
  intent.

A delete token is the pair (store epoch, sequence), and the store takes a
new random epoch (64 bits) at every open: a sequence is unique within an
epoch, and a node refuses (`ABORTED`) every token of another epoch. So no
token handed out before a restart can match a record written after it,
whatever happened to the directory in between: compaction dropping the
records with the highest sequences (CON-10), a lost or damaged bound file
(CON-14, CON-16), a directory restored from an older copy (CON-18). The
guarantee is probabilistic only in that two epochs collide with
probability about 2^-64 (CON-20). The directory lock is an advisory OS
lock: it assumes a local file system (on NFS it depends on the server's
lock support) and that nobody replaces the directory or `store.lock`
while the node runs. A delete decided before a restart is refused
and redone after the next listing. The store holds an exclusive lock on
its directory while open (CON-19). Sequence arithmetic never wraps;
exhaustion fails the write (CON-17, CON-21). `sequence.bound` only keeps
sequences increasing across opens; a damaged file is reported in the open
report and ignored.

Records reach the in-memory index after group commit in any
order; the index keeps a runtime tombstone until every older record has
been applied, so a conditionally deleted key is never resurrected by an
older put that was still on its way (CON-9); a write that fails or panics
on its way leaves the in-flight set too (CON-13), and a group-commit
sync leader that fails or panics hands the leadership on (CON-15). `ListShards` reads live
and quarantined keys from one index snapshot per page (OPS-5, OPS-6).

Conditional deletes need wire protocol 2 (Phase 3.5; a version-1 node
ignores the new fields and deletes unconditionally). A client sends a
conditional delete only to a node that negotiated version 2 and always at
version 2 or higher, so a node rolled back to a version-1 binary since the
negotiation refuses it (outside its window) instead of deleting (CON-22).
An older node reports sequence 0 and would ignore `if_sequence`.
Reconciliation and migration cleanup fail closed there: they delete
nothing on such a node, and its orphans are reclaimed after its upgrade
(CON-12). Cache population, repair, invalidation and eviction still delete
cache copies unconditionally (INT-1/INT-2, Medium): a lost cache copy is a
miss, never wrong data, because cached chunks are verified against the
chunk record and reads fall back to the durable tier.

### 6.7 Clock skew

Safety never depends on clocks agreeing: every decision is a transaction
that reads the state it changes. Skew affects liveness only. One
exception, explicit since Phase 3.12 (ADR-0028): deferred reclamation
bounds the protection of restores by wall clocks, through the nodes'
retention and an allowed skew `ε` (`--reclaim-skew-secs`); skew beyond
`ε` narrows that protection. No other delete decision depends on clocks. A controller
clock far ahead retires pending generations before their writers finish;
those writers then fail at their next append or commit and the client
retries, nothing becomes visible (`expiry_and_commit_exclude_each_other`,
`pending_generation_is_never_collected_before_its_lease_…`). A controller
clock far behind delays reclamation.

### 6.8 Capacity, admission and backpressure (Phase 3.6, ADR-0020)

**Multipart GC.** A completed upload's root is retired like any
generation. When GC collects it (after the grace), root steps of at most
256 references per transaction retire its part generations *as of the
root's retirement time*, then clear the references and leftover part
records; the root is finished once none is left (I-MP2). A reader reaches
the parts only through the root, so the grace the root served covers
them; parts never listed in the completion have been unreachable since
it. Before Phase 3.6 the parts were retired as of the root step and
served a second grace, so every multipart object kept its data for two
grace periods (R-64). A controller pass runs another batch when a root
step retired parts (they are due at once). An aborted upload's parts are
retired by `discard_parts` at the abort and serve their own grace. Each
generation costs three metadata transactions (verify, finish, and the
root step for roots); 10 000 parts are retired in 40 steps.

**Linearization point of physical capacity.** Storage nodes admit every
`PutShard` before taking its body: the write's payload plus a record
overhead must leave the device at or above its class's floor, counting
every admitted write not yet ended. The free-space sample, the check and
the hold are one critical section on the node (a sample taken before it
could predate writes stored meanwhile); the hold ends when the RPC ends (stored,
failed or cancelled). This is the only point where space is decided;
whatever the gateways or the controller believe, no two writes can be
admitted against the same bytes.

| Class | Floor | Writers |
| --- | --- | --- |
| client | reserve | gateways (PUT, UploadPart, copies) |
| migration | reserve | tier migration |
| cache | reserve | cache population (cache nodes) |
| repair | repair floor | member rebuild, stripe repair, cache shard rebuild |
| delete | none | GC, reconciliation, eviction (a tombstone frees space) |

The reserve is `max(2 × segment_max + 64 MiB, 2 % of the device)`, at most
a quarter of it (`--reserve-bytes` overrides); the repair floor is one
segment and 16 MiB, kept for a compaction output and a segment rollover.
The reserve also covers quarantine sidecars and administrative writes.

**Reservation state machine.** A hold has two states, `held` (admitted)
and `released` (the RPC ended); nothing is persisted. Gateway crash: the
stream ends, the node releases. Node crash or restart: there are no holds
after it, and the device's free space already reflects what was stored.
CommitUnknown: holds do not involve metadata; the shards written for a
generation whose commit outcome is unknown are the generation's like any
others (collected if it never becomes visible, §5.2), and the space they
take is visible to the next admission through the device's free space.
Shards of a chunk that a member refused are an incomplete stripe of a
pending generation: no chunk record names them, and reconciliation
removes them once the generation is gone (§6.3).

**Backpressure states** (per tier, controller leader, every health pass):

| State | Entered when (any) | Gateways |
| --- | --- | --- |
| `recovery_only` | usable free < 2 %; no fresh capacity report from any member of the tier | refuse new data and new uploads |
| `write_rejected` | usable free < 5 %; every writable group lacks room (a member with room for fewer than 64 maximum shards, or no member heard from at all) | refuse new data and new uploads |
| `throttled` | usable free < 10 %; full within 1 h at the free-space trend or the GC backlog's growth (each over ≥ 5 min) | new data at GC's reclaim rate plus headroom / 1 h (client bytes, ≥ 1 MiB/s), split over gateways |
| `warning` | usable free < 20 %; full within 4 h; a member's report stale | admit |
| `normal` | otherwise | admit |

Usable free is the tier's free bytes above the devices' reserves (minus
writes in flight) over their size minus the reserves. A worse state
applies at once; a better one needs its thresholds 3 points better for
the dwell time (60 s) and is entered one step at a time, so that writes
come back gradually; the throttle's token bucket starts empty, carries
the reclaim-rate floor whenever the applied state is `throttled` (also on
the way back), and may go at most 30 s of rate into debt. Reports
are judged by the controller's receipt time. Deletes, aborts,
CompleteMultipartUpload, bucket operations and reads are never refused
for capacity.

**Crash recovery of the state.** The state machines live in the leader's
memory. A new leader starts every tier at `normal` and evaluates from
fresh reports at its first health pass; until it has heard a member, the
tier is `recovery_only` (fail closed). Gateways treat the published
record as at least `throttled` once unchanged for its TTL (30 s) and as
`write_rejected` after 5 min (no leader evaluates capacity); a record
already older than its TTL plus 60 s by the gateway's clock when first
seen (a gateway started while no leader publishes) is aged accordingly;
with no
record at all (a controller that predates it) they admit, and the nodes'
reserve holds. A metadata outage keeps the gateway's last record, which
then ages as above.

**Errors.** Capacity refusals are `503 SlowDown` with `Retry-After`; the
message says whether the tier is throttled, refuses new data, or no group
had capacity. Other placement failures and metadata failures stay
`503 ServiceUnavailable`; authorization failures `403`. A node answers
`RESOURCE_EXHAUSTED` for a capacity refusal and for ENOSPC/EDQUOT from
its store; it is not retried by the node client and does not close the
connection.

**Durable vs cache capacity.** Durable tiers are governed by the state
machine above and the nodes' reserve. Cache tiers are bounded by the
cache capacity and watermarks (RFC-006 §5.4); their nodes keep the same
reserve against cache population (class `cache`), and their admission
state is published but does not gate client writes. The GC backlog is
charged to the durable tiers.

**Compaction and repair without room.** Compaction skips a segment whose
live bytes do not fit in the free space (`skipped_no_space`); repair
writes stop at the repair floor (`RESOURCE_EXHAUSTED`), the task is
retried by the repair scheduler once space returns.

### 6.9 Admission of every write path (Phase 3.7, ADR-0021)

| Operation | Size known up front | Physical bytes written | Admission (node: physical) | Throttle (gateway: bandwidth) | After abort / CommitUnknown | Retry double counting |
| --- | --- | --- | --- | --- | --- | --- |
| PutObject (with or without `Content-Length`, `aws-chunked`) | not relied on | shards of each chunk, as placed | every PutShard (client class) | header check (state only), then each chunk as it is formed, before its shards, once per chunk | pending generation expires or is aborted; its shards are GC's; holds end with the RPCs | a chunk retried on another group is not charged again; a client retry is a new request and writes again (charged again, correctly) |
| UploadPart | not relied on | as PutObject | as PutObject | as PutObject | part generation retired by the next part, abort or sweep | as PutObject |
| CopyObject, UploadPartCopy | source size, not relied on | destination chunks (the source is read, not charged) | every PutShard | header check, then each destination chunk; the source version is fixed at the manifest read | destination generation as PutObject; the source is untouched | as PutObject |
| Versioned / conditional PUT, Complete with conditions | — | as PutObject (Complete: none) | as PutObject | as PutObject; Complete writes no data and is never refused | conditions are evaluated at commit; CommitUnknown per RFC-003 §5.8 | — |
| CompleteMultipartUpload, AbortMultipartUpload, DELETE, DeleteObjects | — | metadata only (tombstones later, by GC) | deletes never refused | never refused | idempotent re-runs (ADR-0019) | — |
| Tier migration | chunk size | target shards | migration class (stops at the reserve) | none; tiering is skipped while any tier is `throttled` or worse | intent expires; target shards reconciled | per chunk |
| Cache population | chunk size | cache shards | cache class | none (cache capacity and watermarks) | copy deleted by its tokens or reconciled | per chunk |
| Member repair, stripe repair, cache shard rebuild | shard size | shards | repair class (stops at the repair floor) | none (repair budgets, RFC-008 §8.5) | task retried | per shard |
| Compaction | live bytes of the segment | output segment | skipped without room for the output | none | output removed, source kept | — |

The node is the linearization point for physical space in every row;
the gateway's throttle is the early, soft limit on client bandwidth.

**Leadership terms.** The admission record carries the leader's fencing
token and a generation; it is written through the fenced store, so a
former leader's delayed write fails. A new term (failover, or the same
process leading again) starts the state machines afresh; until every
member of a tier has reported in the term (or the staleness bound has
passed) the tier keeps at least the state published before the term
(`throttled` when there was none) and keeps the groups it excluded, and
it then relaxes only through the hysteresis starting from that state.
Gateways age a record that stops changing (`throttled` after its TTL,
`write_rejected` after 5 minutes), and age a record already older than
its TTL plus 60 s by their own clock when they first see it (a gateway
started while no leader publishes). Which record they take is §6.10.

**Shared file systems.** Nodes report a capacity domain (the device
number of the store's file system scoped to the kernel's boot id,
flagged uncertain for overlay, FUSE and network file systems), whether
they have an allotment and the file system's size. Nodes on one domain
whose allotments are missing or exceed the file system are conflicts:
their groups leave placement (the tier refuses new data with the
reason) unless `--allow-shared-filesystem`. Uncertain or unreported
domains without an allotment are treated the same way: a file system
that cannot be checked is not assumed unshared (an allotment is the
operator's statement that the node's share is its own). `admin
check-config` prints both and fails on either.

**Responsiveness.** Worse states follow the long trend (5 min); a better
one may be entered when the recent trend (1 min) no longer calls for the
current state, once the recent window is covered by samples (not right
after a start or a new term). The throttled rate starts at GC's reclaim rate plus
headroom over the horizon, grows additively while the recent trend keeps
the tier the horizon from full (and every member's report is fresh and
the recent window covered; otherwise it holds, or returns to the formula
when a report is stale), halves within half of it, and stays between
that formula and eight times it. A chunk is owed in full: the throttle
debt is not capped, so it stays below the bounded wait's worth of rate
plus one chunk.

**Placement.** The record carries each group's room (its fullest
member's headroom); gateways place new chunks in the groups with the
most room. Existing data is not moved (no automatic rebalancing).

### 6.10 Reserve layers, record fencing and rebalancing (Phase 3.8, ADR-0022)

**Reserve layers.** From the device's end: the emergency space (quarantine
log, sequence bound: KiB) and one segment plus 16 MiB for compaction
output and rollover form the repair floor; the delete reserve sits on
it, one tombstone record (one record alignment, 4 KiB by default) per
record the store holds plus 1 024; repair may write down to their sum;
client writes, migrations and cache population stop at the client
reserve above it. Deletes are never refused: their tombstones use the
delete reserve, so deleting every record of a full node never reaches
the repair floor (Phase 3.7 Test C: without it, the drain took a node
11 MiB below its reserve). DeviceCapacity reports the delete reserve and
includes it in both floors. The node also reports where its bytes went:
written bytes by record kind and write class, side files, the file
system's use and the overhead beyond the store's files.

| Write | Floor (free bytes it must leave) |
| --- | --- |
| client, migration, cache population | client reserve + delete reserve |
| repair | repair floor + delete reserve |
| compaction | its output's live bytes + 1/64 + 1 MiB of free space, else skipped |
| tombstone (delete) | none (budgeted by the delete reserve) |
| rollover header, quarantine log, sequence bound | none (inside the repair floor) |

**Record fencing.** The admission record carries the cluster
incarnation, the leader's fencing token (its term) and the term's
publication generation. A gateway takes a record only of its own
incarnation, and never one of a lower (term, generation) than the record
it took last, however late it was computed (Phase 3.7 also took a lower
token computed more than 60 s later, meant for a restore that set the
leader counter back; a restore changes the incarnation instead, and
gateways do not serve across an incarnation change). A record that
disappears, or that is ignored, leaves the last one in force, ageing. A
gateway that has seen no record since it started admits for 60 s (a
leader's first pass), then ages from its start: an unknown state fails
closed. A tier the record does not name is `throttled`. A new
incarnation is a new term for the leader's state machines.

**Entering on the recent trend.** Once the recent window is covered, a
worse state on time to full needs the recent trend to agree with the
long one; free-fraction thresholds always apply (`backpressure_sim`:
after a burst with free space growing, needless throttling 200 → 60-82 s
and 7 → 3 transitions, other conditions unchanged).

**Rebalancing.** With `--rebalance`, the leader moves chunks from the
least roomy group of a tier to the roomiest group of the same tier,
scheme and shape, through the tier migration path (intent, copy, read
back, switch conditioned on the placement read; source shards to
reconciliation). A pass does nothing while paused, while a group is not
`Active` or a repair task is open, or in a `recovery_only` tier; writes
its targets as migration class (the node refuses them at the client
reserve and the pass stops); moves at most its per-pass budget and half
the rooms' difference; expires crashed migration intents; persists its
cursor, so that a new leader resumes there. Imbalance start 25 %, stop
10 %. `admin rebalance status|pause|resume|dry-run`. The source's space
returns once reconciliation deletes the moved shards and compaction
reclaims their segments.

### 6.11 Deletes across restores and phase changes (Phase 3.9, ADR-0023)

§6.6 holds within one metadata history. A metadata restore breaks its
first fact: a generation that was `Retiring` (or already finished) when a
delete was decided is referenced again in the restored metadata. A phase
change does not break it, but the phase says no delete should run. In
both cases the delete RPC has already left the controller. So the
condition is decided where the delete is applied:

- Every `DeleteShard` carries a delete fence generation. A storage node
  applies it only while it holds that generation open, checked under the
  shared guard its physical delete holds to the end. A fence change takes
  the exclusive guard and is persisted before it is acknowledged.
- The leader opens a generation for its term only while the cluster is
  `Active` in its incarnation and no operator hold is set. Generations
  only grow and a closed one never reopens, so a delete decided under an
  earlier generation (an earlier term, before a phase change, before a
  restore) is refused by every node that adopted a later one.
- Before a restore the fence is closed, with a hold, on every node, each
  acknowledging it. Only a controller of the incarnation `recovery begin`
  creates, or an explicit release, lifts the hold. A delete decided on
  the metadata before the restore is therefore never applied after it.

Every path that removes shards on a node goes through this check: GC,
reconciliation, cache eviction and cache reconciliation, migration and
rebalancing clean-up, and gateway clean-up of partial writes. Node-local
removal (compaction of superseded records, discarding a segment
truncated at creation) involves no external decision and is not fenced.
The audit of every path is in `docs/verification/phase-3.9-data-safety.md`.
What the fence refuses is kept and retried: GC defers the generation,
and reconciliation counts the shard as kept.

## 7. Repair completion and metadata order

1. Fence reachable members (`UpdateGroupEpoch`); the replacement must
   acknowledge, otherwise the change is not started (§6.4).
2. Commit `{members[index] = replacement, epoch + 1, Repairing,
   cursor = (index, 0)}` in one transaction that fails if the record
   changed since it was read. The cursor names the member being rebuilt so
   that any controller can resume (RFC-011 §5.5).
3. Walk the group in chunk-ID order (§6.5). For each chunk the replacement
   lacks or holds corrupt (per `VerifyShards`): read `k` shards,
   reconstruct, and **validate** before writing: the chunk's recorded
   scheme and `k+m` equal the group's, its recorded placement is this
   group, and the reconstructed chunk's CRC-32C equals the recorded one.
   Write with the new epoch and require the node's returned CRC to match.
   Persist `cursor = (index, chunk_id)`. Stop with `EpochChanged` if a
   write is fenced or the record moved; stop with `Corrupt` or
   `Unrecoverable` without writing anything if validation fails or fewer
   than `k` shards of a live generation exist.
4. **Completeness invariant (I7).** The group becomes `Active` only when,
   in one walk, every member answers, lists every chunk, with equal payload
   lengths across members, and the replacement's shards (optionally every
   member's) pass `VerifyShards`. Otherwise the group stays `Repairing` and
   the cursor is cleared so that the next run walks everything again.

| Property checked at completion | How | Test |
| --- | --- | --- |
| Shard existence on every member | walker presence | `completion_requires_every_member_to_answer`, `paging.rs` |
| Length consistency | `payload_len` per member | `repair_completion.rs` (length mismatch counter) |
| Byte integrity of the rebuilt member | `VerifyShards` (block checksums) | `corrupt_shard_on_the_replacement_blocks_completion_until_rewritten` |
| Byte integrity of other members | `VerifyShards` with `verify_all_members` | `corrupt_source_shard_is_not_propagated_and_blocks_full_verification` |
| EC profile and placement of each rebuilt chunk | chunk record vs group record | `reconstruct_one` (error `Corrupt`) |
| Generation liveness | `generation_is_live` | `delete_and_gc_during_repair_…` |
| Whole-chunk checksum of rebuilt data | recorded CRC-32C vs reconstructed | `reconstruct_one`; a corrupt source never propagates |
| Placement epoch | writes fenced; `EpochChanged` stops the run | `membership_change_during_repair_…` |

Shards of the replaced member are never deleted by repair; reconciliation
reclaims them after step 4 (§6.3, last row).

## 8. Integrity: scrubbing, classification, quarantine and stripe repair

Phase 3.2. Silent corruption is found by reading shards back, classified
against the metadata, kept away from readers by a durable quarantine, and
repaired by reconstructing the affected shards from the surviving ones
without changing membership or epochs. Member replacement (§7) stays the
answer to a failed node; this section handles shards.

### 8.1 Verification levels

| Level | What is checked | Where | By |
| --- | --- | --- | --- |
| L1 record header | magic, version, flags, key (group, chunk id, shard index) equals the index key, payload length, sequence equals the index entry, header CRC-32C, physical location (segment exists, record lies within the segment's valid length) | storage engine `get` | every read, scrub |
| L2 payload | CRC-32C of every payload block (RFC-005) | storage engine `get` | every read (covered blocks), scrub (all blocks) |
| L3 metadata agreement | a live chunk record places the chunk in this group (RFC-004 §5.1 prefix → generation); the shard's length equals the stripe layout's shard length for `(len, k, m, scheme)`; the member holding it is a current member; the chunk record's placement epoch is not newer than the group's | controller scrub | per shard |
| L4 stripe | at least `k` shards verify at L1–L3; the chunk assembled from `k` of them has the chunk record's `crc32c` and `len` | gateway `read_coded`, controller repair | every read, every repair |

Multipart objects add nothing to L1–L4: a part generation's chunk records
are ordinary chunk records (RFC-007 §5.3). The manifest level (root
references ↔ part headers, part checksums, sizes) is checked by the
manifest scrub (§8.6).

### 8.2 Node-side verification and durable quarantine

**`ScrubShards` RPC** (additive to `VerifyShards`): a page of the node's
index entries for a group, each read in full through the validating read
path, returning `ShardScrub { key, payload_len, crc32c, sequence, status }`
with `status ∈ { OK, CHECKSUM_MISMATCH, INVALID_HEADER, TRUNCATED,
QUARANTINED, IO_ERROR }`. The node quarantines on `CHECKSUM_MISMATCH`,
`INVALID_HEADER` and `TRUNCATED`; it never quarantines on `IO_ERROR`
(a transient read failure must not look like corruption) and it never
quarantines on a listing or on the metadata-level classes below, which
only the controller can see. `VerifyShards` keeps its shape and gains the
same durable quarantine semantics.

**Quarantine implementation.** Three designs were compared:

| Design | Evidence kept | Survives restart | Format change | GC interaction |
| --- | --- | --- | --- | --- |
| (a) tombstone the corrupt record | no (indistinguishable from a delete) | yes | no | a tombstone is a delete: reconciliation can no longer tell a corrupt shard from a deleted one |
| (b) a new record flag (`QUARANTINE` marker record) | yes | yes | **yes** (old readers reject unknown flags; needs the RFC-014 format gate) | marker superseded by a later data record |
| (c) sidecar quarantine log next to the segments | yes (bytes untouched until compaction) | yes | no | tombstone or newer data record releases the entry |

(c) is chosen: no format change, the bytes stay for investigation, the
log is append-only and fsynced, and replay is idempotent. Details:

- `<dir>/quarantine.log`: one JSON line per event:
  `{"event":"quarantine","key":…,"segment":…,"offset":…,"sequence":…,"payload_len":…,"reason":…,"unix_ms":…}`,
  `{"event":"release",…,"cause":"superseded"|"tombstone"|"purged"}`.
  Written with the same durability as a record (write, fsync) before the
  index change becomes visible.
- In memory: `quarantined: BTreeMap<ShardKey, QuarantineEntry>`; the key
  is removed from the live index. Effects: `GetShard` → `DATA_LOSS
  "quarantined"`; `ListShards` omits the key unless the request sets
  `include_quarantined`, in which case the entry carries
  `quarantined = true`; `VerifyShards`/`ScrubShards` report
  `QUARANTINED`; `Health` reports `quarantined_shards` and
  `quarantined_bytes`.
- Release: a `PutShard` of the same key (repair) appends a record with a
  higher sequence and removes the entry (`superseded`); a `DeleteShard`
  tombstones and removes it (`tombstone`); compaction of the segment that
  held the bytes drops the bytes (`purged`) — the entry is already gone
  by then or is released with cause `purged`. Quarantined bytes are never
  copied by compaction.
- Open: segments are scanned as before, with two changes: a record whose
  header validates but whose payload fails is **skipped and quarantined**
  (the scan continues at the next record, so a single bit flip no longer
  hides every later record of the segment); a region whose header does
  not validate is a *hole*: the scan resynchronises at the next aligned
  offset holding a fully valid record (bounded by the segment length) and
  records a `hole` event (the keys that were in the hole are simply
  absent from the index and surface as `MissingShard` at L3). A torn tail
  (no later valid record) is truncated as before. The quarantine log is
  replayed after the scan; entries whose record is no longer the newest
  for the key are dropped.
- Safety (I13): quarantine never deletes bytes, never writes metadata,
  and never reduces the set of shards metadata references; a healthy
  shard cannot be quarantined because the only triggers are a failed
  header or block CRC on the node's own read of its own record. The
  `OpenReport` and the quarantine log are the investigation record;
  `arca-lake storage-node quarantine list --dir` prints them offline.

### 8.3 Scrub scheduler (controller)

The leader drives scrubbing so that progress is in metadata and survives
controller failover and node restarts (nodes keep no scrub state).

- Cursor: `("al","scrub",group_id) = ScrubCursor { pass, phase, member,
  after: Option<chunk_id>, started_ms, finished_ms, verified, bytes,
  findings: [u64; 8] }` with `phase ∈ { Shards, Presence, Done }`;
  written after every page in a fenced transaction (RFC-011), so the
  next leader resumes at the page where the previous one stopped.
- Phase `Shards`: for each current member in turn, `ScrubShards` pages
  of at most `--scrub-page` keys and `--scrub-page-mib` payload bytes
  (default 8 MiB: the node stops after the shard that crosses the byte
  bound and reports `more`); every shard is classified (§8.4); findings
  are counted and `CorruptShard`/`Mismatch` findings enqueue a repair
  task. The byte bound is the burst one page puts on a device between
  two pacing sleeps: with 2 MiB shards a 256-key page is 512 MiB read at
  device speed, which the first host run showed costs a third of the
  warm GET throughput even at a 32 MiB/s average; 8 MiB pages spread
  the same budget evenly.
- Phase `Presence`: the merged member listing (`GroupWalker`, quarantined
  shards excluded) yields every chunk id with the members holding it; a
  gap on a live, placed chunk is `MissingShard` (enqueued); a chunk with
  fewer than `k` holders is `UnrecoverableStripe` (recorded, §8.5); a
  chunk nobody references is `UnexpectedShard` (counted; reconciliation
  decides, §6.3). A member whose listing failed is unreachable, not
  empty: its shards are neither present nor missing (the error is
  counted), and a stripe the shard phase already queued (its shard is
  quarantined, hence absent from the listing) is not counted a second
  time. Findings count shards, not stripes: every shard checked against
  a stale chunk record is one `StalePlacementEpoch` finding.
- Scheduling: a group is scrubbed when `now − finished_ms ≥
  --scrub-interval` (default 24 h; 0 disables); `--scrub-concurrency`
  groups at a time (default 1); the pass is paced by a byte budget
  `--scrub-rate-mib` per second over all groups (default 32 MiB/s): the
  worker sleeps until the bytes verified so far fit the budget. The node
  bounds the work per RPC (`limit` ≤ 16 384 keys, reads on the blocking
  pool, one scrub RPC in flight per node: a second one waits).
- Everything is idempotent: re-scrubbing a page re-classifies it; a task
  that already exists is not duplicated (one task per `(group, chunk)`).

### 8.4 Corruption classification

| Class | Condition | Detected by | Response |
| --- | --- | --- | --- |
| Missing Shard | a live chunk record places the chunk in the group and a current member holds no live record for its shard index | presence phase; degraded read; member repair walk | repair task (reconstruct onto the member) |
| Checksum Mismatch | a payload block CRC fails on the node | node read / scrub | node quarantines; repair task |
| Truncated Shard | the record extends past the segment's valid length, or `payload_len` disagrees with the record | node read / scrub / open | node quarantines; repair task |
| Invalid Header | record magic, version, key, sequence or header CRC fails | node read / scrub / open (hole) | node quarantines (or the hole is skipped); the key is then Missing |
| Unexpected Shard | the node holds a valid shard that no generation references, no operation in progress holds, and no delete retired (an orphan; since 0.2.0 confirmed by a listing of that key read after the metadata), or a copy at another member's index | scrub L3; reconciliation | counted by scrub; deleted by reconciliation after the minimum age (§6.3), never by scrub (I16) |
| Stale Placement Epoch | the chunk record's placement epoch is newer than the group record's epoch (metadata ahead of the group: a restore, RFC-015), or the shard sits on a former member | scrub L3 | counted and logged; repair refuses the group until the epoch is reconciled (RFC-015 §5.4); former-member copies go to reconciliation |
| Metadata / Data Mismatch | the shard verifies at L1–L2 but its length is not the layout's shard length, or the assembled chunk's CRC differs from the chunk record (some shard carries wrong bytes with a valid CRC: a misdirected write or a stale record) | scrub L3; repair / read L4 | treated as a corrupt shard: repair finds a verifying `k`-subset (bounded search over the candidates) and rewrites the shards outside it; if no subset verifies → Unrecoverable |
| Unrecoverable Stripe | fewer than `k` shards verify for a live chunk | repair classify; read | task state `Unrecoverable { have, need }`, metric, log, admin listing; GET returns the integrity error, never bytes (I15); re-checked on every scrub pass and every read report; cleared when `k` good shards are found (a returning node, a restored device) |

"Live" means the generation is `Pending` or `Committed` (RFC-008 §6.3);
chunks of retiring generations are left to GC.

Shards that are not findings of their own (0.2.0, ADR-0036), each from
that record's evidence, never from counts:

| Class | Condition | Response |
| --- | --- | --- |
| Retained for reclaim | unreferenced, and the node reports the record retired by a delete (ADR-0028) | counted (`retained`); a damaged one is also a finding of its damage class (`retained_damaged`), not queued |
| In flight | no live record here, but a migration intent targets the group, a pending generation may still record it here, or a collected generation still records it here | counted (`in_flight`); damage is a finding and queued (the task waits for the record, §8.5) |
| Unclassified | the node does not report retirement (0.1.0), the metadata did not decode, or the record changed between the reads | counted (`unclassified`) and as a scrub error; never healthy |

### 8.5 Stripe repair state machine

One task per stripe: `("al","repair",group_id,chunk_id) = RepairTask {
reason, state, targets: Vec<u16>, epoch, attempts, next_attempt_ms,
created_ms, updated_ms, last_error }`. Tasks are created by scrub,
by gateway degraded-read reports (§8.7) and by `admin repair enqueue`;
the stripe repair worker (controller leader, `--repair-workers`
concurrent tasks, `--repair-rate-mib`) drives them.

```text
Detect ─▶ Classify ─▶ Plan ─▶ Reconstruct ─▶ Verify ─▶ Commit ─▶ Cleanup
  │          │          │          │            │
  │          │          │          │            └─ mismatch → Classify (attempts+1)
  │          │          │          └─ epoch rejected → Classify (fresh group record)
  │          │          └─ good < k → Unrecoverable (persisted; re-Classify each pass)
  │          └─ generation not live → Done (GC's problem)
  └─ task exists → no-op
```

| Stage | Action | Persisted | Crash here → next pass |
| --- | --- | --- | --- |
| 1 Detect | create the task (`Detected`) if none exists | task | re-detection is a no-op |
| 2 Classify | read the group record and the chunk record; `VerifyShards` every current member for this chunk; a source is *good* iff its node reports OK and the length equals the layout; a target is a member that is missing, quarantined, corrupt, or outside the verifying subset | nothing | repeated |
| 3 Plan | if the generation is not live → remove the task; if `good < k` → `Unrecoverable { have, need }`; else `Planned { targets, epoch = group.epoch }` | task | re-planned from fresh reads |
| 4 Reconstruct | read `k` good shards, decode, check `crc32c` and `len` against the chunk record (I11); if the chunk CRC fails, try other `k`-subsets of the good shards (at most `C(n,k)`, bounded by `n ≤ 32`); the shards outside the verifying subset join the targets | nothing | repeated; no write happened |
| 5 Verify-before-write and write | encode the verified chunk; for each target write the shard with `PutShard` carrying the planned epoch (I12); the node's returned CRC must equal the shard's CRC; an epoch rejection restarts at Classify | nothing (the write itself is durable on the node; a crash leaves correct bytes that Verify will accept) | repeated: rewriting identical bytes is harmless |
| 6 Verify | `VerifyShards` on every target: OK with the expected shard CRC; else `attempts += 1`, back off, → Classify | task (`attempts`, `next_attempt_ms`) | repeated |
| 7 Commit | remove the task; count the repair as succeeded | task removed | a task whose targets already verify is removed at Classify (no targets) |
| 8 Cleanup | nothing to delete: the node released its quarantine entry when the new record superseded it; metrics updated | — | — |

Idempotence and convergence: every stage re-reads its inputs, writes
only the same bytes to the same keys under the same epoch, and the task
key is unique per stripe, so two detections, a leader failover at any
stage, or a re-issued `admin repair` converge on one of: task removed
(stripe healthy) or `Unrecoverable`. Writes race safely with readers
(the node serves either the quarantined-absent state or the new valid
record), with GC (a chunk whose generation retires during repair is
skipped at Plan; a shard written after that is reclaimed by
reconciliation, as in §5.4), and with member repair (§7) because both use
the epoch: a membership change raises the epoch and every in-flight
stripe write is rejected (I12).

Retry policy: `attempts` with exponential back-off (`30 s · 2^attempts`,
capped at 1 h); after `--repair-max-attempts` (default 8) the task is
`Failed { attempts, last_error }`, visible to the operator, and retried
at the next scrub pass of the group.

### 8.6 Multipart integrity (RFC-007 §5.3)

Manifest scrub (`admin verify-manifests`, and the controller's scrub pass
once per `--scrub-interval`): for every multipart root generation,
every part reference must name a `Committed` generation of kind `Part {
upload_id, part_number }` for the same upload and part number, whose
header `size` and `chunk_count` equal the reference's, whose checksum
equals the reference's, and whose chunk records cover exactly `size`
bytes; the references' sizes must sum to the root's `size` and their
offsets must be contiguous. Findings are `ManifestMismatch` (counted,
logged with the generation; the object is served as long as the parts
verify, because the part manifests are the ground truth, but the
mismatch is an invariant violation to investigate). A dangling reference
(part generation missing or not `Committed`) is `Unrecoverable` for that
byte range (I15) and is reported with the object; GC of such a part
would violate I-MP1, so this finding is also a `gc_skipped_referenced`
investigation. Versioned objects and delete markers add no case: a root
is a generation like any other, and a marker references none.

Full-object checksum verification (`admin integrity verify-object
--decode`, Phase 3.4) decodes every byte through the same coded-read path
as the gateway, from shards a read-only verification found good, and
compares the recomputed ETag, object checksum (composite or full-object)
and part checksums with the stored ones; it is an operator tool, not part
of scheduled scrubbing, and changes nothing (`--quarantine` is the
explicit opt-in to quarantine what it finds).

### 8.7 Gateway degraded-read reports

`read_coded` already decodes around missing or failing shards. From
Phase 3.2 a read that needed parity (`missing` non-empty) is a *degraded
read*: counted (`arca_gateway_degraded_reads_total`) and reported as a
repair task (`Detected`, reason `DegradedRead`) through a per-gateway
deduplicating queue (one report per chunk per 10 min, bounded size), so
that a corruption hit by a client is repaired before the next scrub
pass. An unreadable chunk (`have < k`) is reported the same way with
reason `Unreadable` and the client receives the integrity error.

### 8.8 Metrics

Bounded cardinality: no per-group, per-chunk or per-node labels; the only
label is the class (eight values, §8.4).

| Series | Type | Meaning |
| --- | --- | --- |
| `arca_controller_scrub_passes_total`, `_scrub_shards_verified_total`, `_scrub_bytes_total`, `_scrub_errors_total` | counter | scheduler progress and node/metadata errors during scrub |
| `arca_controller_scrub_groups_in_progress`, `_scrub_groups_overdue` | gauge | groups being scrubbed / past the interval |
| `arca_controller_integrity_findings_total{class}` | counter | findings by class (8 classes) |
| `arca_controller_scrub_shard_classes_total{class}` | counter | scrubbed shards that are not findings: `retained_for_reclaim`, `retained_damaged` (also a finding), `in_flight`, `unclassified` (also a scrub error) (0.2.0) |
| `arca_controller_repair_queue_depth`, `_repair_tasks_unrecoverable`, `_repair_tasks_failed` | gauge | stripe repair queue state |
| `arca_controller_stripe_repairs_succeeded_total`, `_failed_total`, `_shards_rewritten_total`, `_repair_bytes_total` | counter | stripe repair outcomes |
| `arca_controller_data_integrity_status` | gauge | 0 = no open findings; 1 = repairs pending or failed; 2 = unrecoverable stripes present |
| `arca_node_quarantined_shards`, `_quarantined_bytes` | gauge | node quarantine state |
| `arca_node_corrupt_reads_total`, `_scrub_shards_total`, `_scrub_bytes_total`, `_scrub_corrupt_total` | counter | node-side detections |
| `arca_gateway_degraded_reads_total`, `_unreadable_reads_total`, `_integrity_reports_total`, `_integrity_reports_dropped_total` | counter | read-path reports |

### 8.9 Operator interface

`arca-lake admin integrity status` (scrub cursors per group, queue
depth, data integrity status, unrecoverable and failed tasks with chunk
ids), `admin integrity scrub --group <id> [--wait]` (reset the cursor
so the leader's next tick starts the pass, or run it from the admin
process), `admin integrity enqueue --group --chunk`, `admin integrity
list`, `admin integrity verify-manifests` (§8.6; exit code 2 when there
are findings), `arca-lake storage-node quarantine-list --dir <dir>
[--scan]` (offline listing of the quarantine log and, with `--scan`,
the open report and live entries), and `storage-node corrupt-shard`
(test-only, hidden: flips a byte of a record's payload or header on a
stopped node for drills). Mapping the objects that reference an
unrecoverable chunk is a follow-up (the chunk id's generation prefix
identifies the generation; the listing does not resolve it to a key).

### 8.10 Phase 3.10: background work order and isolation

From the background-operations audit (`docs/verification/phase-3.10-recovery.md`).

- **GC pages through the retired queue.** A cursor kept by the leader
  process carries across batches and wraps at the end of the queue; a
  batch that read a full page continues the pass. Generations that
  cannot be collected yet (a member down, the fence closed) no longer
  hold up those behind them at the head of the queue. A batch that
  retired multipart parts starts again from the head, because parts are
  queued as of their root.
- **Repair is isolated per group.** A group's error is logged and
  counted (`arca_controller_repair_group_errors_total`), and the other
  groups go on; only a fencing error stops the task.
- **Failed members first.** Groups with a failed member, or rebuilding
  one for any reason other than an operator's move off a healthy node,
  are repaired before planned moves. Planned moves (a replacement request
  off a healthy node, a drain of a member of an `Active` group) rebuild
  at most `--planned-move-chunks-per-pass` (512) chunks per pass.
- **A pending write's chunk is never an orphan to repair.** A chunk of a
  pending generation without a chunk record may still be placed in any
  group (the gateway writes shards before their records), so repair
  rebuilds it or waits. The leftover of a write retried in another group
  (recorded there) is reconciliation's.
- **Priority, end to end:** Repair > Client writes > GC and compaction >
  Tiering > Rebalance > Prefetch. Tiering and rebalancing need the
  published admission record (current, no tier throttled or worse) and
  no durable group being repaired; rebalancing also yields to tiering
  and to a GC backlog above `--rebalance-gc-backlog-pause-mib`.

### 8.11 Phase 3.11: restore trip wire, epoch intents, queue paging

- **Trip wire (ADR-0025).** A running leader that observes its store
  replaced or locked (a missing leader record or cluster state,
  `database_locked`) latches its deletes off, closes the generation above
  the one in use on the nodes and ends its term; a store watch checks
  every 250 ms. It limits damage once a restore is observed; it does not
  make an unmanaged restore safe (R-94(b)).
- **Deletes bound to the node's disk (ADR-0027).** Every delete carries
  the identity the node acknowledged; reconciliation, fence closes and
  floors skip removed and quarantined nodes.
- **Epoch change intents (M2).** A member move records its intent
  before raising node epochs; any leader resumes it or settles the
  epoch (membership unchanged), so nodes are never left ahead of the
  metadata; a stuck intent holds only its own group.
- **Planned moves keep the client reserve (M3).** Their writes use the
  `Migration` class; failure repairs keep the repair floor.
- **Repair queue paging (M4).** At most 512 entries per transaction, a
  scan window from a persisted, wrapping cursor; priority applies within
  the window.

### 8.12 Phase 3.14: verification reuse, reservations, sweep cursors

- **Differential target verification (ADR-0031).** A repair attempt
  reads again only the target's records written since its last pass of
  the same group, epoch, slot and target, while the node's store epoch
  holds (`VerifyShards.since_*`, `indexed_below`); the completeness check
  before `Active` reads every shard, so §7's order is unchanged. Slots
  are rebuilt eight chunks at a time, results taken in walk order (the
  cursor never passes a chunk not done).
- **Capacity reservations (ADR-0032).** A member move reserves the
  member's bytes on its target in one transaction against the node's
  other reservations (repairs count only repairs); validity is derived
  from the group (or its open epoch intent), so a lost release or a
  stale controller cannot hold room; rebalancing treats reserved room as
  taken. The node's reserves stay the enforcement.
- **Sweep cursors.** Reconciliation (§6.3) and tiering keep a persisted
  cursor (`("al","bgcursor",name)`): a pass resumes where the last one
  stopped, an item whose metadata cannot be decoded is passed over and
  counted, and the sweep wraps; the reconciliation budget defaults to
  every pair per pass.

## 9. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Retry `commit_put` blindly on unknown outcomes | Re-execution after an actual commit would retire the displaced generation twice or publish a second version; resolution by re-read is cheap and exact. |
| Delete shards by age alone | Any clock disagreement becomes data loss; reference checks cost one read per decision. |
| Mark a group `Active` when the replaced member's shards are written | Misses a second membership change in flight; listing every member is O(shards) but makes completion a verified fact. |
| Per-chunk "repaired" flags in metadata | O(chunks) metadata writes per repair (DD-3). |

## 10. Verified and not verified

Verified on the in-process cluster with the in-memory backend and, for the
end-to-end and commit-fault suites, on a real FoundationDB cluster:
everything listed in §5 with a test name. The transport faults are
injected at the client call boundary (`arca_lake_protocol::client`), which
reproduces their observable effect on the protocols; TCP-level behaviour
(resets mid-stream, HTTP/2 stream errors, kernel buffering) is **not**
covered and needs tests on a real network.

Verified on a three-process FoundationDB cluster (`fdb_failover.rs`):
process loss during concurrent PUTs, coordinator loss, quorum loss with
explicit failure, controller leader failover, repair and GC across a
process loss.

Not verified: metadata store unavailability lasting longer than a lease
during a PUT; more than one simultaneous repair on different groups; power
loss of a storage node mid-`PutShard` beyond the storage engine's own
crash tests (RFC-005); real-network behaviour (RFC-011 §8, risk R-35).

## 11. Open questions

- Grace-period measurement by commit version instead of wall clock (OD-16).
- Reconciliation scheduling and rate limits (paging exists, §6.5).
- Whether `skipped_referenced > 0` should halt the controller.

## 12. Acceptance criteria

Labels as RFC-007 §11 (Designed / Implemented / Verified / Not
Implemented / Not Verified).

| # | Criterion | Status |
| --- | --- | --- |
| G1 | All tests named in §5 pass on both backends in CI; the model test covers I1–I5; the controller schedules GC, reconciliation and repair | Verified (Phase 2) |
| G2 | Durable quarantine: a corrupt shard found on read or scrub is absent to readers and repair after a node restart, its bytes remain until compaction, the quarantine log records it, and a repair write releases it (I13) | Implemented, Verified (`storage/tests/quarantine.rs`; `integrity.rs::quarantine_survives_a_node_restart_…`, `…scrub_detects_checksum_mismatch_…`) |
| G3 | Open recovery skips a record with a corrupt payload and resynchronises after a corrupt header; the records after it are indexed | Implemented, Verified (`quarantine.rs::corrupt_header_is_a_hole_…`, `…corrupt_payload_is_quarantined_durably_…`) |
| G4 | `ScrubShards` classifies OK / checksum / header / truncated / quarantined / I/O error and never quarantines on I/O error | Implemented; Verified for OK / checksum / header / truncated / quarantined (`integrity.rs::every_corruption_class_is_classified`); the I/O-error path is Not Verified (no fault injection below the storage engine) |
| G5 | Controller scrub: cursor persisted per page, resumes after leader failover and node restart; rate and concurrency limits observable; every class of §8.4 is produced by a test that injects it | Implemented, Verified (`integrity.rs::scrub_cursor_resumes_…`, `…scrub_skips_a_member_that_is_down_…`, `…every_corruption_class_is_classified`, `…controller_processes_scrub_and_repair_and_a_leader_failover_…`); the byte budget is exercised by the resume test (a 1 B/s pass sleeps after its first page), its throughput effect by the host run (§G10) |
| G6 | Stripe repair: single missing, several missing, corrupt, metadata/data mismatch, unrecoverable (no write, GET error), concurrent detections converge to one task, repair during GET/PUT/DELETE, epoch change mid-repair restarts at Classify, leader failover mid-repair resumes; every reconstructed shard verified before and after the write (I11, I12) | Implemented, Verified (`integrity.rs`: `missing_shards_single_and_multiple_…`, `scrub_detects_checksum_mismatch_…`, `every_corruption_class_…`, `unrecoverable_stripe_…`, `a_member_that_is_down_is_a_retry_…`, `concurrent_detections_and_workers_converge_…`, `repair_runs_safely_under_concurrent_get_put_and_delete`, `a_fenced_member_rejects_the_repair_write_…`, `controller_processes_…_leader_failover_…`); the epoch case is verified through a node fenced ahead of the plan (the write is rejected, the task re-plans after the membership change completes), not through a hook inside the worker |
| G7 | Multipart: manifest scrub covers roots, references, parts, chunk placement, part and full-object checksums; GC after Complete never removes a referenced part (I-MP1) under scrub and repair | Implemented, Verified (`multipart_integrity.rs`: clean verification, part-chunk scrub and repair, GC and sweep after Complete, overwrite collection, versioned roots and delete markers, dangling and mismatched references with no partial read). Full-object checksum verification through the read path (`admin verify-object`) is Not Implemented; the stored checksum is verified on every GET by RFC-007's checksum response path instead |
| G8 | FoundationDB failover during multipart operations (RFC-007 A10) | Implemented (`fdb_failover.rs` step 4b); status per RFC-007 §11 |
| G9 | Metrics of §8.8 exported with bounded cardinality | Implemented, Verified (`integrity.rs::controller_processes_…` reads the controller's `/metrics`); the one label is the eight-value class |
| G10 | Scrubbing at the default rate costs less than 15 % of PUT and GET throughput on the validation hosts | Verified for throughput: cached working set (Phase 3.2: GET +1 %, p99 −3 %); disk-bound working set (Phase 3.4, 24 GiB over 7.8 GiB RAM: GET throughput −6 to −11 %, PUT within noise). Latency: p50 +5 to +13 %, p99 +2 to +8 % (Phase 3.3 on 16 GiB: p95 +72 %). Adaptive budget opt-in (ADR-0018). Physical devices not verified (`docs/verification/performance-regression.md`, Phase 3.4) |
