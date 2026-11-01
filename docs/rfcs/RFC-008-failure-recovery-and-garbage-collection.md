# RFC-008: Failure Recovery and Garbage Collection

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-003, RFC-004, RFC-005
- Related ADRs: ADR-0005 (single-owner chunks, tombstone queue with grace),
  ADR-0007 (commit resolution, reference-checked GC, repair completion)

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

### 6.7 Clock skew

Safety never depends on clocks agreeing: every decision is a transaction
that reads the state it changes. Skew affects liveness only. A controller
clock far ahead retires pending generations before their writers finish;
those writers then fail at their next append or commit and the client
retries, nothing becomes visible (`expiry_and_commit_exclude_each_other`,
`pending_generation_is_never_collected_before_its_lease_…`). A controller
clock far behind delays reclamation.

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

## 8. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Retry `commit_put` blindly on unknown outcomes | Re-execution after an actual commit would retire the displaced generation twice or publish a second version; resolution by re-read is cheap and exact. |
| Delete shards by age alone | Any clock disagreement becomes data loss; reference checks cost one read per decision. |
| Mark a group `Active` when the replaced member's shards are written | Misses a second membership change in flight; listing every member is O(shards) but makes completion a verified fact. |
| Per-chunk "repaired" flags in metadata | O(chunks) metadata writes per repair (DD-3). |

## 9. Verified and not verified

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

## 10. Open questions

- Grace-period measurement by commit version instead of wall clock (OD-16).
- Reconciliation scheduling and rate limits (paging exists, §6.5).
- Whether `skipped_referenced > 0` should halt the controller.

## 11. Acceptance criteria

- All tests named in §5 pass on both backends in CI.
- The model test (`crates/metadata/tests/model.rs`) covers I1–I5.
- A controller process schedules GC, reconciliation, and repair (Phase 2)
  without changing the recipes.
