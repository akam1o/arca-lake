# ADR-0033: Background passes at object scale

Status: **accepted** (Phase 4.1; proposed in Phase 4.0). Concerns
reconciliation and the reclaim pass (ADR-0028, ADR-0030); no change to
deferred reclamation's rules, to the metadata layout, to the node
protocol or to what a judgment reads.

## Context (measured, Phase 4.0)

On the Phase 4.0 test cluster (four storage nodes on one virtual machine,
FoundationDB as one process on another; `docs/verification/phase-4.0-scalability.md`
§10), at 765,134 objects of 1 KiB stored as three replicas (about 2.29
million shard records, 571,000 per node):

- a reconciliation pass took 2,115 s (longest 2,451 s) against its 300 s
  interval: reconciliation ran about 88 % of the time and its cycle grew to
  about 2,415 s;
- a reclaim pass took up to 1,094 s;
- at 10⁵ objects the same passes took 33–148 s and 15 s.

From the code (`crates/controller/src/gc.rs`, `process.rs`):

- reconciliation visits every `(group, node)` pair in turn, pages the
  node's shard listing (1,024 per page) and decides each shard with its own
  metadata transaction (`attribute_shard`), one after another: one
  FoundationDB transaction per stored shard per pass, sequential;
- the reclaim pass visits nodes in turn, pages each node's pending
  retirements, decides each with a metadata read (`shard_needed_in`) and
  sends its keep or reclaim RPC (16 in flight); a pass starts again from
  the beginning.

Consequences (two reviews, Phase 4.0): no other task starves (each task is
its own scheduler); deletes stay fenced (epoch, record token, a group read
again before a non-member's delete); a slow reclaim only keeps data longer,
so R-94(b)'s guarantee is unchanged. The costs are liveness and capacity:
orphans and the sources of moves are removed up to one cycle late and
reclaimed after the retention plus up to one reclaim pass; rebalancing's
settle time (1,500 s by default, from the reconcile interval) is shorter
than the measured cycle, so it can plan again before the space of its
last moves came back; reconciliation's metadata reads are a constant load
on FoundationDB during client traffic.

## Decision

1. **Batch the judgments, same rules.** Attribute up to `B` shards (one
   listing page) in one read-only metadata transaction (point reads of
   the chunk records and of the generations' headers, as today, but
   together), and the reclaim pass's `shard_needed_in` likewise; deletes
   and reclaims stay per record, with their epoch, record token and the
   fresh group read before a non-member's delete.
2. **Bounded parallelism.** Pairs (reconciliation) and nodes (reclaim) are
   processed `P` at a time (default small, e.g. 4), each with its own
   cursor position; the lineage check every 8 pairs stays.
3. **Settle time from what was measured.** Rebalancing's settle time is at
   least the last measured reconciliation cycle (`last_sweep_ms −
   sweep_started_ms`) plus the minimum orphan age, not a constant.
4. **Visibility.** The pass metrics added in Phase 4.0
   (`arca_controller_pass_last_seconds{task}` and the longest) and a gauge
   "pass running since" per task.

Not decided here, and would need its own ADR: a persisted cursor inside a
pair, any change to what a judgment reads or to the retention rules, any
node RPC or metadata layout change.

## Implementation (Phase 4.1)

- `Txn::get_many` and `Txn::get_first_many` (`crates/metadata/src/kv.rs`):
  several point reads, or the first pair of several ranges, at the
  transaction's read version. The default runs them one after another
  (every wrapper and the in-memory store keep their behavior); the
  FoundationDB backend issues them at once. The fence, guard, fault and
  trip wrappers forward them unchanged, so a batch's transaction is
  checked at commit as any other (the delete-class guard checks the
  cluster state even for a read-only transaction).
- `ops::gc::attribute_shards`: per batch, one read-only transaction:
  the first generation of every prefix; then for each shard with one the
  header, the chunk's migration intent, the chunk record and whether a
  second generation shares the prefix — the reads `attribute_shard` makes
  for a prefix with one generation, issued together. A prefix with two
  generations (practically never) is judged by `attribute_shard`'s own
  sequence of reads in the same transaction. The rules are one function
  shared with `attribute_shard`. A record that does not decode is an error
  of its shard in the batch's answer (the reclaim pass counts that record
  as an error and goes on); reconciliation, as before, passes over the
  rest of the pair at that shard until the next sweep. Any other error
  fails the batch. A batch whose transaction keeps failing (retries
  exhausted, as when it outlives the store's transaction age, or a
  backend error) is split in two, down to one shard, whose error then
  counts as before (review: one heavy page must not stop every pass at
  its pair). Differential tests against `attribute_shard`: on memory
  (`crates/metadata/tests/attribute_batch.rs`, 512 random cases) and on
  FoundationDB (`fdb.rs`, 64 fixed-seed cases and one 512-shard batch,
  prefixes with zero bytes).
- Reconciliation (`gc::reconcile_node`): the shards a member holds at its
  own position are attributed per listing page, `ATTRIBUTE_BATCH` (512)
  per transaction, before the page's deletes; former members and
  misdirected copies keep their own group read before each delete.
  Deletes stay one by one, in listing order, each with its group epoch
  and record token.
- Pairs: `--reconcile-parallel-pairs` (default 4, at most 8) at once, in
  slices of 8 pairs: before each slice the lineage advances (as before:
  every 8 pairs) and the cursor records the pairs before it. Results are
  taken as they come, in any order; on a store or leadership error the
  pass returns at once (the slice's pairs still running are dropped, as in
  a crash: their deletes were each decided with token, epoch and fence),
  the cursor stops after the pairs that all finished before it, and the
  rest of the slice is done again next pass. A new sweep's start is recorded in the
  cursor when its first pass begins.
- Reclaim (`gc::reclaim_node`): per page, after the observation stamp and
  the group read, the reference checks of the page's due records in
  batches; `--reclaim-parallel-nodes` (default 4) nodes at once.
- Settle time: rebalancing's effective settle time is the largest of the
  configured one and the measured reconcile cycle (the last full sweep the
  cursor or this process recorded, or the sweep in progress extrapolated
  from the share of pairs before its cursor, which is what a leader that
  took over in the middle of a sweep has) plus
  the reconcile interval (a pair visited just before a move is visited
  again only a sweep and an interval later) plus the minimum orphan age.
  The extrapolation is an estimate, not a bound (reviews): it is linear in
  pairs while a pair's cost follows its shards, so a large group late in
  the sweep makes it short; pairs added or removed during the sweep skew
  it; a settle deadline is fixed when a plan finishes and is not extended
  by a later, longer estimate; and a leader taking over before the first
  slice of a sweep has only the time elapsed (the cursor keeps no length
  of the previous sweep; adding one is a record change, not made here).
  The effect of a short estimate is rebalancing churn, not data risk; gauges
  `arca_controller_rebalance_settle_seconds`,
  `arca_controller_reconcile_sweep_last_seconds`.
- `arca_controller_pass_running_since_seconds{task}` (unix seconds; 0
  when no pass of the task runs).

### Why batching keeps the delete rules (code analysis)

A judgment now reads at the batch's read version, up to one page's
deletes before its own delete (before: just before it). Nothing a delete
relies on depends on that interval:

- the decision is the same function of the same values at one read
  version (differential test: `crates/metadata/tests/attribute_batch.rs`);
- every delete names the record the listing showed (record token): a shard
  written again under the same key since the listing is refused by the
  node (`kept_changed`), whatever the metadata said;
- `NoGeneration` deletes only shards older than the minimum orphan age,
  which is at least the pending lease: a generation with that prefix
  cannot be created later;
- `Unreferenced` means generations with the prefix exist, none pending
  and none places the chunk here; generation states only move forward
  (no recipe makes a retiring generation committed, or a committed one
  pending), and a migration into this group writes new records (refused
  by the token) after its intent;
- deletes carry the group epoch read for the decision (a node fenced to a
  newer epoch refuses) and go through the delete fence (incarnation,
  generation), unchanged; the lineage advances every 8 pairs as before.

The reclaim pass's observation stamp still precedes every metadata read
that judges its page (ADR-0028), and the node still decides on the
retention from that stamp: R-94(b) is not touched.

## Expected effect (estimated, to be measured)

Batching removes the per-shard transaction overhead (begin, read version,
commit round trip), which dominates a read-only judgment on this setup;
the ratio is not measured. Parallelism multiplies by at most `P` until
FoundationDB or the node's listing is the limit. These are estimates; the
Phase 4.1 acceptance is a measured pass time at 10⁶ objects on the same
layout, with the same deletes made as before (a differential test on the
simulator of Phase 3.14 and the deferred-reclamation tests unchanged).

## Alternatives

- Longer intervals: hide the cost and lengthen the cycle further.
- A reconcile budget per pass: exists (default 0 = everything); a budget
  bounds a pass but not the cycle, and slows rebalancing's settle.
- Event-driven orphan detection (record what a failed write left): fewer
  scans, but a protocol or metadata change; Phase 4.2 at the earliest.
