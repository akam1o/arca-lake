# ADR-0033: Background passes at object scale (proposal)

Status: **proposed** (Phase 4.0). Not implemented. To be decided and
implemented in Phase 4.1. Concerns reconciliation and the reclaim pass
(ADR-0028, ADR-0030); no change to deferred reclamation's rules.

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

## Decision (proposed)

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
