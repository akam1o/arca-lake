# ADR-0022: Delete reserve, admission records fenced by incarnation, recent-trend entry, capacity rebalancing

Status: accepted (Phase 3.8)

## Context

Phase 3.7 left four questions. A node went 11 MiB below its reserve in
Test C, cause unknown. Gateways took an admission record of a lower
leader token when it was computed more than 60 s later, a time rule.
Throttling could be needlessly long after a burst. Existing data was
never moved off a full group (R-82).

## Decision

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Tombstone space | A delete reserve (one tombstone per record held plus 1 024) under both floors; deletes stay unrefused | refusing deletes near full (deletes are how space returns); smaller tombstone records (an on-disk format change for 4 KiB per record) |
| Capacity breakdown | Counters by record kind and write class, side files, file system overhead, per node | a per-write ledger in metadata (a hot path for every shard write) |
| Record fencing | Incarnation equality, then (term, generation) monotonic; vanished or ignored records keep the last one ageing; no record since start fails closed after 60 s | the Phase 3.7 time rule (cannot tell a restore from a fast clock); trusting the latest metadata value (a stale read during a partition would win) |
| Entering a worse state | Long and recent trends both draining, once the recent window is covered | the long trend alone (flapping after a burst); the recent trend alone (bursts throttle) |
| Rebalancing | Chunk moves between groups of one shape in a tier through the migration path, leader-only, cursor in metadata, repair first, migration class at the target, per-pass budget | moving whole members (replace-member already exists, cannot split a group's load); rebalancing across shapes (changes durability per object); a separate copy protocol (the migration path is already crash-tested) |

## Consequences

- The delete reserve lowers usable space by about 0.8 % for 512 KiB
  shards and 6 % for 64 KiB ones; nodes full of small shards stop
  accepting client writes earlier.
- Gateways never take a record of another incarnation or an older term;
  a gateway left without any record refuses new data after 5 minutes.
- Rebalanced chunks free their source only after reconciliation and
  compaction; rebalancing is off unless enabled.
