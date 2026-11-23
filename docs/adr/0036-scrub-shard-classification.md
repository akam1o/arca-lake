# ADR-0036: Per-shard scrub classification and records kept for reclaim

Status: **accepted** (0.2.0).

## Context

Since deferred reclamation (ADR-0028) a durable delete retires the
record and the node keeps it until a reclaim, the retention later
(default 24 h). The scrub (RFC-008 §8.3) judged every listed shard
against metadata only: a retired record of a collected generation has
no generation left, so it was counted `UnexpectedShard`, exactly like an
orphan. After deletes the 0.1.0 scrubs reported hundreds of unexpected
shards per group (0.1.0 three-host validation), and the R-110 judge
excused them with a count rule (unexpected shards at most the records
pending reclaim), which says nothing about any one shard.

A second gap (R-120): a damaged shard of a migration's target, found
before the placement switch, was queued; the repair worker dropped the
task (committed generation, record in another group), and after the
switch nothing queued it again until a degraded read of the chunk or the
target's next scrub pass.

## Decision

1. **Evidence per record.** The scrub classifies each listed shard from:
   the node's verification (damage class), whether the node holds that
   record retired (a new `ShardScrub.retired` flag, at the sequence the
   index held when the page was listed), and one metadata transaction
   for that chunk (`gc::scrub_attribution`): a live generation's chunk
   record placing it in the group (`Placed`); a migration intent
   targeting the group, a pending generation, or a collected generation
   still recording it here (`InFlight`); nothing (`Unreferenced`).
2. **Classes.** `Placed`: the existing checks (damage queued, stale
   epoch, metadata/data mismatch). `InFlight`: not a finding; damage is a
   finding and queued. `Unreferenced` and retired: `retained` (not a
   finding). `Unreferenced`, not retired, confirmed by a listing of that
   key read *after* the metadata: `UnexpectedShard` (an orphan) or, if
   damaged, its damage class. Anything that cannot be established (a
   node that does not report retirement, metadata that does not decode,
   a record gone or written again between the reads): `unclassified`,
   counted as a scrub error, never healthy.
3. **Damaged retained records are findings.** Counted under their
   damage class (and in `retained_damaged`), not queued. Justification:
   the record exists so that a restore within the retention finds it
   (ADR-0028); its damage is lost restore margin and a device returning
   bad data, which an operator must see. It is not queued because
   nothing references it, so there is nothing to rebuild against; if a
   restore makes it referenced, the next scrub sees `Placed` and damage
   and queues it.
4. **Read order.** Metadata before the confirming listing: GC removes a
   generation only once the nodes acknowledged its deletes, so a record
   a delete of that generation retired is seen retired by the listing; a
   collection racing the scrub yields `InFlight`, `retained` or
   `unclassified`, never an orphan.
5. **R-120.** The repair worker keeps (re-schedules every minute) a
   task with no record in its group while a fresh migration intent
   (younger than its one-hour TTL) targets the group; after the switch
   it repairs, after a cancel (intent ended) or once the intent is stale
   it drops the task.

## Formats and compatibility

- Wire: `ShardScrub.retired = 7` and `ScrubShardsResponse.retirement_reported = 5`,
  additive protobuf fields. Protocol version unchanged (4, accepts 3).
  A 0.1.0 controller ignores them (prost skips unknown fields). A 0.1.0
  node sends neither; both decode as `false`, the client maps that to
  "not reported" (`client::retirement`, unit-tested), and the controller
  classifies such nodes' unreferenced shards as `unclassified`.
- Metadata: no record changes. The scrub cursor (`("al","scrub",g)`)
  keeps its eight-class `findings` array; `retained` and `in_flight` are
  not persisted (CLI output, metrics and logs only); `unclassified`
  shards are persisted in its `errors`. `Finding` gains no variant, so
  repair tasks read by 0.1.0 decode as before. 0.1.0 readers silently
  drop appended record fields when they rewrite a record, which is why
  nothing is appended.
- Output: the `integrity scrub` line keeps its prefix and appends
  `retained … retained_damaged … in_flight … unclassified …`; parsers
  that match the prefix are unaffected. The R-110 harness parses both
  forms; the judge's count rule applies only to timelines without the
  new fields (binaries before 0.2.0).

## Consequences

- An unexpected shard now means an orphan; records kept for reclaim no
  longer need excusing.
- A mixed cluster with 0.1.0 nodes reports their unreferenced records as
  unclassified (scrub errors) until they are upgraded.
- One extra `ListShards` call (one key) per orphan candidate, none for
  retained or referenced shards.
- A task waiting on a migration counts as actionable repair work: it
  holds tiering and rebalancing passes while it waits, at most the
  intent's TTL after a migration that crashed before its switch.
