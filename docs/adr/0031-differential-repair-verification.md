# ADR-0031: Differential verification of repair targets and parallel rebuilds

Status: **accepted** (Phase 3.14). Amends ADR-0029 (retargeting and the
repair status are unchanged).

## Context

Since Phase 3.13 (R-104) a repair attempt starts by verifying every shard
of the group on its target (`VerifyShards`, each record read in full and
checked), so that a shard that decayed there is rebuilt rather than
skipped. A failure repair runs in attempts of a chunk budget per pass;
each attempt verified the whole target again, so a large group's repair
read its target `attempts` times: O(chunks × attempts), O(chunks²/budget).
Phase 3.13's real-process run S4 waited 430 s for `awaiting-nodes`, most
of it in these passes.

Breakdown, in-process cluster (host B of the Phase 3.14 runs: 4 vCPU,
virtual disk), 2+1, 128 KiB chunks, budget 64 chunks per attempt,
sequential rebuild as in Phase 3.13; the median of three runs, with that
run's step times summed over its attempts:

| Chunks | Attempts | Wall | Start verify | Walk (listing) | Reconstruct (read `k`, decode) | Write | Cursor txns | Completeness | Shards read by verify |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 200 | 4 | 1.62 s | 0.05 s | 0.02 s | 0.94 s | 0.53 s | 0.02 s | 0.03 s | 584 |
| 1,000 | 16 | 8.79 s | 0.93 s | 0.15 s | 4.71 s | 2.69 s | 0.12 s | 0.14 s | 8,680 |
| 4,000 | 63 | 51.5 s | 15.0 s | 0.88 s | 19.2 s | 15.1 s | 0.50 s | 0.57 s | 128,992 |

One full verification pass of the 4,000-shard target: 0.48 s; one
listing: 26 ms. Metadata transactions: one per chunk for the cursor
(8,130 at 4,000 chunks), unchanged by this ADR. The epoch change, the
commit and the backoff are single transactions per attempt or per
failure, not measurable at this scale. The verify share grows
quadratically: about 29 % of the wall time at 4,000 chunks, and it would
dominate a group of 10⁵ chunks (≈ 1,500 attempts × a 12 s pass).

## Options considered

| | Effect | Cost / risk | Kept |
| --- | --- | --- | --- |
| Result cache per epoch, reused as is | no re-read | a record that changed since (rewritten, replaced) taken as verified | no, alone |
| Persisted per-shard progress (FDB) | survives a leader change | a write per shard or per page, a schema; a stale entry must be invalidated by the node's state, which FDB does not see | no |
| **Differential verification** against the node's write sequence | only records written since are read | a new field in `VerifyShards`; the node's sequences must be monotonic and every record below the watermark listed | **yes** |
| Reuse across a retarget | — | the new target is another disk: nothing to reuse | no (another key) |
| **Parallel rebuild** (batches of the walk) | reads of `k` shards and the target's writes overlap | the cursor must never pass a chunk not done | **yes** (8 at a time) |
| Skip the start verification | — | a decayed shard on the target found only at completeness, then a whole walk again | no |

## Decision

- **Node** (`VerifyShards` fields 6/7, response 4/5/6). The response
  carries the node's store epoch (new at every open) and
  `indexed_below`: a sequence below which every record that exists is in
  the index, read under the writer lock before the page is listed
  (`min(next sequence, lowest sequence appended and not yet indexed)`).
  A request with `since_store_epoch` equal to the node's store epoch and
  `since_sequence > 0` skips records whose sequence is below
  `since_sequence` (not read, not reported) and says so
  (`since_applied`); any other request verifies everything, as before.
  Sequences never repeat or go back within a store (CON-17); a record
  written again (a repair, a client write, a rewrite by compaction is
  not: it keeps its sequence and its checksum is verified when copied)
  has a newer one.
- **Controller** (`repair::VerifyCache`, process-wide, in memory). Key:
  group, epoch, slot, target address. Value: the store epoch, the
  watermark of the pass's first page, the corrupt chunks found. The
  start-of-walk verification asks for records since the cached
  watermark; corrupt = cached − verified good + found bad. If the
  node's store epoch changes during a pass, or `since_applied` changes
  between pages, the pass starts again in full, at most three times
  (review: a target that keeps restarting is then a failing target, an
  error that leads to its replacement, not an endless pass). Capacity 4,096 entries
  (cleared when full); a group's entries are dropped when it becomes
  `Active`. A controller that restarts, or a new leader, verifies
  everything once.
- **Completeness is not differential.** The check before `Active` reads
  every shard of the target (`full`), as in Phase 3.13, and replaces the
  cached result: a record that decayed after it was verified (same
  sequence, so the differential pass does not read it) is found there,
  and the group is not made `Active` with it.
- **Parallel rebuild** (`RepairOptions.parallel`, the leader uses 8;
  0/1 is sequential). Consecutive chunks needing a shard are rebuilt and
  written together (`join_all`), at most the budget left; outcomes are
  taken in walk order and the cursor is persisted per chunk in that
  order, so a crash resumes at the first chunk not recorded (what was
  written past it is found present and verified then). A chunk that does
  not need a shard flushes the batch first.

## Guarantees and their tests

| Guarantee | How | Test (the mutant it kills) |
| --- | --- | --- |
| Another epoch never reuses a result | the key holds the epoch | `a_restart_or_another_target_verifies_everything_again` (key without epoch) |
| Changed content is not trusted | a rewrite has a newer sequence; the record at the watermark is read | `the_first_record_after_the_watermark_is_read_again` (`>` for `>=`) |
| A restart (or another disk) is not trusted | store epoch compared on the node and by the controller | as above (node ignores the store epoch; controller ignores `since_applied`) |
| Unindexed records are not below the watermark | `indexed_below` takes the lowest in flight | `storage::indexed_below` (watermark without in-flight) |
| Not `Active` before the shards are there | completeness reads everything | `a_shard_decayed_after_verification_is_found_before_active` (completeness differential) |
| A crash never counts an unwritten chunk | outcomes in walk order, cursor per chunk | `parallel_rebuilds_resume_after_a_crash_and_read_back` |
| An old target cannot pollute | the epoch fence on writes (unchanged); another target is another key | Phase 3.13 retarget suite |
| Quarantined / removed nodes are not targets | unchanged (`healthy`, ADR-0027) | Phase 3.11/3.13 suites |

Two mutants survive and are kept as documented equivalences: ignoring a
store-epoch change in the middle of a pass (the later pages are then
full reads by the node, and the next pass's watermark is refused by the
node, so nothing is taken as verified that was not; only the pass is not
restarted), and not dropping a group's entries at `Active` (memory only:
the next repair has another epoch).

## Measured after (same cluster and method; median of three, runs in rotated order)

| Chunks | Phase 3.13 behaviour | Cache, sequential | Cache, 8 parallel | Shards read by verify |
| --- | --- | --- | --- | --- |
| 200 | 1.62 s | 1.61 s | 0.87 s | 584 → 392 |
| 1,000 | 8.79 s | 8.19 s | 4.27 s | 8,680 → 1,960 |
| 4,000 | 51.5 s | 37.7 s | 18.4 s | 128,992 → 7,968 |

Start-verify time at 4,000 chunks: 15.0 s → 0.78 s in every run. The
write step varies by up to 3× between runs on this virtual disk (an
outlier of 54.9 s with the cache, sequential, came from 30.7 s of
writes): the sequential wall-time gain is within that noise at 200 and
1,000 chunks; the verify reduction and the parallel gain are not.
Retarget (the repair half done on one target moved to another, time to
`Active`), 4,000 chunks: 47.2 s → 17.7 s with 8 parallel; the cache does
not help a retarget (another disk). CPU (user+sys of the test process,
whole measurement, medians): 131 s → 121 s (cache) → 103 s (parallel); peak RSS
58–63 MiB in every case. Real processes and larger shards are not
measured here (Phase 3.14 verification document).

## Consequences

- An earlier node ignores the new fields and verifies everything (its
  `store_epoch` reads 0: the controller never caches it). An earlier
  controller never sends them. No rollback constraint.
- Memory: one entry per (group, epoch, slot, target) under repair, with
  the corrupt set (normally empty), at most 4,096.
- Parallel rebuild raises the target's write concurrency to 8 and the
  sources' read concurrency to 8 × `k`; the node's admission still
  applies per write.
