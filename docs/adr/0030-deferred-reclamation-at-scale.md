# ADR-0030: Deferred reclamation at scale, retention changes and capacity accounting (R-106)

Status: **accepted** (Phase 3.13). Amends ADR-0028 (its invariants are
unchanged).

## Context

ADR-0028's node state was one append-only log (`pending-reclaim.log`)
replayed into a map at start, every change appended and `fsync`ed under
the state's lock, compacted by rewriting the whole state under that
lock. Measured at Phase 3.12's end (release build, one 4-vCPU VM, a
virtual disk; synthetic records):

| Records | Log | Open | Memory | Compaction (lock held) | Retire, durable |
| --- | --- | --- | --- | --- | --- |
| 1e5 | 9.4 MB | 62 ms | 124 B/record | 0.15 s | 0.97 ms, serial |
| 1e6 | 96 MB | 0.66 s | 124 B/record | 1.4 s | 0.98 ms, serial |
| 1e7 | 976 MB | 7.1 s | 124 B/record | 13.6 s | 1.1 ms, serial |

So: every delete waited for its own `fsync` while holding the lock
(about 1,000 retirements/s per node whatever the concurrency); a
compaction at 1e7 stopped every delete, write role note and listing for
13.6 s; `ListPending` returned every record to the controller, which
filtered by age itself.

Also found: lowering a node's retention shortened the protection of
records retired before (the check used the current retention), and
write admission credited GC's deleted bytes as room coming back,
although a deferred delete frees nothing before its reclaim.

## Options considered

| | Effect | Cost | Kept |
| --- | --- | --- | --- |
| Expiry-ordered index (by `retired_ms`) | due records without a scan | a second ordered map (≈ +40 B/record), kept in step with every change | no: the bounded scan below costs 2 ms per 100k records |
| Paging with a bounded scan and a due filter on the node | listing cost bounded per call; only due records cross the network | none | yes (`ListPending.due_only`) |
| Group commit of appends | one `fdatasync` for every append waiting | a writer thread is not needed (the first waiter writes) | yes |
| Incremental (background) compaction | the lock held per chunk of 2,000 records, and for the lines since the seal | a sealed log during a compaction | yes |
| Async log I/O | — | no gain over group commit on one file | no |
| Interned incarnations | 16 B → 4 B per record | — | yes |

## Decision

- **Group commit.** Appends are queued in memory under the lock (no
  system call). A retirement or a durable role note is acknowledged only
  once a waiter wrote every queued line and synced once
  (`Syncer::wait`, one writer at a time, the others woken). A failed
  write or sync makes the state damaged for good (fail-closed: nothing
  reclaimed, deletes still retained). Ends (`x`) and cache roles are not
  waited for (losing them is safe, ADR-0028). The queue is bounded
  (1 MiB, then written at once); a clean stop writes it.
- **Background compaction.** The log is sealed (synced, renamed
  `.prev`, a new log opened) under the lock; the snapshot is written to a
  temporary file in chunks of 2,000 records, each under the lock for a
  moment, and synced; then, under the lock, the lines appended since the
  seal are added, synced, and the file renamed over the log (the appends
  go on in it); `.prev` is removed. Replay order at a start: `.prev`,
  `.log`; every line sets one key's
  state from the line alone, so replaying a suffix over a snapshot that
  contains part of it ends in the same state. A `.prev` (or a torn tail)
  found at a start is merged at once.
- **Due-only listing.** `ListPendingRequest.due_only` (field 4): the
  node returns only records due by its own clock and looks at most
  100,000 records per call (a page may be short or empty with a next
  cursor). The controller's reclaim pass uses it; the node's checks at
  reclaim are unchanged.
- **Retention history** (`pending-reclaim.retention`, a file of its
  own, lines with CRCs): the last configured retention and, for each
  lowering, `(earlier retention, until)`. A record retired before
  `until` is kept at least the earlier retention; `until` is at least
  the newest retirement known at the start (a clock behind it does not
  shorten it). Raising the retention covers everything at once. At a
  retention of 0, deletes are immediate again except of a record retired
  before, which stays retired. An entry is dropped once no retired record
  predates its end (by the records, never by the clock). A damaged history
  fails closed.
- **Retained bytes.** Each record's payload size is kept in memory
  (measured at start from the store, noted at retirement): node health
  `reclaim_retained_bytes` (field 46). `ReclaimShardResponse.payload_bytes`
  (field 1) reports what a reclaim removed:
  `arca_controller_reclaimed_bytes_total`.
- **Admission.** The room write admission expects back is what reclaims
  removed, plus what GC deleted only while no node defers deletes
  (`room_returned_bytes`). Retired records stay counted as used (they
  are on the device: `device_free_bytes` excludes them, `live_bytes`
  includes them); compaction's reclaimable bytes exclude them: nothing
  is counted twice.

## Capacity terms

| Term | Where | Notes |
| --- | --- | --- |
| Logical live bytes | metadata (objects) | client view |
| Physical live bytes | node `live_bytes` | includes retired-retained |
| Retired but retained | node `reclaim_retained_bytes` | until reclaimed (≥ retention) |
| Reclaimable | node `reclaimable_bytes` | dead segment space compaction can free; retired records are not in it |
| Reserved | node reserve (client writes stop above it) | node-enforced |
| Repair reserve | below the client reserve, down to the repair floor | failure repair only |
| Migration reserve | planned moves keep the client reserve | `WriteClass::Migration` |
| Available | node `device_free_bytes` | what admission and placement use |

## Measured after (same VM and method)

| Records | Open | Memory | Background compaction (total) | Longest lock wait during it (8 retiring threads) | Durable retirements/s during it | Reopen after |
| --- | --- | --- | --- | --- | --- | --- |
| 1e5 | 72 ms | 108 B | 0.16 s | 11 ms | 3,069 | 65 ms |
| 1e6 | 0.73 s | 108 B | 1.5 s | 25 ms | 2,009 | 0.65 s |
| 1e7 | 8.1 s | 108 B | 14.1 s | 18 ms | 1,567 | 7.1 s |

Group commit without compaction: 994 retirements/s with one thread,
2,425 with 4, 4,106 with 8, 8,016 with 16 (one `fdatasync` ≈ 1 ms on
that disk). A due scan of 100k records takes about 2 ms.

## Remaining O(N) work and its limits

- **Start** replays every record: about 0.8 µs/record measured (8.1 s at
  1e7; ≈ 80 s at 1e8, *estimated*). Memory 108 B/record (1.1 GB at 1e7).
- **Background compaction** rewrites every record: about 1.4 µs/record
  (14 s at 1e7), off the lock except per chunk.
- **A reclaim pass** walks every record of a node, 100k per call:
  about 2 ms per call at the node (200 ms per pass at 1e7, *plus*
  network round trips).
- Practical bound: 1e7 records per node is the measured scale; beyond
  it, the start time and the memory grow linearly (estimates only).

## Invariants kept (ADR-0028)

- Nothing reclaimed before the retention (the per-record retention is
  never below the one in force when it was retired).
- A clock behind the retirement does not reclaim early (the node's
  checks; the retention history's `until` is never before the newest
  retirement).
- CRC errors, damage, or a failed write or sync: damaged, fail-closed.
- A retirement is acknowledged only once durable.
- Sequence and incarnation per record are unchanged by compaction (the
  snapshot writes each record's line; tested under concurrent changes
  and with crashes at each step).

## Verification

`reclaim.rs` unit tests (background compaction under concurrent
changes, a crash after the seal and before the sealed log's removal,
lowered retention across restarts and a damaged history, retained bytes)
and two ignored measurements (`scale_of_the_retirement_state` with
`ARCA_RECLAIM_SCALE`, `group_commit_throughput`). Real-process load:
Phase 3.13 verification document.

## Compatibility and rollback

- New proto fields only (`due_only`, `reclaim_retained_bytes`,
  `payload_bytes`); an earlier node ignores `due_only` and lists every
  record (the controller still filters by age); an earlier controller
  ignores the new fields.
- Files: outside a compaction the state is one `pending-reclaim.log` in
  the Phase 3.12 line format (a compaction writes the snapshot to a
  temporary file, appends what came after its seal, and renames it over
  the log). A Phase 3.12 node started on a stopped 3.13 node's directory
  reads it as its own. **Rollback boundary:** a `pending-reclaim.prev`
  left by a compaction a crash interrupted is read only by 3.13 (which
  merges it at its next start): start the node once on 3.13 before rolling
  it back. `pending-reclaim.retention` is ignored by 3.12.
- A retention lowered on 3.13 is not honoured by 3.12 (it applies the
  configured retention to every record): keep the earlier retention
  configured when rolling a node back.
