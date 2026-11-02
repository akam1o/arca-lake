# RFC-005: Storage Engine and On-disk Format

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-002
- Related ADRs: ADR-0004 (platforms), ADR-0005 (persistence model)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

A storage node must store shards so that an acknowledged write survives a
crash or power loss, a corrupt byte is never returned as valid, recovery
after a crash is automatic and idempotent, space is reclaimed without
risking live data, and the format can evolve without losing data written by
earlier versions.

## 2. Goals

- Durable acknowledgment: a shard is acknowledged only after it is on
  stable storage.
- Detect every corruption on read; verify only the bytes a range read
  touches.
- Recovery by scanning: no persisted index to keep consistent, no separate
  journal.
- Interrupted compaction is always safe and resumable.
- Explicit, versioned format; readers accept older versions.
- Few open files, sequential writes, bounded memory per request.

## 3. Non-goals

- Raw block devices and SPDK (DD-2 option C; deferred until measurements
  justify them).
- A persisted index (planned once rebuild time on large devices is
  measured).
- Erasure coding or placement logic; the engine stores opaque shards.

## 4. Requirements

| ID | Requirement | Status |
| --- | --- | --- |
| R1 | No acknowledged record is lost on process crash | *Verified*: SIGKILL test, 60 iterations, 3,422 acknowledged operations |
| R2 | No corrupt record is ever returned as valid | *Verified*: randomized bit flips (40), truncations (40), live corruption test |
| R3 | A torn tail is recovered by truncation; corruption in the middle is reported, not silently skipped | *Verified*: truncation test; scan distinguishes a torn tail from a hole by probing for later valid records |
| R4 | Recovery is idempotent | *Verified*: second open changes nothing in every crash test |
| R5 | Interrupted compaction never loses or resurrects data | *Verified*: fault injection at every step |
| R6 | Reads concurrent with compaction never fail or see wrong data | *Verified*: 4 reader threads during repeated compaction |
| R7 | Format v1 files written today open unchanged in the future | *Verified* by a golden segment byte-compared and opened |
| R8 | Power-loss semantics (lost page cache) | *Modelled* by the truncation tests; not yet verified on real hardware with power cuts |

## 5. Proposed design

### 5.1 Layout

One directory per device, `seg-<id:016x>.arca` append-only segments with
monotonically increasing IDs. Segment header (64 bytes, padded to the record
alignment): magic, format version, header length, record alignment, block
size, device UUID, segment ID, creation time, CRC-32C. Records start at
alignment boundaries and consist of a 60-byte fixed header, a per-block
CRC-32C table, a header CRC, the payload, and zero padding. The full byte
layout is in `crates/storage/FORMAT.md`.

Alignment (default 4 KiB in experiments) keeps record starts suitable for
direct I/O later; block size (default 64 KiB) bounds the bytes a range read
must verify.

### 5.2 Sequence rule

Every record carries a per-device sequence number. For a key, the record
with the highest sequence wins, whether data or tombstone, regardless of
which segment holds it. This single rule makes the index a pure function of
the set of records: duplicates and copies in any order yield the same state.
Tombstone sequences are remembered during the rebuild so that a data record
scanned after a newer tombstone stays dead; the memory is dropped after the
rebuild because new sequences are strictly increasing.

### 5.3 Write path and durability

- `put`/`delete` append one record under the writer lock. The active segment
  rolls over when it reaches the configured size; the old segment is synced
  before the new one is created.
- `SyncPolicy::Always`: `fdatasync` under the writer lock, then acknowledge.
- `SyncPolicy::Group` (default): after appending, the caller waits for a sync
  covering its `(segment, end)` position. If no sync is in progress it
  becomes the leader and syncs everything appended so far; otherwise it waits
  on a condition variable and re-checks. Durability positions compare
  lexicographically, which is valid because a segment is fully synced before
  its successor exists.
- Index insertion happens after durability, so a reader can never find a
  record that is not yet durable.

*Verified* (laptop, relative): with 16 concurrent writers of 64 KiB records,
group commit gave about 7.7× the aggregate throughput of per-record syncs;
with one writer the two are equal. On this platform one sync costs about
4 ms, consistent with a full device flush.

### 5.4 Read path

`get(key, range)` looks up the index, re-reads and validates the record
header (magic, version, flags, CRC, matching key and sequence) so that a
stale index entry can never return another record's bytes, reads only the
checksum blocks covering the range, verifies them, and returns the slice. A
record that fails validation is removed from the index ("quarantined") and
reported as corrupt; it is then missing, never wrong. A reader that finds its
segment gone (compacted meanwhile) re-resolves through the index once.

### 5.5 Recovery

On open, every segment is scanned in ID order; each record is validated
including payload blocks. Scanning stops at the first invalid record. If
later aligned positions contain no valid record, the invalid region is a
torn tail: it is truncated if it is in the last segment and otherwise
reported. If a valid record exists later, the region is a hole (corruption):
the valid prefix is indexed and the segment is reported and excluded from
appends. A last segment shorter than its header (crash during creation) is
deleted. The index is rebuilt by the sequence rule.

### 5.6 Compaction

Sealed segments are compacted by copying live data records and the
tombstones that may still shadow older data elsewhere (a tombstone is kept
when any other segment's minimum sequence is below the tombstone's), syncing
the output, compare-and-swapping the index entries, and unlinking the source.
A fault-injection hook at each step is part of the public API so that tests
(and later chaos tooling) can interrupt it. Reads holding the old file
complete on the unlinked inode. Candidates are chosen by dead-byte fraction.

### 5.6a Durable quarantine and resynchronising recovery (Phase 3.2)

A record that fails validation on read or scrub is quarantined durably:
the event is appended (and fsynced) to `<dir>/quarantine.log` before the
key leaves the live index, the bytes stay in the segment until the
record is superseded or tombstoned and the segment compacted, and the
entry is replayed at open. Recovery no longer stops at a record whose
payload fails its block CRCs: the record is skipped and quarantined; a
region whose header fails is a hole and the scan resynchronises at the
next aligned offset holding a fully valid record. Only a torn tail (no
later valid record) is truncated. Design comparison and the node and
controller sides: RFC-008 §8.2, ADR-0015.

### 5.7 Scrubbing

`scrub` reads every live record through the validating read path; corrupt
records are quarantined and listed. Background scheduling, rate limiting,
and reporting to the controller are RFC-008/011.

### 5.8 Versioning and compatibility

Format version is in every segment header; record version and flags in every
record. Unknown versions and unknown flag bits are explicit errors. A golden
v1 segment is checked into the repository and must open and byte-match
forever; a new format version must add a new golden file and a reader for
the old one before any release.

## 6. Alternatives considered

| Alternative | Why not |
| --- | --- |
| One file per shard | Metadata-heavy, per-file fsync, poor layout control (DD-2). |
| Shard bytes in an embedded KV store | LSM write amplification on large values (DD-2). |
| Persisted index with WAL | Second source of truth to keep consistent; rebuild-by-scan is simpler and was proven correct first. Revisit when rebuild time matters. |
| Timer-based group commit | Adds latency on idle systems; leader-based grouping adapts automatically. |
| Dropping tombstones at compaction unconditionally | Resurrects data in older segments (tested: `tombstones_are_kept_while_older_data_may_exist`). |

## 7. Failure scenarios

| Scenario | Behaviour | Evidence |
| --- | --- | --- |
| Process killed mid-append | Torn tail truncated; acknowledged records intact; unacknowledged ones present only if complete | SIGKILL test |
| Power loss after acknowledgment | Data was `fdatasync`ed before the ack; on reopen it is present | Truncation model; hardware verification pending (R8) |
| Bit rot in payload | Block CRC mismatch on read or scrub → corrupt, quarantined | Bit-flip test |
| Bit rot in header | Header CRC mismatch → record invalid; if last, torn tail; else corruption reported | Bit-flip test |
| Crash during compaction before unlink | Duplicate records; sequence rule yields the same state; re-run finishes | Fault-injection test |
| Crash during compaction after unlink | Only the output exists; index rebuilt from it | Fault-injection test |
| Crash during segment creation | Short last file deleted on open | Test in `crash.rs` |
| Segment from another device copied in | Rejected at open (device ID mismatch) | Test in `crash.rs` |
| Full disk | Append returns an I/O error; nothing acknowledged; the partial record is a torn tail on reopen | By construction; test pending |

## 8. Consistency and durability guarantees

- Acknowledged `put`/`delete` under `Always` or `Group` survive process
  crashes; they survive power loss to the extent `fdatasync` guarantees it on
  the platform.
- A `get` returns either the exact bytes of the latest acknowledged write of
  the key, or an explicit error.
- Not guaranteed: `SyncPolicy::Never`; anything after an I/O error that the
  caller ignored.

## 9. Performance considerations

Measured on a laptop (relative only; see `benchmarks/README.md`): sequential
append without sync is a few GiB/s; with sync it is bounded by the device's
flush latency; group commit recovers most of the throughput under
concurrency. Range reads verify only covered blocks. *Hypotheses* for
server hardware: NVMe flush latency is far lower, so the gap between
`Always` and `Group` shrinks but remains under high concurrency; `O_DIRECT`
and io_uring may reduce CPU per byte (to be measured, OD-09).

Index rebuild on open (*Verified* on a laptop with a warm page cache,
relative only; [report](../../benchmarks/reports/2026-11-01-storage-index-rebuild.md)):
linear in records and bytes scanned; on the order of 10^6 records per second.
A persisted index is therefore not needed at Phase 1 scale (ADR-0006).
*Hypothesis*: with a cold cache on a full large device the scan is bounded
by sequential read bandwidth; that measurement decides whether and when a
persisted index is worth a second source of truth.

## 10. Open questions

- Persisted index: decided against for Phase 1 (ADR-0006); revisit with a
  cold-cache measurement on server hardware.
- `O_DIRECT` and io_uring backends (OD-09).
- CRC32C vs CRC64-NVME for blocks (OD-10).
- Tombstone retirement when `min_seq` tracking is coarse (e.g., long-lived
  segments with one old record keep all newer tombstones alive).
- Full-disk and ENOSPC behaviour test.

## 11. Acceptance criteria

- All tests in `crates/storage/tests` pass on Linux in CI (and on macOS for development)
  (*Verified* at the time of writing).
- SIGKILL test runs with ≥ 1,000 iterations in a nightly job (planned).
- A power-loss test on real hardware or a write-cache-dropping simulator
  before Phase 3 exit.
- Golden file test blocks any format change that is not accompanied by a
  new version and reader.
