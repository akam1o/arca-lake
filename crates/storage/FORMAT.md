# Storage engine on-disk format, version 1 (Phase 0 prototype)

Status: **prototype**. This format exists to run the Phase 0 I/O and crash
experiments. RFC-005 will define the production format; it may keep this
layout or supersede it with a new format version. Anything written with
version 1 must remain readable by whatever supersedes it, or be explicitly
declared non-upgradeable before any release.

All integers are little-endian. All checksums are CRC-32C (Castagnoli).

## Device directory

One directory per device:

```text
<device-dir>/
  seg-<segment_id:016x>.arca   append-only segments, segment_id increases monotonically
```

The device identity (UUID) is embedded in every segment header, so a
directory whose segments carry mixed device IDs is rejected at open.

## Segment file

```text
+-----------------------+  offset 0
| segment header (64 B) |
| zero padding          |  up to record_alignment
+-----------------------+  offset = record_alignment
| record 0              |
| zero padding          |  record end rounded up to record_alignment
+-----------------------+
| record 1              |
| ...                   |
```

### Segment header (64 bytes)

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | magic `ARCASEG1` |
| 8 | 2 | format version (`1`) |
| 10 | 2 | header length (`64`) |
| 12 | 4 | record alignment in bytes (power of two, ≥ 64) |
| 16 | 4 | block size in bytes for payload checksums (multiple of 64) |
| 20 | 16 | device UUID |
| 36 | 8 | segment ID |
| 44 | 8 | creation time, Unix nanoseconds (informational) |
| 52 | 8 | reserved, zero |
| 60 | 4 | CRC-32C of bytes 0..60 |

### Record

A record starts at an offset that is a multiple of `record_alignment`.

```text
+----------------------------+
| fixed header (60 B)        |
| block CRC table (4 B × n)  |  n = ceil(payload_len / block_size)
| header CRC (4 B)           |  CRC-32C of fixed header + table
| payload (payload_len B)    |
| zero padding               |  to the next multiple of record_alignment
+----------------------------+
```

Fixed header:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | magic `ARCR` |
| 4 | 1 | record version (`1`) |
| 5 | 1 | flags: bit 0 = tombstone |
| 6 | 2 | shard index |
| 8 | 8 | stripe group ID |
| 16 | 16 | chunk ID |
| 32 | 4 | payload length (bytes) |
| 36 | 4 | block count `n` |
| 40 | 8 | sequence number (monotonic per device) |
| 48 | 12 | reserved, zero |

The shard key is `(group ID, chunk ID, shard index)`. For every key, the
record with the **highest sequence number** wins, whether it is a data
record or a tombstone; segment order and duplicates do not matter. Retries
are therefore idempotent, and records copied forward by compaction (which
keep their sequence numbers) cannot change the outcome. A tombstone has
`payload_len = 0` and marks the key deleted.

Sequence numbers are unique and increasing per device; the next sequence
after recovery is one more than the highest sequence found in any segment.

Each payload block of `block_size` bytes (the last block may be shorter) has
its own CRC so that a byte-range read verifies only the blocks it touches.

## Durability and recovery

- A `put` returns only after the record (header, payload, padding) has been
  written and `fdatasync` has completed, unless the store was opened with a
  non-durable policy for tests.
- A new segment is created by writing its header, syncing the file, and
  syncing the directory.
- On open, every segment is scanned in ID order. Each record is validated
  (magic, version, sanity of lengths, header CRC, block CRCs). Scanning stops
  at the first invalid record. If the invalid region is in the last segment
  and is followed only by zeros or by end of file, it is treated as a torn
  tail from a crash and truncated. Otherwise it is reported as corruption;
  the valid prefix stays readable and the rest is excluded from the index.
- The index is not persisted in version 1; it is rebuilt by scanning.
  During the rebuild, the highest tombstone sequence per key is remembered
  so that a data record scanned after a newer tombstone stays dead.
- Nothing is ever modified in place.

## Durability policies

- `Always`: `fdatasync` under the writer lock after every record.
- `Group` (default): records are appended under the writer lock; the caller
  then waits for a sync that covers its record. The first waiter performs
  one `fdatasync` on behalf of every record appended so far. Both policies
  acknowledge only durable records. When the active segment rolls over, it
  is synced before the new segment is created.
- `Never`: tests and measurements only.

## Compaction

Compaction rewrites a sealed segment `S` into a new segment `N`:

1. Scan `S`. Copy each data record that the index still maps to its
   location in `S`. Copy each tombstone whose sequence is greater than the
   smallest sequence present in any *other* segment (an older record for
   its key may still exist there); drop the rest.
2. `fdatasync` `N` and the directory.
3. Retarget the index entries of the copied records from `S` to `N`
   (compare-and-set: an entry that changed meanwhile is left alone).
4. Unlink `S` and sync the directory.

A crash at any point leaves a consistent store: before step 4 both copies
exist and are identical, and the index rule above is order independent, so
the rebuild yields the same state; after step 4 only `N` exists. Re-running
compaction on `S` completes an interrupted run. Reads that already hold the
file of `S` finish on the unlinked file (POSIX semantics); new reads go
through the index.

## Quarantine log (Phase 3.2, RFC-005 §5.6a)

`<device-dir>/quarantine.log` is an append-only text file, fsynced after
every line, recording records that failed validation and the release of
those entries. It is evidence and history: recovery re-derives the
quarantine state by re-scanning the segments, so the log is never
required for correctness and may be deleted by an operator after
investigation.

```text
quarantine group=<u64> chunk=<32 hex> index=<u16> segment=<u64> offset=<u64> sequence=<u64> len=<u32> reason="<text>" ms=<unix ms>
release    group=<u64> chunk=<32 hex> index=<u16> segment=<u64> offset=<u64> sequence=<u64> cause=superseded|tombstone|purged ms=<unix ms>
```
