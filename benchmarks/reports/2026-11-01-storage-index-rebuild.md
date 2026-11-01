# Benchmark report: storage engine index rebuild on open (developer laptop)

- Date: 2026-11-01
- arca-lake commit: `6939ce8` (harness `crates/storage/examples/index_rebuild.rs`, added in the following commit)
- Author: akam1o
- Purpose: decide whether the storage node needs a persisted shard index or
  whether rebuilding the in-memory index from segments on open is
  acceptable (RFC-005 §5.5 and §10, OD-22, ADR-0006).

> **Scope warning.** Single laptop, internal SSD, files in the system
> temporary directory with the page cache warm from the preceding write.
> Useful for the *order of magnitude* of rebuild cost per record and per
> byte. Absolute numbers say nothing about server devices or cold caches.

## Environment

| Item | Value |
| --- | --- |
| Hardware model | Apple laptop, model identifier `Mac17,3` |
| CPU | Apple M5, 10 cores (4 performance + 6 efficiency) |
| Memory | 24 GiB |
| Storage device | internal NVMe SSD, APFS |
| OS and kernel | macOS 26.5.1, Darwin 25.5.0 |
| Rust toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13), release profile |
| Engine configuration | 1 GiB segments, 512 B alignment, 4 KiB checksum blocks, `SyncPolicy::Never` for the write phase (durability is not measured here) |
| Power / frequency | laptop defaults; not controlled |

## Method

```sh
cargo build --release -p arca-lake-storage --example index_rebuild
./target/release/examples/index_rebuild <dir> 100000 4096
./target/release/examples/index_rebuild <dir> 300000 4096
./target/release/examples/index_rebuild <dir> 1000000 128
```

- Each run writes `records` shards of `payload_bytes` into a fresh
  directory, drops the store, then times `Store::open`, which scans every
  segment and rebuilds the index by the sequence rule (RFC-005 §5.2).
- One run per configuration; no repetition, no cache dropping. The page
  cache was warm from the write phase, so the scan is CPU and syscall
  bound, not device bound.

## Results

| Records | Payload | Segments | Segment bytes | Open (s) | Records / s during rebuild |
| --- | --- | --- | --- | --- | --- |
| 100,000 | 4,096 B | 1 | 0.46 GB | 0.13 | ~750,000 |
| 300,000 | 4,096 B | 2 | 1.38 GB | 0.50 | ~600,000 |
| 1,000,000 | 128 B | 1 | 0.51 GB | 1.02 | ~990,000 |

## Interpretation

- *Verified* (this machine, warm cache): rebuild cost is linear in the
  number of records and in the bytes scanned; one million small records
  rebuild in about one second, and 1.4 GB of 4 KiB records in about half a
  second.
- *Verified*: the scan reads record headers and validates them; the
  per-record cost dominates for small payloads, the per-byte cost for large
  ones (the 300k run processed 3× the bytes of the 1M run in half the
  time per record).
- *Hypothesis*: on a cold cache the scan is bounded by sequential read
  bandwidth of the device; at a few GB/s a 4 TB device full of 1 MiB shards
  (~4 million records) would take on the order of 10³ seconds of I/O if the
  whole segment must be read, or far less if only headers are read with
  seeks. Which of the two applies depends on the record layout and was not
  measured; it is the measurement to run on server hardware before a
  persisted index is designed.
- Decision recorded in ADR-0006: no persisted index in Phase 1. Node restart
  time at Phase 1 scale is dominated by other factors.

Threats to validity: single run per point; warm page cache; laptop power
management; `SyncPolicy::Never` during the write phase does not affect the
open path but means the on-disk layout had no torn tails to handle.

## Raw data

The three output lines of the harness are reproduced in the results table;
no further raw data was produced.
