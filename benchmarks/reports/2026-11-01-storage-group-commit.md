# Benchmark report: storage engine append, group commit, and range reads (developer laptop)

- Date: 2026-11-01
- arca-lake commit: `c9c25a5` (harness `crates/storage/benches/io.rs`)
- Author: akam1o
- Purpose: decide the default durability policy of the storage engine
  (`SyncPolicy::Always` vs `SyncPolicy::Group`) and record the engine's own
  overhead for RFC-005 §9.

> **Scope warning.** Single laptop, internal SSD, files in the system
> temporary directory. Useful for the *relative* comparison of policies on
> one device. Absolute numbers say nothing about server NVMe devices.

## Environment

| Item | Value |
| --- | --- |
| Hardware model | Apple laptop, model identifier `Mac17,3` |
| CPU | Apple M5, 10 cores (4 performance + 6 efficiency) |
| Memory | 24 GiB |
| Storage device | internal NVMe SSD, APFS; `std::fs::File::sync_data` (on this platform a full flush request) |
| OS and kernel | macOS 26.5.1, Darwin 25.5.0 |
| Rust toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13) |
| Engine configuration | `StoreConfig::new` defaults: 1 GiB segments, 4 KiB alignment, 64 KiB checksum blocks |
| Power / frequency | laptop defaults; not controlled |

## Method

```sh
cargo bench -p arca-lake-storage --bench io -- --warm-up-time 1 --measurement-time 3 --noplot
```

- Criterion; 20 samples (append, read) or 10 samples (concurrent) per
  benchmark; 1 s warm-up; 3 s measurement.
- `append/<policy>/<bytes>`: one writer, one record per iteration.
- `append_concurrent/<policy>/<threads>`: `threads` writers each appending
  64 KiB records; throughput is the aggregate; time is per "round" (one
  record from every thread).
- `read/range/<bytes>`: random range reads from 64 × 1 MiB records with
  checksum verification of the covered blocks, data in page cache.

## Results (criterion estimates)

| Benchmark | Time | Throughput |
| --- | ---: | ---: |
| append/Always/64 KiB | 3.86 ms | 16.2 MiB/s |
| append/Always/1 MiB | 4.08 ms | 245 MiB/s |
| append/Group/64 KiB | 3.93 ms | 15.9 MiB/s |
| append/Group/1 MiB | 4.08 ms | 245 MiB/s |
| append/Never/64 KiB | 19.3 µs | 3.15 GiB/s |
| append/Never/1 MiB | 256 µs | 3.82 GiB/s |
| append_concurrent/Always/1 | 3.95 ms | 15.8 MiB/s |
| append_concurrent/Always/4 | 15.7 ms | 15.9 MiB/s |
| append_concurrent/Always/16 | 62.8 ms | 15.9 MiB/s |
| append_concurrent/Group/1 | 3.93 ms | 15.9 MiB/s |
| append_concurrent/Group/4 | 6.35 ms | 39.4 MiB/s |
| append_concurrent/Group/16 | 8.16 ms | 122.5 MiB/s |
| read/range/4 KiB | 9.5 µs | 411 MiB/s |
| read/range/64 KiB | 22.8 µs | 2.68 GiB/s |
| read/range/1 MiB | 189 µs | 5.16 GiB/s |

## Interpretation

*Verified on this machine:*

- **One sync costs about 4 ms** regardless of record size (64 KiB or
  1 MiB), so with a single writer both durable policies are flush-bound and
  identical. The non-durable path shows the engine's own CPU cost is small
  (microseconds).
- **Group commit scales with concurrency; per-record sync does not.** With
  16 writers, `Always` serializes 16 flushes per round (62.8 ms) while
  `Group` needs about two (8.2 ms), giving 7.7× the aggregate throughput.
  Both policies acknowledge only after the data is flushed.
- Range reads of 4 KiB are dominated by fixed costs (header re-validation,
  one block CRC); larger ranges approach memory bandwidth from page cache.

*Hypotheses (not verified here):*

- Server NVMe flush latency is much lower, so absolute append throughput
  will be far higher and the `Always`/`Group` gap smaller at low
  concurrency, but the gap persists at high concurrency.
- `O_DIRECT` and io_uring change CPU per byte, not the flush cost.

*Decision input:* `SyncPolicy::Group` is the default (committed in
`c9c25a5`); it is never less durable than `Always` and never slower in
these measurements.

## Threats to validity

- Laptop device and file system; page cache warm for reads.
- Criterion's reduced measurement time; concurrent benchmark uses a custom
  timing loop with 10 samples.
- Platform-specific `sync_data` semantics; the cost per sync on Linux
  depends on the device's volatile cache handling.

## Raw data

Criterion output in `target/criterion/` at the time of the run; the table
above is the complete set of estimates.
