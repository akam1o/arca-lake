# Benchmark report: erasure coding backends (single core, developer laptop)

- Date: 2026-11-01
- arca-lake commit: `9648f08` (benchmark harness `crates/ec/benches/ec.rs`)
- Author: akam1o
- Purpose: inform OD-07 (default EC backend) and test the hypothesis in
  [technology selection](../../docs/architecture/technology-selection.md#erasure-coding)
  that an O(n log n) Leopard-style code is a poor fit for single-shard
  reconstruction at storage-typical `m`.

> **Scope warning.** This is a single-core run on a laptop. It is useful
> for the *relative* comparison between backends on the same machine. The
> absolute figures are not representative of server hardware, are subject to
> laptop power management, and must not be quoted as arca-lake performance.

## Environment

| Item | Value |
| --- | --- |
| Hardware model | Apple laptop, model identifier `Mac17,3` |
| CPU | Apple M5, 10 cores (4 performance + 6 efficiency), no SMT |
| Memory | 24 GiB |
| Storage device | not relevant (in-memory benchmark) |
| Network | not relevant |
| OS and kernel | macOS 26.5.1, Darwin 25.5.0 |
| Rust toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13) |
| `reed-solomon-erasure` | 6.0.0, pure Rust (`simd-accel` feature **off**) |
| `reed-solomon-simd` | 3.1.0 (runtime-selected NEON engine) |
| Power / frequency | laptop defaults; no pinning; thermal state not controlled |

## Method

```sh
cargo bench -p arca-lake-ec --all-features --bench ec -- --warm-up-time 1 --measurement-time 2 --noplot
```

- Criterion, 100 samples per benchmark, 1 s warm-up, 2 s measurement
  (reduced from criterion defaults; several benchmarks reported that 100
  samples did not fit in 2 s, which widens confidence intervals).
- Single thread. Throughput is bytes of **data** (`k × shard_len`) per
  second, so profiles with different overhead are comparable by payload.
- `encode`: compute `m` parity shards from `k` data shards.
- `reconstruct_1_data_shard`: restore data shard 0 from the remaining
  `k + m − 1` shards (the common degraded-read and repair case).
- `reconstruct_m_shards`: restore shards `0..m` (worst case for the code).
- The Leopard backend creates its encoder/decoder objects per call (see the
  backend's module docs); a pooled implementation would reduce its fixed
  cost, especially at 64 KiB.
- The machine was otherwise idle; a lock-file-only `cargo deny` run
  overlapped briefly with the final benchmarks.

## Results (median throughput, GiB/s of data)

| Profile | Shard | encode gf8 | encode leopard | 1-shard gf8 | 1-shard leopard | m-shard gf8 | m-shard leopard |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4+2 | 64 KiB | 10.37 | 14.17 | 17.61 | 1.36 | 8.99 | 1.32 |
| 4+2 | 1 MiB | 10.13 | 15.16 | 18.45 | 2.82 | 9.00 | 2.52 |
| 6+3 | 64 KiB | 6.69 | 7.79 | 18.04 | 1.26 | 6.03 | 1.17 |
| 6+3 | 1 MiB | 6.56 | 8.18 | 18.14 | 1.83 | 5.94 | 1.64 |
| 8+3 | 1 MiB | 6.43 | 10.40 | 18.35 | 2.36 | 5.75 | 2.07 |
| 8+4 | 64 KiB | 4.90 | 9.84 | 18.04 | 1.64 | 4.55 | 1.58 |
| 8+4 | 1 MiB | 4.78 | 10.29 | 18.25 | 2.33 | 4.30 | 1.98 |
| 12+4 | 1 MiB | 4.68 | 10.54 | 17.32 | 2.96 | 4.21 | 2.38 |
| 16+4 | 64 KiB | 4.79 | 10.80 | 17.81 | 1.75 | 4.50 | 1.65 |
| 16+4 | 1 MiB | 4.67 | 10.64 | 18.10 | 1.69 | 4.23 | 1.41 |

All 108 measurements (including 256 KiB shards and confidence intervals) are
in [`data/2026-11-01-ec-backends-m-series-laptop.tsv`](data/2026-11-01-ec-backends-m-series-laptop.tsv).

## Interpretation

*Verified on this machine:*

- **Single-shard reconstruction** is 6–14× faster with the classic GF(2^8)
  code than with the Leopard code, across every profile and shard size. The
  classic backend reconstructs one shard at roughly memory-copy speed
  (~18 GiB/s regardless of `k` and `m`) because it only needs to combine
  `k` shards with one row of the decoding matrix. The Leopard backend's
  reconstruction cost is essentially the same whether 1 or `m` shards are
  missing, as its README states.
- **Encode** is 1.2–2.3× faster with the Leopard code, and its advantage
  grows with `k + m`. For the classic backend, encode throughput scales
  roughly with `1/m` (each parity shard is a full pass over the data).
- The classic backend was built **without** its C/SIMD acceleration; its
  encode numbers are therefore a floor, not what ISA-L-class code would do.

*Hypotheses (not verified here):*

- With ISA-L or the `simd-accel` feature, classic encode would close much of
  the gap to Leopard. Needs a Linux x86-64 and AArch64 measurement.
- Pooling Leopard encoder/decoder objects would improve its 64 KiB results
  noticeably but would not change the reconstruction picture by an order of
  magnitude.
- Multi-threaded throughput scales linearly for both backends (stripes are
  independent); not measured.

*Consequence for OD-07:* in a storage system the dominant decode case is the
loss of one shard (one failed device or one corrupt block), and repair after
a device failure is exactly `N × reconstruct_1_shard`. On this evidence the
classic GF(2^8) scheme (`rs-gf8-v1`) is the right default for storage
profiles, with the Leopard scheme kept as an optional backend for very wide
profiles where its encode advantage dominates. This recommendation should be
confirmed on server-class hardware before the ADR is written.

## Threats to validity

- Laptop with heterogeneous cores and uncontrolled frequency scaling.
- Reduced measurement time; confidence intervals in the TSV are wider than
  criterion defaults would give.
- Pure-Rust classic backend only; no ISA-L comparison yet.
- Per-call object construction in the Leopard backend inflates its fixed
  cost at small shard sizes.

## Raw data

`benchmarks/reports/data/2026-11-01-ec-backends-m-series-laptop.tsv`
(criterion summary lines; `time_*` and `thrpt_*` are the lower bound,
estimate, and upper bound of the 95% confidence interval).
