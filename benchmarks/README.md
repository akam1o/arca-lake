# Benchmarks

This directory holds benchmark harnesses and *published* benchmark reports.

## Rules

1. **Every published number must be reproducible.** A report must describe
   the environment completely enough that a third party can rerun it:
   hardware model or cloud instance type, CPU, memory, storage device
   class, OS and kernel version, Rust toolchain, arca-lake commit, exact
   command line, and configuration. Use [`reports/TEMPLATE.md`](reports/TEMPLATE.md).
2. **Numbers from undisclosed environments are not published.** Developer
   machines may be used for *relative* comparisons during development, and
   such results may be quoted in pull request discussions as "local,
   relative only", but they do not go into `reports/`, design documents, or
   the README.
3. **No extrapolation.** A report states what was measured. Expectations
   about other hardware are labelled *Hypothesis*.
4. **No non-public information.** See `docs/CONTRIBUTING-POLICY.md`.

## Micro-benchmarks

Criterion benchmarks live inside the crates:

| Crate | Benchmark | What it measures |
| --- | --- | --- |
| `arca-lake-ec` | `benches/ec.rs` | Encode, 1-shard reconstruction, `m`-shard reconstruction per backend, profile, and shard size. Throughput is bytes of *data* (`k × shard_len`) per second. |
| `arca-lake-storage` | `benches/io.rs` | Append throughput per record size with and without `fdatasync`; random range reads with checksum verification. Set `ARCA_STORAGE_BENCH_DIR` to the device under test. |

Run, for example:

```sh
cargo bench -p arca-lake-ec --all-features --bench ec
# faster, for relative comparisons only:
cargo bench -p arca-lake-ec --all-features --bench ec -- --warm-up-time 1 --measurement-time 2
# include 4 MiB shards:
ARCA_EC_BENCH_FULL=1 cargo bench -p arca-lake-ec --all-features --bench ec
```

Results are written by criterion to `target/criterion/`.

## End-to-end harness

- `s3load/`: S3 PUT/GET load generator (anonymous gateway) with
  deterministic verifiable bodies, range GETs, repeated GET passes and one
  JSON summary line per pass.
- `linux/two-host.sh`: starts a cluster on two or three Linux hosts (host
  addresses only from the environment), runs `s3load` phases while
  sampling CPU and network with `sar`, collects gateway cache counters;
  `linux/matrix.sh` is the RFC-009 matrix, `linux/summarize.py` renders
  Markdown, `linux/sanitize.sh` strips hostnames and addresses before
  results are committed. Results: `linux/results/` (RFC-009 §6).
- `transfer/`: shard transfer floors (raw framed TCP versus gRPC, with
  and without TLS), usable across hosts.

## Reports

Published reports are in [`reports/`](reports/). Each report is a single
Markdown file named `YYYY-MM-DD-<topic>.md` and must follow the template.
