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
- `arcabench/` (Phase 4.0): `arcabench`, the S3 workload generator of
  the scalability baseline. Workloads `put`, `get`, `head`, `delete`,
  `list`, `multipart`, `mixed`, `verify`, `create-buckets`; size or size
  range, object count, concurrency, buckets, key layout
  (`sequential`/`hashed`), key distribution (`uniform`, `zipf`, `hot`,
  `sequential`), read/write/delete/list ratios, a fixed seed. Integrity:
  one writer per key; a body is a 16-byte header (index, version, size)
  and a stream of `(seed, index, version)`; a read accepts only a version
  acknowledged or in flight while it ran, of that version's full length
  (ranges: the exact bytes and length); a 404 only when a delete or no
  write was possible; `--state-file` carries every key's versions across
  runs (the same `--objects` throughout). The seed defines the bodies: a
  run that checks another run's objects must use the same `--seed`
  (`--client-id` varies the key choices instead). `verify` exits non-zero
  when a key could not be read (an error other than an allowed 404). The
  generator is closed-loop: latency under saturation is that of the
  requests the clients managed to send (no coordinated-omission
  correction); failed requests are counted by class, not in the
  histogram. `nodebench`: one storage node's `PutShard`/`GetShard` per
  size and concurrency (reads hit shards just written: page cache).
- `arcabench/` `contend` (Phase 4.1): many writers on a few keys through
  S3 (every request and retry attempt logged with its times and answer),
  then a check of what every gateway shows against the log: no
  acknowledged write lost, no stale or backward read, versions (with
  `--versioned`) in real-time order with the ids they were answered with,
  no PUT answered SlowDown visible. Outcomes other than 2xx and SlowDown
  are unknown and constrain nothing.
- `metabench/` `metaops` (Phase 4.0): object metadata operations
  (`populate`, `put`, `overwrite`, `get`, `head`, `delete`, `list-page`,
  `map-version`, `access`) through the gateway's functions on
  FoundationDB, with transaction attempts, reads, rows, bytes and
  conflicts per operation from a counting store.
- `linux/bench-cluster.sh` (Phase 4.0): a two-host cluster on a private
  FoundationDB, `run` (generators detached, samplers on both hosts,
  results stripped of addresses), `quiesce`, `snapshot`, `scrub`,
  `restart-gateways`; `linux/object-count.sh` the object-count campaign;
  `linux/summarize_bench.py` per-run summaries over the generator's
  measurement window.

## Reports

Published reports are in [`reports/`](reports/). Each report is a single
Markdown file named `YYYY-MM-DD-<topic>.md` and must follow the template.
