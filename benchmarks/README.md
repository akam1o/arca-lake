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
  Refusals (Phase 4.1, `arcabench/src/retry.rs`): a `503` is retried only
  with `--retries N` (exponential backoff from `--retry-base-ms` to
  `--retry-max-delay-ms`, `--retry-jitter none|full|equal`,
  `--retry-max-time-ms` from the first refusal, `--retry-honor-retry-after`;
  over the process `--retry-max-concurrency` retries in flight and
  `--retry-max-rate`/`--retry-burst` retries per second; a retry a limit
  would delay past the time budget is not sent). A worker whose operation
  ended refused waits the same backoff before its next one
  (`--reject-pause on`, the default; `off`: at once, the Phase 4.0
  behaviour, under which a refusing cluster sees the whole concurrency as
  a stream of requests). Every wait ends at the run's deadline. The
  summary separates logical operations (`ops`, `ops_rejected`) from the
  requests they made and the HTTP attempts those took (`requests`:
  `requests`, `http_attempts`, `retries`, `retryable_failures`,
  `non_retryable_failures`, `retry_exhausted`, `retry_denied_*`,
  `retry_wait_us_*`, `reject_pauses`; the measured window, as `ops`;
  `requests_whole_run` and `ops_rejected_whole_run` include warm-up and
  drain) and records the policy (`retry_policy`). Operations still
  waiting or in flight at the deadline are ended and counted in
  `ops_cancelled`; their latencies are not in `ops` (with retries or
  pacing, report it next to the percentiles). Ending them leaves work on
  the server: a PUT whose body was sent may still commit (the key's
  version stays pending, so a later verify accepts either outcome), and
  a multipart upload cut between its start and its completion stays
  incomplete until aborted; quiesce between runs of a series. Window
  boundaries are taken by timers, not per request: ratios between
  `ops`, `ops_rejected` and `requests` are exact over long windows and
  approximate at their edges. Every `503` is retried
  alike, including the gateway's "outcome unknown" (R-114): a retried PUT
  then sends the same version again, which the integrity check accepts;
  in a versioned bucket it may leave one more version. A successful
  operation's latency includes its retries' waits. Runs with pacing are
  not the same load as Phase 4.0 or the first Phase 4.1 runs: compare
  offered, attempted and completed rates, not one figure.
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
- `linux/adr35-integration.py` (ADR-0035 option A): a disposable
  three-host cluster with two controllers and two gateways (`setup`,
  `run [SCENARIO ...]`, `collect`, `teardown`; hosts and paths from the
  environment) running the concurrent-initialisation, leader-change,
  reflection and stale-confirmation scenarios with deadlines and checks;
  needs a binary built with `--features fdb,fault-injection`.
  `linux/adr35-loader.py` its bounded GET load, logged per request;
  `linux/adr35-summary.py` a run's publishable summary.
- `linux/r110/` (R-110 capacity retest,
  `docs/verification/phase-4.1-r110-retest-plan.md`; standard library
  only, tests in `linux/r110/tests`, run by CI without any host:
  `cd benchmarks/linux && python3 -I -m unittest discover -s r110/tests -t .`).
  `budget.py`: the run's one budget for every data-plane HTTP attempt
  (token bucket, concurrency, attempts, body bytes reserved before
  sending, live set, run deadline; one process per run, never resumed);
  `ledger.py`: the outcome ledger (an intent before each request, an
  outcome after, CRC per line, fsync, unknown outcomes as candidate
  states); `load.py`: the load process, executing the driver's steps,
  which also stops by itself when the watchdog's heartbeat goes stale, on
  a 5xx other than 503, and when the baseline-key probe (a GET every 5 s)
  fails 3 times in a row or answers in more than 5 s.
  `guard.py`: the on-host guard, parent of the load (its own session),
  stopping the load's whole process group without any network on a
  stale heartbeat, low free space or memory, a check it cannot make, its
  deadline or the run's stop file, and the test services by recorded
  identity on a resource or monitor reason; `watchdog.py`: the external
  watchdog (ssh readings of every host, the heartbeat only after a round
  whose every required check passed, a breach written as the run's stop
  file; it never kills a process); `proc.py`: process identity (pid,
  start time, working directory), never names.
  `sampler.py`: every 15 s, the metrics families of plan §2 from each
  process's endpoint, each node directory's apparent and allocated bytes
  and the controller log's reconcile counts, each sample stamped (run,
  target, wall start and end, monotonic duration) with a status (`ok`,
  `partial`, `error`, `timeout`); a value not read is absent, never 0;
  bounded workers, timeouts and output size. Its own load on the hosts at
  scale is not measured.
  `driver.py`: `check CONFIG` (the default) validates a run
  configuration against the plan and prints the stages, contacting
  nothing; `preflight`, `run` and `teardown` need `--execute` and an
  approval file naming the configuration's sha256, the scope and a time
  window. `run` executes the seven stages (each with its exit condition
  and maximum; the smoke stage's stops in sub-runs of their own), keeps
  the evidence in a directory created once per run id, and judges with
  `judge.py` (plan §8: C1 to C5 and the run's outcome; missing data is
  Inconclusive, never a pass). `remote.py`: the hosts' commands (marker
  checked setup and teardown, processes started and stopped by identity).
  `example-run.json` is an unapproved example with placeholders.
  `tests/mutate.py` removes each protection in turn; every removal must
  fail a test.

## Reports

Published reports are in [`reports/`](reports/). Each report is a single
Markdown file named `YYYY-MM-DD-<topic>.md` and must follow the template.
