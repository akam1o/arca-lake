# Phase 3.5 results

Raw results of the Phase 3.5 reliability closure. Three Linux test hosts:
"host A" (FoundationDB suites and shutdown A/B, isolated 3-process cluster),
"host B" (storage-node memory), "the soak host" (GC capacity, 24 h soak).
Every cluster ran under its own directory and ports; nothing else on the
hosts was used.

## phase35-memory (R-70, host B)

`crates/storage-node/tests/memory_churn.rs`, release build, one storage node
in-process, 16 clients, shards of 64 KiB to 1 MiB, compaction every second
unless noted; one JSON line per minute (anonymous RSS, glibc heap in use and
free, index entry counts, CPU seconds, throughput, put/get p95/p99).

| File | Build | Mode | Length |
| --- | --- | --- | --- |
| `unique-old.jsonl` | 79f0829 with the pre-3.5 index (tombstone history kept for the process's life) | unique keys, each deleted 64 operations later | 90 min |
| `unique-new.jsonl` | 79f0829 | same | 90 min |
| `unique-new-arena2.jsonl` | 79f0829, `MALLOC_ARENA_MAX=2` | same | 60 min |
| `unique-new-nocompaction.jsonl` | 79f0829, compaction off | same | 6 min: stopped when the disk filled (not a measurement) |
| `put-new`, `get-new`, `churn-new`, `idle-new` | 79f0829 | as named | 20 min each |
| `unique-final.jsonl` | 5613392 (final storage code: runtime history kept only for in-flight records) | unique keys | 90 min |

## phase35-shutdown (R-68, host A)

`soak24.sh _shutdown_test`, soak cluster layout, 40 rounds of a gateway and
a controller signalled (TERM/INT alternately) under PUT/DELETE load, every
fifth round right after a FoundationDB process restart. `noexit.txt`: the
binary before the `_exit` change; `fixed.txt`: after (8218493). One line per
shutdown: round, process, signal, exit status, seconds to exit.

## phase35-fdb (R-71, R-72, host A)

`~/arca-p35-fdb-validate.sh` equivalent: the seven FoundationDB suites once
(`suites.txt`), then `fdb_failover` N times on a fresh 3-process cluster
every third run (`failover.txt`: result, requests that failed explicitly,
abort retries, SIGSEGV count, unknown outcomes). Directory names carry the
build. `fdb_restore` and `backup_verify` report 0.00 s: they skip without
their own environment variables.

## phase35-gc-capacity (R-64, the soak host)

`benchmarks/linux/gc-capacity.sh` on the soak cluster layout without faults:
`gcload` steps the operation rate up (one JSON line per step), the leader's
GC gauges and node space are sampled every 15 s (`samples.jsonl`), and the
drain is timed after each mode (`drain.txt`).

| Directory | Build | Modes | Notes |
| --- | --- | --- | --- |
| `run1-put-only-overwrite` | 46b36f5 (GC: 30 s budget, 8 at once) | put-only, overwrite | put-only invalid (gcload wrote the same keys again at every step, fixed in f44ee1b); stopped when put-only's data filled the disk |
| `run2-modes` | 764fcf3 (GC: budget at least the interval, 32 at once) | overwrite, put-delete, versioned, multipart | 256 KiB objects; multipart invalid (256 KiB parts are below the cluster's 1 MiB minimum: every Complete failed and gcload left the uploads open, fixed in 238b883) |
| `run3-multipart` | 238b883 | multipart, 2 × 1 MiB parts | stopped at the 80 ops/s step with the disk 97 % full: GC collected about 14 generations/s against about 120 retired/s; `gc-passes.log` holds the leader's pass reports |

## phase35-soak (first 24 h attempt, the soak host, 9abee45)

`soak24.sh` with the Phase 3.4 configuration (8 workers, a fault every
900 s), everything on the VM's single disk. The disk was 95–98 % busy for
the whole run (`host.jsonl`, `io_ms`). For 13 h: no violation, every
two-hourly full verification of the acknowledged-write ledger clean. At
05:18 Z all three FoundationDB processes stopped with `io_timeout` (1521,
`fdb-trace/`); the harness has no supervisor to restart them
(`operator.log`: restarted by hand at 05:54 Z; the cluster recovered without
restarting a gateway or controller, a seventh full verification completed
clean); at 06:02 Z two stopped again with the disk saturated, and the run
was stopped. Not reached: the end of the workload, convergence and the
integrity snapshots. A 2 h A/B against the Phase 3.4 build
(a8dc96b, which had run 25 h on this host in Phase 3.4) stopped the same
way 1.5 min into its load (`ab-phase34-build/`), so Phase 3.5's changes are
not the cause. A retry with FoundationDB on a second virtual disk
(`FDB_DIR`, `retry-fdb-on-sdb/`) stopped the same way 80 s in: the TLog's
persistent-state commit exceeded `TLOG_MAX_CREATE_DURATION` (10 s,
`IoTimeoutError` context `TLogCommit`), with the second disk 63 % busy but
an average queue depth of 10.8. Idle fsync latency was normal (p50 about
1 ms) and the second disk's fsync rose to p99 33 ms under a sequential
660 MB/s write on the first: the two virtual disks share backing storage,
and since about 05:18 Z concurrent synchronous writes stalled for more than
10 s. The long soak was ended there: 13 h without violations, then an
environment failure; convergence and the integrity snapshots were not
reached.
