# Phase 2.2: Performance and resilience optimization

Status: **in progress**. Phase 2.2 acts on the Phase 2.1 measurements:
progress-based deadlines and retries for shard streams (R-47), the cache
hit-ratio analysis, bounded parallel chunk fetch, the gRPC transport
analysis, repeated measurements, and disk-bound runs. Design records:
ADR-0011; numbers: RFC-009 §6–§8.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 2.2.1 R-47: shard stream deadlines | implemented, measured below | `RpcPolicy`: progress timeout (stalled stream) + absolute deadline (`base + bytes / min_rate`) + bounded retries with backoff for transport errors; storage node aborts stalled client streams; RPC counters exported; CLI `--node-*`; `transport.rs` (8 cases) |
| 2.2.2 Cache hit-ratio analysis | counters implemented, measured below | lookup hits vs served hits vs degraded vs fallbacks (by cause); admission rejections by cause; population failures by cause; populations skipped (no permit) |
| 2.2.3 Parallel chunk fetch | implemented, measured below | `read_parallelism` (CLI default 4, tests 3): ordered delivery, bounded buffering, abort on dropped body, error after prefetched chunks; `parallel_read.rs` (3 cases) |
| 2.2.4 gRPC transport analysis | tooling ready, measured below | `transport-matrix.sh`: transports × TLS × connections × streams × frame size with client and server CPU |
| 2.2.5 Reproducible measurements | tooling ready, measured below | `repeat.sh` (N repetitions, shuffled order, warm-up) + `aggregate.py` (median / min / max / sd, percentiles, CPU-s and net per GiB, hit ratio, RSS, errors) |
| 2.2.6 Disk-bound runs | tooling ready, measured below | `diskbound.sh`: data set larger than the page cache, sequential and random reads, small objects, concurrent PUT/GET; no cache drops, no direct I/O |
| 2.2.7 Cache protection comparison | measured below | repeated runs of none / replication 2 / ec 2+1 with the new counters |
| 2.2.8 Correctness under the optimizations | tests | every suite runs with parallel reads; retry × population × overwrite cases in `transport.rs` / `parallel_read.rs` |

## Results

<!-- PHASE-2-2-RESULTS -->

## Not verified

<!-- PHASE-2-2-NOT-VERIFIED -->
