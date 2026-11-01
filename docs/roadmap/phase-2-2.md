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

Numbers are in RFC-009; the summaries here are the decisions they support.

- **R-47 (§6.8)**: the shipped policy (progress timeout 10 s, deadline
  10 s + bytes / 256 KiB/s, 3 attempts, lease renewal while the upload
  progresses) takes PUT success at 10 % loss from 4/12 to 12/12, a node
  stopped for 20 s from 46/48 to 48/48, and 50 ms ± 10 ms jitter from
  9/12 to 12/12, with 0 mismatches in every verify pass. A 5 s progress
  timeout was measured and rejected (TCP's retransmission backoff at 10 %
  loss exceeds it); a deadline without a rate term was measured and
  rejected (fails every transfer on a slow link).
- **Hit-ratio analysis (§6.9)**: every lookup hit was a served hit on a
  healthy cluster (no degraded hits, no fallbacks of any cause). The hit
  ratio differences between protection modes are the populated fraction,
  which is population bandwidth under the in-flight bound (1× / 1.5× / 2×
  bytes for none / ec 2+1 / replication 2). Raising the bound to 32
  exposed the R-42 hot key (9–45 failed populations per pass), now fixed
  with an atomic usage counter and a snapshot capacity check.
- **Parallel chunk fetch (§6.10)**: at concurrency 1 the warm durable GET
  gains 21 % (p95 halves) and the 100 %-hit GET 87 %; at concurrency 4
  the gateway host is CPU-bound either way. Gateway RSS grows by about
  one chunk.

- **gRPC versus raw TCP (§6.11)**: the first matrix pass found a
  40 ms Nagle/delayed-ACK stall on the storage node's accepted sockets
  (tonic ignores `tcp_nodelay` for a caller-supplied listener), fixed.
  Against the corrected stack gRPC reaches 1.0–1.2 GB/s per stream and
  raw framed TCP 2.3–2.6 GB/s at 2–3× less CPU per GiB; a hybrid data
  path would save at most 20–30 % of the gateway host's CPU per GiB and
  is deferred with that as its entry condition.
- **Repeated comparison (§6.12)**: GET is repeatable to 1–13 %, so the
  pass-3 ordering durable < replication 2 < ec 2+1 < none is real; PUT
  varies up to 2.7× between repetitions (R-48). A cache hit costs the
  gateway half the CPU and 40 % of the network of a durable read;
  populating costs 60–70 % more CPU than a plain miss.

<!-- PHASE-2-2-RESULTS-3 -->

## Not verified

<!-- PHASE-2-2-NOT-VERIFIED -->
