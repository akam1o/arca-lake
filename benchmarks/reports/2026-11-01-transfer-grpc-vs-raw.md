# Benchmark report: shard transfer, gRPC vs raw framed TCP, with and without TLS (loopback, developer laptop)

- Date: 2026-11-01
- arca-lake commit: `c9c25a5` plus the `benchmarks/transfer` tool committed with this report
- Author: akam1o
- Purpose: inform OD-08 (shard transfer protocol) for Phase 0 task 0.4.

> **Scope warning.** Loopback on one laptop: no NIC, no network latency,
> client and server share the CPU. Absolute throughput is memory-copy
> bound and meaningless for a real network. The *relative* costs (CPU per
> byte, latency, memory, behaviour under backpressure) are what this report
> is about.

## Environment

| Item | Value |
| --- | --- |
| Hardware model | Apple laptop, model identifier `Mac17,3` |
| CPU | Apple M5, 10 cores (4 performance + 6 efficiency) |
| Memory | 24 GiB |
| Network | loopback (127.0.0.1) |
| OS and kernel | macOS 26.5.1, Darwin 25.5.0 |
| Rust toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13), release profile |
| tonic / prost | 0.14.9 / 0.14.4 (HTTP/2, 8 MiB stream window, 32 MiB connection window) |
| rustls / tokio-rustls | 0.23 / 0.26, `ring` provider, TLS 1.3, self-signed certificate |
| Tool | `benchmarks/transfer`, 4 Tokio worker threads per process |

## Method

```sh
benchmarks/transfer/run.sh            # main matrix, 5 s per cell
SERVER_DELAY_US=200 CONCURRENCY=8 DURATION=4 benchmarks/transfer/run.sh   # backpressure
# plus: gRPC with --concurrency 8 --connections 8 (8 HTTP/2 connections)
```

- Shard 1 MiB, frames 64 KiB. `put`: client-streaming upload, server
  computes CRC-32C and acknowledges. `get`: server-streaming download.
- Concurrency 1 and 8. For gRPC, concurrency 8 was run both as 8 streams on
  one HTTP/2 connection (`conn=1`) and as 8 connections (`conn=8`).
- CPU is `getrusage` user+system over wall time, per process; RSS is
  `ru_maxrss`.
- Backpressure case: the server sleeps 200 µs per 64 KiB frame (timer
  granularity on this platform makes the effective delay about 1 ms), so
  the client must be throttled by flow control.

## Results

### Main matrix (loopback, 5 s per cell)

| transport | tls | op | conc | MiB/s | p50 ms | p99 ms | client CPU % | server CPU % | client RSS MiB | server RSS MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| grpc | false | put | 1 | 3185 | 0.29 | 0.64 | 125 | 49 | 8 | 7 |
| grpc | false | put | 8 (conn=1) | 2773 | 2.76 | 5.33 | 121 | 109 | 10 | 10 |
| grpc | false | put | 8 (conn=8) | 5857 | 1.34 | 2.12 | 338 | 169 | 15 | 14 |
| grpc | false | get | 1 | 3246 | 0.29 | 0.61 | 87 | 64 | 7 | 7 |
| grpc | false | get | 8 (conn=1) | 2631 | 3.04 | 4.36 | 208 | 72 | 10 | 22 |
| grpc | false | get | 8 (conn=8) | 6019 | 1.34 | 1.98 | 276 | 177 | 16 | 11 |
| grpc | true | put | 1 | 2177 | 0.44 | 0.90 | 110 | 52 | 9 | 8 |
| grpc | true | put | 8 (conn=1) | 1970 | 3.93 | 7.48 | 124 | 110 | 12 | 11 |
| grpc | true | put | 8 (conn=8) | 4212 | 1.80 | 3.45 | 355 | 181 | 17 | 14 |
| grpc | true | get | 1 | 2269 | 0.43 | 0.59 | 95 | 62 | 8 | 8 |
| grpc | true | get | 8 (conn=1) | 2184 | 3.57 | 6.12 | 209 | 77 | 11 | 17 |
| grpc | true | get | 8 (conn=8) | 4984 | 1.56 | 2.70 | 355 | 209 | 16 | 11 |
| raw | false | put | 1 | 7536 | 0.13 | 0.27 | 79 | 48 | 4 | 4 |
| raw | false | put | 8 | 9925 | 0.79 | 1.32 | 285 | 129 | 5 | 4 |
| raw | false | get | 1 | 8279 | 0.11 | 0.27 | 83 | 48 | 4 | 4 |
| raw | false | get | 8 | 10513 | 0.74 | 1.13 | 178 | 127 | 6 | 4 |
| raw | true | put | 1 | 2155 | 0.35 | 1.91 | 65 | 42 | 5 | 5 |
| raw | true | put | 8 | 3874 | 1.59 | 13.24 | 173 | 117 | 7 | 6 |
| raw | true | get | 1 | 2218 | 0.37 | 1.77 | 79 | 43 | 5 | 5 |
| raw | true | get | 8 | 5880 | 1.17 | 5.73 | 292 | 167 | 7 | 6 |

Derived CPU cost (client + server CPU seconds per GiB transferred) for the
8-way cases:

| case | gRPC (conn=8) | raw |
| --- | ---: | ---: |
| plaintext put | 0.89 | 0.43 |
| plaintext get | 0.77 | 0.30 |
| TLS put | 1.30 | 0.77 |
| TLS get | 1.16 | 0.80 |

### Backpressure (server throttled, concurrency 8, 4 s per cell)

| transport | tls | op | MiB/s | p50 ms | p99 ms | client CPU % | server CPU % | client RSS MiB | server RSS MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| grpc | false | put | 308 | 24.5 | 58.2 | 24 | 13 | 9 | 11 |
| grpc | false | get | 363 | 22.0 | 26.9 | 68 | 25 | 7 | 7 |
| grpc | true | put | 326 | 24.5 | 29.1 | 37 | 20 | 10 | 12 |
| grpc | true | get | 358 | 22.1 | 28.1 | 86 | 32 | 9 | 9 |
| raw | false | put | 353 | 22.5 | 26.9 | 20 | 14 | 4 | 4 |
| raw | false | get | 369 | 21.7 | 23.7 | 19 | 11 | 4 | 4 |
| raw | true | put | 359 | 22.2 | 26.8 | 16 | 13 | 6 | 6 |
| raw | true | get | 357 | 22.1 | 26.4 | 43 | 19 | 6 | 6 |

Raw data: `data/2026-11-01-transfer-loopback-m-series-laptop.jsonl`,
`data/2026-11-01-transfer-grpc-8conns-m-series-laptop.jsonl`,
`data/2026-11-01-transfer-backpressure-m-series-laptop.jsonl`.

## Interpretation

*Verified on this machine:*

1. **TLS is the dominant cost for both transports.** Single-stream
   throughput drops to about 2.2 GiB/s for both once TLS is on, from 3.2
   (gRPC) and 7.5–8.3 (raw) GiB/s in plaintext. Under TLS the two
   transports are within about 20% of each other in throughput at 8-way
   concurrency when gRPC is given 8 connections (4.2–5.0 vs 3.9–5.9 GiB/s).
2. **gRPC needs connection fan-out.** Eight streams on one HTTP/2
   connection pinned the server at roughly one core and halved throughput
   compared with eight connections. This is a deployment rule, not a
   protocol limitation: a gateway should keep several connections per
   storage node.
3. **CPU per byte favours raw framing**: about 2× less CPU in plaintext and
   about 1.5–1.7× less under TLS (combined client and server). Memory is
   lower too (4–7 MiB vs 7–22 MiB RSS) because HTTP/2 windows buffer data.
4. **Backpressure works for both.** With a throttled server, every variant
   converges on the same bound, client memory stays flat, and CPU drops in
   proportion; neither transport buffers without limit. gRPC's plaintext
   `put` showed one long tail (p99 58 ms) in this run.
5. **A hand-written protocol is easy to get subtly wrong.** The first raw
   implementation hung randomly under TLS at concurrency ≥ 4 because it did
   not `flush()` at message boundaries; tokio-rustls buffers encrypted
   bytes until flushed, and plaintext TCP hid the bug entirely. gRPC/hyper
   handle this internally. (Fixed in the tool; recorded as R-29.)
6. **Zero copy** is not available to either path as built: both copy
   received frames into buffers, and TLS plus CRC computation require the
   bytes in user space anyway. prost's `Bytes` fields avoid one copy on
   decode; HTTP/2 framing adds one on the gRPC side. Kernel-level zero copy
   (`sendfile`/`splice`) would only apply to plaintext reads from the
   storage engine, which is out of scope while TLS is the default.

*Hypotheses (not verified here):*

- On a real network the per-byte CPU difference matters only once a NIC is
  saturated or gateway CPU is the bottleneck; with TLS on, both transports
  can drive tens of Gbit/s per few cores on server hardware.
- Latency tails on a real network are dominated by the network and the
  storage engine's sync, not by the transport.

## Recommendation for OD-08

Use **gRPC (tonic) for all internal RPC including shard transfer** in
Phase 1, with multiple HTTP/2 connections per storage node, TLS enabled,
and the streaming `PutShard`/`GetShard` service already defined in
`arca-lake-protocol`. Reasons: under TLS the throughput difference is
within the noise of this environment; gRPC provides typed, versioned,
flow-controlled streaming with far less protocol code to maintain and test;
the raw path already produced one transport bug that only appeared under
TLS. The measured CPU-per-byte advantage of raw framing (roughly 1.5× under
TLS) is recorded as the price, to be revisited in Phase 4 (RFC-009) only if
gateway or storage-node CPU proves to be the bottleneck on server hardware.
The framed protocol in the benchmark tool is kept as a reference
implementation for that comparison.

## Threats to validity

- Loopback: client and server compete for the same cores and memory
  bandwidth; the absolute numbers are not transferable.
- Single run per cell, 4–5 s each; p99 values are noisy (see raw TLS put
  13 ms and gRPC plaintext put 58 ms outliers).
- Timer granularity makes the backpressure delay larger than configured.
- The gRPC server used tonic defaults apart from larger windows; no
  tuning of either side beyond what is listed.
