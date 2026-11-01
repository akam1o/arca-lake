# RFC-009: Performance Architecture

- Status: draft (Phase 2.1 measurements, §6–§8)
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-004, RFC-005, RFC-006
- Related ADRs: ADR-0006 (gRPC), ADR-0009, ADR-0010

> This document is public. Every number in it was measured on the
> environment described in §5 with the commands in `benchmarks/linux/`; the
> raw results are committed under `benchmarks/linux/results/` after
> `sanitize.sh`. Numbers from other environments are not mixed in.
> Hypotheses are labelled as such.

## 1. Problem

The read path has three levels (durable erasure-coded read, cache hit from
an unprotected copy, cache hit from a replicated or coded copy) and the
write path has one, plus asynchronous cache population. Each level has a
different CPU, network, and latency profile. The operator needs measured
costs, not estimates, to size cache tiers and to choose a cache protection
mode; the project needs them to know which component bounds throughput.

## 2. Goals

- Measured throughput, latency percentiles, CPU-seconds per GiB, and
  network bytes per GiB for: durable only; cache `none`; cache
  `replication`; cache `ec`; cache hits versus misses; capacity-limited
  caches; chunk size; concurrency.
- The cost of the transport (raw TCP, gRPC, TLS) on the same hosts, so
  that application numbers can be compared with the floor.
- A reproducible method (`benchmarks/linux/two-host.sh`, `matrix.sh`,
  `summarize.py`) that records no environment-specific identifiers.

## 3. Non-goals

- Tuning. This RFC records what the Phase 2.1 code does; optimizations
  are proposed in §9 and done later with their own measurements.
- Disk-bound numbers: the test hosts' data sets fit in the page cache.
- Multi-gateway scaling.

## 4. Method

`s3load` (`benchmarks/s3load`) drives a gateway started with
`--allow-anonymous` over HTTP/1.1: one PUT pass over `N` objects of `S`
bytes at concurrency `C`, then `P` GET passes over the same keys with body
verification. Pass 1 of GET is the first touch (cache misses; populations
start); the driver then polls the cache tier's usage until it has not
changed for three seconds before the next pass, so that pass 2 and 3 are
hit passes without concurrent population traffic, and the usage series
gives the population throughput. `sar -u 1` and `sar -n DEV 1` run on
every host during each phase; CPU-seconds are `(100 − %idle) × cores`
summed over 1 s samples, network bytes are `rx + tx` of the host
interface. Gateway cache counters are snapshotted before and after each
pass.

Threats to validity: phases last 2–30 s, so sar's 1 s resolution gives
the per-GiB CPU and network figures a ±1-sample error (up to ~20 % on the
shortest phases); the data set (1.5 GiB) sits in the page cache of every
node; the three hosts are KVM guests whose host and switch are shared;
one gateway, one load generator, one FoundationDB process. Each
configuration was run once.

## 5. Environment

| Item | Value |
| --- | --- |
| Hosts | three identical KVM guests |
| CPU | AMD Ryzen 7 PRO 4750GE (host), 4 vCPUs per guest, no SMT exposed |
| Memory | 7.7 GiB per guest |
| Storage | one virtio disk (QEMU HARDDISK, 80 GiB, ext4) per guest; nodes share it; data in page cache |
| Network | virtio-net, MTU 1500, 2 vCPU-bound ~20 Gbit/s TCP between guests (§6.1) |
| OS / kernel | Ubuntu 26.04.1 LTS, Linux 7.0.0-38-generic |
| Rust | rustc 1.99.1, release profile |
| FoundationDB | 7.3.77, one `fdbserver` (`single ssd`) on host A |
| arca-lake | commit recorded in each results directory's `mode.txt` run; EC `rs-gf8-v1` 2+1; chunk 4 MiB unless stated; small-object replication off |
| Layout | A: FoundationDB, controller, `d0`, `c0`; B: `d1`, `c1`; C: `d2`, `c2`, gateway, `s3load`. Failure domain = host. |

The two-host runs in `results/2host/` (A: FoundationDB, controller, `d0`,
`d1`; B: `d2`, gateway) predate the three-host layout and are kept for
the transport and durable-only figures only.

## 6. Results

### 6.1 Transport floor (two hosts)

Raw TCP, `iperf3 -t 10` (see `results/2host/raw-tcp.md`): 20–22 Gbit/s
per direction; one stream saturates one vCPU on the sender; RTT 0.19 ms.

`transfer-bench` (1 MiB shards, 4 concurrent streams on one connection,
6 s; `results/2host/transfer-two-host.jsonl`):

| Transport | Op | MiB/s | p50 ms | p99 ms |
| --- | --- | --- | --- | --- |
| raw framed TCP | get | 2737 | 1.5 | 2.2 |
| raw framed TCP | put | 3044 | 1.3 | 2.8 |
| raw framed TCP + TLS | get | 2497 | 1.5 | 3.4 |
| raw framed TCP + TLS | put | 2277 | 1.5 | 4.4 |
| gRPC (tonic) | get | 1125 | 3.5 | 6.6 |
| gRPC (tonic) | put | 1045 | 3.7 | 9.6 |
| gRPC + TLS | get | 679 | 5.8 | 10.3 |
| gRPC + TLS | put | 675 | 5.9 | 11.0 |

TLS costs about 10–25 % on raw TCP and about 40 % on gRPC here; gRPC
itself costs about 60 % against raw framing on the same connection count.
These are per-connection, CPU-bound ceilings, not the NIC.

### 6.2 Three-host matrix

See `results/3host/` and the tables in §6.3–§6.6 (generated by
`summarize.py`).

All runs: 96 objects × 16 MiB (1.5 GiB), RS 2+1, 4 MiB chunks unless
stated, one pass each; `p50`/`p99` are per-object latencies; "hits" and
"misses" are gateway chunk counters of the pass (384 chunks per pass);
CPU and network are per GiB transferred by the client, per host (A, B,
C). Full tables: `summarize.py results/3host`.

### 6.3 Durable only

| Concurrency | PUT MiB/s | PUT p50 / p99 ms | GET MiB/s | GET p50 / p99 ms | GET CPU-s/GiB A / B / C | GET net GB/GiB A / B / C |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 56 | 287 / 365 | 349 | 33 / 74 | 1.4 / 1.2 / 3.9 | 0.54 / 0.54 / 1.08 |
| 4 | 264 | 240 / 319 | 561 | 98 / 124 | 1.9 / 1.7 / 5.7 | 0.50 / 0.54 / 1.08 |
| 8 | 211 | 395 / 1170 | 593 | 197 / 242 | 2.6 / 2.4 / 6.7 | 0.42 / 0.54 / 1.08 |
| 4, 1 MiB chunks | 93 | 698 / 815 | 535 | 99 / 234 | 1.5 / 1.6 / 5.6 | 0.46 / 0.54 / 1.08 |

GET reads the two data shards (one from A, one from B; the parity shard
on C is not read): 0.54 GB per GiB on each of A and B and 1.08 GB on C,
as expected. PUT writes one shard per host (0.54 GB on A and B, 1.08 GB
out of C). Concurrency 8 on the 4-vCPU gateway host (which also runs
`s3load`, a durable node and a cache node) raises p99 without raising
throughput. 1 MiB chunks cost PUT 65 % of its throughput (four times the
chunk commits and RPC round trips per object) and leave GET within 5 %.

### 6.4 Cache tiers, unlimited capacity

Pass 1 is the first touch (all misses, populations in flight, bounded to 8
per gateway); pass 3 is the steady state after two passes of population.

| Mode | Conc. | Pass 1 (miss) MiB/s, p50 ms | Pass 3 MiB/s, p50 ms | Pass 3 hits / misses | Pass 3 CPU-s/GiB A / B / C | Pass 3 net GB/GiB A / B / C |
| --- | --- | --- | --- | --- | --- | --- |
| durable only (ref.) | 4 | 561, 98 | 566, 98 | — | 1.8 / 1.7 / 5.8 | 0.49 / 0.54 / 1.08 |
| none | 1 | 255, 49 | 251, 34 | 384 / 0 | 1.2 / 0.5 / 3.4 | 0.29 / 0.37 / 0.70 |
| none | 4 | 416, 134 | 594, 87 | 368 / 16 | 2.7 / 1.4 / 5.9 | 0.39 / 0.37 / 0.78 |
| none | 8 | 495, 239 | 578, 198 | 211 / 173 | 3.1 / 2.5 / 7.2 | 0.43 / 0.50 / 0.98 |
| replication 2 | 1 | 233, 55 | 303, 37 | 384 / 0 | 2.1 / 0.0 / 3.7 | 0.93 / 0.00 / 1.08 |
| replication 2 | 4 | 429, 126 | 380, 149 | 232 / 152 | 5.4 / 3.2 / 9.1 | 1.05 / 0.56 / 1.76 |
| replication 2 | 8 | 413, 277 | 437, 259 | 180 / 204 | 6.5 / 3.5 / 9.0 | 1.01 / 0.51 / 1.52 |
| ec 2+1 | 1 | 285, 38 | 319, 35 | 372 / 12 | 2.3 / 1.3 / 4.5 | 0.52 / 0.56 / 1.11 |
| ec 2+1 | 4 | 396, 143 | 474, 116 | 315 / 69 | 2.8 / 2.4 / 7.6 | 0.49 / 0.64 / 1.27 |
| ec 2+1 | 8 | 425, 280 | 454, 251 | 177 / 207 | 4.6 / 3.4 / 9.0 | 0.67 / 0.67 / 1.33 |
| ec 2+1, 1 MiB chunks | 4 | 508, 108 | 441, 126 | 276 / 1260 | 4.4 / 3.0 / 8.0 | 0.63 / 0.63 / 1.25 |

Range GET of the first 4 MiB of every object (one chunk), concurrency 4:

| Mode | Pass 1 (miss) ops/s, p50 ms | Pass 2 ops/s, p50 ms | Pass 2 hits / misses |
| --- | --- | --- | --- |
| none | 83, 36 | 113, 25 | 74 / 22 |
| replication 2 | 68, 46 | 81, 39 | 60 / 36 |
| ec 2+1 | 73, 42 | 91, 32 | 62 / 34 |

Population: no population failed in any run (`population_failures` 0).
With the bound of 8 in-flight populations a cold pass at concurrency 4
populated about half of its misses (none: 202 of 384 chunks, 808 MiB in
3.7 s ≈ 218 MiB/s while serving 416 MiB/s of reads; ec: 151 chunks; the
rest were served without populating, `populations_skipped`) and the
usage stopped changing within the pass. Replicated and coded populations
write 2× and 1.5× the bytes of `none` and populate fewer chunks per
pass; after three passes the replicated cache held 60 %, the coded cache
82 %, and `none` 96 % of the chunks at concurrency 4.

### 6.5 Capacity-limited cache (800 MiB for 1.5 GiB of data)

Capacity 800 MiB, high watermark 0.9 (720 MiB), low watermark 0.8
(640 MiB), controller eviction every 2 s, four sequential GET passes at
concurrency 4 over 1.5 GiB. `none` stores 1× the data (200 chunks fit),
`ec 2+1` 1.5× (133 chunks fit).

| Mode | Pass | MiB/s | p50 ms | hits / misses | populated | admissions rejected (at capacity) | skipped (no permit) | fallbacks | usage after pass |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| none | 1 | 421 | 133 | 0 / 384 | 200 | 4 | 180 | 0 | 640 MiB |
| none | 2 | 480 | 109 | 154 / 224 | 68 | 63 | 92 | 6 | 640 MiB |
| none | 3 | 462 | 118 | 160 / 224 | 80 | 75 | 67 | 0 | 640 MiB |
| none | 4 | 459 | 117 | 134 / 246 | 74 | 84 | 88 | 4 | 640 MiB |
| ec 2+1 | 1 | 462 | 119 | 0 / 384 | 89 | 0 | 295 | 0 | 534 MiB |
| ec 2+1 | 2 | 441 | 125 | 89 / 295 | 71 | 28 | 196 | 0 | 636 MiB |
| ec 2+1 | 3 | 391 | 143 | 83 / 299 | 54 | 99 | 146 | 2 | 636 MiB |
| ec 2+1 | 4 | 395 | 141 | 106 / 278 | 54 | 95 | 129 | 0 | 636 MiB |

Usage settles at the low watermark after every pass: eviction runs,
admissions continue between the watermarks, and the cache turns over
under the cyclic scan (classic LRU behaviour: 35–42 % hits for `none`
with 52 % of the data fitting, 22–28 % for `ec` with 35 % fitting). The
few `fallbacks` are chunks evicted between the manifest read and the
chunk read, served from the durable tier as designed. Before the
Phase 2.1 fix (admission refused at the high watermark) the same runs
showed a constant 120 cached chunks, zero evictions, and a hit count
equal to the capacity fraction.

### 6.6 HTTP/2 window size (before and after `f96c60c`)

The namespace fault run (`docs/verification/network-faults.md`) led to
sizing the HTTP/2 stream window to 8 MiB (connection window 32 MiB)
instead of tonic's 64 KiB default. The reduced matrix in
`results/3host-w8m/` repeats four configurations with the new windows:

| Configuration | PUT MiB/s before → after | GET pass 1 before → after | GET warm pass before → after |
| --- | --- | --- | --- |
| durable, concurrency 1 | 56 → 85 | 349 → 341 | 329 → 330 |
| durable, concurrency 4 | 264 → 241 | 561 → 583 | 566 → 585 |
| none, concurrency 1 | 54 → 86 | 255 → 254 | 251 → 271 |
| none, concurrency 4 | 180 → 315 | 416 → 433 | 594 → 633 |
| ec, concurrency 1 | 59 → 110 | 285 → 283 | 319 → 337 |
| ec, concurrency 4 | 193 → 242 | 396 → 456 | 474 → 465 |

Single-stream PUT gained 50–90 % (the shard upload stream was window
bound at 0.19 ms RTT: 64 KiB per round trip); GET changed within the
run-to-run noise. The window change did *not* fix the delay-with-jitter
failures of the fault run, which are retransmission stalls (§7).

### 6.7 Memory

Resident set sizes at the end of each run (`ps`, `rss.txt` per run):
storage nodes 40–75 MiB each (1.5–4.5 GiB of shards, index in memory),
controller 32 MiB, gateway 87–142 MiB (highest in the `ec` runs at
concurrency 8), the private `fdbserver` 95–98 MiB. `s3load` had exited
when the snapshot was taken and is not included.

## 7. Interpretation

*Verified on this environment* (one run each; see §4 for the error
bars):

- **The durable read is already parallel and the cache hit is not.** A
  durable GET fetches `k = 2` shards of 2 MiB from two hosts at once; a
  `none` hit fetches one 4 MiB copy from one host; an `ec` hit fetches
  two shards again. At concurrency 1 the durable read (349 MiB/s, p50
  33 ms) beats the `none` hit (251 MiB/s, 34 ms) on throughput and
  equals it on latency; at concurrency 4 the fully warm `none` cache
  (594 MiB/s, 87 ms) is 6 % faster than durable (561 MiB/s, 98 ms). The
  cache's benefit here is CPU and network on the durable nodes (0.29–0.39
  versus 0.49–0.54 GB per GiB on A and B, 1.2–2.7 versus 1.8–1.9
  CPU-s/GiB), not client latency, because no component is disk-bound.
- **Populating costs more than hitting saves, on this environment.** The
  first-touch pass is 25 % slower than durable-only at concurrency 4
  (416 versus 561 MiB/s) and uses 1.5× the CPU on the gateway host; the
  cache pays for itself only after the working set is warm, and under
  this sequential workload the warm state took two to three passes with
  8 in-flight populations.
- **Protection costs scale as RFC-006 §5.9 predicts.** Replication 2
  stores 2× and moves 2× the bytes per population (1.05 GB per GiB on A
  at pass 3 versus 0.39 for `none`); `ec 2+1` stores 1.5× and splits a
  hit over two hosts (1.27 GB per GiB on the gateway host versus 0.78,
  because a hit crosses two links instead of two thirds of one). Hit
  throughput: `none` 594, `ec` 474, replication 380 MiB/s at
  concurrency 4 (the replicated run was still 40 % misses at pass 3).
  Encode and decode CPU is not visible at these rates: the gateway host
  spends 5.9 (`none`) versus 7.6 (`ec`) CPU-s/GiB on hits, within the
  measurement error of the population traffic in the same pass.
- **Chunk size.** 1 MiB chunks cost PUT 65 % of its throughput in both
  the durable-only and the `ec` cache runs and leave GET within 5–10 %;
  the per-chunk metadata commit and RPC round trip dominate small chunks.
- **Concurrency.** Four concurrent 16 MiB requests saturate the 4-vCPU
  gateway host; eight add latency (p50 197 ms versus 98 ms for durable
  GET) without throughput.
- **Transport floor.** The application reaches 20–25 % of the gRPC
  per-connection ceiling of §6.1 on GET and 10–30 % on PUT: the gateway
  host's CPU (HTTP/1.1 body handling, CRC, erasure coding, four
  processes on four vCPUs) is the bound, not the transport.

*Hypotheses* (not measured): on a disk-bound durable tier a `none` hit
from an SSD cache would remove the HDD seeks and dominate these results;
parallel chunk fetch in the gateway would raise single-request
throughput of all modes by up to the chunk count per object; a dedicated
gateway host would move the PUT bound from CPU to the network.

## 8. Verified and not verified

| Claim | Status | Where |
| --- | --- | --- |
| PUT/GET throughput, latency percentiles, CPU-s/GiB and network bytes/GiB for durable-only and the three cache modes at concurrency 1/4/8 on three Linux hosts | measured (one run each) | §6.3–§6.4, `results/3host/` |
| Cache hit versus miss latency (first touch versus warm passes), range GET | measured | §6.4 |
| Population throughput with the per-gateway bound; zero population failures | measured | §6.4 |
| Capacity-limited cache: LRU turnover between watermarks, admission at capacity | measured, after the Phase 2.1 fix | §6.5 |
| Chunk size 1 MiB versus 4 MiB | measured | §6.3, §6.4 |
| Raw TCP, raw framed, gRPC, TLS floors between two hosts | measured | §6.1 |
| Erasure coding encode/reconstruct cost on the test CPU (e.g. `rs-gf8-v1` 4+2, 1 MiB shards: encode 7.1 GiB/s, one-shard reconstruction 9.4 GiB/s of data; a 4 MiB chunk of the 2+1 profile costs well under 1 ms either way) | measured (criterion, `arca-lake-ec`, host B) | `benchmarks/linux/results/ec-bench-linux.md` (raw `.txt` beside it) |
| Memory | snapshot per run | §6.6 |
| Disk-bound behaviour | not measured (page cache) | §4 |
| Variance across runs | not measured (single run per configuration) | §4 |
| Multi-gateway scaling, more than one FoundationDB process | not measured | §3 |
| Any number on other hardware | not claimed | — |

## 9. Proposed optimizations (hypotheses, not measured)

- Parallel chunk fetch in the gateway read path (chunks of one object are
  currently read one after the other; a hit pass is bounded by one RPC
  per chunk in sequence).
- Population write batching and a per-node population queue (the permit
  count is a global bound today).
- Raw framed transport for shard transfer (ADR-0006 chose gRPC; §6.1
  shows the gap on this environment).
- Sharded cache usage counters (R-42).
- A progress-based (per frame) deadline for shard streams instead of the
  per-RPC timeout, so that impaired links (10 % loss, reordering) produce
  slow transfers rather than timeouts (fault run F1/F2, R-47).

## 10. Open questions

- Which workloads justify replication or EC cache protection given the
  measured population cost (§7).
- Whether hit latency improves enough with parallel chunk fetch to
  justify the cache on networks where the durable read is already
  parallel.
