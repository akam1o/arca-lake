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

### 6.8 Phase 2.2: deadlines and retries under impairment (R-47)

Method: the namespace pair of `docs/verification/network-faults.md`,
`ec 2+1` cache, 8 MiB objects, 4 MiB chunks, PUT concurrency 4 (2 for
the partition and node scenarios), run twice with the same code against
two gateway policies: *old* (one attempt, a single 10 s bound equivalent
to the former per-RPC timeout: `--node-max-attempts 1
--node-progress-timeout-secs 10 --node-min-rate-mib 0`) and *new*
(defaults: progress timeout 5 s, absolute deadline 10 s + 1 MiB/s, three
attempts with 100 ms exponential backoff). Conditions: loss 0 / 2 / 5 /
10 %, delay 50 ms with and without ± 10 ms jitter, socket resets, a 10 s
partition, a node rate-limited to 8 Mbit/s, a node stopped with
`SIGSTOP` for 20 s. Recorded per scenario: PUT success count, throughput,
p50 / p95 / p99, verify pass (missing / mismatches), and the gateway's
RPC counters (stalled, deadline exceeded, retries, exhausted).

Policies compared (all with the 8 MiB HTTP/2 windows; `old` reproduces
the former behaviour exactly: one attempt, one 10 s bound):

| Policy | Attempts | Progress timeout | Absolute deadline |
| --- | --- | --- | --- |
| old | 1 | 10 s (equivalent to the single bound) | 10 s |
| new | 3 | 5 s | 10 s + bytes / 1 MiB/s |
| newb | 3 | 10 s | 10 s |
| newc | 3 | 10 s | 10 s + bytes / 256 KiB/s |
| final | 3 | 10 s | 10 s + bytes / 256 KiB/s, **plus lease renewal while the upload progresses** (the shipped defaults, ADR-0011) |

PUT success and the gateway's RPC counters per scenario
(`summarize_faults.py results/r47/<policy>`; 12 objects × 8 MiB at
concurrency 4 unless stated; "verify" = correct / missing / mismatched
after the fault was removed):

| Scenario | old | new | newb | newc | final |
| --- | --- | --- | --- | --- | --- |
| loss 0 % | 12/12, 134 MiB/s | 12/12, 244 MiB/s | 12/12, 162 MiB/s | 12/12, 100 MiB/s | 12/12, 158 MiB/s |
| loss 2 % | 12/12, 69 MiB/s, p99 0.7 s | 12/12, 47 MiB/s, p99 0.8 s | 12/12, 72 MiB/s | 12/12, 45 MiB/s | 12/12, 68 MiB/s, p99 0.6 s |
| loss 5 % | 12/12, 20 MiB/s, p99 2.2 s | 12/12, 14 MiB/s, p99 3.1 s | 12/12, 9 MiB/s | 12/12, 10 MiB/s, p99 4.9 s | 12/12, 10 MiB/s, p99 5.2 s |
| loss 10 % | **4/12**, 41 deadline timeouts | **7/12**, 106 stalls, 102 retries, 5 exhausted | **4/12**, 95 deadline timeouts, 12 exhausted | **5/12**, 26 stalls, 27 retries, **0 exhausted**; the 7 failures were lease expiry and metadata timeouts (see below) | **12/12**, p50 27 s, p99 43 s, 61 stalls, 56 retries, 5 exhausted (all cache populations, abandoned) |
| delay 50 ms, no jitter | 12/12, 5.7 MiB/s, p95 12 s | 12/12, 5.5 MiB/s, 8 stalls retried | 12/12, 9.8 MiB/s, 7 deadline retries | 12/12, 5.1 MiB/s, no timeouts | 12/12, 5.1 MiB/s, no timeouts |
| delay 50 ms ± 10 ms jitter | 9/12 (3 deadline timeouts) | 12/12, 4 stalls retried | 12/12 | 12/12, no timeouts | 12/12, 4.7 MiB/s, no timeouts |
| socket resets (96 objects) | 96/96 | 96/96, 1 retry | 96/96, 5 retries | 96/96, 8 retries | 96/96, 5 retries |
| 10 s partition (160 objects, concurrency 2) | 157/160 | 158/160 | 158/160 | 157/160 | 159/160 (one metadata transaction timeout) |
| node rate-limited to 8 Mbit/s both ways (4 objects) | 4/4, 1.8 MiB/s, p50 8.8 s, no timeouts | same | same | same | same |
| node SIGSTOPped 20 s (48 objects, concurrency 2) | 46/48 (2 deadline) | 45/48 (6 stalls, 2 exhausted) | 46/48 | 46/48 (2 stalls, 2 retries) | **48/48** (2 stalls, 2 retries; p99 20 s) |

Every verify pass in every run: 0 mismatches; every failed PUT left no
object (atomic visibility and durable acknowledgment unchanged).

What the counters show:

- **The per-RPC timeout was the failure mode on lossy and jittery links**
  (`old`: 41 deadline timeouts at 10 % loss, 3 under jitter), but a
  *short* progress timeout is not the fix: at 10 % loss TCP's
  retransmission backoff produces gaps longer than 5 s, so `new`
  declared 106 stalls and restarted transfers that would have finished;
  it still did better (7/12) because the retries eventually got through.
- **A deadline without a rate term is the same cliff moved** (`newb`: 95
  deadline timeouts, 12 exhausted at 10 % loss).
- **Progress 10 s with a 256 KiB/s rate term removes shard-stream
  failures** (`newc`: 0 exhausted at 10 % loss, no timeouts at all under
  50 ms delay with or without jitter). The PUTs that still failed at
  10 % loss failed in *metadata*: "generation lease expired", "no such
  generation" (the pending generation had been expired and collected
  while the shards were still uploading), and two FoundationDB
  transaction timeouts across the lossy link. An upload slower than the
  pending-generation lease (20 s in this harness) lost its generation.
  That is the `final` change: the gateway renews the lease while chunks
  keep arriving; a stalled upload still expires.
- **Slow but progressing streams never time out** in any policy (8 Mbit/s
  node: 8.8 s per object, 0 timeouts), which was the design goal.
- **A stopped node is detected by the progress timeout**, and retrying a
  node that is still stopped burns the attempts: `new` lost one more PUT
  than `old` on the 20 s stop; `newc` matched `old`. Retries help for
  transient faults (resets: 1–8 retries, 0 failures), not for a node
  that stays down; the controller's health pass is the right tool for
  that.
- Throughput under loss varies ±40 % between runs of the same policy
  (single runs; netem's random loss), so the MiB/s columns rank
  policies only roughly; the success and counter columns are the
  signal.

Harness corrections made during this phase and now in
`netns-faults.sh`: netem's default 1000-packet queue overflowed under
plain 50 ms delay with 8 MiB windows and dropped packets of its own
(`limit` raised); the slow-node case shaped only the node's egress
(now both directions).

**The final policy** (`results/r47/final`): 12/12 at 10 % loss, 48/48
with a node stopped for 20 s, 159/160 across a 10 s partition, 96/96
under resets, 12/12 under both delay profiles, 4/4 on the 8 Mbit/s node;
0 mismatches in every verify pass. Two renewal designs were measured on
the way: renewing once per chunk (between chunk uploads) still lost 5 of
12 PUTs at 10 % loss, because one 4 MiB chunk's shard uploads with
retries outlast the 20 s lease (p50 of a whole 8 MiB PUT at 10 % loss is
27 s); renewing every third of the lease *while* the client's bytes
arrive and *while* a chunk's shards are being uploaded lost none. The
5 exhausted RPCs in that cell were cache-tier populations
(`population_failures_node`), abandoned and retried on a later miss, not
durable writes. The one failure across the partition was a FoundationDB
transaction timeout at the gateway, outside the shard path. The 20 s p99
in the stopped-node case is the progress timeout plus the retry that
found the node still stopped.

### 6.9 Phase 2.2: cache hit-ratio analysis

Method: two hosts (A: FoundationDB, controller, `d0`, `d1`, `c0`, `c1`;
B: `d2`, `c2`, gateway), none / replication 2 / ec 2+1, 96 × 16 MiB,
concurrency 4, three GET passes with populations allowed to settle
between passes, with 8 and with 32 in-flight populations per gateway;
the counters of RFC-006 §5.3 "Hit accounting" before and after each pass.

Counters per pass (384 chunks per pass; `summarize_hits.py
results/hit-analysis`), 8 in-flight populations:

| Mode | Pass | Lookup hits | Served hits | Degraded | Fallbacks (any cause) | Misses | Populated | Skipped (no permit) | Population failures | Readable hit % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| none | 1 / 2 / 3 | 0 / 145 / 205 | 0 / 145 / 205 | 0 | 0 | 384 / 239 / 179 | 145 / 60 / 122 | 239 / 179 / 57 | 0 | 0 / 38 / 53 |
| replication 2 | 1 / 2 / 3 | 0 / 134 / 197 | 0 / 134 / 197 | 0 | 0 | 384 / 250 / 187 | 134 / 63 / 84 | 250 / 187 / 103 | 0 | 0 / 35 / 51 |
| ec 2+1 | 1 / 2 / 3 | 0 / 144 / 196 | 0 / 144 / 196 | 0 | 0 | 384 / 240 / 188 | 144 / 52 / 104 | 240 / 188 / 84 | 0 | 0 / 38 / 51 |

With 32 in-flight populations:

| Mode | Pass 3 hits | Pass 3 populated | Pass 3 skipped | Population failures (metadata), passes 1 / 2 / 3 | Readable hit % |
| --- | --- | --- | --- | --- | --- |
| none | 261 | 114 | 0 | 35 / 45 / 9 | 68 |
| replication 2 | 234 | 128 | 15 | 16 / 21 / 7 | 61 |
| ec 2+1 | 223 | 67 | 82 | 29 / 29 / 12 | 58 |

What the counters show:

- **Every lookup hit was a served hit.** Lookup hits equal served hits
  in all 36 passes; degraded hits and fallbacks of every cause are zero;
  no admission was refused (unlimited capacity, no size cap, threshold
  1); no copy was found stale, corrupt or missing. Protection mode does
  not affect the *readability* of what is cached on a healthy cluster.
- **The hit ratio is the populated fraction.** Misses in pass *n* are
  exactly the chunks not yet populated; the only reason a miss did not
  populate was the in-flight bound (`skipped`). Population throughput is
  what differs: a population writes 1× (`none`), 1.5× (`ec 2+1`) or 2×
  (`replication 2`) the chunk bytes to one, three or two members, so
  with the same bound fewer coded or replicated populations complete per
  pass. In this two-host layout the gap is small (145 / 134 / 144
  chunks in pass 1); in the Phase 2.1 three-host run, where the
  replicated members sat on two different hosts, it was 202 / 87 / 151,
  which produced the 94 % / 60 % / 48–82 % figures.
- **Raising the bound trades failures for coverage.** 32 in-flight
  populations populate more per pass (none: 261 cached after pass 3
  versus 205) but 9–45 populations per pass fail in the metadata
  transaction: every insert adds to the one usage key of the tier, and
  32 concurrent inserts conflict (the R-42 hot key, now observed). Each
  such failure deletes its copy and is retried on a later miss; no
  incorrect state results, but the work is wasted. The fix is in §6.14.
- A fair comparison of protection modes therefore holds *populated
  bytes* constant, not pass count: at equal populated bytes the modes
  serve identical hit ratios; `replication 2` needs 2× and `ec 2+1`
  1.5× the population bandwidth of `none` to reach the same coverage,
  which is the capacity-efficiency trade RFC-006 §5.9 states.

### 6.10 Phase 2.2: parallel chunk fetch

Method: same layout as §6.9, durable only and cache `none`, concurrency
1 and 4, `--read-parallelism 1` versus `4`, three repetitions each in
shuffled order (`repeat.sh`).

Three repetitions each, shuffled order, warm-up pass per setup, taken
after the storage node's no-delay fix (§6.11; `results/parallel-p1`,
`results/parallel-p4`; medians, min–max in the raw tables):

| Configuration | Phase | `read_parallelism 1` MiB/s (p50 / p95 ms) | `read_parallelism 4` MiB/s (p50 / p95 ms) | Change |
| --- | --- | --- | --- | --- |
| durable, concurrency 1 | GET warm (pass 3) | 377 (32 / 34) | 420 (29 / 32) | +12 % |
| durable, concurrency 4 | GET warm | 606 (92 / 107) | 585 (96 / 121) | within noise (CPU-bound) |
| none, concurrency 1 | GET pass 1 (miss + populate) | 294 (44 / 48) | 327 (38 / 42) | +11 % |
| none, concurrency 1 | GET pass 3 (100 % hits) | 405 (30 / 32) | 474 (25 / 29) | **+17 %, p50 −16 %** |
| none, concurrency 4 | GET pass 3 (97–100 % hits) | 701 (78 / 93) | 668 (80 / 107) | within noise |

Largest `arca-lake` process RSS on either host: 93–109 MiB with
parallelism 1, 95–133 MiB with 4. PUT is unchanged by design; its
run-to-run spread (sd 10–74 MiB/s) is the dominant noise in these tables.

The same comparison taken *before* the no-delay fix
(`results/parallel-p1-nagle`, `results/parallel-p4-nagle`) showed +87 %
for the all-hit single request (229 → 428 MiB/s) and +21 % for the
durable one (346 → 417): a serial reader paid the 40 ms stall of §6.11
in full on every affected RPC, while four reads in flight overlapped it.
Most of that gain belonged to the stall, not to parallel fetch; the
numbers above are the gain parallel fetch has on its own.

With both in place the cache hit at concurrency 1 beats the durable read
(474 versus 420 MiB/s, p50 25 versus 29 ms), reversing the Phase 2.1
finding: the hit's single 4 MiB RPC per chunk gains the most from
reading the next chunks ahead, the durable read (two parallel 2 MiB
shard RPCs per chunk) gains less, and at concurrency 4 the gateway host
is CPU-bound either way.

### 6.11 Phase 2.2: gRPC transport matrix

Method: `transport-matrix.sh` between two hosts, 1 MiB shards, 6 s per
cell: gRPC and raw framed TCP, with and without TLS, 1 / 2 / 4
connections, 1 / 4 / 8 streams, 64 KiB / 256 KiB / 1 MiB frames, GET and
PUT; client and server CPU seconds from the bench's own accounting.

Two corrections came out of the first pass of this matrix and apply to
the storage node as much as to the bench: the bench's gRPC server had
been left on tonic's default 64 KiB stream window while the client used
the storage node's 8 MiB / 32 MiB windows (PUT cells were measuring a
stack the gateway does not use), and tonic does not apply `tcp_nodelay`
to a caller-supplied listener, so every accepted socket of the storage
node and of the bench server kept Nagle's algorithm. Nagle plus delayed
ACK stalls a small frame that follows unacknowledged data (gRPC
trailers, HTTP/2 window updates) for about 40 ms; the first pass showed
it as a p99 of 42 ms in every gRPC cell against under 1 ms for raw TCP,
whose accepted sockets the bench had always set to no-delay. The
matrix below was taken with both corrections.

Before/after the no-delay fix, bench server and client in the two
namespaces of the fault harness on one host (veth pair, round trip in
the tens of microseconds, so every delayed ACK shows; 1 MiB shards and
frames, one connection, 6 s per cell; `results/nagle`):

| Transport | Op | Streams | Server sockets | MiB/s | p50 ms | p99 ms |
| --- | --- | --- | --- | --- | --- | --- |
| gRPC | GET | 1 | Nagle (before) | 29 | 41.9 | 43.3 |
| gRPC | GET | 1 | no-delay (after) | 622 | 1.7 | 2.8 |
| gRPC | GET | 4 | Nagle | 750 | 2.8 | 43.8 |
| gRPC | GET | 4 | no-delay | 1250 | 2.8 | 8.1 |
| gRPC | PUT | 1 | Nagle | 947 | 0.8 | 1.4 |
| gRPC | PUT | 1 | no-delay | 1258 | 0.8 | 1.0 |
| gRPC | PUT | 4 | Nagle | 1316 | 2.8 | 7.4 |
| gRPC | PUT | 4 | no-delay | 1437 | 2.7 | 4.6 |
| raw TCP | GET | 1 | no-delay | 3474 | 0.2 | 1.3 |
| raw TCP | PUT | 4 | no-delay | 4215 | 0.9 | 2.0 |

On that link every single-stream GET paid the 40 ms delayed-ACK timer
(the trailers frame after 1 MiB of unacknowledged data); between two
hosts only about 1 % of RPCs did, which is what the first matrix pass
showed as a 42 ms p99. Every two-host measurement before this fix
(Phase 2.1 and the Phase 2.2 runs in §6.9–§6.10) was taken with Nagle on
the storage node's accepted sockets; the repeated comparison in §6.12
and the disk-bound runs in §6.13 were re-taken with the fix, and the
before/after of the whole system is given there.

Full table: `summarize_transport.py results/transport/transport.jsonl`
(112 cells). Digest, 1 MiB shards, 6 s per cell, one run each:

| Transport | TLS | Op | 1 connection, 1 stream MiB/s | Best cell MiB/s (connections × streams, frame) | Client CPU-s/GiB (median, range) | Server CPU-s/GiB (median, range) | Worst p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| gRPC | no | GET | 969–1164 | 1914 (4 × 8, 64 KiB) | 1.53 (0.65–2.14) | 1.22 (0.80–1.68) | 12.5 ms |
| gRPC | no | PUT | 1019–1227 | 1907 (4 × 8, 64 KiB) | 1.23 (0.73–1.77) | 1.59 (0.64–2.29) | 16.6 ms |
| gRPC | yes | GET | 661–681 | 1604 (4 × 8, 64 KiB) | 2.19 (1.29–2.47) | 1.70 (1.21–2.07) | 15.4 ms |
| gRPC | yes | PUT | 594–648 | 1723 (4 × 8, 64 KiB) | 1.71 (1.28–1.92) | 2.19 (1.30–2.64) | 19.9 ms |
| raw framed TCP | no | GET | 2314–2561 | 2988 (4 × 8, 64 KiB) | 0.62 (0.18–1.21) | 0.51 (0.23–0.77) | 7.3 ms |
| raw framed TCP | no | PUT | 1622–1845 | 2921 (1 × 4, 64 KiB) | 0.49 (0.27–0.86) | 0.76 (0.40–1.52) | 9.0 ms |
| raw framed TCP | yes | GET | 1165–1248 | 2708 (4 × 8, 64 KiB) | 1.17 (0.71–1.59) | 0.94 (0.65–1.16) | 9.6 ms |
| raw framed TCP | yes | PUT | 759–1189 | 2721 (1 × 8, 64 KiB) | 0.98 (0.64–1.27) | 1.28 (0.76–1.92) | 10.2 ms |

What the matrix shows:

- **With no-delay the gRPC tail is gone**: the worst p99 across all 56
  gRPC cells is 20 ms (8 streams on 4 connections), against 42–50 ms in
  almost every cell of the first pass; single-stream PUT went from 381
  to 1019 MiB/s on the same link.
- **One stream per connection is the gRPC floor**: 1.0–1.2 GB/s per
  stream without TLS, 0.6–0.7 GB/s with TLS. Streams and connections
  scale it to 1.9 GB/s (1.7 GB/s with TLS) at 4 connections × 8 streams,
  where the client side spends 1.5–2.5 CPU-s per GiB. Frame size
  (64 KiB versus 1 MiB) changes little once windows are large.
- **Raw framed TCP moves 2–2.5× the bytes per stream and costs 2–3×
  less CPU per GiB** than gRPC on the same link (median client CPU 0.6
  versus 1.5 CPU-s/GiB for GET; 0.5 versus 1.2 for PUT). TLS costs
  both transports about 0.5–0.7 CPU-s/GiB and halves the single-stream
  rate; the raw-with-TLS cells sit between gRPC-without-TLS and
  gRPC-with-TLS.
- **The gateway's own bound is elsewhere.** In the two-host layout the
  gateway host (gateway, load generator and one node) spends about 6.0
  CPU-s per GiB on a durable GET at concurrency 4 and the host with two
  durable nodes and FoundationDB about 3.5 (§6.12); the transport's
  share is at most the 1.2–1.5 CPU-s/GiB the matrix attributes to a
  gRPC client and about 1.2 to a server. A hybrid data path (raw framed
  TCP for shard bytes, gRPC for control) would save at most 0.6–1.0
  CPU-s/GiB on each side, i.e. 10–20 % of the gateway host's and 20–30 %
  of the node host's CPU per GiB, at the cost of a second transport with
  its own framing, TLS, timeouts, fencing and fault tests.

Judgment: not now. The measured upper bound on the gain is 20–30 % of
CPU per GiB on a path that is first bounded by the gateway's own
erasure-coding, hashing and copying (§7), and the first pass of this
matrix found a 40 ms-per-RPC defect in how the existing transport was
configured, which was worth more than any transport change. The hybrid
stays listed in §9 with these numbers as its entry condition: it is
justified only once the gateway's non-transport CPU per GiB has been
brought below the transport's share.

### 6.12 Phase 2.2: repeated cache protection comparison

Method: `repeat.sh`, three repetitions in shuffled order with a warm-up
pass per setup, durable only and the three protection modes at
concurrency 4; `aggregate.py` statistics.

Before and after the storage node's no-delay fix (§6.11), three
repetitions each in shuffled order (`results/repeat-c4-nagle`,
`results/repeat-c4`; 96 × 16 MiB objects, 4 MiB chunks, 2 hosts):

| Configuration | Phase | Before: MiB/s median (min–max) | After: MiB/s median (min–max) | After: p50 / p95 / p99 ms | After: hit ratio | After: CPU-s/GiB, node host (2 durable nodes + FoundationDB) / gateway host (gateway + 1 node + load generator) | After: net GB per GiB, node host | Largest `arca-lake` process RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| durable only | PUT | 326 (217–329) | 251 (178–338) | 188 / 602 / 648 | n/a | 7.4 / 10.2 | 1.07 | 102 |
| durable only | GET warm (pass 3) | 592 (589–594) | 574 (574–575) | 97 / 121 / 129 | n/a | 3.5 / 6.0 | 1.06 | 102 |
| none | PUT | 315 (149–330) | 245 (131–351) | 186 / 598 / 660 | n/a | 7.6 / 10.0 | 1.01 | 133 |
| none | GET pass 1 (populating) | 429 (423–441) | 420 (412–436) | 128 / 173 / 181 | 0 % | 5.9 / 8.9 | 1.44 | 133 |
| none | GET pass 3 | 671 (659–675) at 94 % | 655 (641–673) | 79 / 103 / 124 | 97 % | 1.7 / 5.5 | 0.43 | 133 |
| replication 2 | PUT | 188 (159–193) | 176 (162–246) | 299 / 677 / 798 | n/a | 10.7 / 11.4 | 1.08 | 170 |
| replication 2 | GET pass 1 | 402 (402–412) | 369 (369–389) | 150 / 201 / 213 | 0 % | 5.9 / 10.5 | 1.40 | 170 |
| replication 2 | GET pass 3 | 442 (437–451) at 67 % | 465 (455–467) | 117 / 168 / 186 | 80 % | 3.8 / 7.6 | 1.02 | 170 |
| ec 2+1 | PUT | 251 (209–322) | 347 (206–350) | 182 / 210 / 222 | n/a | 6.2 / 9.2 | 1.08 | 105 |
| ec 2+1 | GET pass 1 | 419 (418–425) | 427 (397–441) | 131 / 177 / 206 | 0 % | 5.3 / 9.1 | 1.26 | 105 |
| ec 2+1 | GET pass 3 | 507 (481–528) at 75 % | 497 (482–545) | 109 / 150 / 171 | 76 % | 3.2 / 7.8 | 0.66 | 105 |

What repetition adds to the single runs of Phase 2.1:

- **GET is repeatable to a few percent; PUT is not.** Warm GET medians
  have a spread of 1–13 % across repetitions (sd 0.6–27 MiB/s), so the
  ordering durable < replication 2 < ec 2+1 < none on pass 3 is real.
  PUT spreads by up to 2.7× between repetitions of the same
  configuration (sd 37–90 MiB/s) with p95 latencies of 0.2–0.8 s: the
  PUT path has a periodic stall that the single Phase 2.1 runs could not
  distinguish from configuration effects. Any PUT comparison between
  configurations needs more repetitions than three, and the stall is a
  Phase 2.3 item (§9).
- **At concurrency 4 the no-delay fix changes nothing measurable**
  (every after-median within the before-range or within a few percent):
  the ~1 % of RPCs that paid the 40 ms tail were hidden behind the other
  streams, and the gateway host's CPU is the bound. The fix matters at
  low concurrency (§6.11, §6.10).
- **Cost per GiB served from the cache.** With 97 % hits (`none`, pass
  3) the durable node host spends 1.7 CPU-s and moves 0.43 GB of network
  per GiB, against 3.5 CPU-s and 1.06 GB for the durable read: a hit
  halves that host's CPU and takes 40 % of its network, which is the
  cache's purpose (the durable tier is relieved). The gateway host
  changes little (5.5 versus 6.0 CPU-s/GiB): it still terminates HTTP,
  hashes and copies every byte. Replication 2 at 80 % hits still moves
  1.02 GB per GiB on the node host (one of the two copies lives there)
  and costs it 3.8 CPU-s; ec 2+1 at 76 % hits moves 0.66 GB and costs
  3.2 CPU-s.
- **Population cost.** Pass 1 (every chunk a miss that is also
  populated) costs the node host 5.3–5.9 CPU-s and 1.26–1.44 GB per GiB
  against 3.5 and 1.06 for the plain durable read, and the gateway host
  8.9–10.5 against 6.0: populating adds 50–70 % CPU and 20–35 % network
  to a miss on both sides, independent of protection mode, because the
  gateway writes the copy and a node receives it.
- **Memory.** Largest `arca-lake` process on either host: 102 MiB
  durable, 105 ec, 133 none, 170 replication 2 (the replication writer
  holds two full copies in flight per population).

### 6.13 Phase 2.2: disk-bound runs

Method: `diskbound.sh`, durable only, data set larger than the page cache
of every node (`free -m` recorded before and after), sequential then
random GET passes, 256 KiB objects, and a GET pass with a concurrent PUT
stream. No cache drops, no direct I/O.

Layout: the first host runs FoundationDB and two durable nodes (the two
data shards of every chunk), the second the third node (parity), the
gateway and the load generator. 24 GiB of 16 MiB objects, 4 MiB chunks,
`rs 2+1` (36 GiB of shards: 24 GiB on the first host, 12 GiB on the
second), 7.7 GiB of RAM per host; after the PUT each host's page cache
held 6.8–7.2 GB. Single run (`results/diskbound`; disk reads from
`sar -b`, page cache from `free -m` before and after each phase):

| Phase | Objects | MiB/s | ops/s | p50 / p99 ms | Disk read per GiB served, first host / second host | CPU-s/GiB first / second host |
| --- | --- | --- | --- | --- | --- | --- |
| PUT 24 GiB, concurrency 4 | 1536 × 16 MiB | 207 | 12.9 | 289 / 801 | — | 9.6 / 10.8 |
| GET sequential, first pass | 1536 × 16 MiB | 554 | 34.6 | 99 / 156 | 0.93 GB / 0 | 4.3 / 6.1 |
| GET random, pass 1 | 1536 × 16 MiB | 546 | 34.1 | 101 / 144 | 1.52 GB / 0 | 4.8 / 6.2 |
| GET random, pass 2 | 1536 × 16 MiB | 541 | 33.8 | 103 / 152 | 1.55 GB / 0 | 4.9 / 6.2 |
| GET random with a concurrent PUT stream (107 MiB/s, 256 × 16 MiB, p50 0.58 s) | 1536 × 16 MiB | 442 | 27.6 | 123 / 218 | 1.61 GB / 0 | 6.8 / 7.9 |
| PUT 8192 × 256 KiB, concurrency 8 | 8192 | 65 | 259 | 24 / 115 | — | 39.6 / 26.1 |
| GET 256 KiB sequential | 8192 | 282 | 1130 | 6.9 / 9.0 | 0 / 0 (2 GiB set, page-cache resident) | 8.8 / 8.3 |
| GET 256 KiB random, passes 1–2 | 8192 | 292–296 | 1166–1182 | 6.7 / 9.1 | 0 / 0 | 7.1 / 7.9 |

What it shows:

- **The large random reads came from the device**: 1.5 GB read from
  disk per GiB served on the host with the data shards (the two 2 MiB
  shards of every chunk plus readahead), nothing on the parity host (a
  healthy cluster never reads the parity shard). The sequential first
  pass found a third of its data still in the page cache from the PUT.
- **On this hardware the device is not the bound.** Random reads from
  disk run at 541–546 MiB/s against 574–606 MiB/s for the same request
  stream from a page-cache-resident set (§6.10, §6.12), a 5–10 % cost;
  the virtio disks sustain the ~800 MB/s of shard reads this needs, and
  the gateway host's CPU (6.2 CPU-s/GiB, the same as warm) remains the
  limit. The "disk-bound" condition was met in the sense of the data
  coming from disk, not in the sense of the disk limiting throughput;
  on HDD-backed nodes the same run would be seek-bound and the cache
  tier's latency benefit correspondingly larger (not measured).
- **A concurrent PUT stream costs the reads 19 % of throughput and 50 %
  of p99** (442 MiB/s, p99 218 ms): the PUT's shard writes and metadata
  commits compete for the same CPU, not for the disk (the PUT host's
  disk reads stay at 1.6 GB per GiB served).
- **Small objects are metadata-bound, not disk-bound.** 256 KiB objects
  GET at 1130–1180 per second (p50 6.7 ms) from the page cache, and PUT
  at 259 per second with 40 CPU-s per GiB on the FoundationDB host: the
  per-object begin / append / commit transactions dominate. The 2 GiB
  small set fitted in the page cache, so a disk-bound small-object read
  was not measured here.

Harness note: the first run of this script overwrote the first 1536
large objects with the small ones (same key space) and reported every
GET of the mixed phase as a mismatch; the small phases now use their own
prefix and the run was repeated (`results/diskbound-overlap-bug` keeps
the first run's data for the record; its large-object phases are valid).

### 6.14 Phase 2.3: where a PUT waits (R-48)

Method: `put-profile.sh` (RFC-009 §4 conditions, 96 × 16 MiB objects at
concurrency 4, fresh cluster per repetition) with `PROFILE=1`: the
gateway, every storage node and the store record per-phase histograms
(`arca_gateway_put_phase_seconds{phase}`, `arca_node_phase_seconds{phase}`),
`sampler.sh` records `sar` (CPU, disks, network, context switches,
memory), `pidstat`, the TCP counters, every metrics endpoint and a
FoundationDB status digest once per second, and `s3load --progress`
writes the bytes completed per second; `summarize_putprofile.py` aligns
the series.

Three repetitions on the two-host layout (`results/putprofile`), then
three more with nothing else running on the validation hosts
(`results/putprofile-quiet`) and three with a synchronous write load
(`dd … oflag=dsync` in a loop) running on a *different* VM of the same
hypervisor (`results/putprofile-noisy`):

| Run | PUT MiB/s | p50 / p95 ms | Gateway shard_write mean ms | Gateway append mean ms | Gateway commit mean ms | Node put_store mean ms | Node fdatasync mean / max ms | FoundationDB commit probe s | Disk await ms (node host) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| first set, repetition 1 (CI build and package installs running on other VMs) | 87 | 755 / 1100 | 124 | 57 | 61 | 98 | 55 / 244 | 0.095 | 14.7 |
| first set, repetition 2 | 293 | 190 / 320 | 34 | 11 | 11 | 17 | 10 / 33 | 0.010 | 2.2 |
| first set, repetition 3 | 349 | 180 / 207 | 27 | 7 | 8 | 9 | 6 / 19 | 0.007 | 1.2 |
| quiet, repetitions 1–3 | 198 / 281 / 225 | 290–284 / 322–547 | 53 / 35 / 44 | 23 / 12 / 14 | 21 / 12 / 17 | 34 / 17 / 14 | 21 / 10 / 9 | — | 5.2 / 2.2 / 1.7 |
| noisy (write load on another VM), repetitions 1–3 | 172 / 184 / 137 | 365–465 / 392–563 | 59 / 56 / 74 | 28 / 26 / 37 | 27 / 24 / 43 | 39 / 38 / 52 | 25 / 24 / 33 | — | 4.9 / 4.9 / 6.9 |

What the series show:

- **The slow repetition is slow everywhere that touches a disk, and
  nowhere else.** In the 87 MiB/s run the node's `fdatasync` mean was
  55 ms against 6–10 ms, the FoundationDB commit probe 0.095 s against
  0.007–0.010, metadata append 57 ms against 7–11, on both hosts at
  once; the gateway's own phases (body read 1.5–1.9 ms, encode
  3.5–4.3 ms per chunk) did not change. The node's queue wait
  (`put_queue`) stayed at 0.1 ms: the blocking pool was not the
  bottleneck. Context switches and CPU idle were unremarkable; TCP
  retransmissions (R-49) did not correlate with the slow seconds.
- **Disk latency, not disk saturation.** In the slow run the node host's
  disk reported 14.7 ms average wait at 88 % utilization while writing
  82 MB/s; in the fast runs 1.2–2.2 ms at 50–65 % while writing
  300–358 MB/s. The device was slow, not full.
- **A write load on another VM reproduces it.** With `dd` syncing on
  host A, the nodes' fdatasync on hosts B and C rose from 8–21 ms to
  24–33 ms and PUT fell from 198–281 to 137–184 MiB/s. The virtual disks
  share physical storage; PUT throughput tracked fdatasync latency one
  to one because a PUT waited for each chunk's group commit before
  reading the next chunk (§6.16 is the mitigation).
- Even the quiet set varies 198–281 MiB/s with fdatasync 9–21 ms; the
  first quiet repetition followed the teardown of the previous run,
  whose deleted gigabytes the file system was still freeing. The
  environment's floor of variance is a few tens of percent; the 2.7×
  of Phase 2.2 was neighbouring load (the Phase 2.2 runs overlapped the
  fault matrix on host A and CI builds).


### 6.15 Phase 2.3: GET CPU profile and frame pass-through

Method: `netns-faults.sh cpuprofile` on one host (perf at 499 Hz on the
gateway and one durable node for 20 s of an 8-pass GET at concurrency 4,
96 × 16 MiB objects; durable-only, and the cache tier populating and
warm). Before and after the frame pass-through (ADR-0012).

One host, everything (gateway, three durable nodes, cache nodes,
FoundationDB, load generator) on 4 vCPUs, so the absolute throughput is
below the two-host numbers and the shares are what matter. Gateway
process, 20 s samples at 499 Hz (`results/cpuprofile`,
`results/cpuprofile-zerocopy`):

| Phase | GET MiB/s before, median of 8 passes (min–max) | after | libc (memcpy) share | kernel share | arca-lake share | page zeroing (`kernel_init_pages`) | CRC-32C |
| --- | --- | --- | --- | --- | --- | --- | --- |
| durable, warm | 697 (479–796) | 909 (495–959) | 37 % → 19 % | 45 % → 52 % | 15 % → 24 % | 7.7 % → 7.1 % | 7.1 % → 12.2 % |
| cache ec 2+1, populating | 648 (484–740) | 574 (513–783) | 41 % → 24 % | 37 % → 47 % | 18 % → 24 % | 5.5 % → 6.5 % | 8.4 % → 10.0 % |
| cache ec 2+1, all hits | 673 (657–707) | 695 (606–761) | 43 % → 21 % | 37 % → 48 % | 16 % → 25 % | 5.2 % → 7.1 % | 7.8 % → 12.6 % |

What the profile showed before the change (every phase alike): the
libc share is `__memmove_avx_unaligned_erms` (the offsets resolve to the
AVX copy loop), the largest kernel symbol is the socket copy
(`rep_movs_alternative`, 11–12 %), and `kernel_init_pages` is the
kernel zeroing freshly mapped pages: multi-MiB buffers were allocated
and unmapped per chunk. Erasure decoding does not appear at all (a
healthy read uses the data shards as they are); CRC-32C is 6–7 %. The
storage node has the same shape (file-to-user copy 20 %, page zeroing
9 %, CRC 9.5 %).

The copies were counted in the read path: frames concatenated into a
shard, the shard copied into a `Vec`, the data shards joined into the
chunk — three user-space copies of every byte after the transport's
own decode, each into a fresh allocation. Passing the frames through
(ADR-0012) removes all three on the healthy path: the libc share falls
from 37–43 % to 19–21 %, the arca-lake share rises because CRC is now
the largest user-space cost (10 %), and the kernel share rises in
relative terms because the socket copies are what remains. Page zeroing
stays at 7 %: the storage node still allocates a fresh buffer per shard
read and the gateway still allocates the populate and repair copies;
that is the next item (§9). GET throughput on this host rose by about
30 % for the durable read (two shards per chunk, two concatenations
saved) and by 5–10 % for the cache hit (one shard, one copy saved);
the two-host CPU-seconds per GiB are in §6.19.


### 6.16 Phase 2.3: pipelined chunk placement

Method: `put-profile.sh` with `--write-pipeline 1 / 2 / 4`, three
repetitions each on idle hosts, then depth 1 against 2 with a
synchronous write load running on another VM of the same storage.

Three repetitions per cell, each on a fresh cluster (`results/putprofile-pipe*`,
`results/putprofile-noisy-pipe*`); the fdatasync column is the storage
node's mean over the run, which is the environmental variable the PUT
depends on (§6.14):

| Configuration | PUT MiB/s, repetitions 1–3 | p50 ms | p95 ms | node fdatasync mean ms |
| --- | --- | --- | --- | --- |
| depth 1, idle hosts | 150 / 261 / 332 (median 261) | 352 / 209 / 189 | 772 / 345 / 214 | 27 / 11 / 6 |
| depth 2, idle hosts | 225 / 178 / 193 (median 193) | 195 / 342 / 224 | 560 / 601 / 643 | 17 / 28 / 23 |
| depth 4, idle hosts | 264 / 304 / 305 (median 304) | 219 / 208 / 212 | 367 / 276 / 234 | 12 / 7 / 9 |
| depth 1, write load on another VM | 148 / 129 / 168 (median 148) | 465 / 492 / 363 | 543 / 567 / 461 | 29 / 37 / 26 |
| depth 2, write load on another VM | 218 / 224 / 173 (median 218) | 286 / 281 / 284 | 369 / 347 / 520 | 28 / 28 / 40 |

What it shows:

- **Under the same disk latency, the pipeline recovers what the latency
  took.** With the write load on another VM both depths saw 26–40 ms
  fdatasync means; depth 2 delivered a median 218 MiB/s against 148 for
  depth 1 (+47 %), p50 281–286 ms against 363–492 ms. On idle hosts the
  repetitions did not see the same fdatasync latency (6–28 ms across the
  nine runs), so they are compared at like latency: depth 2 at 28 ms
  gave 178 MiB/s where depth 1 at 27 ms gave 150; depth 4 at 12 ms gave
  264 where depth 1 at 11 ms gave 261.
- **The ceiling does not move.** At single-digit fdatasync latency every
  depth lands at 300–330 MiB/s: the gateway host's CPU (erasure coding,
  hashing, HTTP) is the bound there, as the GET side is (§6.12), and
  more chunks in flight cannot raise it.
- **Depth 4 costs more than it adds here.** It needs twice the memory
  per PUT of depth 2 and makes each shard stream slower on the node
  (`put_recv` 26–37 ms against 4–8 ms, because four chunks' shards
  share the link) for no further gain at these latencies; depth 2 is
  the default.
- The noisy depth-2 third repetition shows the kind of event the
  pipeline cannot hide: one fdatasync of 638 ms on the node turned into
  a 1.85 s p99 for the PUTs waiting on that group commit.

The variance itself remains: the quiet repetitions of one depth still
span 150–332 MiB/s with the node's fdatasync mean. R-48 is therefore
closed as *explained and mitigated* rather than removed: PUT throughput
on this environment is a function of the shared storage's write latency
(§6.14), the pipeline flattens that function by about half at the
latencies measured, and the regression check (§6.19) treats PUT-only
changes as suspect until repeated.


### 6.17 Phase 2.3: metadata hot keys

Method: `metabench` against the FoundationDB cluster on one host: 1 to
32 tasks recording and dropping cache placements of one tier, touching
them, or a mix; with an evictor running the controller's recipe; two
processes on the same tier (two gateways) and on two tiers; a
capacity-limited insert. Every operation runs through the production
transaction loop, whose counters (attempts, conflicts, unknown-outcome
retries, exhausted) are reported.

Single FoundationDB process (`memory` engine) on a 4-vCPU host, 10 s
per cell, one run each (`results/metabench/fdb.jsonl`):

| Workload | Tasks | Placement transactions / s | p50 ms | p99 ms | Attempts | Conflicts | Exhausted |
| --- | --- | --- | --- | --- | --- | --- | --- |
| insert / remove | 1 | 278 | 3.5 | 8.9 | 2784 | 0 | 0 |
| insert / remove | 4 | 1050 | 2.3 | 16.9 | 10504 | 0 | 0 |
| insert / remove | 8 | 3202 | 2.3 | 6.9 | 32024 | 0 | 0 |
| insert / remove | 16 | 6083 | 2.6 | 4.5 | 60848 | 0 | 0 |
| insert / remove | 32 | 8264 | 3.6 | 11.2 | 82774 | 0 | 0 |
| touch (recency) | 1 | 408 | 1.8 | 9.5 | 4143 | 0 | 0 |
| touch (recency) | 4 | 701 | 3.1 | 29.0 | 7268 | 0 | 0 |
| touch (recency) | 8 | 3066 | 2.5 | 6.0 | 31184 | 0 | 0 |
| touch (recency) | 16 | 5160 | 2.9 | 7.4 | 52640 | 0 | 0 |
| touch (recency) | 32 | 1687 | 10.1 | 71.5 | 18945 | 0 | 0 |
| mixed (half insert/remove, half touch) | 1 | 52 | 14.3 | 67.5 | 517 | 0 | 0 |
| mixed (half insert/remove, half touch) | 4 | 342 | 10.5 | 28.0 | 3546 | 0 | 0 |
| mixed (half insert/remove, half touch) | 8 | 600 | 10.6 | 87.8 | 6264 | 0 | 0 |
| mixed (half insert/remove, half touch) | 16 | 1345 | 11.0 | 26.4 | 13992 | 0 | 0 |
| mixed (half insert/remove, half touch) | 32 | 1046 | 27.2 | 47.5 | 11496 | 0 | 0 |
| insert / remove + evictor (controller recipe, 50 sweeps/s) | 8 | 1077 | 6.5 | 18.7 | 11873 | 0 | 0 |
| insert / remove + evictor (controller recipe, 50 sweeps/s) | 32 | 4375 | 7.5 | 14.3 | 45184 | 0 | 0 |
| two processes, same tier (A) | 16 | 1926 | 7.6 | 22.4 | 19269 | 0 | 0 |
| two processes, same tier (B) | 16 | 1929 | 7.5 | 24.4 | 19296 | 0 | 0 |
| two processes, two tiers (A) | 16 | 1577 | 7.5 | 32.1 | 15777 | 0 | 0 |
| two processes, two tiers (B) | 16 | 1578 | 7.5 | 33.9 | 15792 | 0 | 0 |
| insert / remove with a 1 GiB capacity (most inserts refused) | 16 | 15335 | 0.9 | 3.2 | 153376 | 0 | 0 |
| in-memory store reference | 8 | 7801 | 0.7 | 4.4 | 39029 | 0 | 0 |
| in-memory store reference | 32 | 3100 | 7.1 | 33.9 | 15541 | 0 | 0 |

What it shows:

- **No conflicts at all.** Every cell, including 32 concurrent tasks on
  one tier, an evictor removing entries while they are inserted, and
  two processes on the same tier, completed every placement transaction
  on its first attempt (attempts equal operations; conflicts and
  exhausted are zero). The Phase 2.2 change (usage as an atomic
  addition, capacity as a snapshot read) removed the one key that
  conflicted; the recency index keys are unique per entry and the
  generation header is only read (it conflicts solely with a retire of
  that generation). The sharded counter, reservation and quota
  reconciliation alternatives are therefore not needed; the capacity
  guarantee stays as RFC-006 §5.4 states it (an overshoot bounded by
  the populations in flight).
- **The limit is the metadata server, not the keys.** One process
  reaches about 8,300 placement transactions per second at 32 tasks
  (p50 3.6 ms), two processes together about 3,900 (p50 7.5 ms each):
  the single FoundationDB process saturates, and latency, not conflict
  retries, is what grows. At the PUT rates of §6.16 (a few hundred
  chunk appends per second) and the population rates of §6.12 (tens of
  inserts per second) the metadata path has two orders of magnitude of
  headroom on this cluster; a multi-process FoundationDB layout remains
  unmeasured (§8).
- The in-memory store reference (7,800 per second at 8 tasks, falling
  at 32 under its single lock) only bounds the benchmark's own
  overhead; it is not a target.


### 6.18 Phase 2.3: multi-gateway

Method: `multi-gateway.sh`: one, two and three gateways, each driven by
the load generator on its own host with its own key prefix, PUT then two
GET passes, durable-only and with the unprotected cache tier; aggregate
throughput is total bytes over the longest gateway's run.

<!-- MULTI-GATEWAY -->

### 6.19 Phase 2.3: regression check baseline

Three repetitions in shuffled order on the two-host layout after the
frame pass-through, the write pipeline and the population queue
(`results/repeat-zerocopy`; the Phase 2.2 figures are §6.10 and §6.12,
measured after the no-delay fix):

| Configuration | Phase | Phase 2.2 MiB/s (p50 / p95 ms) | Phase 2.3 MiB/s (p50 / p95 ms) | Phase 2.2 → 2.3 CPU-s/GiB, gateway host / node host |
| --- | --- | --- | --- | --- |
| durable, concurrency 1 | GET warm | 420 (29 / 32) | 472 (24 / 28) | 4.6 → 3.5 / 2.8 → 2.2 |
| durable, concurrency 4 | GET warm | 585 (96 / 121) | 695 (78 / 94) | 6.0 → 4.6 / 3.4 → 2.2 |
| durable, concurrency 4 | PUT | 214–251 (sd 35–66) | 340 (180–372, sd 84) | 10.7 → 10.5 / 8.2 → 6.6 |
| none, concurrency 1 | GET all hits | 474 (25 / 29) | 451 (26 / 29) | 4.4 → 4.3 / 1.6 → 1.9 |
| none, concurrency 4 | GET pass 1 (populating) | 441 (125 / 166) | 514 (106 / 139) | 8.8 → 7.4 / 6.2 → 4.4 |
| none, concurrency 4 | GET pass 3 | 668 at 97 % hits (80 / 107) | 703 at 93 % hits (73 / 103) | 5.5 → 5.2 / 2.3 → 1.6 |

What changed on two hosts:

- **The durable read is 12–20 % faster and costs the gateway host 23 %
  less CPU per GiB** (6.0 → 4.6 CPU-s/GiB at concurrency 4), and the
  node host 35 % less (3.4 → 2.2): the gateway's three post-decode copies
  are gone, and the node, which keeps its one file copy, benefits from
  fewer retransmission-free bursts (its share of the work was partly the
  gateway's back-pressure). The cache hit, which only had one
  concatenation to lose, is within noise.
- **PUT at concurrency 4 reached 340 MiB/s** (the Phase 2.2 median was
  214–251 with the same spread); the pipeline's effect is the subject
  of §6.16 and the spread is the environment's.
- **Memory.** The largest `arca-lake` process was 96–110 MiB in the
  durable runs and 105 MiB with the cache at concurrency 1, but 213 MiB
  with the cache at concurrency 4 (133 in Phase 2.2): a queued
  population holds its 4 MiB chunk, and the queue depth was 32 in that
  run. The default is now 8 (32 MiB at this chunk size); the bound is
  documented in RFC-006 §5.3.1.

The regression baseline for this environment (`benchmarks/baselines/lab-2host.json`,
label `lab-2host`) was recorded from this run: PUT 340 MiB/s,
warm GET 695 MiB/s (p50 78, p95 94, p99 100 ms),
4.6 / 2.2 CPU-s/GiB on the gateway / node host, RSS 110 MiB,
hit ratio 0.93, population 70 chunks/s, 149 metadata transactions/s
during PUT. `compare.py` applies its tolerances only to later runs with
that label (`docs/verification/performance-regression.md`).


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

*Phase 2.2 revisions* (three repetitions where stated; §6.8–§6.13):

- **The cache hit is now parallel too, and beats the durable read at
  low concurrency.** With `read_parallelism 4` the single-request `none`
  hit overtook the durable read (474 versus 420 MiB/s, §6.10); parallel
  fetch alone is worth 12–17 % at concurrency 1 and nothing at 4, where
  the gateway host's CPU bounds every mode. The larger gain first seen
  (+87 %) was the Nagle stall below being overlapped, not parallel fetch.
- **The two Phase 2.1 "transport floor" conclusions were half right.**
  The gateway is CPU-bound, but the transport it was measured on had a
  40 ms Nagle/delayed-ACK stall on about 1 % of RPCs (every RPC on a
  low-latency link), fixed in Phase 2.2 (§6.11). Measured against the
  corrected stack, gRPC per stream reaches 1.0–1.2 GB/s and raw framed
  TCP 2.3–2.6 GB/s at 2–3× less CPU per GiB; a hybrid data path could
  save at most 20–30 % of the gateway host's CPU per GiB and is deferred
  behind the gateway's own CPU work (§6.11, §9).
- **Hit ratio is population bandwidth, not readability.** Every lookup
  hit was served; the modes differ only in how many populations complete
  under the in-flight bound (§6.9). A hit costs the durable node host
  half the CPU and 40 % of the network of a durable read while the
  gateway host's cost barely moves (§6.12); a populating miss costs
  50–70 % more CPU than a plain miss on both sides.
- **PUT varies 2.7× between repetitions of one configuration** with p95
  latencies of 0.2–0.8 s (§6.12); GET varies by a few percent. Single-run
  PUT comparisons in §6.3–§6.4 should be read with that spread; the
  stall itself is unexplained (R-48).
- **Resilience.** The shard-stream policy of ADR-0011 takes PUT success
  at 10 % loss from 4/12 to 12/12 and under a 20 s node stop from 46/48
  to 48/48, with slow-but-progressing links never timing out and every
  verify pass clean (§6.8).

- **Reads from disk cost 5–10 % here, and nothing else changes.** With
  a 24 GiB set the random GET pass read 1.5 GB from the device per GiB
  served and still ran at 541–546 MiB/s against 574–606 from the page
  cache (§6.13); the virtio disks are faster than the gateway's CPU
  budget. Small objects are bound by the per-object metadata
  transactions (259 PUT/s, 1130–1180 GET/s), not by any device.

*Phase 2.3 revisions* (§6.14–§6.19):

- **R-48 was the environment, and the code's sensitivity to it.** PUT
  throughput tracked the storage nodes' fdatasync latency one to one;
  that latency is set by what the other VMs on the same physical storage
  are doing (§6.14). Pipelining chunk placements halves the sensitivity
  (§6.16); the ceiling stays the gateway host's CPU.
- **The GET path's CPU was copies, not coding.** Three post-decode copies
  and the allocations behind them were 37–43 % of the gateway's CPU;
  passing frames through removed them, and the two-host durable read
  now costs 4.6 CPU-s/GiB on the gateway host instead of 6.0 (§6.15,
  §6.19). What remains is the kernel's socket copies and CRC-32C.
- **Metadata contention is not a bound.** Zero conflicts in every
  hot-key cell; the single FoundationDB process saturates at about
  8,300 placement transactions per second, two orders of magnitude above
  the rates the data path produces (§6.17).
- **Populations are now bounded per node and yield to reads** without
  measurable cost to the hit ratio at the default settings (§6.19: 93 %
  at pass 3 against 97 % with the unbounded semaphore and a larger
  queue; the smaller queue is a memory bound, RFC-006 §5.3.1).

<!-- INTERP-MULTIGW -->

*Hypotheses* (not measured): a dedicated gateway host would move the PUT
bound from CPU to the network; on hosts whose durable tier is HDD-bound
the cache's latency benefit would be larger than measured here.

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
| Memory | snapshot per run; gateway RSS per configuration and parallelism | §6.6, §6.10, §6.12 |
| Shard-stream policy under loss 0/2/5/10 %, delay with and without jitter, resets, partition, slow node, stopped node (PUT success, throughput, percentiles, timeout and retry counters, verify) | measured, five policies, one run per cell | §6.8, `results/r47/` |
| Hit accounting (lookup / served / degraded / fallback by cause, admission rejections by cause, population failures by cause) | measured, three modes × two population bounds | §6.9, `results/hit-analysis/` |
| Parallel chunk fetch 1 versus 4 | measured, three repetitions | §6.10, `results/parallel-p1/`, `results/parallel-p4/` |
| gRPC versus raw framed TCP × TLS × connections × streams × frame size, with client and server CPU | measured, one run per cell, corrected server configuration | §6.11, `results/transport/` (first pass: `results/transport-nagle/`) |
| Effect of `TCP_NODELAY` on accepted storage-node sockets | measured before/after on a veth link and between hosts | §6.11, `results/nagle/` |
| Variance across runs | measured: three repetitions, shuffled order, warm-up separated (GET 1–13 %, PUT up to 2.7×) | §6.12, `results/repeat-c4/` |
| Disk-bound behaviour: 24 GiB set, sequential and random GET, small objects, concurrent PUT/GET, with device reads per GiB | measured, one run; data came from disk, the device was not the bound on this hardware | §6.13, `results/diskbound/` |
| Where a PUT waits: per-phase timing on gateway, node and store, aligned with per-second system counters; cause of R-48 reproduced with a neighbouring write load | measured (3 + 3 + 3 repetitions) | §6.14, `results/putprofile*` |
| GET CPU profile before and after the frame pass-through | measured (perf, one host; two-host CPU-s/GiB in §6.19) | §6.15, `results/cpuprofile*` |
| Pipelined chunk placement, depths 1 / 2 / 4, idle and under a neighbouring write load | measured, 3 repetitions per cell | §6.16 |
| Metadata hot keys: conflicts and throughput up to 32 tasks, evictor, two processes, two tiers | measured, one run per cell, one FoundationDB process | §6.17, `results/metabench/` |
| Multi-gateway aggregate throughput, 1 / 2 / 3 gateways | measured, one run per cell | §6.18, `results/multigw/` |
| Two-host repeat after Phase 2.3; regression baseline `lab-2host` | measured, 3 repetitions | §6.19, `results/repeat-zerocopy/` |
| Disk-bound behaviour on HDD or slower devices; small-object set larger than the page cache | not measured | §6.13 |
| Cause of the PUT run-to-run variance (R-48) | identified: fdatasync latency on shared virtual storage; mitigated by the write pipeline | §6.14, §6.16 |
| TCP retransmissions on the gateway host under PUT (R-49) | observed, not root-caused | §6.14 |
| Multi-process FoundationDB; metadata contention across many gateways | not measured (one process, up to two client processes) | §6.17 |
| Multi-gateway scaling, more than one FoundationDB process | not measured | §3 |
| Any number on other hardware | not claimed | — |

## 9. Proposed optimizations

Done in Phase 2.2 (measured above): parallel chunk fetch (§6.10), the
progress-based shard-stream policy with retries and lease renewal
(§6.8, ADR-0011), an atomic cache usage counter instead of the hot
read-modify-write key (§6.9, R-42), and `TCP_NODELAY` on accepted
storage-node sockets (§6.11). Done in Phase 2.3: pipelined chunk
placement (§6.16), frame pass-through on GET (§6.15), the population
queue (RFC-006 §5.3.1), permanent phase timing (§6.14).

Still hypotheses (not measured):

- **Storage-node read path**: the node still allocates a fresh buffer
  per shard read and copies file to user space (§6.15: 20 % copy, 9 %
  page zeroing on the node); a reused buffer or `sendfile`-style path
  would remove the zeroing.
- **CRC-32C is now the gateway's largest user-space cost** (10 %,
  §6.15); it could move to the storage node's read (which already
  computes it on write) with the gateway verifying only on the slow
  path, at the cost of trusting the node's CRC on a healthy read.
- **fdatasync batching across PUTs**: the group commit already
  coalesces concurrent writers; a bounded delay before a sync (a few
  ms) would widen the batch under high fdatasync latency at the cost of
  that delay on an idle node. Measure on the shared storage first.
- **Population write batching and a per-node population queue** (the
  permit count is a global bound today; §6.9 shows the bound, not the
  node, limiting coverage).
- **Raw framed transport for shard bytes** (ADR-0006 chose gRPC). Entry
  condition from §6.11: at most 20–30 % of the gateway host's CPU per
  GiB is at stake, so it is justified only after the gateway's own
  erasure-coding, hashing and copying CPU per GiB has been brought
  below the transport's share (about 1.2–1.5 CPU-s/GiB).
- **Reduce gateway CPU per GiB on GET** (3.3–3.5 CPU-s/GiB durable at
  concurrency 4): avoid the copy between the shard frames and the HTTP
  body, and hash while decoding.

## 10. Open questions

- Which workloads justify replication or EC cache protection given the
  measured population cost (§6.12: populating costs 60–70 % more
  gateway CPU than a plain miss; a hit saves half of it).
- Answered for this environment: with parallel chunk fetch the cache
  hit beats the durable read at low concurrency (§6.10), so the cache
  pays in latency as well as in durable-tier CPU and network once warm.
- Whether the PUT stall (R-48) is in the metadata commit path or in the
  storage node.
