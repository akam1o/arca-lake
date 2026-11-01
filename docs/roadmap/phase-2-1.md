# Phase 2.1: Cache protection and Linux validation

Status: **first iteration done** (see "Not verified"). Phase 2.1 adds
replicated and erasure-coded cache groups with degraded reads and
subordinate cache repair, and runs the system on real Linux hosts: a
three-host measurement matrix (RFC-009), transport and TLS floors, and
network fault injection in an isolated namespace pair
(`docs/verification/network-faults.md`).

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 2.1.1 Cache protection design | done | RFC-006 §5.9: none / replication / EC compared on capacity, read and write amplification, CPU, network, hit latency, degraded read, repair, eviction, population, failure domains; ADR-0010 |
| 2.1.2 Cache EC implementation | done | `GroupRecord.role`; `CacheProtection`; shared `read_coded` / `write_coded`; atomic population; degraded hits with read-triggered repair; per-member deletion and reconciliation; `cache_ec.rs` (9 cases × replication, EC) and `cache_ec_fdb.rs` |
| 2.1.3 Cache repair priority and limits | done | durable groups first in every repair pass; cache groups only in passes without durable work; `cache_repair_chunks_per_pass` budget with persisted cursor; gateway read-triggered repair under a permit count |
| 2.1.4 Linux hosts inventory | done | three identical KVM guests (4 vCPU, 7.7 GiB, virtio-net, virtio disk, Ubuntu 26.04.1, kernel 7.0); `tc`/`sch_netem` present; firewall empty (ACCEPT); no Rust, Docker, or FoundationDB preinstalled; toolchain, `foundationdb-clients` (and `-server` on A) installed by the run; no existing service, interface, route, or firewall rule was modified |
| 2.1.5 Transport measurements | done | raw TCP (iperf3), raw framed TCP, gRPC, both with TLS (RFC-009 §6.1) |
| 2.1.6 PUT/GET and cache matrix | done | `benchmarks/linux/matrix.sh` on three hosts (RFC-009 §6.2–§6.6) |
| 2.1.7 Network fault injection | see below | namespace pair on one host with `netem`, `tbf`, link down, `ss -K`, process kills (`deploy/dev/netns-faults.sh`); results in `docs/verification/network-faults.md` |
| 2.1.8 Population back-pressure | done | found in the first two-host run: unbounded concurrent populations failed by the hundreds; now bounded per gateway (`--cache-max-populations`) |

## What the new tests verify

| Suite | Cases | Verified behaviour |
| --- | --- | --- |
| `cache_ec.rs` / `cache_ec_fdb.rs` | 9 × 2 modes each | coded hit from `k` shards with the durable placement untouched; shard loss → degraded hit → repaired on read; member down → degraded, fewer than `k` → durable fallback and invalidation; corrupt shards count as missing, too many fall back; population with a member down records nothing and deletes partial shards; eviction with a member down leaves orphans that reconciliation removes; controller cache repair rebuilds a lost member one chunk per pass until verified; epoch change fences stale populations and keeps placements readable; durable repair and GC leave the coded cache alone |
| Phase 0 / 1 / 2 suites | unchanged | 161 tests in the workspace; FoundationDB suites `fdb` 2, `e2e_fdb` 18, `faults_fdb` 6, `cache_fdb` 12, `cache_ec_fdb` 18 |

## Linux results

All numbers are in RFC-009 §6 and `docs/verification/network-faults.md`;
raw data under `benchmarks/linux/results/` (sanitized of hostnames and
addresses). Highlights, all measured on three identical 4-vCPU KVM
guests with data in the page cache:

- Transport floor between two hosts: raw TCP 20–22 Gbit/s; `transfer-bench`
  1 MiB shards, 4 streams: raw framed 2.7–3.0 GiB/s, gRPC 1.0–1.1 GiB/s,
  gRPC+TLS 0.68 GiB/s.
- Durable only, RS 2+1, 16 MiB objects, 4 MiB chunks: PUT 56 / 241 /
  211 MiB/s and GET 341 / 583 / 593 MiB/s at concurrency 1 / 4 / 8
  (after the window fix for concurrency 1 and 4); GET p50 33 / 94 /
  197 ms.
- Cache `none`, warm: 633 MiB/s at concurrency 4 (durable 583), 271 at
  concurrency 1 (durable 341): the cache halves the bytes read from the
  durable nodes but does not reduce client latency here, because the
  durable read is already parallel over `k` nodes and nothing is
  disk-bound. First-touch passes are 15–25 % slower than durable-only.
- Cache `ec 2+1`: warm 465 MiB/s at concurrency 4; stores 1.5×;
  `replication 2`: 380 MiB/s with 60 % hits after three passes, stores
  2× and moves 2× per population.
- Capacity-limited cache: LRU turnover between the watermarks after the
  Phase 2.1 admission fix; 35–42 % hits for `none` with 52 % of the data
  fitting.
- Population: zero failures after bounding in-flight populations; a cold
  pass populates about half of its misses at concurrency 4.
- Faults (namespace pair, `ec` cache): loss 2 %, bandwidth 100 Mbit/s,
  node and controller restarts, 3 s and 10 s partitions, socket resets,
  GC and repair under impairment all kept every invariant (no partial or
  stale object, explicit failures only, idempotent recovery). Loss 10 %
  and 50 ms delay *with jitter* made shard streams exceed the 10 s RPC
  timeout (explicit 503s, nothing visible); 50 ms delay without jitter
  passed.
- Two code changes came out of the runs: bounded populations
  (`--cache-max-populations`), admission at capacity instead of the high
  watermark, 8 MiB HTTP/2 windows (PUT at concurrency 1: +50–90 %).

## Not verified

- Impairment of a physical NIC between hosts (would require changing an
  interface or firewall of a test host; not done without approval).
  R-35 and R-40 stay High; the namespace-pair run narrows them to the
  cross-host-NIC case only.
- Disk-bound behaviour (data sets fit in the page cache).
- More than one gateway; more than one FoundationDB process on the
  hosts.
- Cache repair priority under a sustained mix of durable and cache
  repairs (by construction only).
- Write-through population; per-bucket tier policies (unchanged from
  Phase 2).

## Acceptance criteria for closing Phase 2.1

1. Every Phase 2.1 CI job passes on every push (met).
2. A cross-host impairment run on an interface dedicated to the test, or
   an approved `tc` filter on the LAN interface, reproducing the
   namespace-pair scenarios.
3. Parallel chunk fetch measured against RFC-009 §6 (optimization
   candidate).
