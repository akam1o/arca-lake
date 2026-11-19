# Phase 2.3: Performance stabilization and scalability

> **Historical phase plan.** The statuses and outstanding tasks below
> record this phase, not the current system. Later work may supersede them.
> See the [roadmap index](README.md) for active priorities and
> [known limitations](../guide/known-limitations.md) for current boundaries.

Original status: **complete** (measurements and documents below). Phase 2.3 acts on the Phase 2.2 measurements:
the cause of the PUT variance (R-48), the gateway's GET CPU, a node-aware
population queue, the metadata hot keys, multi-gateway scaling, and a
performance regression check. Design records: ADR-0012; numbers:
RFC-009 §6.14–§6.19.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| CI state and public artifacts | done | GitHub-hosted runners refused for billing reasons; CI moved to self-hosted Linux runners; artifact sanitizer with a written boundary (`docs/security/public-artifacts.md`) and a CI check; the one commit that carried host names was replaced before anyone else fetched it (private repository; no credential) |
| 2.3.3 R-48 cause | done | per-phase PUT timing on gateway, node and store; per-second sampler; cause: fdatasync latency on shared virtual storage, reproduced with a write load on another VM (RFC-009 §6.14) |
| 2.3.3 PUT variance mitigation | implemented, measured | pipelined chunk placement (`--write-pipeline`), measured quiet and under noise (§6.16) |
| 2.3.4 GET CPU | done | perf profile (§6.15): memcpy 37 %, page zeroing 8 %, CRC 7 %, no EC decode; frame pass-through removes the three post-decode copies; measured before/after |
| 2.3.5 Population queue | implemented, tested | bounded FIFO, workers, per-node permits, bounded yield to reads, retries, shutdown, counters (RFC-006 §5.3.1, ADR-0012) |
| 2.3.6 Metadata hot keys | measured | `metabench` against FoundationDB: tasks 1–32, insert/remove, touch, mixed, evictor, two processes same tier / two tiers (§6.17) |
| 2.3.7 Multi-gateway | measured | `multi-gateway.sh`: 1 / 2 / 3 gateways, durable and cache (§6.18) |
| 2.3.8 Regression check | implemented | `regress.sh` + `compare.py` with per-environment baselines (`docs/verification/performance-regression.md`) |
| 2.3.9 Correctness | tests | every suite passes with the pipeline, the frame pass-through and the queue; new `population_queue.rs` |

## Results

Numbers are in RFC-009; the summaries here are the decisions they support.

- **R-48 (§6.14)**: the PUT variance is the write latency of the shared
  virtual storage. In a slow repetition every disk-touching phase (node
  fdatasync 55 ms mean against 6–10, FoundationDB commit, metadata
  append) was slower at once on both hosts while the gateway's own
  phases were unchanged, and a synchronous write load on a different VM
  reproduced it (fdatasync 8–21 → 24–33 ms, PUT 198–281 → 137–184
  MiB/s). Not a code defect; the code's sensitivity to it was.
- **Pipelined chunk placement (§6.16)**: under the same neighbouring
  write load, depth 2 delivers a median 218 MiB/s against 148 for the
  serial PUT (+47 %); at single-digit fdatasync latency every depth hits
  the gateway's CPU ceiling (300–330 MiB/s). Depth 2 is the default;
  depth 4 costs memory and slows each stream for no further gain.
- **GET CPU (§6.15)**: perf put 37–43 % of the gateway's GET CPU in
  memcpy and 7–8 % in page zeroing; erasure decoding is absent on a
  healthy read. Passing the shard frames through to the HTTP body
  (no concatenation) drops the memcpy share to 19–21 % and raises
  single-host GET throughput by about 30 % for durable reads and 5–10 %
  for cache hits; CRC-32C (10 %) and the kernel's socket copies are what
  remain.
- **Metadata hot keys (§6.17)**: zero transaction conflicts in every
  cell up to 32 tasks, with an evictor, and with two processes on one
  tier; the single FoundationDB process's commit latency is the limit
  (about 8,300 placement transactions per second from one process).
  Sharded counters are not needed; the capacity guarantee is unchanged.
- **Population queue (RFC-006 §5.3.1)**: bounded, node-aware, yields to
  reads, retries node errors, drains at shutdown; five test cases.

- **Multi-gateway (§6.18)**: on three hosts that each run a node and a
  gateway, 1 / 2 / 3 gateways deliver the same aggregate GET (560–744
  MiB/s) and PUT peaks at two gateways (295 MiB/s); the hosts' CPU is
  the bound, metadata (zero conflicts, under 250 transactions/s) and
  the network are not. Scaling needs gateways on their own hosts.
- **Two-host repeat and baseline (§6.19)**: durable GET +12–20 % with
  23 % less gateway CPU per GiB after the frame pass-through; baseline
  `lab-2host` recorded for the regression check.

## Not verified

- Profiles, the hot-key matrix and the multi-gateway runs are single
  runs per cell; the PUT profiles and the two-host repeat have three
  repetitions. PUT numbers on this environment move with the shared
  storage's write latency (R-48) and are compared only at like
  fdatasync latency.
- R-49 (TCP retransmissions on the gateway host under PUT) is observed,
  not explained; it is a virtual-network observation for the hypervisor
  owner.
- Metadata contention was measured against one FoundationDB process
  with up to two client processes; a multi-process cluster and many
  gateways were not measured.
- GET CPU after the frame pass-through was profiled on one host; the
  two-host CPU-seconds per GiB (§6.19) are the production-shaped number.
- The regression check has one baseline (`lab-2host`, recorded from
  §6.19) and has not yet caught a regression; its tolerances are a first
  choice to be tightened with history.
- CI is functional only, on self-hosted Linux runners; macOS is a
  development platform without CI coverage; GitHub-hosted runners remain
  refused for billing reasons on this account.
- The storage node's read path (buffer per shard, file-to-user copy) and
  CRC placement are unchanged (RFC-009 §9).
