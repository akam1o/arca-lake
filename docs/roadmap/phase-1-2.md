# Phase 1.2: Distributed operations and failover

> **Historical phase plan.** The statuses and outstanding tasks below
> record this phase, not the current system. Later work may supersede them.
> See the [roadmap index](README.md) for active priorities and
> [known limitations](../guide/known-limitations.md) for current boundaries.

Original status: **first iteration done** (see "Not verified" for what remains).
Phase 1.2 makes the cluster operable as several processes on several hosts:
a controller process with leader election, paged node APIs, repair
completion that is a verified fact, garbage collection whose safety does
not depend on timing, and failover tests against a multi-process
FoundationDB cluster.

## Progress

| Task | State | Notes |
| --- | --- | --- |
| 1.2.1 GC safety re-verification | done | TOCTOU argument and alternatives in RFC-008 §6.6; deletion epochs §6.4 with the replacement fence acknowledged before membership commits; model test invariant I6; `gc_safety.rs`, `repair_completion.rs::delete_decided_under_an_old_epoch_…` |
| 1.2.2 Repair completion | done | Existence, lengths, `VerifyShards` of the rebuilt member (optionally all), chunk validation against recorded scheme/placement/CRC, cursor cleared on a failed check; RFC-008 §7; `repair_completion.rs` (5) |
| 1.2.3 `ListShards` paging | done | Cursor + limit + payload lengths, `VerifyShards`, k-way merge walker with O(page × members) memory; semantics in RFC-008 §6.5; `paging.rs` (3) |
| 1.2.4 Controller process | done | `arca-lake controller`: election with lease and fencing token on the metadata store (`FencedKv`), GC/reconcile/health/repair schedulers with backoff, auto repair, graceful shutdown, `/metrics` and `/healthz`, tracing logs; RFC-011; `controller.rs` (5) |
| 1.2.5 FoundationDB failover E2E | done (CI and local containers) | Three fdbserver processes with distinct zone localities (`double`, three coordinators): process loss under concurrent PUTs (all committed), coordinator loss, quorum loss with explicit failure in 5 s, leader failover, repair stopped by a real `commit_unknown_result` and completed on re-run, GC across process loss; `fdb_failover.rs`, CI job `fdb-failover`, `deploy/dev/fdb-failover.sh` |
| 1.2.6 Multi-host / network namespace verification | not run | The development container runtime lacks `sch_netem`; a Linux host or two hosts are needed (R-40). Transport faults remain simulated at the client boundary (`transport.rs`) |
| 1.2.7 Design documents | done | RFC-003 §5.9–5.10, RFC-004 §5.13, RFC-008 §6.4–6.7 and §7, RFC-011, ADR-0008 |

## What the new tests verify

| Suite | Cases | Verified behaviour |
| --- | --- | --- |
| `paging.rs` | 3 | Page bound, strict order, no duplicates, resume from a cursor, same set for any page size, `VerifyShards` pages; walk under concurrent PUT/DELETE/GC lists every stable shard once; GC and repair converge with 7-entry pages |
| `repair_completion.rs` | 5 | Corrupt rebuilt shard blocks completion and is rewritten on re-run; corrupt source shard is not propagated (recorded CRC) and blocks full verification; an unreachable member blocks completion; a delete decided under an old epoch is rejected after a fence; an unrecoverable chunk writes nothing |
| `controller.rs` | 5 | Exactly one leader; failover after the lease with token increment; release on graceful shutdown; a fenced old leader cannot commit (raw transaction and recipe); GC scheduler collects and a stale leader's GC is fenced; an interrupted repair is resumed by a fresh controller; health monitor replaces a dead member onto a spare |
| `fdb_failover.rs` | 1 scenario, 7 steps | On a three-process FoundationDB cluster: see 1.2.5 |
| `metadata/tests/model.rs` | I1–I6 | Shard attribution matches the model for every placed chunk, in its group and elsewhere, and for unrecorded indexes |

## Not verified

- Real network impairment (resets, loss, latency, partitions) between
  hosts; node, controller, and gateway restarts under it. Only simulated
  faults at the client boundary exist (RFC-008 §9, R-35, R-40).
- FoundationDB configurations other than `double` with three
  coordinators; multi-host FoundationDB; coordinator changes under load.
- Leader change while a repair's node RPC is in flight on a real network;
  two controllers partitioned from each other (fence handles it by
  construction, no test).
- Repair rate limiting; more than one repair at a time.
- Performance of paging, verification, or fencing under load.

## Acceptance criteria for closing Phase 1.2

1. `fdb-failover` passes in CI on every push.
2. A two-host (or Linux network namespace) run of PUT/GET/repair under
   `tc netem` loss and latency and an `iptables` partition, with results
   recorded as measured on that environment.
3. Operations guide (RFC-013) covering controller deployment, metrics, and
   FoundationDB redundancy requirements.
