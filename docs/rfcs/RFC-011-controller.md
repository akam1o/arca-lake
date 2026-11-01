# RFC-011: Controller

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: RFC-003, RFC-004, RFC-008
- Related ADRs: ADR-0007 (commit resolution, GC evidence, repair completion),
  ADR-0008 (leader election on the metadata store, deletion epochs)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

Garbage collection, reconciliation, repair, and health monitoring must run
continuously, from exactly one place at a time, and survive the death of
the process running them. A second controller must take over without an
operator, and a controller that was paused, partitioned, or slow must not
be able to undo the successor's work when it wakes up.

## 2. Goals

- A long-running process with leader election, lease, and a fencing token,
  using the metadata store's transactions as the only consensus.
- Schedulers for GC, reconciliation, repair, and health with retry and
  backoff; graceful shutdown; metrics; structured logs; a health endpoint.
- Safety that does not depend on the old leader noticing it lost the lease.

## 3. Non-goals

- Admin API beyond `arca-lake admin` (Phase 2).
- Multi-tenant or sharded controllers; one leader manages the cluster.
- Rate limiting of repair traffic (Phase 2).

## 4. Requirements

| ID | Requirement |
| --- | --- |
| C1 | At most one controller commits metadata changes at any time. |
| C2 | A controller that lost leadership cannot commit after the successor's first transaction, regardless of its clock. |
| C3 | Storage node RPCs of a stale controller cannot delete or overwrite a shard the current placement accounts for. |
| C4 | Interrupted repairs resume without operator input. |
| C5 | Failover completes within one lease after a leader dies. |

## 5. Design

### 5.1 Process

`arca-lake controller --cluster-file …` runs [`arca_lake_controller::process`]:
an election loop and, while leader, four scheduler tasks. Every metadata
transaction of a leader goes through `FencedKv` (§5.3). Storage node
clients are the same `NodeClients` the tests use, including their fault
hook.

### 5.2 Leader election and lease

One record `("al","leader") = { holder, token, lease_until }`.

- `try_acquire(holder, now, lease)`: one transaction; succeeds when there
  is no record, the lease expired, or the holder already holds it. The
  token increases on every change of holder and is kept on renewal.
- `renew`: one transaction; succeeds only if holder and token still match.
- `release`: expires the lease at once (graceful shutdown) so a successor
  does not wait.

Followers retry acquisition every `lease / 3`; the leader renews every
`lease / 3`. A transient store error during renewal does not drop
leadership by itself (flapping would be worse); the fence (§5.3) keeps
this safe. *Verified*: `controller.rs::exactly_one_leader_and_failover_after_the_lease`
(token 1 → 2 after a crash, 3 after a release), on FoundationDB in
`fdb_failover.rs`.

### 5.3 Fencing token (`FencedKv`)

Every transaction of a leader reads the leader record before committing
and fails with `KvError::Fenced` unless holder and token match. The read is
part of the transaction, so a leader change that commits between the read
and the commit makes the commit conflict and retry, where the check fails.
Consequences:

- C1 and C2 hold without any clock assumption: the successor's
  `try_acquire` and the old leader's commits are serialized by the store.
- A scheduler task that hits `Fenced` stops immediately; the election loop
  returns to follower.
- Cost: one extra read per transaction and a conflict with every renewal
  (every `lease / 3`), which `transact` retries.

*Verified*: `controller.rs::a_fenced_old_leader_cannot_commit_after_takeover`,
`gc_scheduler_collects_and_old_leader_gc_is_fenced`, `fence.rs` unit test.

### 5.4 Storage node RPCs of a stale leader (C3)

Node RPCs are not fenced by the leadership token; they are fenced by group
epochs (RFC-004 §5.4) and by the deletion-epoch rule (RFC-008 §6.4): every
`DeleteShard`/`PutShard` carries the group epoch under which the decision
was made in a fenced transaction, and a membership change fences the
replacement before committing. A stale leader's RPC therefore either acts
on a placement that is still current (and was decided correctly) or is
rejected. *Verified*: `repair_completion.rs::delete_decided_under_an_old_epoch_is_rejected_after_a_fence`,
`repair.rs::stale_epoch_writer_and_returning_old_member_…`.

### 5.5 Schedulers

| Task | Default interval | What it does | Resume after failover |
| --- | --- | --- | --- |
| gc | 30 s | `gc::run_once` (expire, verify, delete, finish) | Stateless: every pass re-reads the queues |
| reconcile | 5 min | `gc::reconcile_node` for every node × group, paged | Stateless |
| health | 5 s | `Health` RPC per node; `touch_node` on success | Stateless |
| repair | 15 s | Resume every `Repairing` group from its cursor `(member, chunk)`; with `--auto-repair`, replace members of `Active` groups whose node has not been seen for `unhealthy_after`, choosing a spare in another failure domain | The membership commit records the member being rebuilt (cursor at chunk 0), so a successor needs no other state |

Each task runs with exponential backoff (up to 8× the interval) after
errors and stops at once on `Fenced`. *Verified*:
`controller.rs::repair_interrupted_by_a_leader_crash_is_resumed_by_the_successor`,
`health_monitor_triggers_auto_repair_onto_a_spare_node`.

### 5.6 Observability and lifecycle

- `/metrics` (Prometheus text) and `/healthz` (JSON with role and token)
  on `--http`, served by a dependency-free HTTP/1.1 responder.
- Counters: leadership acquisitions and losses, GC passes, collected
  generations, deleted shards, `skipped_referenced` (invariant violations),
  repairs started and completed, chunks reconstructed, task errors; gauges
  for role, token, node counts.
- `tracing` logs, text or JSON lines (`--log-json`).
- SIGTERM/ctrl-c: stop schedulers, wait for the running pass, release the
  lease, exit.

## 6. Failure scenarios

| Scenario | Behaviour | Verified by |
| --- | --- | --- |
| Leader process dies | Followers acquire after the lease; token increments; `Repairing` groups resume from their cursor | `controller.rs`, `fdb_failover.rs` step 5 |
| Leader paused longer than the lease, then resumes | Its next transaction is fenced; it drops to follower; node RPCs in flight are epoch-fenced | `a_fenced_old_leader_cannot_commit_after_takeover` |
| Store unavailable during renewal | Renewal retried until the lease would have expired; fenced transactions fail; when the store returns, either the lease is still ours or a successor exists | design argument; `fdb_failover.rs` steps 6–7 exercise store loss during repair and GC |
| Store loses quorum | Every transaction fails after the configured timeout (`FdbKv` transaction timeout); nothing hangs; nothing is published | `fdb_failover.rs` step 4 |
| Two controllers started at once | One acquires; the other is a follower | `exactly_one_leader_…` |

## 7. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Own consensus (Raft) in the controller | A second consensus system next to the metadata store; the store already provides serializable transactions. |
| Leadership checked only at pass start | A pass that spans a lease boundary could commit after a takeover; per-transaction fencing closes it. |
| Leadership token in node RPCs | Nodes would need to learn the leader record; group epochs already fence the only dangerous RPCs. |
| External lock service | Another dependency with its own availability; the metadata store is already required. |

## 8. Verified and not verified

Verified: everything with a test name above, on the in-memory backend and
(election, failover, repair/GC across process loss) on a three-process
FoundationDB cluster, both in CI (bridge network, test on the runner host)
and in containers on a developer machine. Observed in those runs: six
concurrent PUTs all committed across the loss of one process; with two of
three coordinators killed, PUT and GET failed with an explicit
"transaction timed out" error after the configured 5 s and nothing hung;
the repair that was running when a process died stopped with a real
`commit_unknown_result` from FoundationDB and the re-run completed; a GC
pass during a process loss collected what it could and deferred the rest.
The test cluster declares a distinct machine and zone per process so that
`double` redundancy can place replicas; this is a test topology.

Not verified: leader change while a repair's node RPC is in flight on a
real network; two controllers partitioned from each other but both
reaching the store (the fence handles it by construction, the test does
not exist); clock jumps on the leader (safety does not depend on them,
liveness may); long-term behaviour of renewal conflicts under heavy
controller write load.

## 9. Open questions

- Should `skipped_referenced > 0` halt the controller?
- Admin API and operator commands for resuming or cancelling repairs.
- Repair rate limiting and prioritization.

## 10. Acceptance criteria

- `controller.rs` and `fdb_failover.rs` pass in CI.
- A controller survives a `SIGKILL` of its predecessor with no operator
  action (resume test).
- Metrics and health endpoints documented in the operations guide (Phase 2).
