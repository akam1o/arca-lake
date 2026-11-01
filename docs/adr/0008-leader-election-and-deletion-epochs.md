# ADR-0008: Leader election on the metadata store, deletion epochs, verified repair completion

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-011, RFC-008 §6–§7, RFC-004 §5.13

## Context

Phase 1.2 made the controller a long-running process and had to decide how
leadership is established, how a stale leader is kept harmless, what makes
a physical shard deletion safe given that metadata transactions and node
RPCs are not atomic together, and when a repaired group may be called
healthy. Each decision is backed by tests on the in-memory backend and,
where noted, on a three-process FoundationDB cluster.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Leader election | One leader record in the metadata store with holder, monotonically increasing token, and lease; acquisition, renewal, and release are single transactions. No separate consensus component. | `controller.rs::exactly_one_leader_…`, `fdb_failover.rs` step 5; RFC-011 §5.2 |
| Fencing | Every transaction a leader issues reads the leader record and fails unless holder and token match (`FencedKv`). Safety does not depend on the old leader noticing anything or on clocks. | `fence.rs` unit test, `controller.rs::a_fenced_old_leader_…`; RFC-011 §5.3 |
| Deletion epochs | Every `DeleteShard` and repair `PutShard` carries the group epoch of the fenced transaction that decided it; nodes reject lower epochs; a membership change fences the replacement before committing. This closes the window between a reconciliation decision and its RPC when a node is re-added. Compared alternatives: a `Deleting` reservation state in metadata (adds a state for no extra guarantee, since `Retiring` is already terminal), node-side tombstones with deletion epochs (more node state, same effect). | `repair_completion.rs::delete_decided_under_an_old_epoch_…`; RFC-008 §6.4, §6.6 |
| TOCTOU argument for GC | No transition leaves `Retiring`; chunk IDs are never reused; begin_put commits the header before any shard is written. Therefore a verify-then-delete sequence cannot delete a shard that metadata later accounts for, except through membership changes, which the deletion epoch covers. | RFC-008 §6.6; model test I2/I3/I6; `gc_safety.rs` |
| Paged listing | `ListShards` and `VerifyShards` page by key cursor; pages are snapshots; walkers are k-way merges with bounded memory; consumers are idempotent so a missed or stale entry costs a re-run, never correctness. | `paging.rs`; RFC-008 §6.5 |
| Repair completion | `Active` requires every member to list every chunk with equal lengths and the replacement's shards (optionally all members') to pass `VerifyShards`; rebuilt chunks are validated against recorded scheme, `k+m`, placement and CRC-32C; a failed check clears the cursor. | `repair_completion.rs`; RFC-008 §7 |
| Explicit failure without quorum | `FdbKv` sets a transaction timeout and does not retry timeouts; a store without quorum fails every operation within the timeout. | `fdb_failover.rs` step 4 |

## Alternatives rejected

- Raft or another consensus protocol inside the controller.
- Trusting lease expiry alone (clock-dependent) instead of a per-transaction
  token check.
- Completing a repair when the replacement lists the expected chunk IDs
  (misses corrupt and truncated shards and concurrent membership changes).
- Age-based orphan deletion.

## Consequences

- `KvError::Fenced` is part of the backend contract and is never retried.
- `GroupRecord.repair_cursor` is set at the membership commit so that any
  controller can resume; a `Repairing` group without a cursor is an
  operator case and is logged.
- The three-process FoundationDB configuration used in CI (`double`,
  three coordinators) tolerates one process; this is a test configuration
  and not an availability statement for deployments.
