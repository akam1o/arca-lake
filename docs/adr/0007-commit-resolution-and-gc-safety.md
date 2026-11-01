# ADR-0007: Commit resolution, reference-checked garbage collection, and repair completion

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-003 §5.8, RFC-004 §5.7, RFC-008

## Context

Phase 1 hardening had to decide how the system behaves when a metadata
commit's outcome is unknown, what evidence garbage collection requires
before deleting a shard, and when a repaired stripe group may be declared
healthy. Each decision is now backed by tests on the in-memory backend and,
for the first two, on a real FoundationDB cluster (`faults_fdb.rs`,
`e2e_fdb.rs`).

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Unknown commit outcome | Never retry a non-idempotent recipe blindly. The gateway re-reads the generation (`resolve_commit`); committed is success; pending is retried a bounded number of times; then the generation is fenced by `abort_put`, whose transaction conflicts with a late commit. An abort that finds the generation committed turns the PUT into a success. Idempotent steps (`begin_put`, `append_chunks`) are retried. | `crates/metadata/tests/commit_unknown.rs`; testkit fault suite on both backends; RFC-008 §5.2 |
| GC evidence | A shard is deleted only when one transaction shows metadata does not account for it: the generation is `Retiring` and unreferenced (generation-driven), or no pending generation and no chunk record place it here (reconciliation). Elapsed time is a guard for the no-generation case only. | `gc_safety.rs`, `transport.rs::lost_reply_…`; RFC-008 §6 |
| Repair completion | A group returns to `Active` only when every member lists the same chunk IDs. Progress is a per-member cursor persisted after every chunk; the same command resumes; a concurrent membership change stops the running repair with `EpochChanged`. Repair deletes nothing; reconciliation reclaims former members. | `repair.rs`; RFC-008 §7 |
| Writes during repair | `Repairing` groups accept writes (the replacement receives new shards directly) so that a single-group deployment stays writable. | `concurrent_puts_and_gets_during_repair_…` |
| Fault injection scope | Metadata faults are injected by wrapping the store (`FaultKv`), transport faults at the node-client boundary (`NodeClients` hook). Both are test-only and work against real backends/nodes. Real-network behaviour remains unverified and is listed as such. | `crates/metadata/src/fault.rs`, `crates/protocol/src/client.rs` |

## Alternatives rejected

- Blind retry of `commit_put` on timeouts: double publication or double
  retirement after an actual commit.
- Age-based orphan deletion: turns clock skew into data loss.
- Marking a group `Active` as soon as the replaced member is rebuilt:
  misses a second membership change in flight.
- A tower middleware or a proxy process for transport faults: more code
  for the same observable effects at this stage; revisit when real-network
  tests are built.

## Consequences

- `KvError::CommitUnknown` is part of the backend contract; every backend
  must distinguish it from `Conflict` and `Backend`.
- `GroupRecord.repair_cursor` carries the member index (pre-release layout
  change; no migration because no deployments exist).
- `skipped_referenced` in the GC report is an invariant-violation counter
  and should be alerted on.
