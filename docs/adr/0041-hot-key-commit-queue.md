# ADR-0041: Per-key commit queue and conflict backoff in the gateway

Status: **accepted** (0.2.0 preparation, R-112). Adopted by the
pre-registered A/B ([plan](../verification/0.2.0-hot-key-plan.md),
[results](../verification/0.2.0-hot-key-results.md)): on one key through
four gateways SlowDown 55 % → 0.5–0.8 %, acknowledged PUT/s 1.6–1.7 ×;
acknowledged PUTs on one key wait longer (p50 about 200 → 450 ms, p99
about 0.8 → 1.7 s); 16 keys within 3–6 % (0.97 × PUT/s, 1.06 × p99).
That comparison is of the tag rebuild against the whole candidate, which
carries other 0.2.0 changes too: the gain is attributed to the queue by
mechanism only, the 16-key differences not at all, until the
pre-registered on/off control in one binary (plan §10) is reported. No
record, key, protocol or format change.

## 1. Problem

Concurrent writes to one key conflict in the metadata store: `commit_put`
reads the key's current pointer (`crates/metadata/src/ops/object.rs`), so
of the commits that overlap only one can win, and `transact` retries the
others immediately, up to 32 attempts (`crates/metadata/src/kv.rs`). With
64 writers on one key through four gateways, 54–55 % of PUTs ran out of
attempts and were answered `503 SlowDown` (about 65 acknowledged PUT/s of
about 145; [phase-4.1-r112.md](../verification/phase-4.1-r112.md)); the
metadata benchmark shows about 60 attempts per operation.

## 2. Decision

1. **A FIFO turn per (bucket, key) in each gateway.** PUT (and CopyObject,
   which uses the same write path), DELETE (with or without a version id,
   and each entry of DeleteObjects) and CompleteMultipartUpload wait for
   their key's turn before their commit transaction; at most one commit
   per key per gateway is in flight. Turns are granted in arrival order
   (`tokio::sync::Mutex`, which is fair). The body of a PUT is streamed
   before the turn is taken: only the commit is serialised.
2. **The turn is held until the outcome is decided**: through
   `commit_resolving` (an unknown outcome is resolved, retried or fenced
   under the same turn), the DELETE re-run and Complete's re-runs.
3. **Refusal only before any attempt.** A turn not granted within the
   wait bound W (default 2 s, `--commit-queue-wait-ms`), or a key whose
   queue already holds its maximum of waiters (default 256,
   `--commit-queue-max-waiters`), is answered `503 SlowDown`, and a PUT's
   pending generation is aborted (best effort; if the abort fails the
   lease expires it). No commit was attempted, so nothing is applied:
   SlowDown keeps its meaning (R-114). Once a commit attempt was made the
   turn is never abandoned on W.
4. **Jittered backoff between conflicts.** Conflicts with other gateways'
   commits remain. Queued commits' transactions wait between a conflict
   and the next attempt (full jitter: uniform in `[0, min(50 ms, 2 ms ×
   2^(n−1))]`); the attempt bound (32) and its `TooManyRetries` →
   `SlowDown` (definitely not applied: every attempt conflicted) are
   unchanged. The metadata crate gets the hook (`ConflictBackoff`,
   `transact_with_backoff`, `transact_idempotent_with_backoff`,
   `commit_put_with`, `delete_object_as_with`, `delete_version_with`,
   `complete_with`): every form of DELETE (single, version-specific, each
   DeleteObjects entry) and Complete back off as PUT does. It stays runtime-agnostic, the gateway supplies the
   timer.
5. **Bounded table.** At most 16,384 keys have an entry; an entry is
   removed when nobody holds or waits for its turn. With the table full,
   a commit to another key proceeds without a turn rather than being
   refused; its transactions still wait on the jittered backoff after a
   conflict (only the turn is skipped).
6. **Cancellation.** A turn and a waiting place are released when their
   future is dropped (client gone, task aborted); the pending generation
   is then left to its lease, as before.
7. **The store stays the authority.** The queue only avoids attempts that
   would lose; every commit still reads the pointer and FoundationDB's
   conflict check still decides order between gateways, after a turn
   bypass and across restarts.
8. **Off switch**: `--commit-queue off` restores 0.1.0's behaviour
   (immediate retries, no turns).

Counters on the gateway metrics endpoint:
`arca_gateway_commit_queue_{turns,waited,wait_us,refused_full,refused_wait,bypassed,conflicts,backoff_us}_total`
and the gauge `arca_gateway_commit_queue_keys`.

## 3. Alternatives

- **Fewer or slower retries only** (backoff without the queue): within a
  gateway the writers still compete; backoff spreads them in time but
  every loser still costs a transaction.
- **A cluster-wide per-key lock in the metadata store**: one more
  transaction per write and a lease to recover after a gateway dies; the
  store's own conflict check already gives the order.
- **Folding the GET's map-version read into the manifest transaction**:
  a GET-side cost, not the conflicts; the plan's fallback if this fails.

## 4. Consequences

- A write that would have been refused after about 550 ms of conflicts now
  waits its turn; its latency includes the commits ahead of it (bounded
  by W before the first attempt).
- A gateway's memory for the queue is bounded by the table (16,384 keys)
  and the waiters (each a waiting request already admitted).
- Writers on different keys are not affected (one map lookup per commit).

## 5. Tests

- `crates/gateway/src/cluster/commit_queue.rs`: bound, FIFO, wait bound,
  cancellation of a waiter and of a holder, cleanup, full table, off,
  backoff range.
- `crates/metadata/src/kv.rs`: the backoff runs between conflicts, never
  after the last.
- `crates/testkit/tests/it/hot_key.rs`: concurrent PUTs through one
  gateway all acknowledged; DELETE and Complete among them all succeed;
  conflicts on the key's commits within a gateway (off against on);
  several gateways on one versioned key with refusals (every acknowledged
  PUT exactly one version with its id, real-time order, no refused PUT
  visible, no pending generation left, table empty); unknown commit
  outcomes (`UnknownAfterApply`, `UnknownWithoutApply`,
  `TimeoutAfterApply`) resolved under the turn; every DELETE form
  (version-specific, DeleteObjects with and without a version id) backs
  off after a conflict. The R-112/R-114 tests in `commit_timeouts.rs` are
  unchanged.

## 6. Deviation from the plan

The plan's description of the approach (§4) named DELETE without a
version id among the queued operations. The implementation queues every
DELETE form: version-specific DELETE and each DeleteObjects entry also
take the key's turn (they can move the current pointer, so they contend
like the others), and, since the review, back off after conflicts too.
The A/B measured PUT only; the DELETE forms are covered by the tests
above, not by a measurement.
