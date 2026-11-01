# ADR-0011: Progress-based shard stream deadlines, retries, and bounded parallel reads

- Status: accepted
- Date: 2026-11-02
- Related RFC: RFC-009 §7–§9, RFC-004 §5.6, RFC-008 §9

## Context

The Phase 2.1 fault runs (`docs/verification/network-faults.md`) showed
that a single per-RPC timeout on multi-MiB shard streams turns a slow but
moving transfer into a failure (10 % loss, 50 ms delay with jitter) and
that the gateway could not tell a stalled node from a slow link. The
three-host matrix showed that a GET streamed one chunk at a time. Both
were recorded as R-47 and as RFC-009 §9 candidates.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| Two bounds, not one | A shard stream fails when no frame has moved for `progress_timeout` (stalled) *or* when `base_deadline + bytes / min_rate` passes (progressing but too slowly). Unary RPCs keep a plain timeout. Longer timeouts alone were rejected: they only move the cliff. | `transport.rs::stalled_shard_stream_…`, `too_slow_shard_stream_…`; R-47 matrix in RFC-009 §7 |
| Progress is a frame on the wire | The sender hands frames to the HTTP/2 transport through a one-slot channel and stamps progress when a frame is taken; the receiver stamps progress per received message. No frame is buffered ahead of flow control (backpressure stays with HTTP/2). | `client.rs::put_shard_raw`, `get_shard_raw` |
| Cancellation | Dropping the guarded future resets the HTTP/2 stream; the storage node commits a shard only after the complete stream, so a cancelled upload stores nothing. | `too_slow_shard_stream_hits_the_absolute_deadline` (shard count 0) |
| Retries | Transport-class failures (unavailable, deadline, cancelled, aborted, unknown) are retried up to `max_attempts` with exponential backoff after dropping the cached channel. Node decisions (epoch fence, not found, corrupt, invalid argument) are never retried. `put_shard` is idempotent on the node (same chunk ID, same bytes, same epoch), so a retry after a lost reply commits once. | `transient_reset_on_put_shard_is_retried_and_commits_once`; `delayed_ack_duplicate_and_reordered_put_shards_commit_once` |
| Slow-client detection | The storage node aborts a `PutShard` stream whose client sends no frame for `stream_progress_timeout`, so a stalled gateway cannot hold a stream open indefinitely. | `service.rs::put_shard` |
| Defaults from the matrix | `from_timeout(t)`: progress timeout `t` (not `t/2`: at 10 % loss TCP's retransmission backoff leaves gaps longer than 5 s, and a shorter timeout restarted transfers that would have finished), deadline `t + bytes / 256 KiB/s` (a deadline without a rate term failed every transfer on an 8 Mbit/s link), 3 attempts, 100 ms backoff. | RFC-009 §6.8 (`old`, `new`, `newb`, `newc`, `final`) |
| Lease follows progress | The gateway renews the pending generation's lease every third of the lease while the client's bytes arrive and while a chunk's shard uploads are being attempted, and chunk appends renew as well; a stalled upload fails within the progress timeout and retry budget, after which renewal stops and the lease runs out. Without this, retries that succeed on a lossy link still lost the PUT to an expired or collected generation. | `transport.rs::slow_upload_renews_its_lease_and_commits`; RFC-009 §6.8 |
| Durability unchanged | `m_ack = m` and atomic visibility are untouched: a retry only repeats the same shard write; a PUT whose shard cannot be written after every attempt still fails explicitly. | fault runs: verify passes with 0 mismatches |
| Counters | `RpcStats` (stalled, deadline exceeded, unary timeouts, retries, exhausted) are exported by the gateway so that a run can tell *why* a request failed. | `--metrics-listen`, `summarize_faults.py` |
| Bounded parallel chunk fetch | A GET keeps up to `read_parallelism` chunk reads in flight ahead of the chunk being streamed, delivered in order through a one-slot channel; buffered bytes per GET are bounded by that many chunks; a dropped body aborts in-flight reads; an error on a prefetched chunk ends the stream with an error after the bytes before it. Unlimited prefetch was rejected (memory). | `parallel_read.rs`; every suite runs with parallelism 3 |

## Alternatives rejected

- One longer per-RPC timeout (moves the failure point, cannot distinguish
  stall from slowness).
- Idle timeout only (an infinitely slow stream never fails).
- Retrying node decisions such as the epoch fence (would mask membership
  changes).
- Spawning one task per chunk without a bound (memory proportional to
  the object).

## Consequences

- `NodeClients::new(timeout)` derives a policy (`RpcPolicy::from_timeout`);
  services that need other values use `with_policy`.
- A PUT on an impaired link now takes up to `max_attempts × deadline`
  before failing; the R-47 matrix measures the trade.
- `ClusterConfig.read_parallelism` is part of the gateway configuration;
  test clusters use 3 to exercise ordered delivery everywhere.
