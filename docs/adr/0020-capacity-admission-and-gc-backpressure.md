# ADR-0020: Capacity admission at the storage node, tier backpressure from the controller, multipart parts retired with their root, cache deletes by token

Status: accepted (Phase 3.6)

## Context

Phase 3.5 left R-64 open: a sustained overwrite or delete rate above what
GC reclaims filled the devices (multipart objects of 2 × 1 MiB filled the
soak cluster's disks while GC collected about 14 generations/s), and
nothing limited writes before the devices were full. R-73 (cache deletes
not conditional) was also open. Phase 3.6 measured GC by stage
(`arca_controller_gc_stage_seconds`, the `gc_bench` workloads on
in-memory metadata and FoundationDB, on tmpfs and on disk-backed nodes,
with and without a background PUT load) before changing anything.

What the measurements showed:

- Metadata is not the bound. On FoundationDB a generation costs about 3
  transactions (verify 2–4 ms, finish 3–7 ms, root step 1–2 ms); a
  10 000-part upload completes in 2 transactions and is collected in
  about 3 transactions per part generation, with no conflicts and about
  2 MiB of controller heap.
- DeleteShard is a tombstone append that waits for the node's group
  commit: 1–20 ms on the Linux validation hosts' disks, about 100 ms on
  macOS (`F_FULLFSYNC`). With 32 generations in flight GC collected
  1 000–6 600 generations/s on the Linux hosts, 85–450/s for 16–64 MiB
  objects (one delete per chunk shard).
- The multipart slowness of the soak was not throughput. A completed
  multipart object's parts were retired only when GC collected its root,
  *as of that moment*: they then served a second full grace period. Every
  multipart object therefore kept its data for two grace periods (2 × 300 s
  in the soak), and a controller pass stopped as soon as a batch came
  back short, so the parts waited for the next pass too. At 120 retired
  generations/s that is about 70 GB of data held; the devices filled, and
  once they were full the tombstone appends failed (ENOSPC), so GC
  stalled on its own deletes (`deferred: 775` in the last soak pass).

## Decision

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Retirement time of a completed upload's parts | The root's retirement time: a reader reaches the parts only through the root, so the root's grace covers them; parts never listed in the completion have been unreachable since it. A pass runs another batch when a root step retired parts | `now` (a second grace, the R-64 multipart cause); deleting parts in the root's transaction (unbounded transaction for 10 000 parts) |
| GC parallelism | unchanged (32 generations at once, batches of 1 000) | raising it before the bottleneck was known (the cause was the second grace, not throughput) |
| Lazy tombstones / batched DeleteShard | not done | the delete RPC was not the bound on the Linux hosts; a delete that is not durable before metadata is removed can resurrect a shard after a crash (it would be an orphan for reconciliation, but the extra state is not needed) |
| Linearization point of physical capacity | Node-side admission of every PutShard, before the body is taken: the write is admitted only if the device keeps its class's floor after it, counting every write admitted and not yet ended; the bytes are held until the RPC ends (stored, failed or cancelled). Nothing is persisted: a crash or restart of a gateway or node leaves nothing held | metadata reservation records (a hot key per device or lease records per write; leases and their recovery after a gateway crash or CommitUnknown add exactly the states the problem asks to avoid); gateway-side accounting only (several gateways would admit the same bytes) |
| Write classes and floors | `PutShardHeader.write_class`: client, migration and cache writes stop at the reserve; repair (member rebuild, stripe repair, cache shard rebuild) goes on to the repair floor; deletes are never admitted against capacity (a tombstone is what frees space) | one floor for all (repair could not run on a full device); refusing deletes on a full device (the soak's GC stall) |
| Reserve size | `max(2 × segment_max + 64 MiB, 2 % of the device)`, at most a quarter of it; repair floor `segment_max + 16 MiB` (one compaction output and a rollover); `--reserve-bytes` overrides | a percentage only (too small on small devices for a compaction output, too large on large ones) |
| Capacity refusal on the wire | `RESOURCE_EXHAUSTED` with "insufficient capacity"; ENOSPC and EDQUOT from the store map to it; the client does not retry it and keeps the connection; the gateway tries the next group | `INTERNAL` (the gateway forgot the connection and the client saw an internal error) |
| Tier backpressure | The controller leader evaluates one state machine per tier every health pass: `normal`, `warning`, `throttled`, `write_rejected`, `recovery_only` from the usable free fraction (20 / 10 / 5 / 2 % of total minus reserves), the time to full at the tier's free-space trend or the GC backlog's growth (measured over at least 5 minutes; throttle within 1 h, warn within 4 h), groups whose fullest member has room for fewer than 64 maximum shards (left out of placement; all of them out: `write_rejected`), and report staleness (no fresh report: `recovery_only`; some stale: `warning`). A worse state applies at once; a better one needs its thresholds 3 points better for the dwell time (60 s) and moves one step at a time | a single free-space threshold (no warning before refusal, no reaction to a growing backlog); instantaneous rates (a burst of seconds throttled the test clusters) |
| Publication | One record `("al","admission")` written by the leader each health pass (TTL 30 s; gateways alive; per tier: state, throttle rate, excluded groups, free per mille, reason). Gateways read it with the cluster state every 2 s | per-request reads (a hot key); pushing to gateways (another channel to secure and fence) |
| Gateway enforcement | `throttled`: new data at the tier's rate (GC's recent reclaim rate in client bytes, at least 1 MiB/s) split over the gateways, a token bucket that starts empty and may go one request into debt; `write_rejected` / `recovery_only`: PUT, UploadPart, copies and CreateMultipartUpload refused; deletes, aborts, CompleteMultipartUpload, bucket operations and reads go on. A record unchanged for its TTL counts as at least `throttled`, for 5 min as `write_rejected`; no record at all admits (a controller that predates it), the nodes' reserve still holds | refusing everything (reads and deletes are what recovers the cluster) |
| S3 error | `503 SlowDown` with `Retry-After` (1–30 s throttled, 30 s rejected, 60 s recovery-only, with up to 25 % jitter); the message says throttled, refused or no group with capacity. Placement failures for other reasons and metadata failures stay `503 ServiceUnavailable`; authorization stays `403` | `507 Insufficient Storage` (not an S3 error code; SDKs do not retry it); `ServiceUnavailable` for capacity (indistinguishable from an outage) |
| Compaction without room | a segment whose live bytes (plus 1/64 and 1 MiB) do not fit in the free space is skipped and counted (`skipped_no_space`); an out-of-space error during compaction leaves the store as it was | compacting regardless (ENOSPC mid-copy wastes the pass) |
| Cache deletes (R-73) | The tokens the nodes return for a cache copy's shard writes are recorded next to the placement (`("al","g",gen,"xt",index)`, removed with it); eviction, invalidation and a failed population's cleanup delete by them, a repair records the tokens of what it rewrote. A newer copy under the same key is kept (ABORTED); a node that restarted refuses tokens of its former store epoch and reconciliation removes the copy. Placements without tokens (recorded before) and nodes that hand out none get no delete: their copies are left to reconciliation (an unconditional delete could remove a copy populated meanwhile that its new placement names; found by the cross-model review) | a field in `CachePlacement` (postcard records are not self-describing: a schema change for every reader); an epoch per cache copy (the store already hands out per-record tokens) |

## Consequences

- Retired multipart data is held for one grace period, like any object.
- A cluster cannot be filled past its nodes' reserves by client, cache or
  migration writes, however many gateways write; repair and compaction
  keep room. Deletes and GC work on a full cluster.
- Writes slow down and are then refused before the nodes have to refuse
  them; clients see a retryable `SlowDown` with `Retry-After`. Reads,
  deletes and completions are never refused for capacity.
- A gateway that answers before reading a refused PUT's body may make
  the client see a broken connection instead of the `503` (the request is
  still not applied; SDKs retry it).
- New metrics: `arca_node_capacity_bytes{kind}`,
  `arca_node_admission_admitted_total`, `_admission_refused_total{class}`,
  `arca_controller_backpressure_state`, `_capacity_bytes{kind}`,
  `_gc_backlog_bytes`, `_gc_backlog_physical_bytes`,
  `_gc_reclaim_bytes_per_second`, `_admission_excluded_groups`,
  `_gc_stage_seconds{stage}`, `_gc_transactions_total{kind}`,
  `_gc_reclaimed_bytes_total`, `_gc_roots_collected_total`,
  `_gc_parts_retired_total`, `arca_gateway_admission_*`,
  `_node_capacity_refusals_total`, `_placement_capacity_failures_total`.
- The admission record carries an unchanged-since-v1 encoding (new type);
  the cache token record is a new key: neither changes an existing
  record, and older gateways ignore both.
