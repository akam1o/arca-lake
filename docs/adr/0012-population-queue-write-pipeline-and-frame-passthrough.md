# ADR-0012: Population queue, pipelined chunk placement, and frame pass-through

Status: accepted (Phase 2.3)

## Context

Phase 2.2 left three measured problems (RFC-009 §6.8–§6.13): PUT
throughput varied up to 2.7× between repetitions of one configuration
(R-48); the gateway spent 37 % of its GET CPU in `memcpy` and another
8 % in the kernel zeroing freshly mapped pages (§6.11, §6.15); and cache
populations were bounded only by one semaphore per gateway, with no
limit per node, no queue, no priority over durable reads and no
shutdown. Phase 2.3 instrumented the PUT path per phase on the gateway,
the storage node and the store (RFC-009 §6.14) before changing anything.

## Decisions

| Item | Decision | Evidence |
| --- | --- | --- |
| R-48 is fdatasync latency on shared storage | The per-phase profile showed every disk-touching phase (node fdatasync, FoundationDB commit, metadata append) 10× slower in a slow repetition while the gateway's own phases were unchanged; a synchronous write load on a *different* VM reproduced it. The cause is outside the code; the code's sensitivity to it is what Phase 2.3 reduces. | RFC-009 §6.14, `results/putprofile*` |
| Pipelined chunk placement | A PUT keeps up to `write_pipeline` (default 2) chunk placements in flight while the next chunk is read and encoded, so one group commit on a node does not idle the request. Records are collected in chunk order; the first failure aborts the rest (a cancelled stream stores nothing, ADR-0011); the lease is renewed while waiting; memory per PUT is bounded by that many chunks plus their shards. Commit still waits for every chunk: Durable ACK and atomic visibility are unchanged. | `write.rs::collect_placement`; RFC-009 §6.16 |
| Frame pass-through on GET | `GetShard` returns the frames as received (slices of the transport's receive buffer); durable and cache reads hand the data shards' frames through in chunk order with the padding trimmed, the CRC runs over the frames, and the HTTP body streams them. Only the reconstruction path (a data shard missing), population, repair and migration assemble a contiguous chunk. | `durable.rs::read_coded`, `read.rs`; RFC-009 §6.15 |
| Population queue | Populations go through a bounded FIFO (`population_queue_depth`), a worker pool (`max_populations_in_flight`) and per-node permits taken in node order (`populations_per_node`); overflow drops the population and counts it (backpressure to the miss path, which never waits); node errors are retried with backoff, metadata and corruption errors are not; a population yields up to 200 ms to shard reads in flight on its own nodes and up to 50 ms to reads anywhere (durable reads first, no starvation); shutdown lets running attempts finish and drops queued ones, counted; the dedup marker is released on every exit path. A failed population still only counts. | `popqueue.rs`; `population_queue.rs` (5 cases); RFC-006 §5.3 |
| Phase timing is permanent | The PUT phase histograms (gateway, node, store), the queue counters and the metadata transaction counters stay in the binaries and in `/metrics`: the next regression is diagnosed from them, not from a new instrumentation round. | `arca_gateway_put_phase_seconds`, `arca_node_phase_seconds`, `arca_gateway_meta_txn_*` |
| Performance regression is checked per environment | `regress.sh` + `compare.py` with baselines keyed by an environment label; CI runs the functional suites only. | `docs/verification/performance-regression.md` |
| Artifacts are sanitized by policy | `docs/security/public-artifacts.md` defines the boundary; `sanitize.py` enforces and verifies it before commit and in CI. | `secret scan` job |

## Alternatives rejected

- Deeper write pipelines by default (4): the measured gain over 2 is
  within noise on this environment and the memory bound per PUT doubles.
- A buffer pool for the GET path: once the frames are passed through,
  the remaining copies are the kernel's (socket to user space) and the
  storage node's file read, which a pool does not remove.
- Per-node population limits enforced on the node: the gateway already
  knows which nodes a population writes to, and a node-side limit would
  need a new RPC and a way to refuse without failing the stream.
- Strict priority (populations wait for all reads to finish): starves
  the cache under constant read load; a bounded yield gives reads the
  first turn without that.

## Consequences

- `ClusterConfig` gains `write_pipeline`, `population_queue_depth`,
  `populations_per_node`; the CLI exposes them.
- The gateway's memory per PUT grows by one chunk plus its shards at the
  default pipeline depth; the population queue holds one chunk per
  queued entry (8 × chunk size at the default depth).
- `GetShard` callers that need a contiguous buffer concatenate once
  (`concat_frames`); `read_chunk` returns frames.
