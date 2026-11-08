# ADR-0021: Chunk-level write charging, admission terms, shared file system refusal, recent-trend recovery

Status: accepted (Phase 3.7)

## Context

Phase 3.6 (ADR-0020) left R-80 (nodes sharing a file system defeat
per-node admission), R-81 (copies and PUTs without a length undercharged
by the throttle), R-82 (a full member stops its group while others have
room), R-83 (throttling held writes far below the offered load and lifted
slowly), R-84 (refused large bodies) and R-85 (admission state in the
leader's memory) open.

## Decision

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Where client bandwidth is charged | Each chunk, in the write pipeline shared by PUT, UploadPart and both copies, before its shards are written, once per chunk; a header check refuses early without charging; a throttled chunk waits at most 2 s | the declared length (absent or untrusted; copies have none); reserving the whole object up front (unbounded for streams) |
| Admission across leadership changes | The record carries the fencing token and a generation; a new term restarts the state machines at the published state and keeps it as a floor until every member has reported (or the staleness bound passed); gateways ignore lower tokens and age records by their own clock at first sight | persisting the state machines (a stale leader's view would be inherited as authority); a new leader starting from `normal` (relaxes on partial reports) |
| Shared file systems | Detected from a per-node capacity domain (device number scoped to the kernel boot id; overlay/FUSE/network flagged uncertain); conflicts (no fitting allotments) leave placement unless `--allow-shared-filesystem`; `admin check-config` | supporting shared file systems by a cluster-wide reservation (a metadata hot path for every shard write); trusting operators (the soak layout itself shared one disk) |
| Leaving a throttle | Worse on the long trend, better on the recent trend; adaptive (AIMD) throttled rate between the formula and 8 times it | a shorter long window (bursts throttle); a fixed formula (ratcheted down in Phase 3.6 Test A) |
| Group choice | Room per group published; gateways prefer the most room | weighted random (still sends writes to the fuller groups); rebalancing existing data (not in this phase) |
| Refused large bodies | Header-time refusal, then a bounded drain (16 MiB / 10 s) before the 503 | draining without bound; resetting the connection (clients see an I/O error) |

## Consequences

- Every client write path is throttled by what it writes; the node-side
  reserve stays the hard limit.
- A leader change cannot relax admission on partial information; a
  record that stops changing still throttles and then refuses.
- Clusters whose nodes share a file system must give each node an
  allotment (test clusters do: 2 GiB) or run with
  `--allow-shared-filesystem` (development only).
- Large writes compete chunk by chunk under a throttle: a multi-chunk
  write can be refused part way when many writers share a low rate.
