# Phase 4 roadmap

Phase 4 turns the Phase 3 system into one whose performance and scale
limits are measured and then raised. Phase 4.0 measured; Phases 4.1 and
4.2 act on what it found. Nothing of 4.1 or 4.2 is implemented yet.

| Sub-phase | Scope | Completion criteria | Status |
| --- | --- | --- | --- |
| 4.0 Scalability baseline | benchmark framework (`arcabench`, `nodebench`, `metaops`, `bench-cluster.sh`), validity rules, object count 10³–10⁶, gateway, FoundationDB processes, storage node, concurrency and tail, background passes, metadata operations, profiles, failures under load, R-108 heap, three limited changes with A/B (gateway `TCP_NODELAY`, `--access-records`, `--admission-horizon-secs`), controller pass metrics, two reviews in two rounds | Y1–Y16 with evidence (`docs/verification/phase-4.0-scalability.md`) | delivered with limitations (see the report): 10⁷–10⁸ objects through S3 estimated, not measured (10⁷ metadata-only measured if noted there); one 4-vCPU host for nodes and gateways; virtual disks |
| 4.1 Metadata path and background passes | below | below | not started |
| 4.2 Scale-out validation and capacity efficiency | below | below | not started |

## Phase 4.1 — priorities (from measured bottlenecks)

Ordered by measured impact on this environment and by risk; each item
names its acceptance measurement. Gains not yet measured are estimates.

1. **Access records (ADR-0034, R-109).** Measured: about one
   FoundationDB commit per GET beyond 10⁴ objects; GET 4 KiB +22 % with
   the records off. Do: bounded per-gateway queue, batched writes, the
   generation checked in the same transaction (closes the orphan race),
   `last_ms` of unread generations from their id, recording only where a
   tiering policy exists. Accept: commits per GET ≤ 0.1 at 10⁵ objects;
   GET within the spread of `--access-records off`; tiering tests.
2. **Background passes at object scale (ADR-0033, R-110).** Measured:
   reconciliation 3,566 s per pass at 10⁶ objects (interval 300 s),
   reclaim pass about 3–4 ms per waiting retirement. Do: batched
   read-only judgments per listing page, bounded parallelism, settle time
   from the measured cycle, a "pass running since" gauge. Accept: pass
   time at 10⁶ on the Phase 4.0 layout, the same deletes on the Phase 3.14
   simulator and the deferred-reclamation tests.
3. **The per-request map-version read.** Measured: one of the ~3
   transactions of every GET and of the ~4 of every PUT; the gateway's
   FoundationDB network thread is at ~80 % of a core at 3,000 GETs/s. Do:
   cache the version with a watch or a bounded staleness that the node's
   epoch check already backs (a node rejects a stale epoch). Needs an ADR
   (correctness of a cached map). Accept: transactions per GET and GET
   ops/s, same layout.
4. **Small-object capacity (R-111).** Measured: about 24 KiB of node
   capacity per 1 KiB replicated object (4 KiB alignment and a 4 KiB
   tombstone reserved per record); the tier refused writes at 765,134
   objects with 0.59 GB live per node. Do (4.1, no format change): size
   the delete reserve by the real tombstone record size where the
   alignment allows, or bound it by what compaction can always free;
   design note on packing small objects (format change: 4.2). Accept: the
   object count at `write_rejected` on the same allotments.
5. **Admission horizon on small clusters.** Measured: with the 3,600 s
   horizon a 4 × 6 GiB cluster was throttled at 32 of 2,000 1 MiB objects.
   Do: a horizon relative to the tier's size, or a floor in bytes, so that
   a small cluster filled fast is not throttled with most space free.
6. **Benchmark tooling follow-ups.** An open-loop (rate-driven) mode;
   `Content-Range` checks; state files merged across population steps;
   the generator wait loop bounded; a profile helper that starts on the
   run's measured window.

## Phase 4.2 — scale-out and capacity efficiency

1. Dedicated hosts: gateways, nodes and FoundationDB on separate machines
   with local disks; gateway scaling (the per-process FoundationDB network
   thread), FoundationDB with more processes and roles (refuted as the
   limit here, not tested where it could be).
2. 10⁷ objects through S3 (needs about 240 GB of node capacity for 1 KiB
   objects at today's cost per object), 10⁸ metadata-only.
3. Small-object packing or inline data (an RFC: segment or metadata
   format change, upgrade and rollback).
4. Event-driven orphan detection (fewer full reconciliation scans).
5. The 24 h soak on dedicated storage and physical power-loss tests
   (carried from Phase 3.14; not Phase 4.0 conditions).
