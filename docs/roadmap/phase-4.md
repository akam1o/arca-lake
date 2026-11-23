# Phase 4 roadmap

Phase 4 turns the Phase 3 system into one whose performance and scale
limits are measured and then raised. **Phase 4.0 delivered a baseline;
Phase 4.1 is in progress; Phase 4.2 has not started.** Phase 4.1 has
implemented the changes described below, but its remaining work and unmet
measurement criteria are still open. See the [roadmap index](README.md)
for a concise current-status view.

| Sub-phase | Scope | Completion criteria | Status |
| --- | --- | --- | --- |
| 4.0 Scalability baseline | benchmark framework (`arcabench`, `nodebench`, `metaops`, `bench-cluster.sh`), validity rules, object count 10³–10⁶ through S3 and 10⁷ metadata-only, gateway, FoundationDB processes, storage node, concurrency and tail, background passes, metadata operations, profiles, failures under load, R-108 heap, three limited changes with A/B (gateway `TCP_NODELAY`, `--access-records`, `--admission-horizon-secs`), controller pass metrics, two reviews in two rounds | Y1–Y16 with evidence (`docs/verification/phase-4.0-scalability.md`) | delivered with limitations (see the report): 10⁷ through S3 and 10⁸ estimated; one 4-vCPU host for nodes and gateways; virtual disks. The intermittent `managed_restore` failures (R-113) were subsequently classified in Phase 4.1 |
| 4.1 Metadata scalability and background processing optimization | below | below | in progress: R-112 investigated through S3 and R-114 fixed; R-113 test race corrected; ADR-0033 implemented and measured, with the capacity-pressure criterion incomplete; ADR-0034 decisions 1–5, 7 and 8 implemented, with the commits-per-GET target met and GET-throughput target missed. Evidence and remaining work below |
| 4.2 Scale-out validation and capacity efficiency | below | below | not started |

## How Phase 4.0's figures are to be read

These are baseline observations from Phase 4.0, not current performance
figures. Phase 4.1 follow-ups are linked below; measurements apply only to
the environments described in their reports.

- **Reconciliation does not pile up.** The scheduler starts a pass one
  interval (300 s) after the previous one ended; passes never overlap,
  and the work of a pass is proportional to the shards stored, not to
  the rate of change. At 10⁶ objects (3,566 s per pass) the costs are:
  orphan shards and the sources of moves are removed, and their space
  returned, up to about one cycle late; reconciliation's metadata reads
  are a standing FoundationDB load; rebalancing's 1,500 s settle time is
  shorter than the cycle. Liveness and capacity, not safety (R-110).
- **Gateway scaling was not measured.** One to four gateways shared one
  4-vCPU host with the storage nodes; the fall of GET throughput with
  more gateways is that host's CPU, not a limit of horizontal scaling.
- **Access records explain part of the GET cost.** With them off, GET
  4 KiB rose 22 %; the rest of a GET's metadata cost is the map-version
  read, the manifest read and the gateway's single FoundationDB network
  thread.
- **R-112 was measured on the metadata path only in Phase 4.0.** The
  subsequent [S3 investigation](../verification/phase-4.1-r112.md)
  found no acknowledged write lost or wrong version visible in its
  trials, fixed the separate R-114 answer defects, and left hot-key
  contention as an availability issue.

## Phase 4.1 — Metadata scalability and background processing optimization

### Order

**P0 — correctness and liveness first**

1. **R-112, concurrent overwrite of one key** — *done (Phase 4.1,
   [report](../verification/phase-4.1-r112.md)): no acknowledged write
   lost and no wrong version visible in 34 trials; R-114 fixed.* The
   S3 campaign covered versioning, concurrent writes, restarts and
   faults, followed by integrity checks. Hot-key contention remains
   Medium (availability). Fair per-key commit ordering with a bounded
   wait remains P1 work, preserving transactional preconditions,
   version order and retirement.
2. **R-113, intermittent `managed_restore` failures** — *done
   ([report](../verification/phase-4.1-r113.md)): expected protection
   met by a test race; tests synchronized, no product change.* The
   classification method below is retained as the original criterion.
3. **Reconciliation and reclaim at object scale (ADR-0033, R-110)** —
   *implemented and measured (`docs/verification/phase-4.1-adr33.md`):
   at 10⁶ objects a reconcile pass 2,733 s → 146 s, reclaim 2.74 → 0.55 ms
   per record, retained bytes level for an hour; the reclaim-at-capacity
   run is incomplete (load host stopped; §4c): the reclaim criterion is
   not met, R-110 stays Medium; the
   [retest plan](../verification/phase-4.1-r110-retest-plan.md) (version
   1) was run twice on 2026-11-21 (one run interrupted by the operator at
   stage 3 for a tooling finding, one ended in stage 4), both Inconclusive
   ([results](../verification/phase-4.1-r110-retest-results.md)); plan
   versions 2 and 3 were run on 2026-11-22
   ([results](../verification/phase-4.1-r110-retest-results-2.md)): run 1
   (v2) Failed on C5 only, run 2 (v3) Passed every criterion with an
   earlier 0.1.0 candidate, run 3 (v3) with the 0.1.0 release binary was
   Inconclusive (test hosts unreachable in stage 6).* The
   implementation uses batched read-only
   judgments per listing page, bounded parallelism, settle time from the
   measured cycle, and a "pass running since" gauge. The subsequent
   [recovery and integrity check](../verification/phase-4.1-adr34.md)
   verified the cluster's objects; the capacity retest was not resumed.

**P1 — metadata performance**

4. **Access records (ADR-0034, R-109)** — *decisions 1–5, 7 and 8
   implemented and tested ([report](../verification/phase-4.1-adr34.md));
   decision 6 implemented since as ADR-0035 option A
   ([report](../verification/phase-4.1-adr35.md)).* The gateway uses a
   bounded table and batched writes, checks the generation in the same
   transaction, and gives unread generations a last-access time from
   their creation. In the reported VM A/B, approximately 0.007–0.008
   FoundationDB commits per GET meets the ≤ 0.1 target; GET throughput
   remains 7–14 % below `off`, so its target is not met. The default
   remains `on` (the batched path) with a batch size of 128. Where tiering
   is confirmed disabled, gateways no longer record reads (ADR-0035; the
   throughput result is a quiet-condition one). Two gateways contending
   on one hot generation and a FoundationDB stall under load remain
   unverified on the cluster (unit tests cover these cases).
5. **Transactions and conflicts per request:** count and reduce the
   remaining transactions and conflicts. Phase 4.0 observed about 3
   transactions per GET and 4 per PUT; ADR-0034 reduced the measured
   GET count to about 2.01. Per-key contention and the remaining GET
   work are still open.
6. **Map-version read per request:** cache with bounded staleness backed
   by the nodes' epoch checks (an ADR on the correctness of a cached
   map), or a watch.
7. **Gateway placement check:** gateways on a host of their own (the two
   Phase 4.0 hosts suffice for one or two gateways), to tell whether the
   gateway's FoundationDB network thread (79 % of a core at 3,000
   GETs/s) is a per-process limit; the full scale-out measurement stays
   in 4.2.

**P2 — storage efficiency (design and measurement)**

8. **Small-object capacity (R-111):** analysis of the 24 KiB per 1 KiB
   object (alignment, delete reserve); reserve sized by the real
   tombstone record where the format allows.
9. **Packing ADR** (format and recovery impact; implementation not in
   4.1).
10. **Admission horizon on small clusters** and the benchmark tooling
    follow-ups (open-loop mode, `Content-Range` checks, merged state files,
    bounded waits).

### Completion criteria

Every criterion is measured on the Phase 4.0 harness and layout unless it
says otherwise, A/B against the Phase 4.0 binary, three or more valid
samples per condition in rotated order (`docs/verification/phase-4.0-scalability.md` §1).

| Area | Criterion |
| --- | --- |
| Reconciliation, progress | At 10⁶ objects on the Phase 4.0 layout, reconciliation's cycle (pass plus interval) is consistent with the minimum orphan age and with rebalancing's settle time: either the cycle fits inside both, or the settle time is derived from the measured cycle and a test shows rebalancing does not plan again before its previous moves' space returned. |
| Reconciliation, bounded pass | If a pass gets a time or work budget: the scan resumes where it stopped and covers the whole range in a bounded number of passes, so no (group, node) pair and no shard is skipped forever; shown by a test that forces small budgets, interruptions and leader changes and checks every orphan is eventually judged, and by the cursor's coverage metric. |
| Reclaim | The sustained retirement rate the reclaim pass absorbs is measured; at that delete rate, retained bytes stay level (not growing without bound) over a run of at least an hour at 10⁶ objects; deferred reclamation's rules unchanged. |
| Access records, cost | At most **0.1 FoundationDB commit per GET** at 10⁵ uniformly read objects. Performance target: GET within the spread of `--access-records off`; if not met, the remaining difference is measured and its cause named in the report. |
| Access records, bounded queue | The queue has a fixed bound; drops, queue depth and flush delay are metrics. The effect of drops is evaluated, not only for tiering's safety (moves stay fenced) but for its decisions: promotions missed or delayed, demotions of objects that were read, and bias in the counts between hot and cold objects or between gateways, under a workload that overflows the queue on purpose. Tiering tests pass. |
| Transactions per request | Transactions and commits per GET, HEAD and PUT reported before and after; each change justified by those counts. |
| R-112 | Reproduced through S3 with the outcome stated (data effect or none); severity re-evaluated; if fixed, the reproduction passes and a test fails with the fix removed. |
| R-113 | Classified as below; the cause shown; a test that fails without the fix. |
| R-94(b) | Deferred reclamation tests and the system-model seeds unchanged and passing; any change to a delete or reclaim path reviewed against ADR-0028's hypotheses. |
| Reviews | Two independent reviews; Critical and High findings with failure sequences and fix-removal tests. |

### R-113 classification (before any fix)

The investigation is complete; see the
[classification and test correction](../verification/phase-4.1-r113.md).
The original method is retained here as the basis for that conclusion.

1. Run the `managed_restore` tests of the Phase 3.14 tree (`88732f1`)
   and of the Phase 4.0 tree on the same host, with the same toolchain,
   target directory per tree, alternating, under the same load (idle, and
   alongside a workspace test run), enough runs for a failure rate of the
   observed order (about 3 in 8) to be told from zero.
2. Find which incarnation the failing gateway was built with and which
   one the store holds when it refuses, with the order of the restore
   steps, the test's gateway start and the state refresher.
3. Decide which it is:
   - **expected protection**: the gateway was started (or kept its
     incarnation) before a restore changed it, and refusing is correct;
     then the test's sequence is wrong, and the fix is in the test's
     sequence (start the gateway after the incarnation it must serve),
     never in a weaker assertion;
   - **recovery defect**: the incarnation changed when it should not have
     (a step repeated, a restore path writing a new incarnation twice, a
     refresher reading a stale record), or a gateway that should accept
     refuses; then the product is fixed.
4. A test's expectation is not relaxed to make the failure go away.

## Phase 4.2 — scale-out and capacity efficiency

1. Dedicated hosts: gateways, nodes and FoundationDB on separate machines
   with local disks; gateway scaling (the per-process FoundationDB network
   thread); FoundationDB with more processes and roles (no gain from three
   processes in 4.0, where FoundationDB was not the limit).
2. 10⁷ objects through S3 (about 240 GB of node capacity for 1 KiB
   objects at today's cost per object), 10⁸ metadata-only.
3. Small-object packing or inline data (an RFC: segment or metadata
   format change, upgrade and rollback).
4. Event-driven orphan detection (fewer full reconciliation scans).
5. The 24 h soak on dedicated storage and physical power-loss tests
   (carried from Phase 3.14; not 4.0 or 4.1 conditions).
