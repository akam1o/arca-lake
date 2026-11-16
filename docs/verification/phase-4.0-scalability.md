# Phase 4.0 scalability baseline and bottleneck analysis

Status: Phase 4.0 report. It measures where the current code stops
scaling on the test environment, explains why from the samplers and the
code, makes three small changes with A/B evidence, and sets the Phase 4.1
and 4.2 priorities (`docs/roadmap/phase-4.md`). No metadata layout, segment
format or protocol change was made.

## 1. Method and validity criteria (fixed before the measurements)

Every result below is labelled **measured** (real processes or the
in-process cluster, as stated), **simulated**, **estimated** (an
extrapolation from measured figures, with its basis), **code analysis
only**, or **not verified**.

Tools: `benchmarks/arcabench` (S3 workloads, deterministic verifiable
bodies, per-second timeline, latency histogram with 2 % buckets, the
generator's own CPU, memory and file descriptors), `benchmarks/linux/bench-cluster.sh`
(the cluster and the samplers: `sar`, `pidstat`, file descriptors,
FoundationDB `status json` every 2 s, every process's metrics every 5 s),
`benchmarks/metabench` (metadata operations directly on FoundationDB),
`nodebench` (the storage node's RPCs without a gateway), in-process
tests for the controller at scale.

A sample is **valid** only if all of these hold; otherwise it is
discarded with the reason recorded and run again:

1. no integrity violation (an acknowledged write not read back, a body
   that is not a version written or in flight, a listing of the wrong
   size); a violation stops the measurements until it is explained;
2. the generator is not the limit: its CPU below 85 % of its threads,
   and, for each workload type at least once, a run with two generator
   processes reaches the same aggregate throughput within 10 %;
3. the warm-up (at least 10 s for timed runs) is excluded from the
   measurement window, and the window is at least 30 s (60 s for
   concurrency 512 and above);
4. errors below 1 % of requests unless the run is meant to show errors
   (a saturation point), in which case they are reported by class;
5. no process of the cluster restarted during the window (unless the
   run injects that fault);
6. FoundationDB available throughout (every status sample);
7. samples of one condition are run in rotated order with the other
   conditions of the same comparison, at least three valid samples per
   condition; results are medians with the range.

8. *(added before the second gateway-scaling campaign, after the first
   was discarded: §3)* the cluster is quiescent before a sample: the
   leader's GC backlog 0, every node's compaction backlog below 64 MiB
   and no admitted write in flight, the nodes' disks below 20 %
   utilisation, in three checks about 4 s apart (`bench-cluster.sh quiesce`;
   a run after a quiescence timeout is invalid); write samples
   write fresh keys and are followed by the deletion of those keys.

A difference between conditions is reported as one only if the ranges of
the three samples do not overlap; otherwise "within the spread".

A saturation claim names what saturated first, from the samplers: the
generator (rule 2), the network (above 80 % of the measured 22 Gbit/s
between the hosts), a host's CPU (above 90 % of its cores), a disk
(utilisation above 90 %), or FoundationDB (its `performance_limited_by`
other than `workload`, or its commit latency above 50 ms).

Errata to the text above, none a change of criterion: rule 8 first said
"for 10 s"; the check as implemented and used is the one now described
(three checks about 4 s apart). "A run after a quiescence timeout is
invalid" was added after the first review; no sample of any campaign hit
the timeout.

Environment (measured at the start, recorded with the artifacts): two
virtual machines of 4 vCPUs (AMD Ryzen 7 PRO 4750GE), 7.7 GB of memory,
virtual disks (sequential write 1.2–1.5 GB/s, read 1.1–2.3 GB/s, one
4 KiB `dsync` write about 1.1 ms), 22 Gbit/s between them (iperf3, four
streams), FoundationDB 7.3 single process `ssd` engine on a second
virtual disk of host A unless stated. Nothing here is a statement about
other hardware.

## 2. Benchmark framework

| Piece | What it does | Evidence it works |
| --- | --- | --- |
| `arcabench` | S3 workloads PUT/GET/HEAD/DELETE/ListV2/multipart/mixed/verify; size or size range, object count, concurrency, buckets, key layout, key distribution (uniform/zipf/hot/sequential), read/write/delete/list ratios, warm-up and window, fixed seed, `--retries`; per-op ops/s, MiB/s, p50/p95/p99/max (2 % buckets), errors by HTTP class, retries, generator CPU/RSS/FDs, per-second timeline | Smoke: a state-correct run gives 0 violations; a run with the wrong body seed flags every checked object (1,682 of 2,000; and, by accident, 91 of 91 checked in the first object-count attempt, §3); unit test `short_bodies_and_ranges_do_not_pass` (fails with the length check removed) |
| integrity model | one writer per key; body = header (index, version, size) + stream of (seed, index, version); a read passes only as a version acknowledged or in flight during it, of its full length (a range: exact offset, bytes and length); 404 only if a delete or no write was possible; `verify` exits non-zero on any key it could not read; `--state-file` carries versions across runs | review findings H1/H2 (first review) fixed before the campaigns from E6 on; earlier campaigns were checked with the weaker rule (a body shorter than 16 bytes or a short range would have passed): their sizes were ≥ 1 KiB and ranges were 1 KiB/64 KiB, the final checks of §11 use the fixed rule |
| `nodebench` | one storage node's `PutShard`/`GetShard`, one size and concurrency, warm-up, delete of what it wrote | §8 |
| `metaops` | metadata operations through the gateway's own functions (`begin_put`/`append_chunks_renewing`/`commit_put`, `read_manifest`, `head_object`, `delete_object`, `list_objects`, `map_version`, `record_access`) on FoundationDB, with a counting store (transaction attempts, point reads, range reads, rows, bytes read, sets, clears, bytes written per operation) and the transaction loop's conflict counters | §6 |
| `bench-cluster.sh` | cluster on a private FoundationDB (port and directories of its own; the system FoundationDB untouched), samplers on both hosts, generators detached and polled (the driver survives a network drop), `quiesce` (rule 8), `snapshot`, timed `scrub`, `restart-gateways`, alternative gateway binary (`GW_BIN`) for A/B; addresses stripped from everything copied back | runs below |
| `summarize_bench.py` | per run: every client summed, per-role CPU per host (pidstat), host CPU/net/disk (sar), FoundationDB commit/read latency and rates (status json), all cut to the generator's measured seconds (timeline) | the window cut was added after the first review (M2): with the earlier count-based cut, roles' CPU read 10–25 % low (e.g. E6 GET c32: gateway 1.47 → 1.81 cores); every table here was recomputed with the window cut |
| controller pass metrics | `arca_controller_passes_total`, `_pass_last_seconds`, `_pass_max_seconds` per task (new) | §10 |

Limitations of the framework, kept: the generator is closed-loop (no
coordinated-omission correction: tail latency beyond saturation is that of
the requests sent; errors are counted by class and not in the histogram);
several client processes are summed per process window (offsets of
launch below one second, inside the 10 s warm-up) and their latency
percentiles are the worst of the processes (an upper bound); a failed
delete followed by a failed PUT of the same key forgets the possible delete
(a false positive only); `nodebench` reads hit the page cache.

## 3. Campaigns, invalid runs and their reasons

| Campaign | Layout | Runs | Status |
| --- | --- | --- | --- |
| E1 generator validity | 4 nodes and FoundationDB on A, 1 gateway on B, generator on A | 9 (3 conditions × 3, rotated) | valid |
| E4 gateway scaling, first | nodes on A, gateways on B, one group | 36 | **discarded**: write admission refused PUTs (`write_rejected`, "no group has room (1 excluded)": one group of three 6 GiB nodes) and the compaction left by PUT runs saturated A's disk during later GET runs; rule 8 added |
| E4b gateway scaling | 4 nodes on B, FoundationDB and generator on A, 1/2/4 gateways on B, 4 groups, retention 0, EC 2+1 for all sizes | 36 + deletes | 4 KiB valid; 1 MiB invalid (admission throttled by the time-to-full horizon: population reached 32 of 2,000 objects; the GET "violations" were the harness's accounting of never-written keys) |
| E4c 1 MiB scaling, access-record A/B, TCP_NODELAY A/B | as E4b, horizon 60 s (`--admission-horizon-secs`) | 18 + 6 + 18 | valid except PUT 1 MiB (errors 503 SlowDown above 1 %: reported as admission-limited) |
| E6 concurrency | as E4c, 1 gateway | 36 + deletes | GET valid; PUT at 512 and 1024 invalid (tier `write_rejected`: earlier populations left 1.5–2.0 GB headroom of 18 GB usable; a capacity artifact of the test cluster) |
| E6 first attempt | | 13 | **aborted**: the driver and its chain were killed at one moment (cause not found); rerun under `screen` |
| E7 object count, first two attempts | | | **discarded**: (1) the GET rounds passed `--seed`, which changes the bodies (every checked read flagged, as the negative test predicts) and `screen` sets `WINDOW`, which the script used as the window (0: one pass instead of a timed window); (2) `--client-id` given twice; (3, not discarded for a reason of validity but restarted) rule 8 was not applied between samples |
| E7 object count 10³–10⁵, then to 10⁶ | 4 nodes on B (6.5 GiB), 1 gateway, 1 KiB replicated | 3 checkpoints × 3 rounds + populations | 10³–10⁵ valid except three samples that followed a quiescence timeout (10⁴ GET r3, 10⁵ HEAD r3, 10⁵ LIST r3: excluded, those cells have two samples); the step to 10⁶ **invalid**: 234,866 of 900,000 PUTs refused with 503 (no retries then), then the tier `write_rejected` at 765,134 objects (capacity: §7); a second populate with retries ran for 11 h without one success until stopped (its logs kept) |
| E7 at 765,134 | same cluster | LIST, full verify, scrub, snapshot | valid; the scrub timings of two groups were lost to repeated commands after network drops (one re-ran on a finished cursor, one was timed across outages): groups 6 and 8 timed |
| E7c 10⁵ cross-check and 10⁶ | fresh, 8.5 GiB allotments | 2 checkpoints × 3 rounds, scrubs, final LIST and verify | valid except 10⁵ HEAD r3 (quiescence timeout) and PUT/DELETE at 10⁶ (PUT refused by capacity admission). E7d: nodes restarted with 11 GiB for the PUT samples at 10⁶ (after an unplanned 33-minute outage of every node, §11); DELETE at 10⁶ invalid twice (§7 note 2) |
| E5 FoundationDB 1 vs 3 processes | E4b layout | 24 + deletes, 6 fresh clusters | valid |
| E8, E8b, E8c node RPC sweep | | 3 attempts | **discarded**: E8's deletes are refused by the delete fence (a benchmark is not the leader), so the node filled; E8b/E8c's 1 GiB key sets plus the delete reserve filled it again |
| E8d node RPC sweep | fresh, 10 GiB, bounded key set | 32 configurations × 3 | valid except samples with > 1 % capacity refusals (§10) |
| E9 profiles | | 6 | **discarded**: taken after the runs had ended |
| E9b profiles | fresh E9 layout | 6 | valid (each began inside the run: its warm-up or first measured seconds) |
| E10 failures under load | E9 layout | 1 run + verifies | valid |
| E11 metadata operations | FoundationDB of the E9b cluster | 159 + 9 PUT samples | valid except the first PUT samples (an argument given twice: no output; measured again) |
| E12 controller at scale, R-108 heap | in process, host B | 5 configurations; heap run | background scale valid; the first heap run **invalid** (the test never renewed its delete fence: every delete after 30 s refused, disk full; fixed in the test, run again) |
| E13 S3-level metadata shapes | E9 layout | 12 + verifies | valid |
| sfcheck | | 2 | the state-file delete pass works (18,652 written, 18,652 deleted) |
| smoke-final | | | the final harness end to end (3 clients, scrub with exit codes, gateway restart, teardown) |

Harness errors found during the campaigns and fixed (each found by an
invalid run or a review, recorded above): body seed changed by `--seed`;
`WINDOW` set by screen; `--client-id` twice; a standalone `quiesce`
ending on a host without nodes; edits of the running driver script
corrupted two invocations (afterwards the campaigns ran a frozen copy);
a retried ssh could repeat a non-repeatable command (§16); node restarts
lost the plaintext switch (the restart took the process's arguments,
which do not hold its environment); the E7d delete pass not finding its
state file (not reproduced: `sfcheck`).

Stale sampler processes: runs whose driver ended abnormally (a smoke run
whose second summary was missing, the aborted E6 attempt) left their
samplers running (pidstat, sar, a metrics scrape every 5 s, FoundationDB
`status json` every 2 s on A). One such set ran through E1–E6, a second
through E6. E1 measured one sampler set's effect as within the spread;
the second set's was not measured. They were removed before E7 and the
harness now stops samplers on any exit.

## 4. Generator validity (E1, measured)

GET 4 KiB, 20,000 objects, 64 connections in total, 3 rotated rounds:

| Condition | ops/s median (range) | p99 ms | generator CPU of its threads |
| --- | --- | --- | --- |
| 1 process | 3,180 (2,998–3,313) | 29.7 | 0.08 |
| 2 processes × 32 | 3,146 (3,035–3,295) | 30.3 | 0.05 |
| 1 process, no sampler | 3,114 (3,103–3,272) | 29.1 | 0.08 |

Two processes reach the same throughput (within the spread, rule 2); the
generator uses below 10 % of its threads; the sampler's cost is within the
spread. The generator was not the limit in any later run (its CPU at most 0.40 of
its threads in valid runs, 0.66 in one invalid E4b run, against the 0.85
limit; `client_cpu_util` in the artifacts).

## 5. Gateway scaling and the shared bottleneck (E4b, E4c, measured)

Layout: 4 storage nodes and 1–4 gateways on host B, FoundationDB (one
process), controller and generator on A; EC 2+1 for every size
(small-object replication off), 4 groups, 64 connections spread over the
gateways, 30 s windows, 3 rotated rounds.

| Workload | 1 gateway | 2 gateways | 4 gateways | What is busiest (4 gateways) |
| --- | --- | --- | --- | --- |
| GET 4 KiB ops/s | 2,926 (2,889–3,099) | 2,390 (2,387–2,434) | 1,911 (1,858–1,962) | FoundationDB 1.03 cores (one process; 0.89 at 1 gateway), host B CPU 87 % |
| GET 4 KiB p99 ms | 31.5 | 43.2 | 53.7 | FDB commit 2.9 → 8.9 ms |
| PUT 4 KiB ops/s | 1,017 (739–1,031) | 1,124 (775–1,196) | 944 (898–946) | B's disk 85–93 % (the 4 nodes share one virtual disk) |
| GET 1 MiB MiB/s | 701 (699–708) | 681 (671–689) | 667 (663–679) | **host B CPU 96–99 %** (gateways 2.2–2.3 cores, nodes 1.5 cores); network 5.9 Gbit/s of 22 |
| PUT 1 MiB MiB/s | 142 (123–233) | 186 (30–214) | 210 (205–212) | B's disk 88–99 %; 503 SlowDown 0.1–3 % (admission): not valid as a capacity figure |

Findings:

- Adding gateways on the same host does not add throughput here: B has
  4 vCPUs for the nodes and every gateway. GET 4 KiB even falls with more
  gateways while FoundationDB's single process reaches one core: with
  four client processes FoundationDB does more work per request
  (transactions started per GET stay at 2.95–3.0; commits per GET 0.87;
  the extra cost per request is **not explained** by the counts here:
  read-version batching per client process is a hypothesis, not
  measured). By the criteria of §1, no listed resource reached its
  threshold at GET 4 KiB × 4 gateways (host B 87 %, FoundationDB latency
  below 50 ms); the single-threaded FoundationDB process at 1.03 cores
  is at its own maximum, an observation outside the fixed criteria
  (E5 tests it).
- GET 1 MiB is limited by host B's CPU (gateway decode and copy, the
  nodes' reads): the first criterion met.
- PUT is limited by the shared virtual disk of the nodes (fsync per
  shard, 4 KiB `dsync` ≈ 1.1 ms).
- FoundationDB transactions per request (from `status json`, measured):
  GET ≈ 2.95 started, 0.87 committed (the map-version read, the manifest
  read, the access record); PUT ≈ 4.2 started, 3.1 committed
  (`begin_put`, `append_chunks`, `commit_put`, the map-version read);
  HEAD ≈ 1.0 started, 0 committed (E7). The gateway costs about 0.55 ms
  of CPU per 4 KiB GET.

## 6. Concurrency and tail latency (E6, measured)

One gateway, 64 KiB objects (20,000 for GET; fresh keys for PUT, deleted
after each sample), windows 30 s (60 s at 512 and above), 3 rotated
rounds.

| Concurrency | GET ops/s | GET p50 / p99 / max ms | PUT ops/s | PUT p50 / p99 ms | Busiest |
| --- | --- | --- | --- | --- | --- |
| 1 | 256 (249–271) | 3.7 / 6.9 / 68 | 83 (75–86) | 11.9 / 17.0 | — |
| 8 | 1,241 (1,149–1,285) | 6.3 / 10.8 / 16 | 343 (331–347) | 20.4 / 114 | PUT: disk 87 % |
| 32 | 2,115 (2,044–2,140) | 14.8 / 24.3 / 35 | 541 (524–606) | 57 / 183 | PUT: disk 94 % |
| 128 | 2,902 (2,849–2,967) | 44.1 / 65.5 / 92 | 1,166 (1,034–1,292) | 103 / 242 | GET: B CPU 78 %; PUT: disk 96 % |
| 512 | 3,303 (3,164–3,340) | 157 / 224 / 309 | invalid | | GET: B CPU 81 %, gateway 2.3 cores |
| 1024 | 3,138 (3,114–3,229) | 326 / 447 / 591 | invalid | | GET: as at 512 |

- GET saturates between 128 and 512 connections at about 3,300 ops/s
  (206 MiB/s); beyond it latency grows in proportion to concurrency
  (queueing: p50 157 → 326 ms for 512 → 1024 at the same throughput) and
  throughput falls slightly. The p99 knee is between 32 (24 ms) and 128
  (66 ms). No error at any concurrency.
- What saturated first for GET: none of the fixed thresholds was reached
  (host B 81 %, FoundationDB 0.75–0.81 cores, commit 3.1–3.3 ms); the
  gateway process at 2.3 cores of 4 is the busiest (its threads: §12).
- PUT is disk-bound from 8 connections on (87–96 %); at 512 and 1024 the
  tier went `write_rejected` (headroom 1.5–2.0 GB of 18 GB usable after
  the campaigns' populations): those samples are invalid, a capacity
  artifact of the test cluster, not a concurrency limit.
- The controller used up to 0.6 cores during some samples (reconcile
  passes, §10): background work of the same cluster during client I/O.

## 7. Object count (E7, E7c, measured; 10⁷–10⁸ estimated)

Layout of E6, one gateway, one bucket, 1 KiB objects (below the 64 KiB
small-object threshold: three replicas, groups 5–8), retention 600 s,
horizon 300 s. Populated in steps with sequential keys; at each
checkpoint three rotated rounds of GET and HEAD of uniformly random keys
(one read in ten checked), a timed PUT of fresh keys and a delete pass of
them, and a full ListObjectsV2 (key count checked); rule 8 before every
sample. E7 covered 10³–10⁵ on 6.5 GiB allotments; E7c repeated 10⁵ on a
fresh cluster with 8.5 GiB allotments (cross-check) and went to 10⁶.

| Objects | GET ops/s (c32) | GET p99 ms | FDB commits per GET | HEAD ops/s | LIST page p50 ms (1,000 keys) | PUT ops/s | DELETE ops/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10³ (E7) | 3,347 (3,328–3,404) | 14.3 | 0.23 | 6,816 (6,604–6,949) | 5.0 | 1,039 (911–1,074) | 2,967 (2,597–3,217) |
| 10⁴ (E7) | 2,838 (2,749–2,928) ³ | 16.2 | 0.79 | 6,461 (6,146–6,841) | 5.6 | 1,014 (988–1,093) | 3,008 (2,623–3,147) |
| 10⁵ (E7) | 2,512 (2,099–2,833) | 18.8 | 0.99 | 6,240 (5,904–6,575) ³ | 6.3 (6.1–6.6) ³ | 1,000 (997–1,037) | 2,701 (2,580–2,811) |
| 10⁵ (E7c) | 2,550 (2,534–2,803) | 18.1 | 0.98 | 6,253 (5,936–6,570) ³ | 7.4 | 976 (942–989) | 2,550 (2,446–2,674) |
| 10⁶ (E7c) | 2,398 (2,137–2,502) | 20.0 | 1.02 | 5,971 (5,684–6,426) | 5.7 | 860 (722–1,009) ¹ | not valid ² |

¹ After the nodes' allotments were raised to 11 GiB (below). ³ Two valid
samples (the third followed a quiescence timeout and is excluded):
median of two and the range. ² Both
attempts at 10⁶ deleted more keys than had been written (the first after
PUTs refused by admission, whose keys stay "in flight"; the second
because the delete pass did not find the PUT's state file, cause not
found): a mix of real deletes and deletes of absent keys, not a DELETE
figure.

Cross-check: GET, HEAD and DELETE at 10⁵ reproduce between E7 and E7c
(ranges overlap); PUT (−2.4 %) and LIST page time (+21 %) do not, with
reconciliation using 0.4–0.9 controller cores during E7c's samples.

Findings (measured, with the code that explains them):

- **GET falls with object count because of the access record**: commits
  per GET 0.23 → 0.79 → 0.99 → 1.02 (the coalescing of one write per
  generation per second only helps while keys are read again within a
  second); transactions started per GET 2.25 → 3.0. HEAD, which writes no
  access record, stays at about 6,000 ops/s (1.0 transaction, no commit).
  At 10⁶ the nodes' data (about 12 GB allocated) no longer fits host B's
  page cache: GET samples read the disk (27.7 % busy, against 0.2 % at
  10⁵).
- **LIST** is O(page): 5–7 ms per page of 1,000 keys from 10³ to 10⁶
  (`list_objects`: one range read per 1,000 rows); a full listing of
  10⁶ keys took 1,000 pages at 142 pages/s (≈ 7 s). The bucket's key
  count was exact at every checkpoint (765,134 and 1,000,000 included).
- **PUT** stays disk-bound (B's disk 84–88 %) at about 1,000 ops/s; 3.0–3.2
  commits per PUT.
- **Capacity is the first limit of small objects.** At 765,134 objects
  (E7, 6.5 GiB allotments) the tier went `write_rejected` with 0.59 GB of
  live payload per node: each node's reserve was 4.09 GB = the base
  reserve (1.74 GB: a quarter of the allotment) + the delete reserve
  (live records + 1,024) × 4,096 B; allocated 2.34 GB. Per 1 KiB object:
  3 × 4 KiB allocated (record alignment 4,096) + 3 × 4 KiB of delete
  reserve ≈ 24 KiB of node capacity (code: `storage-node/src/capacity.rs`
  `delete_reserve`, `Reserve::default_for`; confirmed by both reviews).
  E7c reached 10⁶ with 8.5 GiB allotments, then its PUT samples were
  refused again (headroom 0.53 GB per node); the nodes were restarted
  with 11 GiB allotments (data kept) for the PUT samples at 10⁶.

Controller passes over object count (pass metrics, measured; the same
cluster, so these are lower bounds of what a busier cluster sees):

| Objects | reconcile last (longest) s | reclaim longest s | scrub of one replicated group | GC longest s |
| --- | --- | --- | --- | --- |
| 10³ | 5.9 (52.8) | 432 | — | 26.6 |
| 10⁴ | 31.4 (148.3) | 889 | — | 26.9 |
| 10⁵ | 293–363 (516–526) | 742–1,094 | 115–125 s, 75,315 shards | 30.3 |
| 7.65 × 10⁵ | 2,115 (2,451) | 1,094 | 953–984 s, 566,868–581,304 shards (595 shards/s) | 30.3 |
| 10⁶ | 3,566 | 742 | 1,266–1,319 s, 755,268 shards | 30.5 |

- Reconciliation grows linearly with stored shards (about 840 shards/s
  at 10⁶: 3 × 10⁶ shard records in 3,566 s) and is longer than its 300 s
  interval from about 10⁵ objects: from there it runs most of the time
  (code: one `attribute_shard` transaction per listed shard, pairs one
  after another; ADR-0033).
- The reclaim pass grows with the retirements waiting (the samples'
  deletes: 0.1–0.3 million records), not with the objects stored: about
  250–300 records/s (one metadata check per record, keep/reclaim RPCs 16
  at a time).
- Scrub reads about 600 shards/s per group at every size (linear).
- Pass times do not starve other tasks (separate schedulers) and do not
  weaken deferred reclamation (a slow reclaim only keeps data longer);
  they delay orphan removal and the return of space, and make
  rebalancing's 1,500 s settle time shorter than the reconcile cycle
  (both reviews; R-110).

Integrity at scale (measured): a full verify of every key at 765,134
(all 1,000,000 indices: 234,866 never written, exactly the first
population's failed PUTs, which ran without retries; 0 mismatches) and at
10⁶ (1,000,000 read, 0 missing, 0 mismatches; and again after an
unplanned restart of all four nodes, §11); scrubs of every group at 10⁵,
7.65 × 10⁵ and 10⁶ with no finding (at 7.65 × 10⁵ one group's figure was
lost to a repeated command).

10⁷ and 10⁸ (**estimated**, linear from the measured 10⁵–10⁶ slopes on
this environment, not measured): node capacity for 1 KiB objects ≈ 24 GB
(10⁶), 240 GB (10⁷), 2.4 TB (10⁸) plus reserves; one reconciliation pass
≈ 10 h at 10⁷ and ≈ 4 days at 10⁸; one scrub pass of a replicated group
holding a quarter of the shards ≈ 3.5 h and ≈ 1.5 days; a full listing
≈ 70 s and ≈ 12 min (O(page), measured flat); GET/HEAD per-request
metadata cost flat (point reads), GET then bounded by the data path and
the access record. Metadata alone at 10⁷: §9.

## 8. FoundationDB processes (E5, measured)

The E4b layout (4 nodes and the gateways on B, FoundationDB on A),
FoundationDB as 1 or 3 processes (`configure new single ssd`, the same
disk), 4 KiB GET (20,000 objects) and PUT (fresh keys), 1 and 4 gateways,
3 rotated rounds, each on a fresh cluster.

| | 1 process | 3 processes |
| --- | --- | --- |
| GET 4 KiB, 1 gateway | 3,118 (3,087–3,136) | 3,028 (2,927–3,119) |
| GET 4 KiB, 4 gateways | 2,096 (1,991–2,109) | 2,163 (2,102–2,369) |
| PUT 4 KiB, 1 gateway | 1,192 (1,144–1,386) | 1,107 (1,059–1,372) |
| PUT 4 KiB, 4 gateways | 993 (878–1,011) | 989 (987–1,004) |
| FoundationDB CPU, GET 4 gateways | 1.03 cores | 1.72 cores |
| host B CPU, GET 4 gateways | 85 % | 92 % |

Every pair is within the spread: three processes do not add throughput
here. The single process at 1.03 cores was therefore not the limit of
GET with 4 gateways; host B's CPU is (92 % with three processes, above
the 90 % criterion), shared by the gateways and the nodes. The E4b
hypothesis (FoundationDB's single process) is **refuted** for this
environment; FoundationDB's capacity beyond it was not reached.

## 9. Metadata operations on FoundationDB (E11, E13, measured)

`metaops` on host B against the FoundationDB process on A (the E9b
cluster's, arca processes stopped), 64 tasks, 20 s windows after 3 s,
3 rotated rounds per checkpoint, the gateway's own functions; the records
written are those of a real PUT of one 4 KiB chunk (synthetic chunk
records naming group 1; no shard exists). Synthetic data stays in its
own buckets (`seq-*`, `hsh-*`, `many-*`), never mixed with S3 objects.
Per-operation counts are of transaction attempts (retries included).

| Operation (sequential keys, one bucket) | ops/s at 10³ | at 10⁶ | p99 ms (10⁶) | per op: attempts / point reads / range reads / rows / bytes read / writes / bytes written |
| --- | --- | --- | --- | --- |
| `read_manifest` (GET's lookup) | 11,030 | 11,680 | 7.4 | 1 / 3 / 2 / 1 / 310 / 0 / 0 |
| `head_object` | 18,730 | 18,520 | 4.8 | 1 / 3 / 0 / 0 / 242 / 0 / 0 |
| map-version read | 37,650 | 36,730 | 2.5 | 1 / 1 / 0 / 0 / 26 / 0 / 0 |
| access record | 16,950 | 13,140 | 16.9 | 1 / 1 / 0 / 0 / 43 / 1 / 43 |
| `delete_object` | 9,580 | 9,287 | 10.1 | 1 / 3 / 0 / 0 / 242 / 3 / 166 |
| `list_objects` page (1,000 keys) | 1,562 | 1,438 | 60.6 | 3 / 1 / 2 / **2,000** / 184 KB / 0 / 0 |
| populate (`begin_put` + `append_chunks` + `commit_put`) | 2,684 | 3,457 | — | 3 / — / — / — / — / — / 379 |
| overwrite, 1,024 hot keys | 3,287 | 3,392 | 27.7 | 3.02 / 6.1 / 0 / 0 / 407 / 8.1 / 555; conflicts 0.023 per op |
| overwrite, **one hot key** | 148 | 141 | 276 | 60.5 attempts and 54.9 conflicts per op; **56 % of operations failed** ("transaction retried too many times", 32 attempts) |

PUT of fresh keys (measured afterwards: the first PUT samples were lost
to a duplicated argument): 3,785 (3,575–3,897) ops/s at 10⁷ sequential
keys, p99 25 ms, 3 attempts, 5 point reads, 5 writes, 379 bytes.

Layouts at 10⁵ objects: hashed keys (`obj/<4 hex>/<index>`): GET lookup
11,200, HEAD 18,600, PUT 3,350 (3,249–3,432), delete 8,411; 100 buckets:
GET 11,220, HEAD 18,770, PUT 3,536, delete 9,151 — within about 10 % of
the sequential layout; no hot spot on sequential keys at these rates.

**10⁷ objects (metadata only, measured)**: 9 × 10⁶ more objects written
by `populate` (4,273/s for 2,106 s, no error); key-value data 3.3 GB,
FoundationDB disk 4.2 GB. GET lookup 8,683 (8,491–9,603) ops/s (−26 %
against 10⁶, ranges apart), HEAD 14,400 (13,840–14,750) (−22 %), list
page 1,001 (893–1,010) pages/s (−30 %), p99 11.8 / 7.8 / 93.6 ms. The
per-operation counts are unchanged; the drop starts when the key-value
data (3.3 GB) no longer fits FoundationDB's and the host's caches (7.7 GB
host, single `ssd` process): disk reads, an environment limit rather than
an algorithmic one. Layout identity: the records come from the gateway's
own functions (`begin_put`, `append_chunks_renewing`, `commit_put`);
GET and HEAD of random keys over the whole range found every one (0
errors). 10⁸ (estimated): about 33 GB of key-value data, 3.8 × 10⁸ bytes
written per 10⁶ objects; per-operation counts flat; latency then set by
FoundationDB's storage servers' cache and disks (not measured).


Findings:

- Every point operation costs the same from 10³ to 10⁶ objects (within
  about 10 %): FoundationDB point reads and short range reads do not
  depend on the key count here; the metadata (about 380 bytes written per
  object) took 0.61 GB of key-value data and 0.98 GB of disk at 10⁶.
- **ListObjectsV2 reads twice the rows it returns**: a full page of 1,000
  keys reads 2,000 rows in two range reads (the loop reads the next 1,000
  to learn that the listing is truncated; `crates/metadata/src/ops/list.rs`
  `list_objects`); 184 KB read per page. Code analysis also: a delimiter
  listing runs one range read per common prefix, and delete markers are
  skipped inside one request without a bound.
- **Concurrent overwrite of one key does not scale and fails**: 64
  writers of one key commit 141–148 PUTs/s, with 55 conflicts per
  successful PUT and 56 % of PUTs given up after 32 attempts. Through S3
  the data write before the commit spreads the writers out (E13: no
  conflict in the hot-key mix of §9b), so this is the metadata path's
  worst case; S3 expects every concurrent PUT to succeed (last writer
  wins). R-112.
- The map-version read, a third of a GET's transactions, costs about
  1.6 ms at 64-way concurrency and 26 bytes: it is cheap for FoundationDB
  and expensive for the gateway's single FoundationDB network thread
  (§12).

### 9b. S3-level metadata shapes (E13, measured)

E9 layout, one gateway, 64 connections, 30 s windows, 3 rotated rounds:
overwrite of 20,000 existing 4 KiB keys in an unversioned bucket 1,122
(953–1,259) PUTs/s and in a versioned bucket 1,261 (997–1,333) PUTs/s
(within the spread; 3.0–3.3 commits per PUT in both); a mix of 50 % GET
and 50 % PUT over hot keys: 758 GETs/s + 758 PUTs/s, 1.8–1.9 commits per
operation, no FoundationDB conflict (one writer per key by the
generator's design); multipart uploads of 20 MiB in four 5 MiB parts, 8
at a time: 10.5 (10.3–11.1) uploads/s (210 MiB/s, disk 95–97 %), 10–13
commits per upload. Final verifies of both buckets: 0 violations. Not
measured: ListObjectVersions, the GC index scan by itself (GC passes are
in §7).

## 10. Storage node without the S3 path (E8d, measured)

`nodebench` from host A against one storage node on B (its device a
virtual disk shared with three idle nodes), fresh cluster, 10 GiB
allotment, retention 0; `PutShard` over a bounded key set (about 256 MiB,
at most 8,192 keys: each put after the first of a key replaces it) then
`GetShard` of those keys (mostly from the page cache); windows 20 s (10 s
for 16 MiB, 5 s for 128 MiB) after a warm-up; 3 rotated rounds. A sample
with more than 1 % errors is invalid. Node CPU: the busiest node
process per second (pidstat).

| Size | Conc. | PUT MiB/s | PUT p50 / p95 / p99 ms | GET MiB/s | GET p50 / p95 / p99 ms | node cores PUT / GET |
| --- | --- | --- | --- | --- | --- | --- |
| 4 KiB | 1 | 2.0 (1.6–2.5) | 1.53 / 1.74 / 2.2 | 17.1 | 0.22 / 0.30 / 0.38 | 0.15 / 0.60 |
| 4 KiB | 8 | 12.1 (9.4–12.3) | 2.53 / 2.91 / 6.6 | 69.5 | 0.44 / 0.60 / 0.71 | 0.54 / 1.79 |
| 4 KiB | 32 | 23.4 (18.3–33.1) | 3.70 / 12.0 / 15.1 | 118 | 1.03 / 1.51 / 1.83 | 0.95 / 2.30 |
| 4 KiB | 128 | 55.2 (50.0–56.7) | 7.52 / 25.0 / 41.8 | 140 | 3.51 / 5.37 / 6.13 | 1.95 / 2.33 |
| 64 KiB | 1 | 34.6 (24.4–34.9) | 1.77 / 2.04 / 2.3 | 208 | 0.29 / 0.39 / 0.45 | 0.22 / 0.66 |
| 64 KiB | 8 | 84.8 (56.3–109) | 3.89 / 14.0 / 32.8 | 578 | 0.86 / 1.13 / 1.32 | 0.47 / 1.59 |
| 64 KiB | 32 | 154 (150–162) | 9.0 / 31.2 / 42.5 | 752 | 2.67 / 3.65 / 4.25 | 0.82 / 1.94 |
| 64 KiB | 128 | 216 (209–225) | 33.1 / 66.1 / 76.9 | 732 | 10.8 / 15.1 / 17.2 | 1.17 / 1.88 |
| 1 MiB | 1 | 120 (106–186) | 8.9 / 11.7 / 24.3 | 524 | 1.93 / 2.30 / 2.58 | 0.31 / 0.69 |
| 1 MiB | 8 | 236 (175–249) | 32.4 / 58.4 / 67.3 | 903 | 8.8 / 11.5 / 13.6 | 0.67 / 1.70 |
| 1 MiB | 32 | invalid (3/3) | | 913 | 34.7 / 49.9 / 58.2 | 1.10 / 1.74 |
| 16 MiB | 1 | 291 (291–299) | 51 / 70 / 86 | 822 | 19 / 21 / 23 | 0.44 / 1.02 |
| 16 MiB | 4 | 369 (2 valid) | 169 / 245 / 412 | 962 | 67 / 78 / 83 | 1.08 / 1.72 |
| 16 MiB | 16 | 425 (2 valid) | 569 / 896 / 1,074 | 981 | 260 / 297 / 312 | 1.26 / 1.93 |
| 128 MiB | 1 | 282 (2 valid) | 446 / 469 / 469 | 610 | 209 / 231 / 238 | 0.59 / 0.88 |
| 128 MiB | 4 | 461 (410–486) | 1,182 / 1,361 / 1,423 | 896 | 575 / 659 / 700 | 1.40 / 1.74 |

- Small writes are bound by the durable sync (one fsync group per
  write: 1.5 ms at concurrency 1, the disk's 4 KiB `dsync` cost); group
  commit raises 4 KiB to about 14,000 writes/s at 128 connections.
- Large writes reach 425–461 MiB/s, a third of the disk's sequential
  write rate measured with `dd`.
- The invalid samples were refused by the node's capacity admission while
  overwrites outran compaction (available space fell to the reserve;
  `ResourceExhausted: insufficient capacity`): sustained overwrite at
  full speed is limited by compaction (measured as refusals, not as a
  rate).
- Reads of 4 KiB reach 36,000/s (the node's CPU, 2.3 cores, is the
  limit), reads of 64 KiB and more about 750–980 MiB/s.
- Through S3 (§5, §6), the same layout gave 2,900–3,300 GETs/s of 4–64
  KiB and about 1,000–1,200 PUTs/s: the S3 path, not the node, limits
  small requests; for 1 MiB GET (700 MiB/s, host B's CPU full) the node
  path at about 900 MiB/s is close.
- Node RSS during these samples: 765–1,470 MiB as reported by pidstat
  (31–93 MiB in S3 runs, §12); the difference was not analysed (file
  mappings or buffers of large payloads are possibilities, not verified).

## 11. Data integrity under load and failures (measured)

- Every valid sample of every campaign: 0 integrity violations (bodies
  checked one read in ten, full length; ranges exact).
- E10, failures under mixed load (the E9 layout, 20,000 objects of
  16–256 KiB, 64 connections, 60 % reads (a fifth of them HEAD), 5 %
  deletes, 1 % listings, zipf keys, 240 s, `--retries 3`): a storage node
  killed with SIGKILL at 60 s and restarted 30 s later; the controller
  (the only one: the leader) killed with SIGKILL at 150 s and restarted
  10 s later. Results: 0 violations; GET, HEAD, DELETE and LIST without
  errors; 2,366 PUTs (4.5 %) answered 503 SlowDown after 3 retries while
  the node was away (not acknowledged: not counted as written); after
  quiescence a full verify of the 20,000 keys (2,326 deleted keys absent,
  as allowed; 0 mismatches, 0 unverified) and of the 20,000 unrelated
  4 KiB keys (0 missing).
- Unplanned: on the 10⁶-object cluster the four storage nodes were
  restarted with a command line without the plaintext switch; they exited
  at once and the cluster ran without any node for 33 minutes
  (17:30–18:03 UTC, recorded in the campaign log); restarted, the groups
  were `Active` at epoch 1 (no repair began: no target), admission went
  back to `normal` within 4 minutes, and a full verify of the 10⁶ keys
  found 0 missing and 0 mismatches.
- Object-count campaigns: §7 (full verifies at 765,134 and 10⁶ objects,
  scrubs with no finding).
- Incomplete writes are never counted as written: a PUT that ends in an
  error leaves the key "in flight" (either outcome accepted, never
  required); `verify` without `--allow-missing` fails on any absent key,
  with it the absent keys are counted. The 234,866 absent keys at 765,134
  objects equal the first population's 234,866 PUTs refused with 503
  (totals; that population kept no per-key state, so this is a match of
  totals, not of keys: a 503 of admission is answered before anything is
  written).

## 12. Profiles (E9b, measured; CPU only)

`perf record` (199 Hz, DWARF stacks) for 15 s of steady 4 KiB GET and PUT
at 64 connections through one gateway, of the gateway, a storage node and
FoundationDB, and `pidstat -t` per thread (E9 was discarded: its profiles
were taken after the runs had ended, a fixed sleep against a variable
quiescence wait).

- **Gateway (GET 4 KiB, 3,021 ops/s, 1.96 cores):** the FoundationDB
  client's network thread (one per process) used 79 % of a core and 40 %
  of the gateway's samples; `fdb_run_network` is 36 % of the samples
  inclusive. The four tokio workers used about 29 % each and waited for a
  CPU about as long (`%wait` 29–30 %: host B's run queue). System calls
  21 % (socket writes 7.6 %), allocation 2–3 % (`malloc`/`free`), lock
  slow paths about 1 % (spin lock) and futex 4.6 %. PUT: the same shape
  (network thread 42 % of samples).
- **Storage node:** kernel 19–21 % of samples (I/O and network), its own
  code 16–17 %, libc 6–7 %; no single hot function.
- **FoundationDB:** one main thread (98 % of its samples), 0.86–0.95
  cores during GET.

Conclusion (measured): one gateway's FoundationDB network thread at about
80 % of a core is the nearest per-process ceiling for small GETs (a
second gateway process adds another network thread: §5 shows more
gateways on the same 4-vCPU host do not help because the host is full;
on separate hosts this would be the first thing to test). Every metadata
transaction a request saves (map-version read, access record) is work
taken off that thread.

## 13. Controller and background passes at group and node scale (E12, measured in process; E7 on the cluster)

`background_scale` (ignored test, host B, release build): real storage
nodes on loopback, the in-memory metadata store, 64 objects of 64 KiB,
one controller with 1 s intervals, until each pass ran three times.
Memory metadata: pass times are lower bounds of the work per pass
(RPCs, encoding, scans), not FoundationDB figures (**simulated metadata
latency**).

| Nodes / groups | reconcile last (longest) ms | health longest ms | reclaim ms | GC, repair, rebalance ms | RSS growth MiB |
| --- | --- | --- | --- | --- | --- |
| 4 / 10 | 6 (37) | 72 | 0 | 0 | 18 |
| 8 / 100 | 81 (192) | 132 | 1 | 0 | 2 |
| 16 / 1,000 | 1,552 (1,927) | 288 | 10 | ≤ 1 | 8 |
| 32 / 1,000 | 3,203 (3,783) | 557 | 22–26 | ≤ 2 | 7 |
| 32 / 10,000 | 35,321 | 555 | 229–292 | ≤ 35 | 67 |

- Reconciliation costs about 1.2 ms per (group, member) pair even with
  almost no data (one shard listing RPC per pair, pairs one after
  another): 30,000 pairs take 35 s in process. With data it adds one
  metadata transaction per shard (§7: 3,566 s at 10⁶ objects on
  FoundationDB). Both terms are linear; neither is bounded per pass by
  default.
- Health, repair, GC, reclaim with nothing to reclaim and rebalancing
  stay below 0.6 s per pass at 10,000 groups: no pass of these starved
  (each task has its own scheduler; every task completed three passes
  within 108 s at 32 / 10,000).
- When a pass cannot keep up (measured on the cluster, §7):
  reconciliation from about 10⁵ objects (pass 293–526 s against 300 s),
  and at 10⁶ it runs about 92 % of the time; the reclaim pass needs about
  3–4 ms per waiting retirement, so deletes above roughly 250–300 per
  second sustained (on this environment; estimated from those rates)
  outrun one reclaim pass per interval and retained bytes grow.
- Interference with client I/O (measured): with reconciliation running,
  the controller used 0.3–0.9 cores on host A during client samples
  (§6, §7) and its metadata reads share FoundationDB; the effect on
  client throughput was not isolated (the 10⁵ cross-check differed by
  2.4 % for PUT and 21 % for LIST page time, with reconciliation busy in
  E7c).
- Cursor: reconciliation's cursor is per (group, node) pair (Phase 3.14);
  a pass interrupted inside a large pair repeats that pair (code
  analysis; review).


## 14. Optimisations made (measured A/B, regression tests)

| Change | Baseline → change | Evidence | Test that fails without it |
| --- | --- | --- | --- |
| Gateway sets `TCP_NODELAY` on accepted connections (`crates/gateway/src/server.rs`) | 1 KiB range GET at concurrency 8: 184 (184–185) → 716 (665–721) ops/s; p50 43.2 → 10.8 ms, p99 46.8 → 16.7 ms. 64 KiB range and 4 KiB GET: within the spread | E4c, 3 rotated rounds, the Phase 3.14 binary as baseline (`GW_BIN`) | `server::socket_tests::accepted_connections_have_nodelay` (fails with the option removed; it tests the function the accept loop calls, not the loop itself) |
| `gateway --access-records on|off` (default on: unchanged) | GET 4 KiB, 1 gateway, 20,000 objects: on 3,237 (3,210–3,312) → off 3,950 (3,851–4,146) ops/s, p99 29.1 → 24.8 ms, FoundationDB commits 2,784 → 3.7 /s | E4c, 3 rotated rounds | `tiering::a_gateway_without_access_records_writes_none_on_get` (both settings) |
| `controller --admission-horizon-secs` (default 3600: unchanged) | lets a small test cluster be filled at benchmark rates (E4b 1 MiB was refused at 32 of 2,000 objects with 3600) | E4c | default kept; no behaviour change |
| Controller pass metrics | new visibility only | §7, §10 | registry test (`metrics-registry.tsv` parse and naming) |

`--access-records off` is an operator switch, not a default: with it, a
gateway's reads are invisible to tiering (objects in an upper tier that
are read only through it look unread and are demoted); it is safe for
data and for cache eviction (which uses its own recency), and meant for
clusters without tiering or for every gateway together (ADR-0034 for the
real fix). TCP_NODELAY changes no data path: it only stops the kernel
from holding small response pieces.

Not changed (proposals): batched background judgments (ADR-0033),
batched access records (ADR-0034), the map-version read per request, the
delete reserve per small record (R-111).

## 15. Bottlenecks, ranked

Each: the conditions, what it costs (measured), the headroom if removed
(measured upper bound or estimate, labelled), difficulty, priority.

| # | Bottleneck | Conditions | Measured cost | Headroom | Difficulty | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| B1 | Access record per GET | many objects read once per second or less | +0.75–1.0 commit per GET; GET 4 KiB 3,237 → 3,950 ops/s with records off; GET falls 3,347 → 2,398 from 10³ to 10⁶ objects | up to +22 % GET 4 KiB (measured with records off) | medium (ADR-0034) | 4.1 #1 |
| B2 | Reconciliation O(stored shards), sequential | ≥ 10⁵ objects | 3,566 s per pass at 10⁶ (interval 300 s); 0.3–0.9 controller cores and FoundationDB reads during client I/O | pass time ÷ (batch × parallelism) — estimate | medium (ADR-0033) | 4.1 #2 |
| B3 | Reclaim pass per waiting retirement | deletes ≥ ~250–300/s sustained | 3–4 ms per retirement, 1,094 s pass at 0.25 M | as B2 — estimate | medium (ADR-0033) | 4.1 #2 |
| B4 | Gateway's FoundationDB network thread | small requests, one gateway process | 79 % of a core at 3,021 GETs/s; GET 64 KiB saturates at ~3,300 ops/s | fewer transactions per request (B1, B5); more gateway processes on more hosts (not measurable here) | medium | 4.1 #3, 4.2 #1 |
| B5 | Map-version read per request | every GET/PUT | 1 of ~3 transactions per GET, 1 of ~4 per PUT | −1 transaction per request — estimate | ADR needed | 4.1 #3 |
| B6 | Small-object capacity (alignment + delete reserve) | objects ≪ 4 KiB | ≈ 24 KiB of node capacity per 1 KiB object; tier refused writes at 765,134 objects with 0.59 GB live per node | reserve by real tombstone size: up to about half (estimate); packing: format change | medium / high | 4.1 #4, 4.2 #3 |
| B7 | Shared host CPU (test layout) | gateways and nodes on one 4-vCPU host | host B 92–99 % at GET 1 MiB and GET 4 KiB × 4 gateways | environment, not code | — | 4.2 #1 |
| B8 | Durable write sync (node) | PUT of small objects | 1.5 ms per 4 KiB write at concurrency 1; PUT through S3 ≈ 1,000/s, disk 85–96 % | group commit already; dedicated disks | environment | 4.2 #1 |
| B9 | Concurrent overwrite of one key | hot-key writers | 56 % of PUT commits fail at 64 writers (metadata path) | design (R-112) | medium | 4.1 |
| B10 | ListObjectsV2 reads 2 × rows | full pages | 2,000 rows read per 1,000-key page | up to −50 % rows read per page — estimate; delete markers make it non-trivial (review) | low | 4.1 (with tests) |
| B11 | Admission horizon on small clusters | fast fill of a small tier | 1 MiB population throttled at 32 of 2,000 objects (3,600 s horizon) | — | low | 4.1 #5 |
| B12 | Compaction vs sustained overwrite on a node | overwrite at full speed | capacity refusals at 1 MiB × 32 and 16 MiB × 16 in E8d | not measured as a rate | — | 4.2 |

Not bottlenecks here (measured): the generator (≤ 0.40 of its threads);
FoundationDB as one process (3 processes: no gain, §8); the network
(≤ 5.9 Gbit/s of 22); lock contention and allocation in the gateway
(about 1 % and 2–3 % of CPU samples).

## 16. Independent reviews

Two reviews, two rounds each (a Claude subagent and another model).

Round 1 (benchmark framework, product changes): two High (integrity
check accepting truncated or empty bodies; `verify` passing when reads
failed with 5xx) and Mediums (summaries not aligned to the measured
window; clients other than the first and last not waited for; quiescence
passing with a node unobserved; metaops counter windows; nodebench reads
from page cache; latency of a closed-loop generator), Lows. All fixed or
documented before the later campaigns (§2), each High with a test that
fails without the fix (`short_bodies_and_ranges_do_not_pass`; `verify`
exit 5 on any unverified key).

Round 2 (fixes, retries, access records, controller liveness, object
scale): the fixes confirmed. New: **High** — a retried ssh could start a
second generator for one run (the connection drops after the remote side
started it); fixed with launch markers for every command that must not
run twice (generators, samplers, process starts, `register-node`,
`create-groups`, scrub, which is now detached and timed on its host).
Mediums: a population could pass failed PUTs (the final verifies without
`--allow-missing` are the check: §7, §11); the access-record race with GC
is real (an orphan access key of about 50 bytes; no effect on data or on
GC; R-109); `--access-records off` makes upper-tier objects look unread;
an upper-tier object never read may be demoted before `demote_after`
(code analysis; R-109); rebalancing's settle time shorter than the
measured reconcile cycle (R-110); freed space returns late; the delete
reserve dominates small objects' capacity (R-111); reconciliation's
background load inside client samples at 10⁵–10⁶ (recorded per sample
as the controller's CPU). No Critical. No finding against R-94(b): a slow
reclaim only keeps data longer; deletes stay fenced by epoch, record token
and incarnation.

## 17. R-94, R-104, R-106, R-108

- **R-94(b)** stays **Medium, bounded**. Nothing in Phase 4.0 changed
  deletion, retirement or reclamation. The slow reclaim pass at scale
  only keeps retired records longer; deletes stay fenced (epoch, record
  token, incarnation); a long reconciliation pass works from older group
  records but its deletes carry their epoch and token (both reviews). The
  deferred-reclamation tests and the system-model seeds were not changed
  (CI runs them).
- **R-104** (background liveness) stays **Medium**, with new
  measurements: reconciliation's cycle exceeds its interval from about
  10⁵ objects (3,566 s at 10⁶) and rebalancing's settle time is then too
  short; no task starved at 10,000 groups in process; listed as R-110 for
  the scale part (ADR-0033).
- **R-106** (deferred reclamation's operating requirements) stays
  **Medium**: the reclaim pass's cost per waiting retirement (3–4 ms on
  this environment) bounds the sustained delete rate that retention can
  absorb without retained bytes growing; capacity planning must add it to
  the retention window (§13).
- **R-108** (node heap growth in the Phase 3.14 soak): **explained as
  state, not a leak** (measured). (1) The Phase 3.14 soak's heap fits
  1.4–1.9 MiB + 128–153 B per index entry (live, dead and pending
  retirements), correlation 0.75–0.90, residual trend +28–61 KiB/h. (2)
  A 30-minute run with the live set bounded (`memory_churn`, `unique`
  mode, about 1,030 live records, 528,403 operations, 274 GiB written,
  every delete accepted after the test's fence fix): heap in use between
  2.8 and 22.3 MiB without an upward trend (slope −2.2 MiB/h), anonymous
  RSS 60.5–77.7 MiB (slope +8 MiB/h over 30 minutes, inside the
  allocator's free-list swings of 48–67 MiB: not separable in that time),
  file descriptors 17–21, threads 26. Severity **Low**; the 24 h soak on
  dedicated storage stays the way to close it.

## 18. Y1–Y16

Phase 4.0's sixteen requirements, in the order of the plan; judgement,
environment, runs, scale, evidence and gaps. Environment for all: §1.

| Y | Requirement | Judgement | Runs and scale | Evidence | Gaps |
| --- | --- | --- | --- | --- | --- |
| Y1 | Benchmark framework (workloads, parameters, metrics, generator not the bottleneck) | Verified with limitations | E1 9 runs; every campaign | §2, §4 | closed-loop generator; no ListObjectVersions workload |
| Y2 | Object count 10³–10⁸ (≥ 10⁶ measured) | Verified with limitations | E7, E7c: 10³, 10⁴, 10⁵, 7.65 × 10⁵, 10⁶ through S3; E11: 10⁷ metadata-only | §7, §9 | 10⁷ through S3 and 10⁸ estimated; DELETE at 10⁶ not valid; three cells with two samples |
| Y3 | Metadata analysis (transactions, amplification, key patterns, per-op table) | Verified with limitations | E11 168 samples, E13 12 | §9 | GC index scan and ListObjectVersions not measured by themselves; conflicts measured in the metadata path only |
| Y4 | Gateway 1/2/4 and the shared bottleneck | Verified with limitations | E4b, E4c 36 + 18 | §5, §8 | all gateways on one 4-vCPU host |
| Y5 | Storage node 4K–128M, direct API | Verified with limitations | E8d 32 configurations × 3 | §10 | one node; reads from page cache; some large-write samples invalid (capacity) |
| Y6 | Concurrency 1–1,024, saturation, p99 knee | Verified with limitations | E6 36 | §6 | PUT at 512/1,024 invalid (capacity) |
| Y7 | Background operations 10–10,000 groups, 4–32 nodes, keep-up, interference | Verified with limitations | E12 5 configurations (in process), E7 pass metrics | §7, §13 | in-process metadata for the group scale; interference not isolated; tiering, repair and rebalancing with data at scale not exercised |
| Y8 | Integrity under load (sampling + full check) | Verified | every valid run; E10; full verifies at 765,134 and 10⁶; scrubs | §11 | one scrub group's figure lost at 765,134 |
| Y9 | R-94(b) Medium/bounded kept | Verified (by not changing it; reviews) | — | §17 | CI runs the model seeds and deferred-reclamation tests |
| Y10 | R-104 liveness | Verified with limitations | E7, E12 | §13, §17 | the scale part is open (R-110) |
| Y11 | R-106 retention cost | Verified with limitations | E7 reclaim pass | §13, §17 | sustained-delete limit estimated from pass rates |
| Y12 | R-108 heap: leak vs state | Verified with limitations | soak data; E12 30 min | §17 | 30 minutes, not 24 h |
| Y13 | Profiling and bottleneck analysis | Verified with limitations | E9b 6 profiles; thread CPU | §12, §15 | CPU only (no allocation or off-CPU profile); controller not profiled (pass metrics and code analysis instead) |
| Y14 | Limited optimisations with A/B and regression tests | Verified | E4c | §14 | — |
| Y15 | Benchmark reliability (criteria fixed, rotation, ≥ 3 samples, invalid runs recorded) | Verified with limitations | all | §1, §3 | three cells with two valid samples; one wording erratum to rule 8 |
| Y16 | Two independent reviews, gates, documents | Verified with limitations | 2 reviewers × 2 rounds + follow-ups | §16; CI | see the final report for CI; a flaky test seen locally (below) |

## 19. Phase 4.1 and 4.2

The plan and its order are in `docs/roadmap/phase-4.md`, derived from
§15; ADR-0033 and ADR-0034 are the proposals for its first two items.

## 20. Not verified, and where the data is

Not verified in Phase 4.0: 10⁷ objects through S3 and 10⁸ in any form
(estimated, §7, §9); gateways, nodes and FoundationDB on separate hosts;
physical disks; open-loop latency; ListObjectVersions and lifecycle;
tiering and repair at object scale; allocation and off-CPU profiles; the
24 h soak and power loss (not conditions of this phase).

Local test observation: three `managed_restore` integration tests failed
together twice on host A in this tree (`cluster incarnation changed since
this gateway started`) and passed in eight other runs of the same tree,
and in ten runs of the Phase 3.14 tree with the same crate sources; the
failing code path is not touched by Phase 4.0. Recorded as intermittent;
CI is the gate.

Raw results stayed on the operator's machine (they hold host-specific
data); the summaries in this report were produced by
`benchmarks/linux/summarize_bench.py` from them.
