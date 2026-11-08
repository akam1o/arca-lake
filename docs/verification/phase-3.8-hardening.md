# Phase 3.8 pre-publication hardening verification

Deterministic tests (in-process clusters, fault injection, a system
model, a fluid simulator of the backpressure policy) carry most of the
evidence; short measurements on the two validation VMs complement them.
Nothing here verifies behaviour beyond 30 minutes or on physical
storage (R-79, R-86).

## Capacity reserve (H1, H2)

**Cause of the Phase 3.7 finding.** Test C's samples (one node, every
10 s): during the fill, client writes held the node's free space at
exactly the reserve (486 MiB) for five minutes. The 11 MiB went during
the drain, when every object was deleted: allocated bytes rose
1 442 → 1 453 MiB while live bytes fell to 0, about 2 860 shards × one
tombstone record of 4 KiB (the record alignment). Deletes are never
refused (they are how space comes back), so their tombstones came out of
the reserve. Client PUTs never crossed it.

**Fix.** A delete reserve (one tombstone per record held plus 1 024)
under both floors, held per admitted write and sampled with the free
space under the admission lock; deletes of keys the store does not hold
write nothing. Layers and floors: RFC-008 §6.10.

| Test | What it shows |
| --- | --- |
| `capacity::deleting_everything_on_a_full_node_stays_within_the_delete_reserve` | the Phase 3.7 finding reproduced: fill with 32 KiB objects to the reserve, restart a node while full (nothing held, same delete reserve), delete everything, GC without compaction: the plain reserve stays free, tombstones accounted; fails without the delete reserve |
| `io_faults_cluster::gc_tombstones_hitting_enospc_are_deferred_and_collected_once_space_returns` | ENOSPC on every tombstone: the generation is deferred, nothing deleted, live objects read, collected once space returns |
| `io_faults::deleting_an_absent_key_writes_nothing` | repeated deletes of absent keys write no tombstone; reopen consistent |
| existing `capacity::*`, storage `io_faults::*`, storage-node `capacity.rs` | client/repair/migration/cache floors, repair and compaction ENOSPC, rollover ENOSPC, concurrent gateways, leader change, recovery after space returns |

Breakdown now reported per node: `arca_node_written_bytes_total{kind}`
(client, repair, migration, cache; data records, tombstones, rollover
headers, compaction output, quarantine log, sidecar) and
`arca_node_device_usage_bytes{kind}` (segments, side files, file system
used and overhead, delete reserve).

## Backpressure fencing (H3, H4)

Records carry the incarnation, the leader term (fencing token) and the
generation; gateways take only their incarnation's, never a lower (term,
generation); vanished or ignored records age; no record since start
fails closed after 60 s; a tier the record does not name is throttled.

| Test | Condition |
| --- | --- |
| gateway `admission::tests::a_former_leaders_record_is_not_taken` | older term, even computed an hour later; older generation of the same term |
| `…::another_incarnations_record_is_never_taken` | records of another incarnation (higher and lower token) |
| `…::a_vanished_or_missing_record_fails_closed` | no record (grace, throttled, refused), record removed, unnamed tier |
| `capacity::only_newer_records_of_this_incarnation_lift_a_refusal` | end to end through the gateway: other incarnation, older term, removed record do not lift `write_rejected`; a newer term does |
| `capacity::a_new_term_keeps_*` (Phase 3.7, extended) | leader failover in each state, published exclusions kept during warm-up |
| `capacity::a_former_leader_cannot_publish_admission` | old leader's delayed write |
| `system_model` `FormerLeaderGc` | a fenced former leader's GC collects nothing |

FDB quorum loss and gateway/controller partitions keep the last record
ageing (refresh failures keep the last state; the existing
`fdb_failover` suite covers quorum loss for the data path).

## Data-removal and data-movement invariants (H5, H6)

`system_model::random_operation_sequences_agree_with_the_model`: random
seeded sequences of PUT, overwrite, DELETE, multipart complete and
abort, GC with the multipart sweep, reconciliation, compaction, a lost
shard found by scrub and rebuilt by repair, chunk migration (clean,
crashed before and after the switch), cache population and eviction, and
a former leader's fenced GC, on a six-node cluster with two 2+1 groups,
replicated small objects and a cache tier, against a key → bytes model.
Every step: every key reads what the model says. End: exactly the live
objects' shards remain, no node holds capacity. Locally 60 seeds × 100
steps passed; CI runs 8 × 80 per push. With repair disabled the model
fails on the shard count and records the seed
(`$CARGO_TARGET_TMPDIR/system-model-failures.txt`, replay with
`ARCA_MODEL_SEED`).

The ten invariants of the phase plan, with the cross-model audit's
verdict on the final code: GC never deletes a needed shard; referenced
multipart parts are kept; compaction keeps needed tombstones (also with
the absent-key change); delayed deletes carry record tokens; a crash
during migration or rebalancing keeps the durable placement; repair and
moves are fenced by group epochs (the switch now also checks the
target's); the cache never serves a stale copy; deletion stops outside
`Active` (since Phase 3.8 also for read-only reference checks, R-88 for
what remains); CommitUnknown retries are not applied twice; a former
leader's operations are fenced.

## Rebalancing (H7, H8, H9)

| Test (`rebalance.rs`) | What it shows |
| --- | --- |
| `rebalancing_moves_chunks_to_the_roomier_group_while_clients_and_gc_go_on` | dry run plans 0-move, pause moves nothing; moves under concurrent GETs (every read whole), PUTs, deletes and GC; budget per pass held; imbalance smaller after reconciliation and compaction; every object reads back |
| `a_new_leader_resumes_rebalancing_at_the_cursor` | a crashed move before its switch, then two passes as different "leaders": cursor persisted and advanced, the crashed intent expired, each chunk moved once, exactly one copy left after reconciliation |
| `rebalancing_waits_for_repair_and_stops_at_the_targets_reserve` | a Degraded group: no pass; target nodes near their reserve: the pass stops, no node below its reserve, nothing lost |
| `a_read_started_before_a_move_follows_the_chunk` | a GET whose manifest predates the move, sources reconciled away mid-read: whole body, no false integrity report |
| `a_move_whose_target_changed_epoch_is_abandoned` | target epoch bumped between copy and switch: switch refused, copy deleted |
| `an_expired_move_never_deletes_its_successors_shards` | an attempt stalled past its intent's expiry while another moves the chunk: every target member keeps its shard |
| `a_chunk_larger_than_the_budget_still_moves` | one chunk per pass at least |
| controller `rebalance::tests` | plan ranking, shapes, hysteresis, non-Active groups |

Not run on the validation hosts under load.

## Backpressure responsiveness (H10)

`backpressure_sim` (results in
`benchmarks/linux/results/backpressure-sim-phase38.txt`): nine
conditions, reports every 1 and 10 s, Phase 3.6, 3.7 and 3.8 policies.
The one needless restriction found (after a burst with free space
growing: 3.7 re-entered `throttled` on the long trend right after the
recent trend lifted it) went from 200–204 s and 7 transitions to
60–82 s and 3; every other condition is unchanged; no node refusal and no
reserve crossing in any. The simulator is a fluid model, not a
measurement.

## Storage node resources (H11)

`memory_churn` (release build, validation VM, 30 min per mode, 16
clients, shards of 64 KiB–1 MiB; per-minute samples in
`benchmarks/linux/results/node-resources-phase38/`):

| Mode | Ops | Anonymous RSS MiB (min–max after warm-up) | FDs | Threads | Segments | Index live / tombstone history |
| --- | --- | --- | --- | --- | --- | --- |
| unique (soak pattern) | 575 236 | 67–80, flat | 17–19 | 28 | 3–4 | ~1 030 / 0 |
| churn | 807 349 | 71–76, flat | 26–34 (with the segment count) | 26–27 | 13–18 | ~760 / 0 |

Memory, descriptors, threads and index sizes stay bounded with the work
done; descriptors track the segments, which compaction keeps bounded.
Run on the build before the review fixes (none of which touches these
structures).

## Performance (H12, regression)

Two-host regression check (`regress.sh lab-2host`, 3 repetitions):
every indicator within tolerance except the cache hit ratio (0.80
against 0.93: environment, identical with older binaries since Phase
3.1) and multipart PUT (158 against 201 MiB/s). An A/B against the Phase
3.7 binary on the same hosts, alternating per repetition: Phase 3.8 120–
292 MiB/s (median 201, 7 runs), Phase 3.7 145–287 MiB/s (median 192, 6
runs; one setup failed): the same bimodal spread (R-48), no regression.

CPU profile (system-wide `perf`, 480 × 16 MiB PUT at 218 MiB/s, warm
GET at 637–654 MiB/s; `benchmarks/linux/results/cpuprofile-phase38/`),
share of samples in the top symbols:

| Host / phase | Kernel (network, file system) | memcpy | CRC-32C | MD5 (ETag) | Erasure coding |
| --- | --- | --- | --- | --- | --- |
| gateway host, PUT | 21 % | 16 % | 8 % | 17.5 % | 5 % |
| node host, PUT | 27 % | 12 % | 9 % | — | — |
| gateway host, GET | 29 % | 17 % | 5 % | 5 % (attribution unconfirmed: the load client verifies MD5 on the same host) | 1 % |
| node host, GET | 31 % | 15 % | 8 % | — | — |

Bottlenecks: copies and the kernel's network and file system paths,
then MD5 of single-part ETags on the gateway (S3 semantics require it)
and CRC-32C (hardware). No optimization was adopted: none was measured
to help on these hosts; candidates are in RFC-009 §6.22.

## Independent review (H13)

Two models reviewed the Phase 3.8 code and the cross-cutting removal
and movement state machines, then the fixes, twice. Fixed: one Critical
(an expired migration attempt could delete its successor's committed
records), three High (deletes deciding after the cluster left `Active`;
a switch not checking the target's epoch; reads failing on moved
chunks), seven Medium and four Low; each with a regression test except
the write-error path of the Critical (guarded by the same check, by
inspection). One finding stays open as R-88, rated High by one reviewer
and acceptable by the other: a delete decided just before a phase flip
or a restore is still sent afterwards.

## Not verified

- Anything longer than 30 minutes; physical storage.
- Rebalancing on the validation hosts; leader kills under capacity
  pressure on the hosts; FDB quorum loss combined with capacity
  pressure.
- The backpressure changes on real load (the simulator only).
