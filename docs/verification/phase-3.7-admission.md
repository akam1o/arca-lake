# Phase 3.7 admission refinement verification

Deterministic in-process tests carry most of this phase's evidence;
short runs on one validation VM complement them. As in Phase 3.6, no
long-duration run was made (the VMs' shared virtual storage stalls
FoundationDB under sustained sync writes, R-79): nothing here verifies
behaviour beyond about 30 minutes.

## In-process tests (every push, CI)

| Test | What it shows |
| --- | --- |
| `capacity::unknown_length_puts_and_copies_are_charged_by_what_they_write` | PUTs without a length and copies charged by the chunks they write; a throttled writer waits per chunk; contended writers refused part way leave nothing visible and nothing behind after GC |
| `capacity::a_new_term_keeps_the_published_state_until_every_member_reported` | for each published state (`normal`, `throttled`, `write_rejected`, `recovery_only`) a new term with a missing member keeps at least it; past the staleness bound the usual rules; once all report, it converges to `normal` one step per pass; no record: at least `throttled` |
| `capacity::a_new_term_keeps_published_exclusions_until_every_member_reported` | published group exclusions kept during warm-up |
| `capacity::a_former_leader_cannot_publish_admission` | a former leader's delayed publication fails (fenced store) |
| `capacity::nodes_sharing_a_file_system_without_allotments_leave_placement` | shared file system without fitting allotments: conflict, groups out of placement, `SlowDown` with the reason; allowed explicitly: placed again, still reported |
| `capacity::new_chunks_prefer_the_group_with_more_room` | 20 of 20 chunks in the group with more room |
| `multipart_backpressure::*` (in-memory; FoundationDB in CI) | six concurrent uploads with UploadPart and UploadPartCopy under a 2 MiB/s throttle with SDK-like retries, two aborted; under `write_rejected` new uploads and parts refused, completion and abort accepted; after deletes, GC, the multipart sweep and reconciliation exactly the live objects' shards remain and no node holds capacity |
| `refused_bodies::large_writes_refused_for_capacity_reach_s3_clients_as_slowdown` | AWS SDK for Rust, AWS CLI v2, boto3: 1/8/32/64 MiB PUTs and an 8 MiB UploadPart refused as `503 SlowDown` with `Retry-After`, nothing visible, the SDK's connection reused, the upload completes once admitted |
| gateway `admission::tests` | per-chunk rate over gateways, full chunk debt, header check charges nothing, former leader's record ignored, restored lower token taken when clearly later, aging |
| controller `admission::tests`, `backpressure::tests` | shared-domain rules, trends and windows, recent-trend exit, AIMD bounds, no relaxing or speeding up without recent evidence, stale report returns the rate to the formula |
| `mixed_version::rolling_upgrade_with_two_binaries`, `membership_faults::fdb::*`, `fdb_failover`, `fdb_restore` (CI) | existing upgrade, membership, failover and recovery suites on the final build |

## Validation VM runs

Environment: one validation VM (4 vCPU, 7.8 GiB RAM, virtual disks on
shared backing storage), the soak layout of `benchmarks/linux/soak24.sh`
(4 durable + 2 cache nodes, 2 gateways, 2 controllers, 3 FoundationDB
processes, TLS on every RPC) via `benchmarks/linux/capacity-tests.sh`.

### Test C — capacity exhaustion (build before the review fixes)

Each durable node on its own 2 GiB ext4 loop file system (reserve
486 MiB). 1 MiB PUTs at 40/s with 20 % GETs until refused, read-back
while full, delete everything, then 10 PUT/s until full again.

| | Result |
| --- | --- |
| acknowledged before refusal / read back while full | 2 863 / 2 863, 0 missing, 0 wrong |
| deleted | 2 863, 0 failures |
| writes again after the deletes | yes, 8.9 PUT/s of 10 within 4 minutes, until full again; 2 013 acknowledged, all read back |
| node client refusals | 650 and 648 on the two fullest nodes, 0 on the others |
| lowest free space on a node | 475 MiB (reserve 486 MiB) |
| reservations held at the end | 0 on every node |
| admission states seen | `normal`, `warning`, `throttled`, `write_rejected`; back to `normal` after the deletes without intervention |

The two fullest nodes went 11 MiB (2.3 %) into their reserve: client
writes were refused there (650 refusals), so the bytes came from writes
the reserve exists for (compaction output, segment rollover, GC
records); which ones was not recorded. In Phase 3.6 the gateway's
throttle stopped writes before the nodes had to (0 node refusals); with
per-chunk charging and recent-trend recovery the nodes' reserve was the
stop.

### Test D — failure injection

`soak24.sh` workload (acknowledged-write ledger: PUT, GET, ranges,
DELETE, versioning, multipart, copies, conditional writes) on nodes
capped at 4 GiB, a fault every 2 minutes, then 10 minutes of settling
and the final checks.

| | Run 1 (before the review fixes, 20 min) | Run 2 (final build, 12 min) |
| --- | --- | --- |
| faults | node restart, gateway kill, FoundationDB process restart, network delay, node kill, cache node kill, network blackhole; **leader kill skipped** (harness bug below) | node restart, gateway kill, **leader kill**, FoundationDB process restart, network delay |
| ledger keys verified / violations | 1 440 / 0 | 1 440 / 0 |
| operations / errors | 56 407 / 180 (transient, during faults) | 40 686 / 98 |
| final scrub | no missing, corrupt or misplaced shard; 12 unexpected (unreferenced, awaiting GC) | same; 33 unexpected |
| repair queue at the end | 0, 0 unrecoverable | 0, 0 unrecoverable |
| admission | `normal`/`warning`/`throttled` on time to full, never refused | same; after the leader kill the new leader (token 2) published the states it found (`throttled`, `warning`) and left them only through the hysteresis |

Harness bug found in this phase: since the leader record names the
process as `<id>@<instance>` (Phase 3.5), `soak24.sh` found no PID file
for the leader and logged every leader kill as skipped. Affected: the
Phase 3.5 soak and Phase 3.6 Test D (no leader kill happened in them).
Fixed; run 2 above includes one.

### Responsiveness

See [RFC-009 §6.21](../rfcs/RFC-009-performance-architecture.md)
(`capacity_bench`, 3 runs per policy on the final build).

## Not verified

- Anything longer than about 30 minutes; physical storage.
- Multipart saturation (Phase 3.6 Test B) on real FoundationDB at load:
  covered by `multipart_backpressure` on FoundationDB (in CI) and in
  memory, not by a load run.
- Leader kill under capacity pressure (`write_rejected`,
  `recovery_only`) on the validation hosts: covered in-process only.
- FoundationDB quorum loss during backpressure: covered by the existing
  failover suite, not combined with capacity pressure.
- Repair and compaction ENOSPC: the existing Phase 3.6 tests
  (`capacity::*`), not re-run on the VM.
- Refused bodies on a real network or over HTTP/2.
