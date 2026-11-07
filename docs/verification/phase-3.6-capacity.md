# Phase 3.6 capacity and backpressure verification

Short, dense runs replace the 24 h soak for this phase (the validation
VMs' shared virtual storage stalls FoundationDB under sustained sync
writes, R-79). What a run of 30–60 minutes shows is stated per test;
long-duration operation is **not** re-verified here.

Environment: one validation VM (4 vCPU, 7.8 GiB RAM, two virtual disks
on shared backing storage, writeback cache), the soak cluster layout of
`benchmarks/linux/soak24.sh` (4 durable + 2 cache nodes, 2 gateways,
2 controllers, 3 FoundationDB processes, TLS on every RPC), started by
`benchmarks/linux/capacity-tests.sh`. GC every 10 s with a 60 s grace,
admission dwell 60 s, chunks of 1 MiB, 2 + 1 groups (two in tier
`default`, one in `cold`, all on the same four durable nodes).
Load: `gcload` (AWS Rust SDK, standard retries); every acknowledged key
is read back at the end (`--verify`).

## Test A — GC saturation (final build)

Durable nodes capped at 4 GiB (`--capacity-bytes`); 256 KiB overwrites of
4 000 keys with 10 % GETs, target 100 → 1 600 ops/s in 5-minute steps,
then 50 ops/s for 10 minutes.

| Step (ops/s) | Achieved | `SlowDown` | Other errors | PUT p50 / p95 / p99 ms | GET p50 / p99 ms |
| --- | --- | --- | --- | --- | --- |
| 100 | 90.1 | 0 | 0 | 98 / 215 / 446 | 1.5 / 4.3 |
| 200 | 55.4 | 37 402 | 0 | 108 / 268 / 901 | 1.5 / 3.9 |
| 400 | 46.6 | 94 099 | 0 | 145 / 233 / 704 | 1.5 / 10.9 |
| 800 | 39.2 | 204 220 | 0 | 153 / 276 / 560 | 1.6 / 19.8 |
| 1 600 | 46.5 | 418 056 | 0 | 152 / 218 / 322 | 1.9 / 26.0 |
| 50 (load lowered) | 5.6 | 23 664 | 0 | 28 / 540 / 1 037 | 1.5 / 2.2 |

Read-back: 2 144 keys, 0 missing, 0 wrong. Worst state `throttled`
(17 transitions, between `warning` and `throttled`); nodes never refused a
write (`node_capacity_refusals` 0); a durable node's free space never fell
below 1 012 MiB (reserve 576 MiB). Backlog at most 8 790 generations
(2.1 GiB), oldest entry at most 63 s (the grace); 83 337 generations and
30.5 GiB collected; backlog empty 90 s after the load stopped. Node heap
at most 44 MiB.

Limitation: on these 4 GiB devices the throttle holds 256 KiB writes at
about 40–55 ops/s (the time-to-full rule keeps the tier an hour from
full), and after the load is lowered the state returns to `normal` only
after the 5-minute trend window and the dwell (the 50 ops/s step
achieved 5.6 ops/s). The first run, with the reclaim-rate-only throttle,
ratcheted down to 26 ops/s (fixed in `f9df41b`).

## Test B — multipart saturation

Two-part uploads of 1 MiB parts over 1 000 keys (create, upload,
complete; abort on failure), 10 % GETs, 10 → 160 uploads/s, then 10 for
10 minutes, nodes capped at 4 GiB.

Run 1 (build before the review fixes): achieved 9.0, 0.4, 1.3, 12.7, 7.6
uploads/s with 5 335–39 584 `SlowDown` per step and 0 other errors;
read-back of 697 objects, 0 missing, 0 wrong; worst state
`write_rejected` (briefly, no group with room); nodes never refused a
write; the retired queue's oldest entry stayed at the grace (59–60 s),
backlog at most 1 337 generations: multipart objects are collected one
grace after deletion (before Phase 3.6, two grace periods).

Run 2 (final build) is **not counted**: FoundationDB stopped serving
(`IoTimeoutError`, 22 recoveries; R-79) 10 minutes in; gateways got
transaction timeouts (1031), the admission record stopped changing and
gateways refused new data (fail closed), reads timed out. It shows the
fail-closed path, not capacity behaviour. Run 3, with FoundationDB on
the other virtual disk, stopped the same way in its first minute
(`IoTimeoutError`): Test B is **not verified on the final build** in this
environment. In both runs the uploads acknowledged before the stall were
not read back (the script verified only the last step; it now verifies
every step).

## Test C — capacity exhaustion (isolated file systems)

Each durable node on its own 2 GiB ext4 file system (loop device on the
test disk; reserve 486 MiB = a quarter of the device), cache nodes
unchanged. 1 MiB PUTs of new keys at 40/s with 20 % GETs for 8 minutes,
read-back while full, delete everything, then 10 PUT/s for 10 minutes.

| | Run 1 | Run 2 (final build) |
| --- | --- | --- |
| acknowledged before refusal | 2 834 | 2 766 |
| read-back while full | 2 834 / 0 missing / 0 wrong | 2 766 / 0 / 0 |
| node capacity refusals | 0 | 0 |
| lowest free space on a node (reserve) | 501 MiB (486) | 487 MiB (486) |
| refusal path | `normal` → `write_rejected` (no group has room) | same |
| after delete | GC collected 2 834 in 3 s; `write_rejected` → `throttled` → `warning` → `normal` one step per dwell | same |
| writes after recovery | read-back 1 827 / 0 / 0 | 1 921 / 0 / 0 |
| connection errors on refused PUTs | 15 (no body read before the 503) | 0 |

Two of four nodes reached the reserve while the other two were at
37 %: each group spans both full nodes (R-82). States `warning` and
`throttled` were skipped on the way down: 2 GiB devices fill within one
health interval at this rate (R-83); the nodes' reserve held. In run 2
the recovery writes filled the two full members again and the tier went
back to `write_rejected` at the end, as it should.

## Test D — failure injection (build before the review fixes)

`soak24.sh` workload (acknowledged-write ledger, mixed PUT/GET/DELETE,
versioning, multipart, copies, conditional requests) for 40 minutes on
nodes capped at 4 GiB, with a fault every 2 minutes: node restart and
kill, gateway kill, controller restart, FoundationDB process restart,
network delay, blackhole and reset on one node, a corrupted shard,
read and write EIO, cache node kill, certificate rotation. Then 10
minutes of settling and the final checks.

Ledger violations: 0. Manifest verification: 37 roots, 0 findings;
scrub: 0 findings. Admission moved between `normal`, `warning` and
`throttled` (time to full) and never refused; the workload saw errors
during faults (for example 18 of 444 PUTs in one fault minute), none
of them a lost acknowledged write.
Process memory ended within the ranges of the start (storage nodes
55 MiB RSS, controllers 37–57 MiB).

## Not verified

- 24 h or longer operation with backpressure active.
- Physical devices; a disk failure during exhaustion; several nodes on
  one file system (R-80).
- Test B on the final build (FoundationDB stall, R-79), and Test D on the
  final build.
- Throughput with backpressure on devices larger than a few GiB.
