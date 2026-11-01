# Network fault injection on Linux: plan and expected behaviour

Status: **plan written before the runs**; results are recorded in
`benchmarks/reports/` and summarized in `docs/roadmap/phase-2-1.md`.
This document fixes, in advance, what PUT, GET, repair and GC must do
under each fault, so that a run is judged against a prior statement and
not against what happened.

## Environment and isolation

Two kinds of runs are used; neither touches an existing interface,
firewall rule, or route of the test hosts, and neither can affect the SSH
path (RFC-008 §9, R-40):

| Run | Where | What is real | Limitation |
| --- | --- | --- | --- |
| Two-host | two Linux hosts over their LAN interface | real NICs, real switch, two kernels, process restarts and crashes | no impairment: the only path between the hosts is also the SSH path, and shaping it would require changing the hosts' interface or firewall configuration |
| Namespace pair | one Linux host, two network namespaces joined by a `veth` pair created for the run | real TCP/IP stacks, real `sch_netem`/`tbf` on the veth ends, link down for partitions, kernel socket destruction for resets | one kernel; the veth has no NIC offloads or switch in between; bandwidth is memory-bound, so only the *impaired* cases are meaningful |

The namespace pair is created and destroyed by
`deploy/dev/netns-faults.sh`; every impairment is added to a veth end
inside a test namespace and removed with the namespace. The script
refuses to run if the namespaces already exist and always ends with a
teardown that removes them, so that no setting survives the run.

Topology (both kinds): namespace/host 1 runs FoundationDB (a private
`fdbserver` owned by the test user), the controller, durable nodes `d0`
and `d1`, cache node `c0`; namespace/host 2 runs durable node `d2`, cache
nodes `c2` (and `c3` for erasure-coded cache), the gateway and the load
generator. Profile RS 2+1 with failure domain `node` (one label per
process; physical separation is *not* claimed as a failure domain in
these runs). Every cross-namespace operation therefore crosses the
impaired link: the gateway's metadata transactions, one of three durable
shards of every chunk, and one or two cache shards.

## Faults and expected behaviour

Definitions: *explicit failure* means an error response or a closed
connection within the configured timeouts, never a success with wrong or
partial data; *verify pass* means `s3load --op get --verify` over the
whole key space after the fault is removed, where every object must be
either absent (its PUT failed) or byte-identical to the deterministic
body.

| # | Fault (on the veth link) | PUT | GET | Repair | GC |
| --- | --- | --- | --- | --- | --- |
| F1 | Packet loss 2 % and 10 % both directions (`netem loss`) | succeeds, slower; zero errors at 2 %; at 10 % errors are allowed only as explicit timeouts; verify pass clean | succeeds, slower; cache hits still validated by CRC (no mismatch) | reconstructs and completes, slower | collects; reconciliation idempotent |
| F2 | Latency 50 ms ± 10 ms jitter (`netem delay`) | succeeds, zero errors; throughput bounded by RTT × concurrency | succeeds, zero errors | completes | completes |
| F3 | Bandwidth 100 Mbit/s (`tbf`) | succeeds, zero errors; throughput ≤ limit | succeeds, zero errors; cache hits served from the local namespace are *not* bounded by the link when the whole copy is local (`none` protection with the copy in namespace 2) | completes | completes |
| F4 | Connection reset: all established sockets from namespace 2 to the durable node `d0` destroyed once during a run (`ss -K`) | requests whose shard write was in flight fail explicitly or succeed after the gateway's retry on another group (none here, so: fail); no partial object becomes visible (verify pass) | requests whose shard read was in flight fail explicitly; the next request reconnects and succeeds | not exercised | not exercised |
| F5 | Partition: veth down for 10 s during PUTs and GETs | every request started during the partition fails explicitly within the node/metadata timeouts; after link up new requests succeed without restart; verify pass clean; pending generations of failed PUTs are collected by GC after the lease | GETs fail explicitly (the gateway cannot read metadata); afterwards succeed | a repair interrupted by the partition is resumed by the controller from its cursor after link up | a GC pass interrupted by the partition is re-run and converges |
| F6 | Node restart: durable `d2` killed with `SIGKILL` and restarted during GETs; cache node `c2` the same | PUT fails explicitly while `d2` is down (`m_ack = m`, one group); succeeds after restart | GET succeeds throughout: durable reads decode from `d0`,`d1`; cached reads degrade or fall back | after restart `d2` serves its shards; no repair is needed; a repair started while it was down is completed or superseded | unaffected |
| F7 | Controller restart (`SIGKILL`, restart) | unaffected | unaffected | the new controller re-acquires leadership after the lease and resumes any repair from the cursor | GC resumes |
| F8 | Partition while an RPC is in flight: link down for 3 s in the middle of a 16 MiB PUT and a 16 MiB GET (concurrency 4) | the in-flight PUTs fail explicitly; a later GET of those keys is 404 or the full object (never a short body: the commit happens after every shard is acknowledged) | the in-flight GETs end with a closed stream (truncated body is observable by the client as an error, never a wrong byte); later GETs succeed | not exercised | not exercised |

Invariants checked in every run, in addition to the table:

- Strong consistency and atomic visibility: after the run, each key is
  either absent or complete and correct (verify pass); no key is ever
  readable with the wrong generation's bytes.
- Durable acknowledgment: no PUT is reported successful whose object is
  later absent.
- Placement epoch fencing: nodes that were restarted accept only the
  current epoch (checked by `admin status` epochs and the absence of
  `epoch` errors in the gateway log after recovery).
- Idempotent recovery: a second reconciliation and GC pass after the run
  deletes nothing more.
- Object generation isolation: the cache serves no stale generation
  (overwrite during impairment: later GETs return the new bytes).

## What is not covered

- Impairment on a physical NIC between two hosts (needs a change to an
  interface or firewall of a host, see above). The cross-host run covers
  process faults (F6, F7) and performance only.
- Asymmetric or partial partitions (only one direction, or only some
  ports); multi-switch topologies; packet corruption.
- Faults lasting longer than the pending-generation lease or the GC
  grace period (the run keeps them short to stay within one session).
