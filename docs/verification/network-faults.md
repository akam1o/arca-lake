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

## Results (namespace pair, 2026-11-01)

Host A of the Phase 2.1 environment (RFC-009 §5), `deploy/dev/netns-faults.sh
setup ec` (cache protection `ec 2+1`), arca-lake at commit `f96c60c` (8 MiB
HTTP/2 windows). Objects 8 MiB (16 MiB in F8), concurrency 4; `s3load`
JSON lines, gateway and controller metrics, and `admin status` snapshots
are in `benchmarks/linux/results/faults/<scenario>/`. Node RPC timeout
10 s; pending-generation lease 20 s; GC grace 3 s; controller lease 4 s,
health probe 2 s, unhealthy after 6 s, auto repair on.

| # | Scenario | Observed | Against the plan |
| --- | --- | --- | --- |
| — | baseline | PUT 12 × 8 MiB in 0.37 s, GET 0.21 s, verify clean | reference |
| F1 | loss 2 % | PUT 1.24 s, GET 1.0 s / 0.6 s, zero errors, verify clean | as planned |
| F1 | loss 10 % | every PUT failed after the 10 s node timeout (`Timeout expired` on a shard write, reported as 503 with the reason); nothing visible afterwards (verify: 12 missing, 0 mismatches) | explicit timeouts only, as allowed; but *no* PUT completed: at 10 % loss the 4 MiB shard streams stall on retransmission timeouts far beyond 10 s. The timeout is per RPC, not per byte |
| F2 | delay 50 ms ± 10 ms jitter | 6 of 12 PUTs failed on shard-write timeouts; GET of the 6 existing objects clean | **not as planned** (zero errors expected). `netem` jitter reorders packets; TCP then collapses on the 4 MiB streams. The same delay without jitter (F2′) passes |
| F2′ | delay 50 ms, no jitter | PUT 7.2 s (13 MiB/s), GET 4.8 s / 4.4 s, zero errors, verify clean | as planned for F2; throughput bounded by RTT as expected |
| F3 | bandwidth 100 Mbit/s | PUT 4 × 8 MiB in 4.2 s (7.7 MiB/s ≈ 64 Mbit/s of 100 with EC overhead 1.5×), GET 2.8 s then 1.7 s on the second pass (local cache shards), zero errors | as planned |
| F4 | connection reset (`ss -K` on the sockets to `d0`, twice during 2 GET passes over 96 objects) | sockets were destroyed (recorded in `sockets.txt`: new ephemeral ports after each kill), **zero client errors**: the interrupted shard reads were treated as missing and the chunks were decoded from the other two shards; the gateway reconnected for the next request | better than planned: the degraded read path (RFC-004 §5.6) absorbs a mid-stream reset on one node; a reset during a PUT was not exercised |
| F5 | partition 10 s during 160 PUTs (concurrency 2) | the PUT stream paused for the partition and continued: 158 succeeded, 2 failed (the two in flight when the 10 s outage met the 10 s RPC timeout); verify 158 correct, 2 missing, 0 mismatches; GETs afterwards clean; the failed PUTs' pending generations were collected after the lease | partly as planned: requests started during the partition were *delayed*, not failed, because the outage did not exceed the RPC timeout; atomic visibility held |
| F6 | durable node `d2` killed and restarted during 40 GET passes; cache node `c2` the same | GET: zero errors in every pass (durable reads decoded from `d0`,`d1`; cache hits degraded or fell back); PUT while `d2` was down: 2 of 2 failed explicitly (`m_ack = m`); after restart PUT succeeded; verify clean, the two failed keys absent | as planned (a first run had the PUT succeed because the kill had not landed before the PUT; the scenario now records the namespace's listeners after the kill) |
| F7 | controller `SIGKILL` and restart during 96 PUTs | zero errors; the new controller acquired leadership (two acquisitions in the controller log); GETs clean | as planned |
| F8 | 3 s partition in the middle of 48 × 16 MiB PUTs and of 3 GET passes | PUTs: zero errors, the pass stretched from ~2 s to 7 s (stalled, then completed); verify clean; GET pass 1 stretched to 4.7 s, zero errors | as planned for visibility and integrity; the outage was shorter than the RPC timeout so no request failed |
| GC | overwrite of 12 objects under 30 ms delay, then GC | old generations and their cache copies collected (controller `gc_collected_total` advanced), new bytes verified | as planned |
| Repair | `d2` killed for good, spare `d3` registered, 2 % loss during the rebuild | auto repair replaced member 3 with `d3` at epoch 2, reconstructed 280 chunks, verified complete (`repairs_completed_total` 1) in about 35 s; verify clean | as planned |

Findings carried into the design documents:

- The per-RPC node timeout is the wrong unit for multi-MiB shard
  transfers over impaired links (F1 10 %, F2): a progress-based timeout,
  or smaller frames with per-frame deadlines, would turn these failures
  into slow successes. Recorded as R-47 and an RFC-009 §9 item.
- Jitter (reordering) matters more than delay for the shard streams.
- Partitions shorter than the RPC timeout are invisible to clients except
  as latency; longer ones fail only the in-flight requests. Nothing
  partial becomes visible in either case (F5, F8).
- A mid-stream reset on one node during a GET is absorbed by the degraded
  read (F4).

## What is not covered

- Impairment on a physical NIC between two hosts (needs a change to an
  interface or firewall of a host, see above). The cross-host run covers
  process faults (F6, F7) and performance only.
- Asymmetric or partial partitions (only one direction, or only some
  ports); multi-switch topologies; packet corruption.
- Faults lasting longer than the pending-generation lease or the GC
  grace period (the run keeps them short to stay within one session).
