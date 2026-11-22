# Known limitations

arca-lake is **experimental**. This page states what that means
feature by feature. Labels: **Verified** = an automated test or a
recorded measurement exercises it; **Verified with limitations** = the
limitation is stated; **Experimental** = implemented and tested, but its
policy or interface may change; **Not verified**; **Not supported**.
The full register is [risks.md](../architecture/risks.md).

## Status by area

| Area | Status |
| --- | --- |
| S3 operations of the [compatibility matrix](s3-compatibility.md) | Verified (AWS SDK for Rust, AWS CLI v2, boto3) |
| Durability of acknowledged writes, no partial visibility, no deletion of referenced shards | Verified by fault-injected tests (commit timeouts, node and gateway crashes, FoundationDB failover) and short load/fault runs (Phase 3.4–3.7 reports) |
| Erasure coding, degraded reads, repair, scrubbing, quarantine | Verified |
| Cache tiers (read-through, eviction, conditional deletes) | Verified |
| Automatic tiering between durable tiers | Experimental |
| Capacity admission at the storage node (reserve, write classes) | Verified |
| Tier backpressure (throttling, refusal, recovery, failover terms) | Experimental: verified by deterministic tests and short runs; thresholds and rates may change |
| Capacity-aware group choice | Experimental |
| Capacity rebalancing between the groups of a tier (`--rebalance`) | Experimental: in-process tests and process-level load/failover comparisons on validation VMs ([Phase 3.14](../verification/phase-3.14-operational-hardening.md)); off by default; large-scale rebalancing with data remains unverified |
| Shared file system detection | Experimental; overlay, FUSE and network file systems cannot be checked: nodes on them need `--capacity-bytes` allotments (else refused like shared ones), and their sum is then the operator's responsibility |
| Rolling upgrades, cluster recovery, backup verification | Verified within one compatibility window (RFC-014, RFC-015) |
| Managed metadata restore (`admin restore`) | Verified with limitations: in-process tests, real FoundationDB restores and mixed-version drills; requires the documented retention, clock, node and recovery conditions ([Backup and restore](backup-restore.md)) |
| Restores done outside `admin restore` | Bounded protection from deferred reclamation under [ADR-0028](../adr/0028-deferred-reclamation.md), not a general safety guarantee. Backup age, clock skew, every durable node's retention and a new incarnation are conditions; older backups and the documented repeated-restore case are outside protection. Follow [Backup and restore](backup-restore.md) |
| Node lifecycle and identity (`admin node ...`) | Verified by in-process tests (Phase 3.11, ADR-0027) |
| mTLS for node RPCs, FoundationDB TLS, SigV4, bucket policy subset | Verified |
| Batched access records for automatic tiering | Experimental: bounded work and fault behavior tested; the commit-count target is met, but the GET throughput target is not ([details below](#access-record-performance)) |

The S3 [compatibility matrix](s3-compatibility.md) is maintained by hand
from the test suite (RFC-007's acceptance table); it is not generated
from test results.

## Behaviour during a metadata outage

While FoundationDB is unavailable, S3 requests fail with `500` after the
metadata timeout (about 10 s), not `503`. In the 0.1.0 three-host
validation, acknowledged writes stayed readable and every write with an
unknown outcome read as one of its candidates (the earlier body, the new
one, or absent) once FoundationDB returned
([report](../verification/0.1.0-three-host-validation.md)).

## Open risks to plan for in 0.1.0

0.1.0 is experimental and not production-ready. Each item links its
entry in the [risk register](../architecture/risks.md).

- R-117 (residual): a node that has adopted an incarnation refuses
  shard writes of another one (0.1.0), but still accepts a write that
  carries no incarnation, and a node that has adopted none yet accepts
  any. Use one CA or trust domain per cluster; plaintext is for tests
  only.
- R-119 (residual): replacements use the controller's
  `--failure-domain` (a group's own label is not recorded); groups whose
  members were put in one domain by a replacement made before the fix are
  not reported; a node without the failure-domain label stalls the
  replacement of its groups (nothing is placed in a wrong domain).
- R-118: tiering resets a generation's whole access record after
  outcomes that may not have acted on it; tiering accuracy is limited.
- R-111: small objects cost far more node capacity than their payload
  (about 24 KiB for a 1 KiB object with three replicas).
- R-112: concurrent overwrites of one hot key are often answered `503
  SlowDown` (55 % of PUTs with 64 writers on one key, measured).
- R-109: access recording costs GET throughput: 7–14 % below
  `--access-records off`, 12–19 % where tiering acts; with tiering not
  wanted, `admin tiering disable` (after finalizing schema 4) removes it.

## Not verified

- A completed 24-hour soak and sustained operation beyond the reported
  runs. The [Phase 3.14 soak](../verification/phase-3.14-operational-hardening.md)
  stopped by decision after 14 h 13 min 43 s (12 h 50 min 10 s with
  faults); earlier soaks stopped on FoundationDB I/O timeouts (R-79).
  Long-duration behaviour of backpressure remains unverified.
- Physical storage devices and power loss: no test on either; real disk
  failures during capacity exhaustion.
- Network impairment between hosts on their real interfaces (only
  between namespaces on one host; R-40).
- Clusters larger than a few nodes per host and a few hosts; WAN.
- Multi-tenant performance isolation.
- Production-scale throughput and object counts. The
  [scalability report](../verification/phase-4.0-scalability.md) distinguishes
  measured S3 runs, metadata-only measurements, in-process tests and
  estimates; none establishes production-scale performance.

## Not supported

- Several storage nodes on one file system without fitting
  `--capacity-bytes` allotments (refused; R-80).
- Encryption at rest, KMS, object ACLs, lifecycle rules, tagging,
  Object Lock and S3 bucket replication (see the matrix). Storage-level
  replication profiles are a separate, implemented mechanism.
- Windows; container images (none published).

## Known behaviours to plan for

- Under capacity pressure writes are throttled and then refused with
  `503 SlowDown`; small devices fill faster than one evaluation
  interval, and the nodes' reserve is then the stop (R-83).
- One full member stops its erasure-coded group; uneven groups can be
  evened out with `--rebalance` (moves chunks, frees the source after
  reconciliation and compaction), uneven members of one group cannot
  (R-82).
- Each node keeps a delete reserve of one tombstone (4 KiB) per record:
  nodes full of small shards stop taking client writes earlier.
- Large writes under a low throttled rate can be refused part way and
  must be retried (ADR-0021).
- A refused body larger than 16 MiB from a client that does not use
  `100-continue` may be seen as a closed connection on slow links
  (R-84; on loopback all tested clients received the 503).

## Access-record performance

The default `--access-records on` records reads in bounded batches, with
`--access-batch 128`. In the final rotated comparison reported in
[Phase 4.1](../verification/phase-4.1-adr34.md), it used
**0.0066–0.0077 FoundationDB commits per GET**, meeting the target of at most
0.1. GET throughput remained **7–14% below `--access-records off`**, outside
that baseline's measured spread; this target is not met. These are results
from the report's specified VM workloads, not general throughput guarantees.

The remaining per-generation metadata work is an explanation consistent
with the counters, not an isolated experimental proof. Disabling recording
also removes those gateways' read observations from automatic tiering.
Overflow, stale records, failures and shutdown can lose observations and
affect tiering decisions; they do not reduce data durability. The configured
bounds and metrics are described in [Configuration](configuration.md) and
[ADR-0034](../adr/0034-access-records-at-scale.md).
