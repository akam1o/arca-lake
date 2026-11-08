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
| Capacity-aware group choice | Experimental; no automatic rebalancing of existing data |
| Shared file system detection | Experimental; overlay, FUSE and network file systems cannot be checked: nodes on them need `--capacity-bytes` allotments (else refused like shared ones), and their sum is then the operator's responsibility |
| Rolling upgrades, cluster recovery, backup verification | Verified within one compatibility window (RFC-014, RFC-015) |
| mTLS for node RPCs, FoundationDB TLS, SigV4, bucket policy subset | Verified |

## Not verified

- Operation longer than about 13 hours under load (the 24 h soaks of
  Phase 3.4/3.5 stopped on FoundationDB I/O timeouts of the shared
  virtual storage, R-79); long-duration behaviour of backpressure.
- Physical storage devices; real disk failures during capacity
  exhaustion.
- Clusters larger than a few nodes per host and a few hosts; WAN.
- Multi-tenant performance isolation.

## Not supported

- Several storage nodes on one file system without fitting
  `--capacity-bytes` allotments (refused; R-80).
- Encryption at rest, KMS, object ACLs, lifecycle rules, tagging,
  Object Lock, replication (see the matrix).
- Windows; container images (none published).

## Known behaviours to plan for

- Under capacity pressure writes are throttled and then refused with
  `503 SlowDown`; small devices fill faster than one evaluation
  interval, and the nodes' reserve is then the stop (R-83).
- One full member stops its erasure-coded group; uneven members cost
  usable space until a member is replaced (R-82).
- Large writes under a low throttled rate can be refused part way and
  must be retried (ADR-0021).
- A refused body larger than 16 MiB from a client that does not use
  `100-continue` may be seen as a closed connection on slow links
  (R-84; on loopback all tested clients received the 503).
