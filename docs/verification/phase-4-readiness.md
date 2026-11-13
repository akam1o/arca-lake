# Phase 4 readiness assessment

Status at the end of Phase 3.14. An assessment, not a plan of record:
it states what exists, what was measured and where, and what the next
phase should take on first. **Implemented** and **meets a performance or
scale target** are separate columns throughout: no performance or scale
target has been set for the project yet, so the second column records
what was measured and on what, never a pass.

Labels: **Implemented and Verified** (an automated test or a recorded
run exercises it), **Implemented with Limitations** (the limitation is
stated), **Partially Implemented**, **Not Implemented**, **Not
Verified** (implemented, nothing exercises it at the stated condition).

Environment of every measurement below: two 4-vCPU, 7.7 GB virtual
machines with virtual disks, one private single-process FoundationDB;
in-process runs on one of them. Nothing here was measured on physical
disks, on more than two hosts, or for longer than about ten hours.

## 1. S3 compatibility

| Area | Implementation | Verification | Notes |
| --- | --- | --- | --- |
| Buckets (create, delete, head, list, location, versioning, policy subset) | Implemented and Verified | AWS SDK for Rust, AWS CLI v2, boto3 conformance in CI (RFC-007 §11) | bucket names are per cluster; a non-owner gets `403` where AWS says `404` |
| Objects (PUT incl. `aws-chunked`, GET, HEAD, DELETE, DeleteObjects, CopyObject, ListObjectsV2, ListObjectVersions) | Implemented and Verified | same | ListObjects v1 Not Implemented |
| Multipart (create, UploadPart, UploadPartCopy, complete, abort, ListParts, ListMultipartUploads) | Implemented and Verified | same; under backpressure on FoundationDB (CI) | expiry by controller sweep, not lifecycle |
| Versioning | Implemented with Limitations | conformance suite | version ids are 32 hex characters; `null` version id not sent on suspended buckets |
| Range GET, `partNumber` | Implemented and Verified | conformance suite | no checksum headers on ranges |
| Conditional requests (`If-Match`, `If-None-Match`, `If-(Un)Modified-Since`; conditional writes) | Implemented and Verified | conformance suite | `409 ConditionalRequestConflict` never emitted |
| Checksums (CRC32, CRC32C, CRC64NVME, SHA1, SHA256; composite and full-object) | Implemented and Verified | conformance suite | — |
| Error semantics | Implemented with Limitations | conformance suite | capacity refusal is `503 SlowDown` with `Retry-After`; documented differences RFC-007 §5.1.1 |
| Authentication (SigV4 header, presigned) | Implemented and Verified | conformance suite, security tests (RFC-012 §11) | — |
| Authorization (bucket policy subset, credential store) | Implemented with Limitations | RFC-012 tests | object ACLs, bucket ACLs Not Implemented |
| Lifecycle, tagging, GetObjectAttributes, object ACLs | Not Implemented | — | `NotImplemented` |
| CORS, website, logging, notifications, replication, Object Lock, encryption configuration, Select | Not Implemented (not planned) | — | — |

## 2. Performance

| Area | Implementation | Measured (where) | Target met? |
| --- | --- | --- | --- |
| PUT / GET throughput | Implemented | two hosts, release build: PUT about 340 MiB/s, warm GET about 690 MiB/s (`lab-2host` baseline, Phase 3.8 check: 354 / 683; PUT varies up to 2.7× with the virtual disk's `fdatasync`, R-48) | no target set |
| Multipart throughput | Implemented | 8 MiB parts: 120–292 MiB/s (median 201, 7 runs), GET 685 MiB/s | no target set |
| Parallel requests | Implemented | 1–3 gateways: aggregate GET 560–744 MiB/s; host CPU the bound (RFC-009 §6.18) | no target set |
| p95 / p99 | Implemented | per run in RFC-009 §6; rebalancing runs of this phase (verification document §6): PUT p99 of 8 MiB objects about 150 ms in load windows | no target set |
| EC encode / decode | Implemented | `rs-gf8-v1` 4+2, 1 MiB shards: encode 7.1 GiB/s, one-shard reconstruction 9.4 GiB/s per core (criterion) | not a bound at measured rates |
| Gateway CPU | Implemented | CPU-s/GiB per configuration (RFC-009 §6.19); the gateway host's CPU bounds PUT | Not Verified on dedicated hosts |
| Storage node CPU | Implemented | Phase 3.13 reclaim runs: 17–26 CPU-s per 20 min run per node | Not Verified at scale |
| Network overhead | Implemented | network bytes per GiB per mode (RFC-009 §6.3); transport matrix §6.11 | — |
| FDB metadata overhead | Implemented | metabench: 278–8,264 placement transactions/s (1–32 tasks, one FoundationDB process) | multi-process FoundationDB Not Verified |
| Repair | Implemented | this phase: 4,000-chunk in-process repair 51.5 s → 18.4 s (ADR-0031); stripe repair 11–36 MiB/s (Phase 3.3) | Not Verified on real processes at scale |
| Rebalancing | Implemented with Limitations | this phase's A/B (verification document §6) | see there |

## 3. Scalability

| Dimension | Implementation | Largest exercised | Not Verified |
| --- | --- | --- | --- |
| Object count | Implemented (FoundationDB keyspace, paged listings) | the working sets of the soak and load runs (Phase 3.4–3.14 reports; no run aimed at object count) | ≥ 10⁶ objects; listing and GC at that size |
| Bucket count | Implemented | a few per test | thousands |
| Node count | Implemented | 9 nodes in process, 8 real processes on one or two hosts | tens of nodes, several hosts per failure domain |
| Group count | Implemented | 16 groups in process; the sweep simulator 120 groups × 12 nodes | hundreds of real groups |
| Metadata size | Implemented | single FoundationDB process, a few GB | multi-process FoundationDB, metadata ≥ 100 GB |
| Reclaim backlog | Implemented | 1e7 retired records per node (ADR-0030, synthetic) | 1e8 |
| Repair backlog | Implemented | one or two groups repairing at a time on real processes; queue paging tested at its page limits | many groups repairing at once |
| Controller throughput | Implemented (one leader; passes bounded per pass: repair budget, reconcile and tiering cursors this phase) | the above | a single leader's pass time at hundreds of groups and many nodes |

## 4. Operational readiness

| Area | Implementation | Verification | Notes |
| --- | --- | --- | --- |
| Deployment | Implemented with Limitations | quick start, configuration guide; systemd-free manual processes on the validation hosts | no packages, no container images |
| Upgrade / rollback | Implemented and Verified within the compatibility window | CI previous-release job (3.9–3.13 binaries), mixed-version rolling upgrade | rollback to 3.12 conditional on a clean stop (no `.prev`) |
| Backup / restore | Implemented and Verified | `admin restore` drills on FoundationDB; deferred reclamation (R-94(b) bounded, Medium) | restores outside the protection's conditions lose what was reclaimed since |
| Monitoring | Implemented | metric registry checked against every process's output; alert rules; this phase's diagnostics | alert thresholds not tuned on production load |
| Capacity planning | Implemented with Limitations | admission, reserves, reservations (ADR-0032), retained bytes | retention's capacity cost is the operator's (R-106) |
| Failure recovery | Implemented with Limitations | repair, retargeting, differential verification, stripe repair; real-process fault runs (Phase 3.13) | one parity, second failure: waits for a node or an operator (EC limit) |
| Security | Implemented with Limitations | mTLS, FoundationDB TLS, SigV4, policy subset (RFC-012) | no encryption at rest, no KMS |
| Documentation | Implemented | guides, RFCs, ADRs, risks ledger | runbooks not exercised by a third party |
| Long-running operation | Not Verified | longest run of this phase in the verification document §8 | 24 h soak not completed (R-79 history) |
| Physical disks / power loss | Not Verified | SIGKILL and injected I/O faults only | — |

## 5. Recommended Phase 4 priorities

From the measured gaps and the feature differences above, in order:

1. **Long-running and physical validation** (not features): a 24-hour
   soak on dedicated storage with the full background workload, and a
   power-loss test of the node's segment and retirement logs on a
   physical disk. Every guarantee above rests on runs of hours on
   virtual disks.
2. **Scale of the control plane**: a controller pass at hundreds of
   groups and tens of nodes (repair, reconcile with a budget and a
   settle time derived from the sweep, GC backlog of unreachable
   members, health-pass error isolation: the Medium remainder of R-104),
   and multi-process FoundationDB.
3. **Object-count scale**: listing, GC and scrub at 10⁶–10⁸ objects;
   metadata size and FoundationDB transaction limits.
4. **S3 gaps most clients notice**: lifecycle (expiration), tagging,
   GetObjectAttributes, ListObjects v1; then object ACLs.
5. **Performance on dedicated hosts**: gateway CPU per GiB, PUT
   variance root cause on non-virtual storage (R-48), rebalancing and
   repair throughput on real processes at scale.
6. **Packaging and deployment**: packages or images, a supervisor
   configuration, upgrade automation.

Phase 4 is not started by this assessment. A move to Phase 4 does not
mean the system is ready for production use or a formal release.
