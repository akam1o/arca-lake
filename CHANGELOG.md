# Changelog

arca-lake has no releases yet. This page summarizes the unreleased system
and recent changes that affect users and operators. Detailed implementation
history is preserved in the [development archive](docs/archive/README.md).
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the project will follow [Semantic Versioning](https://semver.org/) once it
has releases.

## [Unreleased]

### Added

- Distributed object storage with stateless S3 gateways, FoundationDB
  metadata, append-only storage nodes, per-profile erasure coding or
  replication, and failure-domain-aware placement.
- Core bucket and object operations, multipart uploads, versioning,
  conditional requests, range reads, copy operations, and checksums.
  Supported operations and differences from Amazon S3 are tracked in the
  [compatibility guide](docs/guide/s3-compatibility.md).
- Durable storage tiers, automatic promotion and demotion, and independent
  read-through cache tiers with replication or erasure coding, prefetch
  and pinning.
- Controller election, integrity scrubbing, repair, garbage collection,
  capacity admission and backpressure, and fenced recovery with deferred
  reclamation. See [operational requirements](docs/guide/operations.md)
  and [backup and restore](docs/guide/backup-restore.md) for their conditions.
- SigV4 authentication, a credential store and bucket-policy subset,
  service mTLS and role authorization, and FoundationDB TLS guidance.
  See the [security model](docs/guide/security-model.md) for the boundary.
- Operator commands, metrics, alerts, fault-injection and compatibility
  tests, and reproducible benchmark tooling.

### Changed

- Access records use a bounded per-gateway table and one batched flusher.
  The transaction checks generation liveness, unknown commit outcomes are
  not retried, and observable losses are counted. The default mode is
  `--access-records on` (the batched path) with a batch of 128 generations;
  `legacy` retains the earlier path. Recording only where a tiering policy applies
  (ADR-0034 decision 6) remains unimplemented. In the reported VM A/B,
  the FoundationDB commits-per-GET target was met, while GET throughput
  remained 7–14 % below `off` and missed its target. See the
  [ADR-0034 verification report](docs/verification/phase-4.1-adr34.md).
- Metadata schema 4 (ADR-0035): no record layout changes; it gates
  disabling tiering. This build reads schemas 3 and 4, so a store of
  schema 2 is upgraded through a release of schema 3 first, and a
  metadata backup taken at schema 2 is restored with a release of schema
  3 (and finalized there) before this one. After `admin upgrade
  finalize` writes schema 4, earlier releases refuse to start and the
  cluster cannot be downgraded below it.
- `admin tiering status | enable | disable | set`: the tiering record,
  the cluster's source of truth for whether tiering acts and its policy
  (ADR-0035). Controllers run tiering from it (their `--tiering-*` flags
  only seed an absent record) once the leader has acknowledged it; moves
  a tiering pass started switch only under the policy that decided them;
  demotion waits for a grace after tiering is enabled
  (`--tiering-clock-skew-secs`). Disabling is refused below schema 4.
  Gateways read the tiering and leader records in their two-second
  refresh and stop recording reads only while tiering is confirmed
  disabled by the current leader (unknown, unacknowledged or stale:
  recorded); `--tiering-lease-skew-ms`, `arca_gateway_access_recording`,
  `arca_gateway_access_unrecorded_total`. The flush applies the record's
  promotion cutoff (`arca_gateway_access_dropped_total{reason="cutoff"}`),
  so read counts from before tiering was last enabled never promote
  (`--access-records legacy` does not apply it). On an upgraded
  cluster with `--tiering-*` flags the first pass seeds the record and
  the next acknowledges it: tiering pauses one interval, then nothing is
  demoted for `demote_after` plus the grace margin and earlier read
  counts do not promote until new reads arrive.
- The gateways' `--access-max-age-ms` is at most 30 s (the flush delay
  the tiering grace allows for); 0 or larger values are lowered, with a
  warning.
- Reconciliation and reclaim judge a listing page in one read-only
  metadata transaction and use bounded parallelism. Rebalancing's settle
  time accounts for the measured reconcile cycle. The capacity-pressure
  criterion remains incomplete; see the
  [ADR-0033 verification report](docs/verification/phase-4.1-adr33.md).
- Benchmark retries use bounded exponential backoff and jitter; rejected
  operations pause before the next attempt. Comparisons must account for
  this change in offered load.
- The dependency advisory exception for indirect `chrono` usage is scoped
  to `RUSTSEC-2026-0543`; its rationale and removal conditions are tracked
  as R-115 in the [risk register](docs/architecture/risks.md).

### Fixed

- PUT, UploadPart, CompleteMultipartUpload and DELETE no longer report a
  later attempt's error as a definite refusal when an earlier commit may
  still apply. The outcome is fenced or reported as unknown; resolved
  versioned PUTs return their own version id, and expired unsealed parts
  are not reported as stored. See the
  [commit-outcome verification](docs/verification/phase-4.1-r112.md).
- Restore tests now restart gateways after restoration and verify that
  old gateways are fenced. The intermittent failure was a test race;
  product fencing behavior was unchanged. See the
  [restore-test investigation](docs/verification/phase-4.1-r113.md).

### Limitations

This remains experimental. Long-duration operation under the current
validation criteria and physical-device power-loss behavior are not
verified. Implementation, measured results and unverified conditions are
separate; see [known limitations](docs/guide/known-limitations.md), the
[risk register](docs/architecture/risks.md) and the
[current roadmap](docs/roadmap/README.md).
