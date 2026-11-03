# RFC-013: Operations and Observability

- Status: draft (Phase 3.0; runbooks exercised in Phase 3.3–3.4)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-001, RFC-006, RFC-008, RFC-011, RFC-012, RFC-014, RFC-015
- Related ADRs: ADR-0008, ADR-0011, ADR-0013

This is a public document. It is written for a team of two or three
people who run one or a few clusters; every procedure is meant to be
executable from the commands shown, with a check at each step.

## 1. Problem

The system has processes (gateway, storage node, controller),
FoundationDB, and metadata invariants that must hold across restarts,
upgrades, node changes and restores, but no written procedures, no
documented metric registry, and no alerting guidance. Operators would
have to derive all of this from the RFCs and the code.

## 2. Goals

Procedures for: deployment and initialization, node addition and
removal, device replacement, capacity expansion, controller failover,
FoundationDB recovery, backup and restore (RFC-015), rolling upgrade
(RFC-014), monitoring, alerting, troubleshooting. Each with
preconditions, steps, verification, and rollback where one exists.

## 3. Non-goals

Orchestrator-specific manifests (Kubernetes operators, Nomad jobs) are
examples, not the interface; the interface is the CLI, the metrics and
the admin commands. Managed FoundationDB operation beyond what this
system needs is FoundationDB's documentation.

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| O1 | Every procedure is executable with `arca-lake`, `fdbcli`/`fdbbackup`, and standard host tools | Assumption; verified as runbooks are exercised |
| O2 | Every dangerous step has a dry run or a confirmation and names the metric that proves it worked | Open |
| O3 | A metric registry lists every exported series with type, labels and meaning | Open (§7) |
| O4 | Alert rules exist for the failure modes in RFC-008 §7 and RFC-011 §6 | Open (§8) |
| O5 | The guide stays correct: each runbook is exercised by a test or a soak scenario named next to it | Open |

## 5. Deployment

### 5.1 Topology

| Component | Count | Notes |
| --- | --- | --- |
| FoundationDB | 3 or 5 processes on separate hosts (`double`/`triple` redundancy), TLS on | the metadata truth; its backup is the cluster's backup (RFC-015) |
| Controller | 2 or 3 processes; one leader by lease (RFC-011) | stateless besides the lease; runs GC, repair, eviction, tiering, lifecycle, scrub |
| Storage node | ≥ `k+m` per profile, in ≥ `m+1` failure domains | one process per device directory |
| Gateway | ≥ 2 behind a load balancer | stateless; holds credentials and certificates (RFC-012) |

Secrets on hosts (mode 0600): FoundationDB cluster file, service
certificate and key, gateway credentials file. None of them is ever
written to the metadata store.

### 5.2 Initialization

1. Create the FoundationDB cluster (`configure new double ssd`), enable
   TLS, distribute the cluster file.
2. Start storage nodes with `--listen`, `--dir`, `--metrics-listen
   127.0.0.1:…`, certificates.
3. Register nodes: `arca-lake admin register-node --address … --label
   node=… --label device_class=… --label domain=…`; verify with `admin
   status` (every node `last_seen` recent).
4. Create groups: `admin create-groups --tier default --device-class …
   --failure-domain domain --count N --k 2 --m 1 …`; verify groups
   `Active` with members in distinct domains.
5. Optional cache tier: `admin create-cache-groups …` (RFC-006).
6. Start controllers (`--cache-tier`, `--auto-repair`, `--http`);
   verify one leader (`arca_controller_leader == 1` on exactly one).
7. Start gateways with `--credentials-file` (RFC-012 §5.2; principals and
   keys, reloaded every 30 s when the file changes), `--authorize
   owner-policy` (the default with a credentials file), optionally
   `--bucket-creation` to restrict who creates buckets; audit records go
   to the tracing target `arca.audit`. Smoke test with the AWS CLI
   (`aws s3 mb`, `cp`, `ls`, `rm`).
8. `admin init` (idempotent; stamps the schema, `schema_write`,
   `format_write` and an `Active` cluster state with its incarnation —
   controllers also do it on first start), then record the versions
   (`admin status --versions`: every process and node with its software
   version and P/M/F windows; RFC-014) and take the first backup
   (RFC-015).

### 5.3 Node addition (capacity expansion)

1. Start the node process; `admin register-node`.
2. Create new groups over the new node and existing ones (`admin
   create-groups --count …`); the gateway picks groups by free space
   (`WritableGroup.min_free_bytes`, RFC-004 §5.5), so new groups receive
   new writes. Existing groups are not rebalanced automatically; to move
   data, replace a member (`admin replace-member --group G --index I
   --node M --yes`, Phase 3.3) which runs the repair path.
3. Verify: `arca_controller_nodes_registered` increments; the new node's
   `live_bytes` grows; no `task_errors`.

### 5.4 Node removal (decommission)

Phase 3.3 (implemented as below; the planned `--dry-run` and
`remove-node` are not implemented).

1. `admin decommission-node --node N --yes`: records `Draining` for N
   (`("al","nstate",N)`). The controller leader replaces N in every group
   it belongs to with the member-replacement state machine (epoch fence,
   persisted repair cursor, completeness check), one member per group per
   pass, durable groups before cache groups, keeping the group's device
   class and failure domain, and never choosing a draining or
   decommissioned node. It proceeds only while the cluster is `Active`.
2. Progress: `admin status` prints `node N lifecycle Draining … blocked
   …`; a reason appears when no healthy replacement fits ("no healthy
   replacement with free failure domain") or the cluster is not `Active`;
   nothing is changed while blocked.
3. Done: the leader records `Decommissioned` only when no durable or
   cache group lists N and no group is still `Repairing`; then stop the
   process. A controller crash or failover at any point resumes from the
   `Draining` record and the group's repair cursor.
4. `admin decommission-node --node N --cancel` stops a drain before it
   completes; members already moved stay moved.

### 5.5 Device replacement (failed disk)

A failed device is a node that is unhealthy: with `--auto-repair` the
controller replaces it in every group after `--unhealthy-after-secs`
(RFC-011 §5.5) — no operator action is needed for durability. To move a
member deliberately (a device that is failing but still readable):
`admin replace-member --group G --index I [--node M] --yes`; the leader
carries the request out (only while `Active`), `admin status` shows it
with its last error, and it disappears once the group is `Active` with a
different node at the index. To reuse
the host: stop the process, replace the device, start the node with an
empty directory **under a new node id** (`register-node` again); the old
id is removed once its groups are re-membered (§5.4 step 3). Re-using
the old id with an empty directory is refused: the node's
`group-epochs.txt` is gone and its shards are gone, and a re-registered
empty node would be treated as a member holding nothing (RFC-008 §6,
former-member deletion).

### 5.6 Controller failover

Automatic: the lease (`--lease-secs 10`) expires and a follower acquires
it (RFC-011). Planned handoff: stop the leader (SIGTERM); it releases the
lease and a follower takes over within one election interval
(`lease/3`). Verify `arca_controller_leader_acquired_total` on the new
leader and that in-progress repairs resume (`repair_cursor`). Nothing
needs reconciliation: every leader action is fenced.

### 5.7 FoundationDB recovery

- Process loss within redundancy: FoundationDB heals; arca-lake
  transactions see latency, not errors (`arca_gateway_meta_txn_*`).
- Quorum loss: `fdbcli` recovery per FoundationDB docs; arca-lake
  gateways return `503` (metadata unavailable) and pending generations
  expire by lease when the store returns (RFC-008 §9 "unavailability
  longer than a lease" is the gap: writes in flight at that moment fail
  and are not published; their shards are reconciled as orphans).
- Data loss (restore from backup): RFC-015.

### 5.8 Rolling upgrade

RFC-014 (as amended by ADR-0016). Order: storage nodes, then
controllers, then gateways; one host at a time; verify after each.

1. `admin status --versions` before and after each restart: every node,
   controller and gateway with its software version and P/M/F windows.
   While mixed, new processes negotiate the older protocol and write the
   older record and segment versions (`schema_write`, `format_write`).
2. Rollback before finalize: reinstall the previous binary; every record
   and segment written meanwhile is readable by it.
3. `admin upgrade finalize [--ignore-absent ID …] --wait`: the leader
   checks that every node (live `Health`) and every fresh heartbeat
   reaches the target, then raises `schema_write` (the downgrade
   cut-off), `format_write` and the schema, one compare-and-set step per
   pass; `admin upgrade status` shows the step, the blocking reason and
   the history; a finished upgrade is kept as an audit record.
   `admin upgrade cancel` works only before the cut-off.
4. After finalize the previous binaries refuse to start (schema gate;
   nodes refuse the newer segment format). `admin upgrade check-downgrade
   --schema M` and `storage-node check-downgrade --dir D --format F` say
   in advance whether a given build can run.

### 5.9 Backup and restore

RFC-015: FoundationDB backup of the metadata; node segment directories
are not backed up (they are the data; durability is coding). A metadata
backup is not a data backup: objects whose shards were removed after the
backup point are reported unreadable, never recovered, and writes after
the backup point are lost (RPO = backup point).

Restore runbook (exercised by `fdb_restore.rs`, real
`fdbbackup`/`fdbrestore`):

1. Stop every gateway and controller. Restore the metadata into an
   empty, dedicated cluster: `fdbrestore start --dest-cluster-file F -r
   URL -v VERSION -w` (the version of the restore point; without `-v`
   the restore runs to the end of the mutation log).
2. `admin recovery begin --from LABEL --backup-point-ms MS --yes`: a new
   incarnation and `Restoring`. Every process of the previous
   incarnation is fenced (metadata guard and node `DeleteShard`) even if
   it is still running.
3. Start controllers: the leader raises every group epoch above what any
   member enforces (catch-up), then `VerificationPending`: a resumable
   pass classifies matched, orphan (pre/post backup point), former-member,
   missing, corrupt, degraded, unreadable, placement-epoch, membership,
   manifest and cache findings into the report. Nothing is deleted;
   client writes are refused; reads are served (or fail with the
   integrity error).
4. `admin recovery status` shows the cursor and the report with every
   affected object. The pass ends in `ReadOnly` (clean and complete) or
   `RecoveryRequired` (unreadable data, dangling parts, unanswered nodes);
   repair runs in both.
5. `admin recovery finalize --yes` (`--force` to accept a
   `RecoveryRequired` report; the acceptance is recorded) makes the
   cluster `Active`; reconciliation then reclaims post-restore orphans.
   Start gateways (they adopt the new incarnation at start).
   `admin recovery verify --yes` re-runs the pass from the start;
   `admin recovery resume` continues it.

## 6. Capacity and quotas

- Durable capacity: sum of group members' free bytes; gateways refuse
  writes with `503` when no group has room (`choose_group` fails).
- Alerts at 80 % used per node (`arca_node_segment_bytes` /
  device size, exported by the node in Phase 3.2).
- Cache: `arca_controller_cache_usage_bytes` against capacity;
  watermarks (RFC-006 §5.4).
- Per-bucket usage counters (Phase 3.1, with multipart accounting) feed
  per-tenant quotas enforced at the gateway (RFC-012 §5.3).

## 7. Monitoring: metric registry

All processes export Prometheus text on `--metrics-listen` /
`--http`. The registry is
[`crates/common/src/metrics-registry.tsv`](../../crates/common/src/metrics-registry.tsv)
(normative since Phase 3.4, R-62 closed): one row per metric family with
its type, process, unit, labels and help. It is enforced in CI
(`crates/cli` test `metrics_registry`, `arca-lake-common` tests):

- every family a process exports is in the registry with the same type
  and process, and every registered family is exported (with TLS on;
  an explicitly plaintext process exports `_tls_enabled 0` and no other
  TLS family);
- a family is declared once and its series are contiguous; a histogram
  family has only `_bucket`, `_sum` and `_count` series (the per-phase
  maxima are separate `…_max_seconds` gauges since Phase 3.4);
- labels are declared, and their values come from a closed set or, for
  `op`, an S3 operation name; a value that looks like an address, a path
  or a key fails the test (no bucket, key, principal, host or address
  ever becomes a label);
- counters end in `_total` and only counters do; time is in seconds;
- every family has a `# HELP` line: processes take it from the registry
  at export time.

Main families by process (see the registry for every row):

| Process | Families |
| --- | --- |
| controller | leadership and fencing token; node counts (`_nodes_registered`, renamed from `_nodes_total` in Phase 3.4, and `_nodes_unhealthy`); GC; member repair (`_groups_repairing`); cache tier and tiering; scrubbing; integrity findings and stripe repair queue (`_data_integrity_status`); manifest scrub; cluster phase and upgrade step; scheduler/guard counters; TLS |
| gateway | read-path integrity; read-through cache (25 counters); node RPC retries and timeouts; PUT phases; population queue; metadata transactions; S3 requests by operation and status class, latency by operation, bytes (Phase 3.4); access-key refusals by reason and credential reload failures (Phase 3.4); TLS |
| node | node phases; quarantine; corrupt reads and node scrub work; compaction and segment/live bytes (Phase 3.4); device free/total bytes for placement (Phase 3.4); TLS |

Not implemented (listed before Phase 3.4, removed from the registry until
they exist): request rate limiting and per-principal budgets, and a TLS
certificate gauge for the S3 listener (the gateway serves S3 over plain
HTTP behind a TLS terminator).

## 8. Alerting

Rules: [`deploy/prometheus/arca-lake-alerts.yml`](../../deploy/prometheus/arca-lake-alerts.yml)
(Prometheus rule groups; a test fails when a rule names a metric that no
process exports). Severity `page` = data at risk or service down, `ticket`
= the working day.

| Alert | Condition | Severity | Runbook |
| --- | --- | --- | --- |
| `ArcaUnrecoverableData` | `max(arca_controller_data_integrity_status) == 2` | page | §10.6 |
| `ArcaGcInvariantViolation` | `increase(arca_controller_gc_skipped_referenced_total[1h]) > 0` | page | §10.4 |
| `ArcaNoControllerLeader` | `sum(arca_controller_leader) == 0` for 1 min | page | §5.6 |
| `ArcaGatewayErrors` | 5xx share of S3 requests > 5 % for 10 min | page | §10.1, §5.7 |
| `ArcaDeviceFull` / `ArcaDeviceFilling` | device more than 90 % / 80 % full | page / ticket | §5.3, §6 |
| `ArcaRepairStuck` | a group `Repairing` for 1 h with no chunk rebuilt | ticket | §10.5 |
| `ArcaStripeRepairFailing`, `ArcaQuarantinedShards`, `ArcaCompactionFailing` | failed repair tasks 30 min; quarantined shards 6 h; compaction errors | ticket | §10.6a, §10.6b, RFC-005 §5.6 |
| `ArcaNodeUnhealthy` | a node unhealthy for 5 min (page when several in one group) | ticket | §5.5 |
| `ArcaClusterNotActive` | cluster not Active for 30 min | ticket | §5.9 |
| `ArcaNodeRpcExhaustion`, `ArcaMetadataConflicts` | node RPCs out of retries; > 5 % metadata conflicts | ticket | ADR-0011, RFC-009 §6.17 |
| `ArcaCacheOverWatermark` | cache above its high watermark for 30 min | ticket | RFC-006 §5.4 |
| `ArcaCertificateExpiring`, `ArcaTlsReloadRefused`, `ArcaRpcsDenied`, `ArcaPlaintextNodeRpcs`, `ArcaAuthFailureSpike` | certificate within 14 days; refused reload; RPCs refused by role; plaintext node; access-key refusals | ticket | §10.8, ADR-0017, audit log |

## 9. CI and runner security (public repository)

Checklist applied before the repository becomes public (RFC-012 §5.11):

1. Workflows declare `permissions: contents: read`; no secrets beyond
   `GITHUB_TOKEN`; third-party actions pinned to commit SHAs.
2. Self-hosted runners execute only `push` to `main` by maintainers;
   `pull_request` jobs from forks run on GitHub-hosted runners (when the
   account can use them) or require a maintainer to push the branch.
   `pull_request_target` is never used.
3. Runner hosts: dedicated VMs, no credentials for validation or
   production hosts, no inbound access from them, rebuilt on a schedule
   (ephemeral by policy), runner service user without sudo once the
   workflow no longer needs package installs (a prepared image replaces
   the "prepare the runner" step).
4. Artifacts: only sanitized measurement results are committed
   (`sanitize.py --check` in CI); CI uploads no artifacts.
5. Dependencies: `cargo-deny` on every push; Dependabot or an equivalent
   for advisories; `Cargo.lock` reviewed on update.
6. Third-party tools on the runner are installed from distribution
   packages or, where those are unusable, from the vendor's official
   download pinned by version and SHA-256 in the workflow (the AWS CLI v2
   bundle for the S3 conformance tests; the distribution's `awscli`
   package fails on Python 3.14).

## 10. Troubleshooting runbooks

Each runbook: symptom → checks → action → verification.

### 10.1 Gateway returns 503 for every request

Checks: `arca_gateway_meta_txn_attempts_total` not increasing while
requests arrive → metadata unavailable; `fdbcli --exec status`. Action:
§5.7. Verification: successful `aws s3 ls`.

### 10.2 PUT slow, GET fine

Checks: `arca_gateway_put_phase_seconds{phase="shard_write"}` high and
`arca_node_phase_seconds{phase="store_fsync"}` high → storage write
latency (RFC-009 §6.14); check the device and neighbours. If
`phase="append"`/`"commit"` high and fsync normal → FoundationDB commit
latency. Action: storage first (R-48).

### 10.3 Hit ratio drops

Checks: `populations_skipped`/`dropped_full` rising → queue bound
(RFC-006 §5.3.1); `population_failures_node` → cache node health;
`admissions_rejected_capacity` → tier full. Action accordingly.

### 10.4 `gc_skipped_referenced_total` increased

An invariant violation (RFC-008 I-series): a retiring generation is still
referenced. Action: stop GC (`admin gc pause`, Phase 3.2), dump the
generation (`admin inspect-generation`), file an issue with the dump;
do not resume until understood.

### 10.5 Repair stuck

Checks: controller log for the group; `VerifyShards`/`ListShards`
errors on a member; `repair_cursor` static. Action: check member health;
if a member is dead, `admin replace-member` with another target; the
cursor makes a restart resume.

### 10.6 Unrecoverable stripe reported (Phase 3.2)

`arca_controller_data_integrity_status` is 2, or `admin integrity
status` lists a task in `Unrecoverable { have, need }`. Fewer than `k`
shards of a live chunk verified when the worker classified it; GET of
the object returns `500 InternalError` ("chunk unreadable ... data
integrity error") and never bytes (RFC-008 I15). Action: a member that
is merely unreachable is reported as a retry (`last_error: members
unavailable`), not as unrecoverable; bring it back and the task
completes. For a real unrecoverable task, `storage-node
quarantine-list --dir <dir> --scan` on each member shows which records
failed and why; the bytes are still on the device until compaction and
can be examined. The task is re-checked every hour and clears itself
when `k` good shards are found again (a returned node, a restored
device). Otherwise restore the object from a client-side copy (a new
PUT supersedes the generation; GC retires the damaged one). Never
delete the task by hand: it is the record that the data is missing.

Phase 3.3: `admin integrity status --objects` resolves every
unrecoverable or failed task to bucket, key, version id (and whether it
is the current version), part number and chunk; `admin integrity
verify-object --bucket B --key K [--version-id V]` checks every shard of
one version (every part of a multipart object) on its members and exits
0 healthy, 1 degraded or incomplete, 2 unreadable.

### 10.6a Repair task stuck in `Failed` (Phase 3.2)

`arca_controller_repair_tasks_failed > 0`. The worker gave up after
`--stripe-repair-max-attempts`; `admin integrity list` shows
`last_error` (a node rejecting writes, a read-back mismatch, a
placement epoch ahead of the group). Fix the cause; the task is retried
at `next_attempt_ms` and by the next scrub pass of its group.

### 10.6b Quarantined shards on a node (Phase 3.2)

`arca_node_quarantined_shards > 0`. The node found records whose header
or block CRCs fail; they are absent to readers and repair, the
controller's next scrub pass (or a degraded read) queues their
stripes, and a successful repair releases the entries (`cause=superseded`
in `quarantine.log`). A node that keeps quarantining new records has a
failing device: replace it (§5.5). The log and the bytes are evidence;
`quarantine.log` may be deleted after investigation (recovery
re-derives the state by re-scanning).

### 10.6c Gateway returns 503 "cluster not available" (Phase 3.3)

The message names the reason. "cluster is ReadOnly / Restoring /
VerificationPending / RecoveryRequired": the cluster state does not
admit the request (`admin recovery status`). "cluster incarnation
changed since this gateway started": a restore was marked after this
gateway started; restart it. A controller log line "cluster state: …"
with `arca_controller_guard_refusals_total` rising is the same fence on
the controller side; `passes_skipped_by_state_total` counts the passes
that did not start for that reason.

### 10.7 Node refuses writes: epoch rejected

`FAILED_PRECONDITION epoch`: the node has a newer epoch than the caller
(stale gateway map) → transient, the gateway refreshes; persistent after
a restore → RFC-015 §5.4 epoch catch-up.

### 10.8 Certificate problems

`tls_handshake_failures_total` rising after a rotation → a peer still
has the old root; §RFC-012 5.5.

## 11. Alternatives considered

- A separate admin HTTP API instead of CLI-on-cluster-file: deferred
  (RFC-011 §9); the CLI with the cluster file keeps one privilege model
  until RFC-012's identities are in place, at which point the admin role
  certificate (RFC-012 §5.5) can front an API.
- Push metrics (remote write) instead of scrape: scrape keeps the
  processes free of outbound dependencies; a sidecar can push.

## 12. Failure scenarios

Covered by the runbooks (§10) and the alert table (§8); RFC-015 covers
restore scenarios.

## 13. Acceptance criteria

Labels as RFC-007 §11 (Designed / Implemented / Verified / Not
Implemented / Not Verified).

| # | Criterion | Status |
| --- | --- | --- |
| O-A1 | Every runbook in §5 and §10 executed at least once on validation hosts (Phase 3.3–3.4) with a sanitized transcript linked from the roadmap | Designed |
| O-A2 | Metric registry test: every exported series is listed in §7 | Designed |
| O-A3 | Alert rules exist as a Prometheus rule file in `deploy/` and pass `promtool check rules` | Designed |
| O-A4 | Public-repository CI checklist (§9) items 1, 2, 4, 5 | Implemented (workflow permissions, self-hosted on `push` to `main` only, sanitizer check, cargo-deny); item 3 (dedicated rebuilt runner VMs) Designed |
| O-A5 | Node add/remove/replace-member/decommission commands | Not Implemented (Phase 3.2) |
| O-A6 | Cluster state commands (`set-read-only`, `activate`, `restore mark/verify/report/accept`) | Not Implemented (Phase 3.3) |
