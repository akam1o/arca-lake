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

Phase 3.3, extended in Phase 3.4 (dry run, capacity, the draining node
as a source; `remove-node` is not implemented).

0. `admin decommission-node --node N --dry-run [--failure-domain L]
   [--rate-mib R]`: changes nothing (no metadata write, no shard touched;
   only `Health` and `ListShards` reads) and prints, per group N is in
   (durable first): the member index, the target the controller would
   choose under the same rules, the chunks and bytes to write, the bytes to
   read (k per chunk for EC reconstruction), the sources that answer, the
   target's free space, and every condition that would block or pause the
   move (cluster not `Active`, group not `Active`, too few sources, no
   target in a free failure domain, insufficient capacity), plus a time
   estimate. Exit 2 when something would block it.

1. `admin decommission-node --node N --yes`: records `Draining` for N
   (`("al","nstate",N)`). The controller leader replaces N in every group
   it belongs to with the member-replacement state machine (epoch fence,
   persisted repair cursor, completeness check), one member per group per
   pass, durable groups before cache groups, keeping the group's device
   class and failure domain, and never choosing a draining or
   decommissioned node. It proceeds only while the cluster is `Active`.
2. Progress: `admin status` prints `node N lifecycle Draining … blocked
   …`; a reason appears when no healthy replacement fits ("no healthy
   replacement with free failure domain"), when no candidate has room for
   the member's bytes plus 10 % and 256 MiB ("insufficient capacity …;
   paused until a node has room": nodes report their free space in
   `Health`, capped by `--capacity-bytes`), or the cluster is not
   `Active`; nothing is changed while blocked, and the move resumes by
   itself once the condition clears. While N still answers, its own shard
   of a chunk is a source when fewer than k current members have a good
   one (validated against the chunk checksum); `--repair-source direct` on
   the controller copies N's shards instead of reconstructing (1 read per
   shard instead of k).
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
with its last error (including a capacity pause), and it disappears once
the group is `Active` with a different node at the index.
`admin replace-member … --dry-run` shows the plan without changing
anything (as §5.4 step 0). To reuse
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

Upgrading to protocol version 3 (Phase 3.9, ADR-0023): deletes need the
delete fence on both sides. A version-3 node refuses deletes from
version-2 controllers and gateways, and version-3 clients send none to a
version-2 node. Deletes, GC and reconciliation therefore pause on each
node from its upgrade until the controllers (and, for their own partial
writes, the gateways) speak version 3 and the leader has pushed a fence;
the backlog converges afterwards. Nothing is deleted unfenced at any
point. Rolling a node back to version 2 stops deletes on it; rolling
back nodes and controllers both restores the pre-3.9 behaviour (R-88).

Upgrading to Phase 3.10 (protocol 4, metadata schema 3, ADR-0024):

- **During the rolling upgrade** deletes go on. Phase 3.10 nodes take
  the previous release's (protocol 3) deletes while the cluster is below
  schema 3, and Phase 3.10 controllers and gateways send deletes only to
  protocol-4 nodes. A Phase 3.9 node therefore gets no delete until it is
  upgraded.
- **Rollback before finalize** is allowed in any order, and every
  combination is safe.
  - A Phase 3.9 node gets no delete from Phase 3.10 processes.
  - A Phase 3.10 node takes a Phase 3.9 controller's deletes only until
    a Phase 3.10 controller has led it. From then on it takes only
    protocol-4 deletes (kept across its restarts and rollbacks), so
    rolling back only the controllers pauses deletes until Phase 3.10
    controllers lead again or the nodes are rolled back too.
  - The recommended order is the reverse of the upgrade: gateways, then
    controllers, then nodes.
- **Finalize** raises the schema only once every registered node reports
  schema 3 as its floor and holds a segment of format 2. A node that does
  not answer blocks it, including one named with `--ignore-absent`.
  - Bring it back.
  - Only a node that is decommissioned and named with `--ignore-absent`
    is not waited for. That is an explicit decision: its disk must never
    be started again unless wiped, because a previous-release binary
    could still open it.
  - No new node can be registered while the finalize is past its cut-off.
- **After finalize**:
  - Phase 3.9 controllers, gateways and storage nodes refuse to start.
    The finalize raises the segment format to 2 (the version-1 layout),
    and every node rolls over to it at once; Phase 3.9 nodes do not open
    a store with a format-2 segment.
  - Nodes that hold schema 3, or that a Phase 3.10 controller has led,
    refuse protocol-3 deletes, also on metadata restored from before the
    finalize.
- **Never restore metadata from before the finalize without `admin
  recovery begin`**: `recovery begin` brings the schema back up to what
  the nodes hold.

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

**Supported procedure (Phase 3.11, ADR-0026 option D): `admin restore`.**
It runs the steps below as one resumable procedure and refuses to start
while the safety of any node cannot be confirmed:

```
admin restore plan --source URL --version V --backup-point-ms MS --journal J   # shows cluster, source, destination; prints a token
admin restore run --journal J --confirm TOKEN [--stopped PROC_ID]...           # freeze, fence everywhere, clear, fdbrestore, recovery begin, verify
admin recovery status                                                          # the report
admin restore finish --journal J --operator NAME --yes [--force --reason R]    # finalize; waits for a new generation on every node
```

Keep the journal (a local file) until `finish` completes. From the
step before the clear until the clear, node changes (`admin node ...`,
`admin register-node`) are refused; `admin restore abandon --journal J
--yes` gives the restore up before its clear (the journal is never run
again) and lifts that. Run one managed restore of a cluster at a time,
from one journal. Re-run `run`
or `finish` after any failure: each resumes from its first unrecorded
step. The guarantee covers restores done this way only. A restore done
any other way (a direct `fdbrestore`, a DR switch, a snapshot rollback)
is R-94(b), open (High). ADR-0025 (the trip) and the rejoin lease only
mitigate it.

Manual restore runbook, for when the current metadata is gone, which
`admin restore` refuses (exercised by `fdb_restore.rs`, real
`fdbbackup`/`fdbrestore`; the delete fence steps by
`it/delete_fence.rs`, ADR-0023):

0. **Close the delete fence** while the current metadata is still
   reachable: `admin delete-fence close --reason "restore" --yes`. Every
   registered node acknowledges a new closed generation with an operator
   hold, after the deletes already past its fence have finished; from
   then on no delete decided on the current metadata is applied, however
   late it arrives. The command exits 1 while a node has not
   acknowledged: bring it back and re-run, or, only if its process is
   stopped and stays stopped, re-run with `--absent-node ID`. If the
   current metadata is gone (the reason for the restore), skip to step 1
   and stop every controller and gateway first: `recovery begin` closes
   the fence after the restore and warns about every node that was
   still open (a delete in flight may have landed meanwhile;
   verification reports what it removed).
1. Stop every gateway and controller. Restore the metadata into an
   empty, dedicated cluster: `fdbrestore start --dest-cluster-file F -r
   URL -v VERSION -w` (the version of the restore point; without `-v`
   the restore runs to the end of the mutation log). A controller
   started on the restored metadata now cannot reopen deletes: the nodes
   hold the fence closed for the incarnation it carries.
2. `admin recovery begin --from LABEL --backup-point-ms MS --yes`
   (with the node RPC and TLS options): closes the fence again on every
   node (refused unless each acknowledges, or is declared
   `--absent-node`), then a new incarnation and `Restoring`. Every
   process of the previous incarnation is fenced (metadata guard and
   node `DeleteShard`) even if it is still running. A failure here, or
   at any later step, leaves deletes stopped: re-run the step.
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
5. `admin recovery finalize --yes --operator NAME` makes the cluster
   `Active` in one transaction (Phase 3.4): it is refused, even with
   `--force`, while verification has not finished, while a node's
   listing is missing, or outside `ReadOnly`/`RecoveryRequired`. Critical
   findings (unreadable chunks, multipart manifest findings, placement
   epochs ahead of their group) refuse it without `--force`; with
   `--force` a `--reason` is required. Findings stay in the report;
   every listed object becomes a data-loss record (`admin recovery
   losses`), unrecoverable stripes stay repair tasks, and every attempt,
   refused ones included, is audited with operator, reason and result.
   `admin cluster activate` cannot leave a restore around finalize.
   Finalize also releases the delete fence's operator hold; the leader
   opens a new generation at its next health pass (`admin delete-fence
   status` shows every node at it, open). Reconciliation then reclaims
   post-restore orphans and GC works off its backlog. Start gateways
   (they adopt the new incarnation at start). `admin recovery verify
   --yes` re-runs the pass from the start and `admin recovery resume`
   continues it; both are conditional transactions that never undo a
   finalize that committed meanwhile.

Continuous verification (RFC-015 B-A5, Phase 3.4): a backup is trusted
only after a restore. `deploy/backup/verify-backup.sh run` restores the
newest backup at `--backup-url` (optionally creating one, on its own
backup tag) into a dedicated verification cluster and runs
`admin backup verify-restored` on it: schema, cluster state and epochs,
every committed generation against its chunk records and groups,
multipart references, versioning (one latest version per key, every
version resolving). It refuses without an explicit
`--restore-cluster-file`, when that is the source or shares a coordinator
with it, when it is not marked (`init-target`, which refuses a non-empty
cluster) and when it already holds arca-lake metadata (unless
`--clear-target`); it clears the restored range afterwards. Schedule it
daily; it writes text-file metrics (`arca_backup_*`: last success, backup
age, restore and verification durations, bytes and versions verified,
unresolved findings) and the alerts `ArcaBackupVerificationFailed` and
`ArcaBackupVerificationStale` fire on them. It verifies metadata only: a
metadata backup holds no shard data.

### 5.10 Node lifecycle (Phase 3.11, ADR-0027)

A storage node is `Active`, `Decommissioned` (drained, no members),
`Quarantined` (its disk is not the one registered) or `Removed`.

```
admin node list | status --node N
admin node decommission --node N            # alias drain: members move away
admin node unregister --node N --reason R --yes [--unreachable]
admin node rejoin --node N [--fresh-identity]
admin node adopt-identity --node N --yes     # an unconfirmed node only
admin node keep-removed --node N --yes [--unreachable]   # after a manual restore
```

- Registration gives a node an identity (a random token, written once on
  its disk as `node-identity.txt`); the leader records it acknowledged
  once the node holds it. Deletes and removal pushes carry it; a disk
  holding another identity, or a damaged identity file, refuses them.
- `unregister` and `keep-removed` close the node's delete fence with an
  operator hold on its own disk before recording it removed. A node
  that still answers is removed only once it holds an acknowledged
  identity; `--unreachable` is the operator's statement that its disk
  will never be started again unwiped (nothing is pushed to it).
- A node holding another identity is quarantined and its members are
  repaired; `admin node rejoin` checks it before it comes back (an
  emptied disk only with `--fresh-identity`).
- Metadata restored from before a node had an identity leaves the node
  *unconfirmed* (nothing pushed to it, metric
  `arca_controller_identity_unconfirmed_total`) until `adopt-identity`;
  `admin restore` puts the identities back itself.
- Upgrade rule: upgrade every controller before a leader of this release
  runs; earlier controllers' deletes carry no identity and are refused
  once nodes hold one.

## 6. Capacity and quotas

- Durable capacity (Phase 3.6, RFC-008 §6.8, ADR-0020): every storage
  node keeps a reserve (two segments and 64 MiB, or 2 % of the device;
  `--reserve-bytes`) that client, migration and cache writes cannot use;
  repair may go down to the repair floor; deletes always pass. The
  controller leader turns the devices' reports, the groups' room and the
  GC backlog into a state per tier (`normal`, `warning`, `throttled`,
  `write_rejected`, `recovery_only`; `arca_controller_backpressure_state`)
  that gateways enforce with `503 SlowDown` and `Retry-After`. Reads,
  deletes, aborts and multipart completion are never refused for
  capacity. A group whose fullest member has room for fewer than 64
  maximum shards leaves placement: with an erasure-coded group, one full
  member stops the group however much room the others have.
- Size devices so that the reserve is not needed in normal operation:
  the GC backlog holds retired data for about the grace period plus the
  GC interval at the overwrite/delete rate (multipart parts included,
  ADR-0020), and the active segment of each node (up to one segment) is
  not compacted (R-69).
- Alerts at 80 % used per node (`arca_node_segment_bytes` /
  device size, exported by the node in Phase 3.2) and on the admission
  state (§8).
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
| controller | leadership and fencing token; node counts (`_nodes_registered`, renamed from `_nodes_total` in Phase 3.4, and `_nodes_unhealthy`); GC, with stage timing, transactions and bytes, backlog in bytes and reclaim rate (Phase 3.6); write admission state, durable capacity by kind, groups out of placement (Phase 3.6); member repair (`_groups_repairing`); cache tier and tiering; scrubbing; integrity findings and stripe repair queue (`_data_integrity_status`); manifest scrub; cluster phase and upgrade step; scheduler/guard counters; TLS |
| gateway | read-path integrity; write admission state, record age, throttled and rejected requests, node capacity refusals, chunks without a group with capacity (Phase 3.6); read-through cache (25 counters); node RPC retries and timeouts; PUT phases; population queue; metadata transactions; S3 requests by operation and status class, latency by operation, bytes (Phase 3.4); access-key refusals by reason and credential reload failures (Phase 3.4); TLS |
| node | node phases; quarantine; corrupt reads and node scrub work; compaction and segment/live bytes (Phase 3.4); device free/total bytes for placement (Phase 3.4); device capacity by kind and capacity admission by class (Phase 3.6); TLS |

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
| `ArcaCapacityWarning` / `ArcaCapacityCritical` | a tier in `warning` or worse for 15 min / refusing new data for 2 min | ticket / page | §10.11 |
| `ArcaGcUnableToKeepUp` | throttled while the GC backlog (bytes) still grows, 30 min | ticket | §10.10 |
| `ArcaRepairReserveLow` | a device's free space below 1.5 × its repair floor | page | §10.11 |
| `ArcaAdmissionRejectionHigh` | gateways refuse more than one write per second with `SlowDown` for 10 min | ticket | §10.11 |
| `ArcaCapacityReportStale` | the admission record unchanged for 2 min (no leader evaluates capacity) | page | §10.11, §5.6 |
| `ArcaCacheOverWatermark` | cache above its high watermark for 30 min | ticket | RFC-006 §5.4 |
| `ArcaCertificateExpiring`, `ArcaTlsReloadRefused`, `ArcaRpcsDenied`, `ArcaPlaintextNodeRpcs`, `ArcaAuthFailureSpike` | certificate within 14 days; refused reload; RPCs refused by role; plaintext node; access-key refusals | ticket | §10.8, ADR-0017, audit log |

## 9. CI and runner security (public repository)

Checklist applied before the repository becomes public (RFC-012 §5.11):

*Note (2026-11-22): at publication (2026-11-19) the self-hosted runners
were removed and CI moved to GitHub-hosted runners on pull requests,
pushes to `main` and on demand; items 2 and 3 no longer apply
([public CI](../release/public-ci.md#current-arrangement-2026-11-19)).*

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
0 healthy, 1 degraded or incomplete, 2 unreadable. Since Phase 3.4 it is
read-only: a corrupt shard is reported, not quarantined (a node that
predates read-only verification is reported `Unverified`); `--quarantine`
lets the nodes quarantine what they find, as a scrub would. `--decode`
also decodes every chunk from the shards that verified good, checks it
against its record, and recomputes the object's ETag and checksums
(single-part; multipart composite and full-object, and per part) and the
part layout from the decoded bytes, one chunk in memory at a time; any
mismatch exits 2.

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

Node RPCs (ADR-0017):

- A process refuses to start: the message names the file and the
  problem (unreadable, no `spiffe://<trust-domain>/<role>/<name>` SAN in
  the trust domain, wrong role for the process, the certificate does not
  validate against the bundle, expired). Key contents are never printed.
- `*_tls_handshake_failures_total` rising: a peer presents a certificate
  of another CA, an expired or revoked one, or the wrong identity for the
  address (a node must present its own node id). After a rotation: some
  peer still lacks the new root in its bundle.
- `*_tls_rpcs_denied_total` rising: an authenticated identity called an
  RPC its role may not (a gateway certificate used by a controller, a
  node certificate used by a tool); the node logs the identity and RPC.
- `*_tls_reload_failures_total`: new files did not validate; the process
  keeps the previous material (fix the files; they are re-read every
  `--tls-reload-secs`).
- Rotation: renew leaves under the same CA at any time (new connections
  use them; open ones continue). CA rotation: put both roots in every
  bundle, move every leaf to the new CA, then remove the old root (open
  connections authenticated by it are closed). Revocation: a CRL listing
  the serial closes the peer's connections and refuses it.
- Expiry: alert `ArcaCertificateExpiring` (`*_tls_cert_not_after_seconds`
  within 14 days).

FoundationDB: the cluster file and the TLS flags must agree (all
coordinators `:tls` with `--fdb-tls-*`, none without); a mismatch is
refused at start. A peer verification failure appears as an
unavailable database: check `--fdb-tls-verify-peers` against the server
certificates' SANs and the client certificate against the servers'
trust.

### 10.9 Storage-node memory (Phase 3.5, R-70)

Alerts `ArcaNodeHeapGrowing` (heap in use rising more than 8 MiB/h for
6 h) and `ArcaNodeMemoryHigh` (anonymous memory above 2 GiB for 30 min).

1. Compare `arca_node_process_memory_bytes{kind="heap_in_use"}` with
   `kind="heap_free"`: growth of `heap_free` with flat `heap_in_use` is
   glibc arena retention, not the program holding memory; setting
   `MALLOC_ARENA_MAX=2` in the node's environment bounds it (measured in
   the Phase 3.5 report).
2. Compare `arca_node_index_entries`: `live` follows the shards stored,
   `dead_tracked` the non-live records compaction has not reclaimed yet
   (bounded by compaction), `tombstone_history` must be 0 after startup.
   A heap growing with none of them is unexplained: capture
   `/proc/<pid>/smaps_rollup` and the metrics, then restart.
3. Restart safely: one node at a time, waiting for
   `arca_controller_groups_reduced_redundancy` to be 0 and no group
   `Repairing` first. A node restart costs one shard of redundancy for
   the chunks written during its downtime until stripe repair finishes.

### 10.10 GC backlog (Phase 3.5, R-64; Phase 3.6)

`arca_controller_gc_backlog` is the retired-generation queue after the
last pass (counted up to 200 000); `_bytes` its logical bytes (entries
written before Phase 3.6 carry no size), `_physical_bytes` an estimate
with the groups' coding overhead, `_oldest_age_seconds` its oldest entry,
`arca_controller_gc_reclaim_bytes_per_second` what GC gave back recently.
In steady state the oldest age stays near the grace period plus the GC
interval and the backlog near the retirement rate times that; a
multipart object's parts are retired as of its root and collected in the
same pass (ADR-0020). GC is not keeping up when the backlog keeps growing
(`ArcaGcBacklogGrowing`, `ArcaGcUnableToKeepUp`) or the oldest age grows
well beyond that (`ArcaGcFallingBehind`).

Where the time goes: `arca_controller_gc_stage_seconds{stage}`. On the
validation hosts the metadata stages take milliseconds per generation and
a DeleteShard waits for the node's group commit; a slow `delete_rpc`
points at the device (or at a node that cannot write tombstones: a full
device, §10.11), a slow `verify`/`finish` at FoundationDB. Writes are
throttled to the reclaim rate before the devices fill (§6); lower the
overwrite/delete rate or add device bandwidth if the backlog stays high.

### 10.11 Writes refused or throttled for capacity (Phase 3.6)

Clients see `503 SlowDown` with `Retry-After`; the message says
"write rate limited" (throttled), "new data refused" (write rejected or
recovery only) or "no stripe group has capacity" (every group was left
out or refused). Reads, deletes and completions keep working.

1. `arca_controller_backpressure_state` and the leader's log line
   "write admission state changed" give the tier and the reason.
2. `arca_controller_capacity_bytes{kind}`: `headroom` is what client
   writes may still use; `reclaimable` what compaction can give back;
   `arca_controller_admission_excluded_groups` the groups out of
   placement. Per device: `arca_node_capacity_bytes{kind}` (a group is out
   when one member is short: check `available` against `reserved` on each
   member).
3. Space comes back from deletes, GC (after the grace) and compaction
   (`arca_node_compaction_*`); the state then lifts one step per dwell
   time (60 s), so writes return gradually. Add devices or nodes for a
   lasting fix; `admin replace-member` moves a member off a full device.
4. A stale record (`ArcaCapacityReportStale`): no controller leader is
   publishing (§5.6); gateways throttle after 30 s and refuse new data
   after 5 min until one is back.
5. Nodes refusing writes (`arca_node_admission_refused_total{class}`,
   `arca_gateway_node_capacity_refusals_total`) while the tier is
   `normal` means devices filled faster than the controller saw (a device
   shared with other data, or a burst within one health interval); the
   reserve is what protects the cluster then. Repair refusals
   (`class="repair"`) mean the device is at its repair floor: free space
   before anything else (`ArcaRepairReserveLow`).

### 10.12 Deletes stopped: the delete fence (Phase 3.9, ADR-0023)

Storage nodes apply a shard delete only under the delete fence
generation they hold open. `admin delete-fence status` shows the
recorded fence (generation, open or closed, operator hold, who changed
it and why) and every registered node's, with the reason deletes are
stopped. Metrics:

- controller: `arca_controller_delete_fence_generation`, `_open`,
  `_nodes_current`, `_nodes_stale`, `_refusals_total`
- node: `arca_node_delete_fence_generation`, `_open`, `_hold`,
  `_refusals_total`
- GC and reconciliation: `arca_controller_gc_backlog*`

What deletes stop never stops PUT or GET.

1. **Normal.** Every node is at the leader's generation, open
   (`nodes_stale` 0). A node's generation rises at every leader change.
   Refusals happen briefly after a leader change or a node start, then
   stop.
2. **Before a restore.** Run `admin delete-fence close --reason R --yes`
   while the current metadata is reachable, then stop controllers and
   gateways (§5.9 step 0). Exit 1 means a node did not acknowledge;
   deletes may still run there.
3. **During a restore.** The fence stays closed with the hold. Nothing
   that runs on the restored metadata opens it: a controller of the
   restored incarnation is refused by the nodes, and `recovery begin`
   closes it again before marking. `admin delete-fence release` is
   refused outside `Active`.
4. **After a restore.** `recovery finalize` releases the hold; the
   leader opens a new generation within one health pass. If `status`
   still shows nodes held, a node missed the release: run `admin
   delete-fence release --reason R`, which pushes it to every node.
5. **Fence update failed.**
   - A close that some nodes did not acknowledge leaves deletes possible
     on those nodes only. Re-run it once they answer.
   - An open push a node did not take (unreachable, or a persist error,
     which it logs) leaves that node refusing deletes. The leader
     repeats the push every pass.
   - Nothing reopens deletes on its own except a leader of an `Active`
     cluster.
6. **A node does not respond.** It keeps the fence it last
   acknowledged, persisted across restarts. For a close, a node that
   cannot answer cannot be confirmed: bring it back. Only if its process
   is stopped and stays stopped, use `--absent-node ID`, and keep it
   stopped until the leader of the cluster that follows has pushed it a
   fence (`status` shows it current).
7. **A stale node rejoins** (restarted, partitioned, restored from an
   old disk). It refuses every delete until it holds the leader's
   generation, which happens within one health pass of the leader
   reaching it. It never goes back to a lower generation and never
   reopens a closed one, so deletes queued for it before it left are
   refused (counted in `arca_node_delete_fence_refusals_total`).
8. **GC backlog grows while deletes are stopped.** This is expected; the
   space is returned only once deletes resume. GC defers generations and
   reconciliation keeps orphans (`kept_fenced` in its report). Both
   resume and work the backlog off after the fence is open again (§10.10
   for the pace). If the backlog grows while `status` shows the fence
   open everywhere, the cause is not the fence: see §10.10. A long hold
   uses capacity: watch §10.11.

9. **A node holds its fence after a restart with "cluster facts damaged
   or missing"** in its log (`arca_node_delete_fence_hold` 1, generation
   0). Its `cluster.txt` could not be read, had a damaged line, or is
   missing while the store holds data: the node no longer knows its
   generation floor, hold or incarnation, so it refuses every delete
   until an operator releases it. Check the disk first; then, with the
   cluster `Active`, run `admin delete-fence release --reason R`. The
   leader's next pass opens its generation there. A release names the
   recorded closed generation; a node holding a higher one (it saw a
   later close) keeps its hold, and the command reports it.

10. **Phase 3.10: the rejoin lease** (ADR-0024). An open fence admits
    deletes on a node only for `--fence-lease-secs` (30 s) after the
    leader's last push of it. After a node restart, or a partition longer
    than the lease, the node refuses deletes until the leader's next pass
    reaches it (`arca_node_delete_fence_refusals_total` rises briefly).
    Keep the lease well above `--health-interval-secs`.
11. **"storage nodes hold generation N above the metadata's" or "the
    leader wrote lineage N and reads M"** in the fence reason, with
    `arca_controller_delete_fence_metadata_behind_total` rising: the
    metadata went back under the leader (a restore without `admin
    recovery begin`, or a hand edit). The fence is closed with a hold.
    Stop and run the restore procedure from `recovery begin` (§5.9);
    `admin delete-fence release` only if the metadata is known to be
    current.
12. **"generation pushed open by another opener"** in a node's log: two
    controller processes pushed the same generation number (one of them
    on restored metadata, or a node's opener record was lost). The node
    closed that generation; the leader opens a higher one at its next
    pass. Repeated messages mean two clusters share these nodes.
13. **`arca_controller_incarnation_conflicts_total` rising**: a node
    holds another incarnation at or above the cluster's sequence (it
    missed one `recovery begin` and took part in another). It refuses this
    cluster's deletes. Run `admin recovery begin` again with every node
    reachable: it marks an incarnation above every node's sequence.

There is deliberately no command that forces deletes open.

### 10.13 Deferred reclamation (Phase 3.12, ADR-0028)

- Space does not come back after deletes: it returns after the nodes'
  retention (`reclaim_pending` in health); check
  `arca_controller_reclaimed_total` grows and
  `arca_controller_reclaim_errors_total` does not.
- `arca_controller_nodes_not_deferring` above 0: a node of an earlier
  release or with `--reclaim-retention-secs 0`; restores are not protected
  on it.
- `reclaim_damaged` on a node: stop it, move `pending-reclaim.log` aside,
  start it (nothing was reclaimed meanwhile; what is unneeded is retired
  again).
- After any restore, `admin recovery begin` (or `admin restore`):
  retirements of the earlier incarnation then wait for a delete under the
  new one, and reconciliation re-issues those deletes.

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
