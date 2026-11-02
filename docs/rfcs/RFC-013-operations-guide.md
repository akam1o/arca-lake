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
7. Start gateways with credentials and certificates; smoke test with the
   AWS CLI (`aws s3 mb`, `cp`, `ls`, `rm`).
8. Record the layout version (`admin layout show`; RFC-014) and take the
   first backup (RFC-015).

### 5.3 Node addition (capacity expansion)

1. Start the node process; `admin register-node`.
2. Create new groups over the new node and existing ones (`admin
   create-groups --count …`); the gateway picks groups by free space
   (`WritableGroup.min_free_bytes`, RFC-004 §5.5), so new groups receive
   new writes. Existing groups are not rebalanced automatically; to move
   data, replace a member (`admin replace-member --group G --from N --to
   M`, Phase 3.2) which runs the repair path.
3. Verify: `arca_controller_nodes_total` increments; the new node's
   `live_bytes` grows; no `task_errors`.

### 5.4 Node removal (decommission)

1. `admin decommission-node N --dry-run`: lists groups where N is a
   member and the replacement candidates per failure domain.
2. `admin decommission-node N`: marks the node `Draining`; the controller
   replaces N in every group it belongs to (repair path, one group at a
   time, rate limited by `--repair-rate`), then marks it `Retired`.
3. Wait for `arca_controller_repairs_completed_total` to reach the dry
   run's count and `admin status` to show no group with N; stop the
   process; remove the node record (`admin remove-node N`), which is
   refused while any group still lists it.
4. Rollback: `admin decommission-node N --cancel` before `Retired`
   returns the node to `Active` for groups not yet re-membered.

### 5.5 Device replacement (failed disk)

A failed device is a node that is unhealthy: with `--auto-repair` the
controller replaces it in every group after `--unhealthy-after-secs`
(RFC-011 §5.5) — no operator action is needed for durability. To reuse
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

RFC-014. Order: storage nodes, then controllers, then gateways; one
host at a time; verify after each; finalize the layout version last.

### 5.9 Backup and restore

RFC-015: continuous FoundationDB backup, node segment directories are
not backed up (they are the data; durability is coding), post-restore
verification before reconciliation resumes.

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
`--http`. The registry below is normative; a test asserts that every
exported series is listed.

| Series | Type | Process | Meaning |
| --- | --- | --- | --- |
| `arca_controller_leader`, `_leader_token` | gauge | controller | 1 on the leader; fencing token |
| `arca_controller_leader_acquired_total`, `_lost_total` | counter | controller | elections |
| `arca_controller_nodes_total`, `_nodes_unhealthy` | gauge | controller | membership health |
| `arca_controller_gc_passes_total`, `_gc_collected_total`, `_shards_deleted_total`, `_gc_skipped_referenced_total` | counter | controller | GC; `skipped_referenced > 0` is an invariant violation |
| `arca_controller_repairs_started_total`, `_completed_total`, `_chunks_reconstructed_total`, `_task_errors_total` | counter | controller | repair |
| `arca_controller_cache_usage_bytes`, `_cache_over_watermark`, `_cache_evicted_total`, `_chunks_promoted_total`, `_chunks_demoted_total`, `_cache_repairs_*` | mixed | controller | cache tier |
| `arca_controller_scrub_*` (Phase 3.2) | counter | controller | shards verified / corrupt / repaired / unrecoverable |
| `arca_gateway_cache_*_total` (25 series, RFC-006 §5.3) | counter | gateway | hit accounting |
| `arca_gateway_node_rpc_{stalled,deadline_exceeded,unary_timeouts,retries,exhausted}_total` | counter | gateway | ADR-0011 |
| `arca_gateway_put_phase_seconds{phase}`, `arca_gateway_put_in_flight` | histogram, gauge | gateway | RFC-009 §6.14 |
| `arca_gateway_population_queue_*` | counter, gauge | gateway | RFC-006 §5.3.1 |
| `arca_gateway_meta_txn_{attempts,conflicts,unknown_retries,exhausted}_total` | counter | gateway | metadata transaction loop |
| `arca_gateway_s3_requests_total{op,status}`, `arca_gateway_s3_request_seconds{op}`, `arca_gateway_s3_bytes_total{dir}` (Phase 3.1) | counter, histogram | gateway | request accounting (no key or principal labels) |
| `arca_gateway_auth_failures_total{reason}`, `_rate_limited_total`, `_over_budget_total`, `_credentials_reload_failures_total`, `_tls_cert_not_after_seconds` (Phase 3.1) | counter, gauge | gateway | RFC-012 |
| `arca_node_phase_seconds{phase}`, `arca_node_put_in_flight` | histogram, gauge | node | RFC-009 §6.14 |
| `arca_node_live_shards`, `_live_bytes`, `_segment_bytes`, `_quarantined_total`, `_corrupt_reads_total`, `_scrub_*` (Phase 3.2) | gauge, counter | node | capacity and integrity |

Labels never carry addresses, host names, key ids, bucket names or
object keys (`docs/security/public-artifacts.md` applies to exported
series too).

## 8. Alerting

| Alert | Condition | Severity | Runbook |
| --- | --- | --- | --- |
| No controller leader | `sum(arca_controller_leader) == 0` for 1 min | page | §5.6; check FoundationDB |
| Node unhealthy | `arca_controller_nodes_unhealthy > 0` for 5 min | page if `≥ m` in one group's domain set, else ticket | §5.5 |
| GC invariant violation | `increase(arca_controller_gc_skipped_referenced_total[1h]) > 0` | page | §10.4 |
| Repair stuck | group `Repairing` and `repair_cursor` unchanged for 1 h | ticket | §10.5 |
| Unrecoverable chunk | `arca_controller_scrub_unrecoverable_total > 0` | page | §10.6 |
| Metadata conflicts | `rate(arca_gateway_meta_txn_conflicts_total[5m]) / rate(_attempts_total[5m]) > 0.05` | ticket | RFC-009 §6.17 |
| Metadata unavailable | `rate(arca_gateway_s3_requests_total{status="503"}[5m])` high and FoundationDB status not available | page | §5.7 |
| Capacity | node `segment_bytes` > 80 % of device | ticket at 80 %, page at 90 % | §5.3 |
| Cache over watermark for long | `arca_controller_cache_over_watermark == 1` for 30 min | ticket | RFC-006 §5.4 |
| Certificate expiry | `arca_*_tls_cert_not_after_seconds - time() < 14d` | ticket | RFC-012 §5.2 |
| Auth failures spike | `rate(arca_gateway_auth_failures_total[5m])` > baseline × 10 | ticket | audit log |
| RPC exhaustion | `rate(arca_gateway_node_rpc_exhausted_total[5m]) > 0` | ticket | network, node health |

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

### 10.6 Unrecoverable chunk reported (Phase 3.2)

Fewer than `k` shards verify. Action: `admin inspect-chunk` lists
members and their status; if a member is merely offline, bring it back
(repair resumes); if data is truly lost, the object is marked
`Unreadable` in listing output (`x-amz-meta-arca-integrity: unreadable`
on HEAD) and GET returns `500 InternalError` with a specific message
rather than bytes. Restore from a client-side copy; never un-mark
without verification.

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
