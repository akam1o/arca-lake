# RFC-015: Backup, Restore and Disaster Recovery

- Status (Phase 3.3: implemented as amended by ADR-0016; `admin recovery begin|status|verify|resume|finalize` and `admin cluster set-read-only|activate` are the command names): draft, conditionally approved (Phase 3.0 review reflected; cluster-state record in Phase 3.1, remainder and drills Phase 3.3)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-003, RFC-004, RFC-006, RFC-008, RFC-011, RFC-014
- Related ADRs: ADR-0007 (GC safety), ADR-0008 (deletion epochs), ADR-0013

This is a public document.

## 1. Problem

The metadata store is the only place that says which objects exist and
where their shards are; the storage nodes hold the bytes and know
nothing about objects. A FoundationDB backup is therefore necessary but
not a backup of the system: restoring metadata to time `T` while the
nodes hold the state of time `now` creates three kinds of
contradiction, and the controller's reconciliation and GC — designed to
remove orphans — would turn one of them into data loss if they ran
unchanged. This RFC defines what is backed up, what a restore means,
what must be verified before the cluster is allowed to delete anything,
and what the recovery guarantees are.

## 2. Goals

- Continuous, point-in-time-capable backup of metadata with FoundationDB's
  own tools; a documented RPO and RTO.
- A restore procedure that reconciles metadata-at-`T` with nodes-at-`now`
  safely: nothing is deleted until verification says it is unreferenced
  under the restored metadata *and* the post-`T` writes are accounted
  for.
- Explicit handling of orphaned shards (written after `T`), missing
  shards (deleted after `T` by GC), stale group epochs (raised after
  `T`), and cache metadata.
- Disaster recovery scenarios with their outcomes stated honestly.

## 3. Non-goals

Backing up shard data (durability of the bytes is erasure coding across
failure domains; a copy of every segment would double the footprint —
site-level replication is a later RFC); object-level restore of a
deleted object (versioning covers that within its retention);
continuous replication of metadata to another site (FoundationDB DR
agents can do it; the procedure below applies to the DR copy as to a
backup).

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| B1 | `fdbbackup` continuous backup to an object store or file system, with point-in-time restore to any `T` in the retention window | Assumption (FoundationDB feature; procedure to be exercised) |
| B2 | Restore never results in GC or reconciliation deleting shards that the restored metadata does not reference *until* a verification pass has classified them | Open (§5.3) |
| B3 | Objects whose shards were collected after `T` are detected and reported, never served as bytes | Open (§5.3) |
| B4 | Group epochs on nodes ahead of restored metadata are caught up without data movement | Open (§5.4) |
| B5 | Cache metadata after restore is treated as advisory: wrong placements fall back and are invalidated; the usage counter is recomputed | Partly verified (fallback and invalidation exist, RFC-006 §5.3); recompute Open |
| B6 | A full DR drill (restore into an empty FoundationDB cluster with the existing nodes) is executed on the validation hosts and timed | Open (Phase 3.3) |

## 5. Proposed design

### 5.1 What is backed up

| Item | How | Why |
| --- | --- | --- |
| Metadata (`("al",…)`) | `fdbbackup start -d <url> --continuous` (whole database; arca-lake owns the keyspace) | the truth about objects, placements, groups, epochs, leases, GC queues |
| Schema, `schema_write`, `format_write` and cluster-state keys | inside the above | needed for RFC-014's startup gate and RFC-015's state machine after restore (the restored state is overwritten by `restore mark`) |
| Node identities | node records inside the above; each node's `group-epochs.txt` and segment directory stay on the node | the restore procedure treats nodes as live, not as backups |
| Credentials, certificates, cluster file | **not** in FoundationDB; backed up by the deployment's secret management | RFC-012 §5.10 |
| Configuration (flags, profiles) | deployment's configuration management | reproducibility |

Backups contain no credentials. They contain object keys and user
metadata: encrypt the backup location (FoundationDB supports encrypted
backup URLs; otherwise encrypt the target).

### 5.2 Restore semantics

Restoring to `T` means: every object committed before `T` exists with
its placements as of `T`; every object committed after `T` does not
exist; every deletion after `T` is undone *in metadata*. On the nodes,
however, time went on to `now`:

| Node state vs restored metadata | Name | Consequence | Handling |
| --- | --- | --- | --- |
| Shards of generations committed after `T` | **orphans** | metadata does not reference them; reconciliation would delete them (RFC-008 §6) | they are *not* recoverable as objects (their headers are gone); deleting them is correct, **but only after** verification (§5.3) so that the next class is found first |
| Shards collected by GC after `T` for objects deleted after `T` | **missing** | metadata says the object exists; its shards are gone | detected by verification; the object is marked `Unreadable` and reported; it cannot be served |
| Shards partially collected (some members) | **degraded** | fewer than all, maybe fewer than `k` | ≥ `k` → repair rewrites; < `k` → unreadable, reported |
| Group epochs raised after `T` (repairs, membership changes) | **stale epochs** | restored group records have lower epochs than nodes; nodes reject writes with those epochs | epoch catch-up (§5.4) |
| Membership changed after `T` | **stale membership** | restored group lists an old member that has since been replaced | verification reads from any `k` members that still hold the shard; repair re-establishes the restored membership or the operator accepts the node's view (§5.4) |
| Cache placements | **advisory** | copies may be gone or belong to different generations | fallback and invalidation (exists); usage recomputed |
| Pending generations at `T` | **expired** | leases long gone | GC's expiry pass handles them normally |

### 5.3 Cluster recovery states

Phase 3.0 review item 1.4: the deletion freeze is one consequence of an
explicit cluster state machine stored in metadata.

`("al","meta","cluster") = ClusterState { incarnation: [u8; 16], state,
restored_at_ms: Option<u64>, restored_from: Option<String>,
changed_ms, changed_by }` is read at the start of every mutating
transaction of every component (one extra read of a key that only the
operator or the verification task writes: no conflicts in steady state)
and refreshed by read-only paths every few seconds.

| State | Client reads | Client writes | GC / reconcile / eviction / tiering / lifecycle / multipart sweep | Repair | Verification | Entered by |
| --- | --- | --- | --- | --- | --- | --- |
| `Active` | yes | yes | yes | yes | — | `admin cluster activate` (from `ReadOnly`) or initialization |
| `ReadOnly` | yes | no (`503 ServiceUnavailable`, `x-amz-error-detail: cluster read-only`) | **no** | yes (adds shards, never deletes) | optional | `admin cluster set-read-only` (maintenance) or verification success |
| `Restoring` | **no** (`503`) | no | no | no | no | `admin restore mark --at T` **before** any arca-lake process is started on restored metadata (the command also writes the new incarnation) |
| `VerificationPending` | yes, unverified objects may fail with an integrity error, never with wrong bytes | no | **no** | no (membership may be stale) | running | epoch catch-up completed (§5.4) |
| `RecoveryRequired` | yes, as above; objects classified unreadable return the integrity error | no | **no** | yes, for degraded chunks | re-runnable | verification finished with unreadable objects, missing shards, or an incomplete pass (node absent) |

Transitions: `Active → Restoring` (operator, after `fdbrestore`);
`Restoring → VerificationPending` (epoch catch-up done); `VerificationPending
→ ReadOnly` (verification complete, nothing unreadable, every node
listed); `VerificationPending → RecoveryRequired` (otherwise);
`RecoveryRequired → ReadOnly` (`admin restore accept`, which marks the
listed objects `Unreadable` and is audited; or nodes return and a re-run
finishes clean); `ReadOnly → Active` (`admin cluster activate`);
`Active ↔ ReadOnly` (maintenance). No other transition exists; in
particular there is no path from `Restoring`, `VerificationPending` or
`RecoveryRequired` to `Active` that skips `ReadOnly`, and **no state
other than `Active` allows a client PUT**.

**Cluster Incarnation ID.** `restore mark` writes a fresh incarnation.
Every process records the incarnation it started under and includes it
in every mutating transaction's read set; a mismatch aborts the
transaction with `ClusterIncarnationChanged`, after which the process
re-reads the state and, since it is not `Active`, refuses writes. Storage
nodes persist the incarnation they last saw (`cluster.txt` next to
`group-epochs.txt`, Phase 3.3) and report it in `Health`; the
controller's reconcile and GC refuse to delete on a node whose reported
incarnation differs from metadata's. This fences gateways and
controllers that were running during the restore and a controller whose
lease token predates it (the lease key is restored too, so the fencing
token alone would not distinguish).

**Why `Restoring` is mandatory.** Restored metadata looks exactly like
live metadata; nothing in it says "restored". The operator's `restore
mark` is the only point where that fact enters the system, so the
procedure requires it before any process starts, and the nodes' epoch
fencing provides the backstop if the operator forgets: node epochs are
higher than restored group epochs, so writes with restored epochs are
rejected (`FAILED_PRECONDITION`), and reconcile refuses on an
incarnation mismatch.

### 5.3.1 Restore procedure

1. **Freeze**: stop gateways and controllers (or `set-read-only` first,
   then stop).
2. **Restore** FoundationDB to `T` (`fdbrestore`) or into a fresh
   cluster.
3. **Mark**: `admin restore mark --at T --from <backup-url>` → state
   `Restoring`, new incarnation, `restored_at = T`.
4. **Start** controllers (they see `Restoring`, run nothing but the
   recovery tasks) and gateways (they refuse everything with `503`).
5. **Placement epoch recovery** (§5.4): controller task, metadata-only;
   on completion state → `VerificationPending`. Reads now open.
6. **Verification pass** (§5.3.2) → `ReadOnly` or `RecoveryRequired`.
7. **Report** (`admin restore report`) and **decide**: `accept`, or
   restore to another `T` and repeat from step 2.
8. **Activate** (`admin cluster activate`): writes open; GC, reconcile,
   eviction, tiering, lifecycle and the multipart sweep resume; orphans
   are deleted by reconcile only after `min_orphan_age` as usual.

### 5.3.2 Verification pass

Runs on the controller leader, resumable (cursor per group and per
node), rate limited, in `VerificationPending` or `RecoveryRequired`:

- **Object integrity verification**: for every committed generation
  (objects, multipart roots via their part references, parts), for every
  chunk record: `VerifyShards` on every member of the placement's group;
  classify `ok` (all shards verify), `degraded` (≥ `k` verify), 
  `unreadable` (< `k`). Degraded chunks are queued for repair;
  unreadable generations are listed in the report.
- **Missing shard detection**: a shard that metadata references and no
  member holds (deleted by GC after `T`) is `missing`; a chunk with
  fewer than `k` shards present is `unreadable` (a special case of the
  above) and the report distinguishes "missing" from "corrupt".
- **Orphan classification**: `ListShards` on every node; a shard is
  `referenced` (a live generation places it, RFC-008 §6.3), `orphan
  post-T` (generation id UUIDv7 time ≥ `T` and no generation record:
  written after `T`, unrecoverable as an object because its header is
  gone; the same millisecond counts as post-T, the conservative side; an
  id carries the start time of its write, so a write that started before
  `T` and committed after it reads as pre-T), `orphan pre-T` (older,
  unreferenced: ordinary garbage), or
  `former-member`. Nothing is deleted; counts and bytes per node go to
  the report.
- **Completion criteria**: every group's every referenced chunk has a
  classification; every registered node answered `ListShards`
  completely (a node absent during the pass makes it *incomplete* and
  the state `RecoveryRequired`, unless the operator excludes the node
  with `--exclude-node`, which is audited and recorded in the report);
  the report `("al","meta","restore","report",incarnation)` is written
  in one transaction at the end with the incarnation id, so a report
  from a previous incarnation can never be mistaken for this one.
- **Safe recovery resume**: after a controller crash the next leader
  resumes from the cursors; the state does not change until the final
  transaction; `accept` is refused while the pass is incomplete unless
  `--force` (audited).

### 5.4 Epoch catch-up

Nodes persist the highest epoch per group (`group-epochs.txt`); restored
group records may carry a lower epoch. Writers would be rejected
(`FAILED_PRECONDITION`). Catch-up: for every group, the controller reads
each member's epoch (`Health` extended with per-group epochs, Phase 3.3)
and sets the group record's epoch to `max(member epochs) + 1`, then
fences every member to it (`UpdateGroupEpoch`), as a membership change
does (RFC-008 §6.4). This is metadata-only and does not move data. If a
restored member no longer holds the group (it was replaced after `T`),
the operator chooses: keep the restored membership and let repair
rewrite the member (data movement), or `admin restore adopt-membership`
to take the nodes' current membership for that group (metadata-only;
valid only if ≥ `k` of the restored data shards verify on it).

### 5.5 Cache metadata recovery

After restore the cache tier's placements may point at copies that no
longer exist or that hold different generations' data. Nothing needs a
special pass: a lookup hit that cannot be read falls back to the durable
read and invalidates (RFC-006 §5.3); the recency index is advisory. The
usage counter (`("al","cusage",tier)`) is recomputed by `admin cache
recount` (sum of placement bytes, one range read per tier) so that
capacity checks are correct again; eviction then proceeds normally.

### 5.6 Cluster state enforcement

The `ClusterState` record replaces the earlier `flags` sketch. Enforced
in: the gateway's request admission (state and incarnation checked per
request from a cached copy refreshed every 2 s, and inside every commit
transaction); the controller's scheduler (before every pass) and every
deleting or moving transaction (GC finish, reconcile delete, eviction,
tiering switch, lifecycle delete, multipart sweep); `admin` mutating
commands. The check is a read of one key in the same transaction, so a
stale leader or a stale gateway cannot bypass it.

### 5.7 Recovery objectives

| Scenario | RPO | RTO (estimate; measured in the drill) | Outcome |
| --- | --- | --- | --- |
| FoundationDB process loss within redundancy | 0 | seconds (FoundationDB) | no action |
| FoundationDB quorum loss, disks intact | 0 | minutes (FoundationDB recovery) | no action in arca-lake |
| FoundationDB data loss, continuous backup | backup lag (seconds–minutes) | restore time + epoch catch-up + verification (proportional to shard count; the drill measures per million shards) | post-`T` writes lost; post-`T` deletes undone except where GC already ran → reported unreadable |
| Loss of `≤ m` nodes per group | 0 | repair time | no action beyond repair |
| Loss of `> m` nodes in one group | n/a | — | those chunks unrecoverable; reported; objects marked unreadable |
| Loss of a whole site | everything (no cross-site copy) | rebuild | only what a client kept; cross-site replication is a future RFC |
| Operator error (wrong `T`) | — | repeat restore | no deletion happened before `accept` |

### 5.8 Backup validation

A scheduled job restores the latest backup into a scratch FoundationDB
(no nodes), runs `admin restore verify --metadata-only` (consistency of
records: every current pointer has a header, every header's chunks are
present, every group's members are node records) and reports; a
backup that fails this check pages.

## 6. Alternatives considered

| Decision | Chosen | Alternatives |
| --- | --- | --- |
| Backup unit | FoundationDB backup only + live nodes | full node snapshots (2× capacity, inconsistent with metadata time anyway); application-level export of manifests (slower, same gaps) |
| Post-restore safety | explicit cluster state machine (`Restoring` → `VerificationPending` → `ReadOnly`/`RecoveryRequired` → `Active`) with an incarnation id, checked in every mutating transaction | a single boolean flag (no distinction between "verifying", "needs a decision" and "maintenance"; no fencing of stale processes); rely on `min_orphan_age` (post-`T` orphans older than that would be deleted before anyone looked); a long global pause (humans forget) |
| Unreadable objects | marked, refused, reported | silently 404 (hides loss); serve partial bytes (never) |
| Epochs | catch-up from nodes, metadata-only | rewrite all shards (unnecessary); ignore epochs (breaks fencing) |
| Read-only flag | metadata key checked per transaction | process flags (not fenced; forgotten on restart) |

Evaluated on durability (nothing deleted without classification),
consistency (unreadable is a state, not a lie), performance
(verification is a scan proportional to shard count, rate limited),
operations (five commands, one report), security (flags and reports are
metadata under the controller's identity).

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Controller crashes during verification | cursor; the next leader resumes; the gate stays set |
| A node is down during verification | its shards count as missing for that pass; the report marks the pass incomplete for that node; re-run after the node returns before `accept` |
| Operator runs `accept` with an incomplete report | refused unless `--force`, which is audited |
| Backup target unreachable | FoundationDB backup lags; alert on backup age (RFC-013 §8 gains a rule) |
| Restore into a cluster with a different layout version than the backup | RFC-014 startup gate refuses the processes until the matching software is used |

## 8. Consistency and durability guarantees

After `accept`, the cluster satisfies RFC-008's invariants with respect
to the restored metadata: every referenced shard verified or is queued
for repair, every unreferenced shard is subject to the ordinary orphan
rules, every unreadable object is marked and never served. Durable ACK
holds for writes after the restore; writes between `T` and the failure
are lost and that loss is reported, not hidden.

## 9. Performance considerations

Verification reads every shard header and, with `--deep`, every byte;
header-only verification of a million shards is minutes, deep
verification is bound by disk read throughput (RFC-009 §6.13). Both are
rate limited so that reads continue to be served during the pass.

## 10. Open questions

- Should verification also compare shard CRCs across members to catch
  post-`T` *overwrites* of a chunk id (impossible by construction: chunk
  ids are generation-unique UUIDv7)? Proposal: no.
- Retention of the restore report: keep until the next restore
  (proposal) or forever.

## 11. Acceptance criteria

Labels as RFC-007 §11.

| # | Criterion | Status |
| --- | --- | --- |
| B-A1 | `ClusterState` record exists; gateways refuse PUT/DELETE/multipart/copy with `503` in every state but `Active`; GC, reconcile, eviction, tiering and the multipart sweep refuse to run in every state but `Active` (and `ReadOnly` for repair only) | Verified: `recovery.rs::every_phase_admits_exactly_what_rfc_015_says` (every phase, admission and the commit-time guard per operation class); on FoundationDB `fdb_restore.rs` and `fdb_failover.rs::recovery_survives_…` (deleting passes skipped, shard counts never drop while not `Active`). Amendment (ADR-0016): repair is also admitted in `RecoveryRequired` |
| B-A2 | Incarnation fencing: a process started under incarnation A cannot commit a mutating transaction after `restore mark` writes incarnation B (fault test with a stale leader and a stale gateway) | Verified: `recovery.rs::a_stale_incarnation_is_fenced_…` (metadata and `DeleteShard` on nodes, across a node restart); `fdb_failover.rs::recovery_survives_…` on a three-process FoundationDB cluster with process and quorum loss (stale client-write, delete and repair stores refused before and after the outage, stale node delete refused, stale gateway refused) |
| B-A3 | Drill on validation hosts: write, back up, write and delete more, restore to `T`, mark, epoch catch-up, verify; the report lists exactly the objects whose shards were lost after `T` as unreadable and the post-`T` shards as orphans; GET of an unreadable object returns the integrity error; accept, activate; reconcile deletes orphans; repair fixes degraded chunks; every surviving object reads back | Verified: `fdb_restore.rs` with real `fdbbackup`/`fdbrestore` (restore to the snapshot end, `-v`), on a single-process cluster (CI FoundationDB job, validation host) and on a three-process `double` cluster (CI failover job, validation host). Simplification: the lost-after-`T` object's shards are removed by node-level deletes instead of a GC run (the metadata state is the same); timing is not recorded |
| B-A4 | Verification completion criteria enforced: a node stopped during the pass yields `RecoveryRequired` and `accept` is refused without `--force` | Verified for the state machine: `recovery.rs::a_node_that_does_not_answer_makes_the_verification_incomplete` (a member stopped mid-pass ⇒ `complete = false`, the node listed, `RecoveryRequired`; after it returns a re-run is complete and clean). The `admin recovery finalize` refusal without `--force` is Implemented and not covered by a test |
| B-A5 | Backup validation job detects a corrupted backup fixture | Not Implemented (Phase 3.4) |
| B-A6 | No PUT succeeds between `restore mark` and `activate` | Verified by direct attempts in `recovery.rs`, `fdb_restore.rs` and `fdb_failover.rs` (stale and restarted gateways, every phase); not by a concurrent load generator |
