# RFC-014: Rolling Upgrade and Compatibility

- Status: draft, conditionally approved; Phase 3.3 implements it as amended by ADR-0016 (Phase 3.0 review reflected; prerequisites in Phase 3.1, remainder Phase 3.3)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-003, RFC-005, RFC-008, RFC-011
- Related ADRs: ADR-0006 (versioned formats), ADR-0013

This is a public document.

## 1. Problem

Three things are versioned today and none of them can be upgraded in a
running cluster: the gRPC protocol is checked for exact equality
(`PROTOCOL_VERSION == 1`), the metadata layout version key is declared
but never written or read, and record formats have one decoder. A
cluster can be upgraded only by stopping everything, and a build that
changes any format has no way to know whether every peer can read it.
Phase 3.1 adds record fields (bucket owner, policy, multipart) and
Phase 3.2 adds RPCs (scrub), so this must be solved first.

## 2. Goals

- A cluster with two adjacent software versions (N and N+1) of any
  component serves reads and writes correctly while an upgrade is in
  progress, in a defined order.
- A build can tell, before writing, whether every peer can read what it
  writes; new formats are written only after the operator *finalizes*
  the upgrade.
- Downgrade within the window is possible until finalization and
  refused, with a clear message, after it.
- Metadata schema changes are migrations with a version, idempotent and
  resumable, run by the controller leader.
- The controller hands leadership over without a gap longer than one
  election interval.

## 3. Non-goals

Skipping versions (N → N+2) in one rolling pass; upgrading FoundationDB
itself (its own procedure; the client library version is pinned per
arca-lake release, R-18); zero-downtime for a single-gateway deployment.

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| U1 | Protocol negotiation: a client speaks the highest version both sides support, within a window of two | Open (§5.2) |
| U2 | On-disk segment/record formats: every build reads all versions since 1; writes the newest version only when the cluster minimum allows | Verified for reading v1 (`format_compat.rs`); write gate Open (§5.3) |
| U3 | Metadata layout version written at initialization, checked at startup by every process; migrations bump it | Open (§5.4) |
| U4 | Record decoders for every past version; encoders for the current one; a compatibility test with golden encodings per version | Partly verified (v1 golden in metadata tests); Open for v2 |
| U5 | Upgrade order documented and enforced by the gates (a component refuses to start if the order is violated) | Open (§5.5) |
| U6 | Leader handoff on SIGTERM releases the lease; followers take over within `lease/3` | Verified (RFC-011; `release` on shutdown) |
| U7 | Mixed-version test: nodes at N and N+1 serving one group during PUT/GET/repair | Open (§11) |

## 5. Proposed design

### 5.1 Three independent compatibility axes

Phase 3.0 review item 1.3: compatibility is managed on three axes with
independent version numbers and independent compatibility ranges. A
release declares all three; a change on one axis never implies a change
on another.

| Axis | Symbol | What it versions | Where the number lives | Read range of a build | Write rule |
| --- | --- | --- | --- | --- | --- |
| Wire Protocol | `P` | gRPC messages and RPC semantics between gateway/controller and nodes | `PROTOCOL_VERSION` in `arca-lake-protocol`; advertised per node in `Health { protocol_min, protocol_max }` | `[P_cur − 1, P_cur]` | negotiated per connection (§5.2) |
| Metadata Schema | `M` | key layout and record encodings in FoundationDB | `("al","meta","schema")` (current schema), `("al","meta","schema_write")` (schema that writers may use), record version byte per record | records and layout `[M_cur − 1, M_cur]` | writers encode the version named by `schema_write` (§5.4) |
| Storage Format | `F` | segment header and record format on node devices | `SEGMENT_FORMAT_VERSION`/`RECORD_VERSION` in `arca-lake-storage`; `("al","meta","format_write")` (format nodes may write) | all `F ≤ F_cur` | a node writes `min(F_cur, format_write)` (§5.3) |

The software version (crate version) maps to a triple `(P, M, F)` in the
release notes. A rolling upgrade from release A to release B is
supported iff `P_B − P_A ≤ 1`, `M_B − M_A ≤ 1` and the build B reads
`F_A`. Anything else is not a rolling upgrade and the release notes say
so.

### 5.2 Wire protocol versioning and negotiation

- Every request carries `protocol_version` (exists). The node accepts
  `v` iff `P_cur − 1 ≤ v ≤ P_cur`. `Health` gains `protocol_min` and
  `protocol_max`; clients call `Health` on first connection to a node,
  cache the window, and send `min(P_cur_client, node.protocol_max)`. A
  node whose window does not intersect the client's is reported as
  incompatible (`arca_gateway_node_incompatible_total`) and skipped for
  placement.
- Additive changes (optional fields, new RPCs) do not bump `P`; a client
  tolerates `Unimplemented` for a new RPC on an old node and counts it.
- Semantic changes bump `P`; the node implements both behaviours for the
  window.
- Epochs and fencing are data, not protocol: nodes at different `P` in
  one group agree on epochs.
- **Mixed-version cluster**: a group may hold nodes at `P` and `P + 1`
  indefinitely; repair reads any member and writes the target in the
  target's negotiated protocol and allowed format.

### 5.3 Storage format and the write gate

- Reading: every build keeps decoders for all `F` since 1
  (`format_compat.rs` golden files, one per version).
- **Write gate**: a node writes format `min(F_cur, format_write)`, where
  `format_write` is read from metadata at start and re-read on every
  controller heartbeat response (nodes do not hold the cluster file; the
  controller passes the value in `Health` requests, and a node that has
  never heard a value writes `F` of its own previous release, i.e. the
  conservative choice). **While any node runs a build that cannot read
  `F_cur`, `format_write` stays below `F_cur`**, because finalization
  (§5.5) raises it only after every registered node reports a software
  version that reads `F_cur`. Mixed segments within one directory are
  allowed (the segment header carries its version).
- Downgrade check: `arca-lake storage-node --check-downgrade <F>` scans
  segment headers and refuses if any segment is newer than `F`.

### 5.4 Metadata schema versioning and migration

- **Schema key**: `("al","meta","schema") = M` is written at
  initialization (`admin init`; `create-groups` on an empty store calls
  it) together with `("al","meta","schema_write") = M`. Phase 3.1 writes
  `M = 2` on new clusters and treats a store with data but no schema key
  as `M = 1` (the Phase 2 layout), stamping the key on first contact.
- **Startup gate** (every process, read-only): refuse to start unless
  `M_cur − 1 ≤ schema ≤ M_cur`, with the message `metadata schema M_x
  requires software with M ∈ [x, x+1] (this build: M_cur)`. A build older
  than the schema never starts (downgrade prevention); a build newer
  than `schema + 1` never starts (no skipped versions).
- **Records**: every record carries its version byte (exists); a build
  decodes versions `M_cur − 1` and `M_cur` and encodes the version named
  by `schema_write` (writers read `schema_write` once per process start
  and on a periodic refresh; a stale value is always the *older* one,
  which every peer reads). Records are rewritten only when their
  operation rewrites them (lazy upgrade); a reader of `M_cur − 1` never
  meets an `M_cur` record before finalization.
- **Migrations** are named, idempotent and resumable (cursor in
  `("al","meta","migration",name)`), rate limited, executed by the
  controller leader under its fencing token, and never required for
  correctness before finalization (the old build ignores keys it does
  not know only if they are additive; a migration that is not additive
  is not a rolling upgrade).

### 5.5 Upgrade finalization (idempotent, crash-safe)

`arca-lake admin upgrade finalize --to <release>` performs these steps,
each a separate idempotent transaction that re-checks its precondition,
with a cursor `("al","meta","upgrade") = { target, step, started_ms,
by_token }` so that any crash (operator, controller, FoundationDB) is
resumed by re-running the command or by the controller leader's
finalize task, which picks up an unfinished cursor:

1. **Precondition**: every registered node and every controller and
   gateway heartbeat record (`("al","proc",id)`, written every 30 s with
   software version and `(P, M, F)`) reports a version ≥ target; nodes
   absent for longer than the heartbeat window block finalization unless
   `--ignore-absent <id>` is given (audited). Writes the cursor.
2. **Migration** (if the release has one): runs to completion via its
   own cursor; idempotent re-run after a crash.
3. **`schema_write := M_target`**: from now on writers encode new
   records; old builds can no longer start (the startup gate) — this is
   the **downgrade cut-off** and the release notes say so; a backup
   (RFC-015) is taken automatically before this step unless
   `--no-backup` is given.
4. **`format_write := F_target`**: nodes begin writing the new format at
   their next heartbeat.
5. **`schema := M_target`**, clear the cursor, audit record.

Idempotence: each step is a compare-and-set on the cursor's `step`; two
concurrent invocations serialize on the cursor key; a step that finds
its effect already applied advances the cursor without changing data.
Crash safety: no step depends on an in-memory value from a previous
step; a controller crash during step 2 resumes the migration from its
cursor; during steps 3–5 the next invocation re-reads the keys and
performs the remaining compare-and-sets. The controller leader checks
for an unfinished cursor on every election and finishes it (fenced).

### 5.6 Rollback and downgrade policy

- **Before step 3** (`schema_write` unchanged): roll back by
  reinstalling the previous build on any process in any order; every
  record and segment is of the old version; the cursor, if present, is
  cleared by `admin upgrade cancel`.
- **After step 3**: downgrade is refused by the startup gate. Recovery
  is forward (a hot-fix release in the same window) or restore of the
  pre-finalize backup (RFC-015) with its stated consequences (writes
  after the backup are lost). This is why finalization is a deliberate
  operator action and never automatic.
- **Downgrade prevention** is enforced, not advisory: the startup gate
  on `schema`, the node's refusal to open a segment newer than it
  understands, and the protocol window.
- **Phase 3.14 rollback cases** (real binaries,
  `previous_release::rollback_residuals_with_the_phase_3_13_and_3_12_releases`):
  a controller rolled back to 3.13 in the middle of a repair with a
  capacity reservation completes it (it ignores reservations) and the
  reservation is swept once this release leads again (safe); a node with
  a `pending-reclaim.prev` left by a crash is merged by 3.13 (safe) and
  forgotten by 3.12 while it runs (conditional: nothing deleted, merged
  back later; roll back to 3.12 only from a clean stop); nodes of 3.12,
  3.13 and this release under one leader all hold acknowledged
  identities and open fences (safe); a managed restore with 3.12 and 3.13
  nodes, then forward, repairs and reclaims as usual (safe). New fields
  and keys are ignored by earlier releases (`VerifyShards` 6/7 and 4–6;
  `capresv`, `bgcursor`).

### 5.7 Mixed-version test strategy

A rolling-upgrade test needs two builds. Until a release tag exists, CI
builds the workspace twice: once at `HEAD` and once with
`ARCA_LAKE_TEST_VERSION_BUMP=1`, which makes the build report `(P + 1,
M + 1, F + 1)` with identical semantics except for an additive test-only
field in `Health`, a test-only record field and a test-only segment
header flag. The mixed-version test runs one group with nodes from both
builds under PUT/GET/repair, upgrades the old nodes one at a time under
load, finalizes, verifies every object, and asserts that the old build
refuses to start. Once real tags exist, the previous tag replaces the
bumped build.

### 5.8 Upgrade order and procedure

1. Read the release notes: `(P, M, F)` deltas, migration, estimated
   duration, downgrade cut-off.
2. Storage nodes first, one failure domain at a time (SIGTERM, replace
   binary, start, wait for `Health` and `last_seen`); keep each restart
   under `--unhealthy-after-secs`.
3. Controllers: followers first, leader last (SIGTERM releases the
   lease; the new leader resumes repairs via cursors).
4. Gateways: drain, restart, return.
5. Verify `admin status --versions` shows one version everywhere and the
   conformance smoke test passes.
6. Finalize (§5.5) when satisfied; this is the downgrade cut-off.

Phase 3.11 (ADR-0027): upgrade **every controller** before a leader of
3.11 runs. A 3.11 leader gives the nodes identities, after which nodes
refuse deletes that carry none, which is what earlier controllers send:
a failover to an earlier controller, or a rollback of the controllers,
stalls GC and reconciliation (no data at risk; used space grows). The
new protocol fields are additive (`HealthRequest` 12–14,
`HealthResponse` 37–41, `DeleteShardRequest` 8–9); schema, protocol and
segment format versions are unchanged.

Why this order: nodes must understand the newest protocol before any
client speaks it (clients negotiate down, nodes cannot negotiate up);
the controller must be newest before a migration runs; gateways last so
the data path runs on the most tested combination only after nodes are
done.

### 5.9 Upgrade failure recovery

| Failure | Recovery |
| --- | --- |
| New node binary crashes on start | start the old binary (format not finalized → readable); the group is unaffected (one member) |
| New node starts but misbehaves (errors, corruption reports) | downgrade that node; `admin replace-member` if its data is suspect (repair rewrites it) |
| Controller at N+1 fails migration midway | migration cursor; fix forward with a patched N+1; `schema_write` not yet bumped, so N controllers can still lead (they ignore the partial migration keys, which are additive) |
| Controller crashes during finalize (any step) | the cursor records the step; the next leader or the next `finalize` invocation resumes; no step is half-applied because each is one compare-and-set transaction |
| Finalized, then a defect is found | no downgrade; hot-fix release N+1.1 with the same window; restore is the last resort |
| Gateway at N+1 returns new error shapes clients reject | drain it; downgrade; file the compatibility bug |

## 6. Alternatives considered

| Decision | Chosen | Alternatives |
| --- | --- | --- |
| Compatibility axes | three independent axes `(P, M, F)`, each with an adjacent-version range | one monolithic version (couples a wire change to a format change and forces needless cut-offs); unbounded any-to-any (untestable) |
| Format write gate | `format_write` key raised by finalization after every node reports a reading build | per-node auto-detection (a node cannot see every peer); time-based (unsafe) |
| Record migration | lazy decoders, writers encode the `schema_write` version, explicit idempotent finalize with a cursor | eager rewrite at upgrade (long, blocks downgrade immediately); dual-write (complex); finalize as one big transaction (not resumable, not auditable per step) |
| Protocol negotiation | window advertised in `Health`, client picks | strict equality (today; no rolling upgrade); per-RPC feature flags only (does not cover semantic changes) |
| Upgrade order | nodes → controllers → gateways | gateways first (new clients could speak a protocol no node has) |

Evaluated on consistency and durability (no step changes the commit,
fencing or GC rules; a node's format is self-describing), performance
(negotiation is one `Health` per connection), operations (one finalize
command; refusal messages name the required version), security
(finalization and migrations run under the controller's fenced identity).

## 7. Failure scenarios

Covered in §5.6; additionally: a partition during an upgrade is the same
as any partition (ADR-0011, RFC-008); a FoundationDB upgrade is a
separate procedure performed with all arca-lake processes at the same
version and never concurrently with a layout migration.

## 8. Consistency and durability guarantees

Unchanged. The layout gate guarantees that no process ever reads a
record version or key space it does not understand; the format gate
guarantees that no segment is written that a peer in the window cannot
read; fencing guarantees that a migration runs once at a time.

**Phase 3.12 (no schema or format change).** Nodes defer reclamation as
soon as they run the release; an earlier controller's deletes are retained
and wait for a controller of this release to reclaim them. Controllers of
this release push no delete fence to an earlier release answering where a
node acknowledged an identity (it would apply deletes whatever identity
they carry): such a node deletes nothing until it runs this release again.
Rolling a node back leaves its retired records live; back on this release
they are retired and reclaimed again. Verified against the Phase 3.10 and
3.11 binaries (`previous_release::same_schema_matrix_*`).

## 9. Performance considerations

Negligible on the data path. Migrations are rate-limited background
work; the release notes carry a measured duration per million objects.

## 10. Open questions

- Should `schema_write` also gate protocol version advertisement, so
  that no client *speaks* P_cur before finalization? (Proposal: no;
  protocol is negotiated per peer and both are tested; the gate is for
  persistent formats.)
- Heartbeat records for gateways and controllers (`("al","proc",id)`)
  so that `admin status --versions` can list them: adds a small write
  per minute per process (proposal: yes).

## 11. Acceptance criteria

Labels as RFC-007 §11.

| # | Criterion | Status |
| --- | --- | --- |
| U-A1 | Schema key written at initialization; a store with data but no key is stamped (at `M = 2`, ADR-0016, not `M = 1`); the startup gate refuses builds outside their window with the documented message | Verified: `recovery.rs::startup_gate_refuses_newer_schemas_and_stamps_legacy_stores` (in-memory); `mixed_version.rs` step 9 (FoundationDB, real old binary refused by gateway and controller after finalize) |
| U-A2 | Record version 2 (bucket owner/policy, generation kind/checksum) decodes version-1 records; golden encodings per version pass; a record change without a golden file fails CI | Implemented (`records.rs`, `Record::decode_v1`); Verified for the v1 → v2 decode of every changed record and for the refusal of a newer version byte (`upgrade.rs`); golden files per record version Not Implemented |
| U-A3 | Protocol window: a node accepts `P − 1` and `P`, advertises the window in `Health`, a client negotiates down; incompatible nodes are counted and skipped | Verified: `mixed_version.rs` steps 1–2 (a `P = 2` gateway and controller against `P = 1` nodes, real binaries on FoundationDB); disjoint windows are Implemented (`NodeCaps::compatible`, `node_incompatible`) but not exercised by a third binary |
| U-A4 | Format write gate: with `format_write < F_cur` a node writes the old format; a mixed-version group (bumped build) serves PUT/GET/repair; the old build reads every segment written while the gate was low | Verified: `mixed_version.rs` (nodes upgraded one by one under traffic keep `format_write 1`; node 0 rolled back to the old binary opens and serves everything; after finalize new segments carry format 2 and the old node binary refuses the device) |
| U-A5 | Finalize: idempotent (re-run is a no-op), crash-safe (killed after each step, resumed by the leader), concurrent invocations serialize; after step 3 the old build refuses to start | Verified: `upgrade.rs` (compare-and-set steps, re-request idempotent, a different target refused, cancel refused past the cut-off, leader failover mid-finalize resumes, audit record) and `mixed_version.rs` steps 4 and 9 (leader killed during a requested finalize on FoundationDB; old gateway, controller and node refuse after it). Killing at every individual step is Not Verified |
| U-A6 | Rollback before step 3 by reinstalling the old build under load loses no acknowledged write | Verified for nodes and controllers: `mixed_version.rs` steps 8 (cancel, old controller starts) and the node rollback before the cut-off, every acknowledged object read back; a gateway rollback is the same binary path (step 1 runs an old gateway against the upgraded controller) |
| U-A7 | Controller handoff under a running repair resumes from the cursor within one election interval | Verified (`repair_completion.rs`, `controller.rs`, `membership.rs` resume after a mid-rebuild crash); the interval bound itself is not asserted |
