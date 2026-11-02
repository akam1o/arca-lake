# RFC-014: Rolling Upgrade and Compatibility

- Status: draft (Phase 3.0 design; implementation Phase 3.3)
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

### 5.1 Version vocabulary

- **Software version**: the crate version (`arca-lake --version`),
  semantic.
- **Protocol version** `P`: the gRPC message/semantics version
  (`arca_lake.storage.v1`, `PROTOCOL_VERSION`).
- **Segment format version** `F`: `SEGMENT_FORMAT_VERSION` and
  `RECORD_VERSION` (RFC-005).
- **Layout version** `L`: the metadata key layout and record versions
  taken together (`("al","meta","layout")`).
- **Compatibility window**: a release supports `P ∈ {P_cur−1, P_cur}`,
  reads `F ≤ F_cur`, reads `L ∈ {L_cur−1, L_cur}` and writes `L_cur`
  records only after finalization. A release that would need more than
  that is not a rolling upgrade and the release notes say so.

### 5.2 Protocol versioning

- Every request already carries `protocol_version`. The check becomes:
  accept `v` if `P_min ≤ v ≤ P_cur` (window of two). The `Health`
  response gains `protocol_min` and `protocol_max`; clients (gateway,
  controller) call `Health` on first connection to a node and cache the
  node's window, then send `min(P_cur, node.protocol_max)`.
- Additive changes (new optional fields, new RPCs) do not bump `P`;
  a client must tolerate `Unimplemented` for a new RPC on an old node
  (the scrub RPC in Phase 3.2 is such a case: the controller skips nodes
  that return `Unimplemented` and counts them).
- Semantic changes (field meaning, required new behaviour) bump `P`; the
  node implements both behaviours for the window.
- Group epochs and fencing are unchanged by version: a node at N+1 and a
  node at N in the same group agree on epochs because epochs are data,
  not protocol.

### 5.3 On-disk formats

- Reading: every build keeps decoders for all `F` since 1
  (`format_compat.rs` golden files, one per version; the test asserts
  every golden file still opens and verifies).
- Writing: a node writes `F_cur` only if `cluster.min_software_version`
  (§5.4) is at least the version that introduced `F_cur`; otherwise it
  writes `F_cur−1`. Mixed segments within one directory are allowed
  (the segment header carries its version). Downgrade is possible until
  a segment with the new format exists; `arca-lake storage-node
  --check-downgrade <version>` scans headers and says so.
- Record format changes follow the same rule within a segment; the
  record header's version byte selects the decoder.

### 5.4 Metadata layout version and migrations

- Initialization (`admin init`, new; `create-groups` on an empty cluster
  calls it) writes `("al","meta","layout") = L_cur` and
  `("al","meta","min_software") = <version>`.
- Startup check (every process, read-only): `L` must be in the build's
  window, else refuse to start with the message `layout L_x requires
  software ≥ … (this build: …)`. A build older than the layout never
  starts; a build newer than `L+1` never starts.
- Record versions: every record carries its version byte (exists);
  decoders for `v` and `v−1` exist in a release; **writers write the
  record version allowed by `min_software`** (lazy upgrade of records
  on rewrite is not done: a record is rewritten only when its operation
  rewrites it, and then in the allowed version). This keeps a v1 reader
  working until finalization.
- **Finalization** (`admin upgrade finalize`): the operator runs it when
  every process reports the new software version (`admin status` shows
  versions from the node health RPC and the controller/gateway
  heartbeat records, Phase 3.3). It bumps `min_software` and, if the
  release has a migration, runs it: migrations are idempotent, resumable
  (cursor in `("al","meta","migration",name)`), rate limited, executed by
  the controller leader (fenced), and bump `L` only at the end. Example
  (Phase 3.1): bucket records v1 → v2 (`owner = "legacy"`) is a lazy
  decoder, not a rewrite, so the migration only bumps `L`.
- **Downgrade policy**: before finalization, any process may be downgraded
  to N (it reads `L` and the records it finds, all ≤ its window). After
  finalization, downgrade is refused by the startup check; recovery is
  "fix forward" or restore (RFC-015) with the data-loss consequences
  stated there.

### 5.5 Upgrade order and procedure

1. **Read the release notes**: window, migration, estimated migration
   time, downgrade cut-off.
2. **Storage nodes first**, one failure domain at a time: stop the node
   (SIGTERM; in-flight streams finish or fail with retryable errors that
   the gateway's RPC policy handles, ADR-0011), replace the binary,
   start, wait for `Health` and `last_seen`, check
   `arca_gateway_node_rpc_exhausted_total` flat. The controller's health
   pass tolerates `--unhealthy-after-secs` of absence before repair would
   start; keep each restart under that.
3. **Controllers**: followers first, leader last (SIGTERM releases the
   lease; the new leader, at N+1, resumes repairs via cursors).
4. **Gateways**: drain from the load balancer, restart, return.
5. **Verify**: `admin status --versions` shows one version everywhere;
   the conformance smoke test passes.
6. **Finalize** (§5.4) once the operator is satisfied; take a backup
   right before (RFC-015), since finalization is the downgrade cut-off.

Why this order: nodes must understand the newest protocol before any
client speaks it (clients negotiate down, but a node cannot negotiate
up); the controller must be newest before a migration runs; gateways
last so that the data path is served by the most tested combination
(new gateway → new nodes) only after nodes are done.

### 5.6 Upgrade failure recovery

| Failure | Recovery |
| --- | --- |
| New node binary crashes on start | start the old binary (format not finalized → readable); the group is unaffected (one member) |
| New node starts but misbehaves (errors, corruption reports) | downgrade that node; `admin replace-member` if its data is suspect (repair rewrites it) |
| Controller at N+1 fails migration midway | migration cursor; fix forward with a patched N+1; `L` not yet bumped, so N controllers can still lead (they ignore the partial migration keys, which are additive) |
| Finalized, then a defect is found | no downgrade; hot-fix release N+1.1 with the same window; restore is the last resort |
| Gateway at N+1 returns new error shapes clients reject | drain it; downgrade; file the compatibility bug |

### 5.7 Mixed-version operation rules

- A group may contain nodes at N and N+1 indefinitely (reads and writes
  use the negotiated protocol per node; formats per node).
- Repair reads from any member and writes to the target in the target's
  format.
- A gateway at N with nodes at N+1: works (window). A gateway at N+1
  with a node at N: works (negotiates down); features that need the new
  protocol on that node are refused with a clear error (e.g. scrub on
  that node is skipped and counted).

## 6. Alternatives considered

| Decision | Chosen | Alternatives |
| --- | --- | --- |
| Compatibility window | adjacent versions (N, N+1) | any-to-any (unbounded test matrix); none (stop-the-world upgrades, which is today) |
| Format write gate | cluster `min_software` set by finalization | per-node auto-detection (a node cannot see every peer); time-based (unsafe) |
| Record migration | lazy decoders, rewrite-on-write, explicit finalize | eager rewrite at upgrade (long, blocks downgrade immediately); dual-write (complex) |
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

## 9. Performance considerations

Negligible on the data path. Migrations are rate-limited background
work; the release notes carry a measured duration per million objects.

## 10. Open questions

- Should `min_software` also gate protocol version advertisement, so
  that no client *speaks* P_cur before finalization? (Proposal: no;
  protocol is negotiated per peer and both are tested; the gate is for
  persistent formats.)
- Heartbeat records for gateways and controllers (`("al","proc",id)`)
  so that `admin status --versions` can list them: adds a small write
  per minute per process (proposal: yes).

## 11. Acceptance criteria

- A test starts a cluster with nodes at the previous release's binary
  (built from the previous tag in CI) and the current one in one group,
  runs PUT/GET/repair across them, upgrades the old nodes one at a time
  under load, and verifies every object; then finalizes and verifies
  that the old binary refuses to start.
- Golden files for every segment, record and layout version pass the
  compatibility tests; a format bump without a golden file fails CI.
- A controller handoff under a running repair resumes from the cursor
  within one election interval (exists: `repair_completion.rs`,
  `controller.rs`; extended to assert the interval).
- The downgrade check command reports correctly before and after a
  format bump.
