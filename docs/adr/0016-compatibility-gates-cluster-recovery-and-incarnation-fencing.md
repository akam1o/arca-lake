# ADR-0016: Compatibility gates, cluster recovery state and incarnation fencing

Status: accepted (Phase 3.3)

## Context

RFC-014 (three compatibility axes, write gates, finalize) and RFC-015
(cluster recovery state machine, incarnation, epoch catch-up, restore
verification) were designed in Phase 3.0/3.1 and left Designed. Phase 3.3
implements them. The survey before implementation found: version
constants exist but nothing negotiates or gates on them; the schema and
cluster-state keys exist with no record types or operations; gateway
transactions check nothing cluster-wide; nodes persist only group
epochs. The decisions below fix how the RFC text is realised and amend
it where the implementation differs.

## Decisions

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| Test-only version bump | a cargo feature `version-bump` on `arca-lake-common` (`compat::BUMP = 1`) that every versioned crate adds to its constants: `P = 1 + BUMP`, `M = 2 + BUMP`, `F = 1 + BUMP`; `arca-lake --features version-bump` is a real second binary with the additive test-only `Health` field, record version byte and segment header version | `ARCA_LAKE_TEST_VERSION_BUMP` build environment variable (RFC-014 §5.7: invisible in `Cargo.lock`/`cargo build` lines, easy to leak into a release build); changing constants at runtime (not a different binary, which §5.7 forbids) |
| Where capabilities live | nodes report `(P, M, F)` ranges, software version, their persisted incarnation and per-group epochs in `Health`; gateways and controllers write `("al","proc",id)` heartbeat records every 30 s (RFC-014 §10 proposal adopted) | nodes writing metadata (they have no cluster file by design, RFC-012 S-A10) |
| How a node learns `format_write` and the incarnation | the controller sends both in every `Health` request with the cluster state's `changed_ms`; the node persists `cluster.txt` (`incarnation changed_ms format_write`) and adopts a value only when `changed_ms` is not older than what it holds; a node that never heard a value writes `F_cur − 1` (clamped to the oldest format it can write) | a node-side metadata client; an operator-set flag per node (drifts) |
| Protocol negotiation | a node accepts `[P_cur − 1, P_cur]`; a client sends its own `P` and, on the node's version rejection, calls `Health`, caches `min(P_client, node.protocol_max)` for that address and retries once; disjoint windows are a permanent `FailedPrecondition` counted in `node_incompatible_total` | `Health` before every first request (an extra round trip on every connection; the rejection path costs one round trip only for the mixed-version case) |
| Where the cluster state is checked | a `KvStore` wrapper (`GuardedKv`, like `FencedKv`) that reads the cluster-state record inside the commit of every transaction that wrote something and fails with `KvError::ClusterState` unless the incarnation equals the process's and the state admits the wrapper's operation class; read-only transactions pay nothing; gateways additionally refuse at admission from a copy refreshed every 2 s, controllers skip whole passes the state does not admit | a read in every recipe (dozens of call sites, easy to miss one); admission checks only (a stale process could still commit) |
| Operation classes | `ClientWrite` (Active), `Delete` (Active: GC finish, reconcile, eviction, tiering switch, multipart sweep), `Repair` (Active, ReadOnly, RecoveryRequired), `Verification` (VerificationPending, RecoveryRequired, ReadOnly), `Admin` (any) | one class for the controller (repair must run while deletes are forbidden) |
| Incarnation on nodes | `DeleteShard` carries the caller's incarnation; a node that holds one rejects a different value (`FailedPrecondition "incarnation rejected"`); `PutShard` is not gated by incarnation (repair and client writes under a stale incarnation are blocked by their metadata transaction, and a shard written by a stale writer is an orphan reconciliation removes) | gating every RPC (reads during `Restoring` are blocked by the gateway) |
| Recovery driver | the controller leader runs the recovery state machine (`Restoring` → epoch catch-up → `VerificationPending` → verification with a persisted cursor → `ReadOnly` or `RecoveryRequired`) under its fencing token; `admin recovery begin` writes the new incarnation and `Restoring`, `verify`/`resume` reset or continue the cursor, `finalize` accepts the report and activates, `status` prints everything | running verification from the admin process (not fenced, lost on Ctrl-C, no failover) |
| Finalize driver | `admin upgrade finalize` writes a request into the upgrade cursor; the controller leader executes the steps (each a compare-and-set on `step`) under its fencing token and finishes an unfinished cursor on every election; `--wait` polls | admin executing the steps itself (no fencing against a concurrent leader migration, no resume on crash) |
| Legacy stores | a store with data and no schema key is stamped `schema = schema_write = 2`, `format_write = 1`, `Active` with a fresh incarnation on first contact by a controller or `admin init`; an empty store is initialised with the current versions | treating no key as `M = 1` (RFC-014 §5.4 text; the Phase 3.1/3.2 stores wrote version-2 records, so `M = 1` would be false) |
| Records written below the current schema | the version byte is `min(M_cur, schema_write)`; the postcard body is identical for 2 and 3, so no down-encoder is needed for the test bump; the lowest writable version is a constant `RECORD_WRITE_MIN = 2` and a future layout change needs an explicit down-encoder before it may ship | always writing `M_cur` (an upgraded gateway would write records the not-yet-upgraded ones cannot read) |

## Consequences

- RFC-014 §5.1/§5.3/§5.4/§5.7 and RFC-015 §5.3/§5.4/§5.6 are amended
  as above; acceptance labels move as the tests land.
- New persisted state: `("al","meta","schema_write")`, `("al","meta","format_write")`,
  `("al","meta","cluster")` (record), `("al","proc",id)`, `("al","meta","upgrade")`,
  `("al","meta","restore","cursor")`, `("al","meta","restore","report",incarnation)`;
  on nodes `cluster.txt`.
- `Health` grows additive fields; `DeleteShardRequest` gains `incarnation`.
- Every process refuses to start on a schema outside its window; nodes
  refuse to open segments newer than they understand (unchanged).
