# ADR-0039: Metadata schema 5 and the upgrade from 0.1.0

Status: **accepted** (0.2.0). Concerns the metadata schema
(`crates/metadata/src/keys.rs`), the upgrade finalize (RFC-014 §5.5) and
its preconditions, the controller's treatment of storage nodes of an
earlier release, and the mixed-version tests (RFC-014 §5.7). Record
layouts, the record version byte, the wire protocol version and the
segment format are not changed.

## 1. Problem

0.1.0 (tag `v0.1.0`, commit `5601018`) is published, so a 0.2.0
cluster is either new or a 0.1.0 cluster upgraded in place, with 0.1.0
and 0.2.0 processes side by side during the rolling upgrade (RFC-014
§5.8).

0.2.0 adds protections that a 0.1.0 process does not know about and
therefore bypasses without noticing:

- R-117, strict write ownership: a storage node accepts a shard write
  only from a writer that proves it owns the slot. A 0.1.0 gateway sends
  no proof, and a 0.1.0 node checks none.
- R-119, the failure domain recorded on each group: a 0.1.0 controller
  places and repairs by its own `--failure-domain` flag, not by what the
  group records.

While 0.1.0 processes can run, these protections can only be best
effort. They become guarantees only from a point after which no 0.1.0
process runs and none can start. The only mechanism that keeps an
earlier controller or gateway from starting is the startup gate on the
stored schema (RFC-014 §5.4, `compat::check_startup`): a process
refuses a store whose `schema` is outside its `[schema_min, schema_max]`.
0.1.0 reads schemas 3 and 4, so storing schema 4 can never exclude
it. A schema step is needed.

## 2. Decision

**Schema 5 is the schema of 0.2.0** (`keys::SCHEMA_V5 = 5`,
`SCHEMA_VERSION = 5`, `SCHEMA_VERSION_MIN = 4`).

### 2.1 What 5 means

- **No record layout change.** Records are encoded the same way at
  schema 4 and 5. The record version byte stays 2
  (`records::record_version_of_schema(5) == 2`), so records written on
  either side of the finalize read back on both sides.
- **It gates the 0.2.0 protections.** At `schema_write >= 5`
  (`Gate::schema_v5_enforced`) the protections 0.1.0 would bypass are
  mandatory. Below it the cluster is in the mixed window with 0.1.0 and
  they are *degraded*: 0.2.0 enforces what it can, but cannot rely on a
  0.1.0 process doing so. Each protection's own ADR says what that
  means for it. R-117 keys it to the node's schema floor through
  `pb::WRITE_OWNERSHIP_SCHEMA`, which must equal `keys::SCHEMA_V5`
  (assert it with a test once both are defined).
- **New 0.2.0 metadata goes into separate keys.** 0.1.0 never reads
  those keys and so never misreads them; it does not maintain them
  either. Until the finalize, 0.2.0 treats them as possibly stale.
- **A version-bump build** (ADR-0016) reads `[5, 6]`; `SCHEMA_V5` stays
  5 there.

### 2.2 Read and write rules

| Store | 0.1.0 (`[3, 4]`) | 0.2.0 (`[4, 5]`) |
| --- | --- | --- |
| schema 3 (development commits before 0.1.0) | starts | refuses: upgrade through 0.1.0 first |
| schema 4 (a 0.1.0 store) | starts | starts; protections degraded |
| schema 5 (finalized, or new under 0.2.0) | refuses | starts; protections mandatory |

- A new store initialises at `schema = schema_write = 5` (`compat::init`
  writes the build's `schema_max`), so its protections are mandatory
  from the start.
- A store upgraded from 0.1.0 stays at schema 4 until
  `admin upgrade finalize` raises `schema_write` (step 3, the cut-off)
  and then `schema` (step 5) to 5.

### 2.3 Finalize preconditions

A finalize to schema 5 or later is refused while any 0.1.0 process, or
any process of unknown version, takes part in the cluster:

- **Controllers and gateways** heartbeat their capabilities
  (`("al","proc",id)`). 0.1.0 reports `schema_max = 4`, which is below
  the target, so it blocks. This check already existed. It becomes
  effective for 0.1.0 because the target is now 5.
- **Storage nodes** did not report a schema. `HealthResponse` gains
  `schema_max = 50`, the newest schema whose protections the node
  enforces (`pb::NODE_SCHEMA_MAX`, equal to `keys::SCHEMA_VERSION`). A
  0.1.0 node does not send it, so it reads as 0: "0.1.0 or earlier, or
  an unknown release". From target schema 5 on, a node must report at
  least the target (`upgrade::node_below_schema`); the earlier
  finalizes do not ask for it. The software string is shown to
  operators but not trusted: development builds of 0.2.0 still report
  `0.1.0` until the version is bumped.
- **When it is checked.** At step 1 as before, and again at step 3
  before `schema_write` is raised: a 0.1.0 process started since step 1
  (a rollback within the window) holds the cut-off back. Process
  heartbeats are checked again at step 5: a 0.1.0 controller or gateway
  can start between steps 3 and 5, because its gate checks `schema`,
  which is still 4. Each blocked step records the reason in the cursor
  (`admin upgrade status`) and goes on once the process is stopped (its
  heartbeat removed, gone stale after 90 s, or named with
  `--ignore-absent`).

`admin status --versions` prints, for every process and node, the
software string, the schemas it reads or enforces, and `below-schema-5`
when it is 0.1.0 or unknown. `admin status --versions` and
`admin upgrade status` both print `protections degraded` or
`protections mandatory`.

### 2.4 Rollback before finalize, refusal after

- **Before step 3** any process can go back to 0.1.0, in any order
  (RFC-014 §5.6): every record is unchanged. A 0.2.0 process that comes
  back after a 0.1.0 period re-checks what 0.1.0 may have done in the
  meantime rather than trusting its own 0.2.0-only keys. This is
  defined per protection (R-117, R-119). The finalize refuses while the
  0.1.0 process runs.
- **After step 3** (the cut-off) there is no supported way back other
  than restoring a pre-finalize backup (RFC-015):
  - 0.1.0 controllers and gateways refuse to start once step 5 raised
    `schema`. Between steps 3 and 5 they could start, but step 5 waits
    until they are gone.
  - 0.1.0 storage nodes cannot refuse by themselves: they read no
    metadata and record schema floor 5 without understanding it. So the
    0.2.0 leader quarantines any node that reports a schema below
    `schema_write` once `schema_write >= 5`, with the reason in the
    quarantine record. Quarantine is the existing state for a node that
    must not serve (ADR-0027): the leader sends it no pushes, so it holds
    no open delete fence and takes no delete. It counts as unhealthy, so
    with `--auto-repair` its members are repaired away. `admin node
    rejoin` refuses such a node until it runs a 0.2.0 binary.
  - **Residual.** Gateways keep the capabilities they negotiated per
    node address, and this ADR does not change their write path. A 0.1.0
    binary restarted at a member's address after the finalize can
    therefore still receive shard writes from a gateway until its members
    are moved. Refusing unproven writes is R-117's job, done by 0.2.0
    nodes, and a 0.1.0 node does not do it. Closing the gap needs
    gateways to re-check a node's reported schema against `schema_write`
    on every reconnect.

## 3. Consequences

- **Compatibility impact.**
  - Metadata schema 4 → 5, readable range `[4, 5]`.
  - No change to the metadata record version byte (2), the wire
    protocol version (4, accepts 3), the segment format (2) or the
    storage record version (1).
  - One additive protobuf field, `HealthResponse.schema_max = 50`. prost
    ignores unknown fields, so a 0.1.0 reader drops it, and a 0.1.0
    sender leaves it 0. It is chosen above R-117's 47–49.
  - `NodeCapRecord` already stores a full `Capabilities`; its schema
    fields are now filled, with no layout change.
- **Schema-3 stores** (pre-0.1.0 development commits) are no longer
  served by 0.2.0; 0.1.0 is the hop. The pinned-commit upgrade tests of
  `previous_release.rs` exercised hops that 0.2.0 does not serve
  (schemas 2 → 3 → 4 between development commits and 0.1.0's own
  tree). They are redundant now and are retired from CI. The tag
  `v0.1.0` they led to is the old side instead.
- **RFC-014 §5.7** ("once real tags exist, the previous tag replaces
  the bumped build") is satisfied by a real tag. CI downloads the
  published v0.1.0 archive, pins its SHA-256, checks that `BUILD-INFO`
  names commit `5601018` and caches it by digest, then runs
  `upgrade_v010`: a 0.1.0 store and 0.1.0 processes, then 0.2.0 storage
  nodes, controllers (followers first) and gateways, rollback before
  finalize, the finalize refused while 0.1.0 runs, SIGKILL-interrupted
  steps resumed, the finalize, and 0.1.0 refused after it. The
  version-bump pair stays for the N → N+1 mechanics of the next schema.
- **Risk.** Quarantining every node of a cluster wrongly rolled back
  after the finalize stops writes until the nodes are upgraded and
  rejoined. This is accepted: serving writes from nodes that do not
  enforce the mandatory protections is what the finalize promised not
  to do.
