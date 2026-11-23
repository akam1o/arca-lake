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
  heartbeats are checked again at step 5, before and after `schema` is
  raised: a 0.1.0 controller or gateway can start between steps 3 and 5,
  because its gate checks `schema`, which is still 4, and it runs on
  after the raise until stopped. Each blocked step records the reason in
  the cursor (`admin upgrade status`) and goes on once the process is
  stopped (its heartbeat removed or gone stale after 90 s). A process
  named with `--ignore-absent` is passed over only while its heartbeat
  has not advanced since the request; one that heartbeats after it
  blocks.
- **After the finalize** the leader keeps flagging, at every health
  pass, each fresh heartbeat that reads a schema below `schema_write`:
  an error log naming it and the gauge
  `arca_controller_procs_below_schema`.

`admin status --versions` prints, for every process and node, the
software string, the schemas it reads or enforces, and `below-schema-5`
when it is 0.1.0 or unknown; a heartbeat record older than the 90 s
window is marked `(stale)` instead (a process gone, which blocks
nothing). `admin status --versions` and `admin upgrade status` both
print `protections degraded` or `protections mandatory`.

### 2.4 Rollback before finalize, refusal after

- **Before step 3** any process can go back to 0.1.0, in any order
  (RFC-014 §5.6): every record is unchanged. The finalize refuses while
  the 0.1.0 process runs. That a 0.2.0 process coming back after a
  0.1.0 period re-checks what 0.1.0 did meanwhile (instead of trusting
  its own 0.2.0-only keys) is provided by the 0.2.0 changes of those
  protections (R-117: ADR-0037; R-119), each in its own ADR; this ADR
  provides only the schema gate they key on.
- **From step 3 on** there is no supported way back to 0.1.0:
  - 0.1.0 controllers and gateways refuse to start once step 5 raised
    `schema`. Between steps 3 and 5 they could start, but step 5 waits
    until they are gone.
  - **Restoring a pre-finalize backup does not bring 0.1.0 back.** Step 5
    gives every node schema floor 5, which survives on its disk. A
    restore (`admin restore`, or `admin recovery begin` after a manual
    one) raises both `schema` and `schema_write` of the restored metadata
    to the highest floor the nodes report (`delete_fence::restore_floors`,
    so that a restore does not reopen a closed downgrade). The restored
    cluster is at schema 5 again and 0.1.0 is refused as before. A real
    way back would need every node's floor lowered (no command does this;
    it would undo the guarantee strict nodes rely on) or the nodes
    replaced with empty disks, i.e. a new cluster. It is not provided.
- **Storage nodes of 0.1.0.** They cannot refuse by themselves: they
  read no metadata and record schema floor 5 without understanding it.
  The 0.2.0 side refuses them:
  - **Leader.** Once `schema_write >= 5`, every health pass first asks a
    node facts-free (`Health` with no fence, floor or incarnation). A
    node that reports a schema below `schema_write` is quarantined and is
    sent nothing: no open delete fence, no schema floor, no incarnation.
    A node replaced between that probe and the push is quarantined when
    it answers the push. Quarantine is the existing state for a node that
    must not serve (ADR-0027). It counts as unhealthy, so with
    `--auto-repair` its members are repaired away. `admin node rejoin`
    refuses such a node until it runs a 0.2.0 binary.
  - **Writers (gateways, and the controller's repair and integrity
    writes).** `NodeClients::set_required_node_schema` is set to
    `schema_write` once it is 5 or later, at start and with every
    cluster-state refresh (gateway: every 2 s; leader: every health
    pass, and at the start of every repair, stripe repair and scrub
    pass, so that a `schema_write` raised mid-term by finalize step 3 or
    by a restore's `restore_floors` applies before the next repair
    write). Before a shard write the node's reported schema is checked,
    from a `Health` answer at most 2 s old and asked again after any
    connection failure. The write's own acknowledgement must also carry
    it (`PutShardResponse.schema_max`, 0 from 0.1.0). A node below
    `schema_write` is a failed write: the chunk is never counted written
    on it and goes to another group. Bytes a 0.1.0 node stored before
    answering are an unreferenced record, collected like any failed
    write's.
- **0.1.0 `admin` commands do not check the gate.** Only `admin init`
  runs the startup gate in 0.1.0. `register-node`, `create-groups`,
  `replace-member`, `recovery`, `restore` and the other commands write
  the metadata whatever its schema, and they know none of the 0.2.0
  keys. After the cut-off, use only the 0.2.0 `arca-lake admin`. No
  0.2.0-side guard covers all of them. Where a protection keeps its
  state in a 0.2.0 key, its own change can make the 0.2.0 leader refuse
  to act on a record without it. For example, R-119 can refuse to place
  in or repair a group with no recorded failure domain. That is up to
  those changes.

## 3. Consequences

- **Compatibility impact.**
  - Metadata schema 4 → 5, readable range `[4, 5]`.
  - No change to the metadata record version byte (2), the wire
    protocol version (4, accepts 3), the segment format (2) or the
    storage record version (1).
  - Two additive protobuf fields: `HealthResponse.schema_max = 50`
    (chosen above R-117's 47–49) and `PutShardResponse.schema_max = 5`.
    prost ignores unknown fields, so a 0.1.0 reader drops them, and a
    0.1.0 sender leaves them 0.
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
- **Cost.** One extra facts-free `Health` per node per leader health
  pass, and at most one per node every 2 s per writer, once schema 5 is
  written.
- **Risk.** Quarantining every node of a cluster wrongly rolled back
  after the finalize stops writes until the nodes are upgraded and
  rejoined. This is accepted: serving writes from nodes that do not
  enforce the mandatory protections is what the finalize promised not
  to do.
