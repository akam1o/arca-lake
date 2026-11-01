# RFC-001: Architecture Overview

- Status: draft
- Authors: arca-lake maintainers
- Created: 2026-11-01
- Depends on: —
- Related ADRs: ADR-0004 (platforms), ADR-0005 (initial architecture decisions)

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

arca-lake must provide S3-compatible object storage that is strongly
consistent, erasure coded, natively multi-tiered, horizontally scalable, and
operable by a small team, without hard-coding any property of a particular
deployment. The components, their boundaries, and the data and control
flows between them have to be fixed before the detailed RFCs can be written,
because every later design (data model, metadata transactions, storage
engine, tiering, recovery) depends on where state lives and who is allowed
to change it.

## 2. Goals

- Separate the control plane, the metadata plane, and the data plane so that
  each can fail, scale, and be upgraded independently.
- Keep every component that handles requests stateless, so capacity is added
  by adding processes.
- Keep the controller off the data path: reads and writes continue when it is
  down.
- Make durability and consistency properties of the whole system derivable
  from a small number of explicit protocols with named linearization points.
- Make every environment-specific parameter configuration, not code.

## 3. Non-goals

- File or block semantics.
- Multi-site replication (Phase 3+).
- Choosing a specific metadata store or erasure-coding backend in this RFC;
  those are RFC-003 and RFC-004 with their own evidence.
- Performance targets. RFC-009 defines how performance is measured; no
  numbers are promised here.

## 4. Requirements

| ID | Requirement | Source |
| --- | --- | --- |
| R1 | Broad Amazon S3 API compatibility, implemented incrementally and tracked in a matrix | project brief (*Verified*: public requirement) |
| R2 | Strong consistency for supported S3 operations | project brief |
| R3 | Configurable erasure coding with tier-specific profiles and failure-domain awareness | project brief |
| R4 | Arbitrary, configuration-defined storage tiers including a read-through cache tier | project brief |
| R5 | Horizontal scaling of throughput by adding storage nodes and gateways | project brief |
| R6 | No acknowledgment before the configured durability condition is met | project brief; DD-4 |
| R7 | Explicit failure in preference to silent loss or corruption | project brief; DD-8 |
| R8 | Versioned on-disk formats and internal protocols | project brief |
| R9 | Rolling upgrades and online capacity expansion | project brief |
| R10 | Nothing about scale, topology, tier names, or workload is hard-coded | project brief |

*Assumption* A1: the metadata store provides strictly serializable multi-key
transactions (RFC-003 names the store and its limits).

*Assumption* A2: storage nodes are reachable from gateways over a network
that may partition; no component may assume a partition cannot happen.

## 5. Proposed design

### 5.1 Planes and processes

```text
                 ┌────────────────────────────────────────────────────┐
   S3 clients ──▶│  Gateway (stateless, N instances)                  │
                 │  SigV4 · S3 REST · streaming · EC encode/decode    │
                 └──────┬───────────────────────────┬─────────────────┘
                        │ metadata transactions     │ shard I/O
                        ▼                           ▼
        ┌──────────────────────────┐   ┌──────────────────────────────┐
        │ Metadata plane           │   │ Data plane                   │
        │ metadata layer (library) │   │ Storage nodes (M instances)  │
        │ over a distributed       │   │  local storage engine        │
        │ transactional KV store   │   │  append-only segments        │
        └──────────▲───────────────┘   │  checksums · scrub · repair  │
                   │                   └──────────────▲───────────────┘
                   │ leases, cluster map, work queues │ repair / migrate / GC
        ┌──────────┴──────────────────────────────────┴───────────────┐
        │ Control plane: Controller (leader-elected, 1 active)        │
        │  placement · tiering · repair · scrubbing · GC · admin API  │
        └─────────────────────────────────────────────────────────────┘
```

Four process types are operated: gateway, storage node, controller, and
the metadata store. All arca-lake processes are one binary with
subcommands (`arca-lake gateway | storage-node | controller`).

### 5.2 Gateway

Stateless. Terminates HTTP(S), authenticates with AWS Signature Version 4,
authorizes, parses S3 operations, and executes the read and write protocols
(RFC-003 §5.3, §5.4). The gateway splits object data into chunks, erasure
codes each chunk into `k + m` shards (RFC-004), writes shards directly to
storage nodes, and commits metadata. On reads it fetches the manifest,
reads the minimum shard set, decodes if necessary, and streams the object.

State held: in-flight requests, a cache of the cluster map (stripe groups,
topology, epochs), and bucket configuration. Every gateway can serve every
request. A gateway crash aborts in-flight requests only; the pending
generation left behind is reclaimed by the lease mechanism (RFC-003).

The S3 protocol surface is provided by the `s3s` crate behind the gateway's
own `S3` implementation (OD-06; spike results in `docs/architecture/open-decisions.md`).
Erasure coding happens in the gateway (DD-9, ADR-0005).

### 5.3 Metadata plane

A library (`arca-lake-metadata`) embedded in gateways and the controller
(DD-10, ADR-0005). It defines a transactional key-value abstraction with
FoundationDB-like semantics and limits, the key layout, the record types,
and the transaction recipes that implement every protocol in this document.
The backing store is FoundationDB, pending final measurements (OD-05). The
layer owns: buckets, object keys, versions, generations, chunk manifests and
placements, multipart uploads, stripe groups, the cluster map, placement
epochs, leases, GC queues, and migration state.

The metadata store is the only consensus component in the system; arca-lake
implements no consensus of its own.

### 5.4 Storage node

Owns local devices. Each device runs an independent instance of the storage
engine (`arca-lake-storage`, RFC-005): append-only, checksummed, immutable
records in large segments, an in-memory index rebuilt from the segments on
open, group-committed `fdatasync` before acknowledgment, compaction, and
scrubbing. The node exposes a versioned RPC API (RFC-004 §5.6, measured in
Phase 0 task 0.4) for shard put, ranged get, delete, list-by-group, and
scrub reporting, and enforces placement epochs.

The storage node knows nothing about objects or S3. It stores shards
addressed by `(stripe group, chunk ID, shard index)`.

### 5.5 Controller

One leader (lease in the metadata store), hot standbys. Pure, testable
logic lives in separate crates and is driven by the controller process:

- **Placement** (`arca-lake-placement`): topology model, stripe-group
  creation and validation, capacity balancing, node add/remove, epoch
  management, rebalancing plans (RFC-004).
- **Tiering** (`arca-lake-tiering`): policy evaluation, promotion and
  demotion, cache admission and eviction, migration state machines
  (RFC-006).
- **Repair and scrub orchestration**, **garbage collection** (RFC-008).
- **Admin API**: placement inspection, prefetch, promotion, demotion, pinning,
  migration status, policy management. Separate from the S3 API.

The controller is never on the data path. While it is unavailable, reads and
writes use the last published cluster map; repair, rebalancing, tiering, and
GC pause.

### 5.6 Core flows

**Write.** Allocate a pending generation (one small transaction) → stream
the body, chunk it, encode each chunk, write shards to a stripe group with
the group epoch, each storage node acknowledging only after `fdatasync` →
record manifest and placement entries under the pending generation as
chunks become protected → commit transaction: verify pending and leased,
evaluate conditional headers, publish the version, retire the displaced
generation. The commit is the linearization point (RFC-003 §5.3). Return
success only after commit.

**Read.** One metadata range read fetches pointer, header, and the chunk
records covering the requested range → per chunk prefer a valid cache copy,
then data shards, then decode from any `k` shards → verify checksums before
use → stream.

**Repair.** Controller detects an unhealthy group member, bumps the group
epoch with a replacement member, and reconstructs every shard of that member
from the remaining ones; chunk records do not change (RFC-004, RFC-008).

**Migration.** Copy → verify → switch (one transaction) → retire source after
a grace period; identity never changes (RFC-006).

**GC.** Unreferenced generations enter a retired queue; after a grace period
their shards are deleted and then their metadata (RFC-008).

### 5.7 Configuration

Everything deployment specific is configuration with a schema: tiers, EC
profiles, chunk sizes, topology levels, failure-domain rules, grace periods,
leases, repair rates, listen addresses, credentials sources. The code has no
defaults that encode a particular environment; prototype defaults exist only
for experiments and are labelled as such.

## 6. Alternatives considered

| Alternative | Why not |
| --- | --- |
| Controller on the write path (gateway asks where to place every chunk) | Controller becomes a latency and availability bottleneck; stripe groups published in the cluster map give the same placement quality without the hop (DD-3). |
| Client-computed hash placement without a controller | Cannot express capacity-aware, tier-aware, operator-policy placement (DD-14). |
| Storage-node-side erasure coding | Extra hop and `(k+m)/k` egress on a storage node; gateway CPU is the resource that scales statelessly (DD-9). |
| Separate metadata service process | Extra hop and another component; the library form behind a trait keeps the option open (DD-10). |
| Own consensus / own transactional store | No technical reason outweighs the correctness risk; mature stores exist (DD-5). |
| Object bytes in an embedded KV database | LSM write amplification on large values; rejected by requirement and DD-2. |

## 7. Failure scenarios

| Failure | Behaviour |
| --- | --- |
| Gateway crash | In-flight requests fail; pending generations expire by lease and are reclaimed; no partial objects become visible. |
| Storage node or disk failure | Reads degrade to decoding from `k` shards; writes avoid groups that cannot meet `k + m_ack`; controller repairs; placement epoch fences stale writers. |
| Metadata store unavailable | All S3 operations fail fast with retryable errors; nothing is served from stale caches as authoritative. |
| Controller down | Data path unaffected; background work pauses; leases prevent two leaders. |
| Network partition | Storage nodes on the minority side cannot be written to by gateways that cannot reach them; epochs prevent stale writes after membership changes; the metadata store's own quorum rules apply. |
| Rack failure | Failure-domain-aware groups keep every chunk decodable within the profile's tolerance. |
| Power loss | Storage engine acknowledges only after `fdatasync`; recovery truncates torn tails and rebuilds the index (RFC-005; verified by truncation and SIGKILL tests in Phase 0). |
| Interrupted migration or repair | Resumable from recorded state; sources retained until verified (RFC-006, RFC-008). |
| Silent corruption | Per-block checksums on shards, per-chunk checksums in manifests, ETag end to end; corrupt shards count as missing and trigger repair. |
| Rolling upgrade failure | Versioned formats and protocols; a node refuses to write a format newer than the cluster minimum. |

## 8. Consistency and durability guarantees

- A successful PUT response implies the object's data met the profile's
  durability condition on storage nodes and the version is committed in the
  metadata store. Subsequent reads from any gateway observe it.
- Concurrent operations on a key are linearized by the metadata commit.
- Cache copies never count toward durability (RFC-006).
- Not guaranteed: read-after-write for operations outside the supported set;
  anything during a metadata store outage.

## 9. Performance considerations

*Hypotheses* to be settled by RFC-009 measurements: gateway CPU (TLS, hashing,
EC) is the first bottleneck before storage; aggregate throughput scales with
gateway and storage node counts until the network saturates; the metadata
store's documented testing envelope bounds a single cluster. Phase 0 results
so far are in `benchmarks/reports/` and are relative measurements on a
developer laptop.

## 10. Open questions

- OD-05 (metadata store final choice) and OD-06 (`s3s` adoption) await
  measurements; both are currently recommended as proposed.
- OD-16: how grace periods are measured without relying on synchronized clocks.
- OD-20: metadata partitioning for very large deployments.
- Whether the controller's admin API should be gRPC or HTTP.

## 11. Acceptance criteria

- Every component in §5 exists as a crate with the stated dependency
  direction (`docs/architecture/repository-layout.md`).
- The write and read flows in §5.6 are implemented against the real metadata
  store and real storage nodes in Phase 1 and pass the concurrency tests
  defined in RFC-003 and the crash tests defined in RFC-005.
- The controller can be stopped during a test workload without failing any
  read or write.
