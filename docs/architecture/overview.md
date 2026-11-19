# Architecture overview

arca-lake is an experimental S3-compatible object store. This page describes
the current implementation; it is not a statement of production readiness.
See [S3 compatibility](../guide/s3-compatibility.md) for the implemented API
surface and [Known limitations](../guide/known-limitations.md) for the scope
of verification.

## Components

| Component | Responsibility |
| --- | --- |
| S3 gateway (`arca-lake gateway`) | Authenticates S3 requests, applies configured authorization, streams object data, encodes and decodes shards, and commits object metadata. The S3 listener is HTTP; a deployment provides a TLS terminator. |
| Metadata library (`arca-lake-metadata`) | Runs transactional metadata operations on FoundationDB. Gateways, controllers and administrative commands use the library directly; there is no separate metadata service. An in-memory backend is available for tests and the single-process demonstration. |
| Storage node (`arca-lake storage-node`) | Serves shard RPCs backed by a local append-only segment store. It enforces write durability, checksums, capacity reserves, fencing and conditional deletion. Each CLI process opens one store directory. |
| Controller (`arca-lake controller`) | Uses FoundationDB leases to elect one active leader. Runs placement, health and capacity admission, garbage collection, reconciliation, repair, scrubbing and tiering. Its HTTP endpoint serves health and metrics. |
| Administrative CLI (`arca-lake admin`) | Inspects and changes cluster metadata and invokes node operations for tasks such as registration, recovery and restore. Administrative operations are separate from the S3 API. |

Gateways transfer shards directly to and from storage nodes. The controller
does not forward object data. FoundationDB stores metadata, not object bytes.

Gateways have no authoritative local object state: another gateway can serve
a request using the shared metadata and storage nodes. Local caches, work
queues and pending access records can be lost on a gateway crash. Access
records inform tiering and do not carry durability.

The crate boundaries are described in
[Repository layout](repository-layout.md).

## Object and placement model

| Concept | Meaning |
| --- | --- |
| Bucket and object key | The S3 namespace. Bucket configuration includes versioning and the supported policy settings. |
| Object version | The externally visible S3 version. It points to an internal generation; a delete marker is a version without object data. |
| Generation | One immutable object body and its metadata. An overwrite creates a new generation. |
| Chunk and manifest | A generation is described by an ordered manifest of chunks. Chunks are the units of placement, erasure coding and movement. |
| Stripe group | An ordered set of storage members for a tier and coding profile, validated against configured failure domains. A membership epoch fences stale operations. |
| Placement record | Names a chunk's durable location and any discardable cache locations. Moving data changes placement without changing the object's version identity. |
| Tier | An operator-defined storage class and policy. Durable tiers carry the configured durability requirement; cache copies never count toward it. |

Profiles select erasure coding or replication, chunk sizing and failure-domain
rules. Topology labels are configuration: assigning different labels to
processes on one host does not make them independent failure domains.
Details: [RFC-002](../rfcs/RFC-002-object-and-chunk-data-model.md),
[RFC-004](../rfcs/RFC-004-erasure-coding-and-placement.md) and
[RFC-006](../rfcs/RFC-006-multi-tier-storage.md).

## Write path

1. The gateway creates a pending generation with a lease.
2. It streams the request body into chunks, computes checksums, selects
   eligible groups, encodes shards and writes them directly to the nodes.
   Node acknowledgements follow durable storage; gateway and node capacity
   admission can refuse new data before the write completes.
3. Once the profile's required shard acknowledgements and manifest records
   are in place, the gateway commits the object's visible metadata in a
   transaction. **This metadata commit is the visibility point.** Data from
   a partial or abandoned write is not a successful object version.
4. Generations no longer referenced by retained object versions or multipart
   state are reclaimed asynchronously after the safety checks. An overwrite
   in a versioned bucket preserves the older retained version.

A timed-out metadata commit is not assumed to have failed. Commit-resolution
and idempotency rules prevent retries and garbage collection from treating
an acknowledged or ambiguously committed generation as disposable.
See [RFC-003](../rfcs/RFC-003-distributed-metadata-and-consistency.md),
[ADR-0007](../adr/0007-commit-resolution-and-gc-safety.md) and
[ADR-0019](../adr/0019-commit-outcomes-fencing-and-reliability-closure.md).

Multipart uploads keep part data and manifests until completion or abort.
Completion publishes the selected parts through the metadata protocol;
compatibility details and tests are in
[RFC-007](../rfcs/RFC-007-s3-gateway-compatibility.md).

## Read path

The gateway resolves the version, manifest and current placements, then
streams the requested object or range. It can use a valid cache copy,
read data shards directly, or reconstruct data from enough available shards.
Checksums detect corruption; an unreadable or unrecoverable object fails
explicitly.

Read-through cache fills are bounded background work. Durable-tier migration
copies and verifies target shards before switching metadata to the new
location. Old locations are retired only after the relevant safety checks.
Cache eviction does not reduce an object's durable protection.

Read access records for automatic tiering are coalesced in a bounded table
and flushed to FoundationDB in batches. Records may be dropped under the
documented limits or on a crash, affecting tiering accuracy. The current
implementation reduces commit counts but still has a measured GET throughput
cost relative to disabling access recording; see
[Known limitations](../guide/known-limitations.md#access-record-performance)
and [ADR-0034](../adr/0034-access-records-at-scale.md).

## Control plane and failure handling

The leader publishes placement and write-admission state. Gateways use this
state without routing each data transfer through the controller. A controller
outage pauses background progress and ages admission information: gateways
throttle and eventually refuse new data when admission state is stale
(refusal after five minutes). Reads and operations that free capacity are
not refused by capacity admission. Deployments need controller redundancy.

Repair restores missing shards; scrubbing detects integrity problems; garbage
collection and reconciliation retire unreferenced records. Cluster
incarnations, group epochs, node identities, delete-fence leases and record
tokens constrain stale or ambiguous operations. These mechanisms have
different purposes and must be kept together when changing the protocols.

Metadata restore also depends on **deferred reclamation**: durable shards
remain readable for a configured retention after retirement. Protection is
bounded by backup age, clock skew, node versions and retention, and correct
incarnation handling. It is not a guarantee for arbitrarily old backups.
Use the [Backup and restore guide](../guide/backup-restore.md); the design and
assumptions are in [ADR-0028](../adr/0028-deferred-reclamation.md).

## Deployment and further reading

- [Quick start](../guide/quick-start.md): isolated evaluation on one host.
- [Configuration](../guide/configuration.md) and
  [Operational requirements](../guide/operations.md): storage isolation,
  admission, clocks, monitoring and recovery requirements.
- [Security model](../guide/security-model.md): S3 authentication and
  authorization, TLS termination, service mTLS and FoundationDB TLS.
- [RFC index](../rfcs/README.md) and [ADR index](../adr/README.md): detailed
  protocols, decisions and alternatives.
- [Risk register](risks.md): unresolved risks and evidence of their treatment.
- [Documentation index](../README.md): current guides, verification reports
  and historical development plans.
