# RFC-007: S3 Gateway Compatibility

- Status: draft, conditionally approved (Phase 3.0 review reflected; implementation Phase 3.1)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-001, RFC-002, RFC-003, RFC-008, RFC-012
- Related ADRs: ADR-0005 (s3s), ADR-0007 (commit resolution), ADR-0013

This is a public document.

## 1. Problem

The gateway implements nine S3 operations (bucket create/delete/head/
list, PutObject, GetObject with ranges, HeadObject, DeleteObject,
ListObjectsV2) with single-part ETags and `If-Match`/`If-None-Match: *`
on PUT. Real clients need multipart upload, versioning, conditional
reads, CopyObject, checksums, DeleteObjects and bucket policies, and they
need the semantics AWS clients assume (ETag shape, concurrent multipart
completion, version ids). The metadata layer already models versions,
delete markers and pending generations (RFC-002, RFC-003); the gateway
does not expose them. This RFC fixes the compatibility target, specifies
the designs that touch metadata, and defines the conformance tests. It
does not claim full AWS S3 compatibility: §5.1 is the authoritative list.

## 2. Goals

- A documented compatibility matrix that clients can rely on, with the
  AWS CLI and one AWS SDK exercising it in CI.
- Multipart upload with AWS semantics: part size rules, composite ETag,
  atomic completion, idempotent abort, listing of parts and uploads,
  concurrent completes resolved deterministically, orphaned parts
  collected.
- Object versioning exposed end to end on top of RFC-002 §5.3.
- Conditional requests on GET/HEAD/PUT/DELETE as AWS defines them.
- CopyObject (including metadata directive), DeleteObjects, presigned
  URLs, `x-amz-checksum-*` with the five AWS algorithms, bucket policy
  (RFC-012), a lifecycle subset.

## 3. Non-goals (and never claimed)

Object Lock, legal holds, replication, event notifications, S3 Select,
Glacier/restore, website hosting, CORS beyond a static header set,
Requester Pays, Object Lambda, access points, multi-region access points,
S3 Express, ACLs (accepted and ignored, §5.9), SigV2, SOAP, BitTorrent,
inventory, analytics, batch operations, `x-amz-tagging` beyond storing
and returning tags without tag-based policy conditions.

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| C1 | Nine operations above behave as AWS CLI expects for unversioned buckets | Verified (`crates/gateway/tests/aws_cli.rs`) |
| C2 | Multipart: 5 MiB ≤ part < 5 GiB except last, ≤ 10 000 parts, composite ETag `md5(concat(part md5s))-N`, completion atomic and visible at once, abort idempotent, parts listable, incomplete uploads listable and expirable | Open (§5.3) |
| C3 | Versioning: enable/suspend, `versionId` on PUT response and on GET/HEAD/DELETE, delete markers, `ListObjectVersions`, rule V1 of RFC-002 | Open (§5.4); metadata verified (`crates/metadata/tests/protocol.rs`) |
| C4 | Conditional GET/HEAD (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`), PUT `If-None-Match: *` and `If-Match`, DELETE with `If-Match` | Partly verified (PUT); rest Open (§5.5) |
| C5 | CopyObject within and across buckets, `MetadataDirective`, copy of a specific version, conditional copy sources | Open (§5.6) |
| C6 | `x-amz-checksum-{crc32,crc32c,sha1,sha256,crc64nvme}` stored, verified on PUT, returned on GET/HEAD with `x-amz-checksum-mode`, composite for multipart | Open (§5.7) |
| C7 | DeleteObjects with quiet mode and per-key results, versioned deletes | Open (§5.8) |
| C8 | Presigned GET/PUT URLs with expiry ≤ 7 days | Verified (s3s) |
| C9 | Lifecycle: `Expiration` by days/date with prefix filter, `AbortIncompleteMultipartUpload`, `NoncurrentVersionExpiration` | Open (§5.10) |
| C10 | Error codes and XML shapes as AWS (s3s mapping) | Verified for implemented ops |

## 5. Proposed design

### 5.1 Compatibility matrix

Status after Phase 3.1. **Implemented** = on `main`; **Verified** = exercised
by the conformance suite (`crates/testkit/tests/s3_compat.rs` with the AWS
SDK for Rust, the AWS CLI and boto3, and `phase31.rs` over the `S3` trait);
"not planned" operations return `NotImplemented`. arca-lake does not claim
full AWS S3 compatibility: this table is the claim.

| Area | Operation | Status |
| --- | --- | --- |
| Buckets | Create, Delete, Head, List (owner-scoped), GetBucketLocation | Implemented, Verified |
| Buckets | Get/PutBucketVersioning (`Enabled`/`Suspended`; MFA delete → `NotImplemented`) | Implemented, Verified |
| Buckets | Get/Put/DeleteBucketPolicy (subset of RFC-012 §5.3.3; unknown elements → `MalformedPolicy`) | Implemented, Verified |
| Buckets | Get/Put/DeleteBucketLifecycleConfiguration | Not Implemented (Phase 3.2; `NotImplemented`) |
| Buckets | Get/PutBucketTagging | Not Implemented (`NotImplemented`) |
| Buckets | ACL, CORS, Website, Logging, Notification, Replication, ObjectLock, Encryption, Accelerate, RequestPayment | not planned; `NotImplemented` |
| Objects | Put (incl. `aws-chunked` trailers), Get (range, `partNumber`, `versionId`, `checksum-mode`), Head, Delete (`versionId`, `If-Match`) | Implemented, Verified |
| Objects | DeleteObjects (≤ 1000 keys, `Quiet`, per-key errors) | Implemented, Verified |
| Objects | CopyObject (physical copy; `versionId` source; copy-source conditions; `MetadataDirective`) | Implemented, Verified |
| Objects | ListObjectsV2 | Implemented, Verified |
| Objects | ListObjectVersions (`KeyMarker`/`VersionIdMarker`, delimiter) | Implemented, Verified |
| Objects | ListObjects (v1) | Not Implemented (`NotImplemented`; the CLI and SDKs use v2) |
| Objects | Get/Put/DeleteObjectTagging, GetObjectAttributes | Not Implemented (`NotImplemented`) |
| Objects | Put/GetObjectAcl | Not Implemented (`NotImplemented`; the `public-read` policy of §5.9 is not applied) |
| Objects | RestoreObject, SelectObjectContent, Object Lock | not planned |
| Multipart | Create, UploadPart, UploadPartCopy (range), Complete (`If-None-Match`/`If-Match`), Abort, ListParts, ListMultipartUploads | Implemented, Verified (UploadPartCopy: Implemented, Not Verified by a client test) |
| Conditional | `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` on GET/HEAD; `If-None-Match: *` and `If-Match` on PUT/Complete/Copy destination/DELETE; copy-source conditions | Implemented, Verified (§5.5 table) |
| Auth | SigV4 header, presigned, POST policy (s3s); multiple access keys per principal with rotation; bucket owner + policy subset | Implemented, Verified (presigned/POST policy by s3s's own tests only) |
| Checksums | ETag MD5 (single part), composite ETag, CRC32/CRC32C/CRC64NVME/SHA1/SHA256 on PUT/UploadPart/Complete/Copy, composite and full-object, `checksum-mode` on GET/HEAD | Implemented, Verified |

#### 5.1.1 Documented differences from AWS S3

| Topic | arca-lake | AWS S3 |
| --- | --- | --- |
| Version ids | 32 hex characters | opaque, up to 1024 characters |
| `x-amz-version-id` on unversioned or suspended buckets | omitted | `null` is sent on suspended buckets |
| `409 ConditionalRequestConflict` | never emitted (conditional writes are serialized in FoundationDB; exhausted retries are `503 SlowDown`) | emitted on concurrent conditional writes |
| Conditional CopyObject destination (`If-None-Match: *`, `If-Match`) | supported | added by AWS later than PUT; behaviour matches PUT |
| Missing bucket for a non-owner | `403 AccessDenied` (RFC-012 rule 7) | `404 NoSuchBucket` (bucket names are global) |
| Minimum part size | 5 MiB by default; `--multipart-min-part-bytes` can lower it (non-AWS) | 5 MiB |
| `x-amz-mp-object-size` on Complete | accepted; a mismatch is logged, not an error | mismatch → `400` |
| Ranged GET with `checksum-mode` | no checksum headers (checksums describe whole objects or parts) | same for composite; full-object checksums are omitted for ranges as well |
| `ListObjectVersions` with a version marker whose key no longer exists | resumes after the key | undefined |
| Object ACLs | `NotImplemented` | supported |
| DeleteObjects without `Content-MD5` or `x-amz-checksum-*` | accepted (either is verified when sent) | `400 InvalidRequest` (one of them is required) |
| `CreateBucket` of a bucket the requester owns | `409 BucketAlreadyOwnedByYou` in every region (another principal's bucket: `409 BucketAlreadyExists`) | `409 BucketAlreadyOwnedByYou`, except in us-east-1, where it is `200 OK` for legacy compatibility |
| Multipart upload expiry | controller sweep after `--multipart-expiry` (7 days by default) | lifecycle rule only |

### 5.2 Object model recap (RFC-002) and generation kinds

A key has a current pointer and, when versioning is enabled, a version
history; every version is a *generation* with a header and chunk
records; generations are `Pending` (leased), `Committed`, or `Retiring`
(GC). Phase 3.1 gives the header a *kind*:

| Kind | Chunks | Referenced by | Used for |
| --- | --- | --- | --- |
| `Object` | inline (`("al","g",gen,"c",i)`) | current pointer, version record | single PUT, CopyObject |
| `Part { upload_id, part_number }` | inline | part record while the upload is open; part manifest reference of the root after Complete | one multipart part |
| `MultipartRoot { part_count }` | none of its own; part manifest references `("al","g",root,"m",n)` | current pointer, version record | a completed multipart object |

Multipart and copy build on generations without new placement or GC
concepts: shards are always owned by exactly one generation, and a
generation is never shared between two keys or two versions.

### 5.3 Multipart upload (hierarchical manifest)

Design rule from the Phase 3.0 review: parts are immutable generations,
Complete copies no shard **and no chunk manifest**. Complete publishes a
small *Object Manifest Root* that references the parts' immutable
manifests. The size of the Complete transaction is proportional to the
number of parts (≤ 10 000), never to the number of chunks or bytes.

#### 5.3.1 Structure

| Element | Key | Value | Lifetime |
| --- | --- | --- | --- |
| Multipart Upload Record | `("al","mpu",bucket_id,key,upload_id)` | `MultipartRecord { initiated_ms, owner, content_type, user_metadata, checksum_algorithm, checksum_type, state }` with `state ∈ { Open, Aborting, Completed { root } }` | created by CreateMultipartUpload; removed when the upload is aborted (end of the abort sweep) or when the root generation is finished by GC |
| Part Generation | `("al","g",part_gen,"h")` header with `kind = Part { upload_id, part_number }` | `Pending` while streaming → `Committed` when sealed → `Retiring` when superseded, aborted, discarded by Complete, or when its root is collected | one UploadPart |
| Immutable Part Manifest | `("al","g",part_gen,"c",i)` | `ChunkRecord` (chunk placement) per chunk of the part, offsets relative to the part | written while pending, never modified after the seal |
| Part Record | `("al","mpp",upload_id,part_number)` | `PartRecord { generation, size, etag, checksum, chunk_count, uploaded_ms }` | written by the seal transaction; replaced by a re-upload of the same part number; cleared by abort / root finish |
| Object Manifest Root | `("al","g",root,"h")` header with `kind = MultipartRoot { part_count }`, `chunk_count = 0`, composite ETag and checksum | `Committed` at Complete; `Retiring` when displaced or deleted | the object generation of a completed upload |
| Part Manifest Reference | `("al","g",root,"m",n)` | `PartRef { generation, part_number, offset, size, chunk_count, etag, checksum }` | written by Complete (one key per part so that the root header stays far below the 100 KB value limit); cleared by root finish |
| Object Generation | the root (multipart) or an `Object`-kind generation (single PUT) | pointed to by the current pointer / version record | visibility |
| Chunk Placement | `ChunkRecord.placement` | `{ group_id, epoch }` | unchanged (RFC-004) |

`upload_id` is a UUIDv7 rendered as 32 hex characters (opaque to
clients). Part records live under their own prefix so that
ListMultipartUploads scans only upload records and ListParts scans only
one upload's part records.

#### 5.3.2 Operations

- **CreateMultipartUpload**: one transaction writes the upload record
  (`Open`). Requires the bucket to exist and the principal to be
  authorized for `s3:PutObject` (RFC-012). Returns `upload_id`.
- **UploadPart** (`part_number ∈ [1, 10 000]`): `begin_part` allocates
  a pending generation of kind `Part` (the transaction reads the upload
  record and requires `Open`); the body is streamed, erasure coded and
  placed exactly like a PUT (same chunking, same pipeline, same lease
  renewal, RFC-003 §5.3); the **seal transaction** re-reads the upload
  record (`Open`, else `NoSuchUpload` and the generation is retired by
  the caller), reads the existing part record for `part_number`, retires
  the generation it names (re-upload of the same part number: the old
  part becomes garbage, AWS semantics "last upload wins"), writes the
  new part record, and sets the header `Committed` with size, MD5 ETag
  and checksum. Durable ACK holds per part: every shard of the part is
  durable before the seal. A seal whose commit outcome is unknown is
  resolved by re-reading the header: `Committed` → success; `Pending`
  and leased → retry; `Retiring` while the upload is still `Open` → the
  part was superseded by a concurrent re-upload (success, the client's
  bytes were replaced by its own later request); otherwise failure.
- **ListParts**: range read of `("al","mpp",upload_id,·)` with
  `PartNumberMarker`/`MaxParts`; the upload record must be `Open`.
- **ListMultipartUploads**: range read of `("al","mpu",bucket_id,prefix)`
  returning `Open` records only (`Aborting` and `Completed` are internal
  states), ordered by key then upload id (UUIDv7 order = initiation
  order), with `KeyMarker`/`UploadIdMarker`/`MaxUploads`/`Delimiter`.
- **CompleteMultipartUpload**: **one transaction**, the linearization
  point of the whole upload (§5.3.3):
  1. read the upload record: `Open` → continue; `Completed { root }` →
     if the requested part list equals the root's references (same
     part numbers and ETags) return the same result (idempotent retry
     after a lost reply), else `NoSuchUpload`; `Aborting` or missing →
     `NoSuchUpload`;
  2. validate the request: part numbers strictly ascending
     (`InvalidPartOrder`), at least one part, every listed part has a
     part record whose ETag equals the request's (`InvalidPart`), every
     part except the last is at least the minimum part size
     (`EntityTooSmall`, default 5 MiB, `--multipart-min-part-bytes`);
  3. compute offsets, total size, the composite ETag
     `hex(md5(md5_1 ‖ … ‖ md5_N))-N`, and the composite or full-object
     checksum (§5.7);
  4. evaluate conditional headers (`If-None-Match`, `If-Match`) against
     the current pointer as a PUT commit does (§5.5);
  5. write the root header (`Committed`, kind `MultipartRoot`), one
     `PartRef` per listed part, the current pointer and (versioned
     buckets) the version record, retire the displaced `null` version as
     a PUT does, and set the upload record to `Completed { root }`.
     Listed parts stay `Committed`; parts that were uploaded but **not
     listed** are not touched by this transaction (bounded transaction
     size) and are retired afterwards by the completing gateway and, if
     it dies, by the controller's multipart sweep (§5.3.4).
  The root generation id is chosen before the first attempt, so a retry
  after an unknown commit outcome re-reads the upload record and finds
  either `Open` (retry) or `Completed { root == ours }` (success).
  Transaction size: 10 000 references × (≈ 40 B key + ≈ 90 B value +
  conflict-range and mutation overhead) ≈ 2 MB plus the part records
  read (≈ 1 MB), within FoundationDB's 10 MB limit and the 5 s
  transaction window; the test suite completes a 10 000-part upload
  against FoundationDB and asserts a single commit. Nothing in the
  transaction depends on chunk count or object size.
- **AbortMultipartUpload**: transaction 1 sets the upload record to
  `Aborting` (from `Open`; `Completed` → `NoSuchUpload`; missing →
  `NoSuchUpload`). Then, in batches of at most `abort_batch` part records
  per transaction (default 512, bounded by the transaction size since a
  header rewrite carries the object key), each batch re-reads the upload
  record (`Aborting`), retires the named part generations and clears
  their part records; the last transaction clears the upload record. In-
  flight part generations (pending) are not touched: their seal fails on
  the state check and their lease expiry retires them. A gateway that
  dies mid-abort leaves an `Aborting` record that the controller sweep
  finishes; a client retrying Abort continues the same sweep.
- **UploadPartCopy**: reads a source object (version) through the read
  path and writes a part through the part write path; byte ranges
  supported. Phase 3.1 implements it if time permits, otherwise it is
  `NotImplemented` and listed as such in §5.1.
- **GET/HEAD of a multipart object**: the manifest read returns the root
  header and its references in one transaction; chunk records are then
  resolved per part, lazily and in order, only for the parts that
  overlap the requested range (binary search on `PartRef.offset`). A
  part's manifest is immutable, so it can be cached per generation.
  `x-amz-mp-parts-count` and `partNumber` GET (one part) are supported.
- **Limits**: part number 1–10 000; part size minimum 5 MiB (last part
  excepted) and maximum 5 GiB; object size ≤ 5 TiB; 1 000 open uploads
  per key (soft, RFC-012 C-8).

#### 5.3.3 Linearization points

| Operation | Linearization point |
| --- | --- |
| UploadPart | commit of the seal transaction (the part becomes the upload's part `n`) |
| CompleteMultipartUpload | commit of the Complete transaction: before it, the key's current version is unchanged and ListParts shows the parts; after it, GET/HEAD/List see the object atomically and ListParts/UploadPart return `NoSuchUpload` |
| AbortMultipartUpload | commit of transaction 1 (`Open → Aborting`): after it, Complete and UploadPart fail; the part garbage collection that follows is not observable through the S3 API |
| Concurrent Complete × Complete | the first commit wins; the second reads `Completed { root }` in its own transaction and returns the same result (identical part list) or `NoSuchUpload` |
| Complete × Abort | whichever transaction commits first decides; the loser observes the new state (`NoSuchUpload`) |
| Complete × UploadPart (same part number) | serializable: either the seal commits first and Complete validates the new ETag (mismatch → `InvalidPart`), or Complete commits first and the seal fails with `NoSuchUpload` (its generation is retired) |

#### 5.3.4 Part reference management and garbage collection

Invariant **I-MP1 (referenced parts are never collected)**: a part
generation `P` with `kind = Part { u, n }` is *live* iff the upload record
`u` exists and either its state is `Open` or `Aborting`, or its state is
`Completed { root }` and the root's reference `("al","g",root,"m",n)`
names `P` and the root header exists. Only these transitions retire a
part, and each re-reads the upload record (and the reference) in its own
transaction, so a part is retired only when it is provably not live:

1. the seal of a re-upload of the same part number (upload `Open`,
   previous part record read in the same transaction);
2. the abort sweep (upload `Aborting`);
3. the post-Complete sweep: upload `Completed { root }`, part record
   present, reference for `n` absent or naming another generation
   (discarded part);
4. root finish: the root is `Retiring` and GC is finishing it (§ below).

Defence in depth: GC's `verify_unreferenced` step (RFC-008) refuses to
collect a `Retiring` part generation whose upload record is `Completed {
root }` with a reference still naming it while the root header exists,
and counts the refusal in `gc_skipped_referenced_total` (an invariant
violation to investigate, RFC-013 §10.4).

Invariant **I-MP2 (root collection retires parts, in bounded steps)**:
finishing a `MultipartRoot` generation (root `Retiring`, unreferenced by
pointer and version records) processes its references in batches: each
transaction re-checks the root is `Retiring`, retires the referenced part
generations of the batch and clears those reference keys; the last
transaction clears the upload record and its part records, then the
root subspace. Retired parts are collected by the ordinary GC pass
(their shards are deleted, then their metadata). `Retiring` is terminal,
so a partially finished root is simply finished by the next pass.

Invariant **I-MP3 (shard attribution is unchanged)**: a shard
`(generation prefix, chunk index)` on a node is referenced iff a pending
or committed generation with that prefix places that chunk in that group
(RFC-008 §6.3). Parts are generations, so reconciliation needs no new
rule; a part's shards become orphans only after the part is retired and
collected.

Invariant **I-MP4 (upload records never leak)**: every upload record
reaches removal: `Open` records older than `--multipart-expiry` (default
7 days; 0 disables) are aborted by the controller sweep (lifecycle
`AbortIncompleteMultipartUpload` overrides per bucket); `Aborting`
records are finished by the sweep; `Completed` records are removed when
their root is finished.

The controller **multipart sweep** runs with GC: for each `Aborting`
record it continues the abort; for each `Completed` record it retires
discarded parts (transition 3); for each expired `Open` record it
begins an abort. All its transactions carry the same state checks as
the gateway's.

#### 5.3.5 Failure matrix

| Failure | Behaviour |
| --- | --- |
| Gateway dies during UploadPart | the pending part's lease expires → `expire_pending` retires it → shards collected; the client retries UploadPart |
| Seal commit outcome unknown | resolved by header state (§5.3.2) |
| Complete commit outcome unknown | re-read the upload record: `Completed { ours }` → success; `Open` → retry (same root id); `Aborting`/missing → `NoSuchUpload` |
| Gateway dies after Complete, before discarding unlisted parts | the controller sweep retires them (transition 3) |
| Gateway dies mid-Abort | `Aborting` record; sweep finishes |
| Node loss between UploadPart and Complete | parts are coded and repairable like objects; Complete reads no shard |
| Complete with a part number that was re-uploaded concurrently | the request's ETag no longer matches → `InvalidPart`; the client re-lists parts |
| Root displaced (overwrite/delete) while a GET streams it | the GET holds the manifest; part shards stay until the root is finished and the parts' grace elapses (RFC-008) |
| FoundationDB failover during Complete | the single transaction either committed or not; the retry logic above applies |
| 10 000 parts × many chunks | Complete cost is independent of chunk count; GET resolves part manifests lazily; root finish is batched |

### 5.4 Versioning exposure

- `PutBucketVersioning` → `bucket::set_versioning` (exists);
  `GetBucketVersioning` reads the record. Suspending keeps history.
- PUT returns `x-amz-version-id` (a UUIDv7 rendered as 32 hex characters
  on `Enabled` buckets; `null` on unversioned and suspended buckets, as
  RFC-002 §5.3).
- GET/HEAD with `versionId` read that version's generation (recipe
  `read_version`); a delete marker returns `405 MethodNotAllowed` with
  `x-amz-delete-marker: true`; a missing version `NoSuchVersion`; GET of
  a key whose current version is a delete marker returns `404 NoSuchKey`
  with `x-amz-delete-marker: true`.
- DELETE without `versionId` on an `Enabled` bucket writes a delete
  marker (exists); on a `Suspended` bucket it writes a `null` delete
  marker replacing the `null` version (AWS behaviour); on an unversioned
  bucket it removes the key. DELETE with `versionId` removes exactly that
  version (generation retired, version record cleared; a delete marker
  removed restores the previous version as current) and is idempotent on
  the version id: deleting a version that no longer exists succeeds, so
  a retry after an unknown commit outcome is safe.
- `ListObjectVersions` walks the version records newest first with
  `KeyMarker`/`VersionIdMarker`; the `null` version of a suspended or
  never-versioned bucket has no version record and is synthesized from
  the current pointer; `IsLatest` comes from the current pointer; delete
  markers are listed under `DeleteMarkers`.
- `ListObjectsV2` skips keys whose current version is a delete marker.

**Linearization points** (all in FoundationDB serializable transactions):

| Operation | Point | Consequence |
| --- | --- | --- |
| PUT | commit transaction | before: old current version; after: new version current, old `null` version retiring |
| DELETE (marker or unversioned) | the delete transaction | GET after it returns 404; GET before it returns the full old object (its generation is retired, its shards survive the grace period) |
| DELETE `versionId` | the delete transaction | the version disappears from ListObjectVersions and GET `versionId` atomically |
| GET / HEAD | the manifest read transaction | one consistent snapshot of pointer, header and chunk records (or root references); bytes streamed after it belong to that generation, never to a mix |
| ListObjectsV2 / ListObjectVersions | the range-read transaction of each page | each page is a snapshot; pages are not mutually atomic (as AWS) |
| Concurrent PUT × PUT (same key) | commit order | the later commit's version is current; on `Enabled` buckets both are versions; on others the earlier one is retired |
| Concurrent PUT × DELETE | commit order | either the delete marker is current (PUT's version is non-current) or the PUT's version is current |
| Concurrent GET × DELETE | the GET's manifest read vs the delete commit | the GET either sees the object fully or 404; never a truncated body because of the delete (shards outlive the grace period) |

**Differences from AWS S3 recorded in the matrix**: version ids are 32
hex characters (AWS: opaque, up to 1024 characters); `ListObjectVersions`
orders versions of one key by commit order, which is also AWS's order;
`null` versions do not appear in `ListObjectVersions` on `Enabled`
buckets unless one exists from a suspended or unversioned period (same
as AWS); MFA delete is not supported (`PutBucketVersioning` with
`MFADelete` → `NotImplemented`).

### 5.5 Conditional requests

Evaluated against the ETag and `last_modified` (commit time, second
precision for the HTTP date comparison) of the version the request
resolves to. Strong comparison only; a weak ETag in `If-Match` fails the
precondition, in `If-None-Match` it is compared by opaque-tag equality
as RFC 9110 §8.8.3 (weak comparison) allows. Precedence follows RFC 9110 §13.2.2:
`If-Match` is evaluated before `If-Unmodified-Since` (which is ignored
when `If-Match` is present), `If-None-Match` before `If-Modified-Since`
(ignored when `If-None-Match` is present). AWS's documented combination
rules give the same results and are the reference for the test matrix.

| Operation | Header | Condition false → | Notes |
| --- | --- | --- | --- |
| GET / HEAD | `If-Match` | `412 PreconditionFailed` | |
| GET / HEAD | `If-None-Match` | `304 NotModified` | body omitted; ETag and Last-Modified still sent |
| GET / HEAD | `If-Modified-Since` | `304 NotModified` | ignored if `If-None-Match` present or if the date is invalid |
| GET / HEAD | `If-Unmodified-Since` | `412 PreconditionFailed` | ignored if `If-Match` present |
| GET / HEAD on a missing key | any | `404 NoSuchKey` | the key check precedes the condition |
| PUT | `If-None-Match: *` | `412 PreconditionFailed` | evaluated in the commit transaction (exists) |
| PUT | `If-None-Match: <etag>` | `NotImplemented` | AWS accepts only `*` here; same |
| PUT | `If-Match: <etag>` | `412 PreconditionFailed`; key missing → `404 NoSuchKey` | evaluated in the commit transaction against the current version (AWS behaviour) |
| CompleteMultipartUpload | `If-None-Match: *`, `If-Match` | as PUT | evaluated in the Complete transaction |
| CopyObject | `x-amz-copy-source-if-match` | `412 PreconditionFailed` | against the source version |
| CopyObject | `x-amz-copy-source-if-none-match` | `412 PreconditionFailed` | AWS returns 412, not 304, for copy sources |
| CopyObject | `x-amz-copy-source-if-modified-since` | `412 PreconditionFailed` | |
| CopyObject | `x-amz-copy-source-if-unmodified-since` | `412 PreconditionFailed` | |
| CopyObject | destination `If-None-Match: *` | `412 PreconditionFailed` | evaluated in the destination commit (extension documented in the matrix; AWS added conditional copy destinations later than PUT) |
| DELETE | `If-Match` | `412 PreconditionFailed` | evaluated in the delete transaction |

`409 ConditionalRequestConflict`: AWS returns it when a conditional write
races another write on the same key. arca-lake serializes conditional
writes in FoundationDB transactions (a conflict is retried internally up
to the transaction retry budget), so clients observe `200`, `412` or
`404`, and `503 SlowDown` only when the retry budget is exhausted. The
matrix records that `409` is never emitted.

### 5.6 CopyObject

Physical copy (OD-11): the gateway reads the source (optionally a
`versionId`; the copy-source conditional headers are evaluated against
that version's ETag and commit time) through the normal read path and
writes a new generation through the normal write path (so coding
profile, placement and durability are those of the destination bucket).
The source version is fixed at the manifest read: a concurrent DELETE or
overwrite of the source does not affect the copy (its shards survive the
GC grace period; a copy longer than the grace period of a deleted source
fails with an error, never with wrong bytes, because every chunk is CRC
checked). `MetadataDirective=COPY|REPLACE` controls user metadata and
content type (`REPLACE` takes the request's; `COPY` the source's);
same-key copy requires `REPLACE` (AWS). Destination overwrite follows
the PUT rules (versioning, retiring the displaced `null` version); a
failure after some chunks were placed aborts the destination generation
like a failed PUT. Response `CopyObjectResult` with the new ETag (MD5 of
the bytes copied, so a copy of a multipart object gets a single-part
ETag, as AWS does for copies below the multipart threshold) and
`LastModified`. Alternative rejected: sharing chunks between generations
(reference counting across generations would complicate GC safety,
RFC-008 I1–I10).

### 5.7 Checksums

Algorithms: CRC32, CRC32C, CRC64NVME, SHA1, SHA256 (`crc32fast`, `crc32c`,
`crc64fast-nvme`, `sha1`, `sha2` crates; no checksum code of our own
beyond the standard CRC combination step below).

- **Single PUT**: `s3s` decodes `x-amz-checksum-*` headers and
  `aws-chunked` trailers; the gateway computes the requested algorithm
  over the body in the same pass as MD5; mismatch → `BadDigest`
  (`InvalidRequest` for a malformed value) **before commit**: the
  generation is aborted and nothing is published. Algorithm, value and
  type are stored in the generation header and returned by GET/HEAD with
  `x-amz-checksum-mode: ENABLED` and by `GetObjectAttributes`. A PUT
  without a checksum header stores none (AWS's default CRC64NVME
  computation on the client is a client behaviour; servers that do not
  receive a trailer have nothing to store). `x-amz-sdk-checksum-algorithm`
  without a value is accepted and the value is taken from the trailer.
- **UploadPart**: identical, per part; the part checksum is stored in the
  part record and returned by ListParts.
- **CompleteMultipartUpload**: the client sends each part's checksum
  (optional); the Complete transaction verifies them against the part
  records (`InvalidPart` on mismatch) and derives the object checksum:
  - `COMPOSITE` (default for CRC32, CRC32C, SHA1, SHA256): `algo(c_1 ‖ …
    ‖ c_N)` rendered as base64 with the `-N` suffix, as AWS;
  - `FULL_OBJECT` (the only type for CRC64NVME; optional for CRC32 and
    CRC32C via `x-amz-checksum-type: FULL_OBJECT`): the CRC of the whole
    object computed by combining the parts' CRCs with their lengths
    (standard CRC combination over GF(2); verified by a test against a
    direct CRC of the concatenation). SHA1/SHA256 cannot be combined;
    `FULL_OBJECT` with them → `InvalidRequest`.
  A client-supplied `x-amz-checksum-*` on Complete is compared with the
  derived value (`BadDigest` on mismatch); nothing is published on any
  mismatch.
- **CopyObject**: the destination's checksum is recomputed from the bytes
  copied (the copy reads the source and so verifies the source's stored
  checksum when it has one; a mismatch is a corruption report, `500
  InternalError`, never a published copy).
- ETag stays MD5 (single) / composite MD5 (multipart) and is never a
  checksum value; clients that need a content hash use checksums.
- **`Content-MD5`** (RFC 1864): a value that is not base64 of 16 bytes →
  `400 InvalidDigest`; a value that differs from the MD5 of the body →
  `400 BadDigest`. PutObject and UploadPart compare it with the MD5 the
  gateway computes anyway for the ETag, over the decoded body as it is
  streamed (multi-chunk and `aws-chunked` bodies included, no extra
  buffering), before commit: the pending generation is aborted, a part is
  not recorded, nothing is published. The XML bodies that `s3s` reads in
  full (DeleteObjects, CompleteMultipartUpload, PutBucketPolicy,
  PutBucketVersioning, CreateBucket) are checked by the server wrapper on
  the bytes as `s3s` reads them, after authentication and authorization;
  the operation does not run. Both a mismatch and a malformed value are
  answered at the end of the body (bounded by `s3s`'s XML body limit),
  so the client reads the `400`. The wrapper recognizes those operations
  as `s3s` routes them: the percent-decoded path-style path (a PUT only on
  a bucket path; an object PUT is the backend's, whatever its query or
  path encoding) and decoded query names (`?%64elete`); never an
  `aws-chunked` body. Virtual-hosted-style addressing, not served today,
  would need this matching revisited. Surrounding whitespace is ignored;
  unpadded base64 is malformed. DeleteObjects is authorized per entry
  (RFC-012), after its body is read: an anonymous one with a wrong digest
  is answered `BadDigest`, with the right one `200` with every entry
  `AccessDenied`; other operations answer `AccessDenied` before the body
  is read.
- **`x-amz-checksum-*` of an XML body** (DeleteObjects, PutBucketPolicy,
  PutBucketVersioning, CreateBucket; current SDKs send CRC32 there instead
  of `Content-MD5`): the same server wrapper computes the named algorithm
  (CRC32, CRC32C, CRC64NVME, SHA1, SHA256) over the body as `s3s` reads it
  and answers as PutObject does: mismatch → `400 BadDigest`, a value that
  is not base64 of the algorithm's length or more than one checksum header
  → `400 InvalidRequest`, an algorithm the gateway does not implement →
  `501 NotImplemented`; the operation does not run. With `Content-MD5` as
  well, both must match. On CompleteMultipartUpload the header is the
  object's checksum (above), not the body's, and is left to the backend.
- **Signed payload and body framing** (PUT and UploadPart): `s3s`
  verifies the body against the signed `x-amz-content-sha256` and the
  declared length while the gateway reads it, and the chunk signatures
  and framing of `aws-chunked` bodies. A failed check is the client's
  request and answers as AWS: payload hash → `400
  XAmzContentSHA256Mismatch`, length or framing → `400 IncompleteBody`,
  chunk signature → `403 SignatureDoesNotMatch`, oversized chunk → `400
  EntityTooLarge`. The pending generation is aborted and nothing is
  published; shards placed before the end of the body are collected by
  GC and node reconciliation (RFC-004 §5.10). Only a failure of the
  connection itself stays `500 InternalError`.

### 5.8 DeleteObjects

Up to 1 000 keys; each key resolved and deleted with the same recipe as
DELETE (marker or version), results reported per key; `Quiet` suppresses
successes. Not atomic across keys (AWS is not either); each key's delete
is atomic.

### 5.9 ACLs and other accepted-but-ignored inputs

`x-amz-acl` and `PutObjectAcl` are accepted; `GetObjectAcl` returns an
owner-only full-control ACL. `public-read*` on a bucket without a policy
allowing anonymous read returns `AccessDenied` so that clients are not
misled (RFC-012 §3). Storage class is accepted and recorded
(`STANDARD` returned; a later RFC may map classes to tiers, RFC-006 §10).

### 5.10 Lifecycle subset

Rules with `Filter.Prefix` (and `Tag` later), `Expiration.Days|Date`,
`NoncurrentVersionExpiration.NoncurrentDays`,
`AbortIncompleteMultipartUpload.DaysAfterInitiation`, `Status`. Stored in
the bucket record (v2) as the XML document; evaluated by a controller
scheduler once per `--lifecycle-interval` (default 1 h) using the object
listing with `last_modified`; actions are ordinary deletes and aborts
through the metadata recipes (so GC does the rest). Unknown rule elements
are rejected at `PutBucketLifecycleConfiguration` with
`MalformedXML`. Transitions (storage class changes) are not planned here.

### 5.11 Request identity and errors

Every response carries `x-amz-request-id` (the audit record id,
RFC-012 §5.7) and `x-amz-id-2` (gateway instance id). Error bodies are
the s3s shapes.

## 6. Alternatives considered

| Decision | Chosen | Alternatives |
| --- | --- | --- |
| Multipart completion | hierarchical manifest: root with one reference key per part, parts keep their immutable manifests | copy shards into one generation (2× writes); copy every chunk record into the root in one transaction (unbounded transaction size: forbidden by the Phase 3.0 review); multi-transaction rewrite (non-atomic) |
| Part storage | each part is a generation with its own lease and part record | parts as raw chunk lists in one pending generation (loses per-part Durable ACK and simple abort) |
| Abort / root finish | batched transactions with a terminal state check (`Aborting`, `Retiring`) | one transaction (exceeds the size limit at 10 000 parts with long keys) |
| CopyObject | physical copy through read+write | chunk sharing with refcounts (GC complexity); server-side hard links on nodes (no such primitive) |
| Composite checksum | AWS algorithm per family | full-object only (incompatible with SDK expectations for multipart) |
| Lifecycle evaluation | controller scheduler over listings | per-object timers in metadata (write amplification); external cron (ops burden) |

Evaluated on performance (no extra copies on the data path except
CopyObject by definition), consistency (completion and copy are single
publishes), durability (parts are durable before 200), operations
(orphan handling reuses GC), security (every new operation is a named
action for RFC-012 policies).

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Multipart failures | §5.3.5 (gateway death at every step, unknown commit outcomes, races, node loss, FoundationDB failover) |
| Lifecycle expires an object a client is reading | the read holds the manifest; chunks stay until GC grace (RFC-008) |
| Versioned delete with unknown commit outcome | retry is idempotent on the version id |
| Copy source overwritten mid-copy | the copy reads one generation (the manifest it started with); result is that version |
| Upgrade: v1 gateway meets `mpu` keys or v2 bucket records | layout gate (RFC-014) stops a v1 gateway from serving a v2 cluster |

## 8. Consistency and durability guarantees

Unchanged: Durable ACK (`m_ack = m`) per part and per object, atomic
visibility of completion and copy, strong read-after-write on the
current pointer, generation isolation (a part belongs to one upload and,
after Complete, to one root; parts are never shared), epoch fencing, GC
safety. New invariants I-MP1 … I-MP4 (§5.3.4) bind multipart to GC
safety: a referenced part generation is never retired, a retiring root
retires its parts in bounded steps, shard attribution needs no new
rule, upload records never leak.

## 9. Performance considerations

Multipart completion is metadata-only (one transaction, size-independent
with part adoption). Reads of adopted objects resolve one extra
header per part (cached in the manifest read). CopyObject costs a full
read and write. Checksums add one hash pass per PUT (CRC32C is 6–7 % of
GET CPU today; SHA-256 would be more: Hypothesis ~0.5 GiB/s per core,
measured in Phase 3.1).

## 10. Open questions

- Minimum part size for lab profiles with 4 MiB chunks (proposal: keep
  AWS 5 MiB; allow `--multipart-min-part-bytes` override, documented as
  non-AWS).
- Should the gateway accept `x-amz-tagging` on PUT and return it
  (proposal: yes, stored in the header, no policy conditions on tags).
- CRC64NVME combination availability.
- Follow-up (0.2.0 review of the body error mapping): when a PUT or
  UploadPart fails before its end (a refused body, an I/O error), shards
  already placed for chunks not yet recorded in the generation are
  reclaimed by node reconciliation (RFC-004 §5.10), not by the abort.
  Deleting them at abort time would free the space sooner; no change
  for now.

## 11. Acceptance criteria

Status labels (Phase 3.0 review item 1.5): **Designed** = specified in
this RFC; **Implemented** = code merged on `main`; **Verified** = an
automated test or a recorded measurement exercises it; **Not
Implemented** / **Not Verified** = explicitly not yet. Designed never
implies Implemented or Verified. This table is updated with every
phase report.

| # | Criterion | Status |
| --- | --- | --- |
| A1 | AWS CLI conformance (`aws s3 cp` multipart, `s3api` create/upload-part/complete/abort/list-parts/list-multipart-uploads, put/get-bucket-versioning, list-object-versions, copy-object, delete-objects, get-object `--version-id` and `--checksum-mode`) against the cluster backend in CI | Implemented, Verified (`s3_compat.rs::aws_cli_conformance`; skips where the CLI is absent; the CI runner installs it) |
| A2 | Two AWS SDKs (Rust, Python boto3) pass the same conformance scenarios in CI | Implemented, Verified (`rust_sdk_conformance`, `boto3_conformance` via `tests/compat/boto3_compat.py`) |
| A3 | Multipart ETag equals `md5(concat)-N` computed by the test; single-part ETag equals the body MD5 | Verified (SDK, CLI, boto3, `phase31.rs`) |
| A4 | 10 000-part upload completes with one metadata transaction; GET verifies every byte | Verified on the in-memory store (`multipart.rs`, 10 000 parts, one transaction attempt, 1 KiB keys); Verified on FoundationDB in the CI backend job (`fdb.rs::ten_thousand_part_complete_is_one_transaction_on_foundationdb`) |
| A5 | Concurrent Completes (≥ 8 racers) produce exactly one version and one root | Verified (`multipart.rs`, `phase31.rs`) |
| A6 | Concurrent UploadPart of the same part number keeps exactly one part generation live after GC | Verified (`phase31.rs::multipart_abort_and_reupload_collect_exactly_the_right_shards`; re-upload sequential, superseded shards collected) |
| A7 | Complete × Abort race: exactly one wins; no referenced part is ever retired (`skipped_referenced == 0`) | Verified (`multipart.rs`, `phase31.rs`) |
| A8 | Complete with an unknown commit outcome resolves to exactly one visible version with the chosen root id | Verified (`phase31.rs`, fault-injected lost reply with and without apply) |
| A9 | Gateway death during UploadPart, after Complete and mid-Abort leaves no leaked generation, part record or upload record after GC and the sweep | Verified (`phase31.rs`: UploadPart death → lease expiry; death after Complete → sweep discards unlisted parts; death mid-Abort → `sweep_multipart` finishes the abort and GC returns the shard count to the baseline; expiry of stale open uploads). The gateway "death" is modelled by stopping the gateway's own cleanup, not by killing a process |
| A10 | FoundationDB failover during multipart uploads: every acknowledged part and Complete is durable | Implemented, Verified in CI (`fdb_failover.rs` step 4b, Phase 3.2): UploadPart under process loss, a Complete/Abort race under coordinator loss, two concurrent Completes under process loss (one object, one ETag, or explicit errors), orphan GC and manifest verification afterwards; the object is whole or absent, never partial. CommitUnknown is exercised as the process loss during the concurrent Completes, not injected deterministically (that case is `phase31.rs::complete_with_unknown_commit_outcome_…` on the fault backend) |
| A11 | Versioned bucket semantics incl. markers, restore, suspended `null` | Verified (`versions.rs`, `phase31.rs`, SDK/CLI/boto3) |
| A12 | Conditional GET/HEAD matrix (16 combinations) and PUT/Complete/Copy/DELETE conditions | Verified (`phase31.rs::conditional_get_head_matrix...`) |
| A13 | CopyObject: fixed source version under concurrent DELETE, overwrite, directive, content type, ETag, checksum, conditional copy, failure recovery | Verified (`phase31.rs::copy_object_is_a_physical_copy...`) |
| A14 | Five checksum algorithms on PUT, UploadPart, Complete (composite and full-object), GET/HEAD return; mismatch publishes nothing and returns `BadDigest` | Verified (`phase31.rs::checksums_are_verified...`, CRC combination unit tests, SDK trailer checksums) |
| A15 | Property-based and model-based tests of the versioned key state machine | Implemented, Verified (`version_model.rs`: random PUT / DELETE / DELETE-version / Enable / Suspend sequences against a reference model, 400 cases; it found and fixed a duplicated `null` version record after a version delete restored the null version). The generation model (`model.rs`) and the policy monotonicity property (`policy.rs`) also exist |
| A16 | The compatibility matrix is generated from the conformance suite's list of exercised operations | Not Implemented (maintained by hand; §5.1) |
| A17 | Performance regression with multipart PUT added | see the Phase 3.1 report (`benchmarks/linux/results`) |
| A18 | A signed PUT or UploadPart whose body does not match its `x-amz-content-sha256` is `400 XAmzContentSHA256Mismatch` and changes nothing; re-creating one's own bucket is `409 BucketAlreadyOwnedByYou`, another principal's `409 BucketAlreadyExists` | Verified (`s3_errors.rs`, cluster and memory backends, AWS SDK for Rust; body stream error mapping unit tests in `gateway/src/body.rs`) |
| A19 | `Content-MD5`: malformed → `400 InvalidDigest`, mismatch → `400 BadDigest`, nothing committed, on PutObject (single and multi-chunk, plain and `aws-chunked`) and UploadPart, and on the DeleteObjects and PutBucketPolicy bodies | Verified (`s3_content_md5.rs`, cluster and memory backends, AWS SDK for Rust; `gateway/src/xml_digest.rs` unit tests) |
| A20 | `x-amz-checksum-*` (CRC32, CRC32C, CRC64NVME, SHA1, SHA256) of the DeleteObjects, PutBucketPolicy, PutBucketVersioning and CreateBucket bodies: mismatch → `400 BadDigest`, malformed → `400 InvalidRequest`, the operation not applied; the SDK's own checksums accepted; CompleteMultipartUpload's header stays the object's checksum | Verified (`s3_xml_checksums.rs`, AWS SDK for Rust; `gateway/src/xml_digest.rs` unit tests) |
