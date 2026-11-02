# RFC-007: S3 Gateway Compatibility

- Status: draft (Phase 3.0 design; implementation Phase 3.1)
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

| Area | Operation | Status after Phase 3.1 |
| --- | --- | --- |
| Buckets | Create, Delete, Head, List | implemented |
| Buckets | Get/PutBucketVersioning | Phase 3.1 |
| Buckets | Get/Put/DeleteBucketPolicy | Phase 3.1 (RFC-012) |
| Buckets | Get/Put/DeleteBucketLifecycleConfiguration | Phase 3.1 (subset §5.10) |
| Buckets | Get/PutBucketTagging, GetBucketLocation | Phase 3.1 (tags stored; location fixed string) |
| Buckets | ACL, CORS, Website, Logging, Notification, Replication, ObjectLock, Encryption, Accelerate, RequestPayment | not planned; `NotImplemented` |
| Objects | Put, Get (range), Head, Delete | implemented |
| Objects | DeleteObjects | Phase 3.1 |
| Objects | CopyObject | Phase 3.1 |
| Objects | ListObjectsV2 | implemented |
| Objects | ListObjects (v1), ListObjectVersions | Phase 3.1 |
| Objects | Get/Put/DeleteObjectTagging | Phase 3.1 |
| Objects | GetObjectAttributes | Phase 3.1 |
| Objects | Put/GetObjectAcl | accepted, ACL ignored except `public-read` (RFC-012 §3) |
| Objects | RestoreObject, SelectObjectContent, Object Lock | not planned |
| Multipart | Create, UploadPart, UploadPartCopy, Complete, Abort, ListParts, ListMultipartUploads | Phase 3.1 |
| Auth | SigV4 header, presigned, POST policy | implemented (s3s) |
| Checksums | ETag MD5 (single part), composite ETag, five checksum algorithms, `checksum-mode` | Phase 3.1 |

### 5.2 Object model recap (RFC-002)

A key has a current pointer and, when versioning is enabled, a version
history; every version is a *generation* with a header and chunk
records; generations are `Pending` (leased), `Committed`, or `Retiring`
(GC). Multipart and copy build on generations without new concepts.

### 5.3 Multipart upload

- **Upload id**: a fresh UUIDv7 (`upload_id`), recorded in
  `("al","mpu",bucket_id,key,upload_id) = MultipartRecord { initiated_ms,
  owner, content_type, user_metadata, checksum_algorithm, state:
  Open|Completing|Completed|Aborted }` and in a listing index
  `("al","mpui",bucket_id,key,upload_id)`.
- **Parts**: each `UploadPart` is a PUT of an independent *part
  generation* (`Pending`, lease renewed as RFC-003/ADR-0011) whose header
  carries `part_of: Some(upload_id)` and `part_number`; on success the
  part record `("al","mpu",bucket_id,key,upload_id,"p",part_number) =
  PartRecord { generation, size, etag_md5, checksum }` is written,
  **replacing** any previous generation for that part number (the old
  one goes `Retiring`). Parts are streamed, erasure coded and placed
  exactly like objects (same chunking, same pipeline), so a part is
  durable before `UploadPart` returns 200 (Durable ACK holds per part).
- **Complete**: one metadata transaction reads the record (`Open`), the
  listed parts in order, validates sizes (every part except the last
  ≥ 5 MiB, part numbers ascending, ETags match), then writes a new
  *object generation header* whose chunk records are the concatenation of
  the parts' chunk records with re-based offsets (chunk ids stay the
  parts' chunk ids: shards are not rewritten; the object generation
  *adopts* the part generations' chunks), sets the composite ETag
  `md5(md5_1‖…‖md5_N)-N`, publishes the current pointer (rule V1,
  versioning as for PUT), marks the multipart record `Completed`, and
  marks the part generations as `Adopted { by: object_gen }` so that GC
  never collects their chunks while the object generation lives.
  Atomic visibility: the object appears in one transaction. Chunk record
  count may exceed the single-transaction budget for very large uploads:
  the adoption writes only a *part list* in the object header
  (`parts: Vec<GenId>`) and the read path resolves chunks through the
  part generations; this keeps completion to one transaction regardless
  of size. Alternatives rejected: copying shards into a new generation
  (doubles write traffic and time); rewriting chunk records into the
  object generation across several transactions (loses single-transaction
  atomicity).
- **Concurrent completes** of the same upload: the transaction requires
  state `Open`; the first commit wins and sets `Completed` with the
  resulting version id; a second complete with an identical part list
  returns the same result (idempotent, AWS behaviour); a differing part
  list after completion returns `NoSuchUpload`.
- **Abort**: sets `Aborted`, retires every part generation (GC collects
  them after the grace), removes the record after GC confirms; idempotent
  (`NoSuchUpload` only after the record is gone). `UploadPart` to an
  aborted or completed upload fails with `NoSuchUpload`.
- **Orphaned uploads**: never completed nor aborted. Lifecycle
  `AbortIncompleteMultipartUpload` (§5.10) and a controller default of
  7 days abort them; the part generations are ordinary pending/retiring
  generations, so RFC-008's GC and reconciliation already cover their
  shards; the metadata `mpu` record is removed by the same pass.
- **ListParts / ListMultipartUploads**: range reads of the part records
  and the `mpui` index with the AWS pagination markers.
- **UploadPartCopy**: reads the source (version) through the gateway's
  read path and writes a part; byte ranges supported.
- **Limits**: 10 000 parts, part size 5 MiB–5 GiB (configurable minimum
  for the lab), 2 000 concurrent uploads per bucket soft cap (RFC-012
  C-8).

### 5.4 Versioning exposure

- `PutBucketVersioning` → `bucket::set_versioning` (exists);
  `GetBucketVersioning` reads the record. Suspending keeps history.
- PUT returns `x-amz-version-id` (the generation id rendered as the
  version id; `null` for unversioned/suspended per RFC-002 §5.3).
- GET/HEAD with `versionId` read that version's generation (new recipe
  `read_version`); a delete marker returns `405 MethodNotAllowed` with
  `x-amz-delete-marker: true` as AWS does; a missing version
  `NoSuchVersion`.
- DELETE without `versionId` on a versioned bucket writes a delete marker
  (exists in metadata); with `versionId` it retires that generation (or
  removes the marker) — the RFC-003 §10 open point about
  commit-unknown on versioned delete is resolved by making the versioned
  delete idempotent on the version id (deleting a version that is
  already retiring succeeds).
- `ListObjectVersions` walks `("al","v",bucket_id,key,…)` newest first
  with `KeyMarker`/`VersionIdMarker` pagination; `IsLatest` from the
  current pointer; delete markers listed under `DeleteMarkers`.
- `ListObjectsV2` keeps skipping delete markers (exists).

### 5.5 Conditional requests

Evaluated in the gateway against the manifest read at request start:
`If-Match`/`If-None-Match` against the ETag (strong comparison; weak
tags → `PreconditionFailed`/`NotModified` per RFC 9110), `If-Modified-Since`/
`If-Unmodified-Since` against `last_modified` (commit time, second
precision). Precedence as RFC 9110 §13.2.2. For PUT, `If-None-Match:
<etag>` becomes supported (evaluated in the commit transaction like `*`
today); DELETE gains `If-Match`. Status codes: `304 NotModified` for GET
with `If-None-Match` match or `If-Modified-Since` not newer; `412` for
the rest.

### 5.6 CopyObject

Physical copy (OD-11): the gateway reads the source (optionally a
`versionId`, with the copy-source conditional headers) through the
normal read path and writes a new generation through the normal write
path (so coding profile, placement and durability are those of the
destination bucket); `MetadataDirective=COPY|REPLACE`; same-key copy with
`REPLACE` is the AWS way to change metadata. Response `CopyObjectResult`
with the new ETag and `LastModified`. Alternative rejected: sharing
chunks between generations (reference counting across generations would
complicate GC safety, RFC-008 I1–I10).

### 5.7 Checksums

- Single PUT: s3s decodes `x-amz-checksum-*` headers and `aws-chunked`
  trailers; the gateway computes the requested algorithm over the body
  while hashing MD5 (one pass) and compares; mismatch → `BadDigest`
  (verified by s3s for SHA-256 of the payload already). The algorithm and
  value are stored in the generation header (`checksum: Option<(Algo,
  Vec<u8>)>`) and returned by GET/HEAD when `x-amz-checksum-mode:
  ENABLED` and by `GetObjectAttributes`.
- Multipart: per-part checksums stored in the part record; the object's
  checksum is the AWS *composite* (`checksum-of-checksums` with `-N`) for
  CRC32/CRC32C/SHA1/SHA256, and the *full-object* CRC64NVME when that
  algorithm is used (AWS computes it over the whole object; the gateway
  combines part CRC64s by polynomial combination, available in the
  `crc64fast-nvme` crate; if the combination is not available at
  implementation time, CRC64NVME multipart is refused with
  `InvalidRequest` and documented).
- ETag stays MD5 (single) / composite (multipart); clients that need a
  content hash use checksums.

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
| Multipart completion | adopt part generations; object header lists parts | copy shards into one generation (2× writes); multi-transaction rewrite (non-atomic) |
| Part storage | each part is a generation with its own lease | parts as raw chunk lists in one pending generation (loses per-part Durable ACK and simple abort) |
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
| Gateway dies mid-part | the part generation's lease expires → GC; the client retries `UploadPart` (replaces) |
| Complete commits but reply lost | retry with the same parts returns the same version (idempotent) |
| Two completes with different part lists race | first wins; second gets `NoSuchUpload` |
| Abort races with Complete | state transition decides in one transaction; the loser gets `NoSuchUpload` |
| Node loss between part upload and complete | parts are coded and repairable like objects; complete does not read shards |
| Lifecycle expires an object a client is reading | the read holds the manifest; chunks stay until GC grace (RFC-008) |
| Versioned delete with unknown commit outcome | retry is idempotent on the version id |
| Copy source overwritten mid-copy | the copy reads one generation (the manifest it started with); result is that version |
| Upgrade: v1 gateway meets `mpu` keys or v2 bucket records | layout gate (RFC-014) stops a v1 gateway from serving a v2 cluster |

## 8. Consistency and durability guarantees

Unchanged: Durable ACK (`m_ack = m`) per part and per object, atomic
visibility of completion and copy, strong read-after-write on the
current pointer, generation isolation (part generations are adopted,
never shared), epoch fencing, GC safety (adopted generations are
`Referenced` through the adopting header until it retires).

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

## 11. Acceptance criteria

- The AWS CLI (`aws s3 cp` with multipart threshold, `aws s3api`
  create/upload-part/complete/abort/list-parts/list-multipart-uploads,
  put/get-bucket-versioning, list-object-versions, copy-object,
  delete-objects, get-object with `--version-id` and `--checksum-mode`)
  and the AWS SDK for Rust pass a conformance suite in CI against the
  memory backend, and the same suite passes against the cluster backend
  in the FoundationDB job.
- ETag of a multipart object equals `md5(concat)-N` computed by the test
  from the parts; single-part ETag equals the body MD5.
- Concurrent completes (10 racers) produce one version; concurrent
  uploads of the same part number keep exactly one part generation live
  after GC.
- Abort after complete returns `NoSuchUpload`; complete after abort
  returns `NoSuchUpload`; parts of an aborted upload are collected by GC
  (shard count returns to baseline).
- Versioned bucket: PUT twice, GET each version, DELETE creates a marker,
  GET returns 404 with `x-amz-delete-marker`, DELETE of the marker
  restores, `ListObjectVersions` lists both and the marker.
- Conditional GET/HEAD matrix (16 cases) returns the RFC 9110 codes.
- The compatibility matrix in §5.1 is reproduced in `docs/` from the test
  suite's own list of exercised operations, so the two cannot drift.
