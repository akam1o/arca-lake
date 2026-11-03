# RFC-012: Security Architecture

- Status: draft, conditionally approved (Phase 3.0 review reflected; implementation Phase 3.1–3.3 as noted per control)
- Authors: arca-lake maintainers
- Created: 2026-11-02
- Depends on: RFC-001, RFC-003, RFC-007, RFC-011
- Related ADRs: ADR-0006 (TLS on in production), ADR-0013 (Phase 3.0 decisions)
- Related: [threat model](../security/threat-model.md), [public artifacts policy](../security/public-artifacts.md), `SECURITY.md`

This is a public document; it contains no deployment-specific detail.

## 1. Problem

arca-lake today authenticates S3 requests with one static key (or none),
authorizes every signed request for everything, runs its cluster network
in plaintext with no service identity, has no audit record of S3
requests, and bounds resources only where the data path happened to need
a bound (ADR-0011, RFC-006 §5.3.1). The threat model lists what that
leaves open. This RFC specifies the controls, chosen to reuse what the
`s3s` crate, `tonic`/`rustls` and FoundationDB already provide rather
than to implement cryptography or a policy language of our own, and to
be usable by any deployment of this open-source project without an
external identity provider.

## 2. Goals

- Every S3 request is authenticated (AWS Signature Version 4) unless the
  operator explicitly enables anonymous access, and is authorized against
  the bucket's owner and policy.
- Tenants (principals) cannot see or affect each other's buckets.
- Every connection on the cluster network is mutually authenticated and
  encrypted; a peer's identity, not its address, decides what it may do.
- Credentials can be rotated without downtime; nothing secret is stored
  in the metadata store.
- Every S3 request leaves one audit record with enough to attribute it.
- No single client can exhaust a gateway's memory, connections, or a
  node's streams; limits are explicit and exported.
- The design applies to a single-tenant lab as well as a multi-tenant
  deployment, with the same code paths.

## 3. Non-goals

- Encryption at rest, SSE-S3 or SSE-KMS (deployment-provided disk
  encryption; SSE-C may follow in a later RFC).
- An IAM-compatible policy engine (users, groups, roles, STS, condition
  keys beyond a small set). Bucket policies cover a documented subset.
- Object ACLs and canned ACLs (requests carrying them are accepted and
  the ACL ignored, as many S3-compatible stores do, except `public-read`
  which is refused unless anonymous read is allowed by bucket policy).
- Authorization inside FoundationDB (row-level): the cluster file is the
  credential; RFC-013 says how to protect it.
- DDoS protection beyond the gateway's own limits (a fronting proxy or
  the platform does that).

## 4. Requirements

| # | Requirement | Status |
| --- | --- | --- |
| S1 | SigV4 header and query (presigned) authentication, incl. `aws-chunked` payloads with trailing checksums, with the secret never transmitted | Verified (s3s 0.17 does this today; OD-06) |
| S2 | More than one access key, each mapped to a principal; keys addable, disableable and rotatable at runtime | Open (design §5.2) |
| S3 | Bucket ownership recorded at creation; only the owner (or a policy) grants access | Open (§5.3) |
| S4 | Bucket policy: a JSON subset of the AWS grammar, evaluated per request, denies win | Open (§5.3) |
| S5 | mTLS on gRPC with role- and node-scoped authorization of RPCs | Implemented, Verified (Phase 3.4, §5.5, S-A11) |
| S6 | FoundationDB connections use TLS with client certificates in production | Implemented, Verified on a three-process TLS cluster (Phase 3.4, `fdb_tls.rs`) |
| S7 | Audit record per S3 request: time, request id, principal (key id), source, action, bucket/key, result code, bytes, duration | Open (§5.7) |
| S8 | Rate limits per principal and per source; request and body size limits; in-flight caps; timeouts on every socket read | Partly verified (ADR-0011 stream timeouts; RFC-006 queue bounds); rest Open (§5.8–§5.9) |
| S9 | No credential or key in the metadata store or in logs | Open (verified by test and by the sanitizer policy for artifacts) |
| S10 | Controls add at most a few percent of CPU per GiB on the data path (TLS excepted, measured in RFC-009 §6.11: +0.5–0.7 CPU-s/GiB) | Hypothesis (measure in Phase 3.1) |

## 5. Proposed design

The controls are numbered **C-1 … C-11**; the threat model references them.

### 5.1 C-1 S3 authentication (SigV4)

- `s3s` performs SigV4 verification (headers, presigned query, POST
  policy) and the payload hash check. arca-lake implements only the
  `S3Auth` trait: `get_secret_key(access_key_id) -> SecretKey` from the
  credential store (§5.2). SigV2 stays disabled.
- Anonymous access is a deployment flag (`--allow-anonymous`, existing),
  logged at startup and in every audit record as principal `anonymous`;
  with it set, authorization still runs (§5.3) with the anonymous
  principal, so a bucket must explicitly allow anonymous reads for them
  to succeed. Today's "anonymous means everything is allowed" ends.
- Clock skew: s3s's 15-minute window is kept; the gateway exports a
  counter of `RequestTimeTooSkewed` rejections.

### 5.2 C-4 credential store and rotation

- **Store**: a credentials file (TOML) read by the gateway at start and
  on `SIGHUP`/periodic reload (every 30 s, mtime-based), containing
  principals and their keys:

  ```toml
  [[principal]]
  name = "tenant-a"
  [[principal.key]]
  id = "AKIA…1"          # access key id
  secret = "…"           # or secret_file = "/run/secrets/tenant-a-1"
  state = "active"       # active | inactive
  not_after = "2026-12-31T00:00:00Z"  # optional
  ```

  A principal may hold up to two active keys; rotation is: add key 2,
  distribute it, mark key 1 `inactive` (requests with it are refused with
  `InvalidAccessKeyId` and audited), remove key 1 later. Nothing in the
  file is written to FoundationDB; a gateway restart or reload picks up
  changes atomically (whole-file swap).
- **Secrets in memory** are held in `s3s`'s `SecretKey` (zeroized on
  drop); the file is read with mode checks (refuse world-readable files,
  warn on group-readable).
- **Service credentials** (certificates, §5.5) rotate by file replacement
  plus reload; the CA may list two roots during a CA rotation.
- Alternatives: keys in FoundationDB (rejected: puts secrets in the
  backup and in every process with the cluster file; makes R-19 worse);
  an external IdP/STS (out of scope; the file format leaves room for an
  `issuer` field later).

### 5.3 C-3 bucket ownership, policy, and tenant isolation

Initial scope (Phase 3.0 review item 1.2): bucket owner plus a bucket
policy subset. IAM users/roles/groups, STS and ACLs are out of scope.

#### 5.3.1 Records

`BucketRecord` gains `owner: String` (principal name) and `policy:
Option<String>` (the policy document as stored, validated at
registration). Record version 2 with a version-1 decoder (RFC-014
§5.3): a v1 bucket decodes with `owner = ""` (no owner). A bucket with
no owner is accessible only through `arca-lake admin bucket set-owner`
(B3, cluster-file holder) until an owner is assigned: with no owner and
no policy every principal is denied (rule 1).

#### 5.3.2 Authorization rules (normative)

1. **Default deny.** A request is allowed only if a rule below allows it.
   Absence of a policy, absence of a matching statement, an unknown
   action, or an unknown bucket all deny.
2. **Explicit Deny wins.** If any applicable `Deny` statement matches
   the request, the request is denied, regardless of `Allow` statements
   and regardless of ownership (rule 4 lists the one exception).
3. **Bucket owner rights.** The owner of a bucket is allowed every
   bucket-scoped and object-scoped action on that bucket
   (`s3:*` on `arn:aws:s3:::bucket` and `arn:aws:s3:::bucket/*`) without
   a policy. Ownership is recorded at `CreateBucket` (the authenticated
   principal) and changed only by the admin command. `ListAllMyBuckets`
   returns the principal's own buckets only. `CreateBucket` is allowed
   for every authenticated principal (a deployment flag
   `--bucket-creation allow|deny|list:<principals>` restricts it).
4. **Explicit Deny applies to the owner too**, with one exception: a
   `Deny` can never remove the owner's `s3:PutBucketPolicy`,
   `s3:DeleteBucketPolicy` and `s3:GetBucketPolicy`, so an owner cannot
   lock themselves out of fixing the policy (AWS behaves the same for the
   account root). A policy that denies those actions to the owner is
   accepted but the denial is ineffective for the owner and the audit
   record notes `owner-override`.
5. **Unsupported elements are rejected at registration.**
   `PutBucketPolicy` parses the document against the grammar in §5.3.3;
   any element, action, condition operator, condition key, principal
   form or resource form outside the grammar fails with
   `MalformedPolicy` and the message names the first offending element.
   Nothing is stored that the evaluator does not fully understand, so the
   evaluator never meets an unknown element at request time.
6. **Evaluation errors deny.** Any error while evaluating (a stored
   policy that fails to parse because of a bug or corruption, a missing
   bucket record mid-request, a metadata read error) results in
   `AccessDenied` (or the metadata error for the request as a whole),
   never in an allow. The owner's rule 3 still applies when the bucket
   record itself is readable (a corrupt *policy* does not lock out the
   owner, who can replace it).
7. **No tenant leakage.** Error responses do not reveal whether a bucket
   or key exists to a principal that is not allowed to know: a denied
   request on an existing bucket and a request on a non-existent bucket
   both return `403 AccessDenied` for non-owners (AWS behaviour for
   `HeadBucket`/`GetObject` without `ListBucket`); `ListAllMyBuckets`
   never lists other principals' buckets; audit records of one tenant
   are never returned to another (there is no tenant-facing audit API);
   metrics carry no bucket, key or principal labels.
8. **Gateway and storage node authorization are separate boundaries.**
   The gateway authorizes *S3 principals* against *buckets and objects*
   (this section). Storage nodes know nothing about S3 principals,
   buckets or policies; they authorize *service identities* against
   *RPCs* (§5.5). A compromised gateway therefore has the data-path
   rights of a gateway identity and nothing more (threat T6); a storage
   node never makes an S3 authorization decision.
9. **Internal RPCs carry a service identity and role-based
   authorization** (§5.5): every gRPC connection is mutually
   authenticated; the server maps the peer's certificate to a role and
   checks the role against the RPC table before any argument is read.

Evaluation order per request, in `S3Access::check` before the operation
runs and inside the operation's metadata transaction for the bucket
record read (so a policy change and a write are serialized):

```text
action := action_of(operation)             -- table in §5.3.3
bucket := bucket_record(name)              -- missing → deny (rule 7)
if any Deny statement matches (principal, action, resource, conditions):
    if principal == owner and action ∈ {Put,Get,Delete}BucketPolicy: allow (owner-override, rule 4)
    else deny (rule 2)
if principal == bucket.owner: allow (rule 3)
if any Allow statement matches: allow
deny (rule 1)
```

#### 5.3.3 Supported AWS policy JSON subset (compatibility matrix)

| Element | Supported values | Unsupported (rejected at registration) |
| --- | --- | --- |
| `Version` | `"2012-10-17"` | anything else |
| `Statement` | array of statements (also a single object) | — |
| `Sid` | any string, unique within the document | duplicates |
| `Effect` | `Allow`, `Deny` | — |
| `Principal` | `"*"`, `{"AWS": "*"}`, `{"AWS": "<name>"}` or a list of names (principal names of this deployment; the `arn:aws:iam::…` form is accepted when its last path element is a principal name) | `Service`, `Federated`, `CanonicalUser`, `NotPrincipal` |
| `Action` | string or list of: `s3:GetObject`, `s3:GetObjectVersion`, `s3:PutObject`, `s3:DeleteObject`, `s3:DeleteObjectVersion`, `s3:ListBucket`, `s3:ListBucketVersions`, `s3:ListBucketMultipartUploads`, `s3:ListMultipartUploadParts`, `s3:AbortMultipartUpload`, `s3:GetObjectAttributes`, `s3:GetBucketVersioning`, `s3:PutBucketVersioning`, `s3:GetBucketPolicy`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy`, `s3:GetBucketLocation`, `s3:DeleteBucket`, `s3:*`, and prefix wildcards over this list (`s3:Get*`, `s3:*Object*`) | any action outside the list (including `s3:PutObjectAcl`, `s3:GetObjectAcl`, lifecycle and tagging actions until implemented), `NotAction` |
| `Resource` | `arn:aws:s3:::<bucket>`, `arn:aws:s3:::<bucket>/*`, `arn:aws:s3:::<bucket>/<prefix>*` where `<bucket>` is the policy's bucket; `"*"` | other buckets, `NotResource`, wildcards inside the bucket name |
| `Condition` | `StringEquals`, `StringNotEquals`, `StringLike`, `StringNotLike` on `s3:prefix`, `s3:delimiter`, `s3:x-amz-metadata-directive`, `s3:x-amz-copy-source`; `IpAddress`, `NotIpAddress` on `aws:SourceIp` (CIDR); `Bool` on `aws:SecureTransport` | every other operator or key (`aws:PrincipalArn`, `aws:userid`, `s3:ExistingObjectTag/*`, `aws:CurrentTime`, `...IfExists`, `ForAllValues`, `ForAnyValue`, …) |

Operation → action mapping: Get/HeadObject → `s3:GetObject`
(`s3:GetObjectVersion` when `versionId` is given); PutObject,
CopyObject destination, CreateMultipartUpload, UploadPart,
UploadPartCopy destination, CompleteMultipartUpload → `s3:PutObject`;
CopyObject/UploadPartCopy source → `s3:GetObject`(`Version`) on the
source bucket; DeleteObject(s) → `s3:DeleteObject`
(`s3:DeleteObjectVersion` with `versionId`); ListObjects(V2),
HeadBucket → `s3:ListBucket`; ListObjectVersions →
`s3:ListBucketVersions`; ListMultipartUploads →
`s3:ListBucketMultipartUploads`; ListParts →
`s3:ListMultipartUploadParts`; AbortMultipartUpload →
`s3:AbortMultipartUpload`; bucket versioning/policy/location/delete →
the like-named action. Operations without a listed action (ACLs,
tagging, lifecycle, …) are `NotImplemented` and never authorized.

#### 5.3.4 Tenant isolation

The bucket is the tenant boundary: `owner` on the bucket record; every
lower key (objects, versions, generations, part records, cache
placements) carries the bucket id or a generation id that belongs to one
bucket, so nothing below the bucket needs a tenant dimension. Per-tenant
quotas are an RFC-013 item.

Alternatives: object ACLs (rejected: AWS itself recommends policies;
per-object metadata on every GET costs a read); a separate authorization
service (rejected for the OSS baseline: one more process; the policy is
small enough to evaluate in the gateway from the bucket record it
already reads for every request).

### 5.4 C-2 edge TLS

The gateway serves HTTPS directly (`--tls-cert`, `--tls-key`, rustls,
HTTP/1.1 and HTTP/2) or sits behind a TLS-terminating proxy. With a
proxy, `--trusted-proxies` enables `X-Forwarded-For` for the audit record
and rate limiting; without it the header is ignored. Plain HTTP stays
available for the lab and is logged at startup as insecure.

### 5.5 C-9 internal mTLS and service identity

Implemented in Phase 3.4; decisions and rejected alternatives in
[ADR-0017](../adr/0017-service-mtls-rpc-authorization-and-metadata-tls.md),
which amends this section where noted.

- Every storage-node listener and every node client (gateway,
  controller, admin tools) uses mutual TLS with `rustls`: server and
  client certificates required, chain and validity checked against a
  trust bundle of one or more roots (two during a CA rotation), optional
  CRLs. Certificates, keys, bundles and CRLs are files, re-read when they
  change (polled, `--tls-reload-secs`, default 30 s); a reload whose
  material does not validate keeps the previous one and is counted. When
  the bundle or the CRLs change, connections authenticated under the old
  trust are closed, so a withdrawn identity cannot keep a connection.
- **Identity** (amended): a URI SAN in SPIFFE form
  `spiffe://<trust-domain>/<role>/<name>`, roles `node`, `gateway`,
  `controller`, `admin`; a node's name is its node id. SPIRE or any
  SPIFFE-aware CA can issue these certificates; `arca-lake tls` is a
  development CA for labs and tests.
- **Authorization of RPCs** on the node (amended: nodes do not call
  nodes, so the node role has no data-path rights; `Health` carrying
  cluster facts changes node state and is control-plane only):

  | RPC | gateway | controller | admin | node |
  | --- | --- | --- | --- | --- |
  | `PutShard`, `GetShard`, `ListShards`, `DeleteShard` | yes | yes | yes | no |
  | `VerifyShards`, `ScrubShards`, `UpdateGroupEpoch` | no | yes | yes | no |
  | `Health` without cluster facts | yes | yes | yes | yes |
  | `Health` carrying `format_write` or an incarnation | no | yes | yes | no |

  Checked in each handler before any argument is read; a refusal is
  `PermissionDenied`, logged with the identity (never key material) and
  counted. Fencing is independent and unchanged: the group epoch on every
  write and delete, the incarnation on deletes, the leader token on every
  controller metadata commit refuse an authenticated but stale process.
- **Client-side verification**: a client requires role `node` in its
  trust domain and, when it knows which node serves the address (the
  gateway's map, the controller's node records), exactly that node id;
  host names and IP SANs are not used.
- **FoundationDB**: FoundationDB's own client TLS (`--fdb-tls-ca`,
  `--fdb-tls-cert`, `--fdb-tls-key`, `--fdb-tls-verify-peers`), set before
  the client network starts; a cluster file must list only `:tls`
  coordinators when TLS is configured and none otherwise (fail closed on a
  mismatch); FoundationDB refreshes its certificate files itself.
  FoundationDB has no per-key authorization: a metadata certificate
  grants the whole keyspace (R-19 narrowed to holders of a valid
  certificate, not closed).
- **Lab mode**: `--insecure-plaintext` / `--fdb-insecure-plaintext` (or
  `ARCA_INSECURE_PLAINTEXT=1` / `ARCA_FDB_INSECURE_PLAINTEXT=1`) keep a
  plaintext transport, refused together with TLS flags, logged at startup
  and exported (`*_tls_enabled 0`). Without either TLS or the explicit
  flag a process refuses to start.
- Alternatives: see ADR-0017 (tonic's built-in TLS, DNS SAN checks,
  restart-only rotation, environment-only FoundationDB TLS, IPsec/
  WireGuard as the control).

### 5.6 Admin and metrics endpoints

`/metrics` and `/healthz` bind to loopback by default; binding elsewhere
requires an explicit address and should be behind the platform's scrape
network or the same mTLS (a `--metrics-tls` option mirrors §5.5). No
label carries an address, key id or key name. `arca-lake admin` is
authorized by holding the cluster file (B3); its mutating subcommands
write an entry to the controller's audit log (§5.7) with the operator's
host user name.

### 5.7 C-6 audit logging

- One structured record per S3 request on the gateway, to a dedicated
  `tracing` target `arca.audit` (JSON when `--log-json`), separable from
  operational logs by target. Fields: `ts`, `request_id` (also returned
  as `x-amz-request-id`), `principal`, `key_id`, `source_ip`,
  `action`, `bucket`, `key`, `version_id`, `status`, `error_code`,
  `bytes_in`, `bytes_out`, `duration_ms`, `auth` (`sigv4-header`,
  `sigv4-query`, `anonymous`), `decision` (statement id or `owner`).
- Never logged: secrets, signatures, user metadata values, object bodies.
- Controller mutating actions (membership, epoch, GC finish, repair
  start/complete, policy changes via admin) go to `arca.audit` as well.
- Retention and shipping are the deployment's (RFC-013 §7); the format is
  stable and documented (an `audit_v1` schema file).

### 5.8 C-7 request limits

| Limit | Default | Where |
| --- | --- | --- |
| Object size (single PUT) | 5 GiB (s3s) | s3s config |
| Multipart part size | 5 MiB–5 GiB, 10 000 parts (RFC-007) | gateway |
| XML body | 20 MiB (s3s), 1 MiB for policies | s3s config, gateway |
| Header size / count | hyper defaults (16 KiB per header, 100 headers) | hyper |
| Declared `PutShard` payload length | ≤ shard size of the group's profile (today it is trusted for an allocation) | storage node |
| Concurrent connections per gateway | 10 000 | gateway listener |
| In-flight request bytes per gateway | 25 % of RSS budget flag (`--memory-budget`, default 1 GiB) | gateway admission |
| Requests per second per principal / per source | token bucket, default 2 000/s burst 4 000; `--rate-limit-*` | gateway |

Exceeding a rate limit returns `503 SlowDown` with `Retry-After`;
exceeding a size limit returns the S3 error s3s already produces
(`EntityTooLarge`, `MaxMessageLengthExceeded`).

### 5.9 C-8 resource exhaustion

- Timeouts: TLS handshake 10 s, request header 30 s, body progress
  (no byte for `N` s, default 30 s) on uploads; node streams keep
  ADR-0011's progress and absolute deadlines.
- In-flight caps: the existing bounds (`read_parallelism`,
  `write_pipeline`, population queue depth, repair permits) are kept and
  exported; a gateway-wide byte budget (table above) admits requests and
  refuses with `503 SlowDown` when spent, never by blocking.
- Storage node: the declared payload length is checked against the
  group's shard size before allocation; per-peer stream caps
  (`--max-streams-per-peer`, default 64) and a per-node in-flight byte
  budget; `ListShards`/`VerifyShards` page caps stay (16 384).
- Controller: scheduler back-off and batch limits as RFC-011; repair and
  scrub rates (RFC-008 open question) get explicit `--*-rate` flags in
  Phase 3.2.

### 5.10 Secrets handling rules

Secrets live in files with mode 0600 (checked) or in environment
variables; never in command lines (`ps` exposure: the existing
`--secret-key` flag is kept for the lab, hidden from `--help` env dumps,
and warned about), never in FoundationDB, never in logs or metrics, never
in benchmark artifacts (`docs/security/public-artifacts.md`).

### 5.11 C-11 supply chain and CI

- Dependencies: `Cargo.lock` committed; `cargo-deny` advisories,
  licenses, sources and bans in CI; no git dependencies.
- GitHub Actions: `permissions: contents: read` at the top of every
  workflow; third-party actions pinned to a commit SHA; no repository
  secrets used by CI.
- Self-hosted runners: run only `push` events to `main` by repository
  maintainers; `pull_request` from forks does not run on them (a
  separate workflow with `pull_request_target` is **not** used; fork PRs
  run on GitHub-hosted runners when available, otherwise after a
  maintainer pushes the branch); runners are ephemeral-by-policy (a fresh
  VM per host rebuild, no production credentials on them) and have no
  network path to validation or production hosts. Details and the
  public-repository checklist: RFC-013 §9.

## 6. Alternatives considered

| Decision | Chosen | Alternatives and why not |
| --- | --- | --- |
| Authentication | SigV4 via s3s, file-backed keys | OAuth/OIDC bearer tokens (S3 clients do not speak them); SigV2 (deprecated); custom MAC (never) |
| Authorization model | bucket owner + bucket policy subset | full IAM (too large; most deployments want per-bucket grants); object ACLs (per-object cost, AWS-discouraged); external PDP (extra process) |
| Policy language | AWS JSON subset, rejected when unknown | Cedar/OPA (new language for S3 users; could be a plug-in later); allow-list only (cannot express deny or prefixes) |
| Service identity | X.509 with SPIFFE-shaped SAN over mTLS | pre-shared tokens (distribution problem); network isolation only (no per-RPC authorization; R-19) |
| Credential storage | file, reloadable | FoundationDB (secret in backups and in every process); KMS/Vault (deployment-specific; can feed the file) |
| Audit transport | structured tracing target | database table (volume; coupling); syslog only (loses structure) |
| Rate limiting | in-gateway token buckets | proxy-only (then unauthenticated principals cannot be told apart); global quotas in metadata (hot key) |

Evaluated on: performance (SigV4 and TLS are the only data-path costs;
policy evaluation is a cached bucket record), consistency (ownership and
policy are bucket metadata written in the bucket's transaction),
durability (no new durable state besides two bucket fields), operations
(files and reload, no new service), security (every boundary gets an
identity-based control).

## 7. Failure scenarios

| Scenario | Behaviour |
| --- | --- |
| Credentials file missing or unreadable at start | gateway refuses to start (no fallback to anonymous) |
| Credentials file invalid on reload | keep the previous set, log, export `arca_gateway_credentials_reload_failures_total` |
| Certificate expired | the peer refuses the handshake; metrics `arca_*_tls_handshake_failures_total`; the certificate's `not_after` is exported as a gauge so alerts fire before expiry (RFC-013 §7) |
| CA rotation half done | both roots trusted during the window; a node with only the new cert and a gateway with only the old root fail closed and are visible in handshake failures |
| Policy document corrupt in metadata | fail closed for non-owners; owner access unaffected; `admin bucket-policy` can reset |
| Rate limit misconfigured to zero | startup validation rejects |
| Audit sink unavailable (disk full) | requests continue; dropped-audit counter; alert |
| Upgrade failure (RFC-014) | record version 2 buckets are unreadable by a version-1 gateway: the layout gate prevents a v1 gateway from joining once any v2 record exists |
| Clock skew between client and gateway > 15 min | `RequestTimeTooSkewed`; NTP is an operational requirement |

## 8. Consistency and durability guarantees

Authorization reads the bucket record in the same metadata snapshot as
the operation's own transaction where one exists (PUT commit, DELETE), so
a policy change and a write are ordered by FoundationDB's serializability;
for streaming GET the policy is checked at request start and not
re-evaluated mid-stream. No control changes Durable ACK, atomic
visibility, epoch fencing or GC safety.

## 9. Performance considerations

- SigV4: already paid today (s3s). Hypothesis: < 2 % of gateway CPU per
  request at 16 MiB objects.
- Edge TLS: measured on the transfer bench as +0.5–0.7 CPU-s/GiB
  (RFC-009 §6.11); the same order is expected for gRPC mTLS on the
  cluster network, on both ends. This is the one control with a
  measurable data-path cost; the regression check (§6.19) will carry a
  TLS-on configuration.
- Policy evaluation: the bucket record is already read per request; the
  parsed policy is cached per gateway keyed by bucket id and map version.
- Audit: one JSON line per request; asynchronous writer with a bounded
  queue (drops counted).

## 10. Open questions

- Should `ListBuckets` for a principal with policy grants on others'
  buckets include them? (Proposal: no; only owned buckets, as AWS.)
- Presigned URLs and bucket policies: evaluate the policy for the signing
  principal at request time (proposal) or at signing time (not
  possible: the gateway does not see the signing).
- Certificate issuance in the lab: ship a tiny `arca-lake admin
  issue-cert` built on `rcgen` for development clusters (proposal: yes,
  clearly marked as not for production).

## 11. Acceptance criteria

Labels as RFC-007 §11 (Designed / Implemented / Verified / Not
Implemented / Not Verified); Designed never implies Implemented.

| # | Criterion | Status |
| --- | --- | --- |
| S-A1 | A request signed with an unknown or inactive key is refused (`InvalidAccessKeyId`) and audited; two active keys per principal both sign during a rotation; removing the old key refuses it | Implemented; Verified at the unit level (`auth.rs` tests: inactive, expired, unknown, rotation by file rewrite) and over HTTP for unknown keys (`s3_compat.rs`) |
| S-A2 | Credentials file reload swaps the key set atomically; an invalid file keeps the previous set and counts a failure | Implemented, Verified (unit; `reload_failures` counter) |
| S-A3 | Default deny: a principal cannot head, list, read, write or delete another principal's bucket without a policy; `AccessDenied`, not `NoSuchBucket` | Implemented, Verified (`s3_compat.rs::authorization_rules_over_http`) |
| S-A4 | Explicit Deny beats Allow, for the owner too; the owner keeps `*BucketPolicy` | Implemented, Verified (HTTP) |
| S-A5 | `PutBucketPolicy` rejects every unsupported element with `MalformedPolicy`; nothing unknown is stored | Implemented, Verified (unit grammar tests; HTTP) |
| S-A6 | A corrupt stored policy denies non-owners and lets the owner replace it | Implemented, Verified (`security.rs::corrupt_stored_policy_…`: a document corrupted in metadata denies tenant-b and the owner's data access, the owner reads, replaces and deletes it; unsupported elements are refused with `MalformedPolicy` and the stored document is untouched) |
| S-A7 | Prefix-scoped Allow, `s3:prefix` and `aws:SourceIp` conditions; Deny monotonic | Implemented, Verified (unit incl. the monotonicity property; HTTP for prefix) |
| S-A8 | Anonymous mode: without a policy allowing `*`, anonymous requests are refused | Implemented, Verified (HTTP: 403 before, 200 after a `Principal: "*"` statement, 403 outside it) |
| S-A9 | Audit: one record per request with principal, key id, action, bucket, decision, status; no secret material | Implemented, Verified (`security.rs::audit_log_has_one_record_per_request_and_no_secret_material`: captures the `arca.audit` stream over a mixed sequence incl. a refused key and an anonymous request, asserts one record per request with unique ids and the designed fields and decisions, and scans every record for every secret key, SigV4 signature material and authorization headers). Since Phase 3.2 the record carries the claimed access key id even when the key is refused, and `anonymous` for unsigned requests |
| S-A10 | Gateway/node boundary: nodes never read bucket records or policies | Verified by construction (nodes hold no metadata client; code review) |
| S-A11 | mTLS and role-scoped RPC authorization | Implemented, Verified (Phase 3.4): `storage-node/tests/mtls.rs` (every RPC by role; wrong CA, expired, revoked, impersonating and plaintext peers refused at the handshake; leaf renewal, CA rotation with two roots, root withdrawal closing connections, CRL revocation, invalid reload kept out, all under traffic with every acknowledged PUT read back), `testkit/tests/tls_cluster.rs` (controllers over mTLS, leader failover, an authenticated stale incarnation refused, a gateway identity cannot move epochs, rotation under S3 traffic), the end-to-end suite over mTLS (`e2e_tls.rs`, 18 tests), the 24 h soak over mTLS and FoundationDB TLS |
| S-A12 | Rate limits and in-flight budgets with `503 SlowDown` | Designed (Phase 3.3) |
| S-A13 | TLS performance regression | Measured for metadata (Phase 3.4): FoundationDB TLS costs 6–8 % of metadata transaction throughput at 8 concurrent tasks and 12–14 % at 64, p50 +0.1–0.9 ms, first transaction 4 → 8 ms (`benchmarks/linux/results/phase34-20261103/fdb-tls`); node-RPC mTLS data-path cost not measured separately (RFC-009 §6.11 measured +0.5–0.7 CPU-s/GiB for TLS on the transfer path) |
| S-A14 | CopyObject and UploadPartCopy are authorized on the source bucket (`s3:GetObject[Version]`) as well as the destination | Implemented, Verified (found by the HTTP test during Phase 3.1) |
