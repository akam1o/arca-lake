# RFC-012: Security Architecture

- Status: draft (Phase 3.0; implementation in Phase 3.1–3.3 as noted per control)
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
| S5 | mTLS on gRPC with role- and node-scoped authorization of RPCs | Open (§5.5) |
| S6 | FoundationDB connections use TLS with client certificates in production | Assumption (FoundationDB supports it; deployment guide) |
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

- `BucketRecord` gains `owner: String` (principal name) and
  `policy: Option<Vec<u8>>` (the policy document as stored). Record
  version 2 with a v1 decoder (RFC-014 §5.3): v1 buckets decode with
  `owner = "legacy"`, which only the operator's `admin` tooling can
  reassign; until reassigned, the policy evaluation treats the bucket as
  owned by nobody and refuses tenant access (fail closed).
- **Evaluation order** (per request, in `S3Access::check`):
  1. the operation's required action is derived from the s3s op name
     (`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`,
     `s3:CreateBucket`, `s3:DeleteBucket`, `s3:PutBucketPolicy`, …);
  2. `CreateBucket` and `ListBuckets` are principal-level: allowed for any
     authenticated principal (the bucket becomes theirs); `ListBuckets`
     returns only buckets the principal owns or has `s3:ListBucket` on;
  3. for bucket-scoped actions: the owner is allowed everything; otherwise
     the bucket policy is evaluated: any `Deny` matching wins, then any
     `Allow`, else deny;
  4. the result and the decisive statement id are put in the audit record.
- **Policy grammar** (JSON, AWS shape, documented subset):
  `Version`, `Statement[]` with `Sid`, `Effect` (`Allow|Deny`),
  `Principal` (`"*"`, or `{"AWS": ["arca:principal/<name>", …]}`),
  `Action` (string or list, `s3:*` and prefix wildcards),
  `Resource` (`arn:aws:s3:::bucket` and `arn:aws:s3:::bucket/<prefix>*`),
  `Condition` with only `StringEquals`/`StringLike` on `s3:prefix` and
  `IpAddress`/`NotIpAddress` on `aws:SourceIp`. Any other condition key or
  element → the policy is rejected at `PutBucketPolicy` with
  `MalformedPolicy`; nothing unknown is silently ignored.
- **Tenant isolation** is the consequence: a principal's requests only
  ever touch buckets it owns or is granted; keys, generations and cache
  placements carry the bucket id, so nothing below the bucket needs a
  tenant dimension. Per-tenant quotas are a RFC-013 item (capacity
  counters per bucket already exist for cache; durable usage per bucket is
  added with multipart accounting in RFC-007).
- Alternatives: object ACLs (rejected: AWS itself recommends policies;
  per-object metadata on every GET costs a read); a separate authorization
  service (rejected for the OSS baseline: one more process, and the
  policy is small enough to evaluate in the gateway from a cached bucket
  record, which the gateway already reads for every request).

### 5.4 C-2 edge TLS

The gateway serves HTTPS directly (`--tls-cert`, `--tls-key`, rustls,
HTTP/1.1 and HTTP/2) or sits behind a TLS-terminating proxy. With a
proxy, `--trusted-proxies` enables `X-Forwarded-For` for the audit record
and rate limiting; without it the header is ignored. Plain HTTP stays
available for the lab and is logged at startup as insecure.

### 5.5 C-9 internal mTLS and service identity

- Every gRPC listener (storage node) and client (gateway, controller)
  uses `tonic` with `rustls`: server certificate, client certificate
  required, one private CA per cluster (or the platform's), optional
  second root for rotation. Certificates are files; reload on `SIGHUP`.
- **Identity**: the certificate's SAN carries a URI
  `arca://<cluster-id>/<role>/<name>`: roles `node`, `gateway`,
  `controller`, `admin`; `name` is the node id for nodes. The server
  extracts it with a tonic interceptor and attaches `Peer { role, name }`
  to the request.
- **Authorization of RPCs** on the node:

  | RPC | gateway | controller | node | admin |
  | --- | --- | --- | --- | --- |
  | `PutShard`, `GetShard`, `ListShards`, `VerifyShards` | yes | yes | yes (repair reads) | yes |
  | `DeleteShard` | yes (own placements; epoch-checked as today) | yes | no | yes |
  | `UpdateGroupEpoch` | no | yes | no | yes |
  | `Health`, `Scrub` (Phase 3.2) | yes | yes | yes | yes |

  A gateway's identity cannot fence or re-member a group (the controller
  does that with its own identity), which bounds a compromised gateway
  to the data path.
- **Client-side verification**: when a gateway or controller connects to
  node `n` at the address in `NodeRecord`, it requires the server SAN
  `arca://<cluster-id>/node/<n>`; an address serving another identity is
  refused (spoof resistance without trusting the network).
- **FoundationDB**: the deployment enables FoundationDB's TLS with client
  certificates and a verify-peers string; the cluster file is a secret
  (RFC-013 §5). arca-lake passes the TLS options through the environment
  FoundationDB already honours.
- **Lab mode**: `--insecure-plaintext` keeps today's behaviour for the
  namespace and two-host harnesses and is refused when a certificate flag
  is also given; it is logged at startup.
- Alternatives: SPIFFE/SPIRE (compatible: the SAN format follows SPIFFE's
  shape so an SPIRE deployment can issue these certificates; not
  required); IPsec/WireGuard at the platform (compatible, but gives no
  per-RPC role; kept as defence in depth, not as the control); token-based
  RPC auth (rejected: needs a secret distribution channel that mTLS
  already provides).

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

- A request signed with an inactive or unknown key is refused and
  audited; rotation with two active keys passes a test that signs with
  both during the overlap.
- A principal cannot list, read, write or delete another principal's
  bucket without a policy; with an `Allow` on a prefix it can read only
  that prefix; a `Deny` wins over `Allow`; unknown policy elements are
  rejected at `PutBucketPolicy`.
- Anonymous mode: without a bucket policy allowing `*`, anonymous requests
  are refused.
- A gRPC connection without a client certificate, or with a certificate
  of role `gateway`, cannot call `UpdateGroupEpoch`; a node presenting
  another node's name is refused by the client.
- Audit records exist for every S3 request in the conformance suite and
  contain no secret material (asserted by a scan of the log).
- Rate-limited and over-budget requests return `503 SlowDown` with
  `Retry-After`, and the in-flight budget is never exceeded under a load
  test with 2× the configured concurrency.
- The performance regression check with TLS on both edges reports CPU
  per GiB within the stated hypothesis or the number is recorded.
