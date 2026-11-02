# ADR-0013: Phase 3.0 design decisions (security, S3 compatibility, upgrade, backup)

Status: proposed (Phase 3.0; becomes accepted when the design review of
RFC-007, RFC-012, RFC-013, RFC-014 and RFC-015 closes)

## Context

Phase 3 hardens the system for production: security, S3 compatibility,
rolling upgrades, backup and restore, operations. The RFCs above hold
the designs; this record lists the decisions that cut across them and
why the alternatives lost. Each row was weighed on performance,
consistency, durability, operational load and security.

## Decisions

| Decision | Chosen | Alternatives rejected (reason) |
| --- | --- | --- |
| S3 authentication | SigV4 as implemented by `s3s`; arca-lake supplies only the key store (`S3Auth`) | own signature verification (no reason to re-implement; audit surface); bearer tokens (S3 clients do not send them) |
| Authorization model | bucket owner + bucket-policy subset (AWS JSON grammar, unknown elements rejected), evaluated in the gateway from the bucket record | IAM clone (size; most deployments need per-bucket grants only); object ACLs (per-object read cost; AWS discourages); external policy service (another process on the request path) |
| Tenant isolation | the bucket is the tenant boundary: `owner` on the bucket record, every lower key carries the bucket id | tenant prefix in every key (migration of the whole layout for no isolation gain) |
| Credential storage | reloadable file with two active keys per principal; nothing in FoundationDB | keys in metadata (secret in backups and in every process holding the cluster file); KMS-only (deployment-specific; may feed the file) |
| Service identity | mTLS on every gRPC connection with a SPIFFE-shaped SAN (`arca://<cluster>/<role>/<name>`), role-scoped RPC authorization on nodes | network isolation alone (no per-RPC authorization; R-19); shared tokens (distribution problem solved worse than by certificates) |
| Audit | one structured record per S3 request on a dedicated tracing target; controller mutations likewise | database table (volume, coupling); access logs only (no principal/decision fields) |
| Resource protection | explicit budgets and token buckets in the gateway and node, `503 SlowDown` never blocking | proxy-only limits (cannot distinguish principals); unbounded (today) |
| Multipart completion | adopt part generations into the object header (one transaction, size-independent) | copy shards (2× writes); multi-transaction rewrite (not atomic) |
| CopyObject | physical copy through the read and write paths | shared chunks with reference counts (GC safety complexity) |
| Checksums | AWS composite rules per algorithm family; ETag stays MD5/composite | full-object only (SDK incompatibility for multipart) |
| Compatibility window | adjacent versions; nodes → controllers → gateways; explicit `finalize` gates new persistent formats and migrations | any-to-any (untestable); stop-the-world (today) |
| Protocol versioning | window advertised by nodes in `Health`, negotiated by clients; additive changes do not bump | strict equality (today); feature flags only (semantic changes uncovered) |
| Metadata migration | lazy decoders, rewrite-on-write in the allowed version, resumable fenced migrations, layout key written and checked | eager rewrite (long; immediate downgrade cut-off) |
| Backup unit | FoundationDB continuous backup; nodes are live, not backed up | node snapshots (2× capacity, still time-inconsistent) |
| Post-restore safety | `verification_pending` gate checked in every deleting transaction; unreadable objects marked and refused | rely on orphan age (post-`T` orphans would be deleted unseen); silent 404 for lost objects |
| Admin privilege | cluster file until mTLS identities exist, then an `admin` role certificate | separate admin API first (one more privilege model to secure before identities exist) |
| CI for a public repository | self-hosted runners run `main` pushes by maintainers only; fork PRs on GitHub-hosted or after a maintainer push; actions pinned; read-only token | `pull_request_target` on self-hosted (untrusted code on persistent runners) |

## Consequences

- RFC-003 and RFC-002 gain record version 2 for buckets (owner, policy,
  lifecycle, tags) and the `mpu` key space; RFC-005 gains the format
  write gate; RFC-008 gains the `verification_pending` and `read_only`
  flags; RFC-011 gains migrations, scrub, lifecycle and the restore
  verification as scheduled tasks.
- Implementation order (roadmap Phase 3.1–3.4): compatibility window
  and layout gate first (every later change needs them), then multipart
  and versioning, then security controls, then scrub, then restore
  drills and soak.
- Measured costs to track: TLS on both edges (RFC-009 §6.11 order),
  SHA-256 checksums, policy evaluation; the regression check gains a
  TLS-on configuration.
