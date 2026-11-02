# ADR-0014: Phase 3.1 decisions — hierarchical multipart manifest, authorization rules, compatibility axes, cluster recovery states

Status: accepted (Phase 3.0 review, conditional approval; reflected into
RFC-007, RFC-012, RFC-014, RFC-015 on 2026-11-02)

## Context

The Phase 3.0 design review approved the direction with conditions:
multipart completion must not copy chunk manifests into one
transaction; the authorization rules must be explicit; compatibility
must be split into independent axes; restore safety must be a cluster
state machine rather than a flag; acceptance criteria must distinguish
Designed / Implemented / Verified. This record fixes the decisions that
the implementation of Phase 3.1 depends on.

## Decisions

| Decision | Chosen | Rejected (reason) |
| --- | --- | --- |
| Multipart manifest | hierarchical: parts are immutable generations with their own chunk manifests; Complete writes a root header plus one reference key per part (`("al","g",root,"m",n)`), never a chunk record; GET resolves part manifests lazily per part | copying chunk records into the root (transaction size unbounded in chunk count); one reference blob in the header (100 KB value limit at ~1 500 parts) |
| Complete transaction | exactly one transaction, the linearization point; root id chosen before the first attempt for idempotent retry; unlisted parts retired afterwards by the gateway and the controller sweep | retiring unlisted parts in the same transaction (header rewrites carry the object key: 10 000 × 1 KiB keys exceed the limit) |
| Abort and root finish | batched transactions behind a terminal state (`Aborting` for uploads, `Retiring` for roots) with a state re-check per batch; the controller sweep finishes what a dead gateway left | single transaction (size); lease-expiry only (parts are committed, not pending) |
| Part liveness | invariant I-MP1: a part is live iff its upload is `Open`/`Aborting`, or `Completed { root }` with the root's reference naming it and the root header present; only four transitions retire a part, each re-reading the record; GC's verify step enforces it again | relying on `Committed` alone (would need per-part reference counts) |
| Record versioning | record version 2 (bucket owner/policy, generation kind/checksum, new multipart records); decoders for version 1; schema key written on new clusters and stamped on first contact with Phase 2 data | re-encoding every record at upgrade (RFC-014 §5.4 lazy rule) |
| Authorization | nine normative rules (RFC-012 §5.3.2): default deny, explicit deny wins (owner included, except `*BucketPolicy` for the owner), owner rights, registration-time rejection of unsupported elements, evaluation errors deny, no tenant leakage, separate gateway/node boundaries, service identity + role authorization for RPCs; documented AWS JSON subset | permissive evaluator that ignores unknown elements (silent privilege escalation on a typo) |
| Compatibility axes | wire protocol `P`, metadata schema `M`, storage format `F`, each adjacent-version; `schema_write` and `format_write` keys as the write gates raised only by an idempotent, cursor-driven finalize | one version number for everything |
| Cluster recovery | state machine `Active / ReadOnly / Restoring / VerificationPending / RecoveryRequired` with an incarnation id in `("al","meta","cluster")`, read in every mutating transaction; no client PUT outside `Active`; no deletion outside `Active` | boolean `verification_pending` + `read_only` flags (Phase 3.0 draft) |
| Acceptance criteria | every RFC-007/012/013/014/015 criterion carries a status label, updated per phase report | prose criteria (ambiguous about what was tested) |
| Mixed-version tests | a test-only version-bumped build of `HEAD` until a release tag exists | waiting for a tag (blocks RFC-014 verification) |

## Consequences

- `GenerationHeader` gains `kind` and `checksum`; `BucketRecord` gains
  `owner` and `policy`; new records `MultipartRecord`, `PartRecord`,
  `PartRef`; key spaces `mpu`, `mpp`, `("al","g",root,"m",n)`,
  `("al","meta","schema")`, `("al","meta","cluster")`.
- GC gains a multipart sweep and batched root finish; `verify_unreferenced`
  gains the part liveness check.
- The gateway's manifest reader resolves part manifests lazily; the
  write pipeline is shared by PUT, UploadPart and CopyObject.
- Phase 3.1 implements RFC-007 §5.3–5.8 and RFC-012 §5.1–5.3, §5.7
  (file-backed keys, owner, policy subset, audit); RFC-014 §5.4
  (schema key and v2 decoders) as a prerequisite; RFC-015's cluster
  state record if time allows, otherwise in Phase 3.3.
