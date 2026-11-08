# Experimental release candidate checklist

State at the end of Phase 3.7. **No release, tag, image or publication
has been made**; the items under "needs approval" are the maintainer's.

| Item | Status | Evidence |
| --- | --- | --- |
| Public artifact scan (history and tree) | Done; no credential or environment identifier found | [public-repository-audit.md](public-repository-audit.md) |
| Dependency licenses, advisories, banned crates | Passing on every push | cargo-deny job (`deny.toml`) |
| Secrets scan | Passing on every push (history + tree) | gitleaks job |
| Security review | Threat model and RFC-012 controls verified by tests; cross-model reviews per phase; **no external audit** | `docs/security/threat-model.md`, `security.rs` |
| CI | Green on the final commit (self-hosted runners) | GitHub Actions |
| DCO | Every commit signed off (checked in CI) | DCO step |
| Golden format compatibility | Every metadata record's encoding fixed, including the Phase 3.6/3.7 records | `golden_records.rs` |
| S3 compatibility | Conformance suite (Rust SDK, AWS CLI v2, boto3) | RFC-007 §11 |
| FoundationDB failover | Three-process failover test in CI | `fdb_failover.rs` |
| Recovery drill | Backup/restore and recovery in CI | `fdb_restore.rs`, RFC-015 |
| Capacity safety | Verified by tests and short runs; long-duration Not Verified | ADR-0020/0021, `docs/verification/phase-3.6-capacity.md`, Phase 3.7 report |
| Documentation | README, guides (quick start, platforms, S3 compatibility, configuration, operations, security model, backup/restore, troubleshooting, known limitations), CONTRIBUTING, SECURITY | `docs/guide/` |
| Known risk disclosure | Risk register current | `docs/architecture/risks.md` |
| Public CI security | Review and proposal written; **settings not changed** | [public-ci.md](public-ci.md) |

## Needs approval before publishing

1. Runner and Actions settings of [public-ci.md](public-ci.md) (remove or
   restrict self-hosted runners, fork approval, hosted workflow).
2. Delete old workflow runs and the `gitleaks-results.sarif` artifact,
   or accept their publication.
3. Ask GitHub support to purge unreachable objects (one replaced commit
   of 2026-11-01), or accept the residual exposure.
4. Make the repository public; tag; create a GitHub release with the
   known limitations; announce.

## Blocking for a production-ready claim (not for an experimental one)

- Long-duration operation (≥ 24 h) under load with faults on storage
  that does not stall FoundationDB (R-79).
- Physical devices, real disk failures during exhaustion.
- External security review.
