# Preparing the repository for public access

arca-lake can be published as an experimental project while development
continues. Repository visibility, a versioned software release, and a
production-readiness claim are separate decisions. This page tracks the
repository-publication work; it does not declare a release or change any
GitHub setting.

## Evidence and scope

The documentation review started from
[`ab19a586725d72df300fd02b1418c222695dc33b`](https://github.com/akam1o/arca-lake/tree/ab19a586725d72df300fd02b1418c222695dc33b)
on **2026-11-19**. The
[push workflow for that commit](https://github.com/akam1o/arca-lake/actions/runs/44458654525)
completed successfully. That result applies to that commit and run, not
to a later publication candidate or to repository settings.

The documentation cleanup aligns the reader-facing guides and indexes
with the implemented behavior, identifies historical plans, and preserves
design decisions and verification evidence. Its scope is documentation
accuracy and organization. It does not repeat a complete history or
GitHub artifact audit, execute the runtime test suite, or verify the
current runner inventory and Actions settings.

The [Phase 3.7 audit](public-repository-audit.md),
[CI review](public-ci.md), and
[experimental release checklist](experimental-checklist.md) are historical
snapshots. Their findings remain useful inputs; their old pass results,
object counts, and setting descriptions are not current attestations.

## Before changing repository visibility

Record the candidate commit and the outcome of these checks at publication
time. The follow-up items below have not been completed by the documentation
cleanup.

| Item | Required follow-up | Evidence or policy |
| --- | --- | --- |
| Candidate tree and history | Recheck the candidate tree, commit messages, identities, and all refs intended to be published for credentials and non-public information. Review benchmark data with the sanitizer and the private identifier list where available. | [Public repository policy](../CONTRIBUTING-POLICY.md), [public artifact policy](../security/public-artifacts.md) |
| Public CI and runners | Verify the current runner inventory, runner access, fork workflow approval rules, and token permissions; apply the chosen public CI arrangement before exposing runners to public contributions. The checked-in workflow still targets self-hosted runners on pushes to `main`; that alone does not establish the safety of repository settings. | [Workflow](../../.github/workflows/ci.yml), [CI review and proposal](public-ci.md) |
| Existing workflow logs and artifacts | Inventory what still exists, review what would become visible, and decide whether to retain or remove it. The historical audit identified runner identifiers in logs and a scan artifact; their current presence and disposition have not been rechecked here. | [Historical audit, findings 5 and 9](public-repository-audit.md#findings) |
| Replaced historical commit | Resolve the recorded possibility that a replaced commit remains retrievable by hash: obtain confirmation of removal where needed, or explicitly accept the residual exposure. Removing a file from the current tree does not resolve this item. | [Public artifact incident record](../security/public-artifacts.md#record), [historical audit, finding 4](public-repository-audit.md#findings) |
| Reader and contributor entry points | Review the README, quick start, compatibility matrix, limitations, contributing instructions, license, and security reporting route at the candidate commit. Confirm that the private reporting route described in `SECURITY.md` is available. | [Documentation index](../README.md), [CONTRIBUTING](../../CONTRIBUTING.md), [LICENSE](../../LICENSE), [SECURITY](../../SECURITY.md) |
| Validation claims | Link the candidate's actual check results and record skipped, failed, or unexecuted checks. Keep unresolved findings and measurement conditions visible. | [Known limitations](../guide/known-limitations.md), [risk register](../architecture/risks.md), [latest access-record verification](../verification/phase-4.1-adr34.md) |

Reviewing the evidence and preparing changes is distinct from deleting
GitHub records or changing visibility. Record the maintainer's disposition
of the outstanding publication items when those actions are taken.

## A subsequent software release

Creating a tag, GitHub release, or container image requires a release
candidate, its validation evidence, and release notes that state the
compatibility window and known limitations. Those artifacts are not
prerequisites for making the experimental source repository public.

Continue to describe unverified long-duration operation, physical-device
failure behavior, and security-review limits as documented in
[known limitations](../guide/known-limitations.md). Publishing the source
does not establish those properties. The
[active roadmap](../roadmap/README.md) tracks the remaining engineering
work.
