# Preparing the repository for public access

arca-lake can be published as an experimental project while development
continues. Repository visibility, a versioned software release, and a
production-readiness claim are separate decisions. This page records the
repository-publication work; it does not declare a release.

Status: **the repository was made public on 2026-11-19** as an
experimental project ([publication record](#publication-record-2026-11-19)).
No tag or release exists.

## Publication record (2026-11-19)

| Item | Disposition |
| --- | --- |
| Published commit | `main` at [`55f25621d924ac445218a11bbd35e53ccfcebff1`](https://github.com/akam1o/arca-lake/tree/55f25621d924ac445218a11bbd35e53ccfcebff1) (the documentation cleanup, fast-forwarded from its pull request; author, committer and `Signed-off-by` the project's no-reply identity) |
| Validation | the [push workflow for that commit](https://github.com/akam1o/arca-lake/actions/runs/44469503413) succeeded: format, lint and tests, the FoundationDB backend and failover suites, licenses, history and tree secret scans, environment-identifier check, DCO and Markdown links. Its first attempt failed one test on a 10-second wait (`cache::population_failure_never_fails_the_get`); the commit changed documentation only, the test passed 10 of 10 runs locally and had not failed in the 60 earlier runs, and the rerun of the job passed: classified as timing, not a regression |
| Actions and runners | GitHub Actions disabled for the repository before publication; the three repository-scoped self-hosted runners deregistered, and their services and installations removed from their hosts. No runner is registered |
| Workflow logs and artifacts | 314 workflow runs: the logs of 312 were downloaded and scanned (2 could not be retrieved and are not inspected), each deleted after its scan. Within that scope: 0 tokens, keys or other credentials, 0 organisation or personal identifiers, 0 addresses of the test network. Present and accepted for publication: runner and runner-host names, runner work paths, container-network addresses of the CI's FoundationDB, and the standard work path of GitHub's hosted macOS runner. One artifact remains (a secret scan's result, with no findings) |
| Replaced historical commit | the possibility that the replaced commit of the [incident record](../security/public-artifacts.md#record) remains retrievable by its hash is accepted: it holds a test host name, not a credential |
| Vulnerability reporting | GitHub private vulnerability reporting enabled, the route [SECURITY](../../SECURITY.md) names |
| Fork pull requests | workflows of first-time contributors need a maintainer's approval (GitHub's default) |

After publication, CI moved to GitHub-hosted runners and Actions was
enabled again ([current arrangement](public-ci.md#current-arrangement-2026-11-19)).

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

## Before changing repository visibility (checklist used)

The checklist the publication followed; each item's disposition is in the
[publication record](#publication-record-2026-11-19).

| Item | Required follow-up | Evidence or policy |
| --- | --- | --- |
| Candidate tree and history | Recheck the candidate tree, commit messages, identities, and all refs intended to be published for credentials and non-public information. Review benchmark data with the sanitizer and the private identifier list where available. | [Public repository policy](../CONTRIBUTING-POLICY.md), [public artifact policy](../security/public-artifacts.md) |
| Public CI and runners | Verify the current runner inventory, runner access, fork workflow approval rules, and token permissions; apply the chosen public CI arrangement before exposing runners to public contributions. The checked-in workflow still targets self-hosted runners on pushes to `main`; that alone does not establish the safety of repository settings. | [Workflow](../../.github/workflows/ci.yml), [CI review and proposal](public-ci.md) |
| Existing workflow logs and artifacts | Inventory what still exists, review what would become visible, and decide whether to retain or remove it. The historical audit identified runner identifiers in logs and a scan artifact; their current presence and disposition have not been rechecked here. | [Historical audit, findings 5 and 9](public-repository-audit.md#findings) |
| Replaced historical commit | Resolve the recorded possibility that a replaced commit remains retrievable by hash: obtain confirmation of removal where needed, or explicitly accept the residual exposure. Removing a file from the current tree does not resolve this item. | [Public artifact incident record](../security/public-artifacts.md#record), [historical audit, finding 4](public-repository-audit.md#findings) |
| Reader and contributor entry points | Review the README, quick start, compatibility matrix, limitations, contributing instructions, license, and security reporting route at the candidate commit. Confirm that the private reporting route described in `SECURITY.md` is available. | [Documentation index](../README.md), [CONTRIBUTING](../../CONTRIBUTING.md), [LICENSE](../../LICENSE), [SECURITY](../../SECURITY.md) |
| Validation claims | Link the candidate's actual check results and record skipped, failed, or unexecuted checks. Keep unresolved findings and measurement conditions visible. | [Known limitations](../guide/known-limitations.md), [risk register](../architecture/risks.md), [latest access-record verification](../verification/phase-4.1-adr34.md) |

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
