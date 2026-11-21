# Public CI security review (Phase 3.7)

> **Historical Phase 3.7 snapshot.** Findings, counts, pass results, and
> setting descriptions below apply to that review. They are not a current
> publication clearance. Follow [public repository readiness](public-readiness.md)
> for the remaining work and recheck the actual publication candidate.

## Current arrangement (2026-11-19)

The proposal below was carried out at publication: the self-hosted runners
were removed (proposal 1) and every job moved to GitHub-hosted
`ubuntu-24.04` runners (proposal 4, in the existing workflow rather than a
second one; proposal 3 is moot without self-hosted runners).

- Triggers: `push` to `main`, `pull_request` to `main`, and
  `workflow_dispatch`. Token `contents: read`; no secrets; actions pinned
  to commit SHAs; first-time contributors' runs need a maintainer's
  approval.
- Jobs on every trigger, in parallel: format, lint and tests; the
  FoundationDB backend tests and restore drill; the restore drill on a
  three-process FoundationDB cluster; the three-process failover and
  membership suites; licenses, history and tree secret scans, environment
  identifiers, sign-off and Markdown links. The first hosted run took
  about 14 minutes (its longest job); line-table debug information, a
  dependency cache refreshed by pushes to `main` and the split of the
  three-process jobs bring a pull request's run to about 7–9 minutes.
- The upgrade jobs run on pushes to `main` and on demand, not on pull
  requests, side by side, each on its own FoundationDB container:
  `fdb-mixed-version` (this tree and a version-bumped build of it) and
  `fdb-upgrade` (upgrades from and rollbacks to earlier releases, whose
  binaries are built once per set of release commits and cached:
  binaries only, debug information stripped, checked against a sha256
  manifest after a restore).
- Pull requests are merged by rebase or squash (merge commits disabled);
  the sign-off check skips merge commits.

The repository is private today. This page reviews what changes when it
becomes public and proposes the workflow and settings changes to make
first. **None of the setting changes below has been applied**; they need
the maintainer's decision.

## State at the Phase 3.7 review

- One workflow (`.github/workflows/ci.yml`), triggered only by `push` to
  `main`; every job also checks `github.event_name == 'push'` and the
  ref. No `pull_request` or `pull_request_target` trigger. Permissions
  `contents: read`; no secrets besides `GITHUB_TOKEN`; actions pinned to
  commit SHAs; tools downloaded by pinned version and checksum.
- Every job runs on **repository-level self-hosted runners** (three
  long-lived Linux runners on one host; labels `self-hosted, Linux, X64`).
  The runner user can use `sudo` (package installs, `docker`).
- GitHub-hosted runners have not been usable by the account (billing),
  so there is no hosted CI path today.

## What a public repository changes

1. **Fork pull requests choose their own workflows.** For `pull_request`
   events GitHub runs the workflow files of the pull request's merge
   commit. A fork can add a workflow with `runs-on: self-hosted`; the
   existing `on: push` restriction of `ci.yml` does not prevent that.
   Whether it runs depends only on the repository's *fork pull request
   approval* setting and on whether self-hosted runners accept jobs
   from it. A job on a long-lived runner with `sudo` can persist on the
   host, read caches (`~/.cache/arca-lake`, the cargo target directory)
   and reach whatever the host reaches.
2. **Logs and artifacts become public.** Existing workflow run logs show
   runner paths and the runner names; one artifact
   (`gitleaks-results.sarif`, redacted findings of 2026-10-31) exists.
3. **Issues, discussions and the security policy** become the intake
   channel (SECURITY.md describes private reporting).

## Proposal (to apply before the repository is made public)

| # | Change | Where |
| --- | --- | --- |
| 1 | Remove the repository-level self-hosted runners from the public repository, or move them to a runner group that is not available to public repositories; never attach a long-lived runner with `sudo` to a public repository | repository / account settings |
| 2 | Settings → Actions → General: "Require approval for all external contributors" for fork pull request workflows; "Read repository contents" default token permissions | repository settings |
| 3 | Keep the self-hosted workflow on `push` to `main` only, in a private mirror (or in this repository only while it stays private); maintainers push there | workflow layout |
| 4 | Add a GitHub-hosted workflow for `pull_request` and `push` (below) once hosted runners are available; until then, pull requests get **no CI**, and a maintainer reviews and pushes them to `main` (the self-hosted CI then runs on the maintainer's push) | new workflow |
| 5 | Delete old workflow runs and the existing artifact, or accept that their logs (runner paths and names) become public | repository (destructive: needs approval) |
| 6 | Runners: ephemeral (one job per runner, rebuilt), no `sudo` for the runner user (prepared image instead of the install steps), no credentials or network access to other hosts | runner hosts |

Proposed hosted workflow (not committed as an active workflow: it would
fail on every push while hosted runners are unavailable):

```yaml
name: CI (hosted)
on:
  pull_request:
  push:
    branches: [main]
permissions:
  contents: read
jobs:
  check:
    runs-on: ubuntu-24.04
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@<pinned sha>
      - uses: dtolnay/rust-toolchain@<pinned sha>
        with: { toolchain: "1.99", components: "rustfmt, clippy" }
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets
      - run: cargo test --workspace   # in-memory metadata; no FoundationDB
      - run: python3 -I benchmarks/linux/sanitize.py --check benchmarks/linux/results benchmarks/reports docs
```

Conditions under which CI does not run: no hosted minutes (pull requests
and pushes get no hosted run); self-hosted runners offline (pushes to
`main` wait); after change 1, the FoundationDB, failover and
mixed-version jobs exist only on the self-hosted path.
