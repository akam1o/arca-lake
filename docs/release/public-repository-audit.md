# Public repository audit (Phase 3.7)

Audit of what would become public with the repository. Run on the
working tree and on every commit reachable from any ref (241 commits,
one author identity, `akam1o <akam1o@users.noreply.github.com>`).
Nothing was rewritten or deleted.

## Method

| Check | Tool | Scope |
| --- | --- | --- |
| Credentials, keys, tokens | gitleaks 8.30.1 (CI, every push: full history and working tree); `git log --all -p` searched for private key blocks and access key/token patterns | history + tree |
| Environment identifiers (addresses, user-at-host, home paths, MACs, interface names) | `benchmarks/linux/sanitize.py --check` over every tracked file; `git log --all -p` and commit messages searched for private address ranges, home directories, MAC addresses, non-noreply email addresses | history + tree |
| Licenses of dependencies, advisories, banned crates | cargo-deny (CI, every push) | `Cargo.lock` |
| Project license | `LICENSE` (Apache-2.0), ADR-0001; DCO sign-off on every commit (CI) | tree |
| GitHub objects | `gh api`: releases, packages, artifacts, runners, issues, discussions | repository |

## Findings

| # | Finding | Assessment | Action |
| --- | --- | --- | --- |
| 1 | No credential, private key or token in history or tree. Test fixtures carry obviously fake access keys (`AKIAMIXEDVERSION00000`, `AKIATENANTB0000000000`) and a fixed fake secret in the soak scripts | Acceptable (documented test values) | none |
| 2 | The sanitizer flags 43 lines of the tree: action SHA pins (`uses: …@<sha>`), `user@host` placeholders in script usage text, documentation and loopback addresses in policy and redaction tests (`10.0.0.0/8`, `2001:db8::/32`, `192.168.1.1`), `e2e::` read as IPv6 | False positives: none is a real host, address or user | none |
| 3 | History: no validation host address, no home directory, no MAC, no email other than the noreply address | Clean | none |
| 4 | 2026-11-01: a commit with validation host names in measurement headers was pushed and replaced within a minute (recorded in `docs/security/public-artifacts.md`) | The replaced commit is unreachable but may remain fetchable by hash on GitHub | Before publishing: ask GitHub support to purge unreachable objects, or accept the residual exposure (maintainer decision) |
| 5 | Workflow run logs (203 runs) show runner names and runner work paths; one artifact `gitleaks-results.sarif` (2026-10-31, redacted findings) | Public with the repository | Delete runs and the artifact before publishing, or accept (destructive: needs approval) |
| 6 | Releases: none; packages/container images: none; discussions disabled; issues enabled, none open | Nothing to clean | none |
| 7 | Benchmark results (`benchmarks/linux/results`, 80 MiB) and reports | Sanitized by the measurement scripts and checked in CI | none |
| 8 | Performance figures in the RFCs and verification reports come from the validation VMs and are described as such; no non-public performance requirement is stated | Acceptable | none |
| 9 | Self-hosted runner configuration is not in the repository (only the workflow) | See [public CI review](public-ci.md) | settings changes before publishing |

## Not done (needs the maintainer)

- History rewriting: not needed by any finding above; none performed.
- Making the repository public, tags, releases, container images,
  announcements.
