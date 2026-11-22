# Contributing to arca-lake

arca-lake is an experimental implementation; 0.1.0 is the first
experimental release (candidate; not yet published). Useful contributions
include reproducible bug reports, S3 compatibility fixes, tests of failure
and recovery behavior, documentation, and design review. Start with the
[architecture overview](docs/architecture/overview.md),
[known limitations](docs/guide/known-limitations.md), and
[current roadmap](docs/roadmap/README.md).

## Ground rules

- Read the [public repository policy](docs/CONTRIBUTING-POLICY.md). It
  applies to source, examples, measurements, commits, issues, and pull requests.
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- Contributions use [Apache-2.0](LICENSE). English is the canonical
  documentation language ([ADR-0002](docs/adr/0002-documentation-language.md)).
- Sign off every commit with the Developer Certificate of Origin
  ([ADR-0003](docs/adr/0003-contribution-sign-off.md)):

  ```sh
  git commit -s
  ```

  The `Signed-off-by:` trailer certifies <https://developercertificate.org/>.
  Check your Git name and email before committing; use a GitHub no-reply
  address if you do not want a personal email in public history. CI checks
  every non-merge commit of a pull request for the trailer.
- Pull requests are merged with *rebase and merge* or *squash and merge*
  (merge commits are disabled). Either keeps each commit's author and
  `Signed-off-by:`; the committer is set by GitHub (the merging account
  for a rebase, GitHub for a squash). A squash keeps the
  trailers because the squashed message lists every commit's message: do
  not delete them when editing it.
- Report vulnerabilities through [SECURITY.md](SECURITY.md), rather than a
  public bug report.

## Proposing changes

1. For a bug, describe the commit, public reproduction steps, expected and
   observed behavior, and relevant configuration. Sanitize logs and fixtures.
2. Small fixes and documentation clarifications can be pull requests.
3. For changes to an invariant, format, protocol, or recorded decision,
   open an issue first, then an RFC or a superseding ADR. Use the
   [RFC template](docs/rfcs/0000-template.md) or
   [ADR template](docs/adr/0000-template.md).
4. Resolve blocking design questions and make acceptance criteria testable.
   Record separately what is implemented, what was verified, and what remains
   open. Do not turn an incomplete measurement into a passing result.

## Development setup

Use the Rust toolchain in `rust-toolchain.toml` (rustup selects it
automatically), a C compiler, and `pkg-config`. Linux x86_64 is the primary
tested platform; macOS supports development with in-memory metadata. See
[supported platforms](docs/guide/supported-platforms.md) for the precise scope.

Run the baseline checks from the repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

These commands do not replace FoundationDB integration tests. The `fdb`
feature requires FoundationDB 7.3's client library and libclang; real-backend,
failover, and TLS test setup is described in [deploy/dev](deploy/dev/README.md).
AWS CLI v2 and boto3 are needed for the client conformance tests that use
them; tests that skip absent clients are not evidence that those clients pass.

The [CI workflow](.github/workflows/ci.yml) defines the full feature, backend,
compatibility, and security checks. It runs on GitHub-hosted runners on
pull requests to `main`, pushes to `main` and on demand; the two upgrade
jobs run on pushes and on demand only. First-time contributors' runs wait
for a maintainer's approval. Record your local checks and any unavailable
prerequisites in the pull request.

Dependency changes also require `cargo deny check` (install `cargo-deny`
separately). For documentation-only changes, check relative links, examples
against the source, and the full diff; rebuilding the workspace is not a
substitute for those checks. For benchmark output, follow the
[public artifact policy](docs/security/public-artifacts.md).

## Commit messages

Use Conventional Commits scoped by crate or area:

```text
feat(storage): add erasure-coded chunk writer
fix(metadata): prevent stale generation commits
docs(guide): clarify recovery prerequisites
test(recovery): cover interrupted chunk migration
```

Describe the technical change without reference to non-public environments
or requirements.

## Pull request checklist

- [ ] Review the full diff and commit messages for secrets, internal
      identifiers, and non-public information.
- [ ] Use public, generic documentation and configuration examples.
- [ ] Describe the environment and workload for every performance claim;
      label unmeasured expectations as hypotheses.
- [ ] Sign off every commit.
- [ ] Record applicable checks and results, including anything not run.
- [ ] For format or protocol changes, update the relevant version and
      compatibility tests.
- [ ] Update current guides and limitations when behavior changes; keep
      historical reports scoped to the versions and conditions they tested.
