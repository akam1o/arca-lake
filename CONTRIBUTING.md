# Contributing to arca-lake

Thank you for your interest. arca-lake is in its design phase; the most
valuable contributions right now are design review comments on the RFCs and
architecture documents, and Phase 0 experiments.

## Ground rules

- Read the [public repository policy](docs/CONTRIBUTING-POLICY.md). It
  applies to every artifact, including issues and pull request text.
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- All contributions are licensed under [Apache-2.0](LICENSE) (ADR-0001).
- Sign off every commit with the Developer Certificate of Origin (ADR-0003):

  ```sh
  git commit -s
  ```

  This adds a `Signed-off-by:` trailer certifying
  <https://developercertificate.org/>.

## How design changes happen

1. Small clarifications: open a pull request against the relevant document.
2. Anything that changes an invariant, a format, a protocol, or a decision
   recorded in an ADR: open an issue first, then an RFC or a superseding ADR.
   Templates: `docs/rfcs/0000-template.md`, `docs/adr/0000-template.md`.
3. RFCs are reviewed in pull requests. Acceptance requires that blocking open
   questions are resolved and acceptance criteria are testable.

## Development setup

- Rust stable toolchain as pinned in `rust-toolchain.toml` (rustup installs
  it automatically).
- Linux is the production platform; macOS works for development with the
  blocking I/O backend (ADR-0004).
- Optional: Docker or Podman for FoundationDB and multi-node tests.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo deny check      # requires `cargo install cargo-deny`
```

## Commit messages

Use Conventional Commits scoped by crate or area:

```text
feat(storage): add erasure-coded chunk writer
fix(metadata): prevent stale generation commits
docs(rfc): draft RFC-002 object and chunk data model
test(recovery): cover interrupted chunk migration
```

Describe the technical change. Do not reference non-public environments or
requirements.

## Pull request checklist

- [ ] I reviewed the full diff for secrets, hostnames, internal identifiers,
      and non-public information.
- [ ] New documentation and configuration examples contain only public,
      generic content.
- [ ] Any performance figures come from a fully described, reproducible
      environment, or are labelled as hypotheses.
- [ ] Commits are signed off (`git commit -s`).
- [ ] `cargo fmt`, `cargo clippy`, and `cargo test` pass locally.
- [ ] Format or protocol changes bump the relevant version field and add a
      compatibility test.
