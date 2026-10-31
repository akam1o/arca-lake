# ADR-0002: English is the canonical documentation language

- Status: accepted
- Date: 2026-11-01
- Related RFC: —

## Context

arca-lake is a public open-source project. Design documents, RFCs, ADRs,
code comments, commit messages, issues, and pull requests are all public
artifacts, and the widest contributor and reviewer base reads English.

## Decision

English is the canonical language for all repository artifacts. Translations
may be added under a `docs/i18n/<lang>/` tree; where a translation and the
English text disagree, the English text is authoritative.

## Alternatives rejected

- **Japanese as canonical**: matches the initial maintainers, but limits
  review and contribution from the wider ecosystem.
- **Bilingual canonical documents**: doubles maintenance cost and makes it
  unclear which text governs.

## Consequences

- Reviews may request language clarity fixes; this is normal and not a
  barrier to contribution.
- Discussion in any language is welcome in issues; decisions are recorded in
  English.
