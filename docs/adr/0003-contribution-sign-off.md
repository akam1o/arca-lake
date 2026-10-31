# ADR-0003: Developer Certificate of Origin sign-off

- Status: accepted
- Date: 2026-11-01
- Related RFC: —

## Context

Contributions to an Apache-2.0 project need a clear statement that the
contributor has the right to submit the work under the project license. The
two common mechanisms are a Contributor License Agreement (CLA) and the
Developer Certificate of Origin (DCO).

## Decision

Contributors certify the Developer Certificate of Origin 1.1 by adding a
`Signed-off-by:` trailer to each commit (`git commit -s`). CI checks for the
trailer on every pull request.

## Alternatives rejected

- **CLA**: stronger legal form, but requires a signing workflow and a legal
  entity to hold agreements; disproportionate for the project's current size.
- **No mechanism**: leaves provenance of contributions undocumented.

## Consequences

- `CONTRIBUTING.md` explains the sign-off.
- Pull requests without sign-off are blocked by CI until fixed.
- Moving to a CLA later would require a new ADR and re-consent from
  contributors.
