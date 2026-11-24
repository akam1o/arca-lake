# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems.

Report vulnerabilities privately through GitHub's private vulnerability
reporting for this repository ("Security" tab → "Report a vulnerability").
If that is unavailable, contact a maintainer listed on the repository
privately.

Please include:

- affected component and version or commit,
- a description of the issue and its impact,
- reproduction steps or a proof of concept if available.

We will acknowledge the report, work with you on a fix and a coordinated
disclosure timeline, and credit you if you wish.

## Supported versions

0.2.0 is the second experimental release (0.1.0 was the first).

| Version | Status |
| --- | --- |
| 0.2.x | Experimental: security fixes on a best-effort basis, no guaranteed timeline |
| 0.1.x | Experimental: security fixes on a best-effort basis, no guaranteed timeline |
| `main` | Development; fixes land here first |

No version is production-ready.

## Scope

In scope: arca-lake source code, release binaries and deployment examples
published by this repository (no container image is published), and CI
configuration.

Out of scope: third-party dependencies (report upstream; we will track and
update), and issues in deployments that do not follow the documented
isolation requirements for the metadata store.

## Security design

The [security model](docs/guide/security-model.md) describes the current
controls and their operating conditions. Detailed design rationale is in
[RFC-012](docs/rfcs/RFC-012-security-architecture.md) and the
[threat model](docs/security/threat-model.md). Implementation and test
coverage are scoped to the documented configurations; there has been no
external security audit. See [known limitations](docs/guide/known-limitations.md).
