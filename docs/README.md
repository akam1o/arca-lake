# arca-lake documentation

Start with the guides for current behavior and requirements. Detailed
design rationale, verification evidence, and historical development plans
are linked separately below. arca-lake is experimental and not
production-ready; 0.1.0 is the first experimental release (candidate; not
yet published). Read the [known limitations](guide/known-limitations.md)
before evaluating it.

## Try and operate arca-lake

| Task | Document |
| --- | --- |
| Start a local S3 endpoint or development cluster | [Quick start](guide/quick-start.md) |
| Check the operating system, toolchain, and FoundationDB requirements | [Supported platforms](guide/supported-platforms.md) |
| Check an S3 operation or client difference | [S3 compatibility](guide/s3-compatibility.md) |
| Configure gateways, storage nodes, and controllers | [Configuration](guide/configuration.md) |
| Understand deployment assumptions and capacity behavior | [Operational requirements](guide/operations.md) |
| Configure identities, authorization, and transport security | [Security model](guide/security-model.md) |
| Back up metadata and recover a cluster | [Backup and restore](guide/backup-restore.md) |
| Diagnose a failure or refusal | [Troubleshooting](guide/troubleshooting.md) |
| Understand unsupported or unverified behavior | [Known limitations](guide/known-limitations.md) |

## Understand and contribute

| Document | Purpose |
| --- | --- |
| [Architecture overview](architecture/overview.md) | Current components, data flow, and safety boundaries |
| [Repository layout](architecture/repository-layout.md) | Where implementation, tests, and tools live |
| [Contributing](../CONTRIBUTING.md) | Development setup, review process, and DCO sign-off |
| [Current roadmap](roadmap/README.md) | Active work and the boundary between delivered work and open criteria |
| [ADR index](adr/README.md) | Decisions, alternatives, and the invariants later changes must respect |
| [RFC index](rfcs/README.md) | Detailed designs, API matrices, runbooks, and acceptance criteria |
| [Risk register](architecture/risks.md) | Risks and the evidence behind their mitigations and residual limits |
| [Threat model](security/threat-model.md) | Trust boundaries and security assumptions |

## Evidence and publication preparation

- [Verification index](verification/README.md): measurements, fault tests,
  recovery exercises, and their limitations.
- [Benchmarks](../benchmarks/README.md): harnesses and reproducible reports.
- [Public repository readiness](release/public-readiness.md): preparation
  for repository visibility, separate from a versioned software release.
- [Public artifact policy](security/public-artifacts.md): sanitization of
  measurement output and treatment of previously exposed information.

## Historical material

The [archive index](archive/README.md) links the detailed pre-release
changelog, early phase plans, and initial design surveys. Historical
documents remain available for rationale and traceability; they do not
describe the current implementation unless a current guide explicitly
cites the relevant result. ADR and verification-report paths remain stable.

## How to read status labels

- **Implemented** identifies code that exists. It does not imply that all
  acceptance criteria have passed.
- **Verified** applies only to the cited test or measurement and its stated
  version, configuration, and failure model.
- **Not verified** means the required evidence is absent or incomplete.
- **Assumption** states a condition on which a design or result depends.
- **Hypothesis** requires further evidence; **open** needs a decision or work.
- **Accepted** is an RFC or ADR design status. An accepted design can contain
  deferred implementation, and a draft RFC can describe implemented behavior.

Current guides summarize behavior; accepted ADRs record decisions, and RFCs
provide detailed specifications and acceptance evidence. Follow explicit
supersession and amendment links when decisions have changed. Do not infer
current support or safety from an old plan or a successful test in a
different configuration.

## Documentation policy

English is the canonical language ([ADR-0002](adr/0002-documentation-language.md)).
Every document must be suitable for public release under
[CONTRIBUTING-POLICY.md](CONTRIBUTING-POLICY.md), including examples,
measurements, and text produced by automated tooling.
