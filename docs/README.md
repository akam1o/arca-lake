# arca-lake documentation

This directory holds the design documentation for arca-lake.

## Structure

```text
docs/
├── architecture/      Living architecture documents (updated as RFCs are accepted)
├── rfcs/              Numbered design proposals; the unit of design review
├── adr/               Architecture Decision Records; short, immutable records of decisions
├── roadmap/           Phase plans with scope, tasks, acceptance criteria, risks
├── guide/             User and operator guides (quick start, configuration, limitations)
├── release/           Release preparation: audits, CI review, checklist
├── verification/      Verification reports
└── CONTRIBUTING-POLICY.md
```

## How the documents relate

- **RFCs** propose designs. They are reviewed and either accepted, rejected,
  or superseded. An accepted RFC is the authoritative description of a design
  until superseded.
- **ADRs** record *decisions* (what was chosen, when, why, and what was
  rejected) in a short form. An RFC may produce several ADRs.
- **Architecture documents** summarize the current state for readers who do
  not want to read every RFC. They link back to RFCs and ADRs.
- **Roadmap** documents are plans; they are expected to change.

## Terminology used throughout

Facts, assumptions, hypotheses, and open issues are deliberately labelled:

| Label | Meaning |
| --- | --- |
| **Verified** | Checked against primary sources (official docs, source code, measured results) and cited |
| **Assumption** | A design assumption we proceed on; must be stated explicitly |
| **Hypothesis** | A belief that requires verification (usually by a Phase 0 experiment) |
| **Open** | An unresolved question that needs a decision |

## Public repository policy

Every document here is public. Do not include non-public infrastructure
details, capacity plans, hostnames, credentials, or performance figures that
were not measured in a reproducible, publicly describable environment.
See [CONTRIBUTING-POLICY.md](CONTRIBUTING-POLICY.md).
