# Public repository and confidentiality policy

arca-lake is developed and maintained in public. This policy applies to every
artifact in the repository and every artifact produced around it: source
code, comments, documentation, RFCs, ADRs, tests and fixtures, benchmarks,
configuration examples, commit messages, pull request titles and bodies,
issues, changelogs, release notes, CI logs and artifacts, generated
documentation, and any summaries produced by automated tooling or AI agents.

## What must never appear

- Credentials, tokens, keys, or certificates of any kind, including "test"
  credentials that were ever valid anywhere.
- Internal hostnames, cluster names, IP ranges, endpoints, or other
  infrastructure identifiers of any non-public environment.
- Hardware inventories or specifications of non-public environments,
  including accelerator counts or models.
- Bandwidth, capacity, or performance figures derived from non-public
  requirements or non-public environments.
- Capacity plans, budgets, project plans, customer or organization
  information of non-public origin.
- Benchmark results that were not measured in a reproducible environment that
  can be fully described in public.
- Any detail from which the characteristics of a non-public environment could
  be inferred, even after renaming.

Renaming an internal system while keeping its identifiable characteristics is
not acceptable anonymization.

## How to write public design rationale

Design decisions must be justified by general engineering principles,
reproducible experiments, or requirements that can be stated publicly.
Prefer generic phrasing such as:

- high-throughput distributed workloads
- multi-petabyte scalability
- highly concurrent object access
- workload-aware prefetch
- bandwidth-aware scheduling
- heterogeneous storage tiers

If a design decision cannot be explained without disclosing non-public
information, stop and raise it with the maintainers before writing anything.

## Benchmarks

Only publish numbers measured on a setup that is fully described in the
document (instance type or hardware model, OS and kernel version, software
versions, configuration, workload definition). Never fabricate or extrapolate
results. Unmeasured expectations must be labelled as hypotheses.

## Before every commit

1. Review the staged diff in full.
2. Check for secrets and non-public information (an automated secret scanner
   runs in CI, but it is not a substitute for review).
3. Check the commit message.
4. Review new documentation and configuration examples with particular care.
5. Confirm generated artifacts contain no non-public information.
6. Confirm there are no internal endpoints, credentials, hostnames, or
   infrastructure identifiers.

Commit messages describe the technical change in the repository, for example:

```text
feat(storage): add erasure-coded chunk writer
feat(tiering): implement asynchronous promotion
fix(metadata): prevent stale generation commits
test(recovery): cover interrupted chunk migration
```

Do not justify changes by reference to non-public environment requirements.
