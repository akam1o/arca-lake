# RFC-NNN: Title

- Status: draft | review | accepted | rejected | superseded by RFC-NNN
- Authors:
- Created: YYYY-MM-DD
- Depends on:
- Related ADRs:

> This document is public. It must not contain non-public infrastructure
> details, capacity plans, hostnames, credentials, or unmeasured performance
> figures. See `docs/CONTRIBUTING-POLICY.md`.

## 1. Problem

What problem does this RFC solve, and why now?

## 2. Goals

Bullet list of what the design must achieve.

## 3. Non-goals

What this RFC deliberately does not address.

## 4. Requirements

Functional and non-functional requirements. Label each as *Verified*
requirement (traceable to a public source), *Assumption*, or *Open*.

## 5. Proposed design

The design in enough detail to implement. Include data structures, state
machines, protocols, key layouts, and format versions where relevant.
Call out every linearization point for operations with concurrency.

## 6. Alternatives considered

At least two alternatives with trade-offs and why they were not chosen.

## 7. Failure scenarios

Enumerate failures (crash, partition, disk, corruption, concurrent failures,
interrupted background work, upgrade failure) and the system's behavior.
Prefer explicit failure to silent data loss, and say so for each case.

## 8. Consistency and durability guarantees

What is guaranteed, under which conditions, and what is explicitly not
guaranteed.

## 9. Performance considerations

Expected costs (read/write amplification, metadata operations, CPU). Mark
expectations as *Hypothesis* unless backed by a linked measurement.

## 10. Open questions

Questions that must be resolved before acceptance, and those that can be
deferred to tracked issues.

## 11. Acceptance criteria

Testable criteria. Reference the tests or benchmarks that will demonstrate
them.
