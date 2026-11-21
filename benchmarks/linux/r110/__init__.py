"""Tooling of the R-110 capacity-pressure retest
(docs/verification/phase-4.1-r110-retest-plan.md).

Standard library only. Modules: ``common`` (clocks, atomic files, stop
reasons), ``budget`` (the run's shared limits), ``ledger`` (the outcome
ledger), ``s3`` (the data-plane HTTP client), ``load`` (the load process).
Nothing here connects to a host by itself; see the plan for the approvals
a run needs.
"""
