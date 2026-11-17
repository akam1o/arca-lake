# Phase 4.1 P0-3 results (ADR-0033 A/B)

Behind `docs/verification/phase-4.1-adr33.md`. Layout: 4 storage nodes
and one gateway on one host; FoundationDB, the controller and the load
generator on the other; 1 KiB objects; retention 600 s.

- `reconcile.jsonl`: one line per reconcile pass: objects (`n`), arm
  (`base`: the tree before ADR-0033; `p4`: ADR-0033, 4 pairs at once;
  `p1`: ADR-0033, one pair at once), run, the pass's seconds, wall seconds
  from the controller's start to the end of its first pass, the
  controller's CPU ticks (1/100 s), FoundationDB workload counters before
  and after, and 60 s later.
- `reclaim.jsonl`: one line per reclaim run: the longest reclaim pass
  (`pass_max_s`), seconds from the controller's start until the nodes had
  no waiting record, records waiting before, reclaimed, errors, refused,
  left, FoundationDB counters.
- `retained.jsonl`: every 30 s of the 90 min churn run: seconds since
  start, the nodes' retained bytes and waiting records, the GC backlog,
  records reclaimed so far, the last and longest reclaim pass.
