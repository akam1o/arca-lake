# Phase 4.1 P0-2 results (R-113 A/B)

Behind `docs/verification/phase-4.1-r113.md`: one line per run, tree
(`r113-314` the Phase 3.14 tree, `r113-41` the Phase 4.1 tree before the
test change), condition, exit status, start and end (Unix seconds), the
test result line (filled only for a failing run: the pattern matched upper-case results) and the names of failing tests; `rc` is the authority.
`ab-managed-restore.jsonl`: `--test it managed_restore::`, idle and with
four CPU-bound loops; `ab-whole-it.jsonl`: the whole `it` binary.
