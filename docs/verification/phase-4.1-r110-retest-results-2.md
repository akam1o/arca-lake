# R-110: capacity-pressure retest, results of 2026-11-22

Runs of [plan](phase-4.1-r110-retest-plan.md) versions 2 and 3 on host 2
(the earlier runs of version 1: [results of 2026-11-21](phase-4.1-r110-retest-results.md)).
R-110 stays open whatever these give: a passed run verifies the
small-allotment step only (plan §3).

| Run | Plan | Outcome | Criteria |
| --- | --- | --- | --- |
| `r110-20261122T030322Z` (run 1) | 2 | **Failed** | C1a, C1b, C2, C3a, C3b, C4 Passed; C5 Failed (scrub findings 1,809, all unexpected shards; §2) |
| `r110-20261122T082126Z` (run 2) | 3 | *running* | §3 |

At most three runs of the new plan versions were allowed (0.1.0
preparation); before each, the previous run's cause, the change and the
hypothesis were recorded with the run's evidence on the operator's
machine.

## 1. Common to both runs

| | |
| --- | --- |
| Placement | plan option 1: every test process and the load on host 2; the watchdog watched hosts 1–3 (every host's free memory, free root, load); the driver and the watchdog ran on the operator's machine |
| Settings | the plan's §5 unchanged: 4 nodes of 1 GiB, segment maximum 64 MiB, retention 600 s, skew 60 s, chunk 1 MiB, 256 KiB objects, L0 4,000, D1 1,000, at most 5,000 new objects in stage 4, churn 1/s for 5,400 s |
| Limits, stop rules, judgement values | the plan's §6 and §8 unchanged (C3a 600 s and 60 probes, `d1_chain_s` 1,500 s, C1a and C1b values fixed in version 2 before any run) |
| Dedicated FoundationDB | `--memory 3GiB --cache-memory 1GiB` |
| Smokes before each run, same tooling and binary | final stops confirmed (every service, guard, the sampler, sentinel and watchdog); the sentinel's stop path exercised once (services stopped and confirmed in 1.7 s) |
| Approval | written from the maintainer's delegated instruction of 2026-11-22 (0.1.0 preparation); no value in it was reviewed by the maintainer |

## 2. Run 1 (plan version 2): Failed on C5

Binary `e7b1e603` (main), fdb feature, rustc 1.99.1, sha256
`97041ce77405619627f0e4451e062cc0ef5cc6163f6545f6fb0503b5ba3cfe78`;
tooling `e7b1e60`.

| Stage | UTC | Result |
| --- | --- | --- |
| 1 preflight, 2 smoke | 03:03–03:06 | passed |
| 3 fill and baseline | 03:06–03:26 | L0 4,000 acknowledged |
| 4 pressure | 03:26–03:52 | D1's 1,000 deletes by 03:27:51; refusal 03:34:46–03:50:01 (915 s), ended by a natural recovery; D1's chain over at 03:52:52 |
| 5 drain and recovery | 03:52–04:33 | C2 judged at 04:23:20; C3a's 600 s window with 60 probes |
| 6 steady churn | 04:33–06:03 | completed; live objects within 5 % of L1 |
| 7 integrity | 06:03–06:14 | read-back of 12,122 keys: 0 violations, 0 unresolved; manifests 0 findings (0 multipart roots: vacuous here); scrub 1,809 findings |

| Criterion | Outcome | Evidence |
| --- | --- | --- |
| C1a | Passed | during the 915 s refusal: GC collected 1,004 generations; D1's reclaimed records at least 3,000 (lower bound; 0 subtracted for orphans or capacity-aborted writes), 300 needed; no reclaim refusal or error |
| C1b | Passed | D1 fully reclaimed (3,000 records by the bound) when its chain was seen over, 25 min after D1, well before the 50 min deadline |
| C2 | Passed | 2,680 deletes; live bytes down 981.9 MB of 1,053.8 MB expected; every node's available space rose at least as the rule needs |
| C3a | Passed | 60 probes over 600 s |
| C3b | Passed | 5,255 of 5,255 churn PUTs acknowledged |
| C4 | Passed | allocated and retained bytes within the slope and range limits |
| C5 | **Failed** | "manifest findings 0, scrub findings 1809" |

Cause, from a diagnosis after the run (not part of the judgement):
FoundationDB and the four nodes were restarted from the run's records
(no controller, no gateway) and every group scrubbed again: group 1
879 and group 2 855 findings, **all unexpected shards**, every damage
class 0, the repair queue empty. The nodes' records pending reclaim at
the run's last sample add up to 578 + 578 + 293 + 285 = 1,734 = 879 + 855:
the unexpected shards are the records the nodes retained for the 600 s
reclaim retention after stage 6's last deletes (06:03:50), and stage 7
scrubbed inside that window. Reconciliation ran every 5 min through
stage 7 and removed 0 or 16 orphans a pass: these were not orphans.

The scrub lists a retired record waiting out the retention as an
unexpected shard (it stays in the store's index until reclaimed); that is
not data damage, but it is operator-visible noise after any delete
([known limitations](../guide/known-limitations.md)).

Change for run 2 ([plan](phase-4.1-r110-retest-plan.md) version 3): C5's
rule stays (0 findings, every class); stage 7 reads the manifests and the
scrub only after the background passes settled (the orphan age and two
reconcile intervals after stage 6, no GC backlog, nothing pending
reclaim), and records the findings by class. Run 1 stays Failed.

## 3. Run 2 (plan version 3)

*To be added when the run ends.* Binary: the 0.1.0 candidate archive's
(commit `1ae2c5c`), sha256
`73cbf390fd736d16ae52771bbe4909ec421169b15702fe8ae0c7a2a21b7e7a5e`;
tooling `22c5696`.
