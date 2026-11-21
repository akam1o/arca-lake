# R-110: capacity-pressure retest, results of 2026-11-21

Outcome: **Inconclusive.** The plan's C1 could not be judged and the run
ended in stage 4 (§7 of the [plan](phase-4.1-r110-retest-plan.md): an
exit condition not met ends the run). C2 to C5 were not run. R-110 stays
Medium and open; nothing here passes or closes it.

## 1. What ran

| | |
| --- | --- |
| Run | `r110-20261121T153009Z`, 2026-11-21 15:30–16:47 UTC (stages 1–4, then the 30 min drain observation) |
| Placement | plan option 1: every test process and the load on host 2; the watchdog watched hosts 1–3; the driver and the watchdog ran on the operator's machine |
| System FoundationDB checked (`status minimal` only) | the single-process clusters on hosts 1 and 2 (host 3 has none; the plan named hosts 1 and 3) |
| Binary | `e391d662` (main, product code unchanged since), `cargo build --release --features fdb`, rustc 1.99.1, sha256 `8de7abbc417fe575350d5c24e7b38d0d2233453e7a48d70de6ab7c9fb9a4c1a8`, built on host 2 |
| Tooling | `benchmarks/linux/r110` at `4de8730` (merged in main as `ba87fe4`; later commits change no behaviour this run used except the ones listed in §5) |
| Settings | the plan's §5 unchanged: 4 nodes of 1 GiB, segment maximum 64 MiB, retention 600 s, skew 60 s, chunk 1 MiB, 256 KiB objects, L0 4,000, D1 1,000, at most 5,000 new objects in stage 4 |
| Limits | the plan's §6.1 unchanged: 20 requests/s, burst 5, 4 in flight, 70,000 attempts (68,980 for the run after the smoke stage's share), 5 GiB of request bodies, 8,000 live objects, 7.5 h |
| Dedicated FoundationDB | `--memory 3GiB --cache-memory 1GiB` (chosen at the preflight: 7.6 GB available on host 2; fdbserver refuses a memory limit not above its cache memory) |
| Stop rules, judgement values | the plan's §6.2 and §8 unchanged |
| Approval | written from the user's delegated instruction for the three dedicated nodes (preflight, smoke, run, teardown within the plan's limits); no value in it was reviewed by the user |

## 2. Stages

| Stage | UTC | Result |
| --- | --- | --- |
| 1 Preflight | 15:30:17–15:30:20 | every check of §10 passed (free space 31.6 GB on the data file system, memory, load, clocks within 0.2 s, kernel log readable, ports free, binary sha256, the retained cluster's directories unchanged) |
| 2 Smoke | 15:30:21–15:32:57 | a withheld heartbeat stopped a load by the guard alone (its group confirmed empty); an attempt limit ended another; 100 PUT, GET (100 found), DELETE and the verification: 0 errors |
| 3 Fill and baseline | 15:32:58–15:53:45 | 4,000 objects and the baseline probe's object acknowledged; admission back to normal, GC backlog and pending records 0 |
| 4 Pressure | 15:53:45–16:16:53 | D1's 1,000 deletes done at 15:54:38; the fill reached write rejected at 16:01:30; admission left write rejected by itself at 16:16:45, before D1's maturity (16:19:38, `d1_chain_s` after D1); the exit condition was not met and the run ended (`stage-exit`) |
| Drain | 16:16:54–16:47:00 | observed for 30 min; no resource or monitor rule fired; admission back to normal at 16:18:45 |

Requests: 8,207 attempts, 1.77 GB of request bodies, 4,676 live objects at
the end; no 5xx other than 503, no ledger failure, no protection counter
above 0.

## 3. What the samples show (host 2's clock)

- During the refusal (16:01:30–16:16:45) GC collected D1's generations:
  its backlog went from 1,000 to 4 by 16:05 and to 0 by 16:15.
- D1's shard records were retired and waited out the retention (pending
  records up to 1,000 on the fullest nodes at 16:08), then reclaim ran:
  `arca_controller_reclaimed_total` rose from 300 to 1,236 by 16:15:30
  and to 3,300 by 16:20 (D1's 3,000 records and the smoke stage's 300).
- The nodes' available bytes followed: the two fullest nodes went from
  about 462 MiB before D1 to 249 MiB at the lowest (16:08) and 384 MiB
  after reclaim; the other two stayed above 630 MiB (admission judges a
  group by its fullest member).
- Admission stepped down as reclaim returned space: write rejected (3)
  to throttled (2) at 16:16:45, warning (1) at 16:17:45, normal (0) at
  16:18:45. The controller's backpressure state went from 3 back to 0 by
  16:20.

GC and reclaim therefore kept working while the gateway refused writes,
and the space they returned ended the refusal. That is not a C1 verdict:
C1 judges D1's reclaim over the refused interval once D1 has matured, and
the refusal ended about 3 minutes earlier, with about 936 of D1's 3,000
records reclaimed inside it. Under the plan (§8), admission recovering
before D1 matured is Inconclusive.

## 4. Why, and what would change it

The refusal is ended by the very reclaim C1 measures: the fill stops at
the refusal, the hold adds one refused attempt per second, and the space
D1 returns is enough to lower admission before its last records are
reclaimed. Another run of the same plan would most likely end the same
way. Making C1 judgeable needs a change to the plan, which is not made
here (the plan's values and criteria are not loosened to make a run
pass). For example:

- judge C1 on the refusal's own end when admission recovers because of
  D1's reclaim (a criterion change);
- keep writing during the hold so that space returned is used again (a
  load change: more attempts and bytes than the plan's hold);
- a larger D1 or stage-4 fill relative to the allotments (a budget
  change).

Each is a decision for the plan's approver.

## 5. Earlier attempts and rehearsals

- Smoke rehearsals on host 2 (fresh clusters, own ports and directories)
  found and fixed, before any run: the guard killing its load before the
  load wrote its status; a sub-run's guard reporting services it had
  none of; fdbserver refusing `--memory` not above `--cache-memory`; a
  stop file or alarm read while being written. A sentinel exercise (an
  alarm marked as such) stopped every service and confirmed it in 1.6 s.
- A first run (`r110-20261121T144238Z`, tooling `265a314`) was
  interrupted by the operator at stage 3: a review of the tooling had
  shown that, with the live set at its 8,000 limit after stage 4's fill,
  stage 4's hold and stage 5's probe PUTs would send nothing and C3a could
  only be Inconclusive. It ended Inconclusive (C1: no refusal formed;
  C2–C5 not run); every service was stopped and confirmed; its evidence
  is kept.
- The run of §1 used the corrected tooling (`4de8730`). Its later fixes
  (`c57df76`, `d42ca12`: a stop's ssh timeout, protection checked inside
  running steps, records never replaced) did not affect it: its guard and
  services stopped by themselves before the final stops, and no
  protection counter rose.

## 6. Resources and cleanup

Every process of each run was stopped by identity with its group and
confirmed; teardown removed only the run's two marked directories (and
their empty parent) after every stop was confirmed and the liveness check
held. The retained cluster's directories on host 2 have the same metadata
digest before and after the runs. The system FoundationDB of each host was
only read. Raw evidence (configuration, approval, timeline, samples,
ledger, logs, result) is kept outside the repository, named by the run
identifier.
