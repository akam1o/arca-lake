# Hot-key A/B hk20261124

Arms: tag = tag rebuild (v0.1.0 tag commit rebuilt on the test host; not the published artefact); candidate = candidate (PR head).

Verdict: **adopt**

| Cell | valid | invalid | SlowDown % | ack/s | attempts/ack | ack p50/p95/p99 ms | all p99 ms | unknown |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tag/unv | 3 | 0 | 54.5 | 69.1 | 77.2 | 201/540/809 | 1177 | 0.0 |
| candidate/unv | 3 | 0 | 0.5 | 111.5 | 25.2 | 475/1404/1731 | 1811 | 0.0 |
| tag/ver | 3 | 0 | 55.0 | 67.3 | 80.6 | 194/585/853 | 1106 | 0.0 |
| candidate/ver | 3 | 0 | 0.8 | 117.3 | 25.2 | 425/1369/1738 | 1844 | 0.0 |
| tag/retry | 3 | 0 | 5.3 | 74.8 | 66.6 | 414/2268/3034 | 3474 | 0.0 |
| candidate/retry | 3 | 0 | 0.0 | 103.9 | 28.3 | 494/1577/2321 | 2321 | 0.0 |
| tag/unv16 | 3 | 0 | 0.0 | 508.9 | 7.8 | 93/306/520 | 525 | 0.0 |
| candidate/unv16 | 3 | 0 | 0.0 | 492.0 | 6.9 | 97/324/548 | 548 | 0.0 |
| candidate/fdbstall | 2 | 0 | 2.8 | 68.7 | 25.1 | 456/1419/1781 | 12021 | 9.0 |
| candidate/gwkill | 2 | 0 | 0.0 | 110.3 | 7.6 | 466/1371/1728 | 927 | 115616.5 |

| Target | Scenario | Result | Detail |
| --- | --- | --- | --- |
| SlowDown share ≤ half the tag rebuild's and ≤ 25 % | unv | met | `{"tag": 0.5454344511677437, "candidate": 0.004731573741684021}` |
| attempts per acknowledged PUT ≤ half | unv | met | `{"tag": 77.2301255100001, "candidate": 25.151174149635835}` |
| acknowledged PUT/s ≥ 1.3 × | unv | met | `{"tag": 69.14878244784389, "candidate": 111.46988516890985}` |
| SlowDown share ≤ half the tag rebuild's and ≤ 25 % | ver | met | `{"tag": 0.5497330886231434, "candidate": 0.008030635922732647}` |
| attempts per acknowledged PUT ≤ half | ver | met | `{"tag": 80.64007804168017, "candidate": 25.184009333148385}` |
| acknowledged PUT/s ≥ 1.3 × | ver | met | `{"tag": 67.29844613807114, "candidate": 117.31629903442344}` |
| acknowledged PUT/s ≥ 0.95 × | unv16 | met | `{"tag": 508.8596242721882, "candidate": 492.0463112701778}` |
| SlowDown share ≤ tag rebuild's + 0.5 points | unv16 | met | `{"tag": 0.0003921969681031844, "candidate": 0.0}` |
| acknowledged p99 ≤ 1.5 × | unv16 | met | `{"tag": 519.5043333333333, "candidate": 548.19}` |
| SlowDown after 3 retries ≤ tag rebuild's | retry | met | `{"tag": 0.05281824584582695, "candidate": 0.0}` |
| unknown outcomes 0 | unv | met | `{"candidate_unknown": 0}` |
| unknown outcomes 0 | ver | met | `{"candidate_unknown": 0}` |
| unknown outcomes 0 | retry | met | `{"candidate_unknown": 0}` |
| unknown outcomes 0 | unv16 | met | `{"candidate_unknown": 0}` |
| violations, definite failures visible, scrub findings: 0 in every valid run | all | met | `{"runs": []}` |
| correctness runs | fdbstall | met | `{"valid": 2}` |
| correctness runs | gwkill | met | `{"valid": 2}` |
