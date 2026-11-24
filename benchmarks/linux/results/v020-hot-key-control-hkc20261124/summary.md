# Hot-key A/B hkc20261124

Arms: off = queue-off (the PR head build, gateways with --commit-queue off); on = queue-on (the same binary, default gateways).

Verdict: **adopt**

| Cell | valid | invalid | SlowDown % | ack/s | attempts/ack | ack p50/p95/p99 ms | all p99 ms | unknown |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| off/unv | 3 | 0 | 54.4 | 70.3 | 77.3 | 195/537/770 | 983 | 0.0 |
| on/unv | 3 | 0 | 0.5 | 111.7 | 25.6 | 449/1462/1791 | 1879 | 0.0 |
| off/ver | 3 | 0 | 54.4 | 74.5 | 74.0 | 190/489/825 | 1118 | 0.0 |
| on/ver | 3 | 0 | 2.6 | 99.5 | 31.2 | 483/1587/1928 | 2060 | 0.0 |
| off/unv16 | 3 | 0 | 0.0 | 490.5 | 8.2 | 102/309/508 | 511 | 0.0 |
| on/unv16 | 3 | 1 | 0.0 | 517.0 | 6.7 | 92/310/534 | 534 | 0.0 |

| Target | Scenario | Result | Detail |
| --- | --- | --- | --- |
| SlowDown share ≤ half the tag rebuild's and ≤ 25 % | unv | met | `{"off": 0.543735675260793, "on": 0.004665322021766968}` |
| attempts per acknowledged PUT ≤ half | unv | met | `{"off": 77.34963991263602, "on": 25.555021615906337}` |
| acknowledged PUT/s ≥ 1.3 × | unv | met | `{"off": 70.2556074996262, "on": 111.65014059056728}` |
| SlowDown share ≤ half the tag rebuild's and ≤ 25 % | ver | met | `{"off": 0.5441543697662037, "on": 0.02573986501847907}` |
| attempts per acknowledged PUT ≤ half | ver | met | `{"off": 73.96193398677889, "on": 31.19617245413536}` |
| acknowledged PUT/s ≥ 1.3 × | ver | met | `{"off": 74.52652711639912, "on": 99.52103952192586}` |
| acknowledged PUT/s ≥ 0.95 × | unv16 | met | `{"off": 490.4666186655615, "on": 517.0027956338635}` |
| SlowDown share ≤ tag rebuild's + 0.5 points | unv16 | met | `{"off": 0.00041911327777961386, "on": 0.0}` |
| acknowledged p99 ≤ 1.5 × | unv16 | met | `{"off": 507.7636666666667, "on": 533.5243333333334}` |
| unknown outcomes 0 | unv | met | `{"unknown": 0, "arms": ["off", "on"]}` |
| unknown outcomes 0 | ver | met | `{"unknown": 0, "arms": ["off", "on"]}` |
| unknown outcomes 0 | unv16 | met | `{"unknown": 0, "arms": ["off", "on"]}` |
| violations, definite failures visible, scrub findings: 0 in every valid run | all | met | `{"runs": []}` |
