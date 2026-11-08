| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 354 | +4.1% | 15% | ok |
| get_warm_mib_s | 695 | 683 | -1.8% | 15% | ok |
| get_warm_p50_ms | 78.3 | 81.3 | +3.8% | 25% | ok |
| get_warm_p95_ms | 94.3 | 95.7 | +1.4% | 25% | ok |
| get_warm_p99_ms | 99.9 | 106 | +5.9% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.77 | +2.9% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.05 | -6.5% | 20% | ok |
| rss_mib | 110 | 127 | +15.8% | 20% | ok |
| hit_ratio | 0.932 | 0.802 | -0.130 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 63.7 | -8.9% | 25% | ok |
| meta_txn_per_s | 149 | 157 | +5.4% | 30% | ok |
| multipart_put_mib_s | 201 | 158 | -21.2% | 15% | REGRESSION |
| multipart_get_warm_mib_s | 688 | 685 | -0.5% | 15% | ok |
