| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 205 | -39.6% | 15% | REGRESSION |
| get_warm_mib_s | 695 | 678 | -2.5% | 15% | ok |
| get_warm_p50_ms | 78.3 | 77.2 | -1.4% | 25% | ok |
| get_warm_p95_ms | 94.3 | 106 | +12.3% | 25% | ok |
| get_warm_p99_ms | 99.9 | 112 | +12.2% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 5.06 | +9.0% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.37 | +8.2% | 20% | ok |
| rss_mib | 110 | 116 | +5.4% | 20% | ok |
| hit_ratio | 0.932 | 0.753 | -0.180 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 38.3 | -45.2% | 25% | REGRESSION |
| meta_txn_per_s | 149 | 90.8 | -38.9% | 30% | REGRESSION |
| multipart_put_mib_s | 201 | 247 | +22.9% | 15% | ok |
| multipart_get_warm_mib_s | 688 | 664 | -3.4% | 15% | ok |
