| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 235 | -30.8% | 15% | REGRESSION |
| get_warm_mib_s | 695 | 654 | -5.9% | 15% | ok |
| get_warm_p50_ms | 78.3 | 85.9 | +9.7% | 25% | ok |
| get_warm_p95_ms | 94.3 | 101 | +6.9% | 25% | ok |
| get_warm_p99_ms | 99.9 | 114 | +14.0% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.77 | +2.8% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.09 | -4.6% | 20% | ok |
| rss_mib | 110 | 155 | +41.0% | 20% | REGRESSION |
| hit_ratio | 0.932 | 0.773 | -0.159 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 34.8 | -50.2% | 25% | REGRESSION |
| meta_txn_per_s | 149 | 106 | -28.8% | 30% | ok |
| multipart_put_mib_s | 201 | 134 | -33.6% | 15% | REGRESSION |
| multipart_get_warm_mib_s | 688 | 468 | -32.0% | 15% | REGRESSION |
