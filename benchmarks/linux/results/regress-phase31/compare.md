| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 243 | -28.4% | 15% | REGRESSION |
| get_warm_mib_s | 695 | 689 | -1.0% | 15% | ok |
| get_warm_p50_ms | 78.3 | 80.2 | +2.5% | 25% | ok |
| get_warm_p95_ms | 94.3 | 106 | +12.0% | 25% | ok |
| get_warm_p99_ms | 99.9 | 109 | +9.1% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.6 | -0.8% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.32 | +5.6% | 20% | ok |
| rss_mib | 110 | 126 | +15.0% | 20% | ok |
| hit_ratio | 0.932 | 0.815 | -0.117 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 62 | -11.3% | 25% | ok |
| meta_txn_per_s | 149 | 106 | -28.4% | 30% | ok |
| multipart_put_mib_s | - | 220 | new | - | recorded |
| multipart_get_warm_mib_s | - | 688 | new | - | recorded |
