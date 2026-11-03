| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 312 | -8.0% | 15% | ok |
| get_warm_mib_s | 695 | 681 | -2.1% | 15% | ok |
| get_warm_p50_ms | 78.3 | 79 | +0.9% | 25% | ok |
| get_warm_p95_ms | 94.3 | 102 | +8.1% | 25% | ok |
| get_warm_p99_ms | 99.9 | 112 | +12.6% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.87 | +5.0% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.12 | -3.1% | 20% | ok |
| rss_mib | 110 | 114 | +3.9% | 20% | ok |
| hit_ratio | 0.932 | 0.875 | -0.057 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 67.8 | -2.9% | 25% | ok |
| meta_txn_per_s | 149 | 137 | -8.0% | 30% | ok |
| multipart_put_mib_s | 201 | 248 | +23.4% | 15% | ok |
| multipart_get_warm_mib_s | 688 | 656 | -4.6% | 15% | ok |
