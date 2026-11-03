| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 342 | +0.6% | 15% | ok |
| get_warm_mib_s | 695 | 684 | -1.7% | 15% | ok |
| get_warm_p50_ms | 78.3 | 80.1 | +2.3% | 25% | ok |
| get_warm_p95_ms | 94.3 | 96.6 | +2.4% | 25% | ok |
| get_warm_p99_ms | 99.9 | 109 | +8.7% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.83 | +4.2% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.13 | -2.9% | 20% | ok |
| rss_mib | 110 | 125 | +13.7% | 20% | ok |
| hit_ratio | 0.932 | 0.82 | -0.112 | -0.05 abs | REGRESSION |
| population_chunks_per_s | 69.9 | 68.6 | -1.9% | 25% | ok |
| meta_txn_per_s | 149 | 151 | +1.3% | 30% | ok |
| multipart_put_mib_s | 201 | 276 | +37.5% | 15% | ok |
| multipart_get_warm_mib_s | 688 | 683 | -0.7% | 15% | ok |
