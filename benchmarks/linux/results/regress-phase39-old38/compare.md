| metric | baseline | current | change | tolerance | result |
| --- | --- | --- | --- | --- | --- |
| put_mib_s | 340 | 299 | -11.9% | 15% | ok |
| get_warm_mib_s | 695 | 698 | +0.3% | 15% | ok |
| get_warm_p50_ms | 78.3 | 78 | -0.4% | 25% | ok |
| get_warm_p95_ms | 94.3 | 97.4 | +3.2% | 25% | ok |
| get_warm_p99_ms | 99.9 | 104 | +4.3% | 25% | ok |
| cpu_s_per_gib_gateway_host | 4.64 | 4.76 | +2.5% | 20% | ok |
| cpu_s_per_gib_node_host | 2.19 | 2.14 | -2.6% | 20% | ok |
| rss_mib | 110 | 141 | +28.2% | 20% | REGRESSION |
| hit_ratio | 0.932 | 0.914 | -0.018 | -0.05 abs | ok |
| population_chunks_per_s | 69.9 | 57.4 | -17.8% | 25% | ok |
| meta_txn_per_s | 149 | 133 | -10.6% | 30% | ok |
| multipart_put_mib_s | 201 | 246 | +22.3% | 15% | ok |
| multipart_get_warm_mib_s | 688 | 682 | -0.9% | 15% | ok |
