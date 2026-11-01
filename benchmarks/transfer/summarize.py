#!/usr/bin/env python3
"""Turn transfer-bench JSON lines into a Markdown table.

Usage: benchmarks/transfer/summarize.py results.jsonl
"""
import json
import sys


def main(path: str) -> None:
    clients = []
    servers = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            (clients if d["role"] == "client" else servers).append(d)
    # Pair each client with the server line that followed it.
    print("| transport | tls | op | conc | MiB/s | p50 ms | p99 ms | client CPU % | server CPU % | client RSS MiB | server RSS MiB |")
    print("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for c, s in zip(clients, servers):
        print(
            f"| {c['transport']} | {str(c['tls']).lower()} | {c['op']} | {c['concurrency']} | "
            f"{c['throughput_mib_s']:.0f} | {c['latency_p50_ms']:.2f} | {c['latency_p99_ms']:.2f} | "
            f"{c['cpu_percent']:.0f} | {s['cpu_percent']:.0f} | "
            f"{c['max_rss_bytes'] / 2**20:.0f} | {s['max_rss_bytes'] / 2**20:.0f} |"
        )


if __name__ == "__main__":
    main(sys.argv[1])
