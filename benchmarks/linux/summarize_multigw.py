#!/usr/bin/env python3
"""Aggregate the multi-gateway runs (multi-gateway.sh): per label and phase,
the sum of the gateways' throughput (bytes over the longest gateway's
duration), each gateway's throughput and p50/p99, the CPU-seconds per GiB
on every host (from sar), and the metadata transaction rate of gateway 1.

Usage: summarize_multigw.py <results-dir> [--cores N]
"""

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summarize import cpu_seconds, metric  # noqa: E402


def main():
    argv = sys.argv[1:]
    cores = 4
    if "--cores" in argv:
        i = argv.index("--cores")
        cores = int(argv[i + 1])
        del argv[i : i + 2]
    root = argv[0]
    print("| label | gateways | phase | aggregate MiB/s | per gateway MiB/s | p50 / p99 ms per gateway | CPU-s/GiB per host | meta txn/s (gw1) | conflicts (gw1) |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for label in sorted(os.listdir(root)):
        d = os.path.join(root, label)
        if not os.path.isdir(d):
            continue
        for phase in ["put", "get1", "get2"]:
            files = sorted(f for f in os.listdir(d) if re.match(rf"{phase}-g\d+\.jsonl$", f))
            rows = []
            for f in files:
                lines = [json.loads(l) for l in open(os.path.join(d, f), encoding="utf-8") if l.strip()]
                if lines:
                    rows.append(lines[-1])
            if not rows:
                continue
            total_bytes = sum(r["bytes"] for r in rows)
            longest = max(r["duration_s"] for r in rows)
            gib = total_bytes / float(1 << 30)
            agg = total_bytes / 2**20 / longest if longest else 0.0
            per = " / ".join(f"{r['mib_per_s']:.0f}" for r in rows)
            lat = " / ".join(f"{r['lat_ms_p50']:.0f}:{r['lat_ms_p99']:.0f}" for r in rows)
            sar = os.path.join(d, phase)
            hosts = sorted(f[8:-4] for f in os.listdir(sar) if f.startswith("sar-cpu-")) if os.path.isdir(sar) else []
            cpu = []
            for h in hosts:
                v = cpu_seconds(os.path.join(sar, f"sar-cpu-{h}.txt"), cores)
                cpu.append(f"{h}:{v / gib:.1f}" if v is not None and gib else f"{h}:n/a")
            b = os.path.join(d, f"metrics-before-{phase}.txt")
            a = os.path.join(d, f"metrics-after-{phase}.txt")

            def delta(name):
                x, y = metric(b, name), metric(a, name)
                return None if x is None or y is None else y - x

            att = delta("arca_gateway_meta_txn_attempts_total")
            conf = delta("arca_gateway_meta_txn_conflicts_total")
            txn = f"{att / longest:.0f}" if att is not None and longest else "n/a"
            print(f"| {label} | {len(rows)} | {phase} | {agg:.0f} | {per} | {lat} | {' / '.join(cpu)} | {txn} | {conf if conf is not None else 'n/a'} |")


if __name__ == "__main__":
    main()
