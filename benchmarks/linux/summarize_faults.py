#!/usr/bin/env python3
"""Summarize netns-faults.sh scenario results as Markdown rows.

For every `<results>/<scenario>/` directory: the PUT line of `put.jsonl`
(success count, throughput, latency percentiles), the verify pass, and
the gateway RPC counters (stalled, deadline exceeded, retries, exhausted,
population failures) as the delta between `gw-before.txt` and
`gw-after.txt`.

Usage: summarize_faults.py <results-dir> [<results-dir> ...]
"""

import json
import os
import sys


def metric(path, name):
    if not os.path.exists(path):
        return None
    for line in open(path, encoding="utf-8"):
        if line.startswith(name + " "):
            return int(line.split()[1])
    return None


def delta(d, name):
    a = metric(os.path.join(d, "gw-before.txt"), name)
    b = metric(os.path.join(d, "gw-after.txt"), name)
    return "n/a" if a is None or b is None else str(b - a)


def last_line(path):
    if not os.path.exists(path):
        return None
    rows = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip().startswith("{")]
    return rows[-1] if rows else None


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    print("| run | scenario | PUT ok / total | PUT MiB/s | p50 / p95 / p99 ms | verify ok / missing / mismatch | stalled | deadline | retries | exhausted |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for root in sys.argv[1:]:
        run = os.path.basename(os.path.normpath(root))
        for sc in sorted(os.listdir(root)):
            d = os.path.join(root, sc)
            if not os.path.isdir(d):
                continue
            put = last_line(os.path.join(d, "put.jsonl"))
            ver = last_line(os.path.join(d, "verify.jsonl"))
            if put is None:
                continue
            total = put["ops"] + put["errors"]
            lat = f"{put['lat_ms_p50']:.0f} / {put.get('lat_ms_p95', 0):.0f} / {put['lat_ms_p99']:.0f}"
            vcell = "n/a" if ver is None else f"{ver['ops']} / {ver['missing']} / {ver['mismatches']}"
            print(
                f"| {run} | {sc} | {put['ops']} / {total} | {put['mib_per_s']:.1f} | {lat} | {vcell} | "
                f"{delta(d, 'arca_gateway_node_rpc_stalled_total')} | "
                f"{delta(d, 'arca_gateway_node_rpc_deadline_exceeded_total')} | "
                f"{delta(d, 'arca_gateway_node_rpc_retries_total')} | "
                f"{delta(d, 'arca_gateway_node_rpc_exhausted_total')} |"
            )


if __name__ == "__main__":
    main()
