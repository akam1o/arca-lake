#!/usr/bin/env python3
"""Summarize two-host.sh results as Markdown rows.

Reads every `<results>/<label>/` directory: `put.jsonl`, `get<N>.jsonl`, the
sar captures of each phase, and the gateway metrics snapshots. Computes per
phase: throughput, latency percentiles, CPU-seconds per GiB on each host
(from `sar -u`: (100 - %idle) * cores * seconds / 100), network bytes per
GiB on each host (from `sar -n DEV` eth totals rx+tx), and the cache hit /
miss / degraded counters delta of the phase.

Usage: summarize.py <results-dir> [--cores N] [--iface net0]
"""

import json
import os
import re
import sys


def parse_args(argv):
    cores, iface, root = 4, "net0", None
    i = 0
    while i < len(argv):
        if argv[i] == "--cores":
            cores = int(argv[i + 1])
            i += 2
        elif argv[i] == "--iface":
            iface = argv[i + 1]
            i += 2
        else:
            root = argv[i]
            i += 1
    if root is None:
        sys.exit(__doc__)
    return root, cores, iface


def cpu_seconds(path, cores):
    """CPU-seconds consumed over the capture (every non-average sample)."""
    if not os.path.exists(path):
        return None
    busy = []
    for line in open(path, encoding="utf-8", errors="replace"):
        parts = line.split()
        if len(parts) < 8 or parts[0] == "Average:" or not re.match(r"^\d\d:\d\d:\d\d", parts[0]):
            continue
        try:
            idle = float(parts[-1])
        except ValueError:
            continue
        busy.append(100.0 - idle)
    if not busy:
        return None
    return sum(busy) / 100.0 * cores  # one sample per second


def net_bytes(path, iface):
    """Bytes received + transmitted on `iface` over the capture."""
    if not os.path.exists(path):
        return None
    total = 0.0
    seen = False
    for line in open(path, encoding="utf-8", errors="replace"):
        parts = line.split()
        if len(parts) < 6 or parts[0] == "Average:" or parts[1] != iface:
            continue
        if not re.match(r"^\d\d:\d\d:\d\d", parts[0]):
            continue
        try:
            rx_kb, tx_kb = float(parts[4]), float(parts[5])
        except ValueError:
            continue
        total += (rx_kb + tx_kb) * 1024.0
        seen = True
    return total if seen else None


def disk_read_bytes(path):
    """Bytes read from block devices over the capture (`sar -b`, bread/s in
    512-byte blocks, one sample per second)."""
    if not os.path.exists(path):
        return None
    total = 0.0
    seen = False
    header = None
    for line in open(path, encoding="utf-8", errors="replace"):
        parts = line.split()
        if len(parts) < 5 or parts[0] == "Average:" or not re.match(r"^\d\d:\d\d:\d\d", parts[0]):
            continue
        if "bread/s" in parts:
            header = parts
            continue
        if header is None:
            continue
        try:
            total += float(parts[header.index("bread/s")]) * 512.0
        except (ValueError, IndexError):
            continue
        seen = True
    return total if seen else None


def metric(path, name):
    if not os.path.exists(path):
        return None
    for line in open(path, encoding="utf-8"):
        if line.startswith(name + " "):
            return int(line.split()[1])
    return None


def fmt(v, spec="{:.1f}"):
    return "n/a" if v is None else spec.format(v)


def main():
    root, cores, iface = parse_args(sys.argv[1:])
    print("| label | phase | ops | MiB/s | ops/s | p50 ms | p99 ms | errors | CPU-s/GiB per host | net B/GiB per host | disk read B/GiB per host | hits | misses | degraded | fallbacks |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for label in sorted(os.listdir(root)):
        d = os.path.join(root, label)
        if not os.path.isdir(d):
            continue
        phases = ["put"] + sorted(
            (p[:-6] for p in os.listdir(d) if re.match(r"get\d+\.jsonl$", p)),
            key=lambda s: int(s[3:]),
        )
        for phase in phases:
            jl = os.path.join(d, phase + ".jsonl")
            if not os.path.exists(jl):
                continue
            rows = [json.loads(l) for l in open(jl, encoding="utf-8") if l.strip()]
            if not rows:
                continue
            s = rows[-1]
            gib = s["bytes"] / float(1 << 30) if s["bytes"] else None
            sar = os.path.join(d, phase)
            per = lambda v: (v / gib) if (v is not None and gib) else None
            hosts = sorted(
                f[8:-4] for f in os.listdir(sar) if f.startswith("sar-cpu-")
            ) if os.path.isdir(sar) else []
            cpu = {h: per(cpu_seconds(os.path.join(sar, f"sar-cpu-{h}.txt"), cores)) for h in hosts}
            net = {h: per(net_bytes(os.path.join(sar, f"sar-net-{h}.txt"), iface)) for h in hosts}
            cpu_cell = " / ".join(f"{h}:{fmt(cpu[h])}" for h in hosts) or "n/a"
            net_cell = " / ".join(f"{h}:{fmt(net[h], '{:.2e}')}" for h in hosts) or "n/a"
            disk = {h: per(disk_read_bytes(os.path.join(sar, f"sar-disk-{h}.txt"))) for h in hosts}
            disk_cell = " / ".join(f"{h}:{fmt(disk[h], '{:.2e}')}" for h in hosts) or "n/a"
            hits = misses = degraded = fallbacks = None
            if phase.startswith("get"):
                b = os.path.join(d, "metrics-before-" + phase + ".txt")
                a = os.path.join(d, "metrics-after-" + phase + ".txt")

                def delta(name):
                    x, y = metric(b, name), metric(a, name)
                    return None if x is None or y is None else y - x

                hits = delta("arca_gateway_cache_hits_total")
                misses = delta("arca_gateway_cache_misses_total")
                degraded = delta("arca_gateway_cache_degraded_hits_total")
                fallbacks = delta("arca_gateway_cache_fallbacks_total")
            print(
                f"| {label} | {phase} | {s['ops']} | {s['mib_per_s']:.1f} | {s['ops_per_s']:.2f} | "
                f"{s['lat_ms_p50']:.1f} | {s['lat_ms_p99']:.1f} | {s['errors']}+{s['mismatches']} | "
                f"{cpu_cell} | {net_cell} | {disk_cell} | "
                f"{fmt(hits, '{}')} | {fmt(misses, '{}')} | {fmt(degraded, '{}')} | {fmt(fallbacks, '{}')} |"
            )


if __name__ == "__main__":
    main()
