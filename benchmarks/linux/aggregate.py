#!/usr/bin/env python3
"""Aggregate repeated two-host.sh runs (labels `<config>-r<N>`) into one
Markdown row per configuration and phase: median, min, max and standard
deviation of throughput, the median of p50 / p95 / p99 latency, median
CPU-seconds and network bytes per GiB per host, the cache hit ratio
(hits / (hits + misses + fallbacks)), gateway RSS, and the error rate.

Usage: aggregate.py <results-dir> [--cores N] [--iface net0]
"""

import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summarize import cpu_seconds, metric, net_bytes  # noqa: E402


def parse_args(argv):
    cores, iface, root = 4, "net0", None
    i = 0
    while i < len(argv):
        if argv[i] == "--cores":
            cores = int(argv[i + 1]); i += 2
        elif argv[i] == "--iface":
            iface = argv[i + 1]; i += 2
        else:
            root = argv[i]; i += 1
    if root is None:
        sys.exit(__doc__)
    return root, cores, iface


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "n/a"
    med = statistics.median(xs)
    sd = statistics.pstdev(xs) if len(xs) > 1 else 0.0
    return f"{med:.1f} ({min(xs):.1f}–{max(xs):.1f}, sd {sd:.1f}, n={len(xs)})"


def med(xs, fmt="{:.1f}"):
    xs = [x for x in xs if x is not None]
    return "n/a" if not xs else fmt.format(statistics.median(xs))


def gateway_rss(path):
    if not os.path.exists(path):
        return None
    best = None
    for line in open(path, encoding="utf-8"):
        for m in re.finditer(r"arca-lake=(\d+)", line):
            v = int(m.group(1))
            best = v if best is None or v > best else best
    return None if best is None else best / 1024.0


def main():
    root, cores, iface = parse_args(sys.argv[1:])
    groups = {}
    for label in sorted(os.listdir(root)):
        m = re.match(r"^(.*)-r(\d+)$", label)
        d = os.path.join(root, label)
        if not m or not os.path.isdir(d):
            continue
        groups.setdefault(m.group(1), []).append(d)
    print("| config | phase | MiB/s median (min–max, sd, n) | p50 / p95 / p99 ms (median) | CPU-s/GiB per host (median) | net GB/GiB per host (median) | hit ratio | gateway-host max RSS MiB | errors |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for cfg, dirs in sorted(groups.items()):
        phases = sorted({p[:-6] for d in dirs for p in os.listdir(d) if re.match(r"(put|get\d+)\.jsonl$", p)},
                        key=lambda s: (0, 0) if s == "put" else (1, int(s[3:])))
        for phase in phases:
            thr, p50, p95, p99, errs, hit, rss = [], [], [], [], 0, [], []
            cpu, net = {}, {}
            for d in dirs:
                jl = os.path.join(d, phase + ".jsonl")
                if not os.path.exists(jl):
                    continue
                rows = [json.loads(l) for l in open(jl, encoding="utf-8") if l.strip().startswith("{")]
                if not rows:
                    continue
                s = rows[-1]
                thr.append(s["mib_per_s"]); p50.append(s["lat_ms_p50"]); p95.append(s.get("lat_ms_p95")); p99.append(s["lat_ms_p99"])
                errs += s["errors"] + s["mismatches"]
                gib = s["bytes"] / float(1 << 30) if s["bytes"] else None
                sar = os.path.join(d, phase)
                if os.path.isdir(sar) and gib:
                    for f in os.listdir(sar):
                        if f.startswith("sar-cpu-"):
                            h = f[8:-4]
                            v = cpu_seconds(os.path.join(sar, f), cores)
                            cpu.setdefault(h, []).append(None if v is None else v / gib)
                        if f.startswith("sar-net-"):
                            h = f[8:-4]
                            v = net_bytes(os.path.join(sar, f), iface)
                            net.setdefault(h, []).append(None if v is None else v / gib / 1e9)
                if phase.startswith("get"):
                    b = os.path.join(d, "metrics-before-" + phase + ".txt")
                    a = os.path.join(d, "metrics-after-" + phase + ".txt")
                    vals = {}
                    for name in ("hits", "misses", "fallbacks"):
                        x = metric(b, f"arca_gateway_cache_{name}_total"); y = metric(a, f"arca_gateway_cache_{name}_total")
                        vals[name] = None if x is None or y is None else y - x
                    if all(v is not None for v in vals.values()) and sum(vals.values()) > 0:
                        hit.append(100.0 * vals["hits"] / sum(vals.values()))
                rss.append(gateway_rss(os.path.join(d, "rss.txt")))
            cpu_cell = " / ".join(f"{h}:{med(v)}" for h, v in sorted(cpu.items())) or "n/a"
            net_cell = " / ".join(f"{h}:{med(v, '{:.2f}')}" for h, v in sorted(net.items())) or "n/a"
            print(f"| {cfg} | {phase} | {stats(thr)} | {med(p50)} / {med(p95)} / {med(p99)} | {cpu_cell} | {net_cell} | {med(hit, '{:.0f}%') if hit else 'n/a'} | {med(rss, '{:.0f}')} | {errs} |")


if __name__ == "__main__":
    main()
