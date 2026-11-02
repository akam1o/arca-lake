#!/usr/bin/env python3
"""Reduce a repeat.sh results directory to the performance indicators the
regression check compares (Phase 2.3 §8), as one JSON document:

  put_mib_s, get_warm_mib_s, get_warm_p50_ms / p95 / p99 (durable config),
  cpu_s_per_gib_gateway_host, cpu_s_per_gib_node_host (durable warm GET),
  rss_mib (largest arca-lake process), hit_ratio (cache config, last pass),
  population_chunks_per_s (cache config, first GET pass),
  meta_txn_per_s (durable PUT, gateway 1).

Medians across repetitions. Usage: perf_summary.py <results-dir> [--cores N]
Repetition directories are named <config>-r<N> as repeat.sh writes them.
"""

import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summarize import cpu_seconds, metric  # noqa: E402


def last_line(path):
    rows = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    return rows[-1] if rows else None


def reps(root, config):
    return sorted(d for d in os.listdir(root) if re.match(rf"{re.escape(config)}-r\d+$", d))


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def rss_mib(path):
    best = None
    if not os.path.exists(path):
        return None
    for line in open(path, encoding="utf-8"):
        for m in re.finditer(r"arca-lake=(\d+)", line):
            v = int(m.group(1))
            best = v if best is None or v > best else best
    return None if best is None else best / 1024.0


def main():
    argv = sys.argv[1:]
    cores = 4
    if "--cores" in argv:
        i = argv.index("--cores")
        cores = int(argv[i + 1])
        del argv[i : i + 2]
    root = argv[0]
    durable = [c for c in ("durable-c4", "durable-c1") if reps(root, c)]
    cache = [c for c in ("none-c4", "none-c1", "ec-c4", "repl-c4") if reps(root, c)]
    out = {"results_dir": os.path.basename(os.path.abspath(root)), "cores": cores}
    if durable:
        cfg = durable[0]
        out["durable_config"] = cfg
        puts, gets, p50, p95, p99, cpu_gw, cpu_node, rss, txn = [], [], [], [], [], [], [], [], []
        for r in reps(root, cfg):
            d = os.path.join(root, r)
            put = last_line(os.path.join(d, "put.jsonl"))
            passes = sorted(
                (p for p in os.listdir(d) if re.match(r"get\d+\.jsonl$", p)),
                key=lambda s: int(s[3:-6]),
            )
            if put:
                puts.append(put["mib_per_s"])
                b, a = os.path.join(d, "metrics-before-put.txt"), os.path.join(d, "metrics-after-put.txt")
                x, y = metric(b, "arca_gateway_meta_txn_attempts_total"), metric(a, "arca_gateway_meta_txn_attempts_total")
                if x is not None and y is not None and put["duration_s"] > 0:
                    txn.append((y - x) / put["duration_s"])
            if passes:
                last = passes[-1]
                g = last_line(os.path.join(d, last))
                if g:
                    gets.append(g["mib_per_s"])
                    p50.append(g["lat_ms_p50"])
                    p95.append(g["lat_ms_p95"])
                    p99.append(g["lat_ms_p99"])
                    gib = g["bytes"] / float(1 << 30)
                    sar = os.path.join(d, last[:-6])
                    hosts = sorted(f[8:-4] for f in os.listdir(sar) if f.startswith("sar-cpu-")) if os.path.isdir(sar) else []
                    # two-host layout: "a" holds FoundationDB and two nodes,
                    # "b" the gateway (and one node); three-host: gateway on "c".
                    gw_host = "c" if "c" in hosts else "b"
                    node_host = "a"
                    for h, acc in ((gw_host, cpu_gw), (node_host, cpu_node)):
                        v = cpu_seconds(os.path.join(sar, f"sar-cpu-{h}.txt"), cores)
                        if v is not None and gib:
                            acc.append(v / gib)
            rss.append(rss_mib(os.path.join(d, "rss.txt")))
        out.update(
            put_mib_s=med(puts),
            get_warm_mib_s=med(gets),
            get_warm_p50_ms=med(p50),
            get_warm_p95_ms=med(p95),
            get_warm_p99_ms=med(p99),
            cpu_s_per_gib_gateway_host=med(cpu_gw),
            cpu_s_per_gib_node_host=med(cpu_node),
            rss_mib=med(rss),
            meta_txn_per_s=med(txn),
            repetitions=len(reps(root, cfg)),
        )
    if cache:
        cfg = cache[0]
        out["cache_config"] = cfg
        hits, pops = [], []
        for r in reps(root, cfg):
            d = os.path.join(root, r)
            passes = sorted(
                (p for p in os.listdir(d) if re.match(r"get\d+\.jsonl$", p)),
                key=lambda s: int(s[3:-6]),
            )
            if not passes:
                continue
            last, first = passes[-1][:-6], passes[0][:-6]
            b, a = os.path.join(d, f"metrics-before-{last}.txt"), os.path.join(d, f"metrics-after-{last}.txt")
            h0, h1 = metric(b, "arca_gateway_cache_hits_total"), metric(a, "arca_gateway_cache_hits_total")
            m0, m1 = metric(b, "arca_gateway_cache_misses_total"), metric(a, "arca_gateway_cache_misses_total")
            if None not in (h0, h1, m0, m1) and (h1 - h0 + m1 - m0) > 0:
                hits.append((h1 - h0) / (h1 - h0 + m1 - m0))
            b, a = os.path.join(d, f"metrics-before-{first}.txt"), os.path.join(d, f"metrics-after-{first}.txt")
            g = last_line(os.path.join(d, f"{first}.jsonl"))
            x, y = metric(b, "arca_gateway_cache_populated_total"), metric(a, "arca_gateway_cache_populated_total")
            if g and x is not None and y is not None and g["duration_s"] > 0:
                pops.append((y - x) / g["duration_s"])
        out.update(hit_ratio=med(hits), population_chunks_per_s=med(pops))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
