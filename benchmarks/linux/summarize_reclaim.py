#!/usr/bin/env python3
"""Summarize a reclaim-load.sh output directory (Phase 3.13): per s3load
label, the number of runs, errors, ops/s (median, interquartile range) and
p95/p99 latency; the controller's reclaim rate; each node's CPU seconds,
largest RSS and pending-reclaim file size.

    summarize_reclaim.py <out-dir>
"""
import collections
import json
import statistics as st
import sys


def quartiles(xs):
    return st.quantiles(xs, n=4) if len(xs) > 3 else [min(xs or [0]), 0, max(xs or [0])]


def main():
    d = sys.argv[1]
    by = collections.defaultdict(list)
    for line in open(f"{d}/s3load.jsonl"):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        by[r.get("label")].append(r)
    for label, rs in sorted(by.items()):
        ok = [r for r in rs if r.get("errors", 0) == 0]
        ops = [r["ops_per_s"] for r in ok] or [0]
        p95 = [r["lat_ms_p95"] for r in ok] or [0]
        p99 = [r["lat_ms_p99"] for r in ok] or [0]
        q = quartiles(ops)
        print(
            f"{label}: runs={len(rs)} clean_runs={len(ok)} errors={sum(max(r.get('errors', 0), 0) for r in rs)} "
            f"mismatches={sum(r.get('mismatches', 0) for r in rs)} ops/s median={st.median(ops):.1f} "
            f"IQR={q[0]:.1f}-{q[2]:.1f} p95 median={st.median(p95):.1f} p99 median={st.median(p99):.1f} "
            f"p99 max={max(p99):.1f}"
        )
    s = collections.defaultdict(list)
    for line in open(f"{d}/samples.tsv"):
        parts = line.rstrip("\n").split("\t")
        if parts[0] == "time" or len(parts) != 4:
            continue
        try:
            s[(parts[1], parts[2])].append((int(parts[0]), float(parts[3])))
        except ValueError:
            continue

    def rate(key):
        xs = s.get(key, [])
        return (xs[-1][1] - xs[0][1]) / (xs[-1][0] - xs[0][0]) if len(xs) > 1 and xs[-1][0] > xs[0][0] else 0.0

    rec = s.get(("ctl", "arca_controller_reclaimed_total"), [(0, 0)])
    print(
        f"reclaimed records/s={rate(('ctl', 'arca_controller_reclaimed_total')):.2f} "
        f"reclaimed MiB/s={rate(('ctl', 'arca_controller_reclaimed_bytes_total')) / 2**20:.2f} "
        f"reclaimed total={rec[-1][1]:.0f}"
    )
    for n in ("d0", "d1", "d2"):
        cpu = s.get((n, "cpu_ticks"), [(0, 0), (1, 0)])
        rss = [v for _, v in s.get((n, "rss_kib"), [(0, 0)])]
        pf = [v for _, v in s.get((n, "pending_files_bytes"), [(0, 0)])]
        print(
            f"{n}: cpu_s={(cpu[-1][1] - cpu[0][1]) / 100:.1f} rss_max_MiB={max(rss) / 1024:.1f} "
            f"pending_files_max_KiB={max(pf) / 1024:.1f} end_KiB={pf[-1] / 1024:.1f}"
        )


if __name__ == "__main__":
    main()
