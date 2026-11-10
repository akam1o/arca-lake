"""Compare rebalance-load.sh runs (Phase 3.10, R-93).

    python3 -I benchmarks/linux/summarize_rebalance.py NAME=DIR [NAME=DIR ...]

Per run, for the second window (labels "rebalance-*") against the first
("baseline-*"): PUT and GET throughput and latency, errors and SlowDown
refusals, seconds the gateway spent in write_rejected or worse, the
capacity spread between nodes (largest minus smallest used fraction of
the usable space) at the end, the largest GC backlog, bytes moved, the
converge time, samples where a node was below its reserve, and the
run's failures (body mismatches, acknowledged objects missing).
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def load(path):
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def window(rows, prefix, op):
    sel = [r for r in rows if r.get("label") == f"{prefix}-{op}" and r.get("errors", 0) >= 0]
    if not sel:
        return None
    secs = sum(r["duration_s"] for r in sel)
    mib = sum(r["bytes"] for r in sel) / (1 << 20)
    p50 = sorted(r["lat_ms_p50"] for r in sel)[len(sel) // 2]
    p99 = max(r["lat_ms_p99"] for r in sel)
    errors = sum(r["errors"] for r in sel)
    return {"mib_s": mib / secs if secs else 0.0, "p50": p50, "p99max": p99, "errors": errors}


def samples(path):
    """(time, source, metric, value) rows."""
    out = []
    if not path.exists():
        return out
    for line in path.read_text().splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) == 4:
            try:
                out.append((int(parts[0]), parts[1], parts[2], float(parts[3])))
            except ValueError:
                pass
    return out


def summarize(name, d):
    rows = load(d / "s3load.jsonl")
    s = samples(d / "samples.tsv")
    res = {"run": name}
    for prefix in ("baseline", "rebalance"):
        for op in ("put", "get"):
            w = window(rows, prefix, op)
            if w:
                res[f"{prefix}_{op}_mib_s"] = round(w["mib_s"], 1)
                res[f"{prefix}_{op}_p50_ms"] = round(w["p50"], 1)
                res[f"{prefix}_{op}_p99max_ms"] = round(w["p99max"], 1)
                res[f"{prefix}_{op}_errors"] = w["errors"]
    stderr = d / "s3load-stderr.log"
    res["slowdown_lines"] = (
        sum(1 for l in stderr.read_text(errors="replace").splitlines() if "SlowDown" in l)
        if stderr.exists()
        else 0
    )
    times = sorted({t for t, *_ in s})
    step = (times[1] - times[0]) if len(times) > 1 else 0
    gw = [(t, v) for t, src, m, v in s if src == "gw" and m.startswith("arca_gateway_admission_state")]
    res["write_rejected_s"] = sum(step for _, v in gw if v >= 3)
    res["worst_state"] = int(max((v for _, v in gw), default=0))
    res["gc_backlog_max_mib"] = round(
        max((v for _, src, m, v in s if m == "arca_controller_gc_backlog_bytes"), default=0) / (1 << 20), 1
    )
    moved = [v for _, src, m, v in s if m == "arca_controller_rebalance_moved_bytes_total"]
    res["moved_mib_max_counter"] = round(max(moved, default=0) / (1 << 20), 1)
    stops = [v for _, src, m, v in s if m == "arca_controller_rebalance_capacity_stops_total"]
    res["capacity_stops"] = int(max(stops, default=0))
    # Per sample time and node: total, available, reserved.
    dev = defaultdict(dict)
    for t, src, m, v in s:
        k = re.search(r'kind="(\w+)"', m)
        if m.startswith("arca_node_capacity_bytes") and k:
            dev[(t, src)][k.group(1)] = v
    violations = 0
    spread_at = {}
    for (t, src), kv in dev.items():
        if {"total", "available", "reserved"} <= kv.keys():
            if kv["available"] < kv["reserved"]:
                violations += 1
            usable = max(kv["total"] - kv["reserved"], 1)
            used = (kv["total"] - kv["available"]) / usable
            spread_at.setdefault(t, []).append(used)
    res["reserve_violation_samples"] = violations
    if spread_at:
        last = max(spread_at)
        res["final_spread_pct"] = round(100 * (max(spread_at[last]) - min(spread_at[last])), 1)
        res["max_spread_pct"] = round(
            100 * max(max(v) - min(v) for v in spread_at.values() if len(v) > 1), 1
        )
    runlog = (d / "run.log").read_text() if (d / "run.log").exists() else ""
    m = re.search(r"settled after (\d+) s", runlog)
    res["converge_s"] = int(m.group(1)) if m else None
    r = (d / "result.txt").read_text().strip() if (d / "result.txt").exists() else ""
    m = re.search(r"failures=(\d+)", r)
    res["failures"] = int(m.group(1)) if m else None
    return res


def main():
    runs = [summarize(*a.split("=", 1)[0:1], Path(a.split("=", 1)[1])) for a in sys.argv[1:]]
    keys = []
    for r in runs:
        for k in r:
            if k not in keys:
                keys.append(k)
    print("| metric | " + " | ".join(r["run"] for r in runs) + " |")
    print("| --- |" + " --- |" * len(runs))
    for k in keys[1:]:
        print(f"| {k} | " + " | ".join(str(r.get(k, "")) for r in runs) + " |")


if __name__ == "__main__":
    main()
