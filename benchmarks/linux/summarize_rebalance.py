"""Compare rebalance-load.sh runs (Phase 3.10, R-93).

    python3 -I benchmarks/linux/summarize_rebalance.py NAME=DIR [NAME=DIR ...]

Per run, for the second window (labels "rebalance-*") against the first
("baseline-*"): PUT and GET throughput and latency, errors and SlowDown
refusals, seconds the gateway spent in write_rejected or worse, the
capacity spread between nodes (largest minus smallest used fraction of
the usable space) at the end, the largest GC backlog, bytes moved, the
converge time, samples where a node was below its reserve, and the
run's failures (body mismatches, acknowledged objects missing).

Phase 3.14 adds the condition-A validity rules, fixed before the runs
(`--condition a`): an initial spread of at least 30 points between the
nodes' used fractions when rebalancing resumes; at least 512 MiB moved;
no capacity constraint (no write refused for capacity: never
`write_rejected`, no `SlowDown`; no capacity stop; every node's headroom
above its reserve at least 512 MiB; the admission trend may throttle,
recorded, as the pilots showed it does with ample room); settled
(rebalancing reports `balanced`) within the converge deadline; no
failure. The final spread is reported, not a rule: the first campaign
attempt showed it dominated by space a node had not compacted yet (the
node stopped by the fault), which rebalancing does not act on.
A run that breaks one is reported invalid with the rule, not used.
Condition B (`--condition b`) checks instead that retained bytes grew,
that no node went below its reserve, and that nothing was lost.
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
    ops = sum(r.get("ops", 0) for r in sel)
    med = lambda k: sorted(r.get(k, 0) for r in sel)[len(sel) // 2]
    p99 = max(r["lat_ms_p99"] for r in sel)
    errors = sum(r["errors"] for r in sel)
    return {
        "mib_s": mib / secs if secs else 0.0,
        "ops_s": ops / secs if secs else 0.0,
        "p50": med("lat_ms_p50"),
        "p95": med("lat_ms_p95"),
        "p99": med("lat_ms_p99"),
        "p99max": p99,
        "errors": errors,
        "error_rate": errors / (ops + errors) if ops + errors else 0.0,
    }


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
                res[f"{prefix}_{op}_ops_s"] = round(w["ops_s"], 2)
                res[f"{prefix}_{op}_p50_ms"] = round(w["p50"], 1)
                res[f"{prefix}_{op}_p95_ms"] = round(w["p95"], 1)
                res[f"{prefix}_{op}_p99_ms"] = round(w["p99"], 1)
                res[f"{prefix}_{op}_p99max_ms"] = round(w["p99max"], 1)
                res[f"{prefix}_{op}_errors"] = w["errors"]
                res[f"{prefix}_{op}_error_rate"] = round(w["error_rate"], 4)
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
    runlog = (d / "run.log").read_text() if (d / "run.log").exists() else ""
    m = re.search(r"rebalance resumed at (\d+)", runlog)
    resumed = int(m.group(1)) if m else None
    # Counters restart with a controller: positive steps per source.
    moved_t = increments(s, "arca_controller_rebalance_moved_bytes_total")
    res["moved_mib"] = round(sum(v for _, v in moved_t) / (1 << 20), 1)
    res["capacity_stops"] = int(sum(v for _, v in increments(s, "arca_controller_rebalance_capacity_stops_total")))
    res["skipped_chunks"] = int(sum(v for _, v in increments(s, "arca_controller_rebalance_skipped_chunks_total")))
    if resumed is not None and any(v > 0 for _, v in moved_t):
        last_move = max(t for t, v in moved_t if v > 0)
        res["moving_s_after_resume"] = last_move - resumed
        secs = max(last_move - resumed, step or 1)
        res["rebalance_mib_s"] = round(res["moved_mib"] / secs, 2)
    # Seconds where a group was Repairing while chunks moved.
    repairing = {t for t, src, m, v in s if m.startswith("arca_controller_groups_repairing") and v > 0}
    res["repair_rebalance_overlap_s"] = sum(step for t, v in moved_t if v > 0 and t in repairing)
    res["repairing_s"] = step * len(repairing)
    # Per sample time and node: total, available, reserved.
    dev = defaultdict(dict)
    for t, src, m, v in s:
        k = re.search(r'kind="(\w+)"', m)
        if m.startswith("arca_node_capacity_bytes") and k:
            dev[(t, src)][k.group(1)] = v
    violations = 0
    spread_at = {}
    headroom = []
    retained_at = defaultdict(float)
    for (t, src), kv in dev.items():
        if {"total", "available", "reserved"} <= kv.keys():
            if kv["available"] < kv["reserved"]:
                violations += 1
            headroom.append(kv["available"] - kv["reserved"])
            usable = max(kv["total"] - kv["reserved"], 1)
            used = (kv["total"] - kv["available"]) / usable
            spread_at.setdefault(t, []).append(used)
        if "reclaimable" in kv:
            retained_at[t] += kv["reclaimable"]
    res["reserve_violation_samples"] = violations
    res["min_headroom_mib"] = round(min(headroom, default=0) / (1 << 20), 1)
    res["reserved_mib_per_node"] = round(
        max((kv.get("reserved", 0) for kv in dev.values()), default=0) / (1 << 20), 1
    )
    if retained_at:
        res["reclaimable_mib_max"] = round(max(retained_at.values()) / (1 << 20), 1)
        first = retained_at[min(retained_at)]
        res["reclaimable_mib_growth"] = round((max(retained_at.values()) - first) / (1 << 20), 1)
    ret = defaultdict(float)
    for t, src, m, v in s:
        if m.startswith("arca_node_reclaim_retained_bytes"):
            ret[t] += v
    if ret:
        res["retained_mib_max"] = round(max(ret.values()) / (1 << 20), 1)
    if resumed is not None and spread_at:
        at = min(spread_at, key=lambda t: abs(t - resumed))
        res["spread_at_resume_pct"] = round(100 * (max(spread_at[at]) - min(spread_at[at])), 1)
    if spread_at:
        last = max(spread_at)
        res["final_spread_pct"] = round(100 * (max(spread_at[last]) - min(spread_at[last])), 1)
        res["max_spread_pct"] = round(
            100 * max(max(v) - min(v) for v in spread_at.values() if len(v) > 1), 1
        )
    m = re.search(r"settled after (\d+) s", runlog)
    res["converge_s"] = int(m.group(1)) if m else None
    r = (d / "result.txt").read_text().strip() if (d / "result.txt").exists() else ""
    m = re.search(r"failures=(\d+)", r)
    res["failures"] = int(m.group(1)) if m else None
    m = re.search(r"retention=(\S+)", r)
    res["retention"] = m.group(1) if m else "default"
    return res


def increments(s, metric):
    """(time, positive step) of a counter, per source (restarts ignored)."""
    out = []
    last = {}
    for t, src, m, v in sorted(s):
        if m != metric:
            continue
        if src in last and v >= last[src]:
            out.append((t, v - last[src]))
        last[src] = v
    return out


def validity(r, condition):
    """The rules fixed before the runs (module docstring); [] when valid."""
    broken = []
    if r.get("failures") != 0:
        broken.append("failures")
    if condition == "a":
        if (r.get("spread_at_resume_pct") or 0) < 30:
            broken.append("initial spread < 30 points")
        if r.get("moved_mib", 0) < 512:
            broken.append("moved < 512 MiB")
        if r.get("write_rejected_s", 0) > 0 or r.get("slowdown_lines", 0) > 0:
            broken.append("writes refused for capacity")
        if r.get("capacity_stops", 0) > 0:
            broken.append("capacity stop")
        if r.get("min_headroom_mib", 0) < 512:
            broken.append("headroom above reserve < 512 MiB")
        if r.get("converge_s") is None:
            broken.append("not settled")
    else:
        if r.get("reclaimable_mib_growth", 0) <= 0 and r.get("retained_mib_max", 0) <= 0:
            broken.append("nothing retained")
        if r.get("reserve_violation_samples", 0) > 0:
            broken.append("reserve violated")
    return broken


def main():
    args = sys.argv[1:]
    condition = None
    if len(args) >= 2 and args[0] == "--condition":
        condition = args[1]
        args = args[2:]
    runs = [summarize(*a.split("=", 1)[0:1], Path(a.split("=", 1)[1])) for a in args]
    if condition:
        for r in runs:
            broken = validity(r, condition)
            r["valid"] = "yes" if not broken else "no: " + "; ".join(broken)
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
