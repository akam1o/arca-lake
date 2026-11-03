"""Summary of a soak run made by benchmarks/linux/soak24.sh (Phase 3.4 §7.4).

Usage: python3 -I soak-summary.py RUN_DIR > summary.json

Per process name (processes restarted by faults keep their name): resident
memory, open file descriptors and threads at the start, at the end and at
most, plus the medians of the second and the last hour (growth check).
Metrics: the value of every counter and gauge of interest at the first
and the last scrape, summed per process name. Workload: operations,
errors, violations, GET/PUT p95/p99 per hour. Faults by type. Nothing
host-specific is read or printed.
"""

import collections
import gzip
import json
import os
import statistics
import sys

run = sys.argv[1]


def jsonl(path):
    out = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def med(v):
    return statistics.median(v) if v else None


samples = jsonl(os.path.join(run, "samples.jsonl"))
t0 = min((s["t"] for s in samples), default=0)
t1 = max((s["t"] for s in samples), default=0)
procs = {}
by = collections.defaultdict(list)
for s in samples:
    by[s["proc"]].append(s)
for name, v in sorted(by.items()):
    v.sort(key=lambda s: s["t"])
    hour2 = [s for s in v if t0 + 3600 <= s["t"] < t0 + 7200]
    last = [s for s in v if s["t"] >= t1 - 3600]
    procs[name] = {
        k: {
            "start": v[0][k],
            "end": v[-1][k],
            "max": max(s[k] for s in v),
            "median_hour2": med([s[k] for s in hour2]),
            "median_last_hour": med([s[k] for s in last]),
        }
        for k in ("rss_kib", "fds", "threads")
    }
    procs[name]["pids_seen"] = len({s["pid"] for s in v})

INTEREST = (
    "_in_flight", "_queued", "_queue_depth", "_running", "conflicts", "unknown",
    "orphan", "repair_tasks", "repair_queue", "scrub_", "cache_usage", "evicted",
    "gc_", "errors", "rejected", "exhausted", "tls_", "unrecoverable", "corrupt",
    "quarantine", "fallback", "denied", "tiering", "promot", "demot",
)
first, last = {}, {}
try:
    with gzip.open(os.path.join(run, "metrics.log.gz"), "rt") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 4:
                continue
            t, proc, metric, value = int(parts[0]), parts[1], parts[2], parts[-1]
            base = metric.split("{")[0]
            if not any(i in base for i in INTEREST):
                continue
            try:
                val = float(value)
            except ValueError:
                continue
            key = (proc, metric)
            if key not in first:
                first[key] = (t, val)
            last[key] = (t, val)
except OSError:
    pass
metrics = collections.defaultdict(lambda: {"first": 0.0, "last": 0.0})
for (proc, metric), (_, v) in first.items():
    metrics[f"{proc.split('-')[0] if proc[0] in 'dc' and proc[1:].isdigit() else proc} {metric}"]["first"] += v
for (proc, metric), (_, v) in last.items():
    metrics[f"{proc.split('-')[0] if proc[0] in 'dc' and proc[1:].isdigit() else proc} {metric}"]["last"] += v
metrics = {k: v for k, v in sorted(metrics.items()) if v["first"] or v["last"]}

load = jsonl(os.path.join(run, "logs", "soakload.log"))
reports = [r for r in load if "t_s" in r]
violations = [r for r in load if "violation" in r]
final = next((r for r in load if r.get("final")), None)
ops = collections.defaultdict(lambda: [0, 0])
for r in reports:
    for k, v in r.get("ops", {}).items():
        ops[k][0] += v["ok"]
        ops[k][1] += v["err"]
hours = collections.defaultdict(list)
for r in reports:
    hours[r["t_s"] // 3600].append(r)
per_hour = {
    str(h): {
        "get_p95_ms": med([r["get_p95_ms"] for r in v if r["gets"]]),
        "get_p99_ms": med([r["get_p99_ms"] for r in v if r["gets"]]),
        "put_p95_ms": med([r["put_p95_ms"] for r in v if r["puts"]]),
        "put_p99_ms": med([r["put_p99_ms"] for r in v if r["puts"]]),
        "ops": sum(sum(o["ok"] + o["err"] for o in r["ops"].values()) for r in v),
        "errors": sum(sum(o["err"] for o in r["ops"].values()) for r in v),
    }
    for h, v in sorted(hours.items())
}

faults = collections.Counter()
try:
    with open(os.path.join(run, "faults.log")) as f:
        for line in f:
            p = line.split()
            if len(p) >= 2:
                faults[p[1]] += 1
except OSError:
    pass

fdb = jsonl(os.path.join(run, "fdb.jsonl"))
fdb_ok = [x for x in fdb if "error" not in x]
disk = collections.defaultdict(list)
try:
    with open(os.path.join(run, "disk.log")) as f:
        for line in f:
            p = line.split()
            if len(p) == 3 and p[2].isdigit():
                disk[p[1]].append(int(p[2]))
except OSError:
    pass

print(json.dumps({
    "duration_s": t1 - t0,
    "processes": procs,
    "metrics": metrics,
    "workload": {
        "final": final,
        "ops": {k: {"ok": v[0], "err": v[1]} for k, v in sorted(ops.items())},
        "violations": len(violations),
        "violation_details": violations[:50],
        "per_hour": per_hour,
    },
    "faults": dict(faults),
    "fdb": {
        "samples": len(fdb), "unavailable_samples": len(fdb) - len(fdb_ok),
        "first": fdb_ok[0] if fdb_ok else None, "last": fdb_ok[-1] if fdb_ok else None,
        "unhealthy_samples": sum(1 for x in fdb_ok if not x.get("healthy")),
    },
    "disk_bytes": {k: {"start": v[0], "end": v[-1], "max": max(v)} for k, v in sorted(disk.items())},
}, indent=1, default=str))
