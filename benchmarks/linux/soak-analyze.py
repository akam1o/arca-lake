#!/usr/bin/env python3
"""Long-run degradation analysis of a soak24.sh run directory (Phase 3.14):
hourly RSS, file descriptors and threads per process, workload operations,
errors and latency per hour, FoundationDB status, `write_rejected`
episodes, retained and reserved bytes, backlogs, node heap, counter
totals and faults.

    python3 -I benchmarks/linux/soak-analyze.py RUN_DIR [END_EPOCH] > analysis.json

END_EPOCH (seconds) ignores samples after the workload stopped."""
import collections
import gzip
import json
import statistics
import sys

run = sys.argv[1]
start = int(open(f"{run}/started").read().strip())
end = int(sys.argv[2]) if len(sys.argv) > 2 else None


def hour(t):
    return (t - start) // 3600


def jsonl(path):
    for line in open(path):
        line = line.strip()
        if line.startswith("{"):
            try:
                yield json.loads(line)
            except ValueError:
                pass


out = {}
# Processes: RSS, FDs, threads per hour (median), first/last/max.
procs = collections.defaultdict(lambda: collections.defaultdict(list))
for r in jsonl(f"{run}/samples.jsonl"):
    if end and r["t"] > end:
        continue
    procs[r["proc"]][hour(r["t"])].append(r)
pt = {}
for p, hrs in sorted(procs.items()):
    hs = sorted(hrs)
    rss = {h: statistics.median(x["rss_kib"] for x in hrs[h]) / 1024 for h in hs}
    fds = {h: statistics.median(x["fds"] for x in hrs[h]) for h in hs}
    thr = {h: statistics.median(x["threads"] for x in hrs[h]) for h in hs}
    pt[p] = {
        "rss_mib_by_hour": [round(rss[h], 1) for h in hs],
        "rss_mib_max": round(max(x["rss_kib"] for h in hs for x in hrs[h]) / 1024, 1),
        "fds_by_hour": [fds[h] for h in hs],
        "fds_max": max(x["fds"] for h in hs for x in hrs[h]),
        "threads_by_hour": [thr[h] for h in hs],
        "restarts_seen": len({x["pid"] for h in hs for x in hrs[h]}) - 1,
    }
out["processes"] = pt

# Workload per hour.
wl = collections.defaultdict(lambda: {"ok": collections.Counter(), "err": collections.Counter(),
                                      "get_p95": [], "get_p99": [], "put_p95": [], "put_p99": [],
                                      "gets": 0, "puts": 0})
violations = 0
for r in jsonl(f"{run}/logs/soakload.log"):
    if "epoch_ms" not in r:
        continue
    t = r["epoch_ms"] // 1000
    if end and t > end:
        continue
    h = wl[hour(t)]
    for op, v in r.get("ops", {}).items():
        h["ok"][op] += v["ok"]
        h["err"][op] += v["err"]
    for k in ("get_p95", "get_p99", "put_p95", "put_p99"):
        v = r.get(k + "_ms")
        if v:
            h[k].append(v)
    h["gets"] += r.get("gets", 0)
    h["puts"] += r.get("puts", 0)
    violations = max(violations, r.get("violations", 0))
rows = []
for hh in sorted(wl):
    h = wl[hh]
    ok = sum(h["ok"].values())
    err = sum(h["err"].values())
    rows.append({
        "hour": hh,
        "ops_ok": ok, "ops_err": err,
        "put_ok": h["ok"]["put"] + h["ok"]["vput"] + h["ok"]["multipart"] + h["ok"]["put_conditional"] + h["ok"]["copy"],
        "put_err": h["err"]["put"] + h["err"]["vput"] + h["err"]["multipart"] + h["err"]["put_conditional"] + h["err"]["copy"],
        "get_ok": h["ok"]["get"] + h["ok"]["vget"] + h["ok"]["vget_version"] + h["ok"]["get_range"] + h["ok"]["head"],
        "get_err": h["err"]["get"] + h["err"]["vget"] + h["err"]["vget_version"] + h["err"]["get_range"] + h["err"]["head"],
        "delete_ok": h["ok"]["delete"] + h["ok"]["vdelete"] + h["ok"]["vdelete_version"],
        "delete_err": h["err"]["delete"] + h["err"]["vdelete"] + h["err"]["vdelete_version"],
        "get_p95_ms": round(statistics.median(h["get_p95"]), 1) if h["get_p95"] else None,
        "get_p99_ms": round(statistics.median(h["get_p99"]), 1) if h["get_p99"] else None,
        "put_p95_ms": round(statistics.median(h["put_p95"]), 1) if h["put_p95"] else None,
        "put_p99_ms": round(statistics.median(h["put_p99"]), 1) if h["put_p99"] else None,
    })
out["workload_by_hour"] = rows
out["violations"] = violations

# FoundationDB status samples.
fdb = [r for r in jsonl(f"{run}/fdb.jsonl") if not end or r["t"] <= end]
out["fdb"] = {
    "samples": len(fdb),
    "unhealthy_samples": sum(1 for r in fdb if r.get("healthy") is False),
    "not_fully_recovered_samples": sum(1 for r in fdb if r.get("recovery") not in (None, "fully_recovered")),
    "processes_min": min((r.get("processes") or 0) for r in fdb) if fdb else None,
}

# Metrics.
series = collections.defaultdict(list)
keep = ("arca_gateway_admission_state", "arca_node_reclaim_retained_bytes", "arca_controller_capacity_reserved_bytes",
        "arca_controller_capacity_reservations", "arca_controller_gc_backlog_bytes", "arca_controller_repair_queue_depth",
        "arca_controller_groups_repairing", "arca_controller_rebalance_moved_bytes_total", "arca_controller_reclaimed_bytes_total",
        "arca_controller_leader", "arca_node_process_memory_bytes{kind=\"heap_in_use\"}",
        "arca_controller_capacity_reservation_refusals_total", "arca_node_compaction_backlog_bytes",
        "arca_controller_repairs_completed_total", "arca_controller_repairs_started_total",
        "arca_controller_rebalance_capacity_stops_total")
with gzip.open(f"{run}/metrics.log.gz", "rt") as f:
    for line in f:
        parts = line.split()
        if len(parts) != 4:
            continue
        t, src, m, v = parts
        t = int(t)
        if end and t > end:
            continue
        if m in keep or m.split("{")[0] in keep and "{" not in m:
            try:
                series[(src, m)].append((t, float(v)))
            except ValueError:
                pass
# Admission state timeline of gw-a.
adm = sorted(series.get(("gw-a", "arca_gateway_admission_state"), []))
episodes = []
cur = None
for t, v in adm:
    if v >= 3 and cur is None:
        cur = t
    elif v < 3 and cur is not None:
        episodes.append((cur, t))
        cur = None
if cur is not None:
    episodes.append((cur, adm[-1][0]))
step = (adm[1][0] - adm[0][0]) if len(adm) > 1 else 60
out["write_rejected"] = {
    "episodes": len(episodes),
    "episodes_h": [(round((a - start) / 3600, 2), round((b - a) / 3600, 2)) for a, b in episodes],
    "total_h": round(sum(b - a for a, b in episodes) / 3600, 2),
    "share_of_run": round(sum(b - a for a, b in episodes) / max(1, adm[-1][0] - start), 3) if adm else None,
    "state_counts": dict(collections.Counter(int(v) for _, v in adm)),
    "sample_step_s": step,
}


def by_hour_max(src_filter, m):
    acc = collections.defaultdict(float)
    per = collections.defaultdict(dict)
    for (src, mm), pts in series.items():
        if mm != m or not src_filter(src):
            continue
        for t, v in pts:
            per[t][src] = v
    hrs = collections.defaultdict(list)
    for t, d in per.items():
        hrs[hour(t)].append(sum(d.values()))
    return [round(max(hrs[h]), 1) for h in sorted(hrs)]


mib = 1 / 1048576
out["retained_mib_all_nodes_max_by_hour"] = [round(x * mib) for x in by_hour_max(lambda s: s.startswith("d"), "arca_node_reclaim_retained_bytes")]
out["reserved_mib_max_by_hour"] = [round(x * mib) for x in by_hour_max(lambda s: s.startswith("ctl"), "arca_controller_capacity_reserved_bytes")]
out["reservations_max_by_hour"] = by_hour_max(lambda s: s.startswith("ctl"), "arca_controller_capacity_reservations")
out["gc_backlog_mib_max_by_hour"] = [round(x * mib) for x in by_hour_max(lambda s: s.startswith("ctl"), "arca_controller_gc_backlog_bytes")]
out["repair_queue_max_by_hour"] = by_hour_max(lambda s: s.startswith("ctl"), "arca_controller_repair_queue_depth")
out["groups_repairing_max_by_hour"] = by_hour_max(lambda s: s.startswith("ctl"), "arca_controller_groups_repairing")
out["compaction_backlog_mib_max_by_hour"] = [round(x * mib) for x in by_hour_max(lambda s: True, "arca_node_compaction_backlog_bytes")]
heap = {}
for (src, m), pts in series.items():
    if m == 'arca_node_process_memory_bytes{kind="heap_in_use"}':
        hrs = collections.defaultdict(list)
        for t, v in pts:
            hrs[hour(t)].append(v * mib)
        heap[src] = [round(statistics.median(hrs[h]), 1) for h in sorted(hrs)]
out["node_heap_in_use_mib_by_hour"] = heap


def counter_total(m):
    tot = 0.0
    for (src, mm), pts in series.items():
        if mm != m:
            continue
        pts.sort()
        last = None
        for _, v in pts:
            if last is not None and v >= last:
                tot += v - last
            last = v
    return tot


out["totals"] = {
    "reclaimed_gib": round(counter_total("arca_controller_reclaimed_bytes_total") / 2**30, 2),
    "rebalance_moved_gib": round(counter_total("arca_controller_rebalance_moved_bytes_total") / 2**30, 2),
    "repairs_started": counter_total("arca_controller_repairs_started_total"),
    "repairs_completed": counter_total("arca_controller_repairs_completed_total"),
    "reservation_refusals": counter_total("arca_controller_capacity_reservation_refusals_total"),
    "rebalance_capacity_stops": counter_total("arca_controller_rebalance_capacity_stops_total"),
}
faults = collections.Counter()
for line in open(f"{run}/faults.log"):
    parts = line.split()
    if len(parts) >= 2:
        faults[parts[1]] += 1
out["faults"] = dict(faults)
json.dump(out, sys.stdout, indent=1)
