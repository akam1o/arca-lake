"""Long-term memory and descriptor trends of a soak (Phase 3.4).

Usage: python3 -I soak-memory.py RUN_DIR [WARMUP_S] > memory.json

Per process name and per process lifetime (a fault restart starts a new
PID): RSS and open descriptors at the start, the end and at most; the
least-squares slope after the warm-up (MiB/h, FDs/h) with R²; the median of
the second hour against the median of the last hour of the longest
lifetime (same workload, warm caches on both sides). Per gateway and node
the correlation of RSS with its in-flight work (population queue, PUTs and
GETs in flight) from the metrics log. Leader changes from the controllers'
logs, and each controller's RSS before and an hour after.

A slope with R² close to 1 over many hours after the warm-up is growth to
explain (a cache filling to its bound, a queue, allocator retention, or a
leak); a flat or bounded series is not.
"""

import collections
import gzip
import json
import math
import os
import re
import statistics
import sys

run = sys.argv[1]
warmup = int(sys.argv[2]) if len(sys.argv) > 2 else 7200


def fit(points):
    if len(points) < 10:
        return None
    xs = [p[0] / 3600 for p in points]
    ys = [p[1] for p in points]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((y - (my + slope * (x - mx))) ** 2 for x, y in zip(xs, ys))
    return {"slope_per_h": round(slope, 3), "r2": round(1 - ss_res / ss_tot, 3) if ss_tot else 1.0, "points": len(points)}


samples = []
with open(os.path.join(run, "samples.jsonl")) as f:
    for line in f:
        if line.startswith("{"):
            samples.append(json.loads(line))
t0 = min(s["t"] for s in samples)
t1 = max(s["t"] for s in samples)
by = collections.defaultdict(lambda: collections.defaultdict(list))
for s in samples:
    by[s["proc"]][s["pid"]].append(s)

procs = {}
for name, pids in sorted(by.items()):
    lifetimes = []
    for pid, v in pids.items():
        v.sort(key=lambda s: s["t"])
        lifetimes.append((v[-1]["t"] - v[0]["t"], pid, v))
    lifetimes.sort(reverse=True)
    span, pid, v = lifetimes[0]
    after = [s for s in v if s["t"] - t0 >= warmup]
    rss_fit = fit([(s["t"] - t0, s["rss_kib"] / 1024) for s in after])
    fd_fit = fit([(s["t"] - t0, s["fds"]) for s in after])
    h2 = [s["rss_kib"] / 1024 for s in v if 3600 <= s["t"] - t0 < 7200]
    last = [s["rss_kib"] / 1024 for s in v if s["t"] >= t1 - 3600]
    allv = [s for vv in pids.values() for s in vv]
    procs[name] = {
        "lifetimes": len(pids),
        "longest_lifetime_h": round(span / 3600, 2),
        "rss_mib": {"start": round(v[0]["rss_kib"] / 1024, 1), "end": round(v[-1]["rss_kib"] / 1024, 1),
                     "max_any_lifetime": round(max(s["rss_kib"] for s in allv) / 1024, 1),
                     "median_hour2": round(statistics.median(h2), 1) if h2 else None,
                     "median_last_hour": round(statistics.median(last), 1) if last else None,
                     "trend_after_warmup": rss_fit},
        "fds": {"start": v[0]["fds"], "end": v[-1]["fds"], "max_any_lifetime": max(s["fds"] for s in allv),
                "trend_after_warmup": fd_fit},
        "threads": {"start": v[0]["threads"], "end": v[-1]["threads"], "max_any_lifetime": max(s["threads"] for s in allv)},
    }

# In-flight work per process from the metrics log (one scrape a minute).
QUEUE = ("arca_gateway_population_queue_queued", "arca_gateway_population_queue_running",
         "arca_gateway_put_in_flight", "arca_node_put_in_flight", "arca_node_get_in_flight")
work = collections.defaultdict(dict)
try:
    with gzip.open(os.path.join(run, "metrics.log.gz"), "rt") as f:
        for line in f:
            p = line.split()
            if len(p) >= 4 and p[2].split("{")[0] in QUEUE:
                try:
                    work[p[1]][int(p[0])] = work[p[1]].get(int(p[0]), 0.0) + float(p[3])
                except ValueError:
                    pass
except OSError:
    pass


def corr(a, b):
    if len(a) < 10 or statistics.pstdev(a) == 0 or statistics.pstdev(b) == 0:
        return None
    ma, mb = statistics.fmean(a), statistics.fmean(b)
    cov = statistics.fmean([(x - ma) * (y - mb) for x, y in zip(a, b)])
    return round(cov / (statistics.pstdev(a) * statistics.pstdev(b)), 3)


for name, w in work.items():
    if name not in by:
        continue
    pairs = []
    for vv in by[name].values():
        for s in vv:
            # the scrape of the same minute
            t = min(w, key=lambda x: abs(x - s["t"])) if w else None
            if t is not None and abs(t - s["t"]) <= 30:
                pairs.append((s["rss_kib"] / 1024, w[t]))
    procs[name]["rss_vs_in_flight_corr"] = corr([p[0] for p in pairs], [p[1] for p in pairs])

# Leader changes and controller memory around them.
changes = []
for path in sorted(os.listdir(os.path.join(run, "logs"))):
    if not path.startswith("ctl-"):
        continue
    with open(os.path.join(run, "logs", path), errors="replace") as f:
        for line in f:
            m = re.search(r"(acquired|released) leadership controller=(\S+)", line)
            if m:
                changes.append((line.split()[0], m.group(1), m.group(2)))
changes.sort()


def rss_at(name, t):
    v = [s for vv in by.get(name, {}).values() for s in vv if abs(s["t"] - t) <= 90]
    return round(v[0]["rss_kib"] / 1024, 1) if v else None


import datetime
leader = []
for when, what, who in changes:
    try:
        t = datetime.datetime.strptime(when[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        continue
    leader.append({"t": when[:19] + "Z", "event": what, "controller": who,
                   "rss_mib_at": rss_at(who, t), "rss_mib_1h_later": rss_at(who, t + 3600)})

print(json.dumps({"duration_h": round((t1 - t0) / 3600, 2), "warmup_s": warmup, "processes": procs,
                  "leader_events": leader}, indent=1))
