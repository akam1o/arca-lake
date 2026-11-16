"""Summaries of bench-cluster.sh runs (Phase 4.0).

    python3 -I benchmarks/linux/summarize_bench.py [--json] RUN_DIR...

Per run: the generator's figures (every client process summed: ops/s,
MiB/s; latency percentiles of the first process, or the worst of them),
errors by class, integrity violations, the generator's CPU; from the
samplers, over the measurement window (the samples after the warm-up,
the last two seconds dropped): CPU of each role (gateway, storage node,
controller, FoundationDB, generator) per host in cores, host CPU busy %,
network MiB/s, disk utilisation %, and FoundationDB's commit latency,
commits/s, conflicts/s, reads/s and its own CPU and limiting factor.
"""

import gzip
import json
import re
import statistics
import sys
from pathlib import Path


def opener(p):
    return gzip.open(p, "rt") if str(p).endswith(".gz") else open(p)


def summaries(d):
    out = []
    for p in sorted(d.glob("summary*.json")):
        for line in p.read_text().splitlines():
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def roles(prof):
    """pid -> role from fds.txt (`t pid n argv0 argv1`)."""
    r = {}
    p = prof / "fds.txt"
    if p.exists():
        for line in p.read_text().splitlines():
            f = line.split()
            if len(f) >= 4:
                exe = f[3]
                arg = f[4] if len(f) > 4 else ""
                if exe.startswith("arca-lake") and arg:
                    r[f[1]] = {"storage-node": "node", "gateway": "gateway", "controller": "controller"}.get(arg, arg)
                elif "fdbserver" in exe:
                    r[f[1]] = "fdb"
                elif "arcabench" in exe or "nodebench" in exe or "metaops" in exe:
                    r[f[1]] = "generator"
    return r


def window(d):
    """The generator's measurement window (epoch seconds) from the first
    client's timeline: the seconds it measured, the last one dropped."""
    for name in ("timeline.jsonl.gz", "timeline.jsonl"):
        p = d / name
        if p.exists():
            t = []
            with opener(p) as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if r.get("measuring"):
                        t.append(r["epoch_ms"] / 1000.0)
            if len(t) >= 3:
                return (t[0] - 1.0, t[-1] - 1.0)
    return None


def in_window(win, hms):
    """Whether a sampler's `HH:MM:SS` (UTC, as the hosts) is in `win`."""
    import datetime
    try:
        h, m, s = (int(x) for x in hms.split(":"))
    except ValueError:
        return False
    day = datetime.datetime.fromtimestamp(win[0], datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    t = day.timestamp() + h * 3600 + m * 60 + s
    if t < win[0] - 43200:
        t += 86400
    elif t > win[1] + 43200:
        t -= 86400
    return win[0] <= t <= win[1]


def cut(rows, win, skip, keep):
    """`(time, value)` rows in the window, or by count when there is none."""
    if win:
        return [v for t, v in rows if in_window(win, t)]
    return [v for _, v in rows[skip: len(rows) - keep if keep else None]]


def pidstat(prof, skip, keep, win=None):
    """Mean CPU cores per role over the window, and the peak RSS MiB."""
    p = prof / "pidstat.txt.gz"
    if not p.exists():
        p = prof / "pidstat.txt"
    if not p.exists():
        return {}, {}
    rmap = roles(prof)
    per_t = {}
    rss = {}
    with opener(p) as f:
        for line in f:
            if line.startswith("#") or not line.strip() or line.startswith("Linux"):
                continue
            x = line.split()
            if len(x) < 20:
                continue
            t, pid, cpu = x[0], x[2], x[7]
            role = rmap.get(pid)
            if role is None:
                if x[-1] == "fdbserver" and x[1] != "1000":
                    continue  # another user's FoundationDB (the system one)
                role = {"fdbserver": "fdb", "arcabench": "generator"}.get(x[-1], x[-1])
            try:
                per_t.setdefault(t, {}).setdefault(role, 0.0)
                per_t[t][role] += float(cpu) / 100.0
                rss[role] = max(rss.get(role, 0.0), float(x[12]) / 1024.0)
            except ValueError:
                pass
    ts = cut([(t, t) for t in per_t], win, skip, keep)
    out = {}
    for t in ts:
        for role, v in per_t[t].items():
            out.setdefault(role, []).append(v)
    return {k: round(statistics.mean(v), 2) for k, v in out.items()}, {k: round(v) for k, v in rss.items()}


def sar(prof, skip, keep, win=None):
    p = prof / "sar.txt.gz"
    if not p.exists():
        p = prof / "sar.txt"
    if not p.exists():
        return {}
    cpu, net, disk = [], {}, {}
    section = None
    with opener(p) as f:
        for line in f:
            x = line.split()
            if not x:
                continue
            if "%user" in x or "%usr" in x:
                section = "cpu"
                continue
            if "IFACE" in x:
                section = "net"
                continue
            if "DEV" in x and "%util" in x:
                section = "disk"
                udx = x.index("%util")
                continue
            if x[0] == "Average:" or len(x) < 3:
                continue
            if section == "cpu" and x[1] == "all":
                cpu.append((x[0], 100.0 - float(x[-1])))
            elif section == "net" and x[1] not in ("lo",):
                try:
                    net.setdefault(x[0], 0.0)
                    net[x[0]] += (float(x[4]) + float(x[5])) / 1024.0
                except (ValueError, IndexError):
                    pass
            elif section == "disk":
                try:
                    disk.setdefault(x[1], []).append((x[0], float(x[udx])))
                except (ValueError, IndexError, NameError):
                    pass
    cpu = cut(cpu, win, skip, keep)
    netv = cut(list(net.items()), win, skip, keep)
    res = {}
    if cpu:
        res["host_cpu_busy_pct"] = round(statistics.mean(cpu), 1)
        res["host_cpu_busy_p95"] = round(sorted(cpu)[int(len(cpu) * 0.95) - 1], 1)
    if netv:
        res["net_mib_s"] = round(statistics.mean(netv), 1)
    for dev, v in disk.items():
        v = cut(v, win, skip, keep)
        if v and dev.startswith(("sd", "vd", "nvme")):
            res[f"disk_{dev}_util_pct"] = round(statistics.mean(v), 1)
    return res


def fdb(prof, skip_s, keep_s, win=None):
    p = prof / "fdb.txt"
    if not p.exists():
        return {}
    rows = []
    for line in p.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass
    rows = [r for r in rows if "commit_s" in r]
    if len(rows) < 3:
        return {}
    t0 = rows[0]["ts"]
    t1 = rows[-1]["ts"]
    if win:
        rows = [r for r in rows if win[0] <= r["ts"] <= win[1]]
    else:
        rows = [r for r in rows if t0 + skip_s <= r["ts"] <= t1 - keep_s]
    if not rows:
        return {"fdb_window_samples": 0}
    def m(k, scale=1.0):
        v = [r[k] for r in rows if isinstance(r.get(k), (int, float))]
        return round(statistics.mean(v) * scale, 2) if v else None
    lim = {}
    for r in rows:
        lim[r.get("limiting")] = lim.get(r.get("limiting"), 0) + 1
    return {
        "fdb_commit_ms": m("commit_s", 1000), "fdb_read_ms": m("read_s", 1000),
        "fdb_commits_hz": m("commits_hz"), "fdb_started_hz": m("started_hz"),
        "fdb_conflicted_hz": m("conflicted_hz"), "fdb_reads_hz": m("reads_hz"),
        "fdb_cpu_cores": m("cpu_cores"), "fdb_disk_busy": m("disk_busy"),
        "fdb_limited_by": max(lim, key=lim.get) if lim else None,
        "fdb_kv_mib": m("kv_bytes", 1 / 1048576),
    }


def summarize(d):
    s = summaries(d)
    if not s:
        return {"run": d.name, "missing": True}
    first = s[0]
    warm = first.get("warmup_s", 0) or 0
    dur = first.get("measured_s", 0) or 0
    skip = int(warm) + 2
    res = {"run": d.name, "workload": first.get("workload"), "clients": len(s),
           "concurrency": first.get("concurrency"), "size": first.get("size"),
           "objects": first.get("objects"), "measured_s": round(dur, 1)}
    ops = {}
    for x in s:
        for k, v in x.get("ops", {}).items():
            o = ops.setdefault(k, {"ops_per_s": 0, "mib_per_s": 0, "errors": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "classes": {}})
            o["ops_per_s"] += v["ops_per_s"]
            o["mib_per_s"] += v["mib_per_s"]
            o["errors"] += v["errors"]
            for q, key in (("p50", "lat_ms_p50"), ("p95", "lat_ms_p95"), ("p99", "lat_ms_p99"), ("max", "lat_ms_max")):
                o[q] = max(o[q], v[key])
            for c, n in v.get("errors_by_class", {}).items():
                o["classes"][c] = o["classes"].get(c, 0) + n
    for k, o in ops.items():
        res[f"{k}_ops_s"] = round(o["ops_per_s"], 1)
        res[f"{k}_mib_s"] = round(o["mib_per_s"], 1)
        res[f"{k}_p50_ms"] = round(o["p50"], 2)
        res[f"{k}_p95_ms"] = round(o["p95"], 2)
        res[f"{k}_p99_ms"] = round(o["p99"], 2)
        res[f"{k}_max_ms"] = round(o["max"], 1)
        res[f"{k}_errors"] = o["errors"]
        if o["classes"]:
            res[f"{k}_error_classes"] = o["classes"]
    res["integrity_violations"] = sum(x.get("integrity_violations", 0) for x in s)
    res["unverified"] = sum(x.get("unverified", 0) for x in s)
    q = d / "quiesce.txt"
    if q.exists():
        # A run after a quiescence timeout is not valid (rule 8).
        res["quiesce"] = q.read_text().strip().split("=")[-1]
    res["client_cpu_util"] = round(max(x["client"]["cpu_util_of_threads"] for x in s), 2)
    res["client_saturated"] = any(x["client"]["client_saturated"] for x in s)
    keep = 2
    win = window(d)
    res["window"] = "generator" if win else "count"
    for h in ("a", "b"):
        prof = d / f"profile-{h}"
        if not prof.exists():
            continue
        cores, rss = pidstat(prof, skip, keep, win)
        for k, v in cores.items():
            res[f"{h}_{k}_cores"] = v
        for k, v in rss.items():
            res[f"{h}_{k}_rss_mib"] = v
        for k, v in sar(prof, skip, keep, win).items():
            res[f"{h}_{k}"] = v
        if h == "a":
            res.update(fdb(prof, skip, keep, win))
    return res


def main():
    args = sys.argv[1:]
    as_json = args and args[0] == "--json"
    if as_json:
        args = args[1:]
    rows = [summarize(Path(a)) for a in args]
    if as_json:
        for r in rows:
            print(json.dumps(r))
        return
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    print("| metric | " + " | ".join(str(r["run"]) for r in rows) + " |")
    print("| --- |" + " --- |" * len(rows))
    for k in keys[1:]:
        print(f"| {k} | " + " | ".join(str(r.get(k, "")) for r in rows) + " |")


if __name__ == "__main__":
    main()
