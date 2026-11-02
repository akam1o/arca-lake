#!/usr/bin/env python3
"""Align the per-second series of a PUT profile (put-profile.sh) and show
where the time went in the seconds in which throughput collapsed (R-48).

Reads every `<results>/<label>/` with `put.jsonl`, `put-progress.jsonl`
and `put/profile-<host>/{metrics,sar,pidstat,snmp,fdb}.txt`. Prints, per
repetition: overall throughput, the per-second throughput distribution,
the phase means and maxima of the gateway and the nodes, and a table of
the stall seconds (throughput below half the repetition's median) with
the sampled signals of that second; finally the mean of every signal in
stall seconds against the other seconds, across repetitions.

Usage: summarize_putprofile.py <results-dir> [--stall-fraction 0.5]
"""

import json
import os
import re
import statistics
import sys
from collections import defaultdict

PHASE_RE = re.compile(r'^(arca_(?:gateway_put|node)_phase_seconds)_(sum|count|max_seconds)\{phase="([a-z_]+)"\} ([0-9.e+-]+)')
GAUGE_RE = re.compile(r"^(arca_gateway_put_in_flight|arca_node_put_in_flight) ([0-9.e+-]+)")


def parse_metrics(path):
    """-> {second: {url: {(phase, kind): value, 'in_flight': v}}}"""
    out = defaultdict(dict)
    if not os.path.exists(path):
        return out
    cur_ts = cur_url = None
    for line in open(path, encoding="utf-8", errors="replace"):
        if line.startswith("--- "):
            parts = line.split()
            cur_ts, cur_url = int(float(parts[1])), parts[2]
            out[cur_ts].setdefault(cur_url, {})
            continue
        if cur_ts is None:
            continue
        m = PHASE_RE.match(line)
        if m:
            out[cur_ts][cur_url][(m.group(3), m.group(2))] = float(m.group(4))
            continue
        m = GAUGE_RE.match(line)
        if m:
            out[cur_ts][cur_url]["in_flight"] = float(m.group(2))
    return out


def phase_rates(metrics, url_filter):
    """Per second and phase: (mean ms per observation completed in that
    second, observations), from consecutive samples of sum/count; plus the
    running max in seconds."""
    rates = defaultdict(dict)
    prev = {}
    for ts in sorted(metrics):
        for url, vals in metrics[ts].items():
            if not url_filter(url):
                continue
            phases = {p for (p, _k) in vals if isinstance(p, str)} if vals else set()
            for p in phases:
                s, c = vals.get((p, "sum")), vals.get((p, "count"))
                if s is None or c is None:
                    continue
                key = (url, p)
                if key in prev:
                    ps, pc = prev[key]
                    dc = c - pc
                    rates[ts][key] = ((s - ps) / dc * 1000.0 if dc > 0 else None, dc, vals.get((p, "max_seconds")))
                prev[key] = (s, c)
            if "in_flight" in vals:
                rates[ts][(url, "in_flight")] = (vals["in_flight"], 0, None)
    return rates


def parse_progress(path):
    rows = []
    if not os.path.exists(path):
        return rows
    for line in open(path, encoding="utf-8", errors="replace"):
        if '"progress"' in line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def parse_sar(path, epoch_of_first):
    """-> {second: {'idle': %, 'await_max': ms, 'wkB': kB/s, 'cswch': /s, 'rx_kB','tx_kB'}}"""
    out = defaultdict(dict)
    if not os.path.exists(path):
        return out
    header = None
    first_clock = None
    for line in open(path, encoding="utf-8", errors="replace"):
        parts = line.split()
        if len(parts) < 3 or parts[0] == "Average:":
            header = None if len(parts) < 3 else header
            continue
        if not re.match(r"^\d\d:\d\d:\d\d$", parts[0]):
            continue
        if parts[1] in ("CPU", "DEV", "IFACE") or parts[1].startswith("proc/s") or parts[1].startswith("kbmemfree") or parts[1].startswith("cswch/s"):
            header = parts[1:]
            continue
        if header is None:
            continue
        h, m, s = (int(x) for x in parts[0].split(":"))
        clock = h * 3600 + m * 60 + s
        if first_clock is None:
            first_clock = clock
        ts = epoch_of_first + (clock - first_clock) % 86400
        vals = parts[1:]
        row = out[ts]
        try:
            if header[0] == "CPU" and vals[0] == "all":
                row["idle"] = float(vals[header.index("%idle")])
            elif header[0] == "DEV":
                if "await" in header:
                    aw = float(vals[header.index("await")])
                    row["await_max"] = max(row.get("await_max", 0.0), aw)
                    row["wkB"] = row.get("wkB", 0.0) + float(vals[header.index("wkB/s")])
                    row["util_max"] = max(row.get("util_max", 0.0), float(vals[header.index("%util")]))
            elif header[0] == "IFACE":
                if vals[0] != "lo":
                    row["rx_kB"] = row.get("rx_kB", 0.0) + float(vals[header.index("rxkB/s")])
                    row["tx_kB"] = row.get("tx_kB", 0.0) + float(vals[header.index("txkB/s")])
            elif "cswch/s" in header:
                row["cswch"] = float(vals[header.index("cswch/s")])
            elif "kbmemfree" in header:
                row["kbdirty"] = float(vals[header.index("kbdirty")]) if "kbdirty" in header else None
        except (ValueError, IndexError):
            continue
    return out


def parse_snmp(path):
    """-> {second: {'retrans': cumulative, 'timeouts': cumulative}}"""
    out = {}
    if not os.path.exists(path):
        return out
    ts = None
    names = {}
    for line in open(path, encoding="utf-8", errors="replace"):
        if line.startswith("--- "):
            ts = int(float(line.split()[1]))
            out[ts] = {}
            continue
        if ts is None:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0].split(":")[-2] if parts[0].count(":") >= 2 else parts[0].rstrip(":")
        # "/proc/net/snmp:Tcp:" or "Tcp:" prefix
        tag = parts[0].split(":")[-2] if parts[0].endswith(":") and parts[0].count(":") >= 2 else parts[0].rstrip(":").split(":")[-1]
        if not re.match(r"^-?\d+$", parts[1]):
            names[tag] = parts[1:]
            continue
        if tag in names:
            vals = dict(zip(names[tag], parts[1:]))
            if tag == "Tcp" and "RetransSegs" in vals:
                out[ts]["retrans"] = int(vals["RetransSegs"])
            if tag == "TcpExt" and "TCPTimeouts" in vals:
                out[ts]["timeouts"] = int(vals["TCPTimeouts"])
    return out


def parse_fdb(path):
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path, encoding="utf-8", errors="replace"):
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "ts" in r:
            out[int(r["ts"])] = r
    return out


def first_ts(*dicts):
    ts = [min(d) for d in dicts if d]
    return min(ts) if ts else None


def fmt(v, spec="{:.1f}"):
    return "n/a" if v is None else spec.format(v)


def main():
    argv = sys.argv[1:]
    frac = 0.5
    if "--stall-fraction" in argv:
        i = argv.index("--stall-fraction")
        frac = float(argv[i + 1])
        del argv[i : i + 2]
    root = argv[0]
    stall_rows, other_rows = [], []
    for label in sorted(os.listdir(root)):
        d = os.path.join(root, label)
        if not os.path.isdir(d) or not os.path.exists(os.path.join(d, "put.jsonl")):
            continue
        summary = [json.loads(l) for l in open(os.path.join(d, "put.jsonl"), encoding="utf-8") if l.strip()][-1]
        progress = parse_progress(os.path.join(d, "put-progress.jsonl"))
        hosts = sorted(h[8:] for h in os.listdir(os.path.join(d, "put")) if h.startswith("profile-")) if os.path.isdir(os.path.join(d, "put")) else []
        prof = {}
        for h in hosts:
            base = os.path.join(d, "put", f"profile-{h}")
            metrics = parse_metrics(os.path.join(base, "metrics.txt"))
            snmp = parse_snmp(os.path.join(base, "snmp.txt"))
            t0 = first_ts(metrics, snmp)
            prof[h] = {
                "metrics": metrics,
                "gw": phase_rates(metrics, lambda u: ":9900/" in u),
                "nodes": phase_rates(metrics, lambda u: ":9900/" not in u),
                "sar": parse_sar(os.path.join(base, "sar.txt"), t0 or 0),
                "snmp": snmp,
                "fdb": parse_fdb(os.path.join(base, "fdb.txt")),
            }
        print(f"## {label}: {summary['mib_per_s']:.0f} MiB/s, p50 {summary['lat_ms_p50']:.0f} ms, p95 {summary['lat_ms_p95']:.0f} ms, p99 {summary['lat_ms_p99']:.0f} ms, {summary['ops']} objects in {summary['duration_s']:.1f} s")
        if not progress:
            print("(no progress series)")
            continue
        per_sec = [r["bytes"] / 2**20 for r in progress]
        med = statistics.median(per_sec)
        stalls = [r for r in progress if r["bytes"] / 2**20 < frac * med]
        print(f"per-second MiB/s: median {med:.0f}, min {min(per_sec):.0f}, max {max(per_sec):.0f}; stall seconds (< {frac:.0%} of median): {len(stalls)} of {len(progress)}")
        # phase totals from the last gateway sample
        gw_host = next((h for h in hosts if any(":9900/" in u for ts in prof[h]["metrics"] for u in prof[h]["metrics"][ts])), None)
        if gw_host:
            last = prof[gw_host]["metrics"][max(prof[gw_host]["metrics"])]
            gw = next((v for u, v in last.items() if ":9900/" in u), {})
            print("gateway phases over the run (mean ms per observation / max ms):", ", ".join(
                f"{p} {gw[(p,'sum')]/max(gw[(p,'count')],1)*1000:.1f}/{gw.get((p,'max_seconds'),0)*1000:.0f}"
                for p in ("body_read", "encode", "shard_write", "append", "lease_renew", "commit", "total") if (p, "sum") in gw))
        for h in hosts:
            last = prof[h]["metrics"][max(prof[h]["metrics"])] if prof[h]["metrics"] else {}
            for u, v in sorted(last.items()):
                if ":9900/" in u or not v:
                    continue
                port = u.split(":")[2].split("/")[0]
                print(f"node {h}:{port} phases (mean/max ms):", ", ".join(
                    f"{p} {v[(p,'sum')]/max(v[(p,'count')],1)*1000:.1f}/{v.get((p,'max_seconds'),0)*1000:.0f}"
                    for p in ("put_recv", "put_queue", "put_store", "store_append", "store_rollover", "store_sync_wait", "store_fsync") if (p, "sum") in v and v[(p, "count")] > 0))
        # per-second table of stall seconds
        print("| t | MiB/s | in flight | max lat ms | gw shard_write ms | gw append ms | gw commit ms | gw body_read ms | node put_store max ms (mean) | node fsync max ms | node sync_wait max ms | idle % per host | disk await ms per host | retrans/s per host | fdb commit s | fdb log queue MB |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")

        def signals(r):
            ts = int(r.get("epoch_ms", 0) / 1000) if r.get("epoch_ms") else None
            sig = {"mib_s": r["bytes"] / 2**20, "in_flight": r["in_flight"], "max_lat_ms": r["max_lat_ms"]}
            if ts is None:
                return sig
            for h in hosts:
                g = prof[h]["gw"].get(ts, {})
                for (u, p), (mean, n, mx) in g.items():
                    if p in ("shard_write", "append", "commit", "body_read", "encode") and mean is not None:
                        sig[f"gw_{p}_ms"] = mean
                nodes = prof[h]["nodes"].get(ts, {})
                for (u, p), (mean, n, mx) in nodes.items():
                    if p in ("put_store", "store_fsync", "store_sync_wait", "store_rollover", "put_queue") and mean is not None:
                        sig[f"node_{p}_ms_max"] = max(sig.get(f"node_{p}_ms_max", 0.0), mean)
                sar = prof[h]["sar"].get(ts, {})
                if "idle" in sar:
                    sig[f"idle_{h}"] = sar["idle"]
                if "await_max" in sar:
                    sig[f"await_{h}"] = sar["await_max"]
                if "wkB" in sar:
                    sig[f"wMB_{h}"] = sar["wkB"] / 1024.0
                if "cswch" in sar:
                    sig[f"cswch_{h}"] = sar["cswch"]
                sn, sp = prof[h]["snmp"].get(ts), prof[h]["snmp"].get(ts - 1)
                if sn and sp and "retrans" in sn and "retrans" in sp:
                    sig[f"retrans_{h}"] = sn["retrans"] - sp["retrans"]
                f = prof[h]["fdb"].get(ts) or prof[h]["fdb"].get(ts - 1)
                if f and f.get("commit_s") is not None:
                    sig["fdb_commit_s"] = f["commit_s"]
                    sig["fdb_log_queue_mb"] = (f.get("worst_log_queue") or 0) / 2**20
                    sig["fdb_limiting"] = f.get("limiting")
            return sig

        for r in progress:
            sig = signals(r)
            (stall_rows if r in stalls else other_rows).append(sig)
            if r in stalls:
                per_host = lambda k: " / ".join(fmt(sig.get(f"{k}_{h}")) for h in hosts)
                print(f"| {r['t_s']:.0f} | {sig['mib_s']:.0f} | {sig['in_flight']} | {sig['max_lat_ms']:.0f} | {fmt(sig.get('gw_shard_write_ms'))} | {fmt(sig.get('gw_append_ms'))} | {fmt(sig.get('gw_commit_ms'))} | {fmt(sig.get('gw_body_read_ms'))} | {fmt(sig.get('node_put_store_ms_max'))} | {fmt(sig.get('node_store_fsync_ms_max'))} | {fmt(sig.get('node_store_sync_wait_ms_max'))} | {per_host('idle')} | {per_host('await')} | {per_host('retrans')} | {fmt(sig.get('fdb_commit_s'), '{:.3f}')} | {fmt(sig.get('fdb_log_queue_mb'))} |")
        print()
    if stall_rows:
        print("## signals: mean in stall seconds versus other seconds (all repetitions)")
        print("| signal | stall seconds | other seconds | ratio |")
        print("| --- | --- | --- | --- |")
        keys = sorted({k for r in stall_rows + other_rows for k in r if isinstance(r[k], (int, float))})
        for k in keys:
            a = [r[k] for r in stall_rows if k in r]
            b = [r[k] for r in other_rows if k in r]
            if not a or not b:
                continue
            ma, mb = statistics.mean(a), statistics.mean(b)
            ratio = "n/a" if mb == 0 else f"{ma / mb:.2f}"
            print(f"| {k} | {ma:.2f} | {mb:.2f} | {ratio} |")
        lim = defaultdict(int)
        for r in stall_rows:
            if r.get("fdb_limiting"):
                lim[r["fdb_limiting"]] += 1
        if lim:
            print("fdb performance_limited_by during stall seconds:", dict(lim))


if __name__ == "__main__":
    main()
