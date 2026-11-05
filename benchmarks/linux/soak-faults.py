"""Per-fault table of a soak (benchmarks/linux/soak24.sh, Phase 3.4 §5).

Usage: python3 -I soak-faults.py RUN_DIR [MONITOR_DIR] > faults.json

For every injected fault: time, hold duration (the schedule of soak24.sh),
affected component, client impact (workload errors and the worst GET/PUT
p99 of the reports overlapping the fault window against the run's median),
gateway error codes in the window, integrity violations in the window,
recovery time (fault start to the first error-free workload report after
the fault ended), and residual work (repair queue and GC backlog sampled
about 15 minutes later, when a monitor ran). Errors outside every fault
window (plus GRACE seconds) are unexpected and listed with their gateway
codes; integrity-class codes are unexpected anywhere. Nothing host-specific
is printed.
"""

import collections
import datetime
import glob
import json
import os
import re
import statistics
import sys

run = sys.argv[1]
mon = sys.argv[2] if len(sys.argv) > 2 else None
GRACE = 120
# A fault may act before its log line: soak runs before the harness change
# logged corrupt-shard after stopping the node and corrupting the shard.
PRE = 30

# Hold time and component of each fault kind (soak24.sh do_fault).
KINDS = {
    "node-restart": (20, "storage node (durable)"),
    "gateway-kill": (5, "gateway"),
    "leader-kill": (60, "controller (leader)"),
    "fdb-process-restart": (30, "FoundationDB"),
    "net-delay": (120, "network to one durable node"),
    "node-kill": (30, "storage node (durable)"),
    "cache-node-kill": (20, "storage node (cache)"),
    "net-blackhole": (45, "network to one durable node"),
    "corrupt-shard": (5, "storage node (data)"),
    "eio-read": (90, "storage node (device reads)"),
    "net-reset": (10, "network to one durable node"),
    "cert-rotate": (0, "TLS identity"),
    "eio-write": (90, "storage node (device writes)"),
    "controller-restart": (10, "controller (standby)"),
    "restart-missing": (0, "harness restart of a missing process"),
}
INTEGRITY_CODES = {"BadDigest", "InvalidDigest", "XAmzContentSHA256Mismatch"}


def ts(s):
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()


def ts_log(s):
    s = s.rstrip("Z")
    if "." in s:
        s = s[: s.index(".") + 7]
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=datetime.timezone.utc).timestamp()
    return ts(s + "Z")


faults = []
with open(os.path.join(run, "faults.log")) as f:
    for line in f:
        p = line.split()
        if len(p) < 2 or p[1] in ("faults", "fault", "corrupt-shard-detail"):
            continue
        kind = p[1]
        hold, comp = KINDS.get(kind, (0, "unknown"))
        faults.append({"t": ts(p[0]), "kind": kind, "target": " ".join(p[2:4]) if kind != "corrupt-shard" else p[2], "hold_s": hold, "component": comp})

reports, violations = [], []
with open(os.path.join(run, "logs", "soakload.log")) as f:
    for line in f:
        if not line.startswith("{"):
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if "t_s" in r and "epoch_ms" in r:
            r["end"] = r["epoch_ms"] / 1000
            r["start"] = r["end"] - 60
            r["errors"] = sum(o["err"] for o in r["ops"].values())
            reports.append(r)
        elif "violation" in r:
            violations.append(r)
med_get = statistics.median([r["get_p99_ms"] for r in reports if r.get("gets")] or [0])
med_put = statistics.median([r["put_p99_ms"] for r in reports if r.get("puts")] or [0])

gw_errors = []  # (t, code, normalized message)
for path in glob.glob(os.path.join(run, "logs", "gw-*.log")):
    with open(path, errors="replace") as f:
        for line in f:
            if "S3 error" not in line:
                continue
            m = re.search(r"code=(\w+)", line)
            msg = re.search(r"error=(.*)", line)
            text = msg.group(1) if msg else ""
            text = re.sub(r"[0-9a-f]{16,}", "<id>", text)
            text = re.sub(r"\d+\.\d+\.\d+\.\d+:\d+|127\.0\.0\.1:\d+|<addr>:\d+", "<node>", text)
            text = re.sub(r"\d+", "N", text)[:110]
            try:
                t = ts_log(line.split()[0])
            except ValueError:
                continue
            gw_errors.append((t, m.group(1) if m else "?", text))

meta = []
if mon and os.path.exists(os.path.join(mon, "meta.jsonl")):
    with open(os.path.join(mon, "meta.jsonl")) as f:
        meta = [json.loads(l) for l in f if l.startswith("{")]


def residual(t):
    later = [m for m in meta if m["t"] >= t]
    return {"repair": later[0].get("repair"), "gc": later[0].get("gc")} if later else None


windows = []
rows = []
for i, ft in enumerate(faults):
    start, end = ft["t"] - PRE, ft["t"] + ft["hold_s"]
    w_end = end + GRACE
    windows.append((start, w_end))
    over = [r for r in reports if r["end"] > start and r["start"] < w_end]
    errs = sum(r["errors"] for r in over)
    by_op = collections.Counter()
    for r in over:
        for k, o in r["ops"].items():
            by_op[k] += o["err"]
    after = [r for r in reports if r["start"] >= end]
    first_clean = next((r for r in after if r["errors"] == 0), None)
    codes = collections.Counter(f"{c}: {m}" for t, c, m in gw_errors if start <= t <= w_end)
    rows.append({
        "fault": ft["kind"], "target": ft["target"], "start_utc": datetime.datetime.fromtimestamp(start, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hold_s": ft["hold_s"], "component": ft["component"],
        "client_errors": errs, "errors_by_op": {k: v for k, v in by_op.items() if v},
        "worst_get_p99_ms": round(max([r["get_p99_ms"] for r in over] or [0])),
        "worst_put_p99_ms": round(max([r["put_p99_ms"] for r in over] or [0])),
        "violations": sum(1 for v in violations if start * 1000 <= v.get("t_ms", 0) <= w_end * 1000),
        "recovery_s": round(first_clean["end"] - start) if first_clean else None,
        "gateway_codes": dict(codes.most_common(5)),
        "residual_after_15min": residual(end + 900),
    })


def in_window(t):
    return any(a <= t <= b for a, b in windows)


unexpected_reports = [r for r in reports if r["errors"] and not any(r["end"] > a and r["start"] < b for a, b in windows)]
unexpected_codes = collections.Counter(f"{c}: {m}" for t, c, m in gw_errors if not in_window(t))
integrity_codes = collections.Counter(f"{c}: {m}" for t, c, m in gw_errors if c in INTEGRITY_CODES)
by_kind = collections.defaultdict(list)
for r in rows:
    by_kind[r["fault"]].append(r)
print(json.dumps({
    "baseline_get_p99_ms": round(med_get), "baseline_put_p99_ms": round(med_put),
    "faults": len(rows),
    "by_kind": {k: {"count": len(v), "client_errors": sum(x["client_errors"] for x in v),
                    "violations": sum(x["violations"] for x in v),
                    "recovery_s_max": max([x["recovery_s"] for x in v if x["recovery_s"] is not None] or [0]),
                    "worst_get_p99_ms": max(x["worst_get_p99_ms"] for x in v)} for k, v in sorted(by_kind.items())},
    "expected_errors": sum(r["client_errors"] for r in rows),
    "unexpected": {"reports_with_errors_outside_fault_windows": len(unexpected_reports),
                   "errors_outside_fault_windows": sum(r["errors"] for r in unexpected_reports),
                   "gateway_codes_outside_fault_windows": dict(unexpected_codes.most_common(10)),
                   "integrity_codes_anywhere": dict(integrity_codes)},
    "violations_total": len(violations),
    "rows": rows,
}, indent=1))
