#!/usr/bin/env python3
"""A/B of the hot-key commit path (0.2.0, R-112, ADR-0041). Plan, targets,
invalid-run rules and limits: docs/verification/0.2.0-hot-key-plan.md.

Arms: `tag` (the v0.1.0 tag commit rebuilt on host B with the same
toolchain: the "tag rebuild", not the published artefact) and `candidate`
(the PR head). Both are built with `--features fdb`, release, `--locked`.

Layout (the same for both arms): host A a private FoundationDB process
(own port and data directory, never the system service) and the
controller; host B four storage nodes and four gateways (the system under
test, as in Phase 4.1); host C the generator and checker (`contend`).

  HOST_A=<ssh target> HOST_B=<ssh target> HOST_C=<ssh target> hot-key-ab.py build
  HOST_A=... HOST_B=... HOST_C=... [CAMPAIGN=id] [OUT=dir] hot-key-ab.py run
  HOST_A=... HOST_B=... HOST_C=... hot-key-ab.py teardown <run id>
  HOST_A=... HOST_B=... HOST_C=... hot-key-ab.py smoke <arm> <scenario>   (one uncounted run)
  CONTROL=1 [BUILD_COMMIT=sha] ... build | run   (plan §10: one build,
                                         arms queue-off and queue-on; 2.5 h)
  hot-key-ab.py schedule                 (the rotated order; no host)
  hot-key-ab.py summarize <out dir>      (summary from the run files; no host)

Hosts come only from the environment and are replaced by <host-a|b|c> in
every output; the output directory is passed through sanitize.py at the
end. Work areas: /mnt/arca-fdb/v020-perf/ on hosts A and B,
~/r110-dev/v020-perf/ on host C, each with the marker file `.owner`; a run lives in
<area>/runs/<run id>/ (its own `.owner`). Only processes whose pid file is
in the run directory and whose command line names it are signalled. Time
is bounded per step, per run (RUN_BOUND_S) and for the campaign
(TOTAL_BUDGET_S, default 5 h): a run that would not fit is not started.

Output: OUT/runs/<run id>.json per run (metrics, invalid-run reasons,
check and scrub), OUT/summary.json and OUT/summary.md (per arm and
scenario means over the valid runs, each target of the plan, the verdict).
"""

import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
HOSTS = {h: os.environ.get(f"HOST_{h.upper()}", "") for h in "abc"}
CAMPAIGN = os.environ.get("CAMPAIGN") or time.strftime("hk%Y%m%d-%H%M%S")
OUT = Path(os.environ.get("OUT", str(HERE / "results" / f"v020-hot-key-{CAMPAIGN}")))
# Hosts A and B on their data disks (A's root is nearly full; the builds
# need more than B's root should give); host C in the home directory.
AREA = {"a": "/mnt/arca-fdb/v020-perf", "b": "/mnt/arca-fdb/v020-perf", "c": "$HOME/r110-dev/v020-perf"}
MARKER = "0.2.0 perf (agent)"
ENV = "ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1"
TAG_COMMIT = "5601018"
ARMS = ("tag", "candidate")
ARM_LABEL = {"tag": "tag rebuild (v0.1.0 tag commit rebuilt on the test host; not the published artefact)",
             "candidate": "candidate (PR head)"}
PERF = ("unv", "ver", "retry", "unv16")
CORRECTNESS = ("fdbstall", "gwkill")
MIN_VALID = {"perf": 3, "correctness": 2}
MAX_EXTRA_PER_CELL = 2
# The control of plan §10 (CONTROL=1): one binary, the queue off against on.
CONTROL = os.environ.get("CONTROL") == "1"
if CONTROL:
    ARMS = ("off", "on")
    ARM_LABEL = {"off": "queue-off (the PR head build, gateways with --commit-queue off)",
                 "on": "queue-on (the same binary, default gateways)"}
    PERF = ("unv", "ver", "unv16")
    CORRECTNESS = ()
# The build each arm runs.
BUILD_OF = {"tag": "tag", "candidate": "candidate", "off": "candidate", "on": "candidate"}
BUILDS = tuple(dict.fromkeys(BUILD_OF[a] for a in ARMS))
BASE, TEST = ARMS
TOTAL_BUDGET_S = int(os.environ.get("TOTAL_BUDGET_S", (2.5 if CONTROL else 5) * 3600))
RUN_BOUND_S = int(os.environ.get("RUN_BOUND_S", 1800))
EST_RUN_S = {"perf": 600, "correctness": 1100}
WRITERS, READERS, DURATION_S, SIZE = 64, 8, 60, 4096
assert WRITERS <= 64  # the plan's limit
FDB_PORT = 4720
CTL_HTTP = "127.0.0.1:9740"
NODES = 4
NODE_PORT, NODE_METRICS = 9711, 9721
GATEWAYS = 4
GW_PORT, GW_METRICS = 9701, 9731
BUCKET = "hot"
SETTLE_MIN_S = {"perf": 0, "correctness": 420}
SETTLE_STABLE_S, SETTLE_MAX_S = 60, 900
BUILD_FREE_MIN = 12 << 30
FS_FREE_MIN = 5 << 30  # every filesystem a run touches, checked before each run
GUARD = {"mem_avail_min": 1 << 30, "root_free_min": 3 << 30}
GEN_CPU_BUSY, GEN_CPU_SECS = 0.90, 10
FOREIGN_CORES, FOREIGN_SECS = 0.5, 10
DAMAGE = ["MissingShard", "ChecksumMismatch", "TruncatedShard", "InvalidHeader",
          "UnexpectedShard", "StalePlacementEpoch", "MetadataDataMismatch", "UnrecoverableStripe"]

ADDR = {}


# --- pure helpers (unit-tested in hot_key_ab_test.py) ----------------------

def redact(text, addr=None, hosts=None):
    addr = ADDR if addr is None else addr
    hosts = HOSTS if hosts is None else hosts
    for h, a in addr.items():
        if a:
            text = text.replace(a, f"<host-{h}>")
    for h, t in hosts.items():
        if t:
            text = text.replace(t, f"<host-{h}>")
            if "@" in t:
                text = text.replace(t.split("@", 1)[1], f"<host-{h}>")
    return re.sub(r"(/home|/Users)/[A-Za-z_][A-Za-z0-9_.-]*", r"\1/<user>", text)


def kind(scenario):
    return "correctness" if scenario in CORRECTNESS else "perf"


def schedule(rounds=3, correctness_rounds=2):
    """The rotated order: perf round r runs the scenarios shifted by r, the
    tag rebuild first in even rounds and second in odd ones; a correctness
    round (candidate only, rotated too) follows each of the first
    `correctness_rounds` perf rounds."""
    out = []
    for r in range(rounds):
        order = PERF[r % len(PERF):] + PERF[:r % len(PERF)]
        arms = ARMS if r % 2 == 0 else ARMS[::-1]
        out += [{"round": r + 1, "scenario": s, "arm": a} for s in order for a in arms]
        if r < correctness_rounds and CORRECTNESS:
            c = CORRECTNESS[r % len(CORRECTNESS):] + CORRECTNESS[:r % len(CORRECTNESS)]
            out += [{"round": r + 1, "scenario": s, "arm": TEST} for s in c]
    return out


def cells():
    return [(a, s) for s in PERF for a in ARMS] + [(TEST, s) for s in CORRECTNESS]


def reruns_needed(records, extra_done):
    """Cells short of their minimum valid runs that may still be repeated."""
    out = []
    for a, s in cells():
        valid = sum(1 for r in records if r["arm"] == a and r["scenario"] == s and not r["invalid"])
        if valid < MIN_VALID[kind(s)] and extra_done.get(f"{a}/{s}", 0) < MAX_EXTRA_PER_CELL:
            out.append((a, s))
    return out


def parse_prom(text):
    """Prometheus text → {name{labels}: value}; `--- <label>` lines start a
    section whose label prefixes the keys (`<label>/<name>`)."""
    out, section = {}, ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("--- "):
            section = line[4:].strip() + "/"
            continue
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z_:][A-Za-z0-9_:]*(?:\{[^}]*\})?)\s+(\S+)$", line)
        if not m:
            continue
        try:
            out[section + m.group(1)] = float(m.group(2))
        except ValueError:
            pass
    return out


def metric_sum(snap, name):
    """Sum of a metric (any labels) over every section; None if absent."""
    vals = [v for k, v in snap.items() if re.fullmatch(rf"(?:[^/]*/)?{re.escape(name)}(?:\{{.*\}})?", k)]
    return sum(vals) if vals else None


def counter_delta(before, after, name):
    """After − before of a counter summed over sections; None when absent
    or when any section's counter went down (a process restarted)."""
    keys = [k for k in after if re.fullmatch(rf"(?:[^/]*/)?{re.escape(name)}(?:\{{.*\}})?", k)]
    if not keys:
        return None
    d = 0.0
    for k in keys:
        if k not in before or after[k] < before[k]:
            return None
        d += after[k] - before[k]
    return d


def fdb_counters(status):
    """The counters of interest from `status json`, and whether the
    database is available and healthy."""
    c = status.get("cluster", {})
    wl = c.get("workload", {})
    tx = wl.get("transactions", {})
    ops = wl.get("operations", {})
    ds = status.get("client", {}).get("database_status", {})
    return {
        "available": bool(ds.get("available")), "healthy": bool(ds.get("healthy")),
        "tx_started": tx.get("started", {}).get("counter"),
        "tx_committed": tx.get("committed", {}).get("counter"),
        "tx_conflicted": tx.get("conflicted", {}).get("counter"),
        "tx_rejected": tx.get("rejected_for_queued_too_long", {}).get("counter"),
        "reads": ops.get("reads", {}).get("counter"),
        "writes": ops.get("writes", {}).get("counter"),
        "commit_latency_probe_s": c.get("latency_probe", {}).get("commit_seconds"),
    }


def fdb_delta(before, after):
    out = {}
    for k in ("tx_started", "tx_committed", "tx_conflicted", "tx_rejected", "reads", "writes"):
        a, b = after.get(k), before.get(k)
        out[k] = (a - b) if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a >= b else None
    return out


def percentile(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    i = max(0, min(len(xs) - 1, math.ceil(p / 100 * len(xs)) - 1))
    return xs[i]


def summarize_ops(lines):
    """The contend request log (JSON lines) → PUT outcomes and latencies.
    As `contend --mode check`: a PUT is acknowledged if an attempt was
    answered 2xx, a definite failure if every attempt was 503 SlowDown,
    unknown otherwise."""
    puts, gets, t0, t1 = {}, {"total": 0, "ok": 0}, None, None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        t0 = o["sent_us"] if t0 is None else min(t0, o["sent_us"])
        t1 = o["done_us"] if t1 is None else max(t1, o["done_us"])
        if o["op"] == "get":
            gets["total"] += 1
            gets["ok"] += 200 <= o["status"] < 300
            continue
        puts.setdefault((o["key"], o["id"]), []).append(o)
    acked, definite, unknown, attempts = [], 0, {}, 0
    lat_ack, lat_all = [], []
    for atts in puts.values():
        attempts += len(atts)
        first = min(a["sent_us"] for a in atts)
        last = max(a["done_us"] for a in atts)
        ok = [a for a in atts if 200 <= a["status"] < 300]
        lat_all.append((last - first) / 1000)
        if ok:
            acked.append(ok[0])
            lat_ack.append((ok[0]["done_us"] - first) / 1000)
        elif all(a["status"] == 503 and a.get("code") == "SlowDown" for a in atts):
            definite += 1
        else:
            fin = max(atts, key=lambda a: a["done_us"])
            cause = f"{fin['status']} {fin.get('code') or 'transport'}"
            unknown[cause] = unknown.get(cause, 0) + 1
    dur = ((t1 - t0) / 1e6) if t0 is not None else 0
    n = len(puts)
    return {
        "puts": n, "put_attempts": attempts, "acked": len(acked), "slowdown": definite,
        "unknown": sum(unknown.values()), "unknown_by_cause": unknown,
        "duration_s": round(dur, 3), "acked_per_s": (len(acked) / dur) if dur else None,
        "slowdown_share": (definite / n) if n else None,
        "ack_p50_ms": percentile(lat_ack, 50), "ack_p95_ms": percentile(lat_ack, 95),
        "ack_p99_ms": percentile(lat_ack, 99), "all_p99_ms": percentile(lat_all, 99),
        "all_p95_ms": percentile(lat_all, 95), "gets": gets,
    }


def analyze_samples(samples, tag):
    """Sampler records (one per interval) → resource use of this run's
    processes by role, the host's minimum free memory and root space, the
    seconds the host CPU was above GEN_CPU_BUSY, and processes not of this
    run that used FOREIGN_CORES or more for FOREIGN_SECS or more."""
    roles, foreign, busy_s = {}, {}, 0
    mem_min = root_min = None
    prev = None
    for s in samples:
        mem_min = s["mem_avail"] if mem_min is None else min(mem_min, s["mem_avail"])
        root_min = s["root_free"] if root_min is None else min(root_min, s["root_free"])
        if prev is not None:
            dt = s["t"] - prev["t"]
            tot = s["cpu_total"] - prev["cpu_total"]
            idle = s["cpu_idle"] - prev["cpu_idle"]
            if tot > 0 and (tot - idle) / tot > GEN_CPU_BUSY:
                busy_s += dt
            pp = prev["procs"]
            for pid, p in s["procs"].items():
                if pid not in pp or pp[pid]["start"] != p["start"] or dt <= 0:
                    continue
                cores = (p["ticks"] - pp[pid]["ticks"]) / s["hz"] / dt
                if tag in p["cmd"]:
                    r = roles.setdefault(role_of(p["cmd"]), {"cpu_s": 0.0, "rss_max": 0})
                    r["cpu_s"] += cores * dt
                elif cores >= FOREIGN_CORES and not p.get("self"):
                    name = p["cmd"].split(" ")[0].split("/")[-1]
                    foreign[name] = foreign.get(name, 0.0) + dt
        for p in s["procs"].values():
            if tag in p["cmd"]:
                r = roles.setdefault(role_of(p["cmd"]), {"cpu_s": 0.0, "rss_max": 0})
                r["rss_max"] = max(r["rss_max"], p["rss"])
        prev = s
    span = (samples[-1]["t"] - samples[0]["t"]) if len(samples) > 1 else 0
    for r in roles.values():
        r["cpu_cores_mean"] = (r.pop("cpu_s") / span) if span else None
    return {"roles": roles, "mem_avail_min": mem_min, "root_free_min": root_min, "cpu_busy_s": busy_s,
            "foreign_heavy": {k: round(v, 1) for k, v in foreign.items() if v >= FOREIGN_SECS},
            "span_s": span}


def role_of(cmd):
    for r in ("fdbserver", "storage-node", "gateway", "controller", "contend"):
        if r in cmd:
            return r
    return "other"


def invalid_reasons(r):
    """The plan's §7 rules applied to one run record."""
    why = []
    if r.get("aborted"):
        why.append(f"run aborted: {r['aborted']}")
    if not r.get("binaries_match", False):
        why.append("a binary's SHA-256 does not match its build record")
    if not r.get("fdb_before", {}).get("healthy"):
        why.append("FoundationDB not healthy before the run")
    f = r.get("faults")
    if f is not None:
        lo, hi = r.get("run_window", (None, None))
        if lo is None or not f["times"] or any(not (lo <= t <= hi) for t in f["times"]) \
                or len(f["times"]) != f["expected"]:
            why.append("a fault time outside the run (or a fault missing)")
    if kind(r["scenario"]) == "perf" and r.get("processes_exited"):
        why.append(f"cluster processes exited in a fault-free scenario: {r['processes_exited']}")
    if r.get("processes_exited_unexpected"):
        why.append(f"cluster processes exited unexpectedly: {r['processes_exited_unexpected']}")
    for h, s in (r.get("hosts") or {}).items():
        if s is None:
            why.append(f"no samples from host {h}")
            continue
        if s["mem_avail_min"] is not None and s["mem_avail_min"] < GUARD["mem_avail_min"]:
            why.append(f"host {h}: free memory below 1 GiB")
        if s["root_free_min"] is not None and s["root_free_min"] < GUARD["root_free_min"]:
            why.append(f"host {h}: free root below 3 GiB")
        if s["foreign_heavy"]:
            why.append(f"host {h}: other workload {sorted(s['foreign_heavy'])}")
        if h == "c" and s["cpu_busy_s"] > GEN_CPU_SECS:
            why.append(f"generator host CPU above 90 % for {s['cpu_busy_s']:.0f} s")
    if r.get("load_rc") not in (0, None) or ("load_rc" not in r and not r.get("aborted")):
        why.append("the generator exited with an error")
    chk = r.get("check") or {}
    if chk.get("rc") not in (0, 2) or "violations" not in chk:
        why.append("the check did not complete")
    if not r.get("scrub_complete", False):
        why.append("the scrub did not complete")
    return why


def mean(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else None


def cell_means(records):
    out = {}
    for a, s in cells():
        rs = [r for r in records if r["arm"] == a and r["scenario"] == s and not r["invalid"]]
        m = {k: mean([r["metrics"].get(k) for r in rs]) for k in
             ("slowdown_share", "acked_per_s", "attempts_per_ack", "ack_p50_ms", "ack_p95_ms", "ack_p99_ms",
              "all_p99_ms", "unknown", "fdb_conflicted_per_ack")}
        m["valid_runs"] = len(rs)
        m["invalid_runs"] = sum(1 for r in records if r["arm"] == a and r["scenario"] == s and r["invalid"])
        out[f"{a}/{s}"] = m
    return out


def judge(records):
    """Every target of the plan's §3 and the adoption rule. A target is
    `met`, `not met` or `missing` (too few valid runs or no value)."""
    cm = cell_means(records)
    targets = []

    def t(name, scen, ok, detail):
        targets.append({"target": name, "scenario": scen, "result": ok, "detail": detail})

    def enough(*keys):
        return all(cm[k]["valid_runs"] >= MIN_VALID[kind(k.split("/")[1])] for k in keys)

    def cmp(name, scen, key, rel):
        if scen not in PERF:
            return None
        T, C = cm[f"{BASE}/{scen}"], cm[f"{TEST}/{scen}"]
        if not enough(f"{BASE}/{scen}", f"{TEST}/{scen}") or T[key] is None or C[key] is None:
            return t(name, scen, "missing", {BASE: T[key], TEST: C[key]})
        return t(name, scen, "met" if rel(T[key], C[key]) else "not met", {BASE: T[key], TEST: C[key]})

    for s in ("unv", "ver"):
        cmp("SlowDown share ≤ half the tag rebuild's and ≤ 25 %", s, "slowdown_share",
            lambda T, C: C <= 0.5 * T and C <= 0.25)
        cmp("attempts per acknowledged PUT ≤ half", s, "attempts_per_ack", lambda T, C: C <= 0.5 * T)
        cmp("acknowledged PUT/s ≥ 1.3 ×", s, "acked_per_s", lambda T, C: C >= 1.3 * T)
    cmp("acknowledged PUT/s ≥ 0.95 ×", "unv16", "acked_per_s", lambda T, C: C >= 0.95 * T)
    cmp("SlowDown share ≤ tag rebuild's + 0.5 points", "unv16", "slowdown_share", lambda T, C: C <= T + 0.005)
    cmp("acknowledged p99 ≤ 1.5 ×", "unv16", "ack_p99_ms", lambda T, C: C <= 1.5 * T)
    cmp("SlowDown after 3 retries ≤ tag rebuild's", "retry", "slowdown_share", lambda T, C: C <= T)
    # The control (plan §10) asks it of every run, the main plan of the
    # candidate's fault-free runs.
    judged = ARMS if CONTROL else (TEST,)
    for s in PERF:
        rs = [r for r in records if r["arm"] in judged and r["scenario"] == s and not r["invalid"]]
        unk = sum(r["metrics"].get("unknown") or 0 for r in rs)
        full = all(enough(f"{a}/{s}") for a in judged)
        t("unknown outcomes 0", s, "met" if full and unk == 0 else ("missing" if not full else "not met"),
          {"unknown": unk, "arms": list(judged)})
    bad = [r["run_id"] for r in records if not r["invalid"] and not r["correct"]]
    t("violations, definite failures visible, scrub findings: 0 in every valid run", "all",
      "met" if not bad else "not met", {"runs": bad})
    for s in CORRECTNESS:
        t("correctness runs", s, "met" if enough(f"{TEST}/{s}") else "missing",
          {"valid": cm[f"{TEST}/{s}"]["valid_runs"]})
    results = {x["result"] for x in targets}
    verdict = "not adopted" if "not met" in results else ("inconclusive" if "missing" in results else "adopt")
    return {"verdict": verdict, "targets": targets, "cells": cm}


def summary_md(j, meta):
    lines = [f"# Hot-key A/B {meta.get('campaign')}", "",
             f"Arms: {BASE} = {ARM_LABEL[BASE]}; {TEST} = {ARM_LABEL[TEST]}.", "",
             f"Verdict: **{j['verdict']}**", "", "| Cell | valid | invalid | SlowDown % | ack/s | attempts/ack |"
             " ack p50/p95/p99 ms | all p99 ms | unknown |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]

    def f(x, k=1, pct=False):
        return "—" if x is None else (f"{x * 100:.{k}f}" if pct else f"{x:.{k}f}")
    for c, m in j["cells"].items():
        lines.append(f"| {c} | {m['valid_runs']} | {m['invalid_runs']} | {f(m['slowdown_share'], 1, True)} | "
                     f"{f(m['acked_per_s'])} | {f(m['attempts_per_ack'])} | {f(m['ack_p50_ms'], 0)}/"
                     f"{f(m['ack_p95_ms'], 0)}/{f(m['ack_p99_ms'], 0)} | {f(m['all_p99_ms'], 0)} | "
                     f"{f(m['unknown'], 1)} |")
    lines += ["", "| Target | Scenario | Result | Detail |", "| --- | --- | --- | --- |"]
    for t in j["targets"]:
        lines.append(f"| {t['target']} | {t['scenario']} | {t['result']} | `{json.dumps(t['detail'])}` |")
    return "\n".join(lines) + "\n"


# --- on-host tools (this file is copied to each run directory) ------------

def tool_sample(tag, out, stop, interval=1.0):
    """Every `interval` s until `stop` exists: host CPU, free memory and
    root space, and every process's CPU ticks, start time, RSS, command."""
    hz = os.sysconf("SC_CLK_TCK")
    me = os.getpid()
    with open(out, "a") as f:
        while not os.path.exists(stop):
            t = time.time()
            with open("/proc/stat") as s:
                cpu = [int(x) for x in s.readline().split()[1:]]
            mem = 0
            with open("/proc/meminfo") as m:
                for line in m:
                    if line.startswith("MemAvailable:"):
                        mem = int(line.split()[1]) * 1024
            st = os.statvfs("/")
            procs = {}
            for d in os.listdir("/proc"):
                if not d.isdigit():
                    continue
                try:
                    with open(f"/proc/{d}/stat") as s:
                        fields = s.read().rsplit(")", 1)[1].split()
                    with open(f"/proc/{d}/cmdline", "rb") as c:
                        full = c.read().replace(b"\0", b" ").decode(errors="replace").strip()
                    rss = 0
                    with open(f"/proc/{d}/status") as s:
                        for line in s:
                            if line.startswith("VmRSS:"):
                                rss = int(line.split()[1]) * 1024
                except (OSError, IndexError):
                    continue
                if not full:
                    continue
                # Ours is decided on the whole command line: the run's
                # directory may come after a long argument list.
                cmd = full[:300] + (f" {tag}" if tag in full[300:] else "")
                procs[d] = {"ticks": int(fields[11]) + int(fields[12]), "start": int(fields[19]), "rss": rss,
                            "cmd": cmd, "self": int(d) == me}
            f.write(json.dumps({"t": t, "hz": hz, "cpu_total": sum(cpu[:8]), "cpu_idle": cpu[3] + cpu[4],
                                "mem_avail": mem, "root_free": st.f_bavail * st.f_frsize, "procs": procs}) + "\n")
            f.flush()
            time.sleep(max(0.0, interval - (time.time() - t)))


def tool_summarize(log):
    with open(log) as f:
        print(json.dumps(summarize_ops(f)))


def tool_analyze(samples, tag):
    with open(samples) as f:
        print(json.dumps(analyze_samples([json.loads(l) for l in f if l.strip()], tag)))


# --- plumbing -------------------------------------------------------------

def log(msg):
    line = redact(f"{time.strftime('%H:%M:%S')} {msg}")
    print(line, flush=True)
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "driver.log", "a") as f:
        f.write(line + "\n")


def ssh(h, cmd, timeout=120, check=False, stdin=None):
    try:
        p = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
                            HOSTS[h], cmd], capture_output=True, text=True, timeout=timeout, input=stdin)
    except subprocess.TimeoutExpired:
        if check:
            raise RuntimeError(f"timeout on host {h}: {redact(cmd)[:200]}")
        return 124, "", "timeout"
    if check and p.returncode != 0:
        raise RuntimeError(f"host {h} rc {p.returncode}: {redact(cmd)[:200]}: {redact(p.stderr)[-500:]}")
    return p.returncode, p.stdout, p.stderr


def save(name, text):
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact(text))


def addresses():
    for h in "abc":
        ADDR[h] = ssh(h, "hostname -I | awk '{print $1}'", check=True)[1].strip()


def clock_offsets():
    """Each host's clock minus this machine's (midpoint estimate)."""
    off = {}
    for h in "abc":
        t0 = time.time()
        _, out, _ = ssh(h, "date +%s.%N", check=True)
        t1 = time.time()
        off[h] = {"offset_s": float(out) - (t0 + t1) / 2, "rtt_s": t1 - t0}
    return off


def binpath(h, arm):
    return f"{AREA[h]}/bin/arca-lake-{BUILD_OF.get(arm, arm)}"


def contend_path():
    return f"{AREA['c']}/bin/contend"


def ensure_area(h):
    rc, out, _ = ssh(h, f"mkdir -p {AREA[h]} && cat {AREA[h]}/.owner 2>/dev/null")
    if not out.strip():
        ssh(h, f"echo '{MARKER}' > {AREA[h]}/.owner", check=True)
    elif out.strip() != MARKER:
        raise RuntimeError(f"host {h}: {AREA[h]} has another owner marker")


# --- build ----------------------------------------------------------------

def build():
    """Both arms on host B (release, --features fdb, --locked); the
    candidate's contend too; binaries to hosts A and C with SHA-256 checks.
    Writes build-record.json on host B and OUT/build-record.json."""
    OUT.mkdir(parents=True, exist_ok=True)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=HERE, capture_output=True, text=True,
                          check=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=HERE, capture_output=True, text=True,
                           check=True).stdout.strip()
    if dirty:
        sys.exit("the working tree is not clean: commit the candidate first")
    up = subprocess.run(["git", "rev-parse", "@{u}"], cwd=HERE, capture_output=True, text=True).stdout.strip()
    if up != head:
        sys.exit("HEAD is not the pushed PR head: push the branch first")
    if os.environ.get("BUILD_COMMIT"):
        # A named commit instead of HEAD (the control builds merged main);
        # it must be on the remote's main.
        head = subprocess.run(["git", "rev-parse", os.environ["BUILD_COMMIT"] + "^{commit}"], cwd=HERE,
                              capture_output=True, text=True, check=True).stdout.strip()
        if subprocess.run(["git", "merge-base", "--is-ancestor", head, "origin/main"], cwd=HERE).returncode:
            sys.exit("BUILD_COMMIT is not on origin/main")
    tag = subprocess.run(["git", "rev-parse", TAG_COMMIT + "^{commit}"], cwd=HERE, capture_output=True,
                         text=True, check=True).stdout.strip()
    for h in "abc":
        ensure_area(h)
        ssh(h, f"mkdir -p {AREA[h]}/bin {AREA[h]}/runs", check=True)
    b = AREA["b"]
    _, free, _ = ssh("b", f"df -B1 --output=avail {b} | tail -1", check=True)
    if int(free) < BUILD_FREE_MIN:
        raise RuntimeError(f"host B: {int(free) >> 30} GiB free for the builds, {BUILD_FREE_MIN >> 30} needed")
    record = {"arms": {}, "built": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    for arm, commit in [(b_, {"tag": tag, "candidate": head}[b_]) for b_ in BUILDS]:
        src = f"{b}/src-{arm}"
        ssh("b", f"rm -rf {src} && mkdir -p {src}", check=True)
        # From the repository's root: run in a subdirectory, `git archive`
        # would pack only that subdirectory.
        root = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=HERE, capture_output=True, text=True,
                              check=True).stdout.strip()
        arch = subprocess.run(["git", "archive", "--format=tar", commit], cwd=root, capture_output=True,
                              check=True).stdout
        p = subprocess.run(["ssh", "-o", "BatchMode=yes", HOSTS["b"], f"tar -xf - -C {src}"], input=arch,
                           capture_output=True, timeout=600)
        if p.returncode:
            raise RuntimeError(f"copy of {arm} sources failed")
        log(f"building {arm} ({commit[:12]}) on host B")
        cargo = (f"cd {src} && export PATH=$HOME/.cargo/bin:$PATH && CARGO_TARGET_DIR={b}/target-{arm} "
                 "timeout 5400 nice cargo build --release --locked")
        # The server binary alone in both arms (no feature unification with
        # the generator); the generator from the candidate, separately.
        steps = f"{cargo} -p arca-lake --features fdb > {b}/build-{arm}.log 2>&1"
        if arm == "candidate":
            steps += f" && {cargo} -p arca-lake-arcabench --bin contend >> {b}/build-{arm}.log 2>&1"
        rc, out, err = ssh("b", f"{steps}; echo rc $?; cd {src} && export PATH=$HOME/.cargo/bin:$PATH && "
                                "rustc -V && cargo -V", timeout=11000)
        save(f"build/{arm}.txt", out + err)
        if "rc 0" not in out:
            _, tail, _ = ssh("b", f"tail -50 {b}/build-{arm}.log")
            save(f"build/{arm}-log-tail.txt", tail)
            raise RuntimeError(f"build of {arm} failed")
        ssh("b", f"cp {b}/target-{arm}/release/arca-lake {binpath('b', arm)}", check=True)
        if arm == "candidate":
            ssh("b", f"cp {b}/target-{arm}/release/contend {b}/bin/contend", check=True)
        if not os.environ.get("KEEP_TARGETS"):
            # One target directory at a time on the host's disk.
            ssh("b", f"rm -rf {b}/target-{arm}", timeout=600)
        _, sha, _ = ssh("b", f"sha256sum {binpath('b', arm)} | cut -c1-64", check=True)
        lines = out.strip().splitlines()
        label = ARM_LABEL.get(arm, f"build of {commit}")
        record["arms"][arm] = {"commit": commit, "label": label, "sha256": sha.strip(),
                               "rustc": next((l for l in lines if l.startswith("rustc")), ""),
                               "cargo": next((l for l in lines if l.startswith("cargo")), ""),
                               "profile": "release", "features": "fdb", "locked": True}
    _, csha, _ = ssh("b", f"sha256sum {b}/bin/contend | cut -c1-64", check=True)
    record["contend_sha256"] = csha.strip()
    # Copies: host A both arms (controller, admin); host C contend.
    copies = [("a", binpath("b", a), binpath("a", a), record["arms"][a]["sha256"]) for a in BUILDS]
    copies.append(("c", f"{b}/bin/contend", contend_path(), record["contend_sha256"]))
    for h, src, dst, sha in copies:
        # Remote commands quoted for this machine's shell: $HOME expands on
        # the host.
        put = f"cat > {dst}.part && chmod +x {dst}.part && mv {dst}.part {dst}"
        cmd = (f"ssh -o BatchMode=yes {shlex.quote(HOSTS['b'])} {shlex.quote(f'cat {src}')} | "
               f"ssh -o BatchMode=yes {shlex.quote(HOSTS[h])} {shlex.quote(put)}")
        subprocess.run(cmd, shell=True, check=True, timeout=600)
        _, got, _ = ssh(h, f"sha256sum {dst} | cut -c1-64", check=True)
        if got.strip() != sha:
            raise RuntimeError(f"SHA-256 mismatch after copy to host {h}")
    text = json.dumps(record, indent=1, sort_keys=True)
    for h in "abc":
        ssh(h, f"cat > {AREA[h]}/build-record.json", stdin=text, check=True)
    save("build-record.json", text)
    log("build done: " + ", ".join(f"{a} {record['arms'][a]['commit'][:12]} {record['arms'][a]['sha256'][:16]}"
                                   for a in BUILDS))
    return record


def build_record():
    _, out, _ = ssh("b", f"cat {AREA['b']}/build-record.json", check=True)
    return json.loads(out)


def low_filesystems():
    """Work-area filesystems below FS_FREE_MIN on any host (host A's root,
    already below it and not written by these runs, is watched by the
    plan's 3 GiB rule instead)."""
    low = []
    for h in "abc":
        _, out, _ = ssh(h, f"df -B1 --output=avail,target {AREA[h]} | tail -n +2")
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0].isdigit() and int(parts[0]) < FS_FREE_MIN:
                low.append(f"{h}:{parts[1]} {int(parts[0]) >> 20} MiB")
    return low


def binaries_match(rec):
    ok = True
    for h, path, sha in ([(h, binpath(h, a), rec["arms"][a]["sha256"]) for h in "ab" for a in BUILDS]
                         + [("c", contend_path(), rec["contend_sha256"])]):
        _, got, _ = ssh(h, f"sha256sum {path} | cut -c1-64")
        ok = ok and got.strip() == sha
    return ok


# --- one run --------------------------------------------------------------

class Run:
    def __init__(self, run_id, arm, scenario):
        self.id, self.arm, self.scenario = run_id, arm, scenario
        self.tag = f"v020-perf/runs/{run_id}"
        self.R = {h: f"{AREA[h]}/runs/{run_id}" for h in "abc"}
        self.started = []  # (host, name)

    def bin(self, h):
        return binpath(h, self.arm)

    def start(self, h, name, cmd):
        R = self.R[h]
        ssh(h, f"cd {R} && {{ nohup env {ENV} {cmd} >> {R}/logs/{name}.log 2>&1 < /dev/null & echo $! > {R}/{name}.pid; }}",
            check=True)
        self.started.append((h, name))

    def signal(self, h, names, sig):
        """Signal processes of this run (pid file and command line); the
        host's time just after the signal."""
        R = self.R[h]
        checks = " && ".join(f"p{i}=$(cat {R}/{n}.pid) && grep -qa {self.tag} /proc/$p{i}/cmdline"
                             for i, n in enumerate(names))
        pids = " ".join(f"$p{i}" for i in range(len(names)))
        rc, out, err = ssh(h, f"{checks} && kill -{sig} {pids} && date +%s.%N")
        if rc:
            raise RuntimeError(f"cannot signal {names} on host {h}: {redact(err)}")
        return float(out.split()[0])

    def alive(self, h, name):
        R = self.R[h]
        return ssh(h, f"p=$(cat {R}/{name}.pid 2>/dev/null) && grep -qa {self.tag} /proc/$p/cmdline")[0] == 0

    def admin(self, args, timeout=120):
        return ssh("a", f"env {ENV} {self.bin('a')} admin --cluster-file {self.R['a']}/fdb.cluster {args}",
                   timeout=timeout)

    def gw_endpoints(self):
        return [f"http://{ADDR['b']}:{GW_PORT + i}" for i in range(GATEWAYS)]

    # setup

    def setup(self):
        for h in "abc":
            ensure_area(h)
            if ssh(h, f"test -e {self.R[h]}")[0] == 0:
                raise RuntimeError(f"{self.R[h]} exists on host {h}")
            ssh(h, f"mkdir -p {self.R[h]}/logs && echo '{MARKER}' > {self.R[h]}/.owner && "
                   f"echo 'hk:{re.sub(r'[^A-Za-z0-9]', '', self.id)}@{ADDR['a']}:{FDB_PORT}' > {self.R[h]}/fdb.cluster",
                check=True)
            subprocess.run(["scp", "-q", "-o", "BatchMode=yes", str(Path(__file__).resolve()),
                            f"{HOSTS[h]}:{self.R[h].replace('$HOME', '~')}/tool.py"], check=True, timeout=60)
        busy = []
        for h, ports in (("a", [FDB_PORT, 9740]), ("b", [NODE_PORT + i for i in range(NODES)]
                                                     + [GW_PORT + i for i in range(GATEWAYS)])):
            _, out, _ = ssh(h, "ss -ltnH | awk '{print $4}'")
            busy += [f"{h}:{p}" for p in ports if re.search(rf":{p}$", out, re.M)]
        if busy:
            raise RuntimeError(f"ports in use: {busy}")
        Ra = self.R["a"]
        ssh("a", f"mkdir -p {Ra}/fdb/data {Ra}/fdb/log", check=True)
        self.start("a", "fdb", f"fdbserver -p {ADDR['a']}:{FDB_PORT} -C {Ra}/fdb.cluster -d {Ra}/fdb/data "
                               f"-L {Ra}/fdb/log --memory 2GiB --cache-memory 256MiB")
        time.sleep(3)
        ssh("a", f"timeout 60 fdbcli -C {Ra}/fdb.cluster --exec 'configure new single ssd' --timeout 30", timeout=90)
        for _ in range(60):
            if self.fdb_status().get("available"):
                break
            time.sleep(1)
        else:
            raise RuntimeError("private FoundationDB did not become available")
        Rb = self.R["b"]
        for i in range(NODES):
            self.start("b", f"n{i}", f"{self.bin('b')} storage-node --listen {ADDR['b']}:{NODE_PORT + i} "
                                     f"--dir {Rb}/n{i} --metrics-listen 127.0.0.1:{NODE_METRICS + i} "
                                     f"--capacity-bytes {4 << 30} --segment-max-bytes {64 << 20} "
                                     "--reclaim-retention-secs 60 --reclaim-skew-secs 10 --compaction-interval-secs 30")
        time.sleep(3)
        for i in range(NODES):
            rc, _, err = self.admin(f"register-node --address {ADDR['b']}:{NODE_PORT + i} --label host=b "
                                    f"--label node=n{i} --label device_class=hdd")
            if rc:
                raise RuntimeError(f"register n{i}: {redact(err)[-300:]}")
        rc, out, err = self.admin("create-groups --tier default --device-class hdd --count 4 --k 2 --m 1 "
                                  "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain node")
        if rc:
            raise RuntimeError(f"create-groups: {redact(err)[-300:]}")
        self.start("a", "ctl", f"{self.bin('a')} controller --cluster-file {Ra}/fdb.cluster --id ctl "
                               f"--http {CTL_HTTP} --failure-domain node --unhealthy-after-secs 20 "
                               "--health-interval-secs 5 --gc-interval-secs 10 --gc-grace-secs 30 "
                               "--min-orphan-age-secs 60 --reconcile-interval-secs 30 --reclaim-interval-secs 10 "
                               "--admission-horizon-secs 60 --admission-dwell-secs 10")
        for i in range(GATEWAYS):
            self.start_gateway(i)
        self.wait_ready()

    def gw_cmd(self, i):
        return (f"{self.bin('b')} gateway --backend cluster --cluster-file {self.R['b']}/fdb.cluster "
                f"--allow-anonymous --listen {ADDR['b']}:{GW_PORT + i} --metrics-listen 127.0.0.1:{GW_METRICS + i} "
                "--k 2 --m 1 --chunk-bytes 4194304 --small-threshold 65536 --failure-domain node --lease-secs 300"
                + (" --commit-queue off" if self.arm == "off" else ""))

    def start_gateway(self, i):
        self.start("b", f"gw{i}", self.gw_cmd(i))

    def curl(self, method, url, data=None):
        d = f"--data-binary {shlex.quote(data)}" if data is not None else ""
        _, out, _ = ssh("c", f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 30 -X {method} {d} {shlex.quote(url)}")
        return out.strip()

    def wait_ready(self):
        """A bucket, then a probe PUT and DELETE through every gateway."""
        end = time.time() + 240
        eps = self.gw_endpoints()
        while time.time() < end:
            if self.curl("PUT", f"{eps[0]}/{BUCKET}") in ("200", "409"):
                if all(self.curl("PUT", f"{e}/{BUCKET}/probe-{i}", "probe") == "200"
                       and self.curl("DELETE", f"{e}/{BUCKET}/probe-{i}") == "204" for i, e in enumerate(eps)):
                    break
            time.sleep(5)
        else:
            raise RuntimeError("the gateways did not become ready in 240 s")
        if self.scenario in ("ver",):
            body = ('<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    "<Status>Enabled</Status></VersioningConfiguration>")
            if self.curl("PUT", f"{eps[0]}/{BUCKET}?versioning", body) != "200":
                raise RuntimeError("versioning not enabled")

    # measurements

    def fdb_status(self):
        rc, out, _ = ssh("a", f"timeout 20 fdbcli -C {self.R['a']}/fdb.cluster --exec 'status json' --timeout 10",
                         timeout=30)
        try:
            return fdb_counters(json.loads(out))
        except (ValueError, AttributeError):
            return {"available": False, "healthy": False}

    def gw_metrics(self):
        cmd = "; ".join(f"echo '--- gw{i}'; curl -s --max-time 5 http://127.0.0.1:{GW_METRICS + i}/metrics"
                        for i in range(GATEWAYS))
        return parse_prom(ssh("b", cmd, timeout=60)[1])

    def settle_probe(self):
        cmd = "; ".join(f"echo '--- n{i}'; curl -s --max-time 5 http://127.0.0.1:{NODE_METRICS + i}/metrics"
                        for i in range(NODES))
        nodes = parse_prom(ssh("b", cmd, timeout=60)[1])
        ctl = parse_prom(ssh("a", f"curl -s --max-time 5 http://{CTL_HTTP}/metrics", timeout=30)[1])
        return {"live": metric_sum({k: v for k, v in nodes.items() if 'kind="live"' in k}, "arca_node_index_entries"),
                "pending": metric_sum(nodes, "arca_node_reclaim_pending_records"),
                "gc_backlog": metric_sum(ctl, "arca_controller_gc_backlog")}

    def settle(self):
        start = time.time()
        time.sleep(SETTLE_MIN_S[kind(self.scenario)])
        hist = []
        while time.time() - start < SETTLE_MAX_S:
            p = self.settle_probe()
            p["t"] = round(time.time() - start, 1)
            hist.append(p)
            stable = [x for x in hist if p["t"] - x["t"] <= SETTLE_STABLE_S]
            if (p["pending"] == 0 and p["gc_backlog"] == 0 and hist[0]["t"] <= p["t"] - SETTLE_STABLE_S
                    and all(x["live"] == p["live"] and x["pending"] == 0 for x in stable)):
                return True, hist
            time.sleep(20)
        return False, hist

    def samplers(self, action):
        for h in "abc":
            R = self.R[h]
            if action == "start":
                self.start(h, "sampler", f"python3 -I {R}/tool.py sample {self.tag} {R}/samples.jsonl {R}/sampler.stop")
            else:
                ssh(h, f"touch {R}/sampler.stop")

    def analyze(self, h):
        R = self.R[h]
        rc, out, _ = ssh(h, f"python3 -I {R}/tool.py analyze {R}/samples.jsonl {self.tag}", timeout=300)
        try:
            return json.loads(out)
        except ValueError:
            return None

    def faults(self, t_start_local):
        """Inject the scenario's faults at their times after the run's start;
        their times on this machine's clock."""
        times = []
        plan = {"fdbstall": [(15, "stop"), (27, "cont"), (47, "stop"), (59, "cont")],
                "gwkill": [(20, "kill"), (40, "kill")]}[self.scenario]
        stopped = False
        try:
            for at, what in plan:
                time.sleep(max(0.0, t_start_local + at - time.time()))
                if what == "stop":
                    times.append(self.signal("a", ["fdb"], "STOP") - self.off["a"]["offset_s"])
                    stopped = True
                elif what == "cont":
                    self.signal("a", ["fdb"], "CONT")
                    stopped = False
                else:
                    names = [f"gw{i}" for i in range(GATEWAYS)]
                    times.append(self.signal("b", names, "KILL") - self.off["b"]["offset_s"])
                    for i in range(GATEWAYS):
                        for _ in range(20):
                            if not self.alive("b", f"gw{i}"):
                                break
                            time.sleep(0.25)
                        self.start_gateway(i)
        finally:
            if stopped:
                self.signal("a", ["fdb"], "CONT")
        return {"times": times, "expected": 2}

    def load(self):
        Rc = self.R["c"]
        eps = " ".join(f"--endpoint {e}" for e in self.gw_endpoints())
        extra = {"unv16": "--keys 16", "retry": "--retries 3"}.get(self.scenario, "")
        cmd = (f"{contend_path()} --mode run --log {Rc}/ops.jsonl {eps} --bucket {BUCKET} --key-prefix {self.id}- --writers {WRITERS} "
               f"--readers {READERS} --duration-secs {DURATION_S} --size {SIZE} {extra}")
        wrapper = (f"sh -c {shlex.quote(f'date +%s.%N > {Rc}/run.start; {cmd} > {Rc}/run.json & echo $! > {Rc}/contend.pid; wait $!; echo $? > {Rc}/run.rc; date +%s.%N > {Rc}/run.end')}")
        self.start("c", "load", wrapper)
        self.started.append(("c", "contend"))

    def check(self):
        Rc = self.R["c"]
        eps = " ".join(f"--endpoint {e}" for e in self.gw_endpoints())
        ver = "--versioned" if self.scenario == "ver" else ""
        rc, out, err = ssh("c", f"timeout 900 {contend_path()} --mode check {eps} --bucket {BUCKET} "
                                f"--key-prefix {self.id}- --keys {16 if self.scenario == 'unv16' else 1} "
                                f"--size {SIZE} --log {Rc}/ops.jsonl {ver}", timeout=960)
        try:
            res = json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            res = {"error": redact(err)[-500:]}
        res["rc"] = rc
        return res

    def scrub(self):
        _, out, _ = self.admin("status")
        groups = re.findall(r"^group (\d+)", out, re.M)
        res, complete = {}, bool(groups)
        for g in groups:
            rc, o, e = self.admin(f"integrity scrub --group {g} --wait", timeout=900)
            m = re.search(r"findings \[([\d, ]*)\] enqueued (\d+) errors (\d+)", o)
            if not m or int(m.group(3)):
                complete = False
                res[g] = {"rc": rc, "error": redact((o + e)[-300:])}
                continue
            res[g] = dict(zip(DAMAGE, [int(x) for x in m.group(1).split(",") if x.strip()]))
            # 0.2.0 binaries also print the shards held legitimately or not
            # classified (ADR-0036; unclassified ones count as errors).
            held = re.search(r"retained (\d+) retained_damaged (\d+) in_flight (\d+) unclassified (\d+)", o)
            if held:
                res[g]["held"] = dict(zip(("retained", "retained_damaged", "in_flight", "unclassified"),
                                          (int(x) for x in held.groups())))
        clean = complete and all(not any(v for k, v in r.items() if k not in ("UnexpectedShard", "held"))
                                 for r in res.values())
        return complete, clean, res

    def exited(self):
        names = [("a", "fdb"), ("a", "ctl")] + [("b", f"n{i}") for i in range(NODES)] \
            + [("b", f"gw{i}") for i in range(GATEWAYS)]
        return [f"{h}:{n}" for h, n in names if not self.alive(h, n)]

    def teardown(self):
        for h in "abc":
            R = self.R[h]
            rc, out, _ = ssh(h, f"cat {R}/.owner 2>/dev/null")
            if out.strip() != MARKER:
                log(f"host {h}: no marker in run directory, nothing removed")
                continue
            ssh(h, f"touch {R}/sampler.stop; for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
                   f"grep -qa {self.tag} /proc/$q/cmdline 2>/dev/null && kill -CONT $q 2>/dev/null; "
                   f"grep -qa {self.tag} /proc/$q/cmdline 2>/dev/null && kill $q; done; sleep 3; "
                   f"for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
                   f"grep -qa {self.tag} /proc/$q/cmdline 2>/dev/null && kill -9 $q; done; sleep 1; true", timeout=60)
            dest = OUT / "logs" / self.id / f"host-{h}"
            dest.mkdir(parents=True, exist_ok=True)
            subprocess.run(f"ssh -o BatchMode=yes {shlex.quote(HOSTS[h])} 'cd {R} && for f in logs/*.log fdb/log/*; do "
                           f"[ -f \"$f\" ] && tail -c 2000000 \"$f\" > \"$f.tail\" 2>/dev/null; done; "
                           f"tar -cf - logs/*.tail fdb/log/*.tail run.json run.rc 2>/dev/null' | tar -xf - -C {dest}",
                           shell=True, timeout=300)
            _, left, _ = ssh(h, f"pgrep -af {self.tag} | grep -v pgrep || true")
            if left.strip():
                log(f"host {h}: processes of {self.id} still running, run directory kept:\n{left}")
                continue
            ssh(h, f"rm -rf {R}", timeout=300)
        for f in list((OUT / "logs" / self.id).rglob("*")):
            if f.is_file():
                name = redact(f.name).replace("<", "").replace(">", "")
                if f.suffix not in (".log", ".txt", ".json", ".tail"):
                    name += ".log"
                if name != f.name:
                    f.rename(f.with_name(name))

    def execute(self, rec_build):
        """Setup, measure, settle, check, scrub, teardown. Every step
        bounded; the record carries what the invalid-run rules need."""
        rec = {"run_id": self.id, "arm": self.arm, "label": ARM_LABEL[self.arm], "scenario": self.scenario,
               "commit": rec_build["arms"][BUILD_OF[self.arm]]["commit"],
               "sha256": rec_build["arms"][BUILD_OF[self.arm]]["sha256"],
               "started": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
        deadline = time.time() + RUN_BOUND_S
        try:
            rec["binaries_match"] = binaries_match(rec_build)
            self.off = clock_offsets()
            rec["clock_offsets"] = self.off
            self.setup()
            rec["fdb_before"] = self.fdb_status()
            self.samplers("start")
            time.sleep(3)
            gw0, fdb0 = self.gw_metrics(), rec["fdb_before"]
            self.load()
            Rc = self.R["c"]
            for _ in range(30):
                rc, out, _ = ssh("c", f"cat {Rc}/run.start 2>/dev/null")
                if out.strip():
                    break
                time.sleep(0.5)
            t_start = float(out) - self.off["c"]["offset_s"]
            if kind(self.scenario) == "correctness":
                rec["faults"] = self.faults(t_start)
            end = t_start + DURATION_S + 120
            while time.time() < end and ssh("c", f"test -s {Rc}/run.end")[0] != 0:
                time.sleep(2)
            _, se, _ = ssh("c", f"cat {Rc}/run.start {Rc}/run.end {Rc}/run.rc {Rc}/run.json 2>/dev/null")
            parts = se.split("\n", 3)
            if len(parts) < 3 or not parts[1].strip():
                raise RuntimeError("the load did not end in time")
            rec["run_window"] = (float(parts[0]) - self.off["c"]["offset_s"], float(parts[1]) - self.off["c"]["offset_s"])
            rec["load_rc"] = int(parts[2])
            gw1, rec["fdb_after"] = self.gw_metrics(), self.fdb_status()
            self.samplers("stop")
            time.sleep(2)
            rec["exited_after_load"] = self.exited()
            if kind(self.scenario) == "perf":
                rec["processes_exited"] = rec["exited_after_load"]
            else:
                rec["processes_exited_unexpected"] = rec["exited_after_load"]
            rec["hosts"] = {h: self.analyze(h) for h in "abc"}
            _, summ, _ = ssh("c", f"python3 -I {Rc}/tool.py summarize-log {Rc}/ops.jsonl", timeout=600)
            m = json.loads(summ)
            names = ["arca_gateway_meta_txn_attempts_total", "arca_gateway_meta_txn_conflicts_total",
                     "arca_gateway_meta_txn_exhausted_total", "arca_gateway_meta_txn_unknown_retries_total"] + [
                f"arca_gateway_commit_queue_{n}_total" for n in
                ("turns", "waited", "wait_us", "refused_full", "refused_wait", "bypassed", "conflicts", "backoff_us")]
            m["gateway"] = {n: counter_delta(gw0, gw1, n) for n in names}
            m["fdb"] = fdb_delta(fdb0, rec["fdb_after"])
            att = m["gateway"]["arca_gateway_meta_txn_attempts_total"]
            m["attempts_per_ack"] = (att / m["acked"]) if att is not None and m["acked"] else None
            conf = m["fdb"]["tx_conflicted"]
            m["fdb_conflicted_per_ack"] = (conf / m["acked"]) if conf is not None and m["acked"] else None
            rec["metrics"] = m
            if time.time() > deadline:
                raise RuntimeError("run time bound exceeded before settling")
            rec["settled"], rec["settle"] = self.settle()
            rec["check"] = self.check()
            rec["scrub_complete"], rec["scrub_clean"], rec["scrub"] = self.scrub()
            rec["final_records"] = self.settle_probe()
            rec["exited_at_end"] = self.exited()
            if time.time() > deadline:
                rec["aborted"] = "run time bound exceeded"
        except Exception as e:  # noqa: BLE001
            rec["aborted"] = redact(repr(e))[-800:]
        finally:
            try:
                self.samplers("stop")
            except Exception:  # noqa: BLE001
                pass
            self.teardown()
            rec["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        chk = rec.get("check") or {}
        rec["correct"] = (chk.get("violations") == 0 and chk.get("definite_failures_visible") == 0
                          and rec.get("scrub_clean", False))
        rec.setdefault("metrics", {})
        rec["invalid_reasons"] = invalid_reasons(rec)
        rec["invalid"] = bool(rec["invalid_reasons"])
        return rec


# --- campaign -------------------------------------------------------------

def load_records(out):
    return [json.loads(p.read_text()) for p in sorted((Path(out) / "runs").glob("*.json"))]


def write_summary(out, meta):
    out = Path(out)
    j = judge(load_records(out))
    j["meta"] = meta
    (out / "summary.json").write_text(redact(json.dumps(j, indent=1, sort_keys=True)))
    (out / "summary.md").write_text(redact(summary_md(j, meta)))
    return j


def campaign():
    OUT.mkdir(parents=True, exist_ok=True)
    addresses()
    rec_build = build_record()
    save("build-record.json", json.dumps(rec_build, indent=1, sort_keys=True))
    meta = {"campaign": CAMPAIGN, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "budget_s": TOTAL_BUDGET_S,
            "run_bound_s": RUN_BOUND_S, "plan": "docs/verification/0.2.0-hot-key-plan.md",
            "control": CONTROL, "arms": {a: {**rec_build["arms"][BUILD_OF[a]], "label": ARM_LABEL[a]} for a in ARMS}}
    t0 = time.time()
    deadline = t0 + TOTAL_BUDGET_S
    records = load_records(OUT)
    # Resuming an interrupted campaign: the schedule continues where it
    # stopped (the order is fixed).
    queue = schedule()[len(records):]
    extra, idx, stopped = {}, len(records), None
    while True:
        if not queue:
            more = reruns_needed(records, extra)
            if not more:
                break
            for a, s in more:
                extra[f"{a}/{s}"] = extra.get(f"{a}/{s}", 0) + 1
            queue = [{"round": "repeat", "scenario": s, "arm": a} for a, s in more]
        item = queue.pop(0)
        if time.time() + EST_RUN_S[kind(item["scenario"])] > deadline:
            stopped = "campaign time budget reached"
            log(stopped)
            break
        low = low_filesystems()
        if low:
            stopped = f"filesystems below 5 GiB: {low}"
            log(stopped)
            break
        idx += 1
        run_id = f"{CAMPAIGN}-{idx:02d}-{item['scenario']}-{item['arm']}"
        log(f"run {run_id} (round {item['round']})")
        r = Run(run_id, item["arm"], item["scenario"]).execute(rec_build)
        r["round"] = item["round"]
        save(f"runs/{run_id}.json", json.dumps(r, indent=1, sort_keys=True))
        records.append(r)
        log(f"run {run_id}: {'INVALID ' + '; '.join(r['invalid_reasons']) if r['invalid'] else 'valid'}; "
            f"correct {r['correct']}; ack/s {r['metrics'].get('acked_per_s')}; "
            f"SlowDown {r['metrics'].get('slowdown_share')}")
        meta.update({"elapsed_s": round(time.time() - t0), "stopped": stopped})
        write_summary(OUT, meta)
    meta.update({"finished": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "elapsed_s": round(time.time() - t0),
                 "stopped": stopped})
    save("meta.json", json.dumps(meta, indent=1, sort_keys=True))
    j = write_summary(OUT, meta)
    subprocess.run([sys.executable, "-I", str(HERE / "sanitize.py"), str(OUT)])
    print(f"verdict: {j['verdict']}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "sample":
        return tool_sample(sys.argv[2], sys.argv[3], sys.argv[4])
    if cmd == "summarize-log":
        return tool_summarize(sys.argv[2])
    if cmd == "analyze":
        return tool_analyze(sys.argv[2], sys.argv[3])
    if cmd == "schedule":
        for i, s in enumerate(schedule(), 1):
            print(f"{i:2d} round {s['round']} {s['scenario']:9s} {s['arm']}")
        return None
    if cmd == "summarize":
        j = write_summary(sys.argv[2], {"campaign": Path(sys.argv[2]).name})
        print(j["verdict"])
        return None
    if not all(HOSTS.values()):
        sys.exit("HOST_A, HOST_B and HOST_C are required")
    if cmd == "build":
        addresses()
        build()
    elif cmd == "run":
        campaign()
    elif cmd == "smoke" and len(sys.argv) > 3:
        # One run outside the campaign (driver check; never counted): its
        # record goes to OUT/smoke/.
        addresses()
        low = low_filesystems()
        if low:
            sys.exit(f"filesystems below 5 GiB: {low}")
        r = Run(f"{CAMPAIGN}-smoke-{sys.argv[3]}-{sys.argv[2]}", sys.argv[2], sys.argv[3]).execute(build_record())
        save(f"smoke/{r['run_id']}.json", json.dumps(r, indent=1, sort_keys=True))
        subprocess.run([sys.executable, "-I", str(HERE / "sanitize.py"), str(OUT)])
        print(json.dumps({"invalid_reasons": r["invalid_reasons"], "correct": r["correct"],
                          "aborted": r.get("aborted"), "metrics": {k: v for k, v in r["metrics"].items()
                                                                  if not isinstance(v, dict)}}, indent=1))
    elif cmd == "teardown" and len(sys.argv) > 2:
        addresses()
        r = Run(sys.argv[2], TEST, "unv")
        r.teardown()
    else:
        sys.exit(__doc__)
    return None


if __name__ == "__main__":
    main()
