#!/usr/bin/env python3
"""ADR-0035 integration harness: two controllers, two gateways and a real
FoundationDB on three hosts (ADR-0035 §10.5, §10.6).

Builds a disposable schema-4 cluster in its own directories and port and
runs the scenarios of docs/verification/phase-4.1-adr35.md §7:

  A1, A2  two controllers started at the same instant on a store without a
          tiering record (equal and different --tiering-* flags)
  C1      reflection of enable / disable / set (admin commit, leader
          acknowledgment, each gateway's refreshes and recording decision)
  C2      a gateway's tiering refresh failing, stalled and returning late
  B1      leader change (graceful) with tiering confirmed disabled
  B3a     the leader paused past its lease and resumed (disabled)
  B2      leader change with tiering enabled on two tiers: promotion,
          the promotion cutoff, the grace, access accounting
  B3b     the leader paused while a tiering move it started is held before
          its switch; the policy changes; the old leader resumes

Hosts and paths come from the environment, never from this file:
  HOST_A (FoundationDB, controller ctl-a, load generator, admin),
  HOST_B (six storage nodes, gateway gw0), HOST_C (controller ctl-c,
  gateway gw1): ssh targets.
  REMOTE_BIN  arca-lake built with --features fdb,fault-injection (the
              same path on every host)
  RUN_DIR, FDB_DATA, FDB_PORT  the disposable cluster's directories and
              port (refused if the directories exist at setup)
  OUT         local evidence directory (one subdirectory per invocation,
              never overwritten)
  STOP_FILE   the watchdog's stop file: present -> load stopped, evidence
              collected, exit 3

Usage: adr35-integration.py setup | run [SCENARIO ...] | collect | teardown
Every wait has a deadline; a failed check is recorded and the run stops
(the evidence of a failure is kept, later runs go to new directories).
"""

import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time

E = os.environ
HOSTS = {"a": E.get("HOST_A"), "b": E.get("HOST_B"), "c": E.get("HOST_C")}
BIN = E.get("REMOTE_BIN", "")
RUN = E.get("RUN_DIR", "")
FDB_DATA = E.get("FDB_DATA", "")
FDB_PORT = int(E.get("FDB_PORT", "0") or 0)
STOP_FILE = E.get("STOP_FILE", "")
OUT_ROOT = E.get("OUT", "")
CF = f"{RUN}/fdb.cluster"
ENVP = "ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1"
ADMIN = f"env {ENVP} timeout 60 {BIN} admin --cluster-file {CF}"
BUCKET = "t44"
SIZE = 65536
OBJECTS = [f"o{i:03d}" for i in range(200)]
HOT = [f"h{i}" for i in range(10)]
SPECIAL = ["px", "py", "mv"]
GW = {"gw0": ("b", 9000, 9500), "gw1": ("c", 9001, 9501)}
CTL = {"ctl-a": ("a", 9901), "ctl-c": ("c", 9902)}
NODES = 6
SKEW_SECS = 5  # --tiering-clock-skew-secs of both controllers
# ADR-0035 §10.3/§10.4 constants (ops::tiering): stale bound, refresh, flush
# delay; the gateways' default lease skew.
STALE_MS, REFRESH_MS, FLUSH_MS, LEASE_SKEW_MS, LEASE_MS = 10_000, 2_000, 30_000, 2_000, 10_000
MARGIN_MS = STALE_MS + REFRESH_MS + FLUSH_MS + SKEW_SECS * 1000

# Policies (tiers default -> fast). INERT moves nothing.
INERT = dict(tiers=["default", "fast"], promote_reads=1_000_000, window=600, demote=3600)
INERT2 = dict(tiers=["default", "fast"], promote_reads=999_999, window=600, demote=7200)
MOVE = dict(tiers=["default", "fast"], promote_reads=5, window=600, demote=60)
HOLD = dict(tiers=["default", "fast"], promote_reads=1_000_000, window=600, demote=60)
# Each controller's --tiering-* flags (A1 and A2 set them; restarts reuse).
FLAGS = {"ctl-a": INERT, "ctl-c": INERT2}


class Failed(Exception):
    pass


class Stopped(Exception):
    pass


class Run:
    def __init__(self, label):
        self.dir = os.path.join(OUT_ROOT, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + label)
        os.makedirs(self.dir)
        self.events = open(os.path.join(self.dir, "events.jsonl"), "a", buffering=1)
        self.checks = open(os.path.join(self.dir, "checks.jsonl"), "a", buffering=1)
        self.failed = 0
        print(f"evidence: {self.dir}")

    def ev(self, what, **kw):
        rec = {"t_ms": now_ms(), "event": what, **kw}
        self.events.write(json.dumps(rec) + "\n")
        print(f"  {what} {json.dumps(kw)[:300]}")

    def check(self, name, ok, **detail):
        rec = {"t_ms": now_ms(), "check": name, "ok": bool(ok), **detail}
        self.checks.write(json.dumps(rec) + "\n")
        print(f"  [{'ok' if ok else 'FAIL'}] {name} {json.dumps(detail)[:400]}")
        if not ok:
            self.failed += 1
            raise Failed(name)


def now_ms():
    return int(time.time() * 1000)


def stop_requested():
    return STOP_FILE and os.path.exists(STOP_FILE)


CM = "/tmp/adr35-cm"


def ssh(h, cmd, timeout=120, check=True):
    os.makedirs(CM, exist_ok=True)
    r = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
         "-o", "ControlMaster=auto", "-o", f"ControlPath={CM}/%C", "-o", "ControlPersist=900",
         HOSTS[h], cmd],
        capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise Failed(f"ssh {h}: {cmd[:120]}: rc {r.returncode}: {r.stderr.strip()[-300:]}")
    return r.stdout


def addr(h, _cache={}):
    if h not in _cache:
        _cache[h] = ssh(h, "hostname -I | awk '{print $1}'").strip()
    return _cache[h]


# A host's wall clock in ms. Not `date +%s%3N`: the uutils `date` of the
# test hosts does not zero-pad %3N (a reading up to 0.9 s late).
NOW_MS = "t=${EPOCHREALTIME/./}; echo ${t:0:13}"


def remote_ms(h):
    return int(ssh(h, NOW_MS).strip())


def clock_offsets():
    """Each host's clock minus the local one, from the fastest of five
    round trips (ms; with the round trip, an upper bound of the error)."""
    out = {}
    for h in HOSTS:
        best = None
        for _ in range(5):
            t0 = time.time() * 1000
            r = remote_ms(h)
            t1 = time.time() * 1000
            if best is None or t1 - t0 < best[1]:
                best = (r - (t0 + t1) / 2, t1 - t0)
        out[h] = {"offset_ms": round(best[0], 1), "rtt_ms": round(best[1], 1)}
    return out


# ---------------------------------------------------------------- processes

def start(h, name, cmd, env="", at_ms=None):
    """Start `cmd` on host `h` under the run directory (pid file, log). With
    `at_ms`, the process starts at that wall-clock instant of the host."""
    wait = (f"while :; do t=${{EPOCHREALTIME/./}}; [ ${{t:0:13}} -ge {at_ms} ] && break; sleep 0.002; done; "
            if at_ms else "")
    inner = f"{wait}exec env {ENVP} {env} {cmd}"
    ssh(h, f"mkdir -p {RUN}/logs && cd {RUN} && "
           f"if [ -f {name}.pid ] && kill -0 $(cat {name}.pid) 2>/dev/null; then echo already; exit 0; fi; "
           f"ulimit -n 65536 2>/dev/null; nohup bash -c {shlex.quote(inner)} >> logs/{name}.log 2>&1 < /dev/null & "
           f"echo $! > {name}.pid")


def ours(h, name):
    """The pid of `name` if it is this harness's process: its pid file in
    RUN_DIR, alive, its working directory RUN_DIR, and its command naming
    the run directory or the binary."""
    out = ssh(h, f"p=$(cat {RUN}/{name}.pid 2>/dev/null) || exit 0; kill -0 $p 2>/dev/null || exit 0; "
                 f"echo $p $(readlink /proc/$p/cwd) $(stat -c %U /proc/$p) \"$(tr '\\0' ' ' < /proc/$p/cmdline)\"",
              check=False).strip()
    if not out:
        return None
    pid, cwd, owner, cmdline = out.split(" ", 3)
    if cwd != RUN or (RUN not in cmdline and BIN not in cmdline):
        raise Failed(f"{name} on {h}: pid {pid} is not ours (cwd {cwd}, owner {owner})")
    return int(pid)


def signal(h, name, sig):
    pid = ours(h, name)
    if pid is None:
        raise Failed(f"{name} on {h} is not running")
    ssh(h, f"kill -{sig} {pid}")
    return pid


def stop(h, name, timeout=30):
    pid = ours(h, name)
    if pid is None:
        return
    ssh(h, f"kill -TERM {pid}; for i in $(seq 1 {timeout * 10}); do kill -0 {pid} 2>/dev/null || exit 0; sleep 0.1; done; "
           f"kill -KILL {pid}")
    ssh(h, f"rm -f {RUN}/{name}.pid")


def wait_until(run, what, timeout, cond, every=0.25):
    deadline = time.time() + timeout
    while True:
        if stop_requested():
            raise Stopped(open(STOP_FILE).read().strip() if os.path.exists(STOP_FILE) else "stop")
        v = cond()
        if v:
            return v
        if time.time() > deadline:
            run.check(f"wait: {what}", False, timeout_s=timeout)
        time.sleep(every)


# ---------------------------------------------------------------- the store

STATUS = re.compile(
    r"enabled (true|false) generation (\d+) acknowledged (true|false) \(last by term (\d+) generation (\d+); "
    r"leader term (\d+)\) tiers (\[.*?\]) promote_reads (\d+) promote_window_s (\d+) demote_after_s (\d+) "
    r"grace_from_ms (\d+) promotion_from_ms (\d+) changed_ms (\d+)")


def parse_tiering(text):
    m = STATUS.search(text)
    if not m:
        return None
    g = m.groups()
    return dict(enabled=g[0] == "true", generation=int(g[1]), acknowledged=g[2] == "true",
                applied_term=int(g[3]), applied_generation=int(g[4]), leader_term=int(g[5]),
                tiers=json.loads(g[6]), promote_reads=int(g[7]), window=int(g[8]), demote=int(g[9]),
                grace_from_ms=int(g[10]), promotion_from_ms=int(g[11]), changed_ms=int(g[12]))


def tiering(cmd="status"):
    out = ssh("a", f"{ADMIN} tiering {cmd} 2>&1", check=False)
    return parse_tiering(out), out


def admin_change(run, cmd):
    """An admin change, with the host-A wall clock around it."""
    t0 = remote_ms("a")
    rec, out = tiering(cmd)
    t1 = remote_ms("a")
    if rec is None:
        run.check(f"admin tiering {cmd}", False, output=out.strip()[-300:])
    run.ev("admin", cmd=cmd, start_ms_a=t0, end_ms_a=t1, generation=rec["generation"],
           enabled=rec["enabled"], changed_ms=rec["changed_ms"])
    return rec, t0, t1


def policy_args(p):
    return " ".join(f"--tier {t}" for t in p["tiers"]) + (
        f" --promote-reads {p['promote_reads']} --promote-window-secs {p['window']} "
        f"--demote-after-secs {p['demote']}")


def leader():
    out = ssh("a", f"{ADMIN} status 2>&1 | grep '^leader '", check=False).strip()
    m = re.match(r"leader (\S+) token (\d+) lease_until_ms (\d+)", out)
    if not m:
        return None
    holder = m.group(1)
    return dict(id=holder.split("@")[0], holder=holder, token=int(m.group(2)), lease_until_ms=int(m.group(3)))


def objects(keys=None):
    out = ssh("a", f"{ADMIN} tiering objects --bucket {BUCKET} 2>&1")
    rows = {}
    for line in out.splitlines():
        m = re.match(r"object (\S+) generation (\S+) tiers (\S+) access_count (\S+) access_last_ms (\S+)", line)
        if m:
            k = m.group(1)
            rows[k] = dict(generation=m.group(2), tiers=m.group(3),
                           count=None if m.group(4) == "-" else int(m.group(4)),
                           last_ms=None if m.group(5) == "-" else int(m.group(5)))
    return {k: v for k, v in rows.items() if keys is None or k in keys}


def gw_metrics(name):
    h, _, mport = GW[name]
    out = ssh(h, f"curl -s --max-time 5 127.0.0.1:{mport}/metrics")
    m = {}
    for line in out.splitlines():
        if line.startswith("arca_gateway_access_") and not line.startswith("#"):
            k, _, v = line.rpartition(" ")
            try:
                m[k] = float(v)
            except ValueError:
                pass
    return m


def recording(name):
    return int(gw_metrics(name).get("arca_gateway_access_recording", -1))


def all_recording(want):
    return all(recording(g) == want for g in GW)


def flushed(names=GW):
    """Every read offered to each gateway's table is flushed or counted
    lost, and nothing is pending."""
    ok = True
    for g in names:
        m = gw_metrics(g)
        lost = sum(v for k, v in m.items() if k.startswith("arca_gateway_access_dropped_total")) + \
            m.get("arca_gateway_access_uncertain_total", 0)
        ok &= m.get("arca_gateway_access_reads_total", 0) == m.get("arca_gateway_access_flushed_reads_total", 0) + lost
        ok &= m.get("arca_gateway_access_queue_entries", 1) == 0 and m.get("arca_gateway_access_queue_in_flight", 1) == 0
    return ok


def fault(h, name, text):
    ssh(h, f"printf %s {shlex.quote(text)} > {RUN}/{name}.fault")


# ---------------------------------------------------------------- the load

LOADER = f"{RUN}/adr35-loader.py"


def eps(names=GW):
    return " ".join(f"--endpoint {g}=http://{addr(GW[g][0])}:{GW[g][1]}" for g in names)


def load_start(run, tag, keys, rate=20, workers=2):
    stopf = f"{RUN}/load-{tag}.stop"
    ssh("a", f"rm -f {stopf}; nohup python3 -I {LOADER} get {eps()} --bucket {BUCKET} "
             + " ".join(f"--key {k}" for k in keys)
             + f" --workers {workers} --rate {rate} --stop-file {stopf} --duration-secs 1800"
             f" --out {RUN}/logs/load-{tag}.txt > {RUN}/logs/load-{tag}.err 2>&1 < /dev/null & echo $! > {RUN}/load-{tag}.pid")
    run.ev("load start", tag=tag, keys=len(keys), rate_per_gateway=rate, workers_per_gateway=workers)


def load_stop(run, tag):
    ssh("a", f"touch {RUN}/load-{tag}.stop; p=$(cat {RUN}/load-{tag}.pid 2>/dev/null); "
             f"for i in $(seq 1 200); do kill -0 $p 2>/dev/null || break; sleep 0.1; done")
    run.ev("load stop", tag=tag)


def load_counts(tag, since=0, until=None):
    """Successful GETs of a load file, by (gateway, key)."""
    out = ssh("a", f"cat {RUN}/logs/load-{tag}.txt 2>/dev/null", timeout=300)
    counts, bad = {}, 0
    for line in out.splitlines():
        t, g, k, status, _n = line.split()
        t = int(t)
        if t < since or (until and t > until):
            continue
        if status != "200":
            bad += 1
            continue
        counts[(g, k)] = counts.get((g, k), 0) + 1
    return counts, bad


def gets(run, gw, key, n, tag="once"):
    ssh("a", f"python3 -I {LOADER} once {eps([gw])} --bucket {BUCKET} --key {key} --count {n} "
             f"--out {RUN}/logs/load-{tag}.txt")
    run.ev("gets", gateway=gw, key=key, count=n)


def stop_all_load():
    ssh("a", f"for f in {RUN}/load-*.pid; do [ -f $f ] || continue; touch ${{f%.pid}}.stop; done", check=False)


# ---------------------------------------------------------------- logs

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z\s+(\w+)\s+(\S+): (.*)$")


def iso_ms(ts):
    import datetime
    d = datetime.datetime.strptime(ts[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=datetime.timezone.utc)
    return int(d.timestamp() * 1000)


def log_lines(h, name, contains=None, since_ms=0):
    grep = f"| grep -a -F {shlex.quote(contains)}" if contains else ""
    out = ssh(h, f"cat {RUN}/logs/{name}.log 2>/dev/null {grep}", timeout=300, check=False)
    rows = []
    for raw in out.splitlines():
        line = ANSI.sub("", raw)
        m = TS.match(line)
        if not m:
            continue
        t = iso_ms(m.group(1))
        if t < since_ms:
            continue
        rows.append(dict(t_ms=t, level=m.group(2), target=m.group(3), text=m.group(4)))
    return rows


def field(text, name):
    m = re.search(rf"\b{name}=(\S+)", text)
    return m.group(1) if m else None


def observation(text):
    m = re.search(r"observation=(.*)$", text)
    return m.group(1) if m else ""


OBS = re.compile(r"enabled (true|false) generation (\d+) acknowledged generation (\d+) term (\d+) "
                 r"same incarnation (true|false); leader term (\d+) lease (-?\d+) ms after the read")


def parse_obs(s):
    m = OBS.search(s)
    if not m:
        return None
    g = m.groups()
    return dict(enabled=g[0] == "true", generation=int(g[1]), applied_generation=int(g[2]),
                applied_term=int(g[3]), same_incarnation=g[4] == "true", leader_term=int(g[5]),
                lease_left_ms=int(g[6]))


def refreshes(gw, since_ms):
    """Each completed tiering refresh of a gateway: wall-clock start and
    end (its host's clock) and what it observed."""
    h = GW[gw][0]
    rows = []
    for r in log_lines(h, gw, "tiering ", since_ms - 5000):
        if "tiering refresh" not in r["text"] and "tiering observation changed" not in r["text"]:
            continue
        s, c = field(r["text"], "started_ms"), field(r["text"], "completed_ms")
        if s is None:
            continue
        rows.append(dict(started_ms=int(s), completed_ms=int(c), obs=parse_obs(observation(r["text"])),
                         raw=observation(r["text"])))
    return [r for r in rows if r["started_ms"] >= since_ms - 3000]


def decisions(gw, since_ms):
    h = GW[gw][0]
    rows = []
    for r in log_lines(h, gw, "access recording for tiering changed", since_ms):
        rows.append(dict(t_ms=r["t_ms"], recording=field(r["text"], "recording") == "true",
                         age=field(r["text"], "observation_age_ms"), obs=observation(r["text"])))
    return rows


def acks(since_ms):
    rows = []
    for c, (h, _) in CTL.items():
        for r in log_lines(h, c, "tiering record acknowledged", since_ms):
            rows.append(dict(controller=c, t_ms=r["t_ms"], generation=int(field(r["text"], "generation")),
                             term=int(field(r["text"], "term")), enabled=field(r["text"], "enabled") == "true",
                             grace_from_ms=int(field(r["text"], "grace_from_ms") or 0),
                             promotion_from_ms=int(field(r["text"], "promotion_from_ms") or 0)))
    return sorted(rows, key=lambda r: r["t_ms"])


def ctl_events(since_ms, words=("acquired leadership", "released leadership", "lost leadership",
                                 "stepping down", "tiering record seeded", "flags differ",
                                 "test fault", "tiering pass", "tiering: the tiering record changed",
                                 "fenced", "Fenced")):
    rows = []
    for c, (h, _) in CTL.items():
        for r in log_lines(h, c, None, since_ms):
            if any(w in r["text"] for w in words):
                rows.append(dict(controller=c, t_ms=r["t_ms"], level=r["level"], text=r["text"][:400]))
    return sorted(rows, key=lambda r: r["t_ms"])


# ---------------------------------------------------------------- setup

def controller_cmd(name, flags):
    h, http = CTL[name]
    return (f"{BIN} controller --allow-shared-filesystem --cluster-file {CF} --id {name} "
            f"--http 127.0.0.1:{http} --failure-domain node --tiering-interval-secs 5 "
            f"--tiering-clock-skew-secs {SKEW_SECS} "
            + " ".join(f"--tiering-tier {t}" for t in flags["tiers"])
            + f" --tiering-promote-reads {flags['promote_reads']} --tiering-promote-window-secs {flags['window']} "
              f"--tiering-demote-after-secs {flags['demote']}")


def start_controller(name, flags, at_ms=None):
    h, _ = CTL[name]
    start(h, name, controller_cmd(name, flags),
          env=f"RUST_LOG=info ARCA_TEST_FAULT_FILE={RUN}/{name}.fault", at_ms=at_ms)


def start_gateway(name):
    h, port, mport = GW[name]
    start(h, name,
          f"{BIN} gateway --backend cluster --cluster-file {CF} --allow-anonymous "
          f"--listen {addr(h)}:{port} --metrics-listen 127.0.0.1:{mport} --k 2 --m 1 "
          f"--chunk-bytes 262144 --small-threshold 0 --failure-domain node",
          env=f"RUST_LOG=info,arca_lake_gateway::cluster::backend=debug ARCA_TEST_FAULT_FILE={RUN}/{name}.fault")


def sampler(h, name, port):
    """Gateway access metrics every 0.5 s with the host's clock."""
    script = (f"while [ ! -f {RUN}/sampler.stop ]; do t=${{EPOCHREALTIME/./}}; t=${{t:0:13}}; "
              f"curl -s --max-time 1 127.0.0.1:{port}/metrics | awk -v t=$t "
              f"'/^arca_gateway_access_(reads_total|unrecorded_total|recording|flushed_reads_total|dropped_total|uncertain_total|queue_entries|queue_in_flight)/ {{print t, $1, $2}}'; "
              f"sleep 0.5; done")
    ssh(h, f"cd {RUN} || exit 1; nohup bash -c {shlex.quote(script)} >> logs/{name}-metrics.txt 2>/dev/null < /dev/null & echo $! > {name}-sampler.pid")


def cmd_setup():
    run = Run("setup")
    for h in HOSTS:
        exists = ssh(h, f"ls -d {RUN} {FDB_DATA} 2>/dev/null", check=False).strip()
        if exists:
            raise SystemExit(f"host {h}: {exists} exists: refusing to set up over it")
    cf = f"arca44:p44@{addr('a')}:{FDB_PORT}"
    for h in HOSTS:
        ssh(h, f"mkdir -p {RUN}/logs && echo {cf} > {CF}")
    ssh("a", f"mkdir -p {FDB_DATA}/d0 {FDB_DATA}/l0")
    start("a", "fdb0", f"fdbserver -p {addr('a')}:{FDB_PORT} -C {CF} -d {FDB_DATA}/d0 -L {FDB_DATA}/l0")
    time.sleep(2)
    ssh("a", f"fdbcli -C {CF} --exec 'configure new single ssd' >/dev/null 2>&1; "
             f"for i in $(seq 1 60); do fdbcli -C {CF} --exec 'status minimal' | grep -q 'is available' && exit 0; sleep 1; done; exit 1")
    for i in range(NODES):
        start("b", f"d{i}", f"{BIN} storage-node --listen {addr('b')}:{9110 + i} --dir {RUN}/d{i} "
                            f"--metrics-listen 127.0.0.1:{9710 + i} --capacity-bytes {2 << 30}")
    time.sleep(3)
    ssh("a", f"{ADMIN} init > /dev/null 2>&1")
    for i in range(NODES):
        cls = "hdd" if i < 3 else "ssd"
        ssh("a", f"{ADMIN} register-node --address {addr('b')}:{9110 + i} --label node=n{i} "
                 f"--label device_class={cls} > /dev/null 2>&1")
    for tier, cls in (("default", "hdd"), ("fast", "ssd")):
        ssh("a", f"{ADMIN} create-groups --tier {tier} --device-class {cls} --failure-domain node --count 1 "
                 f"--k 2 --m 1 --chunk-bytes 262144 --small-threshold 0 > /dev/null 2>&1")
    for g in GW:
        start_gateway(g)
    for g, (h, _, mport) in GW.items():
        sampler(h, g, mport)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "adr35-loader.py")) as f:
        loader = f.read()
    ssh("a", f"cat > {LOADER} <<'LOADER_EOF'\n{loader}\nLOADER_EOF")
    st = ssh("a", f"{ADMIN} upgrade status 2>&1")
    run.ev("setup", upgrade_status=st.strip()[-200:], groups=ssh("a", f"{ADMIN} status 2>&1 | grep -c '^group '").strip())
    run.check("schema 4 from init", "schema Some(4)" in st, status=st.strip()[-200:])
    t, _ = tiering()
    run.check("no tiering record before the controllers", t is None)


# ---------------------------------------------------------------- scenarios

def sc_a(run, label, flags_a, flags_c):
    """Two controllers started at the same instant on a store without a
    tiering record."""
    t, out = tiering()
    run.check(f"{label}: no tiering record before", t is None, output=out.strip()[-200:])
    since = remote_ms("a")
    at = since + 3000
    # The status as it changes, from host A, every 0.3 s.
    poll = threading.Event()
    seen = []

    def poller():
        while not poll.is_set():
            rec, _ = tiering()
            seen.append((now_ms(), rec and (rec["generation"], rec["acknowledged"], rec["applied_term"])))
            time.sleep(0.3)

    th = threading.Thread(target=poller, daemon=True)
    th.start()
    # The hosts' clocks are NTP-synchronised (offsets recorded at the start
    # of the run): the same instant on both.
    FLAGS.update({"ctl-a": flags_a, "ctl-c": flags_c})
    start_controller("ctl-a", flags_a, at_ms=at)
    start_controller("ctl-c", flags_c, at_ms=at)
    run.ev(f"{label}: controllers start at", at_ms_a=at)
    rec = wait_until(run, f"{label}: record seeded and acknowledged", 90,
                     lambda: (lambda r: r if r and r["acknowledged"] else None)(tiering()[0]))
    poll.set()
    th.join()
    l = leader()
    ev = ctl_events(since - 1000)
    acquired = [e for e in ev if "acquired leadership" in e["text"]]
    seeded = [e for e in ev if "tiering record seeded" in e["text"]]
    run.ev(f"{label}: controller events", events=ev)
    run.ev(f"{label}: status sequence", seen=[s for i, s in enumerate(seen) if i == 0 or s[1] != seen[i - 1][1]])
    run.check(f"{label}: one leader acquired", len(acquired) == 1 and acquired[0]["controller"] == l["id"],
              acquired=[a["controller"] for a in acquired], leader=l["id"])
    run.check(f"{label}: seeded once, by the leader", len(seeded) == 1 and seeded[0]["controller"] == l["id"],
              seeded=[s["controller"] for s in seeded])
    flags = flags_a if l["id"] == "ctl-a" else flags_c
    got = dict(tiers=rec["tiers"], promote_reads=rec["promote_reads"], window=rec["window"], demote=rec["demote"])
    run.check(f"{label}: the record is the leader's flags, whole", got == flags and rec["generation"] == 1,
              record=got, generation=rec["generation"], leader_flags=flags)
    run.check(f"{label}: acknowledged by the leader's term",
              rec["applied_term"] == l["token"] and rec["applied_generation"] == 1,
              applied_term=rec["applied_term"], leader_term=l["token"])
    run.check(f"{label}: gateways recorded throughout (record enabled or absent)",
              all(d["recording"] for g in GW for d in decisions(g, since)) and all_recording(1))
    return l


def sc_a1(run):
    sc_a(run, "A1", INERT, INERT)


def sc_a2(run):
    for c in CTL:
        stop(CTL[c][0], c)
    # A store without a record again: the key cleared on the disposable
    # cluster only (its cluster file).
    ssh("a", f"fdbcli -C {CF} --exec 'writemode on; clear \\x02al\\x00\\x02tiering\\x00' >/dev/null")
    run.ev("A2: tiering key cleared on the disposable store")
    sc_a(run, "A2", INERT, INERT2)


def populate(run):
    ssh("a", f"python3 -I {LOADER} put {eps(['gw0'])} --bucket {BUCKET} "
             + " ".join(f"--key {k}" for k in OBJECTS + HOT + SPECIAL) + f" --size {SIZE}", timeout=600)
    out = ssh("a", f"python3 -I {LOADER} check {eps()} --bucket {BUCKET} "
                   + " ".join(f"--key {k}" for k in OBJECTS + HOT + SPECIAL) + f" --size {SIZE}", timeout=600)
    run.check("population read back through both gateways", out.strip().endswith(" 0 bad"), result=out.strip())
    objs = objects()
    run.check("every object in tier default", all(v["tiers"] == "default" for v in objs.values())
              and len(objs) == len(OBJECTS + HOT + SPECIAL), objects=len(objs))


def ensure(run, enabled, policy=None):
    """Bring the record to `enabled` (with `policy`), acknowledged, and wait
    for both gateways' decision."""
    t, _ = tiering()
    if enabled:
        if not t["enabled"] or (policy and not same_policy(t, policy)):
            admin_change(run, "enable " + (policy_args(policy) if policy else ""))
    elif t["enabled"]:
        admin_change(run, "disable")
    wait_until(run, "acknowledged", 30, lambda: tiering()[0]["acknowledged"])
    wait_until(run, f"gateways recording {int(enabled)}", 30, lambda: all_recording(1 if enabled else 0))


def same_policy(t, p):
    return (t["tiers"], t["promote_reads"], t["window"], t["demote"]) == (p["tiers"], p["promote_reads"], p["window"], p["demote"])


def reflection(run, label, cmd, want_recording, ackless=False):
    """One admin change and where it reached: the commit (host A), the
    leader's acknowledgment, each gateway's first refresh started after
    the target state, and its recording decision."""
    since = remote_ms("a")
    rec, t0, t1 = admin_change(run, cmd)
    g = rec["generation"]
    if not ackless:
        wait_until(run, f"{label}: acknowledged", 30,
                   lambda: (lambda t: t["acknowledged"] and t["generation"] == g)(tiering()[0]))
    wait_until(run, f"{label}: gateways recording {want_recording}", 30, lambda: all_recording(want_recording))
    time.sleep(REFRESH_MS / 1000 * 2)
    ack = [a for a in acks(since) if a["generation"] == g]
    # The target state: acknowledged (disable, a policy change), or, for an
    # enable, committed (enabled needs no acknowledgment to record).
    target_ms = t1 if ackless else ack[0]["t_ms"]
    out = dict(label=label, cmd=cmd, generation=g, admin_start_ms=t0, admin_end_ms=t1,
               ack_ms=ack[0]["t_ms"] if ack else None, ack_by=ack[0]["controller"] if ack else None,
               target_ms=target_ms, gateways={})
    for gw in GW:
        rows = refreshes(gw, since)
        after = [r for r in rows if r["started_ms"] >= target_ms]
        before = [r for r in rows if r["completed_ms"] <= target_ms]
        first = after[0] if after else None
        if ackless:
            reflects = first and first["obs"] and first["obs"]["generation"] >= g and first["obs"]["enabled"]
        else:
            reflects = first and first["obs"] and first["obs"]["applied_generation"] >= g
        dec = [d for d in decisions(gw, since) if d["recording"] == bool(want_recording)]
        out["gateways"][gw] = dict(
            last_before=before[-1]["completed_ms"] if before else None,
            first_after_started=first["started_ms"] if first else None,
            first_after_completed=first["completed_ms"] if first else None,
            delay_ms=(first["completed_ms"] - target_ms) if first else None,
            period_ms=[b["started_ms"] - a["started_ms"] for a, b in zip(rows, rows[1:])][-5:],
            decision_ms=dec[0]["t_ms"] if dec else None)
        run.check(f"{label}: {gw} reflects it at the first refresh started after the target state",
                  bool(reflects), first=first and first["raw"], target_ms=target_ms,
                  started=first and first["started_ms"])
    run.ev(f"{label}: reflection", **out)
    return out


def sc_c1(run):
    ensure(run, True, INERT)
    load_start(run, "c1", OBJECTS)
    res = []
    try:
        for rep in range(3):
            res.append(reflection(run, f"C1.{rep} disable", "disable", 0))
            res.append(reflection(run, f"C1.{rep} set while disabled",
                                  "set " + policy_args(INERT2 if rep % 2 == 0 else INERT), 0))
            res.append(reflection(run, f"C1.{rep} enable", "enable", 1, ackless=True))
            res.append(reflection(run, f"C1.{rep} set while enabled",
                                  "set " + policy_args(INERT if rep % 2 == 0 else INERT2), 1))
    finally:
        load_stop(run, "c1")
    json.dump(res, open(os.path.join(run.dir, "c1-reflection.json"), "w"), indent=1)


def sc_c2(run):
    """gw0's tiering refresh failing, stalled, returning late; gw1 as the
    control."""
    ensure(run, False)
    load_start(run, "c2", OBJECTS)
    res = {}
    try:
        for mode, text, hold_s in (("fail", "tiering-read fail", 6), ("delay", "tiering-read delay 15000", 25)):
            since = remote_ms("b")
            fault("b", "gw0", text)
            run.ev(f"C2 {mode}: fault on gw0", at_ms_b=since)
            wait_until(run, f"C2 {mode}: gw0 records again", 30, lambda: recording("gw0") == 1)
            time.sleep(hold_s)
            fault("b", "gw0", "")
            cleared = remote_ms("b")
            wait_until(run, f"C2 {mode}: gw0 confirmed again", 40, lambda: recording("gw0") == 0)
            rows = refreshes("gw0", since)
            dec = decisions("gw0", since)
            on = [d for d in dec if d["recording"]]
            # The observation in force when recording resumed: the last read
            # observed before it.
            ok_before = [r for r in rows if on and r["completed_ms"] < on[0]["t_ms"]]
            last_ok = ok_before[-1] if ok_before else None
            off = [d for d in dec if not d["recording"] and d["t_ms"] > (on[0]["t_ms"] if on else 0)]
            resumed_after = on[0]["t_ms"] - last_ok["started_ms"] if on and last_ok else None
            res[mode] = dict(fault_ms=since, cleared_ms=cleared, last_ok_started=last_ok and last_ok["started_ms"],
                             recording_on_ms=on[0]["t_ms"] if on else None, resumed_after_ms=resumed_after,
                             recording_off_ms=off[0]["t_ms"] if off else None, decisions=dec[:12],
                             refreshes=[(r["started_ms"], r["completed_ms"], r["raw"][:80]) for r in rows][:30])
            run.check(f"C2 {mode}: recording resumes at the stale bound after the last good read's start",
                      resumed_after is not None and STALE_MS <= resumed_after <= STALE_MS + 1000,
                      resumed_after_ms=resumed_after)
            if mode == "delay":
                late = [r for r in rows if r["completed_ms"] - r["started_ms"] >= 14000]
                bad = [d for d in dec if not d["recording"] and on and on[0]["t_ms"] < d["t_ms"] < cleared]
                run.check("C2 delay: late replies (old snapshots) did not stop recording",
                          len(late) >= 1 and not bad, late=len(late), wrongly_off=bad[:3])
            run.check(f"C2 {mode}: gw1 (no fault) stayed confirmed",
                      not [d for d in decisions("gw1", since) if d["recording"]])
        # A late reply carrying the disabled state after an enable: the enable
        # lands about 1 s into a held read (each read starts REFRESH_MS after
        # the previous one was observed), whose snapshot, taken before the
        # enable, is observed 6 s old: reads unrecorded for at most the stale
        # bound after the enable.
        # (A 4 s hold: the held snapshot's lease still outlasts its
        # observation by more than the gateways' skew, so that it counts as a
        # confirmation; with a longer hold the rule may correctly refuse it,
        # and the case is not formed. Retried at most three times.)
        for attempt in range(3):
            if attempt:
                ensure(run, False)
            since = remote_ms("b")
            fault("b", "gw0", "tiering-read delay 4000")
            held = wait_until(run, "C2 late: a held read observed", 30,
                              lambda: [r for r in refreshes("gw0", since) if r["completed_ms"] - r["started_ms"] >= 3900])
            next_start = held[-1]["completed_ms"] + REFRESH_MS
            while remote_ms("b") < next_start + 1000:
                time.sleep(0.05)
            rec, t0, t1 = admin_change(run, "enable")
            time.sleep(20)
            fault("b", "gw0", "")
            rows = refreshes("gw0", since)
            dec = decisions("gw0", since)
            spanning = [r for r in rows if r["started_ms"] < t0 and r["completed_ms"] > t1]
            formed = bool(spanning) and spanning[0]["obs"] and not spanning[0]["obs"]["enabled"] and \
                spanning[0]["obs"]["lease_left_ms"] > LEASE_SKEW_MS
            run.ev("C2 late: attempt", attempt=attempt, formed=formed,
                   spanning=spanning[:1] and spanning[0]["raw"])
            if formed:
                break
        late_off = [d for d in dec if not d["recording"] and d["t_ms"] > t1]
        last_on = [d for d in dec if d["recording"] and d["t_ms"] > t1]
        res["late_after_enable"] = dict(enable_start_ms_a=t0, enable_end_ms_a=t1, attempts=attempt + 1,
                                        spanning_read=spanning[:1] and (spanning[0]["started_ms"], spanning[0]["completed_ms"], spanning[0]["raw"]),
                                        off_after_enable_ms=[d["t_ms"] - t1 for d in late_off],
                                        on_after_enable_ms=[d["t_ms"] - t1 for d in last_on], decisions=dec[:20])
        run.check("C2 late: a read started before the enable was observed after it, a confirmation of disabled",
                  formed, spanning=spanning[:1] and spanning[0]["raw"], attempts=attempt + 1)
        # Not recording just after the enable (a decision logged after it, or
        # the state carried from before it), then recording again when the
        # held confirmation went stale, at its read's start plus the stale
        # bound (not at the earlier read's: the late reply was used), within
        # the stale bound of the enable.
        # (Only changes are logged: the state at the enable is the opposite
        # of the first change after it.)
        after = [d for d in dec if d["t_ms"] > t1]
        off_at_enable = bool(after) and (after[0]["recording"] or bool(late_off))
        resumed = last_on[-1]["t_ms"] if last_on else None
        expected = spanning[0]["started_ms"] + STALE_MS if spanning else None
        run.check("C2 late: the old confirmation kept recording off after the enable, for at most the stale bound",
                  bool(formed and off_at_enable and resumed and abs(resumed - expected) <= 500
                       and resumed <= t0 + STALE_MS + 500
                       and not [d for d in late_off if d["t_ms"] > t0 + STALE_MS]),
                  off_at_enable=bool(off_at_enable), resumed_after_enable_ms=resumed and resumed - t1,
                  resumed_minus_spanning_start_ms=resumed and expected and resumed - spanning[0]["started_ms"])
        wait_until(run, "C2 late: recording", 30, lambda: all_recording(1))
    finally:
        fault("b", "gw0", "")
        load_stop(run, "c2")
    json.dump(res, open(os.path.join(run.dir, "c2-stale.json"), "w"), indent=1)


def gw_counters():
    return {g: gw_metrics(g) for g in GW}


def accounting(run, label, tag, before, after, keys_before=None, keys_after=None, keys=None):
    """Reads served (load generator) against each gateway's counters and,
    when given, the persisted access counts."""
    counts, bad = load_counts(tag)
    out = {"load_errors": bad}
    for g in GW:
        served = sum(v for (gg, _), v in counts.items() if gg == g)
        d = {k: after[g].get(k, 0) - before[g].get(k, 0) for k in after[g]}
        rec = d.get("arca_gateway_access_reads_total", 0)
        unrec = d.get("arca_gateway_access_unrecorded_total", 0)
        lost = sum(v for k, v in d.items() if k.startswith("arca_gateway_access_dropped_total")) + \
            d.get("arca_gateway_access_uncertain_total", 0)
        out[g] = dict(served=served, recorded=rec, unrecorded=unrec, flushed=d.get("arca_gateway_access_flushed_reads_total", 0),
                      lost=lost)
        run.check(f"{label}: {g} every served GET recorded or counted unrecorded",
                  served == rec + unrec and bad == 0, **out[g])
        run.check(f"{label}: {g} every recorded read flushed (none dropped or uncertain)",
                  rec == out[g]["flushed"] and lost == 0, **out[g])
    if keys is not None:
        persisted = {k: (keys_after[k]["count"] or 0) - (keys_before[k]["count"] or 0) for k in keys}
        served_k = {k: sum(v for (_, kk), v in counts.items() if kk == k) for k in keys}
        diff = {k: (served_k[k], persisted[k]) for k in keys if served_k[k] != persisted[k]}
        out["persisted"] = sum(persisted.values())
        out["served_total"] = sum(served_k.values())
        run.check(f"{label}: persisted access counts equal the GETs served, per object", not diff,
                  differing=dict(list(diff.items())[:10]), persisted=out["persisted"], served=out["served_total"])
    run.ev(f"{label}: accounting", **out)
    return out


def sc_b1(run):
    """Graceful leader change, tiering confirmed disabled."""
    ensure(run, False)
    before = gw_counters()
    k0 = objects(OBJECTS)
    load_start(run, "b1", OBJECTS)
    time.sleep(5)
    l = leader()
    since = remote_ms("a")
    run.ev("B1: stopping the leader", leader=l)
    stop(CTL[l["id"]][0], l["id"])
    new = wait_until(run, "B1: a new leader", 40, lambda: (lambda x: x if x and x["token"] > l["token"] else None)(leader()))
    wait_until(run, "B1: acknowledged by the new term", 30,
               lambda: (lambda t: t["acknowledged"] and t["applied_term"] == new["token"])(tiering()[0]))
    wait_until(run, "B1: gateways confirmed again", 30, lambda: all_recording(0))
    time.sleep(5)
    load_stop(run, "b1")
    start_controller(l["id"], FLAGS[l["id"]])
    t = tiering()[0]
    rec_policy = dict(tiers=t["tiers"], promote_reads=t["promote_reads"], window=t["window"], demote=t["demote"])
    if FLAGS[new["id"]] != rec_policy:
        differ = [e for e in ctl_events(since, ("flags differ",)) if e["controller"] == new["id"]]
        run.check("B1: the new leader, whose flags differ, runs from the record (and says so)",
                  bool(differ), flags=FLAGS[new["id"]], record=rec_policy)
    wait_until(run, "B1: flushed", 60, flushed)
    after = gw_counters()
    k1 = objects(OBJECTS)
    timeline(run, "B1", since, l, new)
    acc = accounting(run, "B1", "b1", before, after)
    persisted = sum((k1[k]["count"] or 0) - (k0[k]["count"] or 0) for k in OBJECTS)
    recorded = sum(acc[g]["recorded"] for g in GW)
    run.check("B1: persisted access counts equal the reads recorded during the change", persisted == recorded,
              persisted=persisted, recorded=recorded)
    t = tiering()[0]
    run.check("B1: the record unchanged but for its acknowledgment",
              not t["enabled"] and t["applied_term"] == new["token"], record=t)


def timeline(run, label, since, old, new):
    """The ordered events of a leader change and each gateway's decisions,
    with the ADR-0035 §10.3 rule checked on every refresh."""
    ev = ctl_events(since - 2000)
    ak = acks(since - 2000)
    gws = {}
    for gw in GW:
        rows = refreshes(gw, since - 2000)
        dec = decisions(gw, since - 2000)
        gws[gw] = dict(refreshes=[(r["started_ms"], r["completed_ms"], r["raw"]) for r in rows if r["obs"]],
                       decisions=[(d["t_ms"], d["recording"], d["age"]) for d in dec])
        # Every observation, judged by the rule (an observation that
        # confirms disabled: acknowledged generation current, by the leader
        # term, lease beyond the skew).
        for r in rows:
            o = r["obs"]
            if not o:
                continue
            confirms = (not o["enabled"] and o["applied_generation"] == o["generation"]
                        and o["applied_term"] == o["leader_term"] and o["same_incarnation"]
                        and o["lease_left_ms"] > LEASE_SKEW_MS)
            r["confirms"] = confirms
        # The decision after each refresh matches the refresh's rule (the
        # first decision logged after it and before the next refresh).
        wrong = []
        for i, r in enumerate(rows):
            if "confirms" not in r:
                continue
            nxt = rows[i + 1]["completed_ms"] if i + 1 < len(rows) else r["completed_ms"] + STALE_MS
            stale_at = r["started_ms"] + STALE_MS
            for d in dec:
                # (5 ms: a decision evaluated just before the observation was
                # stored may be logged just after it.)
                if r["completed_ms"] + 5 < d["t_ms"] < min(nxt, stale_at) and d["recording"] == r["confirms"]:
                    wrong.append((r["started_ms"], r["raw"][:120], d["t_ms"], d["recording"]))
        gws[gw]["wrong"] = wrong
        run.check(f"{label}: {gw} decisions follow the rule on every observation", not wrong, wrong=wrong[:5])
    run.ev(f"{label}: timeline", old_leader=old, new_leader=new, controller=ev, acks=ak, gateways=gws)
    json.dump(dict(controller=ev, acks=ak, gateways=gws, old=old, new=new),
              open(os.path.join(run.dir, f"{label.lower()}-timeline.json"), "w"), indent=1)


def sc_b3a(run):
    """The leader paused past its lease and resumed, tiering disabled."""
    ensure(run, False)
    load_start(run, "b3a", OBJECTS)
    time.sleep(3)
    l = leader()
    since = remote_ms("a")
    pid = signal(CTL[l["id"]][0], l["id"], "STOP")
    run.ev("B3a: leader paused (SIGSTOP)", leader=l, pid=pid)
    try:
        wait_until(run, "B3a: gateways record (lease within the skew)", 20, lambda: all_recording(1))
        rec_on = remote_ms("a")
        new = wait_until(run, "B3a: a new leader", 40,
                         lambda: (lambda x: x if x and x["token"] > l["token"] else None)(leader()))
        wait_until(run, "B3a: acknowledged by the new term", 30,
                   lambda: (lambda t: t["acknowledged"] and t["applied_term"] == new["token"])(tiering()[0]))
        wait_until(run, "B3a: gateways confirmed again", 30, lambda: all_recording(0))
        time.sleep(3)
    finally:
        signal(CTL[l["id"]][0], l["id"], "CONT")
    resumed = remote_ms("a")
    run.ev("B3a: old leader resumed (SIGCONT)", at_ms_a=resumed)
    wait_until(run, "B3a: the old leader gave up its term", 30,
               lambda: any(e["controller"] == l["id"] and ("lost leadership" in e["text"] or "stepping down" in e["text"])
                           for e in ctl_events(resumed - 1000)))
    time.sleep(10)
    load_stop(run, "b3a")
    t = tiering()[0]
    after = leader()
    run.check("B3a: the new leader keeps the term and the acknowledgment",
              after["token"] == new["token"] and t["applied_term"] == new["token"] and t["acknowledged"],
              leader=after, record=t)
    late = [d for g in GW for d in decisions(g, resumed) if d["recording"]]
    run.check("B3a: the old leader's return did not change any gateway's decision", not late, late=late[:3])
    old_acks = [a for a in acks(resumed - 1000) if a["controller"] == l["id"]]
    run.check("B3a: no acknowledgment by the old leader after its return", not old_acks, acks=old_acks)
    timeline(run, "B3a", since, l, new)
    run.ev("B3a: paused", rec_on_ms_a=rec_on, since_ms_a=since)


def tiers_of(keys):
    o = objects(keys)
    return {k: o[k]["tiers"] for k in keys}


def sc_b2(run):
    """Tiering enabled on two tiers through a leader change: the cutoff,
    promotion, the grace, accounting."""
    ensure(run, True, INERT)
    # px gets an old count (20 reads) under the inert policy.
    gets(run, "gw0", "px", 20, tag="b2-once")
    wait_until(run, "B2: px's 20 reads flushed", 60, lambda: (objects(["px"])["px"]["count"] or 0) >= 20)
    ensure(run, False)
    # Enabled again: the acknowledgment stamps the grace and the cutoff.
    rec, _, _ = admin_change(run, "enable " + policy_args(MOVE))
    t = wait_until(run, "B2: acknowledged", 30, lambda: (lambda t: t if t["acknowledged"] else None)(tiering()[0]))
    t_ack = t["grace_from_ms"]
    run.check("B2: enabling stamped the grace and the cutoff", t["grace_from_ms"] == t["promotion_from_ms"] > rec["changed_ms"] - 1,
              grace_from_ms=t["grace_from_ms"], promotion_from_ms=t["promotion_from_ms"])
    wait_until(run, "B2: gateways record", 30, lambda: all_recording(1))
    gets(run, "gw0", "px", 1, tag="b2-once")
    gets(run, "gw1", "py", 6, tag="b2-once")
    load_start(run, "b2-hot", HOT, rate=10)
    wait_until(run, "B2: py and every hot object promoted", 90,
               lambda: all(v == "fast" for v in tiers_of(HOT + ["py"]).values()))
    o = objects(["px"])["px"]
    run.check("B2: px's old count did not promote it (count replaced at the cutoff)",
              o["tiers"] == "default" and o["count"] == 1, px=o)
    gets(run, "gw1", "px", 5, tag="b2-once")
    wait_until(run, "B2: px promoted by reads after the cutoff", 60, lambda: tiers_of(["px"])["px"] == "fast")
    load_stop(run, "b2-hot")
    wait_until(run, "B2: flushed", 60, flushed)
    before = gw_counters()
    k0 = objects(HOT)
    load_start(run, "b2-window", HOT, rate=10)
    # The leader change inside the grace window: without the grace, py (reset
    # at its promotion) would be demoted demote_after after it.
    time.sleep(3)  # the promotion's reset follows its switch
    promoted = {k: objects([k])[k]["last_ms"] for k in ("px", "py")}
    bound = {k: max(promoted[k], t_ack + MARGIN_MS) + MOVE["demote"] * 1000 for k in promoted}
    no_grace = {k: promoted[k] + MOVE["demote"] * 1000 for k in promoted}
    run.ev("B2: demotion bounds", t_ack=t_ack, margin_ms=MARGIN_MS, promoted_last_ms=promoted, bound=bound,
           without_grace=no_grace)
    fail_at = no_grace["py"] - 5000
    while remote_ms("a") < fail_at:
        time.sleep(0.5)
    l = leader()
    since = remote_ms("a")
    stop(CTL[l["id"]][0], l["id"])
    new = wait_until(run, "B2: a new leader", 40, lambda: (lambda x: x if x and x["token"] > l["token"] else None)(leader()))
    t2 = wait_until(run, "B2: acknowledged by the new term", 30,
                    lambda: (lambda t: t if t["acknowledged"] and t["applied_term"] == new["token"] else None)(tiering()[0]))
    start_controller(l["id"], FLAGS[l["id"]])
    run.ev("B2: leader change", stopped_ms_a=since, new_leader=new,
           inside_the_grace_window=since < no_grace["py"] and new["token"] > l["token"])
    keep = ("generation", "grace_from_ms", "promotion_from_ms", "tiers", "promote_reads", "window", "demote", "enabled")
    run.check("B2: a leader change changed only the acknowledging term",
              all(t2[k] == t[k] for k in keep), before={k: t[k] for k in keep}, after={k: t2[k] for k in keep})
    # Demotion: none before the bound, then it happens.
    first_default = {}
    deadline = max(bound.values()) + 60_000
    while remote_ms("a") < deadline and len(first_default) < 2:
        now = remote_ms("a")
        for k, v in tiers_of(["px", "py"]).items():
            if v == "default" and k not in first_default:
                first_default[k] = now
        time.sleep(1)
    load_stop(run, "b2-window")
    wait_until(run, "B2: flushed after the window", 60, flushed)
    after = gw_counters()
    k1 = objects(HOT)
    timeline(run, "B2", since, l, new)
    for k in ("px", "py"):
        run.check(f"B2: {k} not demoted before demote_after plus the grace margin, then demoted",
                  k in first_default and first_default[k] >= bound[k] and first_default[k] <= bound[k] + 30_000,
                  demoted_ms=first_default.get(k), bound_ms=bound[k], without_grace_ms=no_grace[k])
    run.check("B2: hot objects stayed in the upper tier", all(v == "fast" for v in tiers_of(HOT).values()))
    run.check("B2: gateways recorded throughout", not [d for g in GW for d in decisions(g, since) if not d["recording"]])
    accounting(run, "B2 window", "b2-window", before, after, k0, k1, HOT)


def sc_b3b(run):
    """The leader paused while its tiering move is held before the switch.
    The successor, under the same policy, finds the move in flight: it moves
    nothing and keeps the reads. The policy changes; the old leader resumes:
    its switch is refused, nothing is reset."""
    ensure(run, True, MOVE)
    # Everything else settled (demotions of B2 done).
    wait_until(run, "B3b: earlier objects back in default", 240,
               lambda: all(v == "default" for v in tiers_of(HOT + ["px", "py"]).values()), every=2)
    # A fresh object (no earlier reads, no migration intent left by an
    # earlier run of this scenario).
    key = f"mv{remote_ms('a') % 1_000_000}"
    ssh("a", f"python3 -I {LOADER} put {eps(['gw0'])} --bucket {BUCKET} --key {key} --size {SIZE}")
    run.ev("B3b: object", key=key)
    l = leader()
    h = CTL[l["id"]][0]
    fault(h, l["id"], "tiering-before-switch hold")
    since = remote_ms("a")
    mv = objects([key])[key]
    gets(run, "gw0", key, 6, tag="b3b-once")
    held = wait_until(run, "B3b: the leader's move held before its switch", 60,
                      lambda: [e for e in ctl_events(since, ("test fault: tiering move held",)) if e["controller"] == l["id"]])
    run.check("B3b: the held move is mv's", mv["generation"] in held[0]["text"], held=held[0]["text"], mv=mv)
    pid = signal(h, l["id"], "STOP")
    paused = remote_ms("a")
    run.ev("B3b: leader paused with its move held", leader=l, pid=pid)
    try:
        gets(run, "gw1", key, 3, tag="b3b-once")
        want = 9
        new = wait_until(run, "B3b: a new leader", 40,
                         lambda: (lambda x: x if x and x["token"] > l["token"] else None)(leader()))
        wait_until(run, "B3b: acknowledged by the new term", 30,
                   lambda: (lambda t: t["acknowledged"] and t["applied_term"] == new["token"])(tiering()[0]))
        # Two passes of the successor under the policy that would promote mv.
        skips = wait_until(run, "B3b: the successor found mv's move in flight twice", 60,
                           lambda: (lambda e: e if len(e) >= 2 else None)(
                               [x for x in ctl_events(paused, ("another move of the chunk is in flight",))
                                if x["controller"] == new["id"] and mv["generation"] in x["text"]]))
        wait_until(run, "B3b: mv's reads flushed", 60, lambda: (objects([key])[key]["count"] or 0) >= want)
        o = objects([key])[key]
        run.check("B3b: the successor moved nothing and kept the reads", o["tiers"] == "default" and o["count"] == want,
                  mv=o, expected_count=want, skips=len(skips))
        rec, _, _ = admin_change(run, "set " + policy_args(HOLD))
        wait_until(run, "B3b: the new policy acknowledged by the new term", 30,
                   lambda: (lambda t: t["acknowledged"] and t["applied_term"] == new["token"] and t["generation"] == rec["generation"])(tiering()[0]))
    finally:
        signal(h, l["id"], "CONT")
    resumed = remote_ms("a")
    run.ev("B3b: old leader resumed (SIGCONT), its move still held")
    # Its term is over: its next fenced commits fail (its "lost leadership"
    # comes once every task has ended, the held one included).
    wait_until(run, "B3b: the old leader's commits are fenced", 30,
               lambda: [e for e in ctl_events(resumed - 500, ("is no longer current",)) if e["controller"] == l["id"]])
    released = remote_ms("a")
    fault(h, l["id"], "")
    wait_until(run, "B3b: the held move released and refused", 30,
               lambda: [e for e in ctl_events(released, ("chunk migration failed",)) if e["controller"] == l["id"]])
    wait_until(run, "B3b: the old leader gave up its term", 30,
               lambda: [e for e in ctl_events(released, ("lost leadership",)) if e["controller"] == l["id"]])
    time.sleep(10)
    ev = ctl_events(since - 1000)
    o = objects([key])[key]
    t = tiering()[0]
    run.ev("B3b: controller events", events=ev)
    run.check("B3b: the old term's move did not switch (mv stays in default)", o["tiers"] == "default", mv=o)
    run.check("B3b: the reads recorded meanwhile were not reset", o["count"] == want, mv=o, expected_count=want)
    run.check("B3b: the successor keeps the term and the new policy",
              leader()["token"] == new["token"] and t["generation"] == rec["generation"] and t["applied_term"] == new["token"],
              record=t)
    refused = [e for e in ev if e["controller"] == l["id"] and e["t_ms"] >= released and "chunk migration failed" in e["text"]]
    run.check("B3b: the old leader's switch was refused by its fence", bool(refused) and "no longer current" in refused[0]["text"],
              refused=refused[:1])


SCENARIOS = {"A1": sc_a1, "A2": sc_a2, "populate": populate, "C1": sc_c1, "C2": sc_c2, "B1": sc_b1,
             "B3a": sc_b3a, "B2": sc_b2, "B3b": sc_b3b}
ORDER = ["A1", "A2", "populate", "C1", "C2", "B1", "B3a", "B2", "B3b"]


def system_fdb_ok():
    ok = {}
    for h in ("a", "c"):
        ok[h] = "is available" in ssh(h, "timeout 20 fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal'",
                                      check=False)
    return ok


def collect(run):
    """Every log of this cluster into the evidence directory."""
    for h in HOSTS:
        d = os.path.join(run.dir, "raw", h)
        os.makedirs(d, exist_ok=True)
        subprocess.run(["rsync", "-a", "-e", f"ssh -o BatchMode=yes -o ControlPath={CM}/%C",
                        f"{HOSTS[h]}:{RUN}/logs/", d + "/"], check=False)


def cmd_run(names):
    run = Run("run")
    run.ev("start", scenarios=names, clocks=clock_offsets(), system_fdb=system_fdb_ok())
    rc = 0
    try:
        for n in names:
            if stop_requested():
                raise Stopped("stop file")
            run.ev(f"scenario {n} start")
            SCENARIOS[n](run)
            run.ev(f"scenario {n} done")
    except Failed as e:
        run.ev("failed", detail=str(e))
        rc = 1
    except Stopped as e:
        run.ev("stopped", why=str(e))
        rc = 3
    finally:
        stop_all_load()
        for c, (h, _) in CTL.items():
            # A paused controller is resumed: never left stopped.
            pid = ours(h, c)
            if pid:
                ssh(h, f"kill -CONT {pid}", check=False)
            fault(h, c, "")
        for g, (h, _, _) in GW.items():
            fault(h, g, "")
        run.ev("end", clocks=clock_offsets(), system_fdb=system_fdb_ok(), failed_checks=run.failed)
        collect(run)
    sys.exit(rc)


def cmd_teardown():
    run = Run("teardown")
    names = {"a": ["ctl-a", "fdb0"], "b": ["gw0"] + [f"d{i}" for i in range(NODES)], "c": ["ctl-c", "gw1"]}
    for h in HOSTS:
        ssh(h, f"touch {RUN}/sampler.stop", check=False)
    stop_all_load()
    for h in ("c", "a", "b"):
        for n in names[h]:
            if n != "fdb0":
                stop(h, n)
    stop("a", "fdb0")
    collect(run)
    for h in HOSTS:
        left = ssh(h, f"for p in {RUN}/*.pid; do [ -f $p ] && kill -0 $(cat $p) 2>/dev/null && echo $p; done", check=False).strip()
        run.ev("left running", host=h, pids=left)
        if left and not left.endswith("sampler.pid"):
            run.check(f"teardown: nothing of ours left on {h}", False, left=left)
    for h in HOSTS:
        ssh(h, f"rm -rf {RUN}" + (f" {FDB_DATA}" if h == "a" else ""))
    run.ev("removed", run_dir=RUN, fdb_data=FDB_DATA, system_fdb=system_fdb_ok())


def main():
    for k in ("HOST_A", "HOST_B", "HOST_C", "REMOTE_BIN", "RUN_DIR", "FDB_DATA", "FDB_PORT", "OUT"):
        if not E.get(k):
            raise SystemExit(f"{k} is not set")
    if "p41" in RUN or "p41" in FDB_DATA:
        raise SystemExit("refusing a run directory of the retained cluster")
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "setup":
        cmd_setup()
    elif cmd == "run":
        cmd_run(sys.argv[2:] or ORDER)
    elif cmd == "collect":
        collect(Run("collect"))
    elif cmd == "teardown":
        cmd_teardown()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
