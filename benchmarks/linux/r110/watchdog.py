#!/usr/bin/env python3
"""The external watchdog of the R-110 retest (plan §6.2, layer 3), on the
operator's machine.

  watchdog.py --config FILE

``FILE`` (JSON, not committed with real values): ``run_id``; ``hosts``
(name -> ssh target); ``load_host`` (the name of the host running the
guard and the load); ``system_fdb_hosts`` (names; their system
FoundationDB is only read, ``status minimal``); ``remote_run_dir``,
``remote_data_mount``, ``remote_test_dirs`` (on the load host); ``local_dir``
(this watchdog's log and stop file); optional thresholds (``LIMITS``; never
looser than the plan's) and ``optional_checks`` (checks whose source may
be missing, decided at preflight: ``kernel_log``, ``swap``).

Every ``interval`` (10 s) one round: each host's memory, free space of
``/`` and the data mount, load per CPU; on the load host also swap
activity, CPU iowait, kernel OOM or hung-task messages, the test
directories' allocated bytes and the test FoundationDB; every system
FoundationDB. A breach writes the run's stop file on the load host
(exclusively created: the first reason wins; the guard and the load act
on it) and locally, and the watchdog exits 2. It never kills a process
itself. Only a round in which every required check succeeded and passed
rewrites the load host's heartbeat (a higher ``seq``): a watchdog that
cannot see, or that has stopped, lets the heartbeat go stale and the
guard stops the load within its timeout. A host unreachable
``max_unreachable`` rounds in a row (6, 60 s) is a breach; the load host
unreachable stops the load through the heartbeat even if the stop file
cannot be written.
"""

import argparse
import json
import os
import subprocess
import sys
import time

from . import common

LIMITS = {
    "interval": 10.0,
    "min_free_gib": 3.0,
    "min_mem_mib": 1024.0,
    "max_load_per_cpu": 4.0,
    "swap_window_s": 30.0,
    "iowait_pct": 50.0,
    "iowait_window_s": 60.0,
    "test_fdb_down_s": 60.0,
    "max_unreachable": 6,
    "max_test_dir_bytes": None,
}
# Direction of "looser": a value past which a configuration is refused.
_LOOSER = {"min_free_gib": "lower", "min_mem_mib": "lower", "max_load_per_cpu": "higher",
           "swap_window_s": "higher", "iowait_pct": "higher", "iowait_window_s": "higher",
           "test_fdb_down_s": "higher", "max_unreachable": "higher", "interval": "higher"}

# Run on a host with `python3 -I -`: one JSON line of raw readings. A
# reading that cannot be made is null (the watchdog decides what that
# means), never 0.
CHECK_SCRIPT = r'''
import json, os, subprocess, sys
a = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
out = {}
def tryit(name, f):
    try:
        out[name] = f()
    except Exception as e:
        out[name] = None
        out.setdefault("errors", {})[name] = str(e)[:200]
def mem():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable:"):
            return int(l.split()[1]) * 1024
    raise OSError("no MemAvailable")
def free(p):
    s = os.statvfs(p)
    return s.f_bavail * (s.f_frsize or s.f_bsize)
tryit("mem_available", mem)
tryit("cpus", os.cpu_count)
tryit("load1", lambda: float(open("/proc/loadavg").read().split()[0]))
tryit("free_root", lambda: free("/"))
if a.get("data_mount"):
    tryit("free_data", lambda: free(a["data_mount"]))
if a.get("detail"):
    def vm():
        d = {}
        for l in open("/proc/vmstat"):
            k, v = l.split()
            if k in ("pswpin", "pswpout"):
                d[k] = int(v)
        return d["pswpin"] + d["pswpout"]
    tryit("swap_pages", vm)
    def cpu():
        f = open("/proc/stat").readline().split()[1:]
        v = [int(x) for x in f]
        return [v[4], sum(v)]
    tryit("cpu_iowait_total", cpu)
    def klog():
        r = subprocess.run(["journalctl", "-k", "--since", "-60s", "-o", "cat", "--no-pager"],
                           capture_output=True, text=True, timeout=20)
        if r.returncode != 0:
            raise OSError(r.stderr.strip()[:200] or "journalctl failed")
        return [l for l in r.stdout.splitlines()
                if "Out of memory" in l or "oom-kill" in l or "blocked for more than" in l][:20]
    tryit("kernel_alerts", klog)
    def du():
        total = 0
        for root in a.get("test_dirs", []):
            for d, _, files in os.walk(root):
                for n in files:
                    try:
                        total += os.lstat(os.path.join(d, n)).st_blocks * 512
                    except FileNotFoundError:
                        pass
        return total
    tryit("test_dir_bytes", du)
    if a.get("test_fdb"):
        def tfdb():
            r = subprocess.run(["fdbcli", "-C", a["test_fdb"], "--timeout", "10", "--exec", "status minimal"],
                               capture_output=True, text=True, timeout=20)
            return "is available" in r.stdout
        tryit("test_fdb_available", tfdb)
if a.get("system_fdb"):
    def sfdb():
        r = subprocess.run(["fdbcli", "-C", "/etc/foundationdb/fdb.cluster", "--timeout", "10",
                            "--exec", "status minimal"], capture_output=True, text=True, timeout=20)
        return "is available" in r.stdout
    tryit("system_fdb_available", sfdb)
print(json.dumps(out))
'''


def ssh_runner(target, script, args, timeout=40):
    """Run ``script`` on ``target`` (BatchMode, a 10 s connect timeout)."""
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", target,
                        "python3", "-I", "-", json.dumps(args)],
                       input=script, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout


WRITE_STOP = r'''
import json, os, sys
p, rec = sys.argv[1], sys.argv[2]
try:
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
except FileExistsError:
    print("exists"); sys.exit(0)
os.write(fd, rec.encode()); os.fsync(fd); os.close(fd); print("written")
'''

WRITE_HEARTBEAT = r'''
import json, os, sys
p, rec = sys.argv[1], sys.argv[2]
t = p + ".tmp"
with open(t, "w") as f:
    f.write(rec); f.flush(); os.fsync(f.fileno())
os.replace(t, p)
print("ok")
'''


class Watchdog:
    def __init__(self, cfg, runner=None, clock=None):
        self.cfg = cfg
        self.limits = dict(LIMITS, **cfg.get("limits", {}))
        for k, how in _LOOSER.items():
            v, plan = self.limits[k], LIMITS[k]
            if (how == "lower" and v < plan) or (how == "higher" and v > plan):
                raise ValueError(f"watchdog limit {k}={v} is looser than the plan's {plan}")
        self.runner = runner or (lambda target, script, args: ssh_runner(target, script, args))
        self.clock = clock or common.Clock()
        self.optional = set(cfg.get("optional_checks", []))
        self.unreachable = {h: 0 for h in cfg["hosts"]}
        self.prev = {}
        self.swap_hist = []
        self.iow_hist = []
        self.test_fdb_down_since = None
        self.seq = 0
        os.makedirs(cfg["local_dir"], exist_ok=True)
        self.local_stop = common.StopFile(os.path.join(cfg["local_dir"], "stop.json"))
        self.log_path = os.path.join(cfg["local_dir"], "watchdog.jsonl")

    def log(self, **rec):
        rec["wall_ms"] = self.clock.wall_ms()
        with open(self.log_path, "a") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")

    def _read(self, host, detail):
        c = self.cfg
        args = {"data_mount": c.get("remote_data_mount") if host == c["load_host"] else "/",
                "system_fdb": host in c.get("system_fdb_hosts", [])}
        if detail:
            args.update(detail=True, test_dirs=c.get("remote_test_dirs", []),
                        test_fdb=c.get("remote_test_fdb_cluster"))
        try:
            rc, out = self.runner(c["hosts"][host], CHECK_SCRIPT, args)
        except subprocess.TimeoutExpired:
            return None
        if rc != 0:
            return None
        try:
            return json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return None

    def round(self):
        """One round: ``(breach_reason, detail)`` or ``(None, None)``; a
        round that passed every required check rewrites the heartbeat."""
        lim, c = self.limits, self.cfg
        now = self.clock.monotonic()
        required_ok = True
        for host in c["hosts"]:
            detail = host == c["load_host"]
            r = self._read(host, detail)
            if r is None:
                self.unreachable[host] += 1
                required_ok = False
                if self.unreachable[host] >= lim["max_unreachable"]:
                    return "host-unreachable", {"host": host, "rounds": self.unreachable[host]}
                continue
            self.unreachable[host] = 0

            def need(name):
                v = r.get(name)
                if v is None:
                    raise KeyError(name)
                return v

            try:
                if need("mem_available") < lim["min_mem_mib"] * (1 << 20):
                    return "memory", {"host": host, "bytes": r["mem_available"]}
                if need("free_root") < lim["min_free_gib"] * (1 << 30):
                    return "disk-free", {"host": host, "path": "/", "bytes": r["free_root"]}
                if need("free_data") < lim["min_free_gib"] * (1 << 30):
                    return "disk-free", {"host": host, "path": "data", "bytes": r["free_data"]}
                if need("load1") > lim["max_load_per_cpu"] * need("cpus"):
                    return "load", {"host": host, "load1": r["load1"], "cpus": r["cpus"]}
                if host in c.get("system_fdb_hosts", []) and not need("system_fdb_available"):
                    return "system-fdb", {"host": host}
                if detail:
                    b = self._detail(r, now)
                    if b:
                        return b
            except KeyError as e:
                return "monitor-failed", {"host": host, "missing": str(e), "errors": r.get("errors")}
        if required_ok:
            self.beat()
        return None, None

    def _detail(self, r, now):
        lim = self.limits

        def opt(name, key):
            v = r.get(key)
            if v is None and name not in self.optional:
                raise KeyError(key)
            return v

        swap = opt("swap", "swap_pages")
        if swap is not None:
            self.swap_hist = [(t, v) for t, v in self.swap_hist if t >= now - lim["swap_window_s"]]
            self.swap_hist.append((now, swap))
            if swap > self.swap_hist[0][1]:
                return "swap", {"pages": swap - self.swap_hist[0][1]}
        iow = r.get("cpu_iowait_total")
        if iow is None:
            raise KeyError("cpu_iowait_total")
        self.iow_hist = [(t, v) for t, v in self.iow_hist if t >= now - lim["iowait_window_s"]]
        self.iow_hist.append((now, iow))
        t0, (w0, a0) = self.iow_hist[0]
        if now - t0 >= lim["iowait_window_s"] - lim["interval"] and iow[1] > a0:
            pct = 100.0 * (iow[0] - w0) / (iow[1] - a0)
            if pct > lim["iowait_pct"]:
                return "iowait", {"pct": round(pct, 1)}
        alerts = opt("kernel_log", "kernel_alerts")
        if alerts:
            return "oom", {"lines": alerts[:5]}
        if lim["max_test_dir_bytes"] is not None:
            used = r.get("test_dir_bytes")
            if used is None:
                raise KeyError("test_dir_bytes")
            if used > lim["max_test_dir_bytes"]:
                return "test-dirs", {"bytes": used}
        if self.cfg.get("remote_test_fdb_cluster"):
            up = r.get("test_fdb_available")
            if up is None:
                raise KeyError("test_fdb_available")
            if up:
                self.test_fdb_down_since = None
            else:
                self.test_fdb_down_since = self.test_fdb_down_since or now
                if now - self.test_fdb_down_since >= lim["test_fdb_down_s"]:
                    return "test-fdb", {"down_s": now - self.test_fdb_down_since}
        return None

    def beat(self):
        self.seq = max(self.seq + 1, time.time_ns() // 1_000_000)
        rec = json.dumps({"run_id": self.cfg["run_id"], "seq": self.seq})
        try:
            self.runner(self.cfg["hosts"][self.cfg["load_host"]], WRITE_HEARTBEAT,
                        [self.cfg["remote_heartbeat"], rec])
        except subprocess.TimeoutExpired:
            pass

    def stop(self, reason, detail):
        rec = json.dumps({"reason": reason, "source": "watchdog", "wall_ms": self.clock.wall_ms(),
                          "detail": detail})
        self.local_stop.stop(reason, "watchdog", self.clock.wall_ms())
        written = None
        for _ in range(3):
            try:
                rc, out = self.runner(self.cfg["hosts"][self.cfg["load_host"]], WRITE_STOP,
                                      [os.path.join(self.cfg["remote_run_dir"], "stop.json"), rec])
                if rc == 0:
                    written = out.strip()
                    break
            except subprocess.TimeoutExpired:
                pass
        self.log(event="stop", reason=reason, detail=detail, remote=written)
        return written

    def run(self, max_rounds=None):
        n = 0
        while max_rounds is None or n < max_rounds:
            if self.local_stop.reason():
                self.log(event="stop file present")
                return 0
            reason, detail = self.round()
            if reason:
                self.stop(reason, detail)
                return 2
            n += 1
            self.clock.sleep(self.limits["interval"])
        return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--config", required=True)
    a = p.parse_args(argv)
    return Watchdog(common.read_json(a.config)).run()


if __name__ == "__main__":
    sys.exit(main())
