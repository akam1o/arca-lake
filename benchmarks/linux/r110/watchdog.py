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
directories' allocated bytes, the test FoundationDB and the guard's own
identity; every system FoundationDB. A reading that cannot be made is a
monitor failure unless the approved configuration declares it optional
(``optional_checks``). It never kills a process itself.

What fires is acted on by class (``common.REASONS``): a limit or an anomaly
writes the run's stop file on the load host (the first reason wins; the
guard and the load act on it) and the watching goes on, through the
drain; a resource or monitor rule also writes an alarm file of its own
(``remote_alarms``, read by the sentinel, which stops the services), and
the watchdog exits 2. Only a round in which every required reading was
made and no resource or monitor rule fired rewrites the load host's
heartbeat (a higher ``seq``): a watchdog that cannot see, or that has
stopped, lets the heartbeat go stale, and the guard and the sentinel stop
the load and the services within its timeout. A host unreachable
``max_unreachable`` rounds in a row (6, 60 s) is a monitor breach.
"""

import argparse
import json
import math
import os
import shlex
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
        # No alert is [], a log that cannot be read is an error (journalctl
        # answers 0 without the permission too, saying so).
        r = subprocess.run(["journalctl", "-k", "--since", "-60s", "-o", "cat", "--no-pager"],
                           capture_output=True, text=True, timeout=20)
        said = r.stdout + r.stderr
        if r.returncode != 0 or "No journal files" in said or "not seeing messages" in said \
                or "insufficient permissions" in said:
            raise OSError((r.stderr or r.stdout).strip()[:200] or "journalctl failed")
        return [l for l in r.stdout.splitlines()
                if "Out of memory" in l or "oom-kill" in l or "blocked for more than" in l][:20]
    tryit("kernel_alerts", klog)
    def du():
        # Every error surfaces (os.walk ignores them by default); a file or
        # directory vanishing during the walk (compaction) restarts it, at
        # most 8 walks; a missing test directory is an error.
        def fail(e):
            raise e
        for _ in range(8):
            total, vanished = 0, False
            try:
                for root in a.get("test_dirs", []):
                    if not os.path.isdir(root):
                        raise OSError("test directory missing: " + root)
                    for d, _, files in os.walk(root, onerror=fail):
                        for n in files:
                            total += os.lstat(os.path.join(d, n)).st_blocks * 512
            except FileNotFoundError:
                vanished = True
            if not vanished:
                return total
        raise OSError("files vanished during each of 8 walks")
    tryit("test_dir_bytes", du)
    if a.get("guard_record"):
        def guard():
            # The guard's own liveness: "absent" (not started yet), "ended"
            # (it wrote its result), "alive" (its identity holds) or "gone".
            if not os.path.exists(a["guard_record"]):
                return "absent"
            rec = json.load(open(a["guard_record"]))
            if a.get("guard_result") and os.path.exists(a["guard_result"]):
                return "ended"
            try:
                stat = open("/proc/%d/stat" % rec["pid"]).read()
            except FileNotFoundError:
                return "gone"
            return "alive" if stat.rsplit(")", 1)[1].split()[19] == rec["start"] else "gone"
        tryit("guard", guard)
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


def remote_command(argv):
    """The remote command line for ``python3 -I - ARGV...``: ssh joins its
    words with spaces and the remote shell splits them again, so every
    argument is quoted here, once."""
    return " ".join(["python3", "-I", "-"] + [shlex.quote(str(a)) for a in argv])


def ssh_runner(target, script, argv, timeout=40):
    """Run ``script`` on ``target`` with the arguments ``argv`` (a list of
    strings; BatchMode, a 10 s connect timeout)."""
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", target,
                        remote_command(argv)],
                       input=script, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout


# Written whole under a temporary name, then linked into place: exclusive
# (the first record wins) and never seen half written.
WRITE_STOP = r'''
import json, os, sys
p, rec = sys.argv[1], sys.argv[2]
tmp = "%s.tmp-%d" % (p, os.getpid())
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, rec.encode()); os.fsync(fd); os.close(fd)
try:
    os.link(tmp, p); print("written")
except FileExistsError:
    print("exists")
finally:
    os.unlink(tmp)
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
        for k, v in self.limits.items():
            if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                                  or v <= 0):
                raise ValueError(f"watchdog limit {k}={v!r}: a finite positive number")
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
        self.local_alarms = os.path.join(cfg["local_dir"], "alarms")
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
                        test_fdb=c.get("remote_test_fdb_cluster"),
                        guard_record=c.get("remote_guard_record"), guard_result=c.get("remote_guard_result"))
        try:
            rc, out = self.runner(c["hosts"][host], CHECK_SCRIPT, [json.dumps(args)])
        except subprocess.TimeoutExpired:
            return None
        if rc != 0:
            return None
        try:
            return json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return None

    def round(self):
        """One round over every host and rule: the list of ``(reason,
        detail)`` that fired. The heartbeat is rewritten only when every
        required reading was made and no resource or monitor rule fired (an
        anomaly already reported, such as the test's FoundationDB down, does
        not hold the heartbeat back: the resources are still watched)."""
        lim, c = self.limits, self.cfg
        now = self.clock.monotonic()
        required_ok = True
        out = []
        for host in c["hosts"]:
            detail = host == c["load_host"]
            r = self._read(host, detail)
            if r is None:
                self.unreachable[host] += 1
                required_ok = False
                if self.unreachable[host] >= lim["max_unreachable"]:
                    out.append(("host-unreachable", {"host": host, "rounds": self.unreachable[host]}))
                continue
            self.unreachable[host] = 0

            def need(name):
                v = r.get(name)
                if v is None:
                    raise KeyError(name)
                return v

            try:
                if need("mem_available") < lim["min_mem_mib"] * (1 << 20):
                    out.append(("memory", {"host": host, "bytes": r["mem_available"]}))
                if need("free_root") < lim["min_free_gib"] * (1 << 30):
                    out.append(("disk-free", {"host": host, "path": "/", "bytes": r["free_root"]}))
                if need("free_data") < lim["min_free_gib"] * (1 << 30):
                    out.append(("disk-free", {"host": host, "path": "data", "bytes": r["free_data"]}))
                if need("load1") > lim["max_load_per_cpu"] * need("cpus"):
                    out.append(("load", {"host": host, "load1": r["load1"], "cpus": r["cpus"]}))
                if host in c.get("system_fdb_hosts", []) and not need("system_fdb_available"):
                    out.append(("system-fdb", {"host": host}))
                if detail:
                    out += self._detail(r, now)
            except KeyError as e:
                out.append(("monitor-failed", {"host": host, "missing": str(e), "errors": r.get("errors")}))
        if required_ok and not any(common.reason_class(b[0]) in ("resource", "monitor") for b in out):
            self.beat()
        return out

    def _detail(self, r, now):
        lim = self.limits
        out = []

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
                out.append(("swap", {"pages": swap - self.swap_hist[0][1]}))
        iow = r.get("cpu_iowait_total")
        if iow is None:
            raise KeyError("cpu_iowait_total")
        self.iow_hist = [(t, v) for t, v in self.iow_hist if t >= now - lim["iowait_window_s"]]
        self.iow_hist.append((now, iow))
        t0, (w0, a0) = self.iow_hist[0]
        if now - t0 >= lim["iowait_window_s"] - lim["interval"] and iow[1] > a0:
            pct = 100.0 * (iow[0] - w0) / (iow[1] - a0)
            if pct > lim["iowait_pct"]:
                out.append(("iowait", {"pct": round(pct, 1)}))
        if self.cfg.get("remote_guard_record"):
            g = r.get("guard")
            if g is None:
                raise KeyError("guard")
            if g == "gone":
                out.append(("guard-lost", {}))
        alerts = opt("kernel_log", "kernel_alerts")
        if alerts:
            out.append(("oom", {"lines": alerts[:5]}))
        if lim["max_test_dir_bytes"] is not None:
            used = r.get("test_dir_bytes")
            if used is None:
                raise KeyError("test_dir_bytes")
            if used > lim["max_test_dir_bytes"]:
                out.append(("test-dirs", {"bytes": used}))
        if self.cfg.get("remote_test_fdb_cluster"):
            up = r.get("test_fdb_available")
            if up is None:
                raise KeyError("test_fdb_available")
            if up:
                self.test_fdb_down_since = None
            else:
                self.test_fdb_down_since = self.test_fdb_down_since or now
                if now - self.test_fdb_down_since >= lim["test_fdb_down_s"]:
                    out.append(("test-fdb", {"down_s": now - self.test_fdb_down_since}))
        return out

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

    def alarm(self, reason, detail):
        """A resource or monitor rule fired: an alarm file of its own, on the
        load host (the sentinel stops the services on it) and here (the
        driver's drain reads it). Exclusively created, one per rule that
        fired: an earlier stop never hides a later danger."""
        rec = {"reason": reason, "source": "watchdog", "wall_ms": self.clock.wall_ms(), "detail": detail}
        name = f"{self.clock.wall_ms():013d}-{reason}.json"
        os.makedirs(self.local_alarms, exist_ok=True)
        common.link_new(os.path.join(self.local_alarms, name), json.dumps(rec))
        written = None
        if self.cfg.get("remote_alarms"):
            for _ in range(3):
                try:
                    rc, out = self.runner(self.cfg["hosts"][self.cfg["load_host"]], WRITE_STOP,
                                          [os.path.join(self.cfg["remote_alarms"], name), json.dumps(rec)])
                    if rc == 0:
                        written = out.strip()
                        break
                except subprocess.TimeoutExpired:
                    pass
        self.log(event="alarm", reason=reason, detail=detail, remote=written)

    def run(self, max_rounds=None):
        """Round after round until a resource or monitor rule fires (an
        alarm and the stop: exit 2) or ``max_rounds``. A limit or anomaly
        stop (the load stops, the services stay for the drain) does not end
        the watching: it is reported once, and the rounds go on."""
        n = 0
        reported = set()
        while max_rounds is None or n < max_rounds:
            breaches = self.round()
            danger = [b for b in breaches if common.reason_class(b[0]) in ("resource", "monitor")]
            if danger:
                reason, detail = danger[0]
                self.alarm(reason, detail)
                self.stop(reason, detail)
                return 2
            for reason, detail in breaches:
                if reason not in reported:
                    reported.add(reason)
                    self.stop(reason, detail)
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
