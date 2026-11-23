"""The hosts side of the R-110 driver: ``SshCluster`` (plan §4.1 option 1:
every test process on the load host). Used only by ``driver.py`` after an
approval; nothing here runs on import or from ``driver.py check``.

Every command goes through ``self.ssh(host, command)`` (a ``runner``,
injectable: tests record commands and never connect). Rules the commands
follow, checked by the tests:

- directories: setup creates ``run_dir`` and ``fdb_data`` only if they do
  not exist, and writes a marker with the run id in each; teardown removes
  a directory only if its marker names this run (``rm -rf --`` of the
  configured, validated path, quoted), and keeps everything if any test
  process is still running or a host cannot be checked;
- processes: every test process is started in its own session by
  ``start.py`` (in the run directory), which records its identity (pid,
  start token, cwd) under ``services/``; stops and signals go through the
  identity (``proc.signal_if_same``), never a name; the load runs under the
  guard;
- the system FoundationDB is only read (``status minimal``); the retained
  cluster's directories are never named.
"""

import json
import os
import re
import shlex
import subprocess
import tempfile
import time

from . import budget, common, judge, ledger as ledger_mod

Q = shlex.quote
TOOLS_FILES = ("__init__.py", "common.py", "budget.py", "ledger.py", "s3.py", "load.py", "proc.py",
               "guard.py", "sampler.py")
MARK = ".r110-run"

START = r'''
import json, os, subprocess, sys, time
sys.path.insert(0, sys.argv[1])
from r110 import proc
name, cwd, cmd = sys.argv[2], sys.argv[3], sys.argv[4:]
svc = os.path.join(cwd, "services")
if not os.path.isdir(svc):
    print("no services directory", file=sys.stderr); sys.exit(1)
rec_path = os.path.join(svc, name + ".json")
if os.path.exists(rec_path):
    print("a record exists already: not started twice", file=sys.stderr); sys.exit(1)
# Listed before it starts: a process the run started is never unaccounted.
with open(os.path.join(svc, "started.list"), "a") as f:
    f.write(name + "\n"); f.flush(); os.fsync(f.fileno())
log = open(os.path.join(cwd, "logs", name + ".log"), "ab")
p = subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
ident = None
for _ in range(100):
    try:
        ident = proc.identity(p.pid)
    except proc.Unknown:
        ident = None
    if ident:
        break
    time.sleep(0.05)
if not ident:
    # A process whose identity cannot be recorded could never be stopped
    # by it: it is not left running (the caller's own child, reaped here).
    p.kill(); p.wait()
    print("identity unavailable", file=sys.stderr); sys.exit(1)
ident["argv"] = cmd
json.dump(ident, open(rec_path + ".tmp", "w")); os.replace(rec_path + ".tmp", rec_path)
print(json.dumps(ident))
'''

# Exit 0 and one line "checked N" only when every service the run started
# (services/started.list) has a readable record and none of them, nor any
# member of its group, is alive; anything else (a missing list or record,
# a process that cannot be checked, a reused pid with a live group) is
# printed and exits 1: nothing is removed on a check that did not hold.
ALIVE = r'''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from r110 import proc
d = sys.argv[2]
if not os.path.isdir(d):
    print("unverified: no services directory"); sys.exit(1)
try:
    started = [n for n in open(os.path.join(d, "started.list")).read().split() if n]
except OSError as e:
    print("unverified: no started.list: %s" % e); sys.exit(1)
bad = 0
for name in sorted(set(started)):
    try:
        rec = json.load(open(os.path.join(d, name + ".json")))
    except (OSError, ValueError) as e:
        print("unverified %s: no readable record: %s" % (name, e)); bad = 1; continue
    c = proc.check(rec)
    g = proc.group_state(rec["pgid"]) if proc.valid(rec) else "unknown"
    if c == "same" or g == "alive":
        print("alive %s (%s, group %s)" % (name, c, g)); bad = 1
    elif c in ("unknown", "invalid") or g == "unknown":
        print("unverified %s (%s, group %s)" % (name, c, g)); bad = 1
if bad:
    sys.exit(1)
print("checked %d" % len(set(started)))
'''

# The run's services directory: whether it exists, started.list, the
# record files (one JSON line); exit 1 if it cannot be read.
LISTING = r'''
import json, os, sys
d = sys.argv[1]
if not os.path.exists(d):
    print(json.dumps({"exists": False})); sys.exit(0)
names = sorted(os.listdir(d))
try:
    started = open(os.path.join(d, "started.list")).read().split()
except FileNotFoundError:
    started = None
print(json.dumps({"exists": True, "started": started,
                  "records": [n for n in names if n.endswith(".json")]}))
'''

# Stop one recorded service and its group (proc.stop_identified): one JSON
# line with its verdict (stopped, gone, not-stopped, unverified).
STOP = r'''
import json, sys
sys.path.insert(0, sys.argv[1])
from r110 import common, proc
path, wait = sys.argv[2], float(sys.argv[3])
import os
name = os.path.basename(path)[:-5]
try:
    started = open(os.path.join(os.path.dirname(path), "started.list")).read().split()
except OSError:
    started = None
if not os.path.exists(path) and started is not None and name not in started:
    print(json.dumps({"verdict": "not-started"})); sys.exit(0)
try:
    rec = json.load(open(path))
except (OSError, ValueError) as e:
    print(json.dumps({"verdict": "unverified", "reason": "record unreadable: %s" % e})); sys.exit(0)
print(json.dumps(proc.stop_identified(rec, common.Clock(), term_wait=wait, kill_wait=wait / 2)))
'''


# The host's boot: its boot ID (Linux: changes at every boot), uptime and
# wall time, one JSON line; written to ARGV[2] too when given (setup's
# record, ``host-boot.json`` in the run directory).
BOOT = r'''# r110:boot
import json, os, sys, time
out = {"wall_ms": time.time_ns() // 1000000}
try:
    out["boot_id"] = open(sys.argv[1]).read().strip() or None
except OSError:
    out["boot_id"] = None
try:
    out["uptime_s"] = float(open("/proc/uptime").read().split()[0])
except (OSError, ValueError, IndexError):
    out["uptime_s"] = None
if len(sys.argv) > 2:
    with open(sys.argv[2] + ".tmp", "w") as f:
        json.dump(out, f); f.flush(); os.fsync(f.fileno())
    os.replace(sys.argv[2] + ".tmp", sys.argv[2])
print(json.dumps(out))
'''

REACH_ATTEMPTS, REACH_WAIT_S = 3, 20.0


def ssh_runner(target, command, stdin=None, timeout=120):
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
                        target, command], input=stdin, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


HOST_CHECK = r'''
import json, os
out = {"cpus": os.cpu_count()}
for l in open("/proc/meminfo"):
    if l.startswith("MemAvailable:"):
        out["mem_available"] = int(l.split()[1]) * 1024
out["load1"] = float(open("/proc/loadavg").read().split()[0])
s = os.statvfs("/")
out["free_root"] = s.f_bavail * s.f_frsize
out["swaps"] = open("/proc/swaps").read().splitlines()[1:]
print(json.dumps(out))
'''


# A guard's worst case after a SIGTERM: its grace (15 s), its load group's
# TERM and KILL waits (20 + 10 s), the reap (5 s), the services' stops.
GUARD_STOP_S = 180

# fdbserver's --cache-memory when none is given.
FDB_DEFAULT_CACHE = 2 << 30


def parse_memory(text):
    """Bytes of a FoundationDB memory size (``NMiB`` / ``NGiB``)."""
    m = re.fullmatch(r"([1-9][0-9]{0,6})(MiB|GiB)", text) if isinstance(text, str) else None
    if not m:
        raise ValueError(f"fdb_memory {text!r}: a whole number of MiB or GiB")
    return int(m.group(1)) << (20 if m.group(2) == "MiB" else 30)


class RemoteError(Exception):
    pass


class SshCluster:
    def __init__(self, cfg, runner=None, clock=None, tools_dir=None):
        self.cfg = cfg
        self.runner = runner or ssh_runner
        self.host = cfg["load_host"]
        r = cfg["remote"]
        self.run_dir, self.fdb_data, self.mount = r["run_dir"], r["fdb_data"], r["data_mount"]
        self.ports = r["ports"]
        self.bin = r["binary_path"]
        self.tools = f"{self.run_dir}/tools"
        self.load_dir = f"{self.run_dir}/load"
        self.tools_src = tools_dir or os.path.dirname(os.path.abspath(__file__))
        self.step_n = 0
        self.clock = clock
        self.boot_id_path = "/proc/sys/kernel/random/boot_id"
        self.sleep = time.sleep

    # -- plumbing --------------------------------------------------------------

    def ssh(self, command, stdin=None, check=True, host=None, timeout=120):
        rc, out, err = self.runner(self.cfg["hosts"][host or self.host], command, stdin, timeout)
        if check and rc != 0:
            raise RemoteError(f"rc {rc}: {err.strip()[-300:]}")
        return out

    def py(self, script, *args, check=True, timeout=120):
        return self.ssh("python3 -I - " + " ".join(Q(str(a)) for a in args), stdin=script, check=check,
                        timeout=timeout)

    def tool(self, module, *args):
        code = f"import sys; sys.path.insert(0, {self.tools!r}); from r110.{module} import main; sys.exit(main())"
        return "python3 -I -c " + Q(code) + " " + " ".join(Q(str(a)) for a in args)

    def nodes(self):
        return [f"node{i}" for i in range(self.cfg["settings"]["nodes"])]

    def gateways(self):
        return ["gateway0"]

    def targets(self):
        return [t["name"] for t in self.sampler_config()["targets"]]

    def required_checks(self):
        hosts = list(self.cfg["hosts"])
        return (["space", "dirs_absent", "ports_free", "binary", "tools", "proc_stat", "kernel_log",
                 "fdb_memory", "fdb_memory_fits", "retained"] + [f"host {h}" for h in hosts] + [f"clock {h}" for h in hosts]
                + [f"system_fdb {h}" for h in self.cfg.get("system_fdb_hosts", [])])

    def clock_offset_ms(self, host=None):
        """The host's wall clock minus this machine's, in ms (the midpoint
        of the round trip; time_ns on both sides, no formatted dates)."""
        t0 = time.time_ns()
        out = self.ssh("python3 -I -c 'import time; print(time.time_ns())'", host=host, timeout=30)
        t1 = time.time_ns()
        return (int(out.strip()) - (t0 + t1) // 2) // 1_000_000
    # -- preflight (read-only) ----------------------------------------------------

    def preflight(self):
        c, s = {}, self.cfg["settings"]
        out = self.ssh(f"df -B1 --output=avail {Q(self.mount)} / | tail -2; "
                       f"ls -d {Q(self.run_dir)} {Q(self.fdb_data)} 2>/dev/null; true", check=False)
        lines = out.split()
        try:
            data, root = int(lines[0]), int(lines[1])
        except (IndexError, ValueError):
            return {"space": {"ok": False, "detail": out[-200:]}}
        need = self.cfg["stop_rules"].get("preflight_free_gib", 12.2) - (1.0 if s["allotment_bytes"] < (1 << 30) else 0)
        c["space"] = {"ok": data >= need * (1 << 30) and root >= 3 * (1 << 30), "data": data, "root": root,
                      "need_gib": need}
        c["dirs_absent"] = {"ok": self.run_dir not in out and self.fdb_data not in out}
        ports = "|".join(str(p) for p in self.ports.values())
        busy = self.ssh(f"ss -ltnH | awk '{{print $4}}' | grep -E ':({ports})$' || true", check=False).strip()
        c["ports_free"] = {"ok": busy == "", "busy": busy}
        sha = self.ssh(f"sha256sum {Q(self.bin)} | cut -c1-64", check=False).strip()
        c["binary"] = {"ok": sha == self.cfg["binary"]["sha256"], "sha256": sha}
        for h in self.cfg.get("system_fdb_hosts", []):
            o = self.ssh("timeout 20 fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal'",
                         check=False, host=h)
            c[f"system_fdb {h}"] = {"ok": "is available" in o}
        tools = self.ssh("command -v du iostat journalctl fdbserver fdbcli python3 || true", check=False)
        c["tools"] = {"ok": all(t in tools for t in ("du", "fdbserver", "fdbcli", "python3")), "found": tools.split()}
        c["binary"]["commit_and_features"] = "not readable from the binary: the approved build record's"
        c["proc_stat"] = {"ok": self.ssh("test -r /proc/stat && test -r /proc/vmstat && echo yes || true",
                                         check=False).strip() == "yes"}
        # A kernel line actually read: journalctl exits 0 without permission too.
        klog = self.ssh("journalctl -k -n 1 -o cat --no-pager 2>/dev/null | grep -v -e '^-- ' -e 'No journal files' "
                        "| grep -q . && echo yes || true", check=False).strip()
        c["kernel_log"] = {"ok": klog == "yes" or "kernel_log" in self.cfg.get("optional_checks", []),
                           "readable": klog == "yes"}
        mem = self.ssh("fdbserver --help 2>&1 | grep -c -- '--memory' || true", check=False).strip()
        c["fdb_memory"] = {"ok": mem not in ("", "0")}
        for h in self.cfg["hosts"]:
            raw = self.ssh("python3 -I -", stdin=HOST_CHECK, check=False, host=h, timeout=60)
            try:
                r = json.loads(raw.strip().splitlines()[-1])
                ok = r["mem_available"] >= (1 << 30) and r["load1"] <= 4 * r["cpus"] and r["free_root"] >= 3 << 30
            except (ValueError, IndexError, KeyError, TypeError):
                r, ok = {"raw": raw[-200:]}, False
            c[f"host {h}"] = dict(r, ok=ok)
            try:
                c[f"clock {h}"] = {"ok": True, "offset_ms": self.clock_offset_ms(h)}
            except (RemoteError, ValueError) as e:
                c[f"clock {h}"] = {"ok": False, "error": str(e)[:200]}
        # The test's FoundationDB under its approved limit, with the guard's
        # floor still free beside it; undetermined at a preflight, the most
        # it may be is reported.
        if self.cfg.get("fdb_memory") is None:
            avail = c.get(f"host {self.host}", {}).get("mem_available")
            c["fdb_memory_fits"] = {"ok": True, "fdb_memory": None,
                                    "at_most_bytes": None if avail is None else avail - (1 << 30)}
            c["retained"] = self._retained()
            return c
        try:
            fdb = parse_memory(self.cfg["fdb_memory"])
            avail = c[f"host {self.host}"]["mem_available"]
            c["fdb_memory_fits"] = {"ok": avail >= fdb + (1 << 30), "fdb_memory": self.cfg["fdb_memory"],
                                    "mem_available": avail}
        except (KeyError, TypeError, ValueError) as e:
            c["fdb_memory_fits"] = {"ok": False, "error": str(e)[:200]}
        c["retained"] = self._retained()
        return c

    def _retained(self):
        """The retained cluster's directories unchanged since the last
        record (plan §10): per directory, the sha256 of the sorted listing of
        every entry below it (path, size, modification time; metadata only,
        read with ``find``, nothing opened or changed), compared to
        ``retained_record`` of the configuration."""
        rec = self.cfg.get("retained_record")
        if not rec:
            return {"ok": False, "detail": "no retained_record in the configuration to compare against"}
        host = self.cfg.get("retained_host", self.host)
        now = {}
        for path in sorted(rec):
            out = self.ssh(f"[ -d {Q(path)} ] && find {Q(path)} -printf '%P %s %T@\\n' | LC_ALL=C sort | sha256sum "
                           f"| cut -c1-64 || echo missing", check=False, host=host)
            now[path] = out.strip()
        return {"ok": now == rec, "now": now}

    # -- setup, smoke ----------------------------------------------------------

    def setup(self):
        rid = self.cfg["run_id"]
        self.ssh(f"[ ! -e {Q(self.run_dir)} ] && [ ! -e {Q(self.fdb_data)} ] || "
                 f"{{ echo 'run directories exist' >&2; exit 1; }}; "
                 f"mkdir -p {Q(self.run_dir)}/logs {Q(self.run_dir)}/services {Q(self.run_dir)}/alarms "
                 f"{Q(self.tools)}/r110 {Q(self.fdb_data)} && "
                 f"echo {Q(rid)} > {Q(self.run_dir)}/{MARK} && echo {Q(rid)} > {Q(self.fdb_data)}/{MARK} && "
                 f": > {Q(self.run_dir)}/services/started.list")
        # From here on (this run's directories made, no process started yet)
        # every final stop must account for the services it started
        # (started.list); a leftover directory of the same id is never taken.
        self.setup_started = True
        # The boot the run starts on: a later "gone" is told apart from a
        # stop when the host has booted since.
        self.py(BOOT, self.boot_id_path, f"{self.run_dir}/host-boot.json")
        for f in TOOLS_FILES:
            with open(os.path.join(self.tools_src, f)) as fh:
                self.ssh(f"cat > {Q(self.tools)}/r110/{f}", stdin=fh.read())
        self.start_watchdog()
        # From here to teardown the sentinel watches on the host itself: the
        # smoke stage, the main run and the drain alike (plan §6.2).
        self.start_sentinel()
        self.cluster_services()

    def start_sentinel(self):
        g = self.cfg["stop_rules"]["guard"]
        cmd = self.tool("guard", "sentinel", "--run-dir", self.run_dir, "--heartbeat", f"{self.run_dir}/heartbeat.json",
                        "--heartbeat-id", self.cfg["run_id"], "--data-dir", self.mount,
                        "--services", f"{self.run_dir}/services", "--alarms", f"{self.run_dir}/alarms",
                        "--stop-file", f"{self.load_dir}/stop.json",
                        "--min-free-gib", g["min_free_gib"], "--min-mem-mib", g["min_mem_mib"],
                        "--heartbeat-timeout", g["heartbeat_timeout_s"])
        self.ssh(f"mkdir -p {Q(self.load_dir)}")
        self.start("sentinel", ["sh", "-c", "exec " + cmd])

    def stop_sentinel(self):
        return self._stop("sentinel.json", 120)

    def stop_sampler(self):
        return self._stop("sampler.json", 30)

    def start(self, name, command):
        return self.py(START, self.tools, name, self.run_dir, *command)

    def cluster_services(self):
        """FoundationDB, four nodes with their allotments, controller,
        gateway: the plan's settings (§5), every other flag the default."""
        s, p, rid = self.cfg["settings"], self.ports, self.cfg["run_id"]
        cf = f"{self.run_dir}/fdb.cluster"
        addr = "127.0.0.1"
        self.ssh(f"echo {Q('r110:' + rid.replace('-', '') + '@' + addr + ':' + str(p['fdb']))} > {Q(cf)}")
        # The approved memory limit of the test's FoundationDB (plan §4.1;
        # validated, never left undetermined for a run), on its command line.
        self.start("fdb", ["fdbserver", "-p", f"{addr}:{p['fdb']}", "-C", cf, "-d", f"{self.fdb_data}/d",
                           "-L", f"{self.fdb_data}/l", "--memory", self.cfg["fdb_memory"],
                           *(["--cache-memory", self.cfg["fdb_cache_memory"]] if self.cfg.get("fdb_cache_memory")
                             else [])])
        self.ssh(f"for i in $(seq 1 60); do fdbcli -C {Q(cf)} --exec 'configure new single ssd' >/dev/null 2>&1; "
                 f"fdbcli -C {Q(cf)} --exec 'status minimal' | grep -q 'is available' && exit 0; sleep 1; done; exit 1")
        env = ["env", "ARCA_INSECURE_PLAINTEXT=1", "ARCA_FDB_INSECURE_PLAINTEXT=1"]
        for i in range(s["nodes"]):
            self.start(f"node{i}", env + [self.bin, "storage-node", "--listen", f"{addr}:{p['node'] + i}",
                                          "--dir", f"{self.run_dir}/d{i}", "--metrics-listen",
                                          f"{addr}:{p['node_metrics'] + i}",
                                          "--capacity-bytes", str(s["allotment_bytes"]),
                                          "--segment-max-bytes", str(s["segment_max_bytes"]),
                                          "--reclaim-retention-secs", str(s["reclaim_retention_secs"]),
                                          "--reclaim-skew-secs", str(s["reclaim_skew_secs"])])
        admin = " ".join(Q(x) for x in env + [self.bin, "admin", "--cluster-file", cf])
        self.ssh(f"{admin} init")
        for i in range(s["nodes"]):
            self.ssh(f"{admin} register-node --address {addr}:{p['node'] + i} --label node=n{i} --label device_class=hdd")
        self.ssh(f"{admin} create-groups --tier default --device-class hdd --failure-domain node --count 2 "
                 f"--k 2 --m 1 --chunk-bytes {s['chunk_bytes']} --small-threshold 0")
        self.start("controller", env + [self.bin, "controller", "--cluster-file", cf, "--id", "ctl",
                                        "--http", f"{addr}:{p['controller_metrics']}", "--failure-domain", "node",
                                        "--admission-max-chunk-bytes", str(s["admission_max_chunk_bytes"])])
        self.start("gateway0", env + [self.bin, "gateway", "--backend", "cluster", "--cluster-file", cf,
                                      "--allow-anonymous", "--listen", f"{addr}:{p['gateway']}",
                                      "--metrics-listen", f"{addr}:{p['gateway_metrics']}", "--k", "2", "--m", "1",
                                      "--chunk-bytes", str(s["chunk_bytes"]), "--small-threshold", "0",
                                      "--failure-domain", "node"])
        self.start_sampler()

    def sampler_config(self):
        p, n = self.ports, self.cfg["settings"]["nodes"]
        node_fams = ["arca_node_capacity_bytes", "arca_node_device_usage_bytes", "arca_node_reclaim_pending_records",
                     "arca_node_reclaim_retained_bytes", "arca_node_reclaim_damaged", "arca_node_compaction_backlog_bytes",
                     "arca_node_compactions_total", "arca_node_compaction_reclaimed_bytes_total",
                     "arca_node_compaction_backlog_segments", "arca_node_compaction_blocked_segments",
                     "arca_node_compaction_errors_total", "arca_node_compaction_skipped_no_space_total",
                     "arca_node_dead_records", "arca_node_index_entries", "arca_node_device_free_bytes",
                     "arca_node_reclaim_retention_seconds", "arca_node_admission_refused_total",
                     "arca_node_live_bytes", "arca_node_segment_bytes"]
        ctl_fams = ["arca_controller_gc_backlog", "arca_controller_gc_backlog_bytes", "arca_controller_gc_collected_total",
                    "arca_controller_reclaimed_total", "arca_controller_reclaimed_bytes_total",
                    "arca_controller_reclaim_refused_total", "arca_controller_reclaim_errors_total",
                    "arca_controller_backpressure_state", "arca_controller_capacity_bytes",
                    "arca_controller_gc_skipped_referenced_total", "arca_controller_pass_running_since_seconds",
                    "arca_controller_pass_last_seconds", "arca_controller_gc_backlog_physical_bytes",
                    "arca_controller_gc_backlog_oldest_age_seconds", "arca_controller_shards_deleted_total",
                    "arca_controller_gc_reclaimed_bytes_total", "arca_controller_admission_excluded_groups"]
        gw_fams = ["arca_gateway_admission_state", "arca_gateway_admission_rejected_total",
                   "arca_gateway_admission_throttled_total",
                   "arca_gateway_node_capacity_refusals_total", "arca_gateway_placement_capacity_failures_total"]
        t = [{"name": f"node{i}", "kind": "metrics", "url": f"http://127.0.0.1:{p['node_metrics'] + i}/metrics",
              "families": node_fams} for i in range(n)]
        t += [{"name": f"node{i}-dir", "kind": "dir", "path": f"{self.run_dir}/d{i}"} for i in range(n)]
        t += [{"name": "controller", "kind": "metrics", "url": f"http://127.0.0.1:{p['controller_metrics']}/metrics",
               "families": ctl_fams},
              {"name": "gateway0", "kind": "metrics", "url": f"http://127.0.0.1:{p['gateway_metrics']}/metrics",
               "families": gw_fams},
              {"name": "controller-log", "kind": "log", "path": f"{self.run_dir}/logs/controller.log"}]
        return {"run_id": self.cfg["run_id"], "interval": judge.VALUES["interval_s"], "targets": t}

    def start_sampler(self):
        self.ssh(f"cat > {Q(self.run_dir)}/sampler.json", stdin=json.dumps(self.sampler_config()))
        cmd = self.tool("sampler", "--config", f"{self.run_dir}/sampler.json", "--out", f"{self.run_dir}/samples.jsonl")
        self.start("sampler", ["sh", "-c", "exec " + cmd])

    def start_load(self, sub=None, limits=None, heartbeat=True, remaining_s=None):
        """The load (under the guard) in ``load_dir`` or a sub-run (both read
        the main run's heartbeat, unless ``heartbeat`` is False), with its
        share of the approved run-wide limits (``budget.allocate``) and a
        deadline no later than the run's (``remaining_s``: what is left of
        the run's time; a stage switch never resets it)."""
        d = f"{self.run_dir}/{sub}" if sub else self.load_dir
        rid = self.cfg["run_id"] + (f"-{sub}" if sub else "")
        g = self.cfg["stop_rules"]["guard"]
        self.ssh(f"mkdir -p {Q(d)}/control && echo {Q(json.dumps({'run_id': rid}))} > {Q(d)}/run.json")
        hb = f"{self.run_dir}/heartbeat.json" if heartbeat else f"{d}/no-heartbeat.json"
        lim = dict(budget.allocate(self.cfg["limits"])[sub or "main"], **(limits or {}))
        if remaining_s is not None:
            lim["run_seconds"] = max(1, min(lim["run_seconds"], int(remaining_s)))
        # Each sub-run has a bucket of its own (the main run's is created by
        # stage 3 and must not exist before). Without a heartbeat the load
        # does not read one at all, so that only the guard can stop it: the
        # smoke stage exercises the guard's layer, not the load's.
        load = self.tool("load", "run", "--run-dir", d, "--endpoint", f"http://127.0.0.1:{self.ports['gateway']}",
                         "--object-size", self.cfg["settings"]["object_size"],
                         "--bucket", f"r110-{sub}" if sub else "r110",
                         *(["--heartbeat", hb, "--heartbeat-id", self.cfg["run_id"]] if heartbeat else []),
                         "--limits", json.dumps(lim))
        guard = self.tool("guard", "run", "--run-dir", d, "--heartbeat", hb, "--data-dir", self.mount,
                          "--min-free-gib", g["min_free_gib"], "--min-mem-mib", g["min_mem_mib"],
                          "--heartbeat-timeout", g["heartbeat_timeout_s"], "--heartbeat-id", self.cfg["run_id"],
                          "--max-seconds", lim["run_seconds"] + 600,
                          "--record-dir", f"{self.run_dir}/services", "--record-name", "load-" + sub if sub else "load",
                          # A smoke sub-run's exercised stop must not reach the
                          # main run's services: only the main guard gets them.
                          *([] if sub else ["--services", f"{self.run_dir}/services"]),
                          "--", "sh", "-c", "exec " + load)
        self.start("guard" + (f"-{sub}" if sub else ""), ["sh", "-c", "exec " + guard])
        self.step_n = 0
        return d

    def smoke(self, remaining_s=None):
        """Stage 2 in sub-runs of their own: a withheld heartbeat stops a
        load; an attempt limit ends one; 100 PUT, GET, DELETE and a verify."""
        self.setup()
        res = {"errors": 0}
        t0 = time.monotonic()

        def left():
            return None if remaining_s is None else max(1, remaining_s - (time.monotonic() - t0))

        d = self.start_load("smoke-hb", heartbeat=False, remaining_s=left())
        self._send(d, 1, {"kind": "create-bucket"})
        self._send(d, 2, {"kind": "put", "keys": [f"s{i}" for i in range(10_000)], "rate": 2})
        st = self._wait_exit(d, 180)
        g = self._json(f"{d}/guard.json")
        res["guard"] = g
        # The guard (not the load, which reads no heartbeat here) stopped it,
        # and nothing of the load's group was left.
        res["stop_layers_exercised"] = bool(st and st.get("stopped") == "heartbeat-lost" and g
                                            and g.get("reason") == "heartbeat-lost" and g.get("source") == "guard"
                                            and g.get("group_empty") is True)
        d = self.start_load("smoke-limit", remaining_s=left())
        self._send(d, 1, {"kind": "create-bucket"})
        self._send(d, 2, {"kind": "put", "keys": [f"l{i}" for i in range(100)]})
        st = self._wait_exit(d, 120)
        res["limits_exercised"] = bool(st and st.get("stopped") == "attempts-limit")
        d = self.start_load("smoke-ops", remaining_s=left())
        keys = [f"o{i}" for i in range(100)]
        for n, step in enumerate([{"kind": "create-bucket"}, {"kind": "put", "keys": keys},
                                  {"kind": "get", "keys": keys}, {"kind": "delete", "keys": keys},
                                  {"kind": "verify"}, {"kind": "finish"}], 1):
            self._send(d, n, step)
        st = self._wait_exit(d, 600)
        steps = st.get("steps", []) if st else []
        verify = steps[4]["result"] if len(steps) > 4 else None
        gets = steps[2]["result"].get("counts", {}).get("op-GET-found", 0) if len(steps) > 2 else 0
        if not verify or verify.get("violations") or not verify.get("complete") or gets != len(keys):
            res["errors"] += 1
        res["ops"] = {"verify": verify, "gets_found": gets}
        return res

    def _send(self, d, n, step):
        self.ssh(f"cat > {Q(d)}/control/{n:06d}.json.tmp && mv {Q(d)}/control/{n:06d}.json.tmp "
                 f"{Q(d)}/control/{n:06d}.json", stdin=json.dumps(step))

    def _json(self, path):
        out = self.ssh(f"cat {Q(path)} 2>/dev/null || true", check=False)
        try:
            return json.loads(out)
        except ValueError:
            return None

    def wait_step(self, n, deadline):
        """The result of step ``n`` of the main load once it has ended."""
        while time.monotonic() < deadline:
            st = self._status(self.load_dir)
            if st:
                for s in st.get("steps", []):
                    if s["n"] == n:
                        return s["result"]
                if st.get("exit"):
                    return None
            time.sleep(2)
        return None

    def _status(self, d):
        out = self.ssh(f"cat {Q(d)}/status.json 2>/dev/null || true", check=False)
        try:
            return json.loads(out)
        except ValueError:
            return None

    def _wait_exit(self, d, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            st = self._status(d)
            if st and st.get("exit"):
                return st
            time.sleep(2)
        return None

    # -- the main run's load ----------------------------------------------------

    def send_step(self, step):
        self.step_n += 1
        self._send(self.load_dir, self.step_n, step)
        return self.step_n

    def step_done(self, n):
        self.ssh(f"touch {Q(self.load_dir)}/control/{n:06d}.done")

    def run_step(self, step, deadline, check=None):
        """Send ``step`` and wait for its result (None: the load ended or
        the deadline passed). ``check()``, every 15 s while it runs, may
        return a reason to stop (the driver's protection counters): the
        step is then abandoned (None) and the caller stops the run."""
        n = self.send_step(dict(step, max_seconds=max(1, int(deadline - time.monotonic()))))
        next_check = time.monotonic() + 15
        while time.monotonic() < deadline:
            if check is not None and time.monotonic() >= next_check:
                next_check = time.monotonic() + 15
                if check():
                    return None
            st = self._status(self.load_dir)
            if st:
                for s in st.get("steps", []):
                    if s["n"] == n:
                        return s["result"]
                if st.get("exit"):
                    return None
            time.sleep(2)
        return None

    def start_watchdog(self):
        """The watchdog, on this machine (the driver's child)."""
        import sys
        cfg = {"run_id": self._load_run_id(),
               "hosts": self.cfg["hosts"], "load_host": self.host,
               "system_fdb_hosts": self.cfg.get("system_fdb_hosts", []),
               "remote_run_dir": self.load_dir, "remote_data_mount": self.mount,
               "remote_alarms": f"{self.run_dir}/alarms",
               "remote_heartbeat": f"{self.run_dir}/heartbeat.json",
               "remote_test_dirs": [self.run_dir, self.fdb_data],
               "remote_test_fdb_cluster": f"{self.run_dir}/fdb.cluster",
               "remote_guard_record": f"{self.run_dir}/services/guard.json",
               "remote_guard_result": f"{self.load_dir}/guard.json",
               "remote_sentinel_record": f"{self.run_dir}/services/sentinel.json",
               "remote_sentinel_result": f"{self.run_dir}/sentinel.json",
               "local_dir": self.cfg["local_dir"], "optional_checks": self.cfg.get("optional_checks", []),
               "limits": {"max_test_dir_bytes": self.cfg["stop_rules"]["max_test_dir_bytes"]}}
        os.makedirs(self.cfg["local_dir"], exist_ok=False)
        path = os.path.join(self.cfg["local_dir"], "watchdog.json")
        with open(path, "w") as f:
            json.dump(cfg, f)
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        code = f"import sys; sys.path.insert(0, {here!r}); from r110.watchdog import main; sys.exit(main())"
        self.watchdog = subprocess.Popen([sys.executable, "-I", "-c", code, "--config", path])

    def _load_run_id(self):
        return self.cfg["run_id"]

    def stop_watchdog(self):
        """The watchdog (this process's own child): its verdict."""
        w = getattr(self, "watchdog", None)
        if w is None:
            return {"verdict": "not-started"}
        if w.poll() is None:
            w.terminate()
            try:
                w.wait(30)
            except subprocess.TimeoutExpired:
                w.kill()  # our own child, by its handle: never a guess
                try:
                    w.wait(30)
                except subprocess.TimeoutExpired:
                    return {"verdict": "not-stopped", "pid": w.pid}
        return {"verdict": "stopped", "returncode": w.returncode}

    def stop_record(self):
        local = os.path.join(self.cfg["local_dir"], "stop.json")
        if os.path.exists(local):
            try:
                with open(local) as f:
                    return json.load(f)
            except (OSError, ValueError):
                return {"reason": "stop-file-unreadable"}
        out = self.ssh(f"cat {Q(self.load_dir)}/stop.json 2>/dev/null || true", check=False).strip()
        if not out:
            return None
        try:
            return json.loads(out)
        except ValueError:
            return {"reason": "stop-file-unreadable"}

    def write_stop(self, reason):
        """The run's stop file (the first reason wins), written whole."""
        from .watchdog import WRITE_STOP
        rec = json.dumps({"reason": reason, "source": "driver"})
        self.ssh(f"mkdir -p {Q(self.load_dir)}", check=False)
        self.py(WRITE_STOP, f"{self.load_dir}/stop.json", rec, check=False)

    def samples(self):
        """The sampler's records so far, read incrementally (only the bytes
        written since the last read, whole lines only)."""
        if not hasattr(self, "_samples"):
            self._samples, self._sample_off = [], 0
        out = self.ssh(f"tail -c +{self._sample_off + 1} {Q(self.run_dir)}/samples.jsonl 2>/dev/null || true",
                       check=False, timeout=600)
        end = out.rfind("\n") + 1
        # Parsed before the offset moves: a line that does not parse raises
        # and the same bytes are read again, never skipped.
        new = [json.loads(x) for x in out[:end].splitlines() if x.startswith("{") and '"target"' in x]
        self._sample_off += len(out[:end].encode())
        self._samples += new
        return list(self._samples)

    def ledger_records(self):
        """The ledger, checked line by line as ``ledger.read`` does (CRC;
        corruption raises; a torn last line left out). A ledger that cannot
        be read raises: never an empty one."""
        out = self.ssh(f"cat {Q(self.load_dir)}/ledger.jsonl", timeout=600)
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            f.write(out)
        try:
            recs, _torn = ledger_mod.read(f.name)
        finally:
            os.unlink(f.name)
        return recs

    def live_count(self):
        st = self._status(self.load_dir) or {}
        return (st.get("budget") or {}).get("live")

    def stage4_keys(self):
        return sorted(set(ledger_mod.acknowledged(self.ledger_records(), "PUT", stage="4")))

    def integrity(self, kind):
        cf = f"{self.run_dir}/fdb.cluster"
        admin = f"env ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1 {Q(self.bin)} admin --cluster-file {Q(cf)}"
        if kind == "verify-manifests":
            out = self.ssh(f"{admin} integrity verify-manifests 2>&1", check=False)
            m = re.search(r"roots (\d+) findings (\d+)", out)
            return int(m.group(2)) if m else None
        total = 0
        # The findings by class, summed over the groups (kept for the
        # evidence; the order of FindingClass: missing, checksum, truncated,
        # header, unexpected shard, stale epoch, metadata/data, unrecoverable).
        self.last_scrub_classes = None
        # 0.2.0: the shards the scrub holds legitimately or could not
        # classify, summed over the groups (None from a binary that does
        # not print them: its unexpected shards include the records kept
        # for reclaim). Unclassified shards are also counted in `errors`.
        self.last_scrub_held = None
        classes = None
        held = None
        out = self.ssh(f"{admin} status 2>&1 | grep '^group '", check=False)
        groups = re.findall(r"^group (\d+)", out, re.M)
        if not groups:
            return None
        for g in groups:
            o = self.ssh(f"{admin} integrity scrub --group {g} --wait 2>&1", check=False, timeout=3600)
            m = re.search(r"findings \[([\d, ]*)\] enqueued \d+ errors (\d+)", o)
            h = re.search(r"retained (\d+) retained_damaged (\d+) in_flight (\d+) unclassified (\d+)", o)
            if h:
                row_h = dict(zip(("retained", "retained_damaged", "in_flight", "unclassified"),
                                 (int(x) for x in h.groups())))
                held = row_h if held is None else {k: held[k] + v for k, v in row_h.items()}
                self.last_scrub_held = held
            if not m or int(m.group(2)) > 0:
                return None
            row = [int(x) for x in m.group(1).split(",") if x.strip()]
            classes = row if classes is None else [a + b for a, b in zip(classes, row)]
            total += sum(row)
        self.last_scrub_classes = classes
        self.last_scrub_held = held
        return total

    # -- the host's evidence ---------------------------------------------------

    def reach(self, attempts=REACH_ATTEMPTS, wait_s=REACH_WAIT_S):
        """Whether the load host answers: ``attempts`` tries, ``wait_s``
        apart (a bounded reconnect)."""
        errors = []
        for i in range(attempts):
            try:
                rc, out, err = self.runner(self.cfg["hosts"][self.host], "echo r110-reach", None, 30)
                if rc == 0 and "r110-reach" in out:
                    return {"ok": True, "attempts": i + 1}
                errors.append(f"rc {rc}: {err.strip()[-200:]}")
            except Exception as e:  # noqa: BLE001 - a timeout or a failing ssh: not reached
                errors.append(f"{type(e).__name__}: {e}"[:200])
            if i + 1 < attempts:
                self.sleep(wait_s)
        return {"ok": False, "attempts": attempts, "errors": errors}

    def boot_check(self):
        """The host's boot now against the one the run started on
        (``host-boot.json``): ``rebooted`` True, False, or None when either
        boot ID is unknown."""
        now = json.loads(self.py(BOOT, self.boot_id_path, timeout=30).strip().splitlines()[-1])
        setup = self._json(f"{self.run_dir}/host-boot.json")
        a, b = (setup or {}).get("boot_id"), now.get("boot_id")
        return {"setup": setup, "now": now, "rebooted": (a != b) if a and b else None}

    def evidence_roots(self):
        """What is collected: the run directory without the node stores,
        the test FoundationDB's directory without its data (its trace logs
        and marker)."""
        return [["run", self.run_dir, r"d[0-9]+"], ["fdb", self.fdb_data, r"d"]]

    def _evidence_call(self, script, *args, timeout=120):
        return self.py(script, *args, timeout=timeout)

    def collect_evidence(self, dest, meta=None, **kw):
        """The host's evidence saved under ``dest`` and verified
        (``evidence.collect``): the manifest."""
        from . import evidence
        meta = dict(meta or {}, binary_sha256=self.cfg["binary"]["sha256"],
                    binary_commit=self.cfg["binary"]["commit"])
        return evidence.collect(self._evidence_call, self.evidence_roots(), dest, self.cfg["run_id"], meta=meta, **kw)

    def stop_load(self):
        """The main guard, then its load by its own record (confirmed), and
        the guard's result (whether it confirmed its load's group empty)."""
        # The guard's worst case after a TERM (its grace, its group's TERM
        # and KILL waits, services' stops) is under this wait.
        out = {"guard": self._stop("guard.json", GUARD_STOP_S), "load": self._stop("load.json", 60)}
        # Any smoke sub-run left (a stage 2 that failed): its guard and load,
        # from a listing whose failure is reported (independent review).
        listing = self.services_listing()
        if "error" in listing:
            out["listing"] = {"verdict": "unverified", "reason": f"services not listed: {listing['error']}"}
            return out
        names = set(listing.get("records", [])) | {n + ".json" for n in listing.get("started", [])}
        for n in sorted(names):
            if n.startswith(("guard-smoke", "load-smoke")):
                out[n] = self._stop(n, GUARD_STOP_S if n.startswith("guard") else 60)
        res = self._json(f"{self.load_dir}/guard.json")
        out["guard_result"] = {k: res.get(k) for k in ("reason", "group_empty", "load_reaped")} if res else None
        return out

    def escalation(self):
        """The first resource or monitor alarm the watchdog wrote (its local
        copy), or the sentinel's result on the host, if any."""
        d = os.path.join(self.cfg["local_dir"], "alarms")
        for n in sorted(os.listdir(d)) if os.path.isdir(d) else []:
            try:
                with open(os.path.join(d, n)) as f:
                    return json.load(f)
            except (OSError, ValueError):
                return {"reason": "stop-file-unreadable", "file": n}
        return self._json(f"{self.run_dir}/sentinel.json")

    def _stop(self, name, wait):
        """One recorded process stopped by identity: its verdict (a dict)."""
        # The ssh outlasts the stop it runs (TERM wait, then half of it for
        # KILL): never cut short while the host still waits for a process.
        try:
            out = self.py(STOP, self.tools, f"{self.run_dir}/services/{name}", wait, check=False,
                          timeout=1.5 * wait + 60).strip()
        except subprocess.TimeoutExpired:
            return {"verdict": "unverified", "reason": "the stop's ssh timed out"}
        try:
            return json.loads(out.splitlines()[-1])
        except (ValueError, IndexError):
            return {"verdict": "unverified", "reason": f"no verdict: {out[-200:]}"}

    def services_listing(self):
        """``{"exists", "started", "records"}`` of the run's services
        directory (``started``: started.list, every process the run started,
        listed before it started), or ``{"error": ...}`` when it cannot be
        read: never an empty listing on a failure."""
        rc, out, err = self.runner(self.cfg["hosts"][self.host], "python3 -I - " + Q(f"{self.run_dir}/services"),
                                   LISTING, 120)
        try:
            if rc != 0:
                raise ValueError(f"rc {rc}: {err.strip()[-200:]}")
            res = json.loads(out.strip().splitlines()[-1])
            if not isinstance(res, dict) or not isinstance(res.get("exists"), bool):
                raise ValueError(f"not a listing: {out[-200:]}")
            return res
        except (ValueError, IndexError) as e:
            return {"error": str(e)[:300]}

    def stop_services(self, names=None):
        """The test's gateway, controller, nodes and FoundationDB by
        identity, each with its process group (never the guards, the
        sentinel or the sampler): ``{"verdicts": name -> verdict,
        "listing": ...}``. Every service started.list names gets a verdict
        (a missing record is unverified); ``names`` restricts the stop to
        those (a retry)."""
        listing = self.services_listing()
        out = {"listing": listing, "verdicts": {}}
        if "error" in listing:
            return out
        records = set(listing.get("records", []))
        wanted = {n + ".json" for n in listing.get("started", [])} | records
        for n in sorted(wanted, key=lambda n: (n.startswith("fdb"), n)):
            if n.startswith(("guard", "sampler", "sentinel")) or (names is not None and n not in names):
                continue
            out["verdicts"][n] = self._stop(n, 30)
        return out

    # -- teardown --------------------------------------------------------------

    def teardown(self, evidence_dir=None, **collect_kw):
        """Stop every recorded test process by identity, save the host's
        evidence under ``evidence_dir`` (the run's evidence directory), then
        remove the run's two directories only when (1) every stop was
        confirmed and the liveness check held (``ALIVE``), (2) the evidence
        was saved complete and verified (the saved copies read back, the
        host's files unchanged since) and (3) their markers name this run.
        Anything else keeps everything, with the reason; a teardown can be
        run again. The stops never wait for the collection (it comes after
        them); an unreachable host is neither stopped, collected nor
        touched. A process found gone after the host booted again is
        recorded as ended by the reboot, never as stopped."""
        rid = self.cfg["run_id"]
        out = {"stopped": {}, "removed": []}
        out["reach"] = self.reach()
        if not out["reach"]["ok"]:
            out["kept"] = "the host is unreachable: nothing stopped, collected or removed (run the teardown again)"
            return self._teardown_record(evidence_dir, out)
        try:
            boot = out["host_boot"] = self.boot_check()
        except Exception as e:  # noqa: BLE001 - unknown, not "no reboot"
            boot = out["host_boot"] = {"rebooted": None, "error": f"{type(e).__name__}: {e}"[:300]}
        names = self.ssh(f"ls {Q(self.run_dir)}/services 2>/dev/null || true", check=False).split()
        order = sorted((n for n in names if n.endswith(".json")),
                       key=lambda n: (not n.startswith("guard"), not n.startswith("load"), not n.startswith("sentinel"),
                                      n.startswith("fdb"), n.startswith("sampler")))
        for n in order:
            out["stopped"][n] = self._stop(n, GUARD_STOP_S if n.startswith("guard") else 30)
        gone = sorted(n for n, v in out["stopped"].items() if v.get("verdict") == "gone")
        if gone and boot.get("rebooted") is not False:
            # Gone with the boot, not by any stop: whether the guard stopped
            # the load before is only in its own result (collected below).
            cause = "host-reboot" if boot.get("rebooted") else "unknown (the host's boot could not be compared)"
            for n in gone:
                out["stopped"][n] = dict(out["stopped"][n], cause=cause)
            out["gone_at_reboot" if boot.get("rebooted") else "gone_boot_unknown"] = gone
        unconfirmed = {n: v for n, v in out["stopped"].items()
                       if v.get("verdict") not in ("stopped", "gone", "not-started")}
        if evidence_dir is None:
            out["kept"] = "no evidence directory: nothing is removed before the host's evidence is saved"
            return out
        from . import evidence
        man = self.collect_evidence(evidence_dir, meta={"host_boot": boot}, **collect_kw)
        out["evidence"] = {"complete": man["complete"], "attempt": man["attempt"], "files": len(man["files"]),
                           "incomplete": man["incomplete"], "errors": man["errors"][:20]}
        out["guard_result"] = gr = self._saved_json(evidence_dir, "run/load/guard.json", man)
        if boot.get("rebooted"):
            rec = gr.get("record") if isinstance(gr.get("record"), dict) else None
            out["reboot_note"] = (
                "the host booted again after the run started: the processes found gone ended with that boot, "
                "not by a stop of this teardown or the guard; " + (
                    f"the guard's own result says reason {rec.get('reason')!r}, group_empty {rec.get('group_empty')!r}"
                    if rec else "no result of the guard was saved: whether it stopped the load before is not known"))
        if unconfirmed:
            out["kept"] = f"stop not confirmed: {sorted(unconfirmed)}"
            return self._teardown_record(evidence_dir, out)
        # Nothing is removed unless this check ran to its end and found no
        # process of the run: an unreadable record or a failed check keeps
        # everything.
        rc, alive, err = self.runner(self.cfg["hosts"][self.host], "python3 -I - " + " ".join(
            Q(a) for a in (self.tools, f"{self.run_dir}/services")), ALIVE, 120)
        lines = alive.strip().splitlines()
        if rc != 0 or not lines or not lines[-1].startswith("checked ") or len(lines) > 1:
            out["kept"] = f"still running or not checked (rc {rc}): {alive.strip()[-300:]} {err.strip()[-200:]}"
            return self._teardown_record(evidence_dir, out)
        if not man["complete"]:
            out["kept"] = f"the host's evidence is not saved complete: {man['incomplete']}"
            return self._teardown_record(evidence_dir, out)
        bad = evidence.verify_local(evidence_dir, man)
        if not bad:
            try:
                bad = evidence.changed_since(man, evidence.inventory(
                    self._evidence_call, self.evidence_roots(),
                    timeout=collect_kw.get("inventory_s", evidence.INVENTORY_S)))
            except evidence.Failed as e:
                bad = [str(e)]
        if bad:
            out["kept"] = f"the saved evidence no longer matches: {bad[:10]}"
            return self._teardown_record(evidence_dir, out)
        out["removing"] = True
        if "record_error" in self._teardown_record(evidence_dir, out):
            out.pop("removing")
            out["kept"] = f"the teardown's own record could not be saved: {out['record_error']}"
            return out
        # The run directory last: it holds the tools and the services'
        # records the liveness check reads, so a teardown cut short between
        # the two can be run again to the end.
        # A directory already gone (an earlier teardown removed it) is
        # "absent"; one whose marker does not name this run, or whose removal
        # failed, is kept and fails the teardown.
        for d in (self.fdb_data, self.run_dir):
            r = self.ssh(f"if [ ! -e {Q(d)} ] && [ ! -L {Q(d)} ]; then echo absent; "
                         f"elif [ \"$(cat {Q(d)}/{MARK} 2>/dev/null)\" = {Q(rid)} ] && rm -rf -- {Q(d)}; "
                         f"then echo removed; else echo kept; fi", check=False).strip()
            key = {"removed": "removed", "absent": "absent"}.get(r, "kept_dirs")
            out[key] = out.get(key, []) + [d]
        parent = os.path.dirname(self.run_dir)
        if not out.get("kept_dirs") and parent == os.path.dirname(self.fdb_data) and parent.endswith(rid):
            # Their common parent, named by the run, only if now empty.
            out["parent"] = self.ssh(f"rmdir -- {Q(parent)} 2>/dev/null && echo removed || echo kept",
                                     check=False).strip() or "kept"
        if out.get("kept_dirs"):
            out["kept"] = (f"left on the host: {out['kept_dirs']} (a marker not naming this run, or the removal "
                           "failed; plan §6.2: removing a directory left without its marker)")
        out.pop("removing")
        return self._teardown_record(evidence_dir, out)

    def _teardown_record(self, evidence_dir, out):
        """The teardown's result kept with the run's evidence (private: it
        names the host's directories), one file per attempt."""
        if evidence_dir is None:
            return out
        d = os.path.join(evidence_dir, "private", "teardown")
        try:
            os.makedirs(d, exist_ok=True)
            n = out.setdefault("attempt", len([x for x in os.listdir(d) if x.endswith(".json")]) + 1)
            common.atomic_write_json(os.path.join(d, f"{n:03d}.json"), out)
            out.pop("record_error", None)
        except OSError as e:
            out["record_error"] = f"{type(e).__name__}: {e}"[:300]
        return out

    @staticmethod
    def _saved_json(evidence_dir, rel, man):
        """A record as saved by the collection, or why there is none."""
        if not any(f["path"] == rel for f in man["files"]):
            missing = any(m["path"] == rel for m in man["missing"])
            return {"saved": False, "why": "absent on the host" if missing else "not saved"}
        try:
            return {"saved": True, "record": common.read_json(os.path.join(evidence_dir, "private", "host", rel))}
        except (OSError, ValueError) as e:
            return {"saved": True, "unreadable": f"{type(e).__name__}"}
