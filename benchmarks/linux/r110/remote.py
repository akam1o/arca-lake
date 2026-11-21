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
import time

from . import judge, ledger as ledger_mod

Q = shlex.quote
TOOLS_FILES = ("__init__.py", "common.py", "budget.py", "ledger.py", "s3.py", "load.py", "proc.py",
               "guard.py", "sampler.py")
MARK = ".r110-run"

START = r'''
import json, os, subprocess, sys
sys.path.insert(0, sys.argv[1])
from r110 import proc
name, cwd, cmd = sys.argv[2], sys.argv[3], sys.argv[4:]
svc = os.path.join(cwd, "services")
os.makedirs(svc, exist_ok=True)
rec_path = os.path.join(svc, name + ".json")
if os.path.exists(rec_path):
    rec = json.load(open(rec_path))
    if proc.matches(rec):
        print("already"); sys.exit(0)
log = open(os.path.join(cwd, "logs", name + ".log"), "ab")
p = subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
ident = None
for _ in range(100):
    ident = proc.identity(p.pid)
    if ident and ident["cwd"]:
        break
json.dump(ident, open(rec_path + ".tmp", "w")); os.replace(rec_path + ".tmp", rec_path)
print(json.dumps(ident))
'''

STOP = r'''
import json, os, signal, sys, time
sys.path.insert(0, sys.argv[1])
from r110 import proc
path, wait = sys.argv[2], float(sys.argv[3])
rec = json.load(open(path))
r = proc.signal_if_same(rec, signal.SIGTERM)
end = time.monotonic() + wait
while r == "signalled" and time.monotonic() < end and proc.matches(rec):
    time.sleep(0.1)
if proc.matches(rec):
    r = "killed:" + proc.signal_if_same(rec, signal.SIGKILL)
print(r)
'''


def ssh_runner(target, command, stdin=None, timeout=120):
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
                        target, command], input=stdin, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


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

    # -- plumbing --------------------------------------------------------------

    def ssh(self, command, stdin=None, check=True, host=None, timeout=120):
        rc, out, err = self.runner(self.cfg["hosts"][host or self.host], command, stdin, timeout)
        if check and rc != 0:
            raise RemoteError(f"rc {rc}: {err.strip()[-300:]}")
        return out

    def py(self, script, *args, check=True):
        return self.ssh("python3 -I - " + " ".join(Q(str(a)) for a in args), stdin=script, check=check)

    def tool(self, module, *args):
        code = f"import sys; sys.path.insert(0, {self.tools!r}); from r110.{module} import main; sys.exit(main())"
        return "python3 -I -c " + Q(code) + " " + " ".join(Q(str(a)) for a in args)

    def nodes(self):
        return [f"node{i}" for i in range(self.cfg["settings"]["nodes"])]

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
        return c

    # -- setup, smoke ----------------------------------------------------------

    def setup(self):
        rid = self.cfg["run_id"]
        self.ssh(f"[ ! -e {Q(self.run_dir)} ] && [ ! -e {Q(self.fdb_data)} ] || "
                 f"{{ echo 'run directories exist' >&2; exit 1; }}; "
                 f"mkdir -p {Q(self.run_dir)}/logs {Q(self.run_dir)}/services {Q(self.tools)}/r110 {Q(self.fdb_data)} && "
                 f"echo {Q(rid)} > {Q(self.run_dir)}/{MARK} && echo {Q(rid)} > {Q(self.fdb_data)}/{MARK}")
        for f in TOOLS_FILES:
            with open(os.path.join(self.tools_src, f)) as fh:
                self.ssh(f"cat > {Q(self.tools)}/r110/{f}", stdin=fh.read())
        self.start_watchdog()
        self.cluster_services()

    def start(self, name, command):
        return self.py(START, self.tools, name, self.run_dir, *command)

    def cluster_services(self):
        """FoundationDB, four nodes with their allotments, controller,
        gateway: the plan's settings (§5), every other flag the default."""
        s, p, rid = self.cfg["settings"], self.ports, self.cfg["run_id"]
        cf = f"{self.run_dir}/fdb.cluster"
        addr = "127.0.0.1"
        self.ssh(f"echo {Q('r110:' + rid.replace('-', '') + '@' + addr + ':' + str(p['fdb']))} > {Q(cf)}")
        self.start("fdb", ["fdbserver", "-p", f"{addr}:{p['fdb']}", "-C", cf, "-d", f"{self.fdb_data}/d",
                           "-L", f"{self.fdb_data}/l"])
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
                     "arca_node_admission_refused_total", "arca_node_live_bytes", "arca_node_segment_bytes"]
        ctl_fams = ["arca_controller_gc_backlog", "arca_controller_gc_backlog_bytes", "arca_controller_gc_collected_total",
                    "arca_controller_reclaimed_total", "arca_controller_reclaimed_bytes_total",
                    "arca_controller_reclaim_refused_total", "arca_controller_reclaim_errors_total",
                    "arca_controller_backpressure_state", "arca_controller_capacity_bytes",
                    "arca_controller_gc_skipped_referenced_total", "arca_controller_pass_running_since_seconds"]
        gw_fams = ["arca_gateway_admission_state", "arca_gateway_admission_rejected_total",
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
        self.start("sampler", ["sh", "-c", cmd])

    def start_load(self, sub=None, limits=None, heartbeat=True):
        """The load (under the guard) in ``load_dir`` or a sub-run (both read
        the main run's heartbeat, unless ``heartbeat`` is False)."""
        d = f"{self.run_dir}/{sub}" if sub else self.load_dir
        rid = self.cfg["run_id"] + (f"-{sub}" if sub else "")
        g = self.cfg["stop_rules"]["guard"]
        self.ssh(f"mkdir -p {Q(d)}/control && echo {Q(json.dumps({'run_id': rid}))} > {Q(d)}/run.json")
        hb = f"{self.run_dir}/heartbeat.json" if heartbeat else f"{d}/no-heartbeat.json"
        load = self.tool("load", "run", "--run-dir", d, "--endpoint", f"http://127.0.0.1:{self.ports['gateway']}",
                         "--object-size", self.cfg["settings"]["object_size"], "--heartbeat", hb,
                         "--heartbeat-id", self.cfg["run_id"],
                         *(["--limits", json.dumps(limits or self.cfg["limits"])]))
        guard = self.tool("guard", "run", "--run-dir", d, "--heartbeat", hb, "--data-dir", self.mount,
                          "--min-free-gib", g["min_free_gib"], "--min-mem-mib", g["min_mem_mib"],
                          "--heartbeat-timeout", g["heartbeat_timeout_s"], "--heartbeat-id", self.cfg["run_id"],
                          # A smoke sub-run's exercised stop must not reach the
                          # main run's services: only the main guard gets them.
                          *([] if sub else ["--services", f"{self.run_dir}/services"]),
                          "--", "sh", "-c", load)
        self.start("guard" + (f"-{sub}" if sub else ""), ["sh", "-c", guard])
        self.step_n = 0
        return d

    def smoke(self):
        """Stage 2 in sub-runs of their own: a withheld heartbeat stops a
        load; an attempt limit ends one; 100 PUT, GET, DELETE and a verify."""
        self.setup()
        res = {"errors": 0}
        d = self.start_load("smoke-hb", heartbeat=False)
        self._send(d, 1, {"kind": "put", "keys": [f"s{i}" for i in range(10_000)], "rate": 2})
        st = self._wait_exit(d, 180)
        res["stop_layers_exercised"] = bool(st and st.get("stopped") == "heartbeat-lost")
        d = self.start_load("smoke-limit", limits=dict(self.cfg["limits"], max_attempts=20))
        self._send(d, 1, {"kind": "create-bucket"})
        self._send(d, 2, {"kind": "put", "keys": [f"l{i}" for i in range(100)]})
        st = self._wait_exit(d, 120)
        res["limits_exercised"] = bool(st and st.get("stopped") == "attempts-limit")
        d = self.start_load("smoke-ops")
        keys = [f"o{i}" for i in range(100)]
        for n, step in enumerate([{"kind": "put", "keys": keys}, {"kind": "get", "keys": keys},
                                  {"kind": "delete", "keys": keys}, {"kind": "verify"}, {"kind": "finish"}], 1):
            self._send(d, n, step)
        st = self._wait_exit(d, 600)
        verify = st and st["steps"][3]["result"] if st and len(st.get("steps", [])) > 3 else None
        if not verify or verify.get("violations") or not verify.get("complete"):
            res["errors"] += 1
        res["ops"] = verify
        return res

    def _send(self, d, n, step):
        self.ssh(f"cat > {Q(d)}/control/{n:06d}.json.tmp && mv {Q(d)}/control/{n:06d}.json.tmp "
                 f"{Q(d)}/control/{n:06d}.json", stdin=json.dumps(step))

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

    def run_step(self, step, deadline):
        n = self.send_step(dict(step, max_seconds=max(1, int(deadline - time.monotonic()))))
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

    def start_watchdog(self):
        """The watchdog, on this machine (the driver's child)."""
        import sys
        cfg = {"run_id": self._load_run_id(),
               "hosts": self.cfg["hosts"], "load_host": self.host,
               "system_fdb_hosts": self.cfg.get("system_fdb_hosts", []),
               "remote_run_dir": self.load_dir, "remote_data_mount": self.mount,
               "remote_heartbeat": f"{self.run_dir}/heartbeat.json",
               "remote_test_dirs": [self.run_dir, self.fdb_data],
               "remote_test_fdb_cluster": f"{self.run_dir}/fdb.cluster",
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
        w = getattr(self, "watchdog", None)
        if w and w.poll() is None:
            w.terminate()
            w.wait(30)

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
        rec = json.dumps({"reason": reason, "source": "driver"})
        self.ssh(f"mkdir -p {Q(self.load_dir)} && set -C && cat > {Q(self.load_dir)}/stop.json || true",
                 stdin=rec, check=False)

    def samples(self):
        out = self.ssh(f"cat {Q(self.run_dir)}/samples.jsonl 2>/dev/null || true", check=False, timeout=600)
        return [json.loads(x) for x in out.splitlines() if x.startswith("{") and '"target"' in x]

    def ledger_records(self):
        out = self.ssh(f"cat {Q(self.load_dir)}/ledger.jsonl", check=False, timeout=600)
        recs = []
        for line in out.splitlines():
            if not line.endswith("}"):
                continue
            crc, body = line.split(" ", 1)
            recs.append(json.loads(body))
        return recs

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
        out = self.ssh(f"{admin} status 2>&1 | grep '^group '", check=False)
        groups = re.findall(r"^group (\d+)", out, re.M)
        if not groups:
            return None
        for g in groups:
            o = self.ssh(f"{admin} integrity scrub --group {g} --wait 2>&1", check=False, timeout=3600)
            m = re.search(r"findings \[([\d, ]*)\] enqueued \d+ errors (\d+)", o)
            if not m or int(m.group(2)) > 0:
                return None
            total += sum(int(x) for x in m.group(1).split(",") if x.strip())
        return total

    def collect_logs(self):
        return self.ssh(f"tail -c 2000000 {Q(self.run_dir)}/logs/*.log 2>/dev/null || true", check=False, timeout=600)

    def stop_load(self):
        self.py(STOP, self.tools, f"{self.run_dir}/services/guard.json", 60, check=False)
        self.stop_watchdog()

    # -- teardown --------------------------------------------------------------

    def teardown(self):
        """Stop every recorded test process by identity, then remove the
        run's two directories, only when their markers name this run and
        nothing of the run is left running."""
        rid = self.cfg["run_id"]
        out = {"stopped": {}, "removed": []}
        names = self.ssh(f"ls {Q(self.run_dir)}/services 2>/dev/null || true", check=False).split()
        order = sorted(names, key=lambda n: (not n.startswith("guard"), n.startswith("fdb")))
        for n in order:
            if n.endswith(".json"):
                out["stopped"][n] = self.py(STOP, self.tools, f"{self.run_dir}/services/{n}", 30, check=False).strip()
        alive = self.py(r'''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from r110 import proc
d = sys.argv[2]
for n in os.listdir(d) if os.path.isdir(d) else []:
    if n.endswith(".json") and proc.matches(json.load(open(os.path.join(d, n)))):
        print(n)
''', self.tools, f"{self.run_dir}/services", check=False)
        if alive is None or alive.strip():
            out["kept"] = f"still running: {alive.strip() if alive else 'unknown'}"
            return out
        for d in (self.run_dir, self.fdb_data):
            r = self.ssh(f"[ \"$(cat {Q(d)}/{MARK} 2>/dev/null)\" = {Q(rid)} ] && rm -rf -- {Q(d)} && echo removed || "
                         f"echo kept", check=False).strip()
            out["removed" if r == "removed" else "kept_dirs"] = out.get("removed" if r == "removed" else "kept_dirs", []) + [d]
        return out
