#!/usr/bin/env python3
"""Functional validation of arca-lake on three Linux hosts (0.1.0
experimental release). Plan, expected behaviour and pass/fail rules:
docs/verification/0.1.0-three-host-validation.md.

Layout: host A runs a private FoundationDB process (its own port and data
directory, never the system service), controller ctl-a and storage nodes
a0, a1; host B controller ctl-b, nodes b0, b1 and gateway gw-b; host C
nodes c0, c1, gateway gw-c and the S3 client (three-host-client.py). Node
labels host=a|b|c; groups use --failure-domain host. Plaintext transport
(ARCA_INSECURE_PLAINTEXT, ARCA_FDB_INSECURE_PLAINTEXT).

  HOST_A=user@a HOST_B=user@b HOST_C=user@c [RUN_ID=id] [OUT=dir] \\
    benchmarks/linux/three-host-validate.py run
  HOST_A=... HOST_B=... HOST_C=... RUN_ID=id three-host-validate.py teardown

Each host needs the fdb build of arca-lake at BIN (default
~/r110-dev/v3h/bin/arca-lake), Python 3 and curl; host A fdbserver and
fdbcli. Everything runs under /mnt/arca-fdb/v3h-<run id>/ on every host
(marker file .owner); only processes whose pid file is there and whose
command line names that directory are signalled. Host addresses come only
from the environment and are replaced by <host-a|b|c> in every output;
the output directory is passed through sanitize.py at the end.

Each step is judged PASS, FAIL or NOT RUN with its evidence in
OUT/results.json and OUT/results.md; a FAIL stops the sequence (the later
steps are NOT RUN) and the cluster is torn down.
"""

import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
HOSTS = {h: os.environ.get(f"HOST_{h.upper()}", "") for h in "abc"}
RUN_ID = os.environ.get("RUN_ID") or time.strftime("%Y%m%d-%H%M%S")
R = f"/mnt/arca-fdb/v3h-{RUN_ID}"
BIN = os.environ.get("BIN", "$HOME/r110-dev/v3h/bin/arca-lake")
ENV = "ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1"
OUT = Path(os.environ.get("OUT", str(HERE / "results" / f"v3h-{RUN_ID}")))
MARKER = "0.1.0 three-host validation"
FDB_PORT = 4610
NODE_PORTS = {0: 9611, 1: 9612}
GW_PORT = 9600
CTL_HTTP = "127.0.0.1:9640"
GW_METRICS = "127.0.0.1:9630"
CAPACITY = 3 << 30
SEGMENT = 64 << 20
NODES = ["a0", "b0", "c0", "a1", "b1", "c1"]  # registration order
CONTROLLERS = {"ctl-a": "a", "ctl-b": "b"}
GATEWAYS = {"gw-b": "b", "gw-c": "c"}
DAMAGE = ["MissingShard", "ChecksumMismatch", "TruncatedShard", "InvalidHeader",
          "UnexpectedShard", "StalePlacementEpoch", "MetadataDataMismatch", "UnrecoverableStripe"]

ADDR = {}
RESULTS = []
STEP_ORDER = ["0-setup", "1-placement", "2-s3-basic", "3a-node-kill", "3b-gateway-restart",
              "3c-host-b-nodes-down", "4-controller-takeover", "5-node-return",
              "6-fdb-unavailable", "7a-scrub-repair-corrupt", "7b-wipe-replace", "7c-final"]


# --- plumbing -------------------------------------------------------------

def redact(text):
    for h, a in ADDR.items():
        if a:
            text = text.replace(a, f"<host-{h}>")
    for h, t in HOSTS.items():
        if t:
            text = text.replace(t, f"<host-{h}>")
    return text


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(redact(line), flush=True)
    with open(OUT / "driver.log", "a") as f:
        f.write(redact(line) + "\n")


def ssh(h, cmd, timeout=120, check=False):
    try:
        p = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
             HOSTS[h], cmd], capture_output=True, text=True, timeout=timeout)
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


def ts(h):
    return float(ssh(h, "date +%s.%N", check=True)[1])


def node_host(n):
    return n[0]


def start(h, name, cmd):
    ssh(h, f"cd {R} && {{ nohup env {ENV} {cmd} > {R}/logs/{name}.log 2>&1 < /dev/null & echo $! > {R}/{name}.pid; }}",
        check=True)


def signal(h, name, sig):
    """Signal a process this run started (pid file and command line both
    under the run directory). Returns the host's time just before the
    signal."""
    rc, out, err = ssh(h, f"p=$(cat {R}/{name}.pid) && grep -qa v3h-{RUN_ID} /proc/$p/cmdline && "
                          f"date +%s.%N && kill -{sig} $p", check=False)
    if rc != 0:
        raise RuntimeError(f"cannot signal {name} on host {h}: {redact(err)}")
    return float(out.split()[0])


def signal_many(h, names, sig):
    """Like `signal` for several processes of one host in one command;
    returns the host's time just after the last signal."""
    checks = " && ".join(f"p{i}=$(cat {R}/{n}.pid) && grep -qa v3h-{RUN_ID} /proc/$p{i}/cmdline"
                         for i, n in enumerate(names))
    pids = " ".join(f"$p{i}" for i in range(len(names)))
    rc, out, err = ssh(h, f"{checks} && kill -{sig} {pids} && date +%s.%N")
    if rc != 0:
        raise RuntimeError(f"cannot signal {names} on host {h}: {redact(err)}")
    return float(out.split()[0])


def alive(h, name):
    rc, _, _ = ssh(h, f"p=$(cat {R}/{name}.pid 2>/dev/null) && grep -qa v3h-{RUN_ID} /proc/$p/cmdline")
    return rc == 0


def wait_dead(h, name, secs=30):
    end = time.time() + secs
    while time.time() < end:
        if not alive(h, name):
            return True
        time.sleep(0.5)
    return False


def admin(args, timeout=60, h="a"):
    rc, out, err = ssh(h, f"env {ENV} {BIN} admin --cluster-file {R}/fdb.cluster {args}", timeout=timeout)
    return rc, out, err


def node_cmd(n):
    h, i = n[0], int(n[1])
    return (f"{BIN} storage-node --listen {ADDR[h]}:{NODE_PORTS[i]} --dir {R}/n-{n} "
            f"--metrics-listen 127.0.0.1:{9621 + i} --capacity-bytes {CAPACITY} --segment-max-bytes {SEGMENT}")


def ctl_cmd(cid):
    return (f"{BIN} controller --cluster-file {R}/fdb.cluster --id {cid} --http {CTL_HTTP} "
            "--failure-domain host --unhealthy-after-secs 20 --health-interval-secs 5 "
            "--gc-interval-secs 10 --gc-grace-secs 30 --min-orphan-age-secs 90 --reconcile-interval-secs 30 "
            "--admission-horizon-secs 60 --admission-dwell-secs 10 --scrub-interval-secs 0")


def gw_cmd(g):
    h = GATEWAYS[g]
    return (f"{BIN} gateway --backend cluster --cluster-file {R}/fdb.cluster --allow-anonymous "
            f"--listen {ADDR[h]}:{GW_PORT} --metrics-listen {GW_METRICS} --k 2 --m 1 "
            "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain host --lease-secs 60")


def fdb_cmd():
    return (f"fdbserver -p {ADDR['a']}:{FDB_PORT} -C {R}/fdb.cluster -d {R}/fdb/data -L {R}/fdb/log "
            "--memory 2GiB --cache-memory 256MiB")


def gw_spec():
    return ",".join(f"{g}={ADDR[h]}:{GW_PORT}" for g, h in GATEWAYS.items())


# --- status parsing -------------------------------------------------------

def status():
    rc, out, err = admin("status", timeout=60)
    if rc != 0:
        raise RuntimeError(f"admin status failed: {redact(err)[-300:]}")
    nodes, groups, leader, life = {}, {}, None, {}
    for line in out.splitlines():
        m = re.match(r"^node (\d+) (\S+) labels (.*) last_seen_ms (\d+)$", line)
        if m:
            labels = dict(re.findall(r'\("([^"]+)", "([^"]*)"\)', m.group(3)))
            nodes[int(m.group(1))] = {"name": labels.get("node"), "host": labels.get("host"),
                                      "last_seen_ms": int(m.group(4))}
            continue
        m = re.match(r"^group (\d+) tier (\S+) (\S+) (\d+)\+(\d+) epoch (\d+) (\w+) members \[([\d, ]*)\]", line)
        if m:
            groups[int(m.group(1))] = {"scheme": m.group(3), "k": int(m.group(4)), "m": int(m.group(5)),
                                       "epoch": int(m.group(6)), "state": m.group(7),
                                       "members": [int(x) for x in m.group(8).split(",") if x.strip()]}
            continue
        m = re.match(r"^leader (\S+) token (\d+) lease_until_ms (\d+)", line)
        if m:
            leader = {"holder": m.group(1).split("@")[0], "token": int(m.group(2)),
                      "lease_until_ms": int(m.group(3))}
            continue
        m = re.match(r"^node (\d+) lifecycle (\w+)", line)
        if m:
            life[int(m.group(1))] = m.group(2)
    return {"nodes": nodes, "groups": groups, "leader": leader, "lifecycle": life, "raw": out}


def node_id(st, name):
    return next(i for i, n in st["nodes"].items() if n["name"] == name)


def ctl_metric(name):
    st = status()
    if not st["leader"]:
        return None, None
    h = CONTROLLERS.get(st["leader"]["holder"])
    rc, out, _ = ssh(h, f"curl -s --max-time 5 http://{CTL_HTTP}/metrics | grep -E '^{name}[ {{]'")
    vals = [float(l.split()[-1]) for l in out.splitlines() if l.strip()]
    return st["leader"]["holder"], (sum(vals) if vals else None)


def gw_metrics():
    res = {}
    for g, h in GATEWAYS.items():
        _, out, _ = ssh(h, f"curl -s --max-time 5 http://{GW_METRICS}/metrics | grep -E 'degraded|unreadable|integrity_report' | grep -v '^#'")
        res[g] = out.strip().splitlines()
    return res


def integrity_status():
    rc, out, err = admin("integrity status", timeout=60)
    m = re.search(r"repair queue depth (\d+) unrecoverable (\d+) failed (\d+)", out)
    return (dict(depth=int(m.group(1)), unrecoverable=int(m.group(2)), failed=int(m.group(3))) if m else None), out


def wait_queue_empty(secs):
    end = time.time() + secs
    last = (None, "")
    while time.time() < end:
        q, out = integrity_status()
        last = (q, out)
        if q and q["depth"] == 0:
            return q, out
        time.sleep(5)
    return last


def scrub(groups):
    """Scrub each group to completion from host A; findings by class."""
    res = {}
    for g in groups:
        rc, out, err = admin(f"integrity scrub --group {g} --wait", timeout=900)
        m = re.search(r"verified (\d+) bytes (\d+) findings \[([\d, ]+)\] enqueued (\d+) errors (\d+)", out)
        if not m:
            res[g] = {"rc": rc, "error": redact((out + err)[-400:])}
            continue
        f = [int(x) for x in m.group(3).split(",")]
        res[g] = {"verified": int(m.group(1)), "bytes": int(m.group(2)),
                  "findings": dict(zip(DAMAGE, f)), "enqueued": int(m.group(4)), "errors": int(m.group(5))}
    return res


def damage_free(scrubbed, ignore=("UnexpectedShard",)):
    for r in scrubbed.values():
        if "findings" not in r or r["errors"]:
            return False
        if any(v for k, v in r["findings"].items() if k not in ignore):
            return False
    return True


# --- client ---------------------------------------------------------------

CLIENT = f"python3 {R}/client.py"


def client_sync(cmd, phase, extra="", timeout=900):
    rc, out, err = ssh("c", f"cd {R} && {CLIENT} {cmd} {R}/state.json {R}/ledger.jsonl {phase} "
                            f"{shlex.quote(gw_spec())} {extra}", timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"client {cmd} {phase} rc {rc}: {redact(err)[-800:]}")
    return json.loads(out.strip().splitlines()[-1])


class Load:
    def __init__(self, phase, extra=""):
        self.phase, self.extra = phase, extra

    def __enter__(self):
        ssh("c", f"rm -f {R}/stop-{self.phase}", check=True)
        ssh("c", f"cd {R} && {{ nohup {CLIENT} load {R}/state.json {R}/ledger.jsonl {self.phase} "
                 f"{shlex.quote(gw_spec())} --duration 1800 --pause-ms 25 --stop-file {R}/stop-{self.phase} {self.extra} "
                 f"> {R}/logs/client-{self.phase}.log 2>&1 < /dev/null & echo $! > {R}/client-{self.phase}.pid; }}",
            check=True)
        time.sleep(1)
        return self

    def __exit__(self, *exc):
        ssh("c", f"touch {R}/stop-{self.phase}")
        if not wait_dead("c", f"client-{self.phase}", 150):
            log(f"client {self.phase} did not stop in 150 s")
        return False


def ledger(phase=None):
    _, out, _ = ssh("c", f"cat {R}/ledger.jsonl", timeout=300)
    recs = [json.loads(l) for l in out.splitlines() if l.strip()]
    return [r for r in recs if phase is None or r["phase"] == phase]


def counts(recs):
    c = {}
    for r in recs:
        k = f"{r['op']} {r['gw']} {r['outcome']}" + (f" {r['status']}" if r["outcome"] not in ("ok", "ack") else "")
        c[k] = c.get(k, 0) + 1
    return dict(sorted(c.items()))


BAD = ("error", "unknown", "refused")


def within(r, a, b):
    return r["t0"] >= a and r["t1"] <= b


def samples(recs, n=8):
    return [{k: r.get(k) for k in ("op", "gw", "key", "status", "outcome", "err", "t0", "t1")} for r in recs[:n]]


def verify(phase, collapse=False):
    """GET every key of the ledger state; violations and unanswered keys."""
    summary = client_sync("verify", phase, "--collapse" if collapse else "", timeout=1200)
    recs = ledger(phase)
    by_key = {}
    for r in recs:
        by_key.setdefault(r["key"], []).append(r)
    unanswered = [k for k, rs in by_key.items() if all(x["outcome"] in BAD for x in rs)]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    resolved = {}
    for r in recs:
        if r["outcome"] == "ok" and r["cands"] > 1:
            resolved[r["resolved"]] = resolved.get(r["resolved"], 0) + 1
    absent = sum(1 for r in recs if r["outcome"] == "ok" and r["status"] == 404)
    present = sum(1 for r in recs if r["outcome"] == "ok" and r["status"] == 200)
    return {"keys": len(by_key), "present_ok": present, "absent_ok": absent, "violations": len(viol),
            "violation_samples": samples(viol), "unanswered": len(unanswered),
            "multi_candidate_resolved": resolved, "client": summary,
            "ok": not viol and not unanswered and len(by_key) == summary["keys"]}


def result(step, verdict, evidence, expected=""):
    RESULTS.append({"step": step, "verdict": verdict, "expected": expected, "evidence": evidence})
    save("results.json", json.dumps(RESULTS, indent=1, sort_keys=True))
    log(f"STEP {step}: {verdict}")
    save(f"steps/{step}.json", json.dumps(RESULTS[-1], indent=1, sort_keys=True))
    return verdict


# --- setup and teardown ---------------------------------------------------

def addresses():
    for h in "abc":
        ADDR[h] = ssh(h, "hostname -I | awk '{print $1}'", check=True)[1].strip()


def setup():
    t_setup = time.time()
    env_lines = []
    for h in "abc":
        rc, _, _ = ssh(h, f"test -e {R}")
        if rc == 0:
            raise RuntimeError(f"{R} exists on host {h}: pick another RUN_ID")
        ssh(h, f"mkdir -p {R}/logs && echo '{MARKER}' > {R}/.owner && echo 'v3h:{RUN_ID.replace('-', '')}@{ADDR['a']}:{FDB_PORT}' > {R}/fdb.cluster",
            check=True)
        _, out, _ = ssh(h, f"uname -r; nproc; free -m | sed -n 2p; df -h /mnt/arca-fdb | tail -1; {BIN} --version; sha256sum {BIN.replace('$HOME', '~')} | cut -c1-64; fdbcli --version | head -1",
                        check=True)
        env_lines.append(f"host {h}:\n{out}")
    save("environment.txt", "\n".join(env_lines))
    subprocess.run(["scp", "-q", "-o", "BatchMode=yes", str(HERE / "three-host-client.py"),
                    f"{HOSTS['c']}:{R}/client.py"], check=True, timeout=60)
    # Private FoundationDB on host A.
    ssh("a", f"mkdir -p {R}/fdb/data {R}/fdb/log", check=True)
    start("a", "fdb", fdb_cmd())
    time.sleep(3)
    ssh("a", f"timeout 60 fdbcli -C {R}/fdb.cluster --exec 'configure new single ssd' --timeout 30", timeout=90)
    for _ in range(60):
        _, out, _ = ssh("a", f"timeout 20 fdbcli -C {R}/fdb.cluster --exec 'status minimal' --timeout 10", timeout=30)
        if "is available" in out:
            break
        time.sleep(1)
    else:
        raise RuntimeError("private FoundationDB did not become available")
    for n in NODES:
        start(node_host(n), f"n-{n}", node_cmd(n))
    time.sleep(3)
    for n in NODES:
        h, i = n[0], int(n[1])
        rc, out, err = admin(f"register-node --address {ADDR[h]}:{NODE_PORTS[i]} --label host={h} "
                             f"--label node={n} --label device_class=hdd")
        if rc != 0:
            raise RuntimeError(f"register {n}: {redact(err)}")
    rc, out, err = admin("create-groups --tier default --device-class hdd --count 2 --k 2 --m 1 "
                         "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain host")
    save("setup/create-groups.txt", out + err)
    if rc != 0:
        raise RuntimeError(f"create-groups: {redact(err)}")
    for cid, h in CONTROLLERS.items():
        start(h, cid, ctl_cmd(cid))
    for g, h in GATEWAYS.items():
        start(h, g, gw_cmd(g))
    # Ready: a bucket, then a probe object written and read through each
    # gateway (admission needs the leader's first passes).
    ready = None
    end = time.time() + 240
    while time.time() < end:
        rc, out, _ = ssh("c", f"{CLIENT} mkbucket {shlex.quote(gw_spec().split(',')[0])}")
        if rc == 0:
            ok = True
            for g, h in GATEWAYS.items():
                u = f"http://{ADDR[h]}:{GW_PORT}/v3h/probe-{g}"
                _, code, _ = ssh("c", f"head -c 2000 /dev/urandom > {R}/probe.bin && "
                                      f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 30 -T {R}/probe.bin {u}; "
                                      f"curl -s -o /dev/null -w ' %{{http_code}}' --max-time 30 -X DELETE {u}")
                ok = ok and code.strip() == "200 204"
            if ok:
                ready = time.time() - t_setup
                break
        time.sleep(5)
    _, cc, cce = admin("check-config")
    save("setup/check-config.txt", cc + cce)
    st = status()
    save("setup/status.txt", st["raw"])
    procs = {f"{h}:{p}": alive(h, p) for h, p in
             [("a", "fdb")] + [(n[0], f"n-{n}") for n in NODES] + [(h, c) for c, h in CONTROLLERS.items()]
             + [(h, g) for g, h in GATEWAYS.items()]}
    ok = ready is not None and all(procs.values())
    return result("0-setup", "PASS" if ok else "FAIL",
                  {"ready_seconds": ready and round(ready, 1), "processes_running": procs,
                   "check_config_tail": redact(cc + cce)[-600:]},
                  "every process running; a 2000-byte PUT and DELETE succeed through both gateways within 240 s")


def teardown():
    if not ADDR:
        addresses()
    for h in "abc":
        rc, out, _ = ssh(h, f"cat {R}/.owner 2>/dev/null")
        if out.strip() != MARKER:
            log(f"host {h}: no marker in {R}, nothing removed")
            continue
        ssh(h, f"for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
               f"grep -qa v3h-{RUN_ID} /proc/$q/cmdline 2>/dev/null && kill -CONT $q 2>/dev/null; "
               f"grep -qa v3h-{RUN_ID} /proc/$q/cmdline 2>/dev/null && kill $q; done; sleep 3; "
               f"for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
               f"grep -qa v3h-{RUN_ID} /proc/$q/cmdline 2>/dev/null && kill -9 $q; done; sleep 1; true", timeout=60)
        # Logs (sanitized later) then the run directory, once nothing of ours runs.
        dest = OUT / "logs" / f"host-{h}"
        dest.mkdir(parents=True, exist_ok=True)
        subprocess.run(f"ssh -o BatchMode=yes {HOSTS[h]} 'cd {R} && tar -cf - logs fdb/log 2>/dev/null' | tar -xf - -C {dest}",
                       shell=True, timeout=300)
        rc, left, _ = ssh(h, f"pgrep -af v3h-{RUN_ID} | grep -v pgrep || true")
        if left.strip():
            log(f"host {h}: processes still running, {R} kept:\n{left}")
            continue
        ssh(h, f"rm -rf {R}", timeout=300)
        _, rest, _ = ssh(h, f"ls -d {R} 2>/dev/null; df -h /mnt/arca-fdb | tail -1")
        log(f"host {h}: removed run directory; {rest.strip()}")
    for f in list((OUT / "logs").rglob("*")):
        if not f.is_file():
            continue
        # FoundationDB trace file names carry the address.
        name = redact(f.name).replace("<", "").replace(">", "")
        if f.suffix not in (".log", ".txt", ".json"):
            name += ".log"
        if name != f.name:
            f.rename(f.with_name(name))


# --- steps ----------------------------------------------------------------

def step1():
    st = status()
    save("step1/status.txt", st["raw"])
    names = {i: n["name"] for i, n in st["nodes"].items()}
    hosts = {i: n["host"] for i, n in st["nodes"].items()}
    groups = {}
    ok = len(st["nodes"]) == 6 and len(st["groups"]) == 4
    schemes = sorted((g["scheme"], g["k"], g["m"]) for g in st["groups"].values())
    ok = ok and schemes == [("replication-v1", 1, 2)] * 2 + [("rs-gf8-v1", 2, 1)] * 2
    for gid, g in st["groups"].items():
        mh = [hosts[m] for m in g["members"]]
        groups[gid] = {"scheme": g["scheme"], "state": g["state"], "members": [names[m] for m in g["members"]],
                       "member_hosts": mh}
        ok = ok and sorted(mh) == ["a", "b", "c"] and g["state"] == "Active"
    # create_groups_from: lowest group count, then node id, per host.
    expect_sets = [{"a0", "b0", "c0"}, {"a1", "b1", "c1"}]
    ok = ok and all(set(g["members"]) in expect_sets for g in groups.values())
    leader = st["leader"]["holder"] if st["leader"] else None
    ok = ok and leader in CONTROLLERS
    _, cc, _ = admin("check-config")
    return result("1-placement", "PASS" if ok else "FAIL",
                  {"nodes": {names[i]: hosts[i] for i in names}, "groups": groups, "leader": leader,
                   "check_config": redact(cc).strip().splitlines()[:12]},
                  "6 nodes; 2 rs-gf8-v1 2+1 and 2 replication-v1 1+2 groups, Active; every group one member per host; "
                  "members {a0,b0,c0} or {a1,b1,c1}; a leader ctl-a or ctl-b")


def step2():
    phase = "s2"
    c = client_sync("load", phase, "--ops 400 --cover --put-rate-mib 0", timeout=1200)
    recs = ledger(phase)
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    puts = {(r["gw"], r["size"]) for r in recs if r["op"] == "PUT" and r["outcome"] == "ack"}
    from importlib import util
    spec = util.spec_from_file_location("client", HERE / "three-host-client.py")
    mod = util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    covered = all((g, s) in puts for g in GATEWAYS for s in mod.SIZES)
    dels = sum(1 for r in recs if r["op"] == "DELETE" and r["outcome"] == "ack")
    v = verify("s2-verify")
    ok = not bad and covered and dels > 0 and v["ok"] and v["absent_ok"] > 0
    return result("2-s3-basic", "PASS" if ok else "FAIL",
                  {"ops": counts(recs), "bad_samples": samples(bad), "every_size_acked_on_both": covered,
                   "deletes_acked": dels, "verify": v, "client": c},
                  "0 errors, 0 violations; every size class acked through both gateways; verify reads every key, "
                  "deleted keys answer 404")


def node_kill_window(phase, victims, absence, extra_expect):
    """Kill storage nodes under load for `absence` seconds, then restart."""
    with Load(phase):
        time.sleep(15)
        # One command per host signals all of its victims at once; the
        # window starts once every victim has been signalled.
        t_kill = max(signal_many(h, [f"n-{v}" for v in victims if node_host(v) == h], "KILL")
                     for h in sorted({node_host(v) for v in victims}))
        for v in victims:
            wait_dead(node_host(v), f"n-{v}", 10)
        mid_metric = None
        time.sleep(absence)
        if extra_expect == "unhealthy":
            mid_metric = ctl_metric("arca_controller_nodes_unhealthy")
            time.sleep(5)
        t_restart = ts("c")
        for v in victims:
            start(node_host(v), f"n-{v}", node_cmd(v))
        time.sleep(25)
    return t_kill, t_restart, mid_metric


def step3a():
    phase = "s3a"
    gm0 = gw_metrics()
    t_kill, t_restart, _ = node_kill_window(phase, ["b0"], 15, None)
    gm1 = gw_metrics()
    recs = ledger(phase)
    absent_window = [r for r in recs if within(r, t_kill, t_restart)]
    bad_absent = [r for r in absent_window if r["outcome"] in BAD]
    inflight = [r for r in recs if r["t0"] < t_kill < r["t1"]]
    after = [r for r in recs if r["t0"] >= t_restart + 10]
    bad_after = [r for r in after if r["outcome"] in BAD]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    puts_acked = sum(1 for r in absent_window if r["op"] == "PUT" and r["outcome"] == "ack")
    gets_ok = sum(1 for r in absent_window if r["op"] == "GET" and r["outcome"] == "ok" and r["status"] == 200)
    q = wait_queue_empty(240)
    v = verify("s3a-verify")
    ok = (not bad_absent and not bad_after and not viol and v["ok"] and puts_acked > 0 and gets_ok > 0
          and q[0] and q[0]["unrecoverable"] == 0 and q[0]["failed"] == 0)
    return result("3a-node-kill", "PASS" if ok else "FAIL",
                  {"absence_s": round(t_restart - t_kill, 1), "ops": counts(recs),
                   "absence_window": counts(absent_window), "errors_in_absence": samples(bad_absent),
                   "in_flight_at_kill": counts(inflight), "errors_after_restart_10s": samples(bad_after),
                   "violations": len(viol), "gateway_metrics_before": gm0, "gateway_metrics_after": gm1,
                   "repair_queue_after": q[0], "verify": v},
                  "while b0 is down: PUTs acked (another group), GETs answered (degraded k of 3), "
                  "0 errors for requests started after the kill and finished before the restart; 0 errors from "
                  "10 s after the restart; repair queue drains with 0 unrecoverable/failed; verify clean")


def step3b():
    phase = "s3b"
    with Load(phase):
        time.sleep(10)
        t_kill = signal("b", "gw-b", "KILL")
        wait_dead("b", "gw-b", 10)
        time.sleep(10)
        t_restart = ts("b")
        start("b", "gw-b", gw_cmd("gw-b"))
        time.sleep(25)
    recs = ledger(phase)
    gwc_bad = [r for r in recs if r["gw"] == "gw-c" and r["outcome"] in BAD]
    gwb_after = [r for r in recs if r["gw"] == "gw-b" and r["t0"] >= t_restart + 5 and r["outcome"] in BAD]
    gwb_down = [r for r in recs if r["gw"] == "gw-b" and within(r, t_kill, t_restart)]
    inflight = [r for r in recs if r["gw"] == "gw-b" and r["t0"] < t_kill < r["t1"]]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    v = verify("s3b-verify")
    ok = not gwc_bad and not gwb_after and not viol and v["ok"]
    return result("3b-gateway-restart", "PASS" if ok else "FAIL",
                  {"down_s": round(t_restart - t_kill, 1), "ops": counts(recs), "gw_b_while_down": counts(gwb_down),
                   "gw_b_in_flight_at_kill": counts(inflight), "gw_c_errors": samples(gwc_bad),
                   "gw_b_errors_from_5s_after_restart": samples(gwb_after), "violations": len(viol), "verify": v},
                  "gw-c: 0 errors throughout; gw-b: requests fail while it is down (refused), 0 errors from 5 s "
                  "after its restart; in-flight requests at the kill are unknown outcomes; verify clean")


def step3c():
    phase = "s3c"
    t_kill, t_restart, _ = node_kill_window(phase, ["b0", "b1"], 20, None)
    recs = ledger(phase)
    win = [r for r in recs if within(r, t_kill, t_restart)]
    get_bad = [r for r in win if r["op"] == "GET" and r["outcome"] in BAD]
    # Any PUT started while both nodes were down must never be acknowledged,
    # whenever it finished (independent review: not only those finished
    # before the restart).
    put_acked = [r for r in recs if r["op"] == "PUT" and t_kill <= r["t0"] < t_restart and r["outcome"] == "ack"]
    put_failed = [r for r in win if r["op"] == "PUT" and r["outcome"] != "ack"]
    after_bad = [r for r in recs if r["t0"] >= t_restart + 10 and r["outcome"] in BAD]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    q = wait_queue_empty(240)
    v = verify("s3c-verify")
    ok = (not get_bad and not put_acked and put_failed and not after_bad and not viol and v["ok"]
          and q[0] and q[0]["unrecoverable"] == 0 and q[0]["failed"] == 0)
    statuses = {}
    for r in put_failed:
        statuses[str(r["status"])] = statuses.get(str(r["status"]), 0) + 1
    return result("3c-host-b-nodes-down", "PASS" if ok else "FAIL",
                  {"down_s": round(t_restart - t_kill, 1), "window": counts(win), "get_errors": samples(get_bad),
                   "put_acked_in_window": samples(put_acked), "put_failure_statuses": statuses,
                   "put_failure_samples": samples(put_failed, 3), "errors_after_restart_10s": samples(after_bad),
                   "violations": len(viol), "repair_queue_after": q[0], "verify": v},
                  "both nodes of host B down: every group lacks one member; GETs answered (k of 3), no PUT started "
                  "while they were down is ever acked (all shards required), PUTs in the window fail (by kind: "
                  "status and error recorded); 0 errors 10 s after restart")


def step4():
    phase = "s4"
    cycles = []
    with Load(phase):
        time.sleep(5)
        for cyc in range(3):
            st = status()
            old = st["leader"]["holder"]
            other = next(c for c in CONTROLLERS if c != old)
            t_kill = signal(CONTROLLERS[old], old, "KILL")
            # Poll the leader record from host C until the other holds it.
            poll = (f"end=$((SECONDS+60)); while [ $SECONDS -lt $end ]; do t=$(date +%s.%N); "
                    f"l=$(env {ENV} {BIN} admin --cluster-file {R}/fdb.cluster status 2>/dev/null | grep '^leader'); "
                    f"echo \"$t $l\"; case \"$l\" in \"leader {other}@\"*) break;; esac; sleep 0.2; done")
            _, out, _ = ssh("c", poll, timeout=120)
            took = None
            for line in out.splitlines():
                parts = line.split()
                if len(parts) > 2 and parts[2].startswith(other + "@"):
                    took = float(parts[0]) - t_kill
                    break
            wait_dead(CONTROLLERS[old], old, 10)
            start(CONTROLLERS[old], old, ctl_cmd(old))
            time.sleep(8)
            st2 = status()
            cycles.append({"killed": old, "new_leader": st2["leader"]["holder"] if st2["leader"] else None,
                           "takeover_s": took and round(took, 2), "restarted_alive": alive(CONTROLLERS[old], old)})
            time.sleep(5)
    recs = ledger(phase)
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    v = verify("s4-verify")
    _, lg, _ = ssh("a", f"grep -E 'acquired leadership|standing by|lost leadership' {R}/logs/ctl-a.log | tail -20")
    _, lgb, _ = ssh("b", f"grep -E 'acquired leadership|standing by|lost leadership' {R}/logs/ctl-b.log | tail -20")
    save("step4/leader-log-lines.txt", "ctl-a:\n" + lg + "\nctl-b:\n" + lgb)
    ok = (all(c["takeover_s"] is not None and c["takeover_s"] <= 20 and c["new_leader"] != c["killed"]
              and c["restarted_alive"] for c in cycles) and not bad and v["ok"])
    return result("4-controller-takeover", "PASS" if ok else "FAIL",
                  {"cycles": cycles, "ops": counts(recs), "bad_samples": samples(bad), "verify": v},
                  "after SIGKILL of the leader the other controller holds the lease within 20 s (lease 10 s + "
                  "poll lease/3); the restarted one stands by; 0 client errors; verify clean")


def step5():
    phase = "s5"
    st0 = status()
    victim = "a1"
    vid = node_id(st0, victim)
    groups_of = [g for g, x in st0["groups"].items() if vid in x["members"]]
    t_kill, t_restart, mid = node_kill_window(phase, [victim], 40, "unhealthy")
    recs = ledger(phase)
    win = [r for r in recs if within(r, t_kill, t_restart)]
    bad_win = [r for r in win if r["outcome"] in BAD]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    after_bad = [r for r in recs if r["t0"] >= t_restart + 10 and r["outcome"] in BAD]
    end = t_restart + 60  # counted from the restart (host clock, NTP-synchronised)
    healthy_after = None
    while time.time() < end:
        _, val = ctl_metric("arca_controller_nodes_unhealthy")
        if val == 0:
            healthy_after = round(time.time() - t_restart, 1)
            break
        time.sleep(3)
    st1 = status()
    same = all(st1["groups"][g]["members"] == st0["groups"][g]["members"] and st1["groups"][g]["state"] == "Active"
               for g in st0["groups"])
    q = wait_queue_empty(300)
    sc = scrub(groups_of)
    v = verify("s5-verify")
    ok = (mid and mid[1] == 1 and healthy_after is not None and same and not bad_win and not after_bad
          and not viol and damage_free(sc) and q[0] and q[0]["unrecoverable"] == 0 and q[0]["failed"] == 0
          and v["ok"])
    return result("5-node-return", "PASS" if ok else "FAIL",
                  {"victim": victim, "down_s": round(t_restart - t_kill, 1),
                   "unhealthy_metric_during": mid, "unhealthy_0_after_s": healthy_after,
                   "groups_unchanged_active": same, "window": counts(win), "errors_in_window": samples(bad_win),
                   "errors_after_restart_10s": samples(after_bad), "violations": len(viol),
                   "repair_queue_after": q[0], "scrub": sc, "verify": v},
                  "node down > --unhealthy-after-secs (20 s): leader counts it unhealthy (metric 1); no member moved "
                  "(no --auto-repair); writes go to the other groups; after return metric 0, groups unchanged, scrub "
                  "of its groups 0 damage findings, repair queue drains, verify clean")


def step6():
    out = {}
    ok_all = True
    for mode in ("SIGSTOP", "SIGKILL"):
        phase = f"s6-{mode.lower()}"
        with Load(phase, "--timeout 90"):
            time.sleep(10)
            t_stop = signal("a", "fdb", "STOP" if mode == "SIGSTOP" else "KILL")
            time.sleep(30)
            t_resume = ts("a")
            if mode == "SIGSTOP":
                signal("a", "fdb", "CONT")
            else:
                wait_dead("a", "fdb", 10)
                start("a", "fdb", fdb_cmd())
            # Ready again: the leader record renewed and a probe PUT through
            # each gateway.
            ready = None
            end = time.time() + 240
            while time.time() < end:
                okp = True
                for g, h in GATEWAYS.items():
                    u = f"http://{ADDR[h]}:{GW_PORT}/v3h/probe6-{g}"
                    _, code, _ = ssh("c", f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 30 -T {R}/probe.bin {u}", timeout=60)
                    okp = okp and code.strip() == "200"
                if okp:
                    ready = ts("c") - t_resume
                    break
                time.sleep(3)
            time.sleep(30)
            t_window_end = ts("c")
        recs = ledger(phase)
        inside = [r for r in recs if within(r, t_stop + 1, t_resume)]
        inside_ok = [r for r in inside if r["outcome"] in ("ack", "ok")]
        straddle = [r for r in recs if r["t0"] < t_resume < r["t1"] or r["t0"] < t_stop < r["t1"]]
        viol = [r for r in recs if r["outcome"] == "VIOLATION"]
        post = [r for r in recs if ready is not None and r["t0"] >= t_resume + ready and r["t1"] <= t_window_end]
        post_bad = [r for r in post if r["outcome"] in BAD]
        procs = {f"{h}:{p}": alive(h, p) for h, p in
                 [(n[0], f"n-{n}") for n in NODES] + [(h, c) for c, h in CONTROLLERS.items()]
                 + [(h, g) for g, h in GATEWAYS.items()]}
        st = status()
        unknown_ops = [r for r in recs if r["op"] in ("PUT", "DELETE") and r["outcome"] in ("unknown", "error")]
        v = verify(f"{phase}-verify", collapse=True)
        ok = (not viol and ready is not None and not post_bad and all(procs.values()) and st["leader"] is not None
              and v["ok"])
        ok_all = ok_all and ok
        status_counts = {}
        for r in inside:
            k = f"{r['op']} {r['outcome']} {r['status']}"
            status_counts[k] = status_counts.get(k, 0) + 1
        lat = sorted(r["t1"] - r["t0"] for r in inside if r["outcome"] in BAD)
        out[mode] = {"outage_s": round(t_resume - t_stop, 1), "ready_after_resume_s": ready and round(ready, 1),
                     "inside_outage": status_counts, "inside_outage_successes": samples(inside_ok),
                     "failed_latency_s_min_median_max": [round(lat[0], 1), round(lat[len(lat) // 2], 1), round(lat[-1], 1)] if lat else None,
                     "straddling": counts(straddle), "violations": len(viol), "violation_samples": samples(viol),
                     "errors_in_30s_after_ready": samples(post_bad), "post_ready_ops": len(post),
                     "processes_alive": procs, "leader_after": st["leader"] and st["leader"]["holder"],
                     "indeterminate_writes": len(unknown_ops), "verify": v, "ok": ok}
    return result("6-fdb-unavailable", "PASS" if ok_all else "FAIL", out,
                  "requests inside the outage answer 5xx (10 s transaction timeout) or time out; no wrong bytes; "
                  "every acked PUT reads back; every indeterminate op reads as one of its candidates; within 240 s "
                  "of the restore PUTs succeed through both gateways, then 30 s with 0 errors; every process alive")


def live_keys():
    """Keys whose only candidate is a body, with its size (client state)."""
    _, out, _ = ssh("c", f"cat {R}/state.json", check=True)
    keys = json.loads(out)["keys"]
    return sorted((k, c[0][1]) for k, c in keys.items() if len(c) == 1 and c[0] != "absent")


def step7a():
    # Shards of objects that are current (not of overwritten or deleted
    # ones, which the node retains for the reclaim period): chunk 0 of a
    # few live keys, located from their generation (`verify-object`) and
    # the group of c1's member that holds it.
    st = status()
    victim = "c1"
    vid = node_id(st, victim)
    groups_of = {g: x["members"].index(vid) for g, x in st["groups"].items() if vid in x["members"]}
    ec = [g for g in groups_of if st["groups"][g]["scheme"] == "rs-gf8-v1"]
    rep = [g for g in groups_of if st["groups"][g]["scheme"] == "replication-v1"]
    # Only keys last written while every node was up (steps 2 and 4): a
    # write diverted from a group with a member down can leave a stray copy
    # of its chunk there, which is not the object's (run rc2).
    last_put = {}
    for r in ledger():
        if r["op"] == "PUT":
            last_put[r["key"]] = r
    safe = {k for k, r in last_put.items() if r["phase"] in ("s2", "s4") and r["outcome"] == "ack"}
    live = [(k, z) for k, z in live_keys() if k in safe]
    cands = [(k, z) for k, z in live if z > 65536][:12] + [(k, z) for k, z in live if z <= 65536][:12]
    gens = {}
    for k, z in cands:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {shlex.quote(k)}", timeout=120)
        m = re.search(r"generation ([0-9a-f]{32})", o)
        if m:
            gens[k] = (z, m.group(1))
    signal("c", f"n-{victim}", "TERM")
    wait_dead("c", f"n-{victim}", 30)
    corrupted = []
    want = {"ec": 2, "rep": 1}
    for k, (z, gen) in gens.items():
        kind = "ec" if z > 65536 else "rep"
        if want[kind] == 0:
            continue
        chunk = gen[:24] + "00000000"
        for g in (ec if kind == "ec" else rep):
            rc, o, e = ssh("c", f"env {ENV} {BIN} storage-node corrupt-shard --dir {R}/n-{victim} "
                                f"--group {g} --chunk {chunk} --index {groups_of[g]}", timeout=120)
            if rc == 0:
                corrupted.append({"key": k, "size": z, "group": g, "index": groups_of[g],
                                  "detail": redact(o + e).strip()[-300:]})
                want[kind] -= 1
                break
    start("c", f"n-{victim}", node_cmd(victim))
    time.sleep(10)
    # Each flipped shard must be the object's own: verify-object (read-only)
    # lists the chunk, in that group, as affected.
    for c in corrupted:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {shlex.quote(c['key'])}", timeout=300)
        c["referenced"] = bool(re.search(rf"^  part \S+ chunk 0 id \S+ group {c['group']} good ", o, re.M))
    sc1 = scrub(sorted(groups_of))
    found = sum(v for r in sc1.values() for c, v in r.get("findings", {}).items() if c != "UnexpectedShard")
    q = wait_queue_empty(300)
    sc2 = scrub(sorted(groups_of))
    objs = {}
    for c in corrupted:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {shlex.quote(c['key'])} --decode", timeout=300)
        objs[c["key"]] = {"rc": rc, "out": redact(o).strip().splitlines()[:4]}
    v = verify("s7a-verify")
    _, nlog, _ = ssh("c", f"grep -E 'damaged|quarantin' {R}/logs/n-{victim}.log | tail -5")
    ok = (len(corrupted) == 3 and all(c.get("referenced") for c in corrupted)
          and found >= len(corrupted) and q[0] and q[0]["depth"] == 0
          and q[0]["unrecoverable"] == 0 and q[0]["failed"] == 0 and damage_free(sc2)
          and all(o["rc"] == 0 for o in objs.values()) and v["ok"])
    return result("7a-scrub-repair-corrupt", "PASS" if ok else "FAIL",
                  {"victim": victim, "corrupted": corrupted, "node_log": redact(nlog).strip().splitlines(),
                   "scrub_before_repair": sc1, "damage_findings_before_repair": found, "repair_queue": q[0],
                   "scrub_after_repair": sc2, "verify_object_after": objs, "verify": v},
                  "one byte flipped in chunk 0 of 2 live EC objects and 1 live replicated object on c1's member "
                  "(keys last written while every node was up; verify-object shows each flipped chunk affected); "
                  "scrub reports at least one damage finding per flipped shard; the repair queue empties within "
                  "300 s; a second scrub finds no damage; verify-object --decode exits 0 for each; verify clean")


def step7b():
    st0 = status()
    victim, spare = "b1", "b0"
    vid, sid = node_id(st0, victim), node_id(st0, spare)
    groups_of = {g: x["members"].index(vid) for g, x in st0["groups"].items() if vid in x["members"]}
    signal("b", f"n-{victim}", "TERM")
    wait_dead("b", f"n-{victim}", 30)
    ssh("b", f"test -f {R}/.owner && rm -rf {R}/n-{victim}", check=True)
    start("b", f"n-{victim}", node_cmd(victim))
    quarantined_after = None
    t0 = time.time()
    nl = ""
    while time.time() - t0 < 90:
        # Lifecycle states: `admin node list` (`admin status` prints only
        # membership records).
        _, nl, _ = admin("node list")
        row = next((l.split() for l in nl.splitlines() if l.split()[:1] == [str(vid)]), [])
        if len(row) > 2 and row[2].lower().startswith("quarantin"):
            quarantined_after = round(time.time() - t0, 1)
            break
        time.sleep(3)
    save("step7b/node-list-after-wipe.txt", nl)
    save("step7b/status-after-wipe.txt", status()["raw"])
    # Reads while its shards are gone.
    v_deg = verify("s7b-degraded")
    replace = {}
    for g, idx in groups_of.items():
        rc, o, e = admin(f"replace-member --group {g} --index {idx} --yes", timeout=120)
        replace[g] = {"rc": rc, "out": redact(o + e).strip()[-400:]}
    done_after = None
    t1 = time.time()
    while time.time() - t1 < 900:
        st = status()
        if all(st["groups"][g]["state"] == "Active" and vid not in st["groups"][g]["members"] for g in groups_of):
            done_after = round(time.time() - t1, 1)
            break
        time.sleep(5)
    save("step7b/status-after-replace.txt", st["raw"])
    members = {g: [st["nodes"][m]["name"] for m in st["groups"][g]["members"]] for g in groups_of}
    hosts_ok = all(sorted(st["nodes"][m]["host"] for m in st["groups"][g]["members"]) == ["a", "b", "c"]
                   for g in groups_of)
    target_ok = all(sid in st["groups"][g]["members"] for g in groups_of)
    sc = scrub(sorted(st["groups"]))
    v = verify("s7b-verify")
    ok = (quarantined_after is not None and v_deg["ok"] and done_after is not None and hosts_ok and target_ok
          and damage_free(sc) and v["ok"])
    return result("7b-wipe-replace", "PASS" if ok else "FAIL",
                  {"victim": victim, "quarantined_after_s": quarantined_after, "verify_while_wiped": v_deg,
                   "replace": replace, "replaced_after_s": done_after, "members_after": members,
                   "one_member_per_host": hosts_ok, "target_is_b0": target_ok, "scrub_all": sc, "verify": v},
                  "the wiped node restarts without its identity and is quarantined (\"emptied or replaced disk\"); "
                  "GETs still answered (k of 3); replace-member rebuilds each of its groups onto b0 (the only "
                  "healthy non-member in domain b); groups Active, one member per host; scrub 0 damage; verify clean")


def step7c():
    st = status()
    sc = scrub(sorted(st["groups"]))
    q, qout = integrity_status()
    rc, vm, vme = admin("integrity verify-manifests", timeout=300)
    v = verify("s7c-final")
    recs = ledger()
    acked = sum(1 for r in recs if r["op"] == "PUT" and r["outcome"] == "ack")
    allv = sum(1 for r in recs if r["outcome"] == "VIOLATION")
    _, ps, _ = ssh("c", f"python3 -c \"import json;d=json.load(open('{R}/state.json'));print(d['put_bytes'])\"")
    ok = damage_free(sc) and q and q["unrecoverable"] == 0 and rc == 0 and v["ok"] and allv == 0
    return result("7c-final", "PASS" if ok else "FAIL",
                  {"scrub_all": sc, "integrity_status": q, "verify_manifests": redact(vm + vme).strip()[-400:],
                   "verify": v, "ledger_ops": len(recs), "puts_acked_total": acked, "violations_total": allv,
                   "logical_put_bytes": int(ps.strip() or 0), "groups": st["groups"]},
                  "scrub of every group 0 damage findings (UnexpectedShard reported, not judged), no unrecoverable "
                  "task, verify-manifests clean, every key reads back as its candidate, 0 violations in the ledger")


STEPS = [("1-placement", step1), ("2-s3-basic", step2), ("3a-node-kill", step3a),
         ("3b-gateway-restart", step3b), ("3c-host-b-nodes-down", step3c), ("4-controller-takeover", step4),
         ("5-node-return", step5), ("6-fdb-unavailable", step6), ("7a-scrub-repair-corrupt", step7a),
         ("7b-wipe-replace", step7b), ("7c-final", step7c)]


def write_md(meta):
    lines = [f"# three-host validation run {RUN_ID}", "", f"source {meta.get('sha')}", ""]
    for r in RESULTS:
        lines += [f"## {r['step']}: {r['verdict']}", "", f"expected: {r['expected']}", "", "```json",
                  json.dumps(r["evidence"], indent=1, sort_keys=True)[:20000], "```", ""]
    save("results.md", "\n".join(lines))


def run():
    OUT.mkdir(parents=True, exist_ok=True)
    meta = {"run_id": RUN_ID, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "sha": os.environ.get("SOURCE_SHA", "")}
    addresses()
    stopped = False
    try:
        try:
            v = setup()
        except Exception as e:  # noqa: BLE001
            v = result("0-setup", "FAIL", {"exception": redact(str(e))})
        if v != "PASS":
            stopped = True
        only = os.environ.get("STOP_AFTER")
        for name, fn in STEPS:
            if only and STEP_ORDER.index(name) > STEP_ORDER.index(only):
                result(name, "NOT RUN", {"reason": f"STOP_AFTER={only} (script debugging)"})
                continue
            if stopped:
                result(name, "NOT RUN", {"reason": "an earlier step failed"})
                continue
            log(f"step {name}")
            try:
                v = fn()
            except Exception as e:  # noqa: BLE001
                v = result(name, "FAIL", {"exception": redact(repr(e))[-1500:]})
            if v != "PASS":
                stopped = True
        # Evidence: the ledger and the final status.
        try:
            subprocess.run(f"ssh -o BatchMode=yes {HOSTS['c']} 'cat {R}/ledger.jsonl' > {OUT}/ledger.jsonl",
                           shell=True, timeout=300)
            save("final-status.txt", status()["raw"])
        except Exception as e:  # noqa: BLE001
            log(f"evidence: {e}")
    finally:
        teardown()
        meta["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        save("meta.json", json.dumps(meta, indent=1))
        write_md(meta)
        subprocess.run([sys.executable, str(HERE / "sanitize.py"), str(OUT)])
    for r in RESULTS:
        print(f"{r['step']}: {r['verdict']}")


def main():
    if not all(HOSTS.values()):
        sys.exit("HOST_A, HOST_B and HOST_C are required")
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "run":
        run()
    elif cmd == "teardown":
        OUT.mkdir(parents=True, exist_ok=True)
        teardown()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
