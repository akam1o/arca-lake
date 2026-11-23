#!/usr/bin/env python3
"""Functional validation of arca-lake on three Linux hosts (0.2.0). Plan,
expected behaviour and pass/fail rules:
docs/verification/0.2.0-three-host-validation.md (the 0.1.0 plan it extends:
docs/verification/0.1.0-three-host-validation.md).

Layout: host A runs a private FoundationDB process (its own port and data
directory, TLS, never the system service), controller ctl-a and storage
nodes a0, a1; host B controller ctl-b, nodes b0, b1 and gateway gw-b; host C
nodes c0, c1, gateway gw-c and the S3 client (three-host-client.py). Node
labels host=a|b|c; groups use --failure-domain host. Mutual TLS for node
RPCs and FoundationDB TLS from one CA per cluster (`arca-lake tls`); S3
requests SigV4-signed with keys from a credentials file.

  HOST_A=user@a HOST_B=user@b HOST_C=user@c [RUN_ID=id] [OUT=dir] \\
    benchmarks/linux/three-host-validate.py run|upgrade
  HOST_A=... HOST_B=... HOST_C=... RUN_ID=id three-host-validate.py teardown

`run` is the main scenario (steps 0 to 10), `upgrade` the upgrade from the
published 0.1.0 (steps U0 to U9). Every host needs, under ROOT (default
/mnt/arca-fdb/v020-v3h, marker file .owner), bin/arca-lake (the fdb build
under test) and for `upgrade` bin/arca-lake-0.1.0 (the release binary),
Python 3; host A and B fdbserver and fdbcli; host C botocore. Everything
runs under ROOT/run-<run id>/ on every host (marker file .owner); only
processes whose pid file is there and whose command line names that
directory are signalled. Host addresses come only from the environment and
are replaced by <host-a|b|c> in every output; the output directory is passed
through sanitize.py at the end. Secrets (S3 keys, TLS keys) never leave the
hosts' run directories.

Each step is judged PASS, FAIL or NOT RUN with its evidence in
OUT/results.json and OUT/results.md; a FAIL stops the sequence (the later
steps are NOT RUN) and the cluster is torn down.
"""

import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
HOSTS = {h: os.environ.get(f"HOST_{h.upper()}", "") for h in "abc"}
RUN_ID = os.environ.get("RUN_ID") or time.strftime("%Y%m%d-%H%M%S")
ROOT = os.environ.get("ROOT", "/mnt/arca-fdb/v020-v3h")
R = f"{ROOT}/run-{RUN_ID}"
TAG = f"{ROOT}/run-{RUN_ID}/"
BIN = f"{ROOT}/bin/arca-lake"
BIN_V010 = f"{ROOT}/bin/arca-lake-0.1.0"
OUT = Path(os.environ.get("OUT", str(HERE / "results" / f"v3h020-{RUN_ID}")))
MARKER = "0.2.0 three-host validation"
TD = "v3h.arca-lake.invalid"
FDB_PORT = 4610
FDB2_PORTS = {"a": 4620, "b": 4621}  # second clusters of steps 8a and 8b (on host B)
NODE_PORTS = {0: 9611, 1: 9612, 2: 9613}
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
IDS = {}  # node name -> node id
RESULTS = []
SECRETS = []
# Binary per process (the upgrade scenario switches them); admin's too.
BINS = {}
ADMIN = {"bin": BIN}
MAIN_STEPS = ["0-setup", "1-placement", "1b-schema5-domains", "1c-transport-security", "2-s3-basic",
              "2b-sigv4-ownership", "2c-multipart", "3a-node-kill", "3b-gateway-restart",
              "3c-host-b-nodes-down", "4-controller-takeover", "5-node-return", "6-fdb-unavailable",
              "7a-scrub-repair-corrupt", "7b-wipe-replace", "7c-final", "8a-r117-other-ca",
              "8b-r117-same-ca", "8c-new-node-joins", "9-scrub-classes", "10-final"]
UPGRADE_STEPS = ["U0-setup-0.1.0", "U1-data-0.1.0", "U2-storage-nodes", "U3-controllers",
                 "U4-record-failure-domains", "U5-gateways", "U6-rollback-before-finalize",
                 "U7-finalize", "U8-after-finalize", "U9-final"]
STEP_ORDER = MAIN_STEPS + UPGRADE_STEPS


# --- plumbing -------------------------------------------------------------

def redact(text):
    for h, a in ADDR.items():
        if a:
            text = text.replace(a, f"<host-{h}>")
    for h, t in HOSTS.items():
        if t:
            text = text.replace(t, f"<host-{h}>")
    for s in SECRETS:
        text = text.replace(s, "<secret>")
    return text


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(redact(line), flush=True)
    with open(OUT / "driver.log", "a") as f:
        f.write(redact(line) + "\n")


def ssh(h, cmd, timeout=120, check=False, input=None):
    try:
        p = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
             HOSTS[h], cmd], capture_output=True, text=True, timeout=timeout, input=input)
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


def q(s):
    return shlex.quote(s)


# TLS settings per process (the certificate directory R/pki/<cert>: cert.pem,
# key.pem, ca.pem). Node RPC settings for every process; FoundationDB client
# settings for everything but storage nodes.
def node_tls(cert):
    d = f"{R}/pki/{cert}"
    return f"ARCA_TLS_CA={d}/ca.pem ARCA_TLS_CERT={d}/cert.pem ARCA_TLS_KEY={d}/key.pem ARCA_TRUST_DOMAIN={TD}"


def fdb_tls(cert="admin"):
    d = f"{R}/pki/{cert}"
    return (f"ARCA_FDB_TLS_CA={d}/ca.pem ARCA_FDB_TLS_CERT={d}/cert.pem ARCA_FDB_TLS_KEY={d}/key.pem "
            f"ARCA_FDB_TLS_VERIFY_PEERS={q(f'Check.Valid=1,S.subjectAltName>=URI:spiffe://{TD}/fdb/')}")


def proc_env(name):
    if name.startswith("n-") or name.startswith("imp-"):
        return node_tls(name)
    if name == "fdb" or name.startswith("fdb2"):
        return ""
    return f"{node_tls(name)} {fdb_tls(name)}"


def start(h, name, cmd, env=None):
    env = proc_env(name) if env is None else env
    ssh(h, f"cd {R} && {{ echo \"=== start $(date -u +%FT%TZ): {cmd.split()[0].rsplit('/', 1)[-1]}\" >> {R}/logs/{name}.log; "
           f"nohup env {env} {cmd} >> {R}/logs/{name}.log 2>&1 < /dev/null & echo $! > {R}/{name}.pid; }}",
        check=True)


def signal(h, name, sig):
    """Signal a process this run started (pid file and command line both
    under the run directory). Returns the host's time just before the
    signal."""
    rc, out, err = ssh(h, f"p=$(cat {R}/{name}.pid) && grep -qa {TAG} /proc/$p/cmdline && "
                          f"date +%s.%N && kill -{sig} $p", check=False)
    if rc != 0:
        raise RuntimeError(f"cannot signal {name} on host {h}: {redact(err)}")
    return float(out.split()[0])


def signal_many(h, names, sig):
    """Like `signal` for several processes of one host in one command;
    returns the host's time just after the last signal."""
    checks = " && ".join(f"p{i}=$(cat {R}/{n}.pid) && grep -qa {TAG} /proc/$p{i}/cmdline"
                         for i, n in enumerate(names))
    pids = " ".join(f"$p{i}" for i in range(len(names)))
    rc, out, err = ssh(h, f"{checks} && kill -{sig} {pids} && date +%s.%N")
    if rc != 0:
        raise RuntimeError(f"cannot signal {names} on host {h}: {redact(err)}")
    return float(out.split()[0])


def alive(h, name):
    rc, _, _ = ssh(h, f"p=$(cat {R}/{name}.pid 2>/dev/null) && grep -qa {TAG} /proc/$p/cmdline")
    return rc == 0


def wait_dead(h, name, secs=30):
    end = time.time() + secs
    while time.time() < end:
        if not alive(h, name):
            return True
        time.sleep(0.5)
    return False


def stop(h, name, secs=30):
    """SIGTERM, wait; SIGKILL if still running."""
    if alive(h, name):
        signal(h, name, "TERM")
        if not wait_dead(h, name, secs):
            signal(h, name, "KILL")
            wait_dead(h, name, 10)


def admin(args, timeout=60, h="a", env=None, cluster_file=None, bin=None):
    env = f"{node_tls('admin')} {fdb_tls('admin')}" if env is None else env
    cf = cluster_file or f"{R}/fdb.cluster"
    rc, out, err = ssh(h, f"env {env} timeout {timeout} {bin or ADMIN['bin']} admin --cluster-file {cf} {args}",
                       timeout=timeout + 30)
    return rc, out, err


def fdbcli(cmd, timeout=30):
    d = f"{R}/pki/admin"
    return ssh("a", f"timeout {timeout + 10} fdbcli -C {R}/fdb.cluster --tls-certificate-file {d}/cert.pem "
                    f"--tls-key-file {d}/key.pem --tls-ca-file {d}/ca.pem --exec {q(cmd)} --timeout {timeout}",
               timeout=timeout + 30)


def bin_of(name):
    return BINS.get(name, BIN)


def node_cmd(n, capacity=CAPACITY):
    h, i = n[0], int(n[1])
    return (f"{bin_of('n-' + n)} storage-node --listen {ADDR[h]}:{NODE_PORTS[i]} --dir {R}/n-{n} "
            f"--metrics-listen 127.0.0.1:{9621 + i} --capacity-bytes {capacity} --segment-max-bytes {SEGMENT}")


def ctl_cmd(cid, http=CTL_HTTP, cluster_file=None):
    return (f"{bin_of(cid)} controller --cluster-file {cluster_file or R + '/fdb.cluster'} --id {cid} "
            f"--http {http} --failure-domain host --unhealthy-after-secs 20 --health-interval-secs 5 "
            "--gc-interval-secs 10 --gc-grace-secs 30 --min-orphan-age-secs 90 --reconcile-interval-secs 30 "
            "--admission-horizon-secs 60 --admission-dwell-secs 10 --scrub-interval-secs 0")


def gw_cmd(g, listen=None, metrics=GW_METRICS):
    h = GATEWAYS.get(g, "c")
    return (f"{bin_of(g)} gateway --backend cluster --cluster-file {R}/fdb.cluster "
            f"--credentials-file {R}/creds.toml --listen {listen or ADDR[h] + ':' + str(GW_PORT)} "
            f"--metrics-listen {metrics} --k 2 --m 1 "
            "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain host --lease-secs 60")


def fdb_cmd():
    d = f"{R}/pki/fdb"
    return (f"fdbserver -p {ADDR['a']}:{FDB_PORT}:tls -C {R}/fdb.cluster -d {R}/fdb/data -L {R}/fdb/log "
            f"--tls-certificate-file {d}/cert.pem --tls-key-file {d}/key.pem --tls-ca-file {d}/ca.pem "
            "--memory 2GiB --cache-memory 256MiB")


def gw_spec():
    return ",".join(f"{g}={ADDR[h]}:{GW_PORT}" for g, h in GATEWAYS.items())


# --- PKI and credentials --------------------------------------------------

def tls_issue(ca, role, name, cert):
    """Issue on host A into R/pki/<cert> from CA directory R/<ca>."""
    ssh("a", f"{BIN} tls issue --ca-dir {R}/{ca} --trust-domain {TD} --role {role} --name {name} "
             f"--out {R}/pki/{cert} --days 7 > /dev/null", check=True)


def push_pki():
    """Copy R/pki (certificates and their keys, not the CA keys) from host A
    to hosts B and C."""
    for h in "bc":
        p = subprocess.run(f"ssh -o BatchMode=yes {HOSTS['a']} 'tar -cf - -C {R} pki' | "
                           f"ssh -o BatchMode=yes {HOSTS[h]} 'tar -xf - -C {R}'", shell=True, timeout=120)
        if p.returncode != 0:
            raise RuntimeError(f"pki copy to host {h} failed")


def write_credentials():
    """Two principals with one key each: the TOML file on the gateway hosts,
    the JSON file for the client on host C. Generated here, never saved."""
    keys = {}
    for p in ("owner", "other"):
        ak = f"AKIAV3H{p.upper()}{secrets.token_hex(4).upper()}"
        sk = secrets.token_hex(20)
        SECRETS.append(sk)
        keys[p] = [ak, sk]
    toml = "".join(f'[[principal]]\nname = "{p}"\n[[principal.key]]\nid = "{ak}"\nsecret = "{sk}"\n\n'
                   for p, (ak, sk) in keys.items())
    for h in "bc":
        ssh(h, f"umask 077 && cat > {R}/creds.toml", check=True, input=toml)
    ssh("c", f"umask 077 && cat > {R}/creds.json", check=True, input=json.dumps(keys))


# --- status parsing -------------------------------------------------------

def status():
    rc, out, err = admin("status", timeout=60)
    if rc != 0:
        raise RuntimeError(f"admin status failed: {redact(err)[-300:]}")
    nodes, groups, leader, life, domains = {}, {}, None, {}, {}
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
        m = re.match(r"^failure_domain group (\d+) label (\S+) source (\S+)", line)
        if m:
            domains[int(m.group(1))] = {"label": m.group(2), "source": m.group(3)}
            continue
        m = re.match(r"^leader (\S+) token (\d+) lease_until_ms (\d+)", line)
        if m:
            leader = {"holder": m.group(1).split("@")[0], "token": int(m.group(2)),
                      "lease_until_ms": int(m.group(3))}
            continue
        m = re.match(r"^node (\d+) lifecycle (\w+)", line)
        if m:
            life[int(m.group(1))] = m.group(2)
    return {"nodes": nodes, "groups": groups, "leader": leader, "lifecycle": life, "domains": domains, "raw": out}


def node_id(st, name):
    return next(i for i, n in st["nodes"].items() if n["name"] == name)


def node_state(nid):
    _, nl, _ = admin("node list")
    row = next((l.split() for l in nl.splitlines() if l.split()[:1] == [str(nid)]), [])
    return (row[2] if len(row) > 2 else None), nl


def node_status(nid, **kw):
    rc, out, err = admin(f"node status --node {nid}", **kw)
    return redact(out + err).strip()


def ctl_metric(name):
    st = status()
    if not st["leader"]:
        return None, None
    h = CONTROLLERS.get(st["leader"]["holder"])
    rc, out, _ = ssh(h, f"curl -s --max-time 5 http://{CTL_HTTP}/metrics | grep -E '^{name}[ {{]'")
    vals = [float(l.split()[-1]) for l in out.splitlines() if l.strip()]
    return st["leader"]["holder"], (sum(vals) if vals else None)


def node_metric(h, port, name):
    _, out, _ = ssh(h, f"curl -s --max-time 5 http://127.0.0.1:{port}/metrics | grep -E '^{name}[ {{]'")
    vals = [float(l.split()[-1]) for l in out.splitlines() if l.strip()]
    return sum(vals) if vals else None


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
        q_, out = integrity_status()
        last = (q_, out)
        if q_ and q_["depth"] == 0:
            return q_, out
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
        # 0.2.0 binaries also print the shards held legitimately or not
        # classified (the latter are counted in errors too).
        h = re.search(r"retained (\d+) retained_damaged (\d+) in_flight (\d+) unclassified (\d+)", out)
        if h:
            res[g]["held"] = dict(zip(("retained", "retained_damaged", "in_flight", "unclassified"),
                                      (int(x) for x in h.groups())))
    return res


def damage_free(scrubbed, ignore=("UnexpectedShard",)):
    for r in scrubbed.values():
        if "findings" not in r or r["errors"]:
            return False
        if any(v for k, v in r["findings"].items() if k not in ignore):
            return False
    return True


def group_check():
    rc, out, err = admin("group check")
    return rc, redact(out + err).strip()


# --- client ---------------------------------------------------------------

CLIENT = f"python3 {R}/client.py"


def client_sync(cmd, phase, extra="", timeout=900):
    rc, out, err = ssh("c", f"cd {R} && {CLIENT} {cmd} {R}/state.json {R}/ledger.jsonl {phase} "
                            f"{q(gw_spec())} --creds {R}/creds.json {extra}", timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"client {cmd} {phase} rc {rc}: {redact(err)[-800:]}")
    return json.loads(out.strip().splitlines()[-1])


def probe(key, spec=None):
    _, out, err = ssh("c", f"{CLIENT} probe {q(spec or gw_spec())} {key} --creds {R}/creds.json", timeout=120)
    try:
        return json.loads(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"error": redact(err)[-300:]}


class Load:
    def __init__(self, phase, extra=""):
        self.phase, self.extra = phase, extra

    def __enter__(self):
        ssh("c", f"rm -f {R}/stop-{self.phase}", check=True)
        ssh("c", f"cd {R} && {{ nohup {CLIENT} load {R}/state.json {R}/ledger.jsonl {self.phase} "
                 f"{q(gw_spec())} --creds {R}/creds.json --duration 1800 --pause-ms 25 "
                 f"--stop-file {R}/stop-{self.phase} {self.extra} "
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


def chunk_groups(key):
    """Groups of the current generation's chunks of `key` that are not
    whole (`integrity verify-object` prints only those); None if the object
    line is missing (not found or not read)."""
    rc, o, e = admin(f"integrity verify-object --bucket v3h --key {q(key)}", timeout=120)
    if not re.search(rf"^object v3h/{re.escape(key)} .* verdict \w+", o, re.M):
        return None
    return {int(g) for g in re.findall(r"^  part \S+ chunk \d+ id \S+ group (\d+) ", o, re.M)}


def acked_in(recs, t0, t1):
    """Keys whose last write in the ledger is a PUT/MPU acked that started
    in [t0, t1)."""
    last = {}
    for r in recs:
        if r["op"] in ("PUT", "DELETE", "MPU"):
            last[r["key"]] = r
    return sorted(k for k, r in last.items() if r["op"] in ("PUT", "MPU") and r["outcome"] == "ack"
                  and t0 <= r["t0"] < t1)


def no_chunk_on(keys, groups_of_node):
    """Run while the node of `groups_of_node` is stopped: a chunk counted
    written in one of its groups is then not whole and printed. For every
    key: the groups of its chunks that are not whole; the keys with such a
    chunk in one of `groups_of_node`, and those that could not be read."""
    placed, bad, unread = {}, [], []
    for k in keys:
        gs = chunk_groups(k)
        if gs is None:
            unread.append(k)
            continue
        placed[k] = sorted(gs)
        if gs & set(groups_of_node):
            bad.append(k)
    return placed, bad, unread


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


def all_procs():
    return ([("a", "fdb")] + [(n[0], f"n-{n}") for n in NODES] + [(h, c) for c, h in CONTROLLERS.items()]
            + [(h, g) for g, h in GATEWAYS.items()])


def setup(step="0-setup", old=False):
    """The cluster, every process on 0.1.0 when `old`."""
    t_setup = time.time()
    if old:
        for n in NODES:
            BINS[f"n-{n}"] = BIN_V010
        for p in list(CONTROLLERS) + list(GATEWAYS):
            BINS[p] = BIN_V010
        ADMIN["bin"] = BIN_V010
    env_lines = []
    for h in "abc":
        rc, out, _ = ssh(h, f"cat {ROOT}/.owner 2>/dev/null; test -e {R}")
        if not out.startswith("0.2.0 v3h"):
            raise RuntimeError(f"{ROOT} on host {h} has no owner marker")
        if rc == 0:
            raise RuntimeError(f"{R} exists on host {h}: pick another RUN_ID")
        ssh(h, f"mkdir -p {R}/logs && echo '{MARKER}' > {R}/.owner && "
               f"echo 'v3h:{RUN_ID.replace('-', '')}@{ADDR['a']}:{FDB_PORT}:tls' > {R}/fdb.cluster", check=True)
        _, out, _ = ssh(h, f"uname -r; nproc; free -m | sed -n 2p; df -h / /mnt/arca-fdb | tail -2; "
                           f"{BIN} --version; sha256sum {BIN} {BIN_V010} | cut -c1-64; "
                           "fdbcli --version 2>/dev/null | head -1; "
                           "python3 -c 'import botocore; print(\"botocore\", botocore.__version__)' 2>&1 | tail -1",
                        check=True)
        env_lines.append(f"host {h}:\n{out}")
    save("environment.txt", "\n".join(env_lines))
    subprocess.run(["scp", "-q", "-o", "BatchMode=yes", str(HERE / "three-host-client.py"),
                    f"{HOSTS['c']}:{R}/client.py"], check=True, timeout=60)
    # PKI on host A: the cluster's CA and another CA (steps 1c, 8a).
    ssh("a", f"mkdir -p {R}/pki && {BIN} tls init-ca --dir {R}/ca-main --name 'v3h main CA' --days 7 > /dev/null && "
             f"{BIN} tls init-ca --dir {R}/ca-other --name 'v3h other CA' --days 7 > /dev/null", check=True)
    tls_issue("ca-main", "fdb", "1", "fdb")
    tls_issue("ca-main", "admin", "admin", "admin")
    tls_issue("ca-other", "admin", "admin", "admin-other")
    for p in list(CONTROLLERS) + list(GATEWAYS) + ["ctl-old", "gw-old"]:
        tls_issue("ca-main", "controller" if p.startswith("ctl") else "gateway", p, p)
    # Private FoundationDB on host A, TLS.
    ssh("a", f"mkdir -p {R}/fdb/data {R}/fdb/log", check=True)
    start("a", "fdb", fdb_cmd())
    time.sleep(3)
    fdbcli("configure new single ssd")
    for _ in range(60):
        _, out, _ = fdbcli("status minimal", 10)
        if "is available" in out:
            break
        time.sleep(1)
    else:
        raise RuntimeError("private FoundationDB did not become available")
    rc, out, err = admin("init")
    save("setup/init.txt", out + err)
    if rc != 0:
        raise RuntimeError(f"admin init: {redact(err)[-300:]}")
    # Nodes are registered first: their certificates carry their ids.
    for n in NODES:
        h, i = n[0], int(n[1])
        rc, out, err = admin(f"register-node --address {ADDR[h]}:{NODE_PORTS[i]} --label host={h} "
                             f"--label node={n} --label device_class=hdd")
        m = re.search(r"node (\d+) registered", out)
        if rc != 0 or not m:
            raise RuntimeError(f"register {n}: {redact(out + err)[-300:]}")
        IDS[n] = int(m.group(1))
        tls_issue("ca-main", "node", str(IDS[n]), f"n-{n}")
    push_pki()
    write_credentials()
    for n in NODES:
        start(node_host(n), f"n-{n}", node_cmd(n))
    time.sleep(3)
    rc, out, err = admin("create-groups --tier default --device-class hdd --count 2 --k 2 --m 1 "
                         "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain host")
    save("setup/create-groups.txt", out + err)
    if rc != 0:
        raise RuntimeError(f"create-groups: {redact(err)}")
    for cid, h in CONTROLLERS.items():
        start(h, cid, ctl_cmd(cid))
    for g, h in GATEWAYS.items():
        start(h, g, gw_cmd(g))
    # Ready: the owner's bucket, then a signed probe object written and
    # deleted through each gateway (admission needs the leader's first
    # passes).
    ready = None
    end = time.time() + 240
    pr = None
    while time.time() < end:
        rc, out, _ = ssh("c", f"{CLIENT} mkbucket {q(gw_spec().split(',')[0])} --creds {R}/creds.json")
        if rc == 0:
            pr = probe("probe")
            if all(pr.get(g) == "200 204" for g in GATEWAYS):
                ready = time.time() - t_setup
                break
        time.sleep(5)
    _, cc, cce = admin("check-config")
    save("setup/check-config.txt", cc + cce)
    _, us, use = admin("upgrade status")
    save("setup/upgrade-status.txt", us + use)
    st = status()
    save("setup/status.txt", st["raw"])
    procs = {f"{h}:{p}": alive(h, p) for h, p in all_procs()}
    ok = ready is not None and all(procs.values())
    want_schema = "schema Some(4) schema_write Some(4)" if old else "schema Some(5) schema_write Some(5)"
    if old:
        ok = ok and want_schema in us
    return result(step, "PASS" if ok else "FAIL",
                  {"ready_seconds": ready and round(ready, 1), "processes_running": procs, "probe": pr,
                   "node_ids": IDS, "binaries": {"admin": ADMIN["bin"], **BINS} if old else "all current",
                   "upgrade_status": redact(us).strip().splitlines()[:3],
                   "check_config_tail": redact(cc + cce)[-600:]},
                  "every process running; a signed 2000-byte PUT and DELETE succeed through both gateways "
                  "within 240 s" + (f"; `{want_schema}`" if old else ""))


def teardown():
    if not ADDR:
        addresses()
    for h in "abc":
        rc, out, _ = ssh(h, f"cat {R}/.owner 2>/dev/null")
        if out.strip() != MARKER:
            log(f"host {h}: no marker in {R}, nothing removed")
            continue
        ssh(h, f"for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
               f"grep -qa {TAG} /proc/$q/cmdline 2>/dev/null && kill -CONT $q 2>/dev/null; "
               f"grep -qa {TAG} /proc/$q/cmdline 2>/dev/null && kill $q; done; sleep 3; "
               f"for p in {R}/*.pid; do [ -f \"$p\" ] || continue; q=$(cat \"$p\"); "
               f"grep -qa {TAG} /proc/$q/cmdline 2>/dev/null && kill -9 $q; done; sleep 1; true", timeout=60)
        # Logs (sanitized later), never keys or credentials; then the run
        # directory, once nothing of ours runs.
        dest = OUT / "logs" / f"host-{h}"
        dest.mkdir(parents=True, exist_ok=True)
        subprocess.run(f"ssh -o BatchMode=yes {HOSTS[h]} 'cd {R} && tar -cf - logs fdb/log fdb2a/log fdb2b/log 2>/dev/null' "
                       f"| tar -xf - -C {dest}", shell=True, timeout=300)
        rc, left, _ = ssh(h, f"pgrep -af {TAG} | grep -v pgrep || true")
        if left.strip():
            log(f"host {h}: processes still running, {R} kept:\n{left}")
            continue
        if os.environ.get("KEEP_RUN_DIR"):
            log(f"host {h}: run directory kept (KEEP_RUN_DIR)")
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


# --- main scenario steps ----------------------------------------------------

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


def step1b():
    _, us, use = admin("upgrade status")
    st = status()
    rc, chk = group_check()
    doms = st["domains"]
    ok = ("schema Some(5) schema_write Some(5)" in us and "protections mandatory" in us
          and set(doms) == set(st["groups"]) and len(doms) == 4
          and all(d == {"label": "host", "source": "created"} for d in doms.values())
          and rc == 0 and "groups breaking their failure domain: 0; groups without a recorded failure domain: 0" in chk)
    return result("1b-schema5-domains", "PASS" if ok else "FAIL",
                  {"upgrade_status": redact(us + use).strip().splitlines()[:3], "domains": doms,
                   "group_check_rc": rc, "group_check": chk.splitlines()},
                  "schema Some(5) schema_write Some(5), protections mandatory; every group's failure domain "
                  "recorded `host` (source created); group check exit 0 with 0 breaking and 0 unrecorded")


def step1c():
    nodes = {}
    ok = True
    for n in NODES:
        s = node_status(IDS[n])
        good = "write ownership strict" in s and "identity given (acknowledged: true)" in s
        nodes[n] = {"status": s.splitlines(), "ok": good}
        ok = ok and good
    tls_logs = {}
    for h, p in all_procs()[1:]:
        _, c, _ = ssh(h, f"grep -c 'mutual TLS enabled' {R}/logs/{p}.log")
        tls_logs[p] = int(c.strip() or 0)
    ok = ok and all(v > 0 for v in tls_logs.values())
    port = {n: 9621 + int(n[1]) for n in NODES}
    hs0 = {n: node_metric(n[0], port[n], "arca_node_tls_handshake_failures_total") or 0 for n in NODES}
    plain, foreign = {}, {}
    for n in NODES:
        plain[n] = node_status(IDS[n], env=f"ARCA_INSECURE_PLAINTEXT=1 {fdb_tls('admin')}")
        foreign[n] = node_status(IDS[n], env=f"{node_tls('admin-other')} {fdb_tls('admin')}")
    hs1 = {n: node_metric(n[0], port[n], "arca_node_tls_handshake_failures_total") or 0 for n in NODES}
    ok = ok and all("does not answer" in v for v in plain.values())
    ok = ok and all("does not answer" in v for v in foreign.values())
    ok = ok and all(hs1[n] > hs0[n] for n in NODES)
    t = time.time()
    rc, out, err = admin("status", timeout=30, env=f"ARCA_FDB_INSECURE_PLAINTEXT=1 {node_tls('admin')}")
    fdb_plain = {"rc": rc, "seconds": round(time.time() - t, 1), "tail": redact(out + err).strip()[-300:]}
    ok = ok and rc != 0
    return result("1c-transport-security", "PASS" if ok else "FAIL",
                  {"node_status": nodes, "mutual_tls_log_lines": tls_logs,
                   "plaintext_admin_node_status": {n: [l for l in v.splitlines() if "answer" in l] for n, v in plain.items()},
                   "foreign_ca_admin_node_status": {n: [l for l in v.splitlines() if "answer" in l] for n, v in foreign.items()},
                   "handshake_failures_before_after": {n: [hs0[n], hs1[n]] for n in NODES},
                   "fdb_plaintext_admin": fdb_plain},
                  "every node strict and its identity acknowledged; every process logs mutual TLS; plaintext and "
                  "foreign-CA admin get `does not answer` from every node and each node counts handshake failures; "
                  "a plaintext FoundationDB client does not succeed")


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
    ok = not bad and covered and dels > 0 and v["ok"] and v["absent_ok"] > 0 and c.get("signed")
    return result("2-s3-basic", "PASS" if ok else "FAIL",
                  {"ops": counts(recs), "bad_samples": samples(bad), "every_size_acked_on_both": covered,
                   "deletes_acked": dels, "verify": v, "client": c},
                  "0 errors, 0 violations; every size class acked through both gateways; verify reads every key, "
                  "deleted keys answer 404; every request signed")


def step2b():
    c = client_sync("auth-check", "s2b", timeout=600)
    v = verify("s2b-verify")
    ok = c["ok"] and c["signed"] and v["ok"] and len(c["cases"]) >= 2 * 25
    return result("2b-sigv4-ownership", "PASS" if ok else "FAIL",
                  {"botocore": c.get("botocore"), "cases": c["cases"], "failed": c["failed"], "verify": v},
                  "every SigV4 and ownership case answers its expected status and code through both gateways "
                  "(plan table); refused writes leave the key unchanged; verify clean")


def step2c():
    c = client_sync("multipart", "s2c", timeout=900)
    v = verify("s2c-verify")
    ok = c["ok"] and v["ok"]
    return result("2c-multipart", "PASS" if ok else "FAIL",
                  {"key": c.get("key"), "size": c.get("size"), "cases": c["cases"], "failed": c["failed"],
                   "verify": v},
                  "create, parts with part 2 replaced, InvalidPart for the replaced ETag, complete, exact read-back, "
                  "4 exact ranges (206), abort then NoSuchUpload and the key absent; verify clean")


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
    q_ = wait_queue_empty(240)
    v = verify("s3a-verify")
    ok = (not bad_absent and not bad_after and not viol and v["ok"] and puts_acked > 0 and gets_ok > 0
          and q_[0] and q_[0]["unrecoverable"] == 0 and q_[0]["failed"] == 0)
    return result("3a-node-kill", "PASS" if ok else "FAIL",
                  {"absence_s": round(t_restart - t_kill, 1), "ops": counts(recs),
                   "absence_window": counts(absent_window), "errors_in_absence": samples(bad_absent),
                   "in_flight_at_kill": counts(inflight), "errors_after_restart_10s": samples(bad_after),
                   "violations": len(viol), "gateway_metrics_before": gm0, "gateway_metrics_after": gm1,
                   "repair_queue_after": q_[0], "verify": v},
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
    # whenever it finished.
    put_acked = [r for r in recs if r["op"] == "PUT" and t_kill <= r["t0"] < t_restart and r["outcome"] == "ack"]
    put_failed = [r for r in win if r["op"] == "PUT" and r["outcome"] != "ack"]
    after_bad = [r for r in recs if r["t0"] >= t_restart + 10 and r["outcome"] in BAD]
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    q_ = wait_queue_empty(240)
    v = verify("s3c-verify")
    ok = (not get_bad and not put_acked and put_failed and not after_bad and not viol and v["ok"]
          and q_[0] and q_[0]["unrecoverable"] == 0 and q_[0]["failed"] == 0)
    statuses = {}
    for r in put_failed:
        statuses[str(r["status"])] = statuses.get(str(r["status"]), 0) + 1
    return result("3c-host-b-nodes-down", "PASS" if ok else "FAIL",
                  {"down_s": round(t_restart - t_kill, 1), "window": counts(win), "get_errors": samples(get_bad),
                   "put_acked_in_window": samples(put_acked), "put_failure_statuses": statuses,
                   "put_failure_samples": samples(put_failed, 3), "errors_after_restart_10s": samples(after_bad),
                   "violations": len(viol), "repair_queue_after": q_[0], "verify": v},
                  "both nodes of host B down: every group lacks one member; GETs answered (k of 3), no PUT started "
                  "while they were down is ever acked (all shards required), PUTs in the window fail (by kind: "
                  "status and error recorded); 0 errors 10 s after restart")


def takeover(old, other, t_kill):
    """Poll the leader record from host C until `other` holds it; seconds
    after `t_kill`, or None."""
    poll = (f"end=$((SECONDS+60)); while [ $SECONDS -lt $end ]; do t=$(date +%s.%N); "
            f"l=$(env {node_tls('admin')} {fdb_tls('admin')} {ADMIN['bin']} admin --cluster-file {R}/fdb.cluster "
            f"status 2>/dev/null | grep '^leader'); "
            f"echo \"$t $l\"; case \"$l\" in \"leader {other}@\"*) break;; esac; sleep 0.2; done")
    _, out, _ = ssh("c", poll, timeout=120)
    for line in out.splitlines():
        parts = line.split()
        if len(parts) > 2 and parts[2].startswith(other + "@"):
            return float(parts[0]) - t_kill
    return None


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
            took = takeover(old, other, t_kill)
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
    q_ = wait_queue_empty(300)
    sc = scrub(groups_of)
    v = verify("s5-verify")
    ok = (mid and mid[1] == 1 and healthy_after is not None and same and not bad_win and not after_bad
          and not viol and damage_free(sc) and q_[0] and q_[0]["unrecoverable"] == 0 and q_[0]["failed"] == 0
          and v["ok"])
    return result("5-node-return", "PASS" if ok else "FAIL",
                  {"victim": victim, "down_s": round(t_restart - t_kill, 1),
                   "unhealthy_metric_during": mid, "unhealthy_0_after_s": healthy_after,
                   "groups_unchanged_active": same, "window": counts(win), "errors_in_window": samples(bad_win),
                   "errors_after_restart_10s": samples(after_bad), "violations": len(viol),
                   "repair_queue_after": q_[0], "scrub": sc, "verify": v},
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
            # Ready again: a signed probe PUT and DELETE through each gateway.
            ready = None
            end = time.time() + 240
            while time.time() < end:
                pr = probe("probe6")
                if all(pr.get(g) == "200 204" for g in GATEWAYS):
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
        procs = {f"{h}:{p}": alive(h, p) for h, p in all_procs()[1:]}
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
    # Shards of objects that are current: chunk 0 of a few live keys,
    # located from their generation (`verify-object`) and the group of c1's
    # member that holds it.
    st = status()
    victim = "c1"
    vid = node_id(st, victim)
    groups_of = {g: x["members"].index(vid) for g, x in st["groups"].items() if vid in x["members"]}
    ec = [g for g in groups_of if st["groups"][g]["scheme"] == "rs-gf8-v1"]
    rep = [g for g in groups_of if st["groups"][g]["scheme"] == "replication-v1"]
    # Only keys last written while every storage node was up (steps 2, 2b,
    # 3b and 4; dbg5: steps 2 and 4 alone left no replicated key in c1's
    # replication group).
    last_put = {}
    for r in ledger():
        if r["op"] == "PUT":
            last_put[r["key"]] = r
    safe = {k for k, r in last_put.items() if r["phase"] in ("s2", "s2b", "s3b", "s4") and r["outcome"] == "ack"}
    live = [(k, z) for k, z in live_keys() if k in safe]
    cands = [(k, z) for k, z in live if z > 65536] + [(k, z) for k, z in live if z <= 65536]
    gens = {}
    for k, z in cands:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {q(k)}", timeout=120)
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
            rc, o, e = ssh("c", f"env {node_tls('n-' + victim)} {BIN} storage-node corrupt-shard --dir {R}/n-{victim} "
                                f"--group {g} --chunk {chunk} --index {groups_of[g]}", timeout=120)
            if rc == 0:
                corrupted.append({"key": k, "size": z, "group": g, "index": groups_of[g],
                                  "detail": redact(o + e).strip()[-300:]})
                want[kind] -= 1
                break
    start("c", f"n-{victim}", node_cmd(victim))
    time.sleep(10)
    for c in corrupted:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {q(c['key'])}", timeout=300)
        c["referenced"] = bool(re.search(rf"^  part \S+ chunk 0 id \S+ group {c['group']} good ", o, re.M))
    sc1 = scrub(sorted(groups_of))
    found = sum(v for r in sc1.values() for c, v in r.get("findings", {}).items() if c != "UnexpectedShard")
    q_ = wait_queue_empty(300)
    sc2 = scrub(sorted(groups_of))
    objs = {}
    for c in corrupted:
        rc, o, e = admin(f"integrity verify-object --bucket v3h --key {q(c['key'])} --decode", timeout=300)
        objs[c["key"]] = {"rc": rc, "out": redact(o).strip().splitlines()[:4]}
    v = verify("s7a-verify")
    _, nlog, _ = ssh("c", f"grep -E 'damaged|quarantin' {R}/logs/n-{victim}.log | tail -5")
    ok = (len(corrupted) == 3 and all(c.get("referenced") for c in corrupted)
          and found >= len(corrupted) and q_[0] and q_[0]["depth"] == 0
          and q_[0]["unrecoverable"] == 0 and q_[0]["failed"] == 0 and damage_free(sc2)
          and all(o["rc"] == 0 for o in objs.values()) and v["ok"])
    return result("7a-scrub-repair-corrupt", "PASS" if ok else "FAIL",
                  {"victim": victim, "corrupted": corrupted, "node_log": redact(nlog).strip().splitlines(),
                   "scrub_before_repair": sc1, "damage_findings_before_repair": found, "repair_queue": q_[0],
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
        state, nl = node_state(vid)
        if state and state.lower().startswith("quarantin"):
            quarantined_after = round(time.time() - t0, 1)
            break
        time.sleep(3)
    save("step7b/node-list-after-wipe.txt", nl)
    save("step7b/status-after-wipe.txt", status()["raw"])
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
    rc_c, chk = group_check()
    doms_ok = all(st["domains"].get(g, {}).get("label") == "host" for g in st["groups"])
    sc = scrub(sorted(st["groups"]))
    v = verify("s7b-verify")
    # The emptied disk, without members now, comes back with a new identity.
    rc_j, oj, ej = admin(f"node rejoin --node {vid} --fresh-identity --yes")
    rejoined, sj = None, ""
    t2 = time.time()
    while time.time() - t2 < 60:
        state, _ = node_state(vid)
        sj = node_status(vid)
        if state == "active" and "identity given (acknowledged: true)" in sj and "write ownership strict" in sj:
            rejoined = round(time.time() - t2, 1)
            break
        time.sleep(3)
    ok = (quarantined_after is not None and v_deg["ok"] and done_after is not None and hosts_ok and target_ok
          and damage_free(sc) and v["ok"] and rc_c == 0 and doms_ok and rc_j == 0 and rejoined is not None)
    return result("7b-wipe-replace", "PASS" if ok else "FAIL",
                  {"victim": victim, "quarantined_after_s": quarantined_after, "verify_while_wiped": v_deg,
                   "replace": replace, "replaced_after_s": done_after, "members_after": members,
                   "one_member_per_host": hosts_ok, "target_is_b0": target_ok, "group_check_rc": rc_c,
                   "group_check": chk.splitlines(), "domains_after": st["domains"], "scrub_all": sc, "verify": v,
                   "rejoin_fresh_identity": {"rc": rc_j, "out": [l for l in redact(oj + ej).strip().splitlines()
                                                                 if not l.startswith("20")]},
                   "rejoined_active_after_s": rejoined,
                   "status_after_rejoin": [l for l in sj.splitlines() if not l.startswith("20")]},
                  "the wiped node restarts without its identity and is quarantined; GETs still answered (k of 3); "
                  "replace-member rebuilds each of its groups onto b0 (the only healthy non-member in domain b); "
                  "groups Active, one member per host; group check exit 0, every domain still host; scrub 0 damage; "
                  "verify clean; then `node rejoin --fresh-identity --yes` makes b1 active (no members) with a new "
                  "identity acknowledged and strict within 60 s")


def final_check(step, extra_rule=None):
    st = status()
    sc = scrub(sorted(st["groups"]))
    q_, qout = integrity_status()
    rc, vm, vme = admin("integrity verify-manifests", timeout=300)
    m = re.search(r"roots (\d+) findings (\d+)", vm)
    roots = int(m.group(1)) if m else None
    v = verify(f"{step}-verify")
    recs = ledger()
    acked = sum(1 for r in recs if r["op"] in ("PUT", "MPU") and r["outcome"] == "ack")
    allv = sum(1 for r in recs if r["outcome"] == "VIOLATION")
    _, ps, _ = ssh("c", f"python3 -c \"import json;d=json.load(open('{R}/state.json'));print(d['put_bytes'])\"")
    rc_c, chk = group_check()
    ok = (damage_free(sc) and q_ and q_["unrecoverable"] == 0 and rc == 0 and v["ok"] and allv == 0
          and roots is not None and roots > 0 and rc_c == 0)
    ev = {"scrub_all": sc, "integrity_status": q_, "verify_manifests": redact(vm + vme).strip()[-400:],
          "multipart_roots": roots, "verify": v, "ledger_ops": len(recs), "puts_acked_total": acked,
          "violations_total": allv, "logical_put_bytes": int(ps.strip() or 0), "groups": st["groups"],
          "group_check_rc": rc_c, "group_check": chk.splitlines()}
    if extra_rule:
        ok = extra_rule(ev) and ok
    return result(step, "PASS" if ok else "FAIL", ev,
                  "scrub of every group 0 damage findings (UnexpectedShard reported, not judged), no unrecoverable "
                  "task, verify-manifests exit 0 with at least one multipart root, every key reads back as its "
                  "candidate, 0 violations in the ledger; group check exit 0")


def step7c():
    return final_check("7c-final")


# --- R-117: another cluster's node at c1's address -------------------------

def second_cluster(tag, ca, admin_cert, ctl_cert, imp_cert):
    """A second cluster on host B (FoundationDB plaintext, own port) whose
    node `imp-<tag>` has c1's id and lives at c1's address (not started
    here). Returns its admin function."""
    cid = IDS["c1"]
    port = FDB2_PORTS[tag]
    cf = f"{R}/fdb2{tag}.cluster"
    ssh("b", f"mkdir -p {R}/fdb2{tag}/data {R}/fdb2{tag}/log && echo 'z{tag}:z{tag}{RUN_ID.replace('-', '')}@{ADDR['b']}:{port}' > {cf}",
        check=True)
    start("b", f"fdb2{tag}", f"fdbserver -p {ADDR['b']}:{port} -C {cf} -d {R}/fdb2{tag}/data -L {R}/fdb2{tag}/log "
                             "--memory 1GiB --cache-memory 128MiB")
    time.sleep(3)
    ssh("b", f"timeout 60 fdbcli -C {cf} --exec 'configure new single ssd' --timeout 30", timeout=90)
    for _ in range(60):
        _, out, _ = ssh("b", f"timeout 20 fdbcli -C {cf} --exec 'status minimal' --timeout 10", timeout=30)
        if "is available" in out:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"second cluster {tag}: FoundationDB not available")
    env = f"ARCA_FDB_INSECURE_PLAINTEXT=1 {node_tls(admin_cert)}"

    def adm2(args, timeout=60):
        return admin(args, timeout=timeout, h="b", env=env, cluster_file=cf, bin=BIN)

    rc, out, err = adm2("init")
    if rc != 0:
        raise RuntimeError(f"second cluster {tag}: init: {redact(out + err)[-300:]}")
    # Ids below c1's at addresses nothing listens on, then the impostor.
    for i in range(1, cid):
        adm2(f"register-node --address 127.0.0.1:{19000 + i} --label host=z --label node=placeholder{i}")
    rc, out, err = adm2(f"register-node --address {ADDR['c']}:{IMP_ADOPT_PORT} --label host=z --label node=impostor")
    m = re.search(r"node (\d+) registered", out)
    if not m or int(m.group(1)) != cid:
        raise RuntimeError(f"second cluster {tag}: the impostor got id {m and m.group(1)}, not {cid}")
    tls_issue(ca, "node", str(cid), imp_cert)
    tls_issue(ca, "controller", "ctl-z", ctl_cert)
    push_pki()
    return adm2, cf


def replace_and_rejoin(nid, groups, ev):
    """A quarantined node rejoins only without members
    (crates/metadata/src/ops/node_lifecycle.rs:344-375): its slots are
    rebuilt in their domain first, then it rejoins. Returns its state."""
    ev["replace"] = {}
    for g in groups:
        idx = status()["groups"][g]["members"].index(nid)
        rc, o, e = admin(f"replace-member --group {g} --index {idx} --yes", timeout=120)
        ev["replace"][g] = {"rc": rc, "out": redact(o + e).strip()[-300:]}
    ev["replaced_after_s"] = None
    t1 = time.time()
    while time.time() - t1 < 900:
        st = status()
        if all(st["groups"][g]["state"] == "Active" and nid not in st["groups"][g]["members"] for g in groups):
            ev["replaced_after_s"] = round(time.time() - t1, 1)
            break
        time.sleep(5)
    st = status()
    ev["members_after_replace"] = {g: [st["nodes"][m]["name"] for m in st["groups"][g]["members"]] for g in groups}
    ev["one_member_per_host_after_replace"] = all(
        sorted(st["nodes"][m]["host"] for m in st["groups"][g]["members"]) == ["a", "b", "c"] for g in groups)
    rc, o, e = admin(f"node rejoin --node {nid} --yes")
    ev["rejoin"] = {"rc": rc, "out": [l for l in redact(o + e).strip().splitlines() if not l.startswith("20")]}
    time.sleep(10)
    return node_state(nid)[0]


IMP_ADOPT_PORT = 9614  # where the impostor adopts the second cluster's identity


def r117_step(step, tag, same_ca):
    """Steps 8a and 8b: another cluster's node (its identity adopted from
    its own controller first, at another port) started at c1's address
    while the main cluster serves load."""
    st0 = status()
    cid = IDS["c1"]
    c1_groups = [g for g, x in st0["groups"].items() if cid in x["members"]]
    ca = "ca-main" if same_ca else "ca-other"
    admin_cert = "admin" if same_ca else "admin-other"
    adm2, cf = second_cluster(tag, ca, admin_cert, f"ctl-z{tag}", f"imp-{tag}")
    imp = f"imp-{tag}"

    def imp_cmd(port):
        return (f"{BIN} storage-node --listen {ADDR['c']}:{port} --dir {R}/{imp} "
                f"--metrics-listen 127.0.0.1:9632 --capacity-bytes {1 << 30} --segment-max-bytes {SEGMENT}")

    ev = {"c1_id": cid, "c1_groups": c1_groups, "ca": "main" if same_ca else "other"}
    # The impostor becomes the second cluster's node `cid` (identity and
    # incarnation adopted, strict) before it ever answers at c1's address.
    t_adopt = time.time()
    start("c", imp, imp_cmd(IMP_ADOPT_PORT))
    start("b", f"ctl-z{tag}", ctl_cmd("ctl-z", http="127.0.0.1:9641", cluster_file=cf),
          env=f"ARCA_FDB_INSECURE_PLAINTEXT=1 {node_tls(f'ctl-z{tag}')}")
    adopted, s2 = None, ""
    while time.time() - t_adopt < 60:
        _, o, e = adm2(f"node status --node {cid}")
        s2 = redact(o + e)
        if "identity given (acknowledged: true)" in s2 and "write ownership strict" in s2:
            adopted = round(time.time() - t_adopt, 1)
            break
        time.sleep(2)
    ev["impostor_adopted_after_s"] = adopted
    ev["impostor_status_second_cluster_at_adoption"] = [l for l in s2.strip().splitlines() if not l.startswith("20")]
    stop("b", f"ctl-z{tag}")
    stop("c", imp)
    phase = f"s8{tag}"
    quarantined_after = None
    with Load(phase):
        time.sleep(10)
        stop("c", "n-c1")
        t_imp = ts("c")
        start("c", imp, imp_cmd(NODE_PORTS[1]))
        while ts("c") - t_imp < 45:
            if quarantined_after is None:
                state, _ = node_state(cid)
                if state == "quarantined":
                    quarantined_after = round(ts("c") - t_imp, 1)
            time.sleep(3)
        rc, o, e = admin(f"node rejoin --node {cid} --check")
        ev["main_rejoin_check_while_impostor"] = {"rc": rc, "out": [l for l in redact(o + e).strip().splitlines()
                                                                    if not l.startswith("20")]}
        ev["main_node_status_while_impostor"] = [l for l in node_status(cid).splitlines() if not l.startswith("20")]
        t_end = ts("c")
    imp_entries = node_metric("c", 9632, "arca_node_index_entries")
    ev["impostor_index_entries"] = imp_entries
    ev["impostor_handshake_failures"] = node_metric("c", 9632, "arca_node_tls_handshake_failures_total")
    stop("c", imp)
    # Nothing at c1's address now: a chunk counted on the impostor would
    # show as not whole.
    recs = ledger(phase)
    keys = acked_in(recs, t_imp, t_end)
    placed, on_c1, unread = no_chunk_on(keys, c1_groups)
    _, rej, _ = ssh("c", f"grep -hcE 'incarnation rejected|node rejected' {R}/logs/{imp}.log {R}/logs/gw-c.log")
    _, rejb, _ = ssh("b", f"grep -cE 'incarnation rejected|node rejected' {R}/logs/gw-b.log")
    ev["refusal_log_lines"] = {"impostor+gw-c": [int(x) for x in rej.split()], "gw-b": int(rejb.strip() or 0)}
    stop("b", f"fdb2{tag}")
    start("c", "n-c1", node_cmd("c1"))
    time.sleep(10)
    state, _ = node_state(cid)
    ev["c1_state_on_return"] = state
    if state == "quarantined":
        state = replace_and_rejoin(cid, c1_groups, ev)
    ev["c1_state_after"] = state
    ev["quarantined_after_s"] = quarantined_after
    q_ = wait_queue_empty(300)
    sc = scrub(c1_groups)
    rc_c, chk = group_check()
    v = verify(f"{phase}-verify")
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    late_bad = [r for r in recs if r["t0"] >= t_imp + 10 and r["t1"] <= t_end and r["outcome"] in BAD]
    ev.update({"window_s": round(t_end - t_imp, 1), "ops": counts(recs), "acked_puts_in_window": len(keys),
               "chunk_groups_not_whole_of_acked": {k: g for k, g in placed.items() if g},
               "acked_with_chunk_in_c1_groups": on_c1, "unreadable": unread,
               "errors_from_10s": samples(late_bad), "violations": len(viol), "repair_queue_after": q_[0],
               "scrub_c1_groups": sc, "group_check_rc": rc_c, "verify": v})
    ok = (adopted is not None and keys and not on_c1 and not unread and imp_entries == 0 and not viol
          and not late_bad and q_[0] and q_[0]["unrecoverable"] == 0 and q_[0]["failed"] == 0
          and damage_free(sc) and rc_c == 0 and v["ok"] and state == "active")
    if same_ca:
        rj = ev["main_rejoin_check_while_impostor"]
        ok = (ok and quarantined_after is not None and quarantined_after <= 60
              and any("another identity token" in l for l in ev["main_node_status_while_impostor"])
              and sum(ev["refusal_log_lines"]["impostor+gw-c"]) + ev["refusal_log_lines"]["gw-b"] > 0
              and rj["rc"] != 0 and any("another cluster" in l for l in rj["out"])
              and ev.get("replaced_after_s") is not None and ev["one_member_per_host_after_replace"]
              and ev["rejoin"]["rc"] == 0)
    else:
        ok = ok and quarantined_after is None and (ev["impostor_handshake_failures"] or 0) > 0
    return result(step, "PASS" if ok else "FAIL", ev,
                  "the impostor adopts the second cluster's identity (strict) at another port, then answers at "
                  "c1's address; no PUT acked while it runs has a chunk in c1's groups (verify-object with nothing "
                  "at the address); it stores 0 shards; 0 errors from 10 s after its start; "
                  + ("the leader quarantines c1's id within 60 s for another identity token, rejoin --check refuses "
                     "naming another cluster's disk, refusals logged; after c1's return its slots are replaced in "
                     "domain c and rejoin --yes makes it active; " if same_ca else
                     "the impostor counts TLS handshake failures and c1 is not quarantined; ")
                  + "queue drains, scrub of c1's groups 0 damage, group check exit 0, verify clean")


def step8a():
    return r117_step("8a-r117-other-ca", "a", same_ca=False)


def step8b():
    return r117_step("8b-r117-same-ca", "b", same_ca=True)


def step8c():
    st0 = status()
    rc, out, err = admin(f"register-node --address {ADDR['c']}:{NODE_PORTS[2]} --label host=c --label node=c2 "
                         "--label device_class=hdd")
    m = re.search(r"node (\d+) registered", out)
    if rc != 0 or not m:
        return result("8c-new-node-joins", "FAIL", {"register": redact(out + err)[-400:]})
    nid = int(m.group(1))
    IDS["c2"] = nid
    tls_issue("ca-main", "node", str(nid), "n-c2")
    push_pki()
    t0 = time.time()
    start("c", "n-c2", node_cmd("c2", capacity=1 << 30))
    joined, s = None, ""
    while time.time() - t0 < 60:
        s = node_status(nid)
        if "identity given (acknowledged: true)" in s and "write ownership strict" in s:
            joined = round(time.time() - t0, 1)
            break
        time.sleep(2)
    rc, cg, cge = admin("create-groups --tier default --device-class hdd --count 1 --k 2 --m 1 "
                        "--chunk-bytes 4194304 --small-threshold 65536 --failure-domain host")
    st = status()
    new = sorted(set(st["groups"]) - set(st0["groups"]))
    hosts = {i: n["host"] for i, n in st["nodes"].items()}
    # Members: the fewest groups per host, then the lowest id (c1, which
    # rejoined without members in 8b, before c2 for the first group).
    groups_ok = (len(new) == 2 and any(nid in st["groups"][g]["members"] for g in new) and all(
        st["groups"][g]["state"] == "Active"
        and sorted(hosts[m] for m in st["groups"][g]["members"]) == ["a", "b", "c"]
        and st["domains"].get(g) == {"label": "host", "source": "created"} for g in new))
    rc_c, chk = group_check()
    with Load("s8c"):
        time.sleep(40)
    recs = ledger("s8c")
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    s_after = node_status(nid)
    m = re.search(r"live shards (\d+)", s_after)
    live = int(m.group(1)) if m else 0
    sc = scrub(new)
    v = verify("s8c-verify")
    ok = (joined is not None and rc == 0 and groups_ok and rc_c == 0 and live > 0 and damage_free(sc)
          and not bad and v["ok"])
    return result("8c-new-node-joins", "PASS" if ok else "FAIL",
                  {"node_id": nid, "joined_after_s": joined, "status_at_join": s.splitlines(),
                   "create_groups": redact(cg + cge).strip().splitlines(),
                   "new_groups": {g: {**st["groups"][g], "members": [st["nodes"][m]["name"] for m in st["groups"][g]["members"]],
                                      "domain": st["domains"].get(g)} for g in new},
                   "group_check_rc": rc_c, "status_after_load": s_after.splitlines(), "live_shards": live,
                   "ops": counts(recs), "bad_samples": samples(bad), "scrub_new_groups": sc, "verify": v},
                  "c2 acknowledged and strict within 60 s; 2 new Active groups, one member per host, c2 in at least one, "
                  "domain host (created); group check exit 0; c2 holds shards after a 40 s load; scrub 0 damage; "
                  "0 client errors; verify clean")


def step9():
    # Quiet: no load since 8c; wait past the orphan age and a reconcile pass.
    time.sleep(150)
    st = status()
    sc = scrub(sorted(st["groups"]))
    held = {g: r.get("held") for g, r in sc.items()}
    ok = all("findings" in r and r["errors"] == 0 and r.get("held") is not None
             and r["held"]["unclassified"] == 0 and r["held"]["in_flight"] == 0
             and r["held"]["retained_damaged"] == 0 and not any(r["findings"].values()) for r in sc.values())
    retained = sum((r.get("held") or {}).get("retained", 0) for r in sc.values())
    ok = ok and retained > 0
    return result("9-scrub-classes", "PASS" if ok else "FAIL",
                  {"scrub": sc, "held": held, "retained_total": retained},
                  "after 150 s without load: every group errors 0, unclassified 0, in_flight 0, retained_damaged 0, "
                  "every finding class 0 (UnexpectedShard included); retained > 0 in total")


def step10():
    return final_check("10-final")


MAIN = [("1-placement", step1), ("1b-schema5-domains", step1b), ("1c-transport-security", step1c),
        ("2-s3-basic", step2), ("2b-sigv4-ownership", step2b), ("2c-multipart", step2c),
        ("3a-node-kill", step3a), ("3b-gateway-restart", step3b), ("3c-host-b-nodes-down", step3c),
        ("4-controller-takeover", step4), ("5-node-return", step5), ("6-fdb-unavailable", step6),
        ("7a-scrub-repair-corrupt", step7a), ("7b-wipe-replace", step7b), ("7c-final", step7c),
        ("8a-r117-other-ca", step8a), ("8b-r117-same-ca", step8b), ("8c-new-node-joins", step8c),
        ("9-scrub-classes", step9), ("10-final", step10)]


# --- upgrade scenario steps -------------------------------------------------

def u1():
    c = client_sync("load", "u1", "--ops 400 --cover --put-rate-mib 0", timeout=1200)
    mp = client_sync("multipart", "u1-mp", timeout=900)
    recs = ledger("u1")
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    v = verify("u1-verify")
    ok = not bad and mp["ok"] and v["ok"]
    return result("U1-data-0.1.0", "PASS" if ok else "FAIL",
                  {"ops": counts(recs), "bad_samples": samples(bad), "multipart_failed": mp["failed"],
                   "client": c, "verify": v},
                  "0 errors, 0 violations; a multipart upload with a replaced part completes and reads back exact")


def restart_with(h, name, cmd_fn, binary):
    stop(h, name)
    t = ts(h)
    BINS[name] = binary
    start(h, name, cmd_fn())
    return t


def u2():
    phase = "u2"
    seen = {}
    restarts = []
    with Load(phase):
        time.sleep(10)
        for n in NODES:
            t = restart_with(n[0], f"n-{n}", lambda n=n: node_cmd(n), BIN)
            restarts.append(t)
            back = None
            while ts("c") - t < 60:
                _, nl = node_state(IDS[n])
                row = next((l.split() for l in nl.splitlines() if l.split()[:1] == [str(IDS[n])]), [])
                if row and alive(n[0], f"n-{n}") and int(row[-1]) < 3 and ts("c") - t > 6:
                    back = round(ts("c") - t, 1)
                    break
                time.sleep(2)
            seen[n] = back
            time.sleep(15)
        t_end = ts("c")
    recs = ledger(phase)
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    bounds = restarts + [t_end]
    late_bad = [r for r in recs if r["outcome"] in BAD and any(
        bounds[i] + 10 <= r["t0"] and r["t1"] <= bounds[i + 1] for i in range(len(restarts)))]
    v = verify("u2-verify")
    ok = not viol and not late_bad and all(x is not None for x in seen.values()) and v["ok"]
    return result("U2-storage-nodes", "PASS" if ok else "FAIL",
                  {"seen_after_s": seen, "ops": counts(recs), "errors_10s_after_restarts": samples(late_bad),
                   "violations": len(viol), "verify": v},
                  "each node on 0.2.0 seen again within 60 s; 0 violations; 0 errors from 10 s after each restart "
                  "until the next stop; verify clean")


def u3():
    phase = "u3"
    cycles = []
    with Load(phase):
        time.sleep(5)
        lead = status()["leader"]["holder"]
        follower = next(c for c in CONTROLLERS if c != lead)
        for cid in (follower, lead):
            other = next(c for c in CONTROLLERS if c != cid)
            t = restart_with(CONTROLLERS[cid], cid, lambda cid=cid: ctl_cmd(cid), BIN)
            # A leader again (either controller: the restarted one may take
            # the lease back first) with a lease running past now.
            took = None
            while ts("a") - t < 30:
                st = status()
                if st["leader"] and st["leader"]["lease_until_ms"] / 1000 > ts("a"):
                    took = ts("a") - t
                    break
                time.sleep(1)
            time.sleep(10)
            st = status()
            cycles.append({"upgraded": cid, "was_leader": cid == lead, "leader_within_s": took and round(took, 2),
                           "leader_after": st["leader"] and st["leader"]["holder"], "alive": alive(CONTROLLERS[cid], cid)})
    ADMIN["bin"] = BIN
    recs = ledger(phase)
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    rc, chk = group_check()
    v = verify("u3-verify")
    ok = (all(c["alive"] and c["leader_after"] in CONTROLLERS for c in cycles)
          and all(c["leader_within_s"] is not None and c["leader_within_s"] <= 20 for c in cycles)
          and not bad and rc == 1 and "no recorded failure domain" in chk and "degraded" in chk and v["ok"])
    return result("U3-controllers", "PASS" if ok else "FAIL",
                  {"cycles": cycles, "ops": counts(recs), "bad_samples": samples(bad), "group_check_rc": rc,
                   "group_check": chk.splitlines(), "verify": v},
                  "follower then leader on 0.2.0; after each restart a controller holds a running lease within 20 s; "
                  "0 client errors; group check "
                  "exit 1 with `no recorded failure domain` and `degraded`; verify clean")


def u4():
    rc_d, dry, drye = admin("group set-failure-domain --all-unrecorded --label host --dry-run")
    rc_c0, chk0 = group_check()
    rc_s, setout, sete = admin("group set-failure-domain --all-unrecorded --label host")
    rc_c, chk = group_check()
    st = status()
    n_rec = len(re.findall(r"failure domain 'host' recorded", setout))
    ok = (rc_d == 0 and len(re.findall(r"would record failure domain 'host'", dry)) == 4 and rc_c0 == 1
          and rc_s == 0 and n_rec == 4 and rc_c == 0 and len(st["domains"]) == 4
          and all(d == {"label": "host", "source": "operator"} for d in st["domains"].values()))
    return result("U4-record-failure-domains", "PASS" if ok else "FAIL",
                  {"dry_run": {"rc": rc_d, "out": redact(dry + drye).strip().splitlines()},
                   "group_check_after_dry_run_rc": rc_c0,
                   "set": {"rc": rc_s, "out": redact(setout + sete).strip().splitlines()},
                   "group_check_rc": rc_c, "group_check": chk.splitlines(), "domains": st["domains"]},
                  "dry run lists 4 groups and records nothing; the run records `host` on 4 groups (exit 0); group "
                  "check exit 0; every domain source operator")


def versions():
    _, out, err = admin("status --versions")
    return redact(out + err)


def u5():
    phase = "u5"
    windows = {}
    with Load(phase):
        time.sleep(10)
        for g in ("gw-c", "gw-b"):
            h = GATEWAYS[g]
            t_stop = ts(h)
            t = restart_with(h, g, lambda g=g: gw_cmd(g), BIN)
            windows[g] = (t_stop, t)
            time.sleep(25)
    recs = ledger(phase)
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    bad = []
    for g, (t_stop, t) in windows.items():
        other = next(x for x in GATEWAYS if x != g)
        bad += [r for r in recs if r["gw"] == other and r["outcome"] in BAD and t_stop - 1 <= r["t0"] <= t + 25]
        bad += [r for r in recs if r["gw"] == g and r["outcome"] in BAD and t + 5 <= r["t0"] <= t + 25]
    time.sleep(5)
    vs = versions()
    fresh_old = [l for l in vs.splitlines() if l.endswith("below-schema-5")]
    v = verify("u5-verify")
    ok = not viol and not bad and not fresh_old and "protections degraded" in vs and v["ok"]
    return result("U5-gateways", "PASS" if ok else "FAIL",
                  {"ops": counts(recs), "bad_samples": samples(bad), "violations": len(viol),
                   "versions": vs.splitlines(), "below_schema_5_fresh": fresh_old, "verify": v},
                  "gw-c then gw-b on 0.2.0: the other gateway 0 errors, the restarted one 0 errors from 5 s after; "
                  "no fresh below-schema-5 line; protections degraded; verify clean")


def u6():
    phase = "u6"
    lead = status()["leader"]["holder"]
    follower = next(c for c in CONTROLLERS if c != lead)
    back = [("c", "n-c1", lambda: node_cmd("c1")), (CONTROLLERS[follower], follower, lambda: ctl_cmd(follower)),
            ("c", "gw-c", lambda: gw_cmd("gw-c"))]
    ev = {"rolled_back": ["n-c1", follower, "gw-c"]}
    with Load(phase):
        time.sleep(5)
        for h, name, fn in back:
            restart_with(h, name, fn, BIN_V010)
            time.sleep(8)
        time.sleep(25)
        ev["alive_on_0.1.0"] = {name: alive(h, name) for h, name, _ in back}
        ev["versions"] = versions().splitlines()
        rc, o, e = admin("upgrade finalize --wait --timeout-secs 30", timeout=60)
        ev["finalize"] = {"rc": rc, "out": redact(o + e).strip().splitlines()[-6:]}
        _, us, _ = admin("upgrade status")
        ev["upgrade_status"] = redact(us).strip().splitlines()
        rc2, o2, e2 = admin("upgrade cancel")
        ev["cancel"] = {"rc": rc2, "out": redact(o2 + e2).strip().splitlines()}
        t_load_end = ts("c")
    recs = ledger(phase)
    viol = [r for r in recs if r["outcome"] == "VIOLATION"]
    gwc_put_ack = sum(1 for r in recs if r["gw"] == "gw-c" and r["op"] == "PUT" and r["outcome"] == "ack")
    v_mixed = verify("u6-verify-mixed")
    sc = scrub(sorted(status()["groups"]))
    ev["scrub_mixed_not_judged"] = sc
    for h, name, fn in back:
        restart_with(h, name, fn, BIN)
        time.sleep(8)
    time.sleep(20)
    _, us2, _ = admin("upgrade status")
    v = verify("u6-verify")
    flagged = [l for l in ev["versions"] if l.endswith("below-schema-5")]
    fin_text = " ".join(ev["finalize"]["out"])
    ok = (all(ev["alive_on_0.1.0"].values()) and not viol and gwc_put_ack > 0 and v_mixed["ok"]
          and len(flagged) >= 2 and ev["finalize"]["rc"] != 0
          and ("0.1.0 or earlier" in fin_text or "reads schema 4" in fin_text)
          and "schema Some(4) schema_write Some(4)" in us2 and v["ok"])
    ev.update({"ops": counts(recs), "violations": len(viol), "gw_c_puts_acked_on_0.1.0": gwc_put_ack,
               "flagged_below_schema_5": flagged, "verify_mixed": v_mixed, "upgrade_status_after": redact(us2).strip().splitlines()[:3],
               "verify": v})
    return result("U6-rollback-before-finalize", "PASS" if ok else "FAIL", ev,
                  "c1, the follower controller and gw-c run 0.1.0 on the schema-4 store and serve (PUTs acked "
                  "through gw-c, 0 violations); status --versions flags them; finalize refused naming a 0.1.0 "
                  "node or a process reading schema 4; schema still 4 after the return; verify clean")


def u7():
    time.sleep(100)  # heartbeats of the 0.1.0 processes stopped in U6 go stale (90 s)
    rc, o, e = admin("upgrade finalize --wait --timeout-secs 300", timeout=360)
    _, us, _ = admin("upgrade status")
    nodes = {n: node_status(IDS[n]).splitlines() for n in NODES}
    strict = all(any("write ownership strict" in l for l in s) for s in nodes.values())
    ok = (rc == 0 and "finalized" in o and "schema Some(5) schema_write Some(5)" in us
          and "protections mandatory" in us and strict)
    return result("U7-finalize", "PASS" if ok else "FAIL",
                  {"finalize": {"rc": rc, "out": redact(o + e).strip().splitlines()[-8:]},
                   "upgrade_status": redact(us).strip().splitlines(), "node_status": nodes},
                  "finalize exit 0 (`finalized`); schema 5 / schema_write 5, protections mandatory; every node strict")


def u8():
    ev = {}
    v0 = verify("u8-verify-all")
    c = client_sync("load", "u8", "--ops 300 --put-rate-mib 0", timeout=1200)
    recs = ledger("u8")
    bad = [r for r in recs if r["outcome"] in BAD or r["outcome"] == "VIOLATION"]
    mp = client_sync("multipart", "u8-mp", timeout=900)
    # 0.1.0 controller and gateway refused.
    BINS["ctl-old"] = BIN_V010
    BINS["gw-old"] = BIN_V010
    start("a", "ctl-old", ctl_cmd("ctl-old", http="127.0.0.1:9642"))
    start("c", "gw-old", gw_cmd("gw-old", listen=f"{ADDR['c']}:{GW_PORT + 1}", metrics="127.0.0.1:9631"))
    exited = {}
    t0 = time.time()
    while time.time() - t0 < 30 and len(exited) < 2:
        for h, p in (("a", "ctl-old"), ("c", "gw-old")):
            if p not in exited and not alive(h, p):
                exited[p] = round(time.time() - t0, 1)
        time.sleep(1)
    for h, p in (("a", "ctl-old"), ("c", "gw-old")):
        _, tail, _ = ssh(h, f"tail -3 {R}/logs/{p}.log")
        ev[f"{p}_log_tail"] = redact(tail).strip().splitlines()
        if alive(h, p):
            stop(h, p)
    ev["old_exited_after_s"] = exited
    # A 0.1.0 storage node at c1.
    cid = IDS["c1"]
    st0 = status()
    c1_groups = [g for g, x in st0["groups"].items() if cid in x["members"]]
    q_after = None
    with Load("u8-old-node"):
        time.sleep(5)
        t_old = restart_with("c", "n-c1", lambda: node_cmd("c1"), BIN_V010)
        while ts("c") - t_old < 60:
            state, _ = node_state(cid)
            if state == "quarantined":
                q_after = round(ts("c") - t_old, 1)
                break
            time.sleep(2)
        time.sleep(max(0, 40 - (ts("c") - t_old)))
        t_end = ts("c")
    rc, o, e = admin(f"node rejoin --node {cid} --check")
    ev["rejoin_check_0.1.0"] = {"rc": rc, "out": redact(o + e).strip().splitlines()}
    # Stopped: a chunk counted on the 0.1.0 node would show as not whole.
    stop("c", "n-c1")
    recs2 = ledger("u8-old-node")
    keys = acked_in(recs2, t_old, t_end)
    placed, on_c1, unread = no_chunk_on(keys, c1_groups)
    BINS["n-c1"] = BIN
    start("c", "n-c1", node_cmd("c1"))
    time.sleep(10)
    state = replace_and_rejoin(cid, c1_groups, ev)
    v = verify("u8-verify")
    viol2 = [r for r in recs2 if r["outcome"] == "VIOLATION"]
    ev.update({"verify_all_before": v0, "load": counts(recs), "bad_samples": samples(bad),
               "multipart_failed": mp["failed"], "quarantined_after_s": q_after, "c1_groups": c1_groups,
               "acked_while_0.1.0_node": len(keys), "chunk_groups_of_acked": placed,
               "acked_with_chunk_in_c1_groups": on_c1, "unreadable": unread, "ops_old_node": counts(recs2),
               "c1_state_after": state, "verify": v, "client": c})
    ok = (v0["ok"] and not bad and mp["ok"] and set(exited) == {"ctl-old", "gw-old"}
          and q_after is not None and keys and not on_c1 and not unread and not viol2
          and rc != 0 and "schema" in " ".join(ev["rejoin_check_0.1.0"]["out"])
          and ev["replaced_after_s"] is not None and ev["one_member_per_host_after_replace"]
          and ev["rejoin"]["rc"] == 0 and state == "active" and v["ok"])
    return result("U8-after-finalize", "PASS" if ok else "FAIL", ev,
                  "every key reads back; 300-op load 0 errors; multipart exact; 0.1.0 controller and gateway exit "
                  "within 30 s; a 0.1.0 node at c1 quarantined within 60 s and no PUT acked meanwhile has a chunk "
                  "in c1's groups; rejoin --check refuses it naming the schema; on 0.2.0 again its slots are replaced in "
                  "their domain, then rejoin --yes makes it active")


def u9():
    def rule(ev):
        return all((r.get("held") or {}).get("unclassified", 1) == 0 for r in ev["scrub_all"].values())
    return final_check("U9-final", rule)


UPGRADE = [("U1-data-0.1.0", u1), ("U2-storage-nodes", u2), ("U3-controllers", u3),
           ("U4-record-failure-domains", u4), ("U5-gateways", u5), ("U6-rollback-before-finalize", u6),
           ("U7-finalize", u7), ("U8-after-finalize", u8), ("U9-final", u9)]


def write_md(meta):
    lines = [f"# three-host validation run {RUN_ID} ({meta.get('scenario')})", "", f"source {meta.get('sha')}", ""]
    if meta.get("continue_on_fail"):
        lines += ["**Script debugging run (CONTINUE_ON_FAIL): not a validation; verdicts after the first FAIL "
                  "are not judged.**", ""]
    for r in RESULTS:
        lines += [f"## {r['step']}: {r['verdict']}", "", f"expected: {r['expected']}", "", "```json",
                  json.dumps(r["evidence"], indent=1, sort_keys=True)[:30000], "```", ""]
    save("results.md", "\n".join(lines))


def run(scenario):
    OUT.mkdir(parents=True, exist_ok=True)
    meta = {"run_id": RUN_ID, "scenario": scenario, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "sha": os.environ.get("SOURCE_SHA", "")}
    addresses()
    stopped = False
    first, steps = ("0-setup", MAIN) if scenario == "run" else ("U0-setup-0.1.0", UPGRADE)
    try:
        try:
            v = setup(first, old=scenario == "upgrade")
        except Exception as e:  # noqa: BLE001
            v = result(first, "FAIL", {"exception": redact(repr(e))[-1500:]})
        if v != "PASS":
            stopped = True
        only = os.environ.get("STOP_AFTER")
        # Script debugging only: go on after a FAIL to exercise the later
        # steps' code. Such a run is not a validation: every verdict after
        # the first FAIL is unjudged (results.md says so).
        go_on = bool(os.environ.get("CONTINUE_ON_FAIL"))
        meta["continue_on_fail"] = go_on
        for name, fn in steps:
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
            if v != "PASS" and not go_on:
                stopped = True
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
    if cmd in ("run", "upgrade"):
        run(cmd)
    elif cmd == "teardown":
        OUT.mkdir(parents=True, exist_ok=True)
        teardown()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
