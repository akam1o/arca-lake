#!/usr/bin/env python3
"""S3 client with an outcome ledger for the three-host validation
(benchmarks/linux/three-host-validate.py). Runs on the client host; only
the Python standard library.

Every key has a set of candidate states (bodies by SHA-256, or absent):
an acknowledged PUT or DELETE replaces the set, an operation whose outcome
is not known (5xx, timeout, connection lost after sending) adds to it, a
request refused before it was sent changes nothing. A GET must answer one
of the candidates (200 with a body of a candidate's length and SHA-256, or
404 when absent is a candidate); anything else is a violation.

  three-host-client.py load   STATE LEDGER PHASE GATEWAYS [options]
  three-host-client.py verify STATE LEDGER PHASE GATEWAYS [--collapse]
  three-host-client.py mkbucket GATEWAY

GATEWAYS: label=host:port,label=host:port. Labels, never addresses, are
written to the ledger. The summary is printed as one JSON object.
"""

import argparse
import hashlib
import http.client
import json
import os
import random
import socket
import sys
import threading
import time

BUCKET = "v3h"
# Sizes cover a few bytes past the small-object threshold (64 KiB) and the
# chunk size (4 MiB), and multi-chunk objects up to 16 MiB.
SIZES = [1024, 4097, 65535, 65537, 300_000, 1 << 20, (4 << 20) - 1, (4 << 20) + 1, 9 << 20, 16 << 20]
WEIGHTS = [4, 2, 1, 1, 2, 2, 1, 1, 1, 0.5]
ABSENT = "absent"


def body_for(seed, size):
    return random.Random(seed).randbytes(size)


def now():
    return time.time()


class Gateways:
    def __init__(self, spec):
        self.items = []
        for part in spec.split(","):
            label, hp = part.split("=", 1)
            host, port = hp.rsplit(":", 1)
            self.items.append((label, host, int(port)))


def request(gw, method, path, body=None, timeout=60.0):
    """(status, body_sha, body_len, outcome_hint, error). outcome_hint:
    'http' (an answer), 'refused' (not sent), 'unknown' (sent, no answer)."""
    _, host, port = gw
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        try:
            conn.connect()
        except (ConnectionRefusedError, OSError) as e:
            if isinstance(e, socket.timeout):
                return None, None, 0, "refused", f"connect timeout: {e}"
            return None, None, 0, "refused", f"connect: {type(e).__name__}: {e}"
        headers = {"Content-Length": str(len(body) if body is not None else 0)}
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        h = hashlib.sha256()
        n = 0
        while True:
            b = resp.read(1 << 20)
            if not b:
                break
            h.update(b)
            n += len(b)
        return resp.status, h.hexdigest(), n, "http", None
    except Exception as e:  # noqa: BLE001 - every failure is classified
        # Sent (or partly sent) without an answer: the outcome is unknown.
        return None, None, 0, "unknown", f"{type(e).__name__}: {e}"[:300]
    finally:
        conn.close()


class State:
    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            with open(path) as f:
                d = json.load(f)
        else:
            d = {"keys": {}, "put_bytes": 0, "seq": 0}
        self.keys = d["keys"]  # key -> list of candidates ("absent" or [sha, size])
        self.put_bytes = d["put_bytes"]
        self.seq = d["seq"]
        self.lock = threading.Lock()

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"keys": self.keys, "put_bytes": self.put_bytes, "seq": self.seq}, f)
        os.replace(tmp, self.path)

    def cands(self, key):
        return self.keys.get(key, [ABSENT])


def cand_match(cands, status, sha, n):
    if status == 404:
        return ABSENT in cands
    if status == 200:
        return any(c != ABSENT and c[0] == sha and c[1] == n for c in cands)
    return None  # not an answer about the content


class Runner:
    def __init__(self, args):
        self.args = args
        self.gws = Gateways(args.gateways).items
        self.state = State(args.state)
        self.ledger = open(args.ledger, "a")
        self.ledger_lock = threading.Lock()
        self.stop = threading.Event()
        self.rate_lock = threading.Lock()
        self.rate_next = now()

    def log(self, rec):
        rec["phase"] = self.args.phase
        with self.ledger_lock:
            self.ledger.write(json.dumps(rec, sort_keys=True) + "\n")
            self.ledger.flush()

    def throttle(self, nbytes):
        rate = self.args.put_rate_mib * (1 << 20)
        if rate <= 0:
            return
        with self.rate_lock:
            start = max(self.rate_next, now())
            self.rate_next = start + nbytes / rate
        d = start - now()
        if d > 0:
            time.sleep(d)

    def do_put(self, tid, gw, key, size):
        st = self.state
        with st.lock:
            st.seq += 1
            seed = f"{key}#{st.seq}#{self.args.phase}"
        body = body_for(seed, size)
        sha = hashlib.sha256(body).hexdigest()
        self.throttle(size)
        t0 = now()
        status, _, _, hint, err = request(gw, "PUT", f"/{BUCKET}/{key}", body, self.args.timeout)
        t1 = now()
        if hint == "http" and 200 <= status < 300:
            outcome = "ack"
        elif hint == "refused":
            outcome = "refused"
        elif hint == "http":
            outcome = "error"
        else:
            outcome = "unknown"
        with st.lock:
            cur = st.cands(key)
            if outcome == "ack":
                st.keys[key] = [[sha, size]]
            elif outcome == "unknown" or (outcome == "error" and status is not None and status >= 500):
                st.keys[key] = cur + [[sha, size]]
            st.put_bytes += size
            n = len(st.keys.get(key, [ABSENT]))
        self.log({"t0": round(t0, 3), "t1": round(t1, 3), "tid": tid, "gw": gw[0], "op": "PUT", "key": key,
                  "size": size, "sha": sha, "status": status, "outcome": outcome, "err": err, "cands": n})
        return outcome

    def do_delete(self, tid, gw, key):
        st = self.state
        t0 = now()
        status, _, _, hint, err = request(gw, "DELETE", f"/{BUCKET}/{key}", None, self.args.timeout)
        t1 = now()
        if hint == "http" and 200 <= status < 300:
            outcome = "ack"
        elif hint == "refused":
            outcome = "refused"
        elif hint == "http":
            outcome = "error"
        else:
            outcome = "unknown"
        with st.lock:
            cur = st.cands(key)
            if outcome == "ack":
                st.keys[key] = [ABSENT]
            elif outcome == "unknown" or (outcome == "error" and status >= 500):
                if ABSENT not in cur:
                    st.keys[key] = cur + [ABSENT]
            n = len(st.keys.get(key, [ABSENT]))
        self.log({"t0": round(t0, 3), "t1": round(t1, 3), "tid": tid, "gw": gw[0], "op": "DELETE", "key": key,
                  "status": status, "outcome": outcome, "err": err, "cands": n})
        return outcome

    def do_get(self, tid, gw, key, collapse=False):
        st = self.state
        with st.lock:
            cands = list(st.cands(key))
        t0 = now()
        status, sha, n, hint, err = request(gw, "GET", f"/{BUCKET}/{key}", None, self.args.timeout)
        t1 = now()
        m = cand_match(cands, status, sha, n) if hint == "http" else None
        if m is True:
            outcome = "ok"
        elif m is False:
            outcome = "VIOLATION"
        elif hint == "refused":
            outcome = "refused"
        elif hint == "http":
            outcome = "error"
        else:
            outcome = "unknown"
        which = None
        if m is True:
            if status == 404:
                which = "absent"
            else:
                idx = next(i for i, c in enumerate(cands) if c != ABSENT and c[0] == sha and c[1] == n)
                which = "latest" if idx == len(cands) - 1 else f"earlier-{len(cands) - 1 - idx}"
            if collapse and len(cands) > 1:
                with st.lock:
                    st.keys[key] = [ABSENT] if status == 404 else [[sha, n]]
        self.log({"t0": round(t0, 3), "t1": round(t1, 3), "tid": tid, "gw": gw[0], "op": "GET", "key": key,
                  "status": status, "len": n, "sha": sha, "outcome": outcome, "err": err, "cands": len(cands),
                  "resolved": which, "expected": None if m is not False else cands})
        return outcome

    def keys_of(self, tid):
        return [f"k{tid:02d}/{i:03d}" for i in range(self.args.keys_per_thread)]

    def worker(self, tid, deadline, ops):
        rnd = random.Random(f"{self.args.phase}/{tid}/{time.time()}")
        keys = self.keys_of(tid)
        done = 0
        # Every size through every gateway first (load with --cover).
        if self.args.cover:
            for gi, gw in enumerate(self.gws):
                for si, size in enumerate(SIZES):
                    if (gi * len(SIZES) + si) % self.args.threads != tid:
                        continue
                    key = keys[(gi * len(SIZES) + si) // self.args.threads % len(keys)]
                    self.do_put(tid, gw, key, size)
                    self.do_get(tid, gw, key)
                    done += 2
        while not self.stop.is_set() and now() < deadline and (ops == 0 or done < ops):
            gw = self.gws[rnd.randrange(len(self.gws))]
            key = keys[rnd.randrange(len(keys))]
            r = rnd.random()
            if r < self.args.put_frac and self.state.put_bytes < self.args.put_budget_mib * (1 << 20):
                self.do_put(tid, gw, key, rnd.choices(SIZES, WEIGHTS)[0])
            elif r < self.args.put_frac + self.args.del_frac:
                self.do_delete(tid, gw, key)
            else:
                self.do_get(tid, gw, key)
            done += 1
            if self.args.pause_ms:
                time.sleep(self.args.pause_ms / 1000)
            if done % 20 == 0:
                with self.state.lock:
                    self.state.save()

    def load(self):
        deadline = now() + self.args.duration if self.args.duration else float("inf")
        per = self.args.ops // self.args.threads if self.args.ops else 0
        ts = [threading.Thread(target=self.worker, args=(t, deadline, per)) for t in range(self.args.threads)]
        for t in ts:
            t.start()
        stop_file = self.args.stop_file
        while any(t.is_alive() for t in ts):
            if stop_file and os.path.exists(stop_file):
                self.stop.set()
            time.sleep(0.2)
        for t in ts:
            t.join()
        with self.state.lock:
            self.state.save()

    def verify(self):
        keys = sorted(self.state.keys)
        lock = threading.Lock()
        it = iter(enumerate(keys))

        def run(tid):
            while True:
                with lock:
                    try:
                        i, key = next(it)
                    except StopIteration:
                        return
                # Alternate gateways; a key whose GET is not an answer is
                # retried once through the other gateway.
                gw = self.gws[i % len(self.gws)]
                out = self.do_get(tid, gw, key, collapse=self.args.collapse)
                if out in ("error", "unknown", "refused"):
                    self.do_get(tid, self.gws[(i + 1) % len(self.gws)], key, collapse=self.args.collapse)

        ts = [threading.Thread(target=run, args=(t,)) for t in range(self.args.threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        with self.state.lock:
            self.state.save()


def mkbucket(spec):
    gw = Gateways(spec).items[0]
    status, _, _, hint, err = request(gw, "PUT", f"/{BUCKET}", b"", 30)
    print(json.dumps({"status": status, "hint": hint, "err": err}))
    return 0 if status in (200, 409) else 1


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "mkbucket":
        return mkbucket(sys.argv[2])
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["load", "verify"])
    p.add_argument("state")
    p.add_argument("ledger")
    p.add_argument("phase")
    p.add_argument("gateways")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--keys-per-thread", type=int, default=40)
    p.add_argument("--ops", type=int, default=0)
    p.add_argument("--duration", type=float, default=0)
    p.add_argument("--stop-file")
    p.add_argument("--put-frac", type=float, default=0.45)
    p.add_argument("--del-frac", type=float, default=0.15)
    p.add_argument("--put-rate-mib", type=float, default=2.0)
    p.add_argument("--put-budget-mib", type=float, default=2600)
    p.add_argument("--pause-ms", type=float, default=0)
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--cover", action="store_true")
    p.add_argument("--collapse", action="store_true")
    args = p.parse_args()
    r = Runner(args)
    t0 = now()
    if args.cmd == "load":
        r.load()
    else:
        r.verify()
    r.ledger.close()
    print(json.dumps({"phase": args.phase, "cmd": args.cmd, "seconds": round(now() - t0, 1),
                      "put_bytes_total": r.state.put_bytes, "keys": len(r.state.keys)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
