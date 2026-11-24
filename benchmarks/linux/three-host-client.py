#!/usr/bin/env python3
"""S3 client with an outcome ledger for the three-host validation
(benchmarks/linux/three-host-validate.py). Runs on the client host; the
Python standard library, and botocore for SigV4 signing when `--creds` is
given (the 0.2.0 validation signs every request; without botocore the
client exits non-zero, it never falls back to unsigned requests).

Every key has a set of candidate states (bodies by SHA-256, or absent):
an acknowledged PUT or DELETE replaces the set, an operation whose outcome
is not known (5xx, timeout, connection lost after sending) adds to it, a
request refused before it was sent changes nothing. A GET must answer one
of the candidates (200 with a body of a candidate's length and SHA-256, or
404 when absent is a candidate); anything else is a violation.

  three-host-client.py load   STATE LEDGER PHASE GATEWAYS [options]
  three-host-client.py verify STATE LEDGER PHASE GATEWAYS [--collapse]
  three-host-client.py mkbucket GATEWAY [--creds FILE]
  three-host-client.py auth-check STATE LEDGER PHASE GATEWAYS --creds FILE
  three-host-client.py multipart  STATE LEDGER PHASE GATEWAYS --creds FILE

GATEWAYS: label=host:port,label=host:port. Labels, never addresses, are
written to the ledger. The summary is printed as one JSON object. FILE is
JSON {"owner": [access key, secret], "other": [access key, secret]};
secrets are never printed or written to the ledger.
"""

import argparse
import base64
import hashlib
import http.client
import json
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import zlib

BUCKET = "v3h"
REGION = "us-east-1"
# (access key, secret) every request is signed with; None: unsigned.
CREDS = None
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


def sign_headers(method, host, port, path, query, body, creds, headers):
    """SigV4 headers (botocore's S3 signer: payload SHA-256 signed over
    plain HTTP) for the request as it will be sent."""
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    url = f"http://{host}:{port}{path}" + (f"?{query}" if query else "")
    req = AWSRequest(method=method, url=url, data=body or b"", headers=dict(headers))
    S3SigV4Auth(Credentials(creds[0], creds[1]), "s3", REGION).add_auth(req)
    return dict(req.headers.items())


def call(gw, method, path, body=None, timeout=60.0, query="", creds="default", headers=None,
         keep_body=False, send_body=None):
    """One request. Returns (status, body_sha, body_len, outcome_hint, error,
    content) where outcome_hint is 'http' (an answer), 'refused' (not sent)
    or 'unknown' (sent, no answer); content is the response body when
    keep_body (else None). `creds` "default" uses CREDS; None is unsigned.
    `send_body` replaces the signed body on the wire (a tampered request)."""
    _, host, port = gw
    if creds == "default":
        creds = CREDS
    hdrs = dict(headers or {})
    if creds is not None:
        hdrs = sign_headers(method, host, port, path, query, body, creds, hdrs)
    wire = send_body if send_body is not None else body
    hdrs["Content-Length"] = str(len(wire) if wire is not None else 0)
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        try:
            conn.connect()
        except (ConnectionRefusedError, OSError) as e:
            if isinstance(e, socket.timeout):
                return None, None, 0, "refused", f"connect timeout: {e}", None
            return None, None, 0, "refused", f"connect: {type(e).__name__}: {e}", None
        conn.request(method, path + (f"?{query}" if query else ""), body=wire, headers=hdrs)
        resp = conn.getresponse()
        h = hashlib.sha256()
        n = 0
        kept = []
        while True:
            b = resp.read(1 << 20)
            if not b:
                break
            h.update(b)
            n += len(b)
            if keep_body:
                kept.append(b)
        content = {"body": b"".join(kept), "headers": {k.lower(): v for k, v in resp.getheaders()}} \
            if keep_body else None
        return resp.status, h.hexdigest(), n, "http", None, content
    except Exception as e:  # noqa: BLE001 - every failure is classified
        # Sent (or partly sent) without an answer: the outcome is unknown.
        return None, None, 0, "unknown", f"{type(e).__name__}: {e}"[:300], None
    finally:
        conn.close()


def request(gw, method, path, body=None, timeout=60.0):
    """(status, body_sha, body_len, outcome_hint, error), signed with CREDS."""
    return call(gw, method, path, body, timeout)[:5]


def xml_tag(content, tag):
    body = content["body"] if content else b""
    m = re.search(rf"<{tag}>(.*?)</{tag}>", body.decode("utf-8", "replace"), re.S)
    return m.group(1) if m else None


def b64_md5(data):
    return base64.b64encode(hashlib.md5(data).digest()).decode()


def b64_crc32(data):
    """`x-amz-checksum-crc32`: base64 of the big-endian CRC-32."""
    return base64.b64encode(struct.pack(">I", zlib.crc32(data) & 0xFFFFFFFF)).decode()


def delete_xml(keys):
    objs = "".join(f"<Object><Key>{k}</Key></Object>" for k in keys)
    return f'<?xml version="1.0" encoding="UTF-8"?><Delete>{objs}</Delete>'.encode()


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


    # --- 0.2.0 checks: SigV4 and ownership, multipart -------------------

    def expect(self, cases, name, gw, res, statuses, codes=None):
        """Record one checked request: its status and S3 error code must be
        among the expected ones."""
        status, _, _, hint, err, content = res
        code = xml_tag(content, "Code")
        ok = hint == "http" and status in statuses and (codes is None or code in codes)
        cases.append({"case": name, "gw": gw[0], "status": status, "code": code, "hint": hint,
                      "err": err, "expect": [sorted(statuses), sorted(codes, key=str) if codes else None], "ok": ok})
        self.log({"t0": round(now(), 3), "t1": round(now(), 3), "tid": 0, "gw": gw[0], "op": "CHECK",
                  "key": name, "status": status, "outcome": "ok" if ok else "VIOLATION", "err": code})
        return ok

    def note_refused_put(self, key, res, body):
        """A PUT expected to be refused: if it was taken (2xx) or its outcome
        is unknown (5xx, no answer), the ledger state follows, so a later
        GET is judged against what may be stored."""
        cand = [hashlib.sha256(body).hexdigest(), len(body)]
        with self.state.lock:
            if res[3] == "http" and 200 <= res[0] < 300:
                self.state.keys[key] = [cand]
            elif res[3] == "unknown" or (res[3] == "http" and res[0] >= 500):
                self.state.keys[key] = self.state.cands(key) + [cand]

    def note_delete_objects(self, keys, res):
        """As `note_refused_put` for a DeleteObjects of `keys`."""
        with self.state.lock:
            for k in keys:
                cur = self.state.cands(k)
                if res[3] == "http" and 200 <= res[0] < 300:
                    self.state.keys[k] = [ABSENT]
                elif (res[3] == "unknown" or (res[3] == "http" and res[0] >= 500)) and ABSENT not in cur:
                    self.state.keys[k] = cur + [ABSENT]

    def auth_check(self, creds):
        owner, other = tuple(creds["owner"]), tuple(creds["other"])
        unknown = ("AKIAV3HUNKNOWN000000", "0" * 40)
        wrong = (owner[0], "1" * 40)
        cases = []
        for gi, gw in enumerate(self.gws):
            key = f"auth/{gw[0]}"
            path = f"/{BUCKET}/{key}"
            # Owner: signed PUT, GET, HEAD (through the ledger), DELETE of a
            # scratch key.
            cases.append({"case": "owner PUT", "gw": gw[0], "ok": self.do_put(0, gw, key, 4097) == "ack"})
            cases.append({"case": "owner GET", "gw": gw[0], "ok": self.do_get(0, gw, key) == "ok"})
            self.expect(cases, "owner HEAD", gw, call(gw, "HEAD", path, keep_body=True), {200})
            scratch = f"auth/{gw[0]}-scratch"
            cases.append({"case": "owner PUT scratch", "gw": gw[0],
                          "ok": self.do_put(0, gw, scratch, 1024) == "ack"})
            cases.append({"case": "owner DELETE scratch", "gw": gw[0],
                          "ok": self.do_delete(0, gw, scratch) == "ack"})
            # Anonymous.
            self.expect(cases, "unsigned GET", gw, call(gw, "GET", path, creds=None, keep_body=True),
                        {403}, {"AccessDenied"})
            self.expect(cases, "unsigned PUT", gw,
                        call(gw, "PUT", f"/{BUCKET}/auth/{gw[0]}-anon", b"x" * 100, creds=None, keep_body=True),
                        {403}, {"AccessDenied"})
            cases.append({"case": "unsigned PUT not stored", "gw": gw[0],
                          "ok": self.do_get(0, gw, f"auth/{gw[0]}-anon") == "ok"})
            # Bad credentials.
            self.expect(cases, "unknown access key", gw, call(gw, "GET", path, creds=unknown, keep_body=True),
                        {403}, {"InvalidAccessKeyId"})
            self.expect(cases, "wrong secret", gw, call(gw, "GET", path, creds=wrong, keep_body=True),
                        {403}, {"SignatureDoesNotMatch"})
            # The body changed after signing: refused, the key unchanged.
            signed = body_for(f"tamper-signed-{gw[0]}", 4097)
            sent = body_for(f"tamper-sent-{gw[0]}", 4097)
            res = call(gw, "PUT", path, signed, keep_body=True, send_body=sent)
            self.note_refused_put(key, res, sent)
            self.expect(cases, "body changed after signing", gw, res, {400, 403},
                        {"XAmzContentSHA256Mismatch", "SignatureDoesNotMatch", "BadDigest"})
            cases.append({"case": "tampered PUT not stored", "gw": gw[0], "ok": self.do_get(0, gw, key) == "ok"})
            # Added after the rehearsal for features merged since: a
            # Content-MD5 that does not match the body.
            res = call(gw, "PUT", path, body_for(f"md5-{gw[0]}", 4097), keep_body=True,
                       headers={"Content-MD5": b64_md5(b"not the body")})
            self.note_refused_put(key, res, body_for(f"md5-{gw[0]}", 4097))
            self.expect(cases, "Content-MD5 mismatch", gw, res, {400}, {"BadDigest"})
            cases.append({"case": "Content-MD5 mismatch not stored", "gw": gw[0],
                          "ok": self.do_get(0, gw, key) == "ok"})
            # DeleteObjects with a wrong x-amz-checksum-crc32: nothing
            # deleted; then with the right one: both deleted.
            dkeys = [f"auth/{gw[0]}-del{i}" for i in (1, 2)]
            for k in dkeys:
                cases.append({"case": "owner PUT (to delete)", "gw": gw[0], "ok": self.do_put(0, gw, k, 1024) == "ack"})
            xml = delete_xml(dkeys)
            res = call(gw, "POST", f"/{BUCKET}", xml, query="delete", keep_body=True,
                       headers={"x-amz-checksum-crc32": b64_crc32(b"not the body")})
            self.note_delete_objects(dkeys, res)
            self.expect(cases, "DeleteObjects checksum mismatch", gw, res, {400}, {"BadDigest"})
            for k in dkeys:
                cases.append({"case": "DeleteObjects mismatch deleted nothing", "gw": gw[0],
                              "ok": self.do_get(0, gw, k) == "ok"})
            res = call(gw, "POST", f"/{BUCKET}", xml, query="delete", keep_body=True,
                       headers={"x-amz-checksum-crc32": b64_crc32(xml)})
            self.note_delete_objects(dkeys, res)
            self.expect(cases, "DeleteObjects right checksum", gw, res, {200})
            cases.append({"case": "DeleteObjects right checksum no per-key error", "gw": gw[0],
                          "ok": res[3] == "http" and xml_tag(res[5], "Error") is None
                          and (res[5]["body"].count(b"<Deleted>") == len(dkeys))})
            for k in dkeys:
                cases.append({"case": "DeleteObjects deleted", "gw": gw[0], "ok": self.do_get(0, gw, k) == "ok"})
            # Another principal on the owner's bucket.
            self.expect(cases, "other GET", gw, call(gw, "GET", path, creds=other, keep_body=True),
                        {403}, {"AccessDenied"})
            self.expect(cases, "other PUT", gw, call(gw, "PUT", path, b"y" * 100, creds=other, keep_body=True),
                        {403}, {"AccessDenied"})
            self.expect(cases, "other DELETE", gw, call(gw, "DELETE", path, creds=other, keep_body=True),
                        {403}, {"AccessDenied"})
            self.expect(cases, "other LIST", gw,
                        call(gw, "GET", f"/{BUCKET}", query="list-type=2", creds=other, keep_body=True),
                        {403}, {"AccessDenied"})
            cases.append({"case": "owner object unchanged", "gw": gw[0], "ok": self.do_get(0, gw, key) == "ok"})
            # The other principal's own bucket.
            ob = f"/{BUCKET}-other"
            if gi == 0:
                self.expect(cases, "other creates its bucket", gw, call(gw, "PUT", ob, b"", creds=other,
                                                                       keep_body=True), {200})
            else:
                self.expect(cases, "other re-creates its bucket", gw, call(gw, "PUT", ob, b"", creds=other,
                                                                          keep_body=True),
                            {200, 409}, {None, "BucketAlreadyOwnedByYou"})
            data = body_for(f"other-{gw[0]}", 2000)
            self.expect(cases, "other PUT own bucket", gw,
                        call(gw, "PUT", f"{ob}/o-{gw[0]}", data, creds=other, keep_body=True), {200})
            res = call(gw, "GET", f"{ob}/o-{gw[0]}", creds=other, keep_body=True)
            self.expect(cases, "other GET own bucket", gw, res, {200})
            cases.append({"case": "other GET own bucket exact", "gw": gw[0],
                          "ok": res[1] == hashlib.sha256(data).hexdigest()})
            self.expect(cases, "owner GET other's bucket", gw,
                        call(gw, "GET", f"{ob}/o-{gw[0]}", creds=owner, keep_body=True), {403}, {"AccessDenied"})
            self.expect(cases, "other creates the owner's bucket", gw,
                        call(gw, "PUT", f"/{BUCKET}", b"", creds=other, keep_body=True),
                        {409}, {"BucketAlreadyExists"})
        self.state.save()
        return {"cases": cases, "failed": [c for c in cases if not c["ok"]], "ok": all(c["ok"] for c in cases)}

    def multipart(self):
        """Create, parts (one uploaded twice), a refused complete, complete,
        read-back and ranges; then an aborted upload."""
        from urllib.parse import quote
        cases = []
        g = self.gws
        st = self.state
        with st.lock:
            st.seq += 1
            n = st.seq
        key = f"mp/{self.args.phase}-{n}"
        path = f"/{BUCKET}/{key}"
        mib = 1 << 20
        parts = {1: body_for(f"{key}#1", 5 * mib), "2a": body_for(f"{key}#2a", 5 * mib),
                 "2b": body_for(f"{key}#2b", 5 * mib), 3: body_for(f"{key}#3", 3 * mib + 1)}
        res = call(g[0], "POST", path, b"", query="uploads", keep_body=True)
        self.expect(cases, "create", g[0], res, {200})
        uid = xml_tag(res[5], "UploadId")
        if not uid:
            return {"cases": cases, "failed": cases, "ok": False}
        q = quote(uid, safe="")
        etags = {}
        for i, (pn, label) in enumerate([(1, 1), (2, "2a"), (2, "2b"), (3, 3)]):
            gw = g[(i + 1) % len(g)]
            res = call(gw, "PUT", path, parts[label], query=f"partNumber={pn}&uploadId={q}", keep_body=True,
                       timeout=120)
            self.expect(cases, f"upload part {label}", gw, res, {200})
            etags[label] = (res[5] or {}).get("headers", {}).get("etag")

        def complete_xml(labels):
            items = "".join(f"<Part><PartNumber>{pn}</PartNumber><ETag>{etags[lb]}</ETag></Part>"
                            for pn, lb in labels)
            return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()

        res = call(g[1 % len(g)], "POST", path, complete_xml([(1, 1), (2, "2a"), (3, 3)]),
                   query=f"uploadId={q}", keep_body=True)
        self.expect(cases, "complete with the replaced part's ETag", g[1 % len(g)], res, {400}, {"InvalidPart"})
        whole = parts[1] + parts["2b"] + parts[3]
        sha = hashlib.sha256(whole).hexdigest()
        t0 = now()
        res = call(g[0], "POST", path, complete_xml([(1, 1), (2, "2b"), (3, 3)]), query=f"uploadId={q}",
                   keep_body=True, timeout=180)
        t1 = now()
        status, hint = res[0], res[3]
        outcome = ("ack" if hint == "http" and status == 200 and xml_tag(res[5], "Error") is None
                   else "refused" if hint == "refused" else "error" if hint == "http" else "unknown")
        with st.lock:
            if outcome == "ack":
                st.keys[key] = [[sha, len(whole)]]
            elif outcome == "unknown" or (outcome == "error" and status >= 500):
                st.keys[key] = st.cands(key) + [[sha, len(whole)]]
            st.put_bytes += len(whole)
        self.log({"t0": round(t0, 3), "t1": round(t1, 3), "tid": 0, "gw": g[0][0], "op": "MPU", "key": key,
                  "size": len(whole), "sha": sha, "status": status, "outcome": outcome, "err": res[4],
                  "cands": len(st.cands(key))})
        cases.append({"case": "complete", "gw": g[0][0], "status": status, "ok": outcome == "ack"})
        for i, gw in enumerate(g):
            cases.append({"case": "read back", "gw": gw[0], "ok": self.do_get(0, gw, key) == "ok"})
        p1 = len(parts[1])
        ranges = [("0-99", 0, 100), (f"{p1 - 10}-{p1 + 9}", p1 - 10, 20),
                  (f"{2 * p1 + 1000}-{2 * p1 + 1999}", 2 * p1 + 1000, 1000), ("-100", len(whole) - 100, 100)]
        for i, (spec, off, ln) in enumerate(ranges):
            gw = g[i % len(g)]
            res = call(gw, "GET", path, headers={"Range": f"bytes={spec}"}, keep_body=True)
            ok = self.expect(cases, f"range {spec}", gw, res, {206})
            cases.append({"case": f"range {spec} exact", "gw": gw[0],
                          "ok": ok and res[5]["body"] == whole[off:off + ln]})
        # An aborted upload leaves nothing.
        akey = f"{key}-aborted"
        apath = f"/{BUCKET}/{akey}"
        res = call(g[1 % len(g)], "POST", apath, b"", query="uploads", keep_body=True)
        self.expect(cases, "create (to abort)", g[1 % len(g)], res, {200})
        auid = quote(xml_tag(res[5], "UploadId") or "none", safe="")
        res = call(g[0], "PUT", apath, body_for(f"{akey}#1", 5 * mib), query=f"partNumber=1&uploadId={auid}",
                   keep_body=True, timeout=120)
        self.expect(cases, "upload part (to abort)", g[0], res, {200})
        aetag = (res[5] or {}).get("headers", {}).get("etag")
        self.expect(cases, "abort", g[1 % len(g)],
                    call(g[1 % len(g)], "DELETE", apath, query=f"uploadId={auid}", keep_body=True), {204})
        self.expect(cases, "list parts after abort", g[0],
                    call(g[0], "GET", apath, query=f"uploadId={auid}", keep_body=True), {404}, {"NoSuchUpload"})
        body = (f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{aetag}</ETag></Part>"
                "</CompleteMultipartUpload>").encode()
        self.expect(cases, "complete after abort", g[0],
                    call(g[0], "POST", apath, body, query=f"uploadId={auid}", keep_body=True),
                    {404}, {"NoSuchUpload"})
        cases.append({"case": "aborted key absent", "gw": g[0][0], "ok": self.do_get(0, g[0], akey) == "ok"})
        st.save()
        return {"key": key, "size": len(whole), "cases": cases, "failed": [c for c in cases if not c["ok"]],
                "ok": all(c["ok"] for c in cases)}


def load_creds(path):
    """Set CREDS to the owner's key; returns the whole file. Fails without
    botocore: a signed check is never skipped."""
    global CREDS
    with open(path) as f:
        creds = json.load(f)
    import botocore  # noqa: F401 - required, see the module docstring
    CREDS = tuple(creds["owner"])
    return creds


def mkbucket(spec, creds_path=None):
    if creds_path:
        load_creds(creds_path)
    gw = Gateways(spec).items[0]
    status, _, _, hint, err = request(gw, "PUT", f"/{BUCKET}", b"", 30)
    print(json.dumps({"status": status, "hint": hint, "err": err}))
    return 0 if status in (200, 409) else 1


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "probe":
        # probe GATEWAYS KEY --creds FILE: a signed 2000-byte PUT and its
        # DELETE through each gateway; prints the statuses per label.
        load_creds(sys.argv[5])
        out = {}
        for gw in Gateways(sys.argv[2]).items:
            path = f"/{BUCKET}/{sys.argv[3]}-{gw[0]}"
            a = request(gw, "PUT", path, os.urandom(2000), 30)[0]
            b = request(gw, "DELETE", path, None, 30)[0]
            out[gw[0]] = f"{a} {b}"
        print(json.dumps(out))
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == "mkbucket":
        return mkbucket(sys.argv[2], sys.argv[4] if len(sys.argv) > 4 and sys.argv[3] == "--creds" else None)
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["load", "verify", "auth-check", "multipart"])
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
    p.add_argument("--creds")
    args = p.parse_args()
    creds = load_creds(args.creds) if args.creds else None
    if args.cmd in ("auth-check", "multipart") and not creds:
        sys.exit(f"{args.cmd} needs --creds")
    r = Runner(args)
    t0 = now()
    out = {}
    if args.cmd == "load":
        r.load()
    elif args.cmd == "verify":
        r.verify()
    elif args.cmd == "auth-check":
        import botocore
        out = r.auth_check(creds)
        out["botocore"] = botocore.__version__
    else:
        out = r.multipart()
    r.ledger.close()
    out.update({"phase": args.phase, "cmd": args.cmd, "seconds": round(now() - t0, 1),
                "put_bytes_total": r.state.put_bytes, "keys": len(r.state.keys), "signed": CREDS is not None})
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
