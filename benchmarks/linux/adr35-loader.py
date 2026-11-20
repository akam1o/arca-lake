#!/usr/bin/env python3
"""Bounded S3 GET load for the ADR-0035 integration harness.

Closed loop: WORKERS requests in flight per gateway at most, each worker
capped at RATE / WORKERS requests per second. Every request is written as
one line `t_ms gateway key status bytes` (t_ms: this host's wall clock at
the response), so that the reads each gateway served can be counted
independently of the gateways' own counters.

  get   --endpoint NAME=URL ... --bucket B --key K ... [--workers 2]
        [--rate 20] [--stop-file F] [--duration-secs S] --out FILE
  once  --endpoint NAME=URL --bucket B --key K --count N --out FILE
  put   --endpoint NAME=URL --bucket B --key K ... --size BYTES
  check --endpoint NAME=URL --bucket B --key K ... --size BYTES

Requires a gateway started with --allow-anonymous (test clusters only).
Standard library only.
"""

import argparse
import hashlib
import http.client
import os
import sys
import threading
import time
import urllib.parse


def body(key, size):
    seed = hashlib.sha256(key.encode()).digest()
    out = bytearray()
    while len(out) < size:
        seed = hashlib.sha256(seed).digest()
        out += seed
    return bytes(out[:size])


class Conn:
    def __init__(self, url):
        u = urllib.parse.urlsplit(url)
        self.host, self.port = u.hostname, u.port or 80
        self.c = None

    def request(self, method, path, data=None):
        for attempt in range(2):
            try:
                if self.c is None:
                    self.c = http.client.HTTPConnection(self.host, self.port, timeout=10)
                headers = {"Content-Length": str(len(data))} if data is not None else {}
                self.c.request(method, path, body=data, headers=headers)
                r = self.c.getresponse()
                payload = r.read()
                return r.status, payload
            except (OSError, http.client.HTTPException):
                self.c = None
                if attempt == 1:
                    return 0, b""
        return 0, b""


def endpoints(args):
    out = []
    for e in args.endpoint:
        name, url = e.split("=", 1)
        out.append((name, url))
    return out


def cmd_get(args):
    eps = endpoints(args)
    lock = threading.Lock()
    out = open(args.out, "a", buffering=1)
    stop = threading.Event()
    deadline = time.time() + args.duration_secs if args.duration_secs else None
    per_worker = args.rate / args.workers if args.rate else 0

    def worker(name, url, offset):
        c = Conn(url)
        i = offset
        while not stop.is_set():
            key = args.key[i % len(args.key)]
            i += args.workers
            t0 = time.time()
            status, payload = c.request("GET", f"/{args.bucket}/{key}")
            t = int(time.time() * 1000)
            with lock:
                out.write(f"{t} {name} {key} {status} {len(payload)}\n")
            if per_worker:
                pause = 1.0 / per_worker - (time.time() - t0)
                if pause > 0:
                    stop.wait(pause)

    threads = [
        threading.Thread(target=worker, args=(n, u, w), daemon=True)
        for n, u in eps
        for w in range(args.workers)
    ]
    for t in threads:
        t.start()
    while True:
        if args.stop_file and os.path.exists(args.stop_file):
            break
        if deadline and time.time() >= deadline:
            break
        time.sleep(0.1)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    out.close()


def cmd_once(args):
    (name, url), = endpoints(args)
    c = Conn(url)
    with open(args.out, "a", buffering=1) as out:
        for key in args.key:
            for _ in range(args.count):
                status, payload = c.request("GET", f"/{args.bucket}/{key}")
                out.write(f"{int(time.time() * 1000)} {name} {key} {status} {len(payload)}\n")
                if status != 200:
                    print(f"GET {key}: {status}", file=sys.stderr)
                    sys.exit(1)


def cmd_put(args):
    (name, url), = endpoints(args)
    c = Conn(url)
    status, _ = c.request("PUT", f"/{args.bucket}")
    if status not in (200, 409):
        print(f"create bucket: {status}", file=sys.stderr)
    for key in args.key:
        for attempt in range(20):
            status, payload = c.request("PUT", f"/{args.bucket}/{key}", body(key, args.size))
            if status == 200:
                break
            time.sleep(1)
        else:
            print(f"PUT {key}: {status} {payload[:200]!r}", file=sys.stderr)
            sys.exit(1)


def cmd_check(args):
    bad = 0
    for name, url in endpoints(args):
        c = Conn(url)
        for key in args.key:
            status, payload = c.request("GET", f"/{args.bucket}/{key}")
            if status != 200 or payload != body(key, args.size):
                bad += 1
                print(f"{name} {key}: status {status}, {len(payload)} bytes", file=sys.stderr)
    print(f"checked {len(args.key) * len(args.endpoint)} reads, {bad} bad")
    sys.exit(1 if bad else 0)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["get", "once", "put", "check"])
    p.add_argument("--endpoint", action="append", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--key", action="append", required=True)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--rate", type=float, default=20.0, help="per gateway, requests/s")
    p.add_argument("--stop-file")
    p.add_argument("--duration-secs", type=float, default=0)
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--size", type=int, default=65536)
    p.add_argument("--out", default="/dev/null")
    args = p.parse_args()
    {"get": cmd_get, "once": cmd_once, "put": cmd_put, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
