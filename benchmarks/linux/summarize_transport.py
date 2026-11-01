#!/usr/bin/env python3
"""Tabulate transport-matrix.sh output: one row per client run with the
server run's CPU next to it (matched by label).

Usage: summarize_transport.py <out>/transport.jsonl
"""

import json
import sys


def main():
    rows = [json.loads(l) for l in open(sys.argv[1], encoding="utf-8") if l.strip().startswith("{")]
    clients = {r["label"]: r for r in rows if r.get("role") == "client"}
    servers = {r["label"][len("server-"):]: r for r in rows if r.get("role") == "server"}
    print("| transport | tls | conns | streams | frame | op | MiB/s | p50 ms | p99 ms | client CPU % | client CPU-s/GiB | server CPU % | server CPU-s/GiB |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for label in sorted(clients):
        c = clients[label]
        s = servers.get(label)
        gib = c["bytes"] / float(1 << 30) if c["bytes"] else None
        def cpu_s(r):
            return None if r is None else r.get("cpu_user_s", 0.0) + r.get("cpu_sys_s", 0.0)
        def per_gib(r):
            v = cpu_s(r)
            return "n/a" if v is None or not gib else f"{v / gib:.2f}"
        parts = label.split("-")
        transport, tls, conns, conc, frame, op = parts[0], parts[1][3:], parts[2][4:], parts[3][4:], parts[4][5:], parts[5]
        print(f"| {transport} | {tls} | {conns} | {conc} | {int(frame) // 1024} KiB | {op} | {c['throughput_mib_s']:.0f} | {c['latency_p50_ms']:.2f} | {c['latency_p99_ms']:.2f} | "
              f"{c.get('cpu_percent', 0):.0f} | {per_gib(c)} | {s.get('cpu_percent', 0) if s else 'n/a'} | {per_gib(s)} |")


if __name__ == "__main__":
    main()
