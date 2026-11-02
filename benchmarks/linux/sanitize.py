#!/usr/bin/env python3
"""Remove environment-specific identifiers from measurement artifacts, then
verify that none remain.

The boundary between what a committed artifact may contain and what it may
not is defined in docs/security/public-artifacts.md; this script is the
mechanical part of it. It is structure-aware where the artifact's format
makes an identifier recognizable (sar headers, sar interface columns, ps
output, admin status lines, FoundationDB cluster strings) and falls back to
pattern rules for free text. Measurement-relevant facts (kernel version,
CPU count, interface *role*, process memory, timestamps, every number) are
kept.

Usage:
  sanitize.py [--deny-list FILE] PATH...        rewrite in place, then verify
  sanitize.py --check [--deny-list FILE] PATH... verify only (exit 1 on findings)

A deny list is a text file of literal strings that must never appear (your
hostnames, usernames, domains, machine IDs). It lives outside the
repository; the default is $ARCA_PRIVATE_IDENTIFIERS or
~/.config/arca-lake/private-identifiers.txt if present. Findings are
reported redacted.
"""

import os
import re
import sys

TEXT_SUFFIXES = (".txt", ".jsonl", ".json", ".md", ".err", ".log", ".csv", ".tsv")

# Addresses that identify no environment (loopback, unspecified,
# documentation ranges, the namespace harness's own veth pair).
PUBLIC_ADDR = re.compile(
    r"^(127\.\d+\.\d+\.\d+|0\.0\.0\.0|255\.255\.255\.\d+|192\.0\.2\.\d+|198\.51\.100\.\d+|203\.0\.113\.\d+|10\.77\.0\.[12])$"
)

IPV4 = re.compile(r"(?<![\w.])(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?![\w.])")
IPV6 = re.compile(r"(?<![\w:])((?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,6}:(?:[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,5})?)(?![\w:])")
MAC = re.compile(r"\b([0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5})\b")
USER_AT_HOST = re.compile(r"\b([A-Za-z_][A-Za-z0-9_.-]*)@([A-Za-z0-9][A-Za-z0-9.-]*)\b")
HOME_PATH = re.compile(r"(/home|/Users)/([A-Za-z_][A-Za-z0-9_.-]*)")
SAR_HEADER = re.compile(r"^(Linux \S+ )\(([^)]*)\)")
FDB_CLUSTER = re.compile(r"\b([A-Za-z0-9_]+):([A-Za-z0-9_]+)@(?=[\w<.:,]+)")
SSH_KEY = re.compile(r"\b(ssh-(?:rsa|ed25519|dss)|ecdsa-sha2-nistp\d+) [A-Za-z0-9+/=]{20,}")
MACHINE_ID = re.compile(r"\b[0-9a-f]{32}\b")
HOSTNAME_LINE = re.compile(r"^(hostname|host|nodename)\s*[:=]\s*([A-Za-z0-9<][A-Za-z0-9.>-]*)\s*$", re.IGNORECASE)
# Linux interface names other than loopback, as they appear in sar -n DEV
# and ip/ifconfig output. Mapped to net0, net1, ... per file in order of
# first appearance so that the role (first interface = the benchmark link)
# survives while the real name does not.
IFACE = re.compile(r"\b(eth\d+|en[ops]\d+[a-z0-9]*|ens\d+|eno\d+|wl[a-z0-9]+|bond\d+|br-[0-9a-f]+|br\d+|docker\d+|veth[0-9a-f]+|virbr\d+|tap\d+|tun\d+)\b")
# Everything above except the measurement-neutral kernel/CPU header.
PRIVATE_PATTERNS = {
    "ipv4": IPV4,
    "ipv6": IPV6,
    "mac": MAC,
    "user@host": USER_AT_HOST,
    "home path": HOME_PATH,
    "sar hostname": SAR_HEADER,
    "fdb cluster id": FDB_CLUSTER,
    "ssh key": SSH_KEY,
    "machine id": re.compile(r"\bmachine[-_ ]id\b.*\b[0-9a-f]{32}\b", re.IGNORECASE),
    "hostname line": HOSTNAME_LINE,
    "interface": IFACE,
}


def ipv4_sub(m):
    a = m.group(1)
    return a if PUBLIC_ADDR.match(a) else "<addr>"


def sanitize_text(text):
    out = []
    iface_map = {}
    for line in text.split("\n"):
        line = SAR_HEADER.sub(lambda m: m.group(1) + "(<host>)", line)
        line = HOSTNAME_LINE.sub(lambda m: f"{m.group(1)}: <host>", line)
        line = SSH_KEY.sub(lambda m: f"{m.group(1)} <key>", line)
        line = MAC.sub("<mac>", line)
        line = IPV6.sub("<addr6>", line)
        line = IPV4.sub(ipv4_sub, line)
        line = USER_AT_HOST.sub("<user>@<host>", line)
        line = HOME_PATH.sub(lambda m: f"{m.group(1)}/<user>", line)
        line = FDB_CLUSTER.sub("<cluster>:<cluster>@", line)
        line = re.sub(r"(?i)(machine[-_ ]id\b[^0-9a-f]*)[0-9a-f]{32}", r"\1<machine-id>", line)

        def iface_sub(m):
            name = m.group(1)
            if name not in iface_map:
                iface_map[name] = f"net{len(iface_map)}"
            return iface_map[name]

        line = IFACE.sub(iface_sub, line)
        out.append(line)
    return "\n".join(out)


def findings(text, deny):
    found = []
    for n, line in enumerate(text.split("\n"), 1):
        for name, pat in PRIVATE_PATTERNS.items():
            for m in pat.finditer(line):
                if name == "ipv4" and PUBLIC_ADDR.match(m.group(1)):
                    continue
                if name == "sar hostname" and m.group(2) == "<host>":
                    continue
                if name == "hostname line" and m.group(2) == "<host>":
                    continue
                found.append((n, name, redact(m.group(0))))
        for word in deny:
            if word and word in line:
                found.append((n, "deny list", redact(word)))
    return found


def redact(s):
    return s[:2] + "…" + s[-1:] if len(s) > 4 else "…"


def load_deny(path):
    if path is None:
        path = os.environ.get("ARCA_PRIVATE_IDENTIFIERS") or os.path.expanduser(
            "~/.config/arca-lake/private-identifiers.txt"
        )
    if not os.path.exists(path):
        return []
    return [l.strip() for l in open(path, encoding="utf-8") if l.strip() and not l.startswith("#")]


def files_under(paths):
    for p in paths:
        if os.path.isfile(p):
            yield p
            continue
        for root, dirs, names in os.walk(p):
            dirs[:] = [d for d in dirs if d not in (".git", "target", "__pycache__")]
            for n in names:
                if n.endswith(TEXT_SUFFIXES):
                    yield os.path.join(root, n)


def main(argv):
    check = False
    deny_path = None
    paths = []
    i = 0
    while i < len(argv):
        if argv[i] == "--check":
            check = True
        elif argv[i] == "--deny-list":
            deny_path = argv[i + 1]
            i += 1
        else:
            paths.append(argv[i])
        i += 1
    if not paths:
        sys.exit(__doc__)
    deny = load_deny(deny_path)
    changed = 0
    bad = 0
    for f in files_under(paths):
        try:
            text = open(f, encoding="utf-8", errors="replace").read()
        except OSError as e:
            print(f"{f}: {e}", file=sys.stderr)
            bad += 1
            continue
        if not check:
            new = sanitize_text(text)
            if new != text:
                open(f, "w", encoding="utf-8").write(new)
                changed += 1
                text = new
        for n, kind, what in findings(text, deny):
            print(f"{f}:{n}: {kind}: {what}", file=sys.stderr)
            bad += 1
    mode = "checked" if check else f"sanitized ({changed} files rewritten)"
    print(f"{mode}; findings: {bad}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main(sys.argv[1:])
