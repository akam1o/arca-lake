import subprocess, sys, json, os
t = int(sys.argv[1])
F = os.environ["F"]
def keys(prefix_hex):
    begin = prefix_hex
    end = prefix_hex[:-4] + "\\x01"  # tuple string terminator \x00 -> \x01
    cmd = ["timeout", "120", "fdbcli", "-C", F + "/fdb.cluster",
           "--tls-certificate-file", F + "/pki/admin/cert.pem",
           "--tls-key-file", F + "/pki/admin/key.pem",
           "--tls-ca-file", F + "/pki/ca/ca.pem", "--timeout", "60",
           "--exec", f"getrangekeys {begin} {end} 1000000"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=150)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    return [l for l in r.stdout.splitlines() if l.startswith("`")]
def unescape(s):
    out, i = bytearray(), 0
    while i < len(s):
        if s[i] == "\\" and s[i+1:i+2] == "x":
            out.append(int(s[i+2:i+4], 16)); i += 4
        elif s[i] == "\\" and s[i+1:i+2] == "\\":
            out.append(0x5c); i += 2
        else:
            out.append(ord(s[i])); i += 1
    return bytes(out)
def tuple_int(b, i):
    code = b[i]
    if code == 0x14: return 0, i + 1
    if 0x14 < code <= 0x1c:
        n = code - 0x14
        return int.from_bytes(b[i+1:i+1+n], "big"), i + 1 + n
    return None, i
res = {"t": t}
P = "\\x02al\\x00\\x02{}\\x00"
for name in ("gc", "pend", "repair", "mpu"):
    ks = keys(P.format(name))
    res[name] = None if ks is None else len(ks)
    if name == "gc" and ks:
        b = unescape(ks[0][1:ks[0].rindex("'")])
        prefix = len(b"\x02al\x00\x02gc\x00")
        v, _ = tuple_int(b, prefix)
        if v is not None:
            res["gc_oldest_age_s"] = max(0, t - v // 1000)
print(json.dumps(res))
