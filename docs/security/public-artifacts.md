# Public artifacts: what measurement data may contain

The repository carries raw measurement results (`benchmarks/linux/results/`,
`benchmarks/reports/`) so that every number in the RFCs can be traced. Those
files are produced on private validation hosts. This page defines the
boundary between what they may contain once committed and what they may
not, and how the boundary is enforced.

## Boundary

| Class | Public (kept) | Not public (removed before commit) |
| --- | --- | --- |
| Hosts | role names used by the harness (`a`, `b`, `c`, `d0`, `c2`, `gateway`) | hostnames, FQDNs, machine IDs, hardware serials |
| Addresses | loopback, unspecified, documentation ranges (RFC 5737), the namespace harness's own veth pair | every other IPv4/IPv6 address, with or without port |
| Link layer | — | MAC addresses |
| People | — | usernames, user-at-host forms, home directories (`/home/<name>`, `/Users/<name>`) |
| Paths | harness run directories under `/tmp`, repository-relative paths | anything under a home directory |
| Network interfaces | the interface *role* (`net0` = the first non-loopback interface, the benchmark link; `lo`) | real interface names (the kernel's `eth`-, `ens`-, `enp`-style names, bridges, veths) |
| Cluster identity | FoundationDB *version* | FoundationDB cluster description and ID, SSH keys, tokens |
| Environment facts | kernel release string, CPU count, memory sizes, device class labels, timestamps and dates, software versions | — |
| Measurements | every number, label, percentile, counter | — |

Kernel release and CPU count stay because the interpretation of a
measurement depends on them (RFC-009 §3 describes the environment in those
terms); they identify a distribution and a VM size, not a machine.

## Enforcement

1. Every measurement script (`two-host.sh`, `repeat.sh`, `diskbound.sh`,
   `netns-faults.sh`, `transport-matrix.sh`) runs
   `benchmarks/linux/sanitize.py` on its output directory before it
   returns. The script rewrites what it recognizes (sar headers, sar
   interface columns, `ps` output, admin status lines, FoundationDB
   cluster strings) and applies pattern rules to free text, then
   **verifies** that no pattern of the not-public classes remains and
   exits non-zero otherwise.
2. A private deny list of literal strings (your hostnames, usernames,
   domains) can be kept *outside* the repository at
   `~/.config/arca-lake/private-identifiers.txt` (or
   `$ARCA_PRIVATE_IDENTIFIERS`); the verifier also fails on any of them.
   Never commit that file.
3. Before every commit that adds results, run
   `python3 -I benchmarks/linux/sanitize.py --check benchmarks/linux/results docs`
   and review `git diff --stat`.
4. CI (`secret scan` job) runs the same `--check` over `benchmarks/` and
   `docs/` on every push, next to the gitleaks history scan for
   credentials.

## What to do if something slipped through

- Remove it from the working tree and commit the fix. Do not rewrite
  published history on your own: the only accepted force-push is of a
  commit that nobody else can have fetched yet, and even then record it
  in the commit message of the follow-up.
- A hostname or address that reached a remote is an information leak,
  not a credential: note it in the risk register and move on. A
  credential (key, token, password) is revoked or rotated immediately,
  independent of any history rewrite.
- GitHub keeps unreachable commits fetchable by hash until its garbage
  collection runs; for a private repository that means collaborators,
  for a public one anybody with the hash. Ask GitHub support to purge if
  the content warrants it.

## Record

- 2026-11-01: one commit with sar headers naming three validation hosts
  was pushed to `main` and replaced one minute later by an amended commit
  (the repository was private; no credential was involved). The sanitizer
  had not run on that directory because the measurement script aborted
  before its last step. The verifier step and the CI check above were
  added as a result.
