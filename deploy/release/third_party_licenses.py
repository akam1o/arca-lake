#!/usr/bin/env python3
"""Write the third-party licence notices of the arca-lake binary.

The crates are those linked into the binary: ``cargo tree`` of the
``arca-lake`` package for the release target and features, normal edges
only (build and dev dependencies and proc macros are not linked). For each crate the
SPDX expression from its manifest and the text of every licence or notice
file it ships (LICENSE*, LICENCE*, COPYING*, NOTICE*, COPYRIGHT*) are
written. Crates shipping no such file are listed at the top; their terms
are their SPDX expression.

    python3 -I deploy/release/third_party_licenses.py OUT [--target T] [--features F]

Exits non-zero if a crate's licence is outside deny.toml's allow list.
"""

import argparse
import json
import os
import subprocess
import sys
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MIT = """Copyright (c) the authors named in the entry

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
PREFIXES = ("license", "licence", "copying", "notice", "copyright")


def cargo(*args):
    return subprocess.run(["cargo", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def shipped(target, features):
    out = cargo("tree", "--locked", "-p", "arca-lake", "--features", features, "--target", target,
                "-e", "normal,no-proc-macro", "--prefix", "none", "--format", "{p}")
    crates = set()
    for line in out.splitlines():
        parts = line.replace(" (*)", "").split()
        if len(parts) >= 2 and parts[1].startswith("v"):
            crates.add((parts[0], parts[1][1:]))
    return crates


def allowed_terms():
    with open(os.path.join(ROOT, "deny.toml"), "rb") as f:
        return set(tomllib.load(f)["licenses"]["allow"])


def terms_ok(expr, allow):
    # Each alternative of an OR must be checked only if no other is allowed;
    # an AND needs every part. Good enough for the expressions crates use.
    expr = expr.replace("/", " OR ").replace("(", " ").replace(")", " ")
    for alt in expr.split(" OR "):
        parts = [p.strip() for p in alt.split(" AND ")]
        if all(p in allow for p in parts if p):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--target", default="x86_64-unknown-linux-gnu")
    ap.add_argument("--features", default="fdb")
    a = ap.parse_args()
    want = shipped(a.target, a.features)
    meta = json.loads(cargo("metadata", "--locked", "--all-features", "--format-version", "1", "--filter-platform", a.target))
    members = set(meta["workspace_members"])
    pkgs = {(p["name"], p["version"]): p for p in meta["packages"]}
    allow = allowed_terms()
    entries, missing, bad = [], [], []
    for key in sorted(want):
        p = pkgs[key]
        if p["id"] in members:
            continue
        expr = p.get("license") or ""
        if not expr or not terms_ok(expr, allow):
            bad.append(f"{key[0]} {key[1]}: {expr or 'no licence expression'}")
        d = os.path.dirname(p["manifest_path"])
        files = sorted(f for f in os.listdir(d)
                       if f.lower().startswith(PREFIXES) and os.path.isfile(os.path.join(d, f)))
        if p.get("license_file"):
            lf = os.path.relpath(os.path.join(d, p["license_file"]), d)
            if lf not in files and os.path.isfile(os.path.join(d, lf)):
                files.append(lf)
        texts = []
        for f in files:
            with open(os.path.join(d, f), encoding="utf-8", errors="replace") as fh:
                texts.append((f, fh.read().rstrip() + "\n"))
        if not texts:
            missing.append(f"{key[0]} {key[1]} ({expr})")
        entries.append((key, expr, p.get("repository") or "", p.get("authors") or [], texts))
    with open(a.out, "w", encoding="utf-8") as o:
        o.write("Third-party software in the arca-lake binary\n")
        o.write("=" * 44 + "\n\n")
        o.write(f"Target {a.target}, features {a.features}; {len(entries)} crates linked "
                "(cargo tree, normal dependencies).\n")
        o.write("The FoundationDB client library (libfdb_c, Apache-2.0) is linked dynamically and is "
                "not included: install it from FoundationDB's packages.\n\n")
        if missing:
            o.write("Crates that ship no licence file (their terms are the SPDX expression):\n")
            o.writelines(f"  {m}\n" for m in missing)
            o.write("\n")
        for (name, ver), expr, repo, authors, texts in entries:
            o.write("-" * 72 + "\n")
            o.write(f"{name} {ver}\nLicense: {expr}\n")
            if repo:
                o.write(f"Repository: {repo}\n")
            for f, t in texts:
                o.write(f"\n--- {f} ---\n{t}")
            if not texts:
                o.write(f"Copyright: {', '.join(authors) or 'the authors of ' + name}\n"
                        "No licence file in the package: the standard texts below apply.\n")
            o.write("\n")
        if missing:
            o.write("=" * 72 + "\nStandard texts for the crates above that ship no licence file\n\n")
            o.write("--- Apache License 2.0 ---\n")
            with open(os.path.join(ROOT, "LICENSE"), encoding="utf-8") as fh:
                o.write(fh.read().rstrip() + "\n\n")
            o.write("--- MIT License ---\n" + MIT)
    print(f"{len(entries)} crates, {len(missing)} without a licence file, {len(bad)} not allowed", file=sys.stderr)
    for b in bad:
        print(f"not allowed: {b}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
