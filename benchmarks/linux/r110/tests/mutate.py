"""Mutation checks of the R-110 tooling tests: each protection removed in
turn, the tests run, the file restored. Every line must report FAILED (or
TIMEOUT). Run from benchmarks/linux: python3 -I r110/tests/mutate.py
(it edits the sources in place and restores them; run it on a clean
tree)."""
import subprocess, sys
muts = [
 ("token bucket bypassed", "r110/budget.py", "if self._tokens >= 1.0 - 1e-9:", "if True:"),
 ("attempt limit removed", "r110/budget.py", 'if self._attempts + 1 > self.limits["max_attempts"]:', "if False:"),
 ("body budget removed", "r110/budget.py", 'elif self._body + body_bytes > self.limits["max_body_bytes"]:', "elif False:"),
 ("concurrency slot removed", "r110/budget.py", 'if self._inflight < self.limits["concurrency"]:', "if True:"),
 ("live limit removed", "r110/budget.py", 'if new_live and self._live + 1 > self.limits["max_live"]:', "if False:"),
 ("reject pause removed", "r110/load.py", "                    if self.clock.sleep(delay, self.budget._cancel):\n                        return\n                else:", "                    pass\n                else:"),
 ("ledger failure ignored", "r110/load.py", '''            try:
                self.ledger.write(rec)
            except ledger_mod.LedgerFailure:
                self.stop("ledger-failure")
                raise budget_mod.Stopped("ledger-failure") from None''', '''            try:
                self.ledger.write(rec)
            except ledger_mod.LedgerFailure:
                pass'''),
 ("unknown taken as not applied", "r110/ledger.py", "        else:\n            cur.add(new)\n    return state", "        else:\n            pass\n    return state"),
 ("stop file ignored", "r110/budget.py", "        rec = self.stop_file.reason()\n", "        rec = None\n"),
 ("resume allowed", "r110/budget.py", "os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)\n        except FileExistsError:\n            os.close(fd)", "os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)\n        except FileExistsError:\n            os.close(fd)"),
]
for name, path, old, new in muts:
    src = open(path).read()
    assert src.count(old) == 1, (name, src.count(old))
    open(path, "w").write(src.replace(old, new))
    try:
        r = subprocess.run([sys.executable, "-I", "-W", "ignore", "-m", "unittest", "discover", "-s", "r110/tests", "-t", "."],
                           capture_output=True, text=True, timeout=240)
        last = [l for l in r.stderr.splitlines() if l.startswith(("OK", "FAILED"))]
        print(f"{name}: {last[-1] if last else 'no result'}")
    except subprocess.TimeoutExpired:
        print(f"{name}: TIMEOUT (counts as detected: the run did not pass)")
    finally:
        open(path, "w").write(src)
