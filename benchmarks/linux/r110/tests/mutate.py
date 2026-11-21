"""Mutation checks of the R-110 tooling tests: each protection removed in
turn, the tests run, the file restored. Every line must report FAILED (or
TIMEOUT). Run from benchmarks/linux: python3 -I r110/tests/mutate.py
[test_module ...]
(it edits the sources in place and restores them; run it on a clean
tree)."""
import subprocess, sys
# (name, file, original, replacement[, test module]): the module defaults
# to test_load.
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
 ("load ignores the heartbeat", "r110/load.py", "                if self._hb.check():", "                if False:"),
 ("heartbeat replay accepted", "r110/common.py", '(self.seq is None or rec["seq"] > self.seq)', "True", "test_guard"),
 ("first heartbeat not awaited", "r110/common.py", "self.fresh_until = clock.monotonic() + timeout", "self.fresh_until = float('inf')", "test_guard"),
 ("service identity not checked", "r110/proc.py", "    if not matches(rec):\n        return \"other\"\n", "", "test_guard"),
 ("blind check passes", "r110/guard.py", "        except (OSError, ValueError):\n            return \"monitor-failed\", \"guard\"", "        except (OSError, ValueError):\n            pass", "test_guard"),
 ("load group not stopped", "r110/guard.py", "        empty = proc.stop_group(pgid, self.clock, child.poll)", "        empty = True", "test_guard"),
 ("TERM only", "r110/proc.py", "((signal.SIGTERM, term_wait), (signal.SIGKILL, kill_wait))", "((signal.SIGTERM, term_wait),)", "test_guard"),
 ("watchdog beats on a failed round", "r110/watchdog.py", "        if required_ok:\n            self.beat()", "        self.beat()", "test_guard"),
 ("watchdog takes a missing reading as healthy", "r110/watchdog.py", "            except KeyError as e:\n                return \"monitor-failed\"", "            except KeyError as e:\n                continue\n                return \"monitor-failed\"", "test_guard"),
]
for entry in muts:
    name, path, old, new = entry[:4]
    module = entry[4] if len(entry) > 4 else "test_load"
    if len(sys.argv) > 1 and module not in sys.argv[1:]:
        continue
    src = open(path).read()
    assert src.count(old) == 1, (name, src.count(old))
    open(path, "w").write(src.replace(old, new))
    try:
        r = subprocess.run([sys.executable, "-I", "-W", "ignore", "-c",
                            "import sys, unittest; sys.path.insert(0, '.'); "
                            f"unittest.main(module='r110.tests.{module}', argv=['m'])"],
                           capture_output=True, text=True, timeout=600)
        last = [l for l in r.stderr.splitlines() if l.startswith(("OK", "FAILED"))]
        print(f"{name}: {last[-1] if last else 'no result'}")
    except subprocess.TimeoutExpired:
        print(f"{name}: TIMEOUT (counts as detected: the run did not pass)")
    finally:
        open(path, "w").write(src)
