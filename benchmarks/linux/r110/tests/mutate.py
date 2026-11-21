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
 ("load ignores the heartbeat", "r110/load.py", "            if stale:\n                self.budget.stop(\"heartbeat-lost\", \"load\")", "            if False:\n                pass"),
 ("heartbeat replay accepted", "r110/common.py", '(self.seq is None or rec["seq"] > self.seq)', "True", "test_guard"),
 ("first heartbeat not awaited", "r110/common.py", "self.fresh_until = clock.monotonic() + timeout", "self.fresh_until = float('inf')", "test_guard"),
 ("service identity not checked", "r110/proc.py", "    if not matches(rec):\n        return \"other\"\n", "", "test_guard"),
 ("blind check passes", "r110/guard.py", "        except (OSError, ValueError):\n            return \"monitor-failed\", \"guard\"", "        except (OSError, ValueError):\n            pass", "test_guard"),
 ("load group not stopped", "r110/guard.py", "        empty = proc.stop_group(pgid, self.clock, child.poll)", "        empty = True", "test_guard"),
 ("TERM only", "r110/proc.py", "((signal.SIGTERM, term_wait), (signal.SIGKILL, kill_wait))", "((signal.SIGTERM, term_wait),)", "test_guard"),
 ("watchdog beats on a failed round", "r110/watchdog.py", "        if required_ok:\n            self.beat()", "        self.beat()", "test_guard"),
 ("watchdog takes a missing reading as healthy", "r110/watchdog.py", "            except KeyError as e:\n                return \"monitor-failed\"", "            except KeyError as e:\n                continue\n                return \"monitor-failed\"", "test_guard"),
 ("sampler: a vanished file is an error", "r110/sampler.py", "                except FileNotFoundError:\n                    vanished = True\n                    break", "                except FileNotFoundError:\n                    raise", "test_sampler"),
 ("sampler: vanishing gives zeros", "r110/sampler.py", "            return \"partial\", {}, str(e)", "            return \"ok\", {\"apparent_bytes\": {\"value\": 0, \"unit\": \"bytes\"}}, None", "test_sampler"),
 ("sampler: an absent family passes", "r110/sampler.py", "    if missing:\n        return \"partial\", values", "    if False:\n        return \"partial\", values", "test_sampler"),
 ("sampler: a line being written is counted", "r110/sampler.py", "        end = data.rfind(b\"\\n\") + 1", "        end = len(data)", "test_sampler"),
 ("sampler: rotation ignored", "r110/sampler.py", "        if size < self.offset:", "        if False:", "test_sampler"),
 ("sampler: unbounded output", "r110/sampler.py", "            if self.written + len(line) > self.cfg[\"max_out_bytes\"]:", "            if False:", "test_sampler"),
 ("sampler: other errors hidden", "r110/sampler.py", "        except OSError as e:\n            return \"error\", {}, f\"{errno", "        except OSError as e:\n            return \"ok\", {}, f\"{errno", "test_sampler"),
 ("driver: no --execute needed", "r110/driver.py", "    if not a.execute:\n        print(", "    if False:\n        print(", "test_driver"),
 ("driver: approval not checked", "r110/driver.py", "        check_approval(a.approval, sha, a.command, now)\n", "        pass\n", "test_driver"),
 ("driver: approval window ignored", "r110/driver.py", "    if not w0 <= now <= w1:", "    if False:", "test_driver"),
 ("driver: approval of another config accepted", "r110/driver.py", "    if a.get(\"config_sha256\") != config_sha:", "    if False:", "test_driver"),
 ("driver: looser limits accepted", "r110/driver.py", "        if k not in budget.DEFAULTS or v > budget.DEFAULTS[k]:", "        if False:", "test_driver"),
 ("driver: looser judgement accepted", "r110/driver.py", "        if judge.VALUES.get(k) != v:", "        if False:", "test_driver"),
 ("driver: evidence overwritten", "r110/driver.py", "        os.mkdir(self.dir)", "        os.makedirs(self.dir, exist_ok=True)", "test_driver"),
 ("driver: evidence failure passes", "r110/driver.py", "            if self.result[\"outcome\"] == judge.PASSED:\n                self.result[\"outcome\"] = judge.INCONCLUSIVE", "            pass", "test_driver"),
 ("driver: stage exit not enforced", "r110/driver.py", "        if not ok and not self.stop:\n            # Its", "        if False:\n            # Its", "test_driver"),
 ("judge: integrity stop not a failure", "r110/judge.py", "    if cls == \"integrity\" or forbidden or", "    if forbidden or", "test_driver"),
 ("judge: forbidden counters ignored", "r110/judge.py", "    if cls == \"integrity\" or forbidden or", "    if cls == \"integrity\" or", "test_driver"),
 ("judge: sample gaps allowed", "r110/judge.py", "    return all(b - a <= factor * interval_ms for a, b in zip(ts, ts[1:]))", "    return True", "test_driver"),
 ("judge: C1 no backlog passes", "r110/judge.py", "    if backlog0 == 0:", "    if False:", "test_driver"),
 ("judge: C1 unknown pre-refusal reclaims as 0", "r110/judge.py", "    if before is None:\n        return verdict(\"C1\"", "    if before is None:\n        before = 0\n    if False:\n        return verdict(\"C1\"", "test_driver"),
 ("judge: C2 D1 pending ignored", "r110/judge.py", "        if p0 > 0:", "        if False:", "test_driver"),
 ("judge: C2 probes not bounded", "r110/judge.py", "        freed = decrease + probe_puts * shard", "        freed = decrease", "test_driver"),
 ("judge: C2 du not checked", "r110/judge.py", "            if abs(d - (sg + sd)) > v[\"c2_du_tolerance\"] * (sg + sd):", "            if False:", "test_driver"),
 ("judge: C5 incomplete passes", "r110/judge.py", "    if not vr.get(\"complete\") or mf is None or sc is None:", "    if False:", "test_driver"),
 ("remote: teardown without marker", "r110/remote.py", "[ \\\"$(cat {Q(d)}/{MARK} 2>/dev/null)\\\" = {Q(rid)} ] && rm", "rm", "test_driver"),
 ("remote: teardown while running", "r110/remote.py", "        if alive is None or alive.strip():", "        if False:", "test_driver"),
 ("remote: smoke guard stops main services", "r110/remote.py", "*([] if sub else [\"--services\", f\"{self.run_dir}/services\"]),", "\"--services\", f\"{self.run_dir}/services\",", "test_driver"),
 ("remote: setup reuses directories", "r110/remote.py", "        self.ssh(f\"[ ! -e {Q(self.run_dir)} ] && [ ! -e {Q(self.fdb_data)} ] || \"\n                 f\"{{ echo 'run directories exist' >&2; exit 1; }}; \"\n", "        self.ssh(\"true; \"\n", "test_driver"),
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
