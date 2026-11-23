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
 ("unknown taken as not applied", "r110/ledger.py", "        else:\n            cur.add(new)\n            pending.setdefault(key, set()).add(new)\n    return state", "        else:\n            pass\n    return state"),
 ("stop file ignored", "r110/budget.py", "        rec = self.stop_file.reason()\n", "        rec = None\n"),
 ("resume allowed", "r110/budget.py", "os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)\n        except FileExistsError:\n            os.close(fd)", "os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)\n        except FileExistsError:\n            os.close(fd)"),
 ("load ignores the heartbeat", "r110/load.py", "            if stale:\n                self.budget.stop(\"heartbeat-lost\", \"load\")", "            if False:\n                pass"),
 ("heartbeat replay accepted", "r110/common.py", '(self.seq is None or rec["seq"] > self.seq)', "True", "test_guard"),
 ("first heartbeat not awaited", "r110/common.py", "self.fresh_until = clock.monotonic() + timeout", "self.fresh_until = float('inf')", "test_guard"),
 ("service identity not checked", "r110/proc.py", "    c = check(rec)\n    if c != \"same\":\n        return c\n    try:\n        os.kill", "    try:\n        os.kill", "test_guard"),
 ("blind check passes", "r110/guard.py", "        except (OSError, ValueError):\n            return \"monitor-failed\", \"guard\"", "        except (OSError, ValueError):\n            pass", "test_guard"),
 ("load group not stopped", "r110/guard.py", "        stop = proc.stop_group(pgid, self.clock)", "        stop = {\"confirmed\": True, \"state\": \"empty\", \"signals\": []}", "test_guard"),
 ("TERM only", "r110/proc.py", "((signal.SIGTERM, term_wait), (signal.SIGKILL, kill_wait))", "((signal.SIGTERM, term_wait),)", "test_guard"),
 ("watchdog beats on a failed round", "r110/watchdog.py", "        if required_ok and not any(common.reason_class(b[0]) in (\"resource\", \"monitor\") for b in out):\n            self.beat()", "        self.beat()", "test_guard"),
 ("watchdog takes a missing reading as healthy", "r110/watchdog.py", "            except KeyError as e:\n                out.append((\"monitor-failed\"", "            except KeyError as e:\n                continue\n                out.append((\"monitor-failed\"", "test_guard"),
 ("sampler: a vanished file is an error", "r110/sampler.py", "                except FileNotFoundError:\n                    vanished = True\n                    break", "                except FileNotFoundError:\n                    raise", "test_sampler"),
 ("sampler: vanishing gives zeros", "r110/sampler.py", "            return \"partial\", {}, str(e)", "            return \"ok\", {\"apparent_bytes\": {\"value\": 0, \"unit\": \"bytes\"}}, None", "test_sampler"),
 ("sampler: an absent family passes", "r110/sampler.py", "    if missing:\n        return \"partial\", values", "    if False:\n        return \"partial\", values", "test_sampler"),
 ("sampler: a line being written is counted", "r110/sampler.py", "        end = data.rfind(b\"\\n\") + 1", "        end = len(data)", "test_sampler"),
 ("sampler: rotation ignored", "r110/sampler.py", "        if size < self.offset:", "        if False:", "test_sampler"),
 ("sampler: unbounded output", "r110/sampler.py", "            if self.written + len(line) > self.cfg[\"max_out_bytes\"]:", "            if False:", "test_sampler"),
 ("sampler: other errors hidden", "r110/sampler.py", "        except OSError as e:\n            return \"error\", {}, f\"{errno", "        except OSError as e:\n            return \"ok\", {}, f\"{errno", "test_sampler"),
 ("driver: no --execute needed", "r110/driver.py", "    if not a.execute:\n        print(", "    if False:\n        print(", "test_driver"),
 ("driver: approval not checked", "r110/driver.py", "        check_approval(a.approval, sha, a.command, now, duration)\n", "        pass\n", "test_driver"),
 ("driver: approval window ignored", "r110/driver.py", "    if not w0 <= now <= w1:", "    if False:", "test_driver"),
 ("driver: approval of another config accepted", "r110/driver.py", "    if a.get(\"config_sha256\") != config_sha:", "    if False:", "test_driver"),
 # Survives by design: budget.allocate (called by validate) refuses the
 # same limits (above the plan's or not finite): a second layer.
 ("driver: looser limits accepted", "r110/driver.py", "        if k not in budget.DEFAULTS or not budget.valid_limit(v) or v > budget.DEFAULTS[k]:", "        if k not in budget.DEFAULTS or not budget.valid_limit(v):", "test_driver"),
 ("driver: looser judgement accepted", "r110/driver.py", "        if judge.VALUES.get(k) != v:", "        if False:", "test_driver"),
 ("driver: evidence overwritten", "r110/driver.py", "        os.mkdir(self.dir)", "        os.makedirs(self.dir, exist_ok=True)", "test_driver"),
 ("driver: evidence failure passes", "r110/driver.py", "            if self.result[\"outcome\"] == judge.PASSED:\n                self.result[\"outcome\"] = judge.INCONCLUSIVE", "            pass", "test_driver"),
 ("driver: stage exit not enforced", "r110/driver.py", "        if not ok and not self.stop:\n            # Its", "        if False:\n            # Its", "test_driver"),
 ("judge: integrity stop not a failure", "r110/judge.py", "    if cls == \"integrity\" or forbidden or", "    if forbidden or", "test_driver"),
 ("judge: forbidden counters ignored", "r110/judge.py", "    if cls == \"integrity\" or forbidden or", "    if cls == \"integrity\" or", "test_driver"),
 ("judge: sample gaps allowed", "r110/judge.py", "    return all(b - a <= factor * interval_ms for a, b in zip(ts, ts[1:]))", "    return True", "test_driver"),
 ("judge: C2 D1 pending ignored", "r110/judge.py", "        if p0 > 0:", "        if False:", "test_driver"),
 ("judge: C2 probes not bounded", "r110/judge.py", "        freed = decrease + probe_puts * shard", "        freed = decrease", "test_driver"),
 ("judge: C2 du not checked", "r110/judge.py", "            if abs(d - (sg + sd)) > v[\"c2_du_tolerance\"] * (sg + sd):", "            if False:", "test_driver"),
 ("judge: C5 incomplete passes", "r110/judge.py", "    if not vr.get(\"complete\") or mf is None or sc is None:", "    if False:", "test_driver"),
 ("remote: teardown without marker", "r110/remote.py", "[ \\\"$(cat {Q(d)}/{MARK} 2>/dev/null)\\\" = {Q(rid)} ] && rm", "rm", "test_driver"),
 ("remote: a running process not reported", "r110/remote.py", "        print(\"alive %s (%s, group %s)\" % (name, c, g)); bad = 1", "        pass", "test_driver"),
 ("remote: smoke guard stops main services", "r110/remote.py", "*([] if sub else [\"--services\", f\"{self.run_dir}/services\"]),", "\"--services\", f\"{self.run_dir}/services\",", "test_driver"),
 ("remote: setup reuses directories", "r110/remote.py", "        self.ssh(f\"[ ! -e {Q(self.run_dir)} ] && [ ! -e {Q(self.fdb_data)} ] || \"\n                 f\"{{ echo 'run directories exist' >&2; exit 1; }}; \"\n", "        self.ssh(\"true; \"\n", "test_driver"),
 # Review findings (second round).
 ("load: NaN limits accepted", "r110/budget.py", "    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0", "    return True"),
 ("load: bucket and list not in the ledger", "r110/load.py", "            try:\n                self.ledger.write(rec)\n            except ledger_mod.LedgerFailure:\n                self.stop(\"ledger-failure\")\n                raise budget_mod.Stopped(\"ledger-failure\") from None\n            r = self._client()", "            try:\n                if op in (\"PUT\", \"DELETE\", \"GET\"):\n                    self.ledger.write(rec)\n            except ledger_mod.LedgerFailure:\n                self.stop(\"ledger-failure\")\n                raise budget_mod.Stopped(\"ledger-failure\") from None\n            r = self._client()"),
 ("load: outcome write failure ignored", "r110/load.py", "            try:\n                self.ledger.write(out)\n            except ledger_mod.LedgerFailure:\n                self.stop(\"ledger-failure\")\n                raise budget_mod.Stopped(\"ledger-failure\") from None", "            try:\n                self.ledger.write(out)\n            except ledger_mod.LedgerFailure:\n                pass"),
 ("load: verify ledger failure ignored", "r110/load.py", "                except ledger_mod.LedgerFailure:\n                    self.stop(\"ledger-failure\")\n                    raise budget_mod.Stopped(\"ledger-failure\") from None\n            with lock:", "                except ledger_mod.LedgerFailure:\n                    pass\n            with lock:"),
 ("load: 5xx does not stop", "r110/load.py", "        if r.status >= 500 and r.status != 503:\n            self.stop(\"unexpected-5xx\")", "        pass"),
 ("load: refused new key keeps its slot", "r110/load.py", "            if new_live and result in (None, \"refused\", \"not-sent\"):", "            if new_live and result is None:"),
 ("load: connection refused taken as sent", "r110/s3.py", "            if self._c.sock is None:\n                # A failed connection sends nothing: ``not-sent``, not unknown.\n                self._c.connect()\n", ""),
 ("load: probe failures ignored", "r110/load.py", "                if bad >= failures or slow:\n                    self.stop(\"probe\")\n                    return", "                pass"),
 ("watchdog: arguments not quoted", "r110/watchdog.py", "    return \" \".join([\"python3\", \"-I\", \"-\"] + [shlex.quote(str(a)) for a in argv])", "    return \" \".join([\"python3\", \"-I\", \"-\"] + [str(a) for a in argv])", "test_guard"),
 ("guard: signals itself (pid)", "r110/guard.py", "        if rec[\"pid\"] in mine:", "        if False:", "test_guard"),
 ("guard: signals the guard records", "r110/guard.py", "KEEP_PREFIXES = (\"guard\", \"sampler\", \"sentinel\")", "KEEP_PREFIXES = (\"sampler\", \"sentinel\")", "test_guard"),
 ("guard: stop file unread after the load exits", "r110/guard.py", "                rec = self.stop_file.reason()\n                if rec is not None:\n                    reason, source = rec.get(\"reason\", \"stop-file-unreadable\"), \"stop-file\"\n                break", "                break", "test_guard"),
 ("guard: NaN accepted", "r110/guard.py", "    if not all(math.isfinite(x) and x > 0 for x in nums):", "    if False:", "test_guard"),
 ("watchdog: guard loss ignored", "r110/watchdog.py", "            if g == \"gone\":\n                out.append((\"guard-lost\", {}))", "            pass", "test_guard"),
 ("watchdog: NaN accepted", "r110/watchdog.py", "            if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)\n                                  or v <= 0):", "            if False:", "test_guard"),
 ("proc: a null identity matches", "r110/proc.py", "    if not valid(rec):\n        return \"invalid\"\n    try:\n        tok", "    try:\n        tok", "test_guard"),
 ("judge: a stop with every criterion met passes", "r110/judge.py", "all(r[\"outcome\"] == PASSED for r in res) and not stop and not unwatched:", "all(r[\"outcome\"] == PASSED for r in res) and not unwatched:", "test_driver"),
 ("judge: unsampled protections pass", "r110/judge.py", "all(r[\"outcome\"] == PASSED for r in res) and not stop and not unwatched:", "all(r[\"outcome\"] == PASSED for r in res) and not stop:", "test_driver"),
 ("judge: C3a one probe enough", "r110/judge.py", "    need = int(v[\"c3a_hold_s\"] // v[\"c3a_probe_interval_s\"])", "    need = 1", "test_driver"),
 ("judge: no-outcome probe dropped", "r110/judge.py", "        out.append(outcomes.get((i[\"op_id\"], i[\"attempt\"]), \"no-outcome\"))", "        if (i[\"op_id\"], i[\"attempt\"]) in outcomes:\n            out.append(outcomes[(i[\"op_id\"], i[\"attempt\"])])", "test_driver"),
 ("judge: partial samples dropped", "r110/judge.py", "        if s.get(\"target\") != target or s.get(\"status\") not in (\"ok\", \"partial\"):", "        if s.get(\"target\") != target or s.get(\"status\") != \"ok\":", "test_driver"),
 ("driver: stale values used", "r110/driver.py", "                if now - x[\"ended_wall_ms\"] > 2 * self.iv_ms:\n                    return None\n                return", "                return", "test_driver"),
 ("driver: exception not a stop", "r110/driver.py", "                except Exception as e:  # noqa: BLE001 - a failure of the driver is a stop", "                except ZeroDivisionError as e:", "test_driver"),
 ("driver: stage 7 stop unseen", "r110/driver.py", "        self.tl[\"7\"][\"verify\"] = r\n        if self._stopped(\"7\"):\n            return False", "        self.tl[\"7\"][\"verify\"] = r", "test_driver"),
 ("driver: final stop record not read", "r110/driver.py", "                rec = self.c.stop_record()\n                if rec:\n                    self.stop = dict(rec, stage=self.current)", "                pass", "test_driver"),
 ("driver: refusal not held for D1", "r110/driver.py", "            return self.clock.monotonic() >= d1_done + CHAIN_S and self._quiet(s)", "            return self._quiet(s)", "test_driver"),
 ("driver: drain escalation ignored", "r110/driver.py", "                d[\"services\"] = self.c.stop_services()", "                pass", "test_driver"),
 ("driver: approval scope unchecked", "r110/driver.py", "    if not isinstance(sc, list) or not all(isinstance(x, str) and x in SCOPES for x in sc):", "    if False:", "test_driver"),
 ("driver: approval window unbounded", "r110/driver.py", "    if not w0 < w1 or w1 - w0 > datetime.timedelta(hours=MAX_WINDOW_H):", "    if False:", "test_driver"),
 ("remote: teardown on a failed check", "r110/remote.py", "        if rc != 0 or not lines or not lines[-1].startswith(\"checked \") or len(lines) > 1:", "        if lines and not lines[-1].startswith((\"checked \", \"unreadable\")):", "test_driver"),
 ("remote: ledger unchecked", "r110/remote.py", "            recs, _torn = ledger_mod.read(f.name)", "            recs = [json.loads(x.split(' ', 1)[1]) for x in open(f.name) if x.strip()]", "test_driver"),
 ("remote: smoke budget not reserved", "r110/budget.py", "        main[k] = approved[k] - sum(share[k] for share in SMOKE.values())", "        main[k] = approved[k]", "test_driver"),
 # Review findings (third round).
 ("remote: sub-runs share the main bucket", "r110/remote.py", "\"--bucket\", f\"r110-{sub}\" if sub else \"r110\",", "\"--bucket\", \"r110\",", "test_driver"),
 ("remote: smoke-hb without its bucket", "r110/remote.py", "        self._send(d, 1, {\"kind\": \"create-bucket\"})\n        self._send(d, 2, {\"kind\": \"put\", \"keys\": [f\"s{i}\" for i in range(10_000)], \"rate\": 2})", "        self._send(d, 1, {\"kind\": \"put\", \"keys\": [f\"s{i}\" for i in range(10_000)], \"rate\": 2})", "test_driver"),
 ("remote: smoke takes the load's own stop for the guard's", "r110/remote.py", "and g.get(\"reason\") == \"heartbeat-lost\" and g.get(\"source\") == \"guard\"", "", "test_driver"),
 ("remote: smoke load reads the heartbeat", "r110/remote.py", "*([\"--heartbeat\", hb, \"--heartbeat-id\", self.cfg[\"run_id\"]] if heartbeat else []),", "\"--heartbeat\", hb, \"--heartbeat-id\", self.cfg[\"run_id\"],", "test_driver"),
 ("guard: SIGTERM ignored", "r110/guard.py", "        if getattr(self, \"terminated\", False):\n            return \"operator\", \"guard\"", "", "test_guard"),
 ("driver: a sampling gap ends the hold", "r110/driver.py", "            if hold_on and self._left(s, 3):", "            if hold_on and not self._admission(s, 3):", "test_driver"),
 ("driver: C3a judged with a probe in flight", "r110/driver.py", "        if self.c.wait_step(n, deadline) is None or self._stopped(\"5\"):\n            return False\n", "", "test_driver"),
 # Survives by design, as "driver: looser limits accepted" above.
 ("driver: NaN limits accepted", "r110/driver.py", "        if k not in budget.DEFAULTS or not budget.valid_limit(v) or v > budget.DEFAULTS[k]:", "        if k not in budget.DEFAULTS or v > budget.DEFAULTS[k]:", "test_driver"),
 ("driver: retained record unchecked", "r110/driver.py", "        raise ConfigError(\"retained_record: absolute directory -> sha256 of its listing (plan §10)\")", "        pass", "test_driver"),
 ("driver: stale against the newest sample", "r110/driver.py", "                if now - x[\"ended_wall_ms\"] > 2 * self.iv_ms:", "                if max(y[\"ended_wall_ms\"] for y in samples) - x[\"ended_wall_ms\"] > 2 * self.iv_ms:", "test_driver"),
 ("judge: a no-outcome probe passes", "r110/judge.py", "    elif not reasons and \"no-outcome\" in probes:", "    elif False:", "test_driver"),
 ("judge: C3b no-outcome PUTs pass", "r110/judge.py", "        if acked + open_ >= v[\"c3b_ack_fraction\"] * len(ops) and not refused:", "        if acked + open_ >= v[\"c3b_ack_fraction\"] * len(ops):", "test_driver"),
 # F1-F8 (the third review round's findings).
 ("F1: background steps run in line", "r110/load.py", "            if s.get(\"background\"):", "            if False:", "test_integration"),
 ("F2: the churn deletes a protected key", "r110/load.py", "idx = next((j for j, key in enumerate(self.live_order) if key not in self.protected), None)", "idx = 0 if self.live_order else None", "test_integration"),
 ("F2: the probe does not protect its key", "r110/load.py", "        with self._lock:\n            self.protected.add(key)\n        self._probe_end.clear()", "        self._probe_end.clear()", "test_integration"),
 ("F2: no dedicated baseline object", "r110/driver.py", "                   {\"kind\": \"put\", \"keys\": [BASE_KEY]}]},", "                   ]},", "test_integration"),
 ("F3: an unreadable process table is empty", "r110/proc.py", "    except Unknown:\n        return \"unknown\"\n    return \"alive\"", "    except Unknown:\n        return \"empty\"\n    return \"alive\"", "test_guard"),
 ("F3: an uncheckable process is gone", "r110/proc.py", "    except Unknown:\n        return \"unknown\"\n    if cwd is None:", "    except Unknown:\n        return \"gone\"\n    if cwd is None:", "test_guard"),
 ("F3: EPERM taken as stopped", "r110/proc.py", "        st = group_state(pgid)\n        if st == \"empty\":\n            return {\"confirmed\": True, \"state\": st, \"signals\": sent}\n        sent.append", "        st = group_state(pgid)\n        if st == \"empty\" or (sent and sent[-1][1] == \"denied\"):\n            return {\"confirmed\": True, \"state\": st, \"signals\": sent}\n        sent.append", "test_guard"),
 ("F3: the load reaped before its group is stopped", "r110/guard.py", "        stop = proc.stop_group(pgid, self.clock)\n", "        child.wait()\n        stop = proc.stop_group(pgid, self.clock)\n", "test_guard"),
 ("F3: a missing started.list passes", "r110/remote.py", "    print(\"unverified: no started.list: %s\" % e); sys.exit(1)", "    started = []", "test_driver"),
 ("F3: teardown after an unconfirmed stop", "r110/remote.py", "        if unconfirmed:", "        if False:", "test_driver"),
 ("F4: the watchdog ends at an anomaly", "r110/watchdog.py", "                    self.stop(reason, detail)\n            n += 1", "                    self.stop(reason, detail)\n                    return 2\n            n += 1", "test_integration"),
 ("F4: no alarm", "r110/watchdog.py", "                self.alarm(reason, detail)\n", "", "test_integration"),
 ("F4: the sentinel ignores alarms", "r110/guard.py", "        a = self.alarm()\n        if a:", "        a = None\n        if a:", "test_integration"),
 ("F4: the sentinel ignores the heartbeat", "r110/guard.py", "        r = self.hb.check()\n        if r:\n            return r, {\"source\": \"sentinel\"}", "        r = None", "test_integration"),
 ("F4: the sentinel passes a blind check", "r110/guard.py", "        except (OSError, ValueError) as e:\n            return \"monitor-failed\", {\"source\": \"sentinel\"", "        except (OSError, ValueError) as e:\n            return None\n            return \"monitor-failed\", {\"source\": \"sentinel\"", "test_integration"),
 ("F4: an anomaly holds the heartbeat back", "r110/watchdog.py", "        if required_ok and not any(common.reason_class(b[0]) in (\"resource\", \"monitor\") for b in out):", "        if required_ok and not out:", "test_guard"),
 ("F5: the memory limit not passed", "r110/remote.py", "\"-L\", f\"{self.fdb_data}/l\", \"--memory\", self.cfg[\"fdb_memory\"],", "\"-L\", f\"{self.fdb_data}/l\",", "test_driver"),
 ("F5: an undetermined memory limit runs", "r110/driver.py", "        if execute:\n            raise ConfigError(\"fdb_memory undetermined", "        if False:\n            raise ConfigError(\"fdb_memory undetermined", "test_driver"),
 ("F6: the run's remaining time not applied", "r110/remote.py", "            lim[\"run_seconds\"] = max(1, min(lim[\"run_seconds\"], int(remaining_s)))", "            pass", "test_driver"),
 ("F6: smoke shares that do not fit pass", "r110/budget.py", "        if main[k] <= 0:", "        if False:", "test_driver"),
 ("F8: walk errors ignored", "r110/watchdog.py", "                    for d, _, files in os.walk(root, onerror=fail):", "                    for d, _, files in os.walk(root):", "test_integration"),
 ("F8: an unreadable kernel log is no alert", "r110/watchdog.py", "        if r.returncode != 0 or \"No journal files\" in said", "        if r.returncode != 0 and \"No journal files\" in said", "test_integration"),
 ("F8: any monitoring may be optional", "r110/driver.py", "    if not isinstance(opt, list) or not set(opt) <= {\"kernel_log\", \"swap\"}:", "    if False:", "test_driver"),
 # Review of #24 (fourth round).
 ("H1: a process with its main thread gone is dead", "r110/proc.py", "                if state in (\"Z\", \"X\") and threads_alive(int(n)):", "                if False:", "test_guard"),
 ("H2: the guard records no load", "r110/guard.py", "        if getattr(self.args, \"record_dir\", None) and not self.record_load(child):", "        if False:", "test_guard"),
 ("H2: teardown waits less than the guard", "r110/remote.py", "GUARD_STOP_S = 180", "GUARD_STOP_S = 30", "test_driver"),
 ("M1: the sentinel unwatched", "r110/watchdog.py", "            if k == \"gone\":\n                out.append((\"sentinel-lost\", {}))", "            pass", "test_guard"),
 ("malformed stop file crashes", "r110/common.py", "        if not isinstance(rec, dict) or not isinstance(rec.get(\"reason\"), str):\n            return {\"reason\": \"stop-file-unreadable\", \"detail\": text[:200]}\n        return rec", "        return rec", "test_integration"),
 ("L1: a record without its group", "r110/proc.py", "        and isinstance(rec.get(\"pgid\"), int) and not isinstance(rec[\"pgid\"], bool) and rec[\"pgid\"] > 1", "        and True", "test_guard"),
 ("L3: an unconfirmed guard ended", "r110/watchdog.py", "            unconfirmed = res.get(\"group_empty\") is False or res.get(\"services_confirmed\") is False", "            unconfirmed = res.get(\"services_confirmed\") is False", "test_integration"),
 ("P1: a slow probe answer passes", "r110/load.py", "                if bad >= failures or slow:", "                if bad >= failures:", "test_load"),
 ("P1: no live headroom", "r110/load.py", "                if self.budget.live_count() >= self.budget.limits[\"max_live\"] - headroom:", "                if False:", "test_integration"),
 ("P1: a full live set ends the hold", "r110/load.py", "                    self._count(f\"{kind}-live-limit\")", "                    break", "test_load"),
 ("P1: protection counters judged only afterwards", "r110/driver.py", "            bad = self.protection(samples)", "            bad = None", "test_driver"),
 ("P1: approval window only at the start", "r110/driver.py", "    if end > w1:", "    if False:", "test_driver"),
 ("P1: recovery only taken as recovered", "r110/driver.py", "        return v is not None and v >= 3", "        return v == 3", "test_driver"),
 ("P1: a live slot leaks in the waits", "r110/budget.py", "            if new_live:\n                self.live_release()\n            raise", "            raise", "test_load"),
 ("P1: a late unknown write forgotten", "r110/ledger.py", "            state[key] = {new} | pending.get(key, set())", "            state[key] = {new}", "test_load"),
 ("P1: a failing step finishes", "r110/load.py", "                except Exception:  # noqa: BLE001 - a worker that fails stops the run, never silently\n                    self.stop(\"load-error\")", "                except Exception:  # noqa: BLE001 - a worker that fails stops the run, never silently\n                    pass", "test_load"),
 ("P1: an interrupt not a stop", "r110/driver.py", "                except KeyboardInterrupt:\n                    self._stop(st[\"stage\"], \"operator\", detail=\"interrupted (SIGINT)\")\n                    break", "", "test_driver"),
 ("P1: probe keys left after stage 5", "r110/driver.py", "        if probes and not self.run_step(", "        if False and not self.run_step(", "test_integration"),
 ("P1: system FDB hosts optional", "r110/driver.py", "    if not isinstance(sf, list) or not sf or not set(sf) <= set(cfg[\"hosts\"]):", "    if False:", "test_driver"),
 # Re-review of #24.
 ("a stop's ssh shorter than the stop", "r110/remote.py", "                          timeout=1.5 * wait + 60).strip()", "                          timeout=120).strip()", "test_driver"),
 ("protection unchecked in a running step", "r110/driver.py", "        return self.c.run_step(step, deadline, check=check)", "        return self.c.run_step(step, deadline)", "test_driver"),
 ("a load record replaced", "r110/guard.py", "        return common.link_new(os.path.join(d, name + \".json\"), json.dumps(ident))", "        common.atomic_write_json(os.path.join(d, name + \".json\"), ident)\n        return True", "test_guard"),
 ("an unconfirmed sentinel ended", "r110/watchdog.py", "            unconfirmed = res.get(\"group_empty\") is False or res.get(\"services_confirmed\") is False", "            unconfirmed = res.get(\"group_empty\") is False", "test_integration"),
 ("an interrupt in the drain loses the evidence", "r110/driver.py", "            except KeyboardInterrupt:\n                self.ev.failures.append(\"drain: interrupted (SIGINT): the stops and the evidence follow\")\n", "", "test_driver"),
 # The final stops (0.1.0 preparation).
 ("finish: unconfirmed services pass", "r110/driver.py", "        if not stop_confirmed(v):\n            out[n] = v\n    return out", "    return out", "test_driver"),
 ("finish: watching ended with stops unconfirmed", "r110/driver.py", "            self.ev.failures.append(f\"final stops not confirmed: {sorted(problems)}\")\n            return False", "            self.ev.failures.append(f\"final stops not confirmed: {sorted(problems)}\")", "test_integration"),
 ("finish: an empty listing after setup passes", "r110/driver.py", "    if setup_started and not started:\n        out[\"listing\"] = \"started.list is empty after setup\"", "    pass", "test_driver"),
 ("finish: no retry", "r110/driver.py", "FINAL_STOP_ROUNDS = 2  # retries", "FINAL_STOP_ROUNDS = 0  # retries", "test_driver"),
 ("protection: only the last 200 samples", "r110/driver.py", "        for x in samples[self._prot_n:]:", "        for x in samples[-200:]:", "test_driver"),
 # Review of #27.
 ("finish: guards not accounted for", "r110/driver.py", "                if name.startswith(\"guard\") and key not in load:", "                if False:", "test_driver"),
 ("finish: the sampler not confirmed", "r110/driver.py", "        if not stop_confirmed(sampler):\n            problems[\"sampler.json\"] = sampler", "        pass", "test_driver"),
 ("finish: stop_load not a dict", "r110/driver.py", "        if not isinstance(load, dict):\n            problems[\"stop_load\"] = f\"no result: {load!r}\"[:300]\n        elif \"error\" in load:", "        if \"error\" in load:", "test_driver"),
 ("samples: offset moved before parsing", "r110/remote.py", "        new = [json.loads(x) for x in out[:end].splitlines() if x.startswith(\"{\") and '\"target\"' in x]\n        self._sample_off += len(out[:end].encode())", "        self._sample_off += len(out[:end].encode())\n        new = [json.loads(x) for x in out[:end].splitlines() if x.startswith(\"{\") and '\"target\"' in x]", "test_integration"),
 # Plan v2: C1a and C1b.
 ("v2: C1a short refusal judged", "r110/judge.py", "    if (t_e - t_r) < v[\"c1a_min_refusal_s\"] * 1000:", "    if False:", "test_driver"),
 ("v2: C1a progress not required", "r110/judge.py", "        reasons.append(f\"at least {bound} of D1's records reclaimed during the refusal, {need:.0f} needed\")", "        pass", "test_driver"),
 ("v2: attribution ignored", "r110/judge.py", "    return parts[\"reclaimed\"] - parts[\"reconcile_deleted\"] - 3 * parts[\"node_capacity_refusals\"], parts", "    return parts[\"reclaimed\"], parts", "test_driver"),
 ("v2: a counter reset accepted", "r110/judge.py", "    if not covered(pts, t0, t1, iv, gap) or not _monotone(pts):", "    if not covered(pts, t0, t1, iv, gap):", "test_driver"),
 ("v2: C1b deadline ignored", "r110/judge.py", "    if t_q > deadline:", "    if False:", "test_driver"),
 ("v2: C1b lost retirements pass", "r110/judge.py", "    return verdict(\"C1b\", FAILED, [f\"{parts['reclaimed']} records reclaimed by the quiet point, {need} needed\"], **ev)", "    return verdict(\"C1b\", PASSED, [], **ev)", "test_driver"),
 ("v2: stage 5 before D1 quiet", "r110/driver.py", "        # Not quiet by the deadline: stage 5 cannot take a clean baseline.\n        return bool(quiet)", "        return True", "test_driver"),
 ("v2: C1a fails before D1 was due", "r110/judge.py", "    if t_d is None or t_e < t_d + v[\"d1_chain_s\"] * 1000:", "    if t_d is None:", "test_driver"),
 ("v2: subtractions inside only", "r110/judge.py", "             \"node_capacity_refusals\": _increase_outer(samples, \"gateway0\",", "             \"node_capacity_refusals\": _increase(samples, \"gateway0\",", "test_driver"),
 ("v2: C1b read at the quiet point", "r110/judge.py", "    bound, parts = d1_reclaimed_bound(samples, t_d, t_q + 2 * iv, v)", "    bound, parts = d1_reclaimed_bound(samples, t_d, t_q, v)", "test_driver"),
 ("v2: C1b unsampled violations ignored", "r110/judge.py", "    if unsampled:\n        return", "    if False:\n        return", "test_driver"),
 ("v2: C1a subtractions from the refusal only", "r110/judge.py", "    bound, parts = d1_reclaimed_bound(samples, t_r, t_e, v, sub_from=s4.get(\"d1_done_ms\"))", "    bound, parts = d1_reclaimed_bound(samples, t_r, t_e, v)", "test_driver"),
 ("v2: natural recovery ends the run", "r110/driver.py", "                tl[\"refusal_ended_by\"] = \"natural recovery\"\n                self.c.step_done(n)\n            return", "                tl[\"refusal_ended_by\"] = \"natural recovery\"\n                self.c.step_done(n)\n                return True\n            return", "test_driver"),
 # Plan v3: stage 7 after the background passes settled.
 ("v3: scrub without the settle", "r110/driver.py", "        settled = self.wait(\"7\", wait_end, lambda s: self.host_ms() >= tl7[\"settle_after_ms\"] and self._quiet(s)) \\\n            if self.clock.monotonic() < wait_end else False", "        settled = True", "test_driver"),
 ("v3: settle by time only", "r110/driver.py", "lambda s: self.host_ms() >= tl7[\"settle_after_ms\"] and self._quiet(s)", "lambda s: self.host_ms() >= tl7[\"settle_after_ms\"]", "test_driver"),
 ("v3: unexpected shards excused after the settle", "r110/judge.py", "        if \"settled_ms\" in s7 and s7[\"settled_ms\"] is None and isinstance(by, list)", "        if isinstance(by, list)", "test_driver"),
 ("v3: unbounded unexpected shards excused", "r110/judge.py", " and isinstance(pend, (int, float)) and by[4] <= pend:", ":", "test_driver"),
 ("v3: other classes excused", "r110/judge.py", "                and sum(by) == sc and sum(by) == by[4] and isinstance(pend", "                and sum(by) == sc and isinstance(pend", "test_driver"),
 ("teardown: removed with the evidence incomplete", "r110/remote.py", "        if not man[\"complete\"]:\n            out[\"kept\"]", "        if False:\n            out[\"kept\"]", "test_evidence"),
 ("teardown: saved copies not read again", "r110/remote.py", "        bad = evidence.verify_local(evidence_dir, man)", "        bad = []", "test_evidence"),
 ("teardown: the host not listed again", "r110/remote.py", "                bad = evidence.changed_since(man, evidence.inventory(self._evidence_call, self.evidence_roots()))", "                bad = []", "test_evidence"),
 ("teardown: an unreachable host touched", "r110/remote.py", "        if not out[\"reach\"][\"ok\"]:", "        if False:", "test_evidence"),
 ("teardown: gone at a reboot taken as stopped", "r110/remote.py", "        if gone and boot.get(\"rebooted\") is not False:", "        if False:", "test_evidence"),
 ("evidence: a capped log is complete", "r110/evidence.py", "        if entry[\"capped\"]:", "        if False:", "test_evidence"),
 ("evidence: a failed file is complete", "r110/evidence.py", "    if man[\"errors\"]:\n        man[\"incomplete\"]", "    if False:\n        man[\"incomplete\"]", "test_evidence"),
 ("evidence: a copy not matching the host's sha256 takes its name", "r110/evidence.py", "        if got != want:", "        if False:", "test_evidence"),
 ("evidence: not resumed", "r110/evidence.py", "    if os.path.isfile(target) and sha256_file(target) == want:\n        return h, \"already saved\"", "    if False:\n        return h, \"already saved\"", "test_evidence"),
 ("finish: collected before the stops", "r110/driver.py", "        self.final_stops()\n        if hasattr(self.c, \"boot_check\"):", "        self._call(\"collect_evidence\", self.ev.dir)\n        self.final_stops()\n        if hasattr(self.c, \"boot_check\"):", "test_driver"),
 ("finish: incomplete evidence passes", "r110/driver.py", "            if not man[\"complete\"]:\n                self.ev.failures.append", "            if False:\n                self.ev.failures.append", "test_driver"),
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
        print(f"{name}: {last[-1] if last else f'KILLED rc {r.returncode} (detected: the run did not pass)'}")
    except subprocess.TimeoutExpired:
        print(f"{name}: TIMEOUT (counts as detected: the run did not pass)")
    finally:
        open(path, "w").write(src)
