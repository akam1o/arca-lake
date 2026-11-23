"""The host's evidence before teardown (``evidence.py``,
``SshCluster.teardown``): real processes started and stopped by their
records, the real scripts run here through a stand-in for ssh, and real
failures: a lost connection, a transfer cut mid-file, a reconnect, a
checksum mismatch, permission errors, a reboot-like disappearance (the
processes killed and the boot ID changed), a log over the cap."""

import base64
import contextlib
import copy
import hashlib
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

from r110 import common, driver, evidence, proc, remote
from r110.tests.test_driver import EXAMPLE

SLEEPER = [sys.executable, "-I", "-c", "import time; time.sleep(300)"]
SSH_DOWN = (255, "", "ssh: connect to host HOST_B port 22: No route to host")


class Host:
    """``SshCluster``'s runner: the command runs here (``sh -c``), as ssh
    would run it on the host. ``down``: every call fails as an unreachable
    host's ssh does; ``drop_after_reads``: the connection is lost after that
    many file reads (and stays lost); ``corrupt``: how many reads have one
    byte of their data changed in transit (-1: all)."""

    def __init__(self):
        self.down = False
        self.drop_after_reads = None
        self.corrupt = 0
        self.before_read = None  # (path, fn): fn called once, before that file's first read
        self.fail_rm = None  # a path: its removal fails as a lost connection does
        self.reads = 0
        self.log = []  # (kind, command) of every call made

    @staticmethod
    def kind(command, stdin):
        for tag in ("inventory", "file", "boot"):
            if stdin and stdin.startswith(f"# r110:{tag}"):
                return "read" if tag == "file" and " read " in command else tag
        if stdin == remote.STOP:
            return "stop"
        if "rm -rf" in command:
            return "rm"
        return "other"

    def __call__(self, target, command, stdin=None, timeout=120):
        k = self.kind(command, stdin)
        self.log.append(k)
        if self.down or (k == "rm" and self.fail_rm and self.fail_rm in command):
            return SSH_DOWN
        if k == "read":
            self.reads += 1
            if self.before_read and self.before_read[0] in command:
                f, self.before_read = self.before_read[1], None
                f()
            if self.drop_after_reads is not None and self.reads > self.drop_after_reads:
                self.down = True
                return SSH_DOWN
        r = subprocess.run(["sh", "-c", command], input=stdin, capture_output=True, text=True, timeout=timeout)
        out = r.stdout
        if k == "read" and self.corrupt and '"data"' in out:
            self.corrupt -= 1
            rec = json.loads(out)
            data = bytearray(base64.b64decode(rec["data"]))
            if data:
                data[0] ^= 0xFF
            out = json.dumps(dict(rec, data=base64.b64encode(bytes(data)).decode())) + "\n"
        return r.returncode, out, r.stderr


def nocache(files):
    return {k: v for k, v in files.items() if "/__pycache__/" not in k}


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.started = []
        self.addCleanup(self.kill_started)
        t = self.tmp.name
        with open(EXAMPLE) as f:
            cfg = json.load(f)
        cfg["fdb_memory"] = "3GiB"
        cfg["local_dir"] = os.path.join(t, "operator")
        self.cfg = driver.validate(cfg)
        self.host = Host()
        c = remote.SshCluster(self.cfg, runner=self.host)
        # Under a parent named by the run, as on the host.
        top = os.path.join(t, self.cfg["run_id"])
        c.run_dir, c.fdb_data, c.mount = os.path.join(top, "run"), os.path.join(top, "fdb"), t
        c.tools, c.load_dir = c.run_dir + "/tools", c.run_dir + "/load"
        c.boot_id_path = os.path.join(t, "boot_id")
        c.sleep = lambda s: None  # the reconnect's waits: the host's state is set by the test, not by time
        self.c = c
        rid = self.cfg["run_id"]
        for d in ("services", "logs", "alarms", "load/control", "tools", "d0"):
            os.makedirs(os.path.join(c.run_dir, d))
        for d in ("d", "l"):
            os.makedirs(os.path.join(c.fdb_data, d))
        shutil.copytree(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), c.tools + "/r110",
                        ignore=shutil.ignore_patterns("tests", "__pycache__"))
        self.write("run/.r110-run", rid + "\n")
        self.write("fdb/.r110-run", rid + "\n")
        self.write("run/services/started.list", "")
        self.boot("boot-1")
        c.py(remote.BOOT, c.boot_id_path, c.run_dir + "/host-boot.json")
        self.write("run/load/ledger.jsonl", "".join(f'{{"type":"intent","op_id":{i},"pad":"{"x" * 200}"}}\n'
                                                     for i in range(1000)))
        self.write("run/load/guard.json", json.dumps({"reason": "heartbeat-lost", "group_empty": True}))
        self.write("run/load/status.json", json.dumps({"exit": "stopped"}))
        self.write("run/samples.jsonl", '{"target": "controller"}\n' * 50)
        self.write("run/logs/node0.log", bytes(range(256)) * 40)  # not UTF-8: saved as bytes
        self.write("run/d0/segment-1", b"\0" * 4096)  # a node store: data, not evidence
        self.write("fdb/l/trace.0.xml", "<Event/>\n" * 100)
        self.write("fdb/d/storage.sqlite", b"\0" * 4096)
        for n in ("node0", "node1"):
            c.start(n, SLEEPER)
            self.started.append(common.read_json(os.path.join(c.run_dir, "services", n + ".json")))
        self.ev = os.path.join(t, "evidence", self.cfg["run_id"])
        self.host.log.clear()

    def kill_started(self):
        for rec in self.started:
            try:
                os.killpg(rec["pgid"], signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def path(self, rel):
        label, rest = rel.split("/", 1)
        return os.path.join(self.c.run_dir if label == "run" else self.c.fdb_data, rest)

    def write(self, rel, data):
        with open(self.path(rel), "wb" if isinstance(data, bytes) else "w") as f:
            f.write(data)

    def boot(self, boot_id):
        with open(self.c.boot_id_path, "w") as f:
            f.write(boot_id + "\n")

    def evidence_files(self):
        """rel -> sha256 of every file collection must save."""
        out = {}
        for label, top in (("run", self.c.run_dir), ("fdb", self.c.fdb_data)):
            for d, dirs, files in os.walk(top):
                if d == top:
                    dirs[:] = [x for x in dirs if x not in ("d0", "d")]
                for n in files:
                    p = os.path.join(d, n)
                    out[label + "/" + os.path.relpath(p, top)] = sha(p)
        return out

    def saved(self, rel):
        return os.path.join(self.ev, "private", "host", rel)

    def manifest(self):
        return common.read_json(os.path.join(self.ev, "private", "manifest.json"))

    def assertKept(self, out, why):
        self.assertIn(why, out.get("kept", ""), out)
        self.assertEqual(out["removed"], [])
        self.assertTrue(os.path.isdir(self.c.run_dir) and os.path.isdir(self.c.fdb_data))


class Teardown(Base):
    maxDiff = None

    def test_every_file_is_saved_and_verified_before_anything_is_removed(self):
        want = self.evidence_files()
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        self.assertEqual(out["removed"], [self.c.fdb_data, self.c.run_dir])
        self.assertEqual(out["parent"], "removed")
        self.assertFalse(os.path.exists(self.c.run_dir))
        self.assertEqual({out["stopped"][n + ".json"]["verdict"] for n in ("node0", "node1")}, {"stopped"})
        # The stops came first; the collection after them; the removal last.
        log = self.host.log
        self.assertLess(max(i for i, k in enumerate(log) if k == "stop"), log.index("inventory"))
        self.assertLess(max(i for i, k in enumerate(log) if k in ("inventory", "read")), log.index("rm"))
        man = self.manifest()
        self.assertTrue(man["complete"])
        # The tools' bytecode the stops' own scripts wrote is saved too.
        self.assertEqual(nocache({f["path"]: f["sha256"] for f in man["files"]}), nocache(want))
        for rel, digest in want.items():
            self.assertEqual(sha(self.saved(rel)), digest, rel)
        self.assertEqual(sorted(man["excluded"]), ["fdb/d", "run/d0"])
        self.assertEqual(man["tools"], evidence.tools_sha256())
        self.assertEqual(man["meta"]["binary_sha256"], self.cfg["binary"]["sha256"])
        self.assertIs(out["host_boot"]["rebooted"], False)
        self.assertEqual(out["guard_result"]["record"]["reason"], "heartbeat-lost")
        # The public summary names no host path; the private manifest does.
        with open(os.path.join(self.ev, "public", "evidence-summary.json")) as f:
            public = f.read()
        self.assertNotIn(self.tmp.name, public)
        self.assertIn(self.tmp.name, json.dumps(man))
        self.assertTrue(json.loads(public)["complete"])
        # The teardown's own record, kept with the evidence.
        rec = common.read_json(os.path.join(self.ev, "private", "teardown", "001.json"))
        self.assertEqual(rec["removed"], out["removed"])

    def test_an_unreachable_host_is_left_alone_and_a_reconnect_collects_first(self):
        self.host.down = True
        out = self.c.teardown(self.ev)
        self.assertKept(out, "unreachable")
        self.assertEqual(out["reach"]["attempts"], remote.REACH_ATTEMPTS)
        self.assertEqual(set(self.host.log), {"other"})  # only the reach: no stop, read or removal
        for rec in self.started:
            self.assertEqual(proc.check(rec), "same")
        self.host.down = False
        self.host.log.clear()
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        self.assertLess(self.host.log.index("inventory"), self.host.log.index("rm"))
        self.assertTrue(self.manifest()["complete"])

    def test_a_transfer_cut_mid_file_is_never_complete_and_a_retry_resumes(self):
        want = self.evidence_files()
        self.host.drop_after_reads = 5
        out = self.c.teardown(self.ev, chunk=4096)
        self.assertKept(out, "")  # the host lost: kept, whatever check found it first
        # The stops did not wait for the collection: they were made before it.
        self.assertEqual({out["stopped"][n + ".json"]["verdict"] for n in ("node0", "node1")}, {"stopped"})
        man = self.manifest()
        self.assertFalse(man["complete"])
        self.assertIn("run/load/ledger.jsonl", [e["path"] for e in man["errors"]])
        self.assertFalse(os.path.exists(self.saved("run/load/ledger.jsonl")), "a partial file took the name")
        for d, _, files in os.walk(os.path.join(self.ev, "private")):
            self.assertEqual([n for n in files if n.endswith(".part")], [], d)
        self.assertFalse(json.load(open(os.path.join(self.ev, "public", "evidence-summary.json")))["complete"])
        # The host is back: the next teardown resumes (what was saved is not
        # read again), completes and only then removes.
        first = {f["path"] for f in man["files"]}
        self.assertTrue(first)
        self.host.down, self.host.drop_after_reads, self.host.reads = False, None, 0
        out = self.c.teardown(self.ev, chunk=4096)
        self.assertNotIn("kept", out, out)
        man = self.manifest()
        self.assertEqual((man["attempt"], man["complete"]), (2, True))
        self.assertEqual(nocache({f["path"]: f["sha256"] for f in man["files"]}), nocache(want))
        need = sum(-(-f["size"] // 4096) for f in man["files"] if f["path"] not in first)
        self.assertEqual(self.host.reads, need)

    def test_a_checksum_mismatch_is_retried_and_never_saved(self):
        self.host.corrupt = -1  # every read
        out = self.c.teardown(self.ev)
        self.assertKept(out, "not saved complete")
        errs = {e["path"]: e["error"] for e in self.manifest()["errors"]}
        self.assertIn("sha256 mismatch", errs["run/load/ledger.jsonl"])
        self.assertFalse(os.path.exists(self.saved("run/load/ledger.jsonl")))
        nonempty = [f for f in self.evidence_files() if os.path.getsize(self.path(f))]
        self.assertEqual(self.host.log.count("read"), (1 + evidence.RETRIES) * len(nonempty))
        self.host.corrupt = 1  # one read in transit: its retry saves it
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)

    def test_a_collection_past_its_deadline_is_never_complete(self):
        # The deadline passes between the inventory and the first file.
        t = iter([0.0, 0.0])
        out = self.c.teardown(self.ev, deadline_s=100, clock=lambda: next(t, 1e9))
        self.assertKept(out, "not saved complete")
        self.assertEqual({out["stopped"][n + ".json"]["verdict"] for n in ("node0", "node1")}, {"stopped"})
        man = self.manifest()
        self.assertEqual(man["files"], [])
        self.assertIn("deadline", {e["path"]: e["error"] for e in man["errors"]}["run/load/ledger.jsonl"])
        self.assertEqual(self.host.log.count("read"), 0)
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)

    def test_a_file_rewritten_during_its_transfer_is_fetched_again(self):
        """status.json is replaced whole (same length, other bytes) between
        the host's hash and the read: every chunk is intact, the file is
        not what was hashed. It must not be saved under the old sha256."""
        old, new = json.dumps({"exit": "stopped"}), json.dumps({"exit": "XXXXXXX"})
        self.assertEqual(len(old), len(new))
        self.write("run/load/status.json", old)
        self.host.before_read = (self.path("run/load/status.json"),
                                 lambda: self.write("run/load/status.json", new))
        man = self.c.collect_evidence(self.ev)
        entry = {f["path"]: f for f in man["files"]}["run/load/status.json"]
        digest = hashlib.sha256(new.encode()).hexdigest()
        self.assertEqual((entry["sha256"], sha(self.saved("run/load/status.json"))), (digest, digest))

    def test_a_saved_copy_or_a_host_file_changed_after_collection_keeps_the_run(self):
        collect = self.c.collect_evidence
        for change in ("saved", "host"):
            def tampered(dest, **kw):
                man = collect(dest, **kw)
                if change == "saved":
                    with open(self.saved("run/load/ledger.jsonl"), "ab") as f:
                        f.write(b"x")
                else:  # a writer still running on the host
                    with open(self.path("run/logs/node0.log"), "ab") as f:
                        f.write(b"late line\n")
                return man
            self.c.collect_evidence = tampered
            out = self.c.teardown(self.ev)
            self.assertKept(out, "no longer matches")
            self.assertIn("run/load/ledger.jsonl" if change == "saved" else "run/logs/node0.log: changed",
                          out["kept"])
        self.c.collect_evidence = collect
        # Collected again: the changed log saved anew, the earlier copy kept apart.
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        self.assertTrue(os.path.isdir(os.path.join(self.ev, "private", "superseded")))

    def test_permission_errors_keep_the_run(self):
        if os.geteuid() == 0:
            self.skipTest("root reads and writes anything")
        locked = self.path("run/load/guard.json")
        os.chmod(locked, 0)
        out = self.c.teardown(self.ev)
        os.chmod(locked, 0o644)
        self.assertKept(out, "not saved complete")
        errs = {e["path"]: e["error"] for e in self.manifest()["errors"]}
        self.assertIn("PermissionError", errs["run/load/guard.json"])
        # The operator's side: the evidence directory cannot be written.
        ev2 = os.path.join(self.tmp.name, "ro")
        os.makedirs(ev2)
        os.chmod(ev2, 0o500)
        self.addCleanup(os.chmod, ev2, 0o700)
        out = self.c.teardown(os.path.join(ev2, "run"))
        self.assertKept(out, "not saved complete")

    def test_a_reboot_is_recorded_as_such_never_as_a_stop(self):
        """Run r110-20261122T162239Z: every process ended with the host's
        reboot and no result of the guard was left to read."""
        os.remove(self.path("run/load/guard.json"))
        for rec in self.started:
            os.killpg(rec["pgid"], signal.SIGKILL)
        for rec in self.started:
            subprocess.run(["sh", "-c", f"while kill -0 {rec['pid']} 2>/dev/null; do sleep 0.05; done"], timeout=30)
        self.boot("boot-2")
        out = self.c.teardown(self.ev)
        self.assertTrue(out["host_boot"]["rebooted"])
        self.assertEqual(out["gone_at_reboot"], ["node0.json", "node1.json"])
        for n in out["gone_at_reboot"]:
            self.assertEqual(out["stopped"][n], {"verdict": "gone", "cause": "host-reboot"})
        self.assertIn("not known", out["reboot_note"])
        self.assertEqual(out["guard_result"], {"saved": False, "why": "absent on the host"})
        self.assertIn("run/load/guard.json", [m["path"] for m in self.manifest()["missing"]])
        self.assertIs(json.load(open(os.path.join(self.ev, "public", "evidence-summary.json")))["host_rebooted"], True)
        # Gone and the evidence saved: the removal may proceed.
        self.assertNotIn("kept", out, out)

    def test_a_guard_result_saved_before_a_reboot_is_what_the_record_says(self):
        for rec in self.started:
            os.killpg(rec["pgid"], signal.SIGKILL)
        for rec in self.started:
            subprocess.run(["sh", "-c", f"while kill -0 {rec['pid']} 2>/dev/null; do sleep 0.05; done"], timeout=30)
        self.boot("boot-2")
        out = self.c.teardown(self.ev)
        self.assertIn("'heartbeat-lost'", out["reboot_note"])

    def test_a_log_over_the_cap_is_saved_as_its_tail_and_keeps_the_run(self):
        out = self.c.teardown(self.ev, log_cap=1000)
        self.assertKept(out, "capped")
        self.assertEqual({out["stopped"][n + ".json"]["verdict"] for n in ("node0", "node1")}, {"stopped"})
        f = {x["path"]: x for x in self.manifest()["files"]}["run/logs/node0.log"]
        self.assertTrue(f["capped"])
        with open(self.path("run/logs/node0.log"), "rb") as a, open(self.saved("run/logs/node0.log"), "rb") as b:
            self.assertEqual(b.read(), a.read()[-1000:])
        self.assertEqual(self.manifest()["limits"]["log_cap"], 1000)
        out = self.c.teardown(self.ev, log_cap=1 << 20)  # the cap raised
        self.assertNotIn("kept", out, out)
        self.assertEqual(self.manifest()["limits"]["log_cap"], 1 << 20)

    def test_a_collection_after_the_removal_keeps_every_saved_entry(self):
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        first = self.manifest()
        public = json.load(open(os.path.join(self.ev, "public", "evidence-summary.json")))
        # Collected again with nothing left on the host: the manifest and the
        # summary keep every file saved, each saved copy read again.
        man = self.c.collect_evidence(self.ev)
        self.assertEqual((man["attempt"], man["complete"]), (2, True))
        self.assertEqual(self.manifest(), man)
        self.assertEqual({f["path"]: f["sha256"] for f in man["files"]},
                         {f["path"]: f["sha256"] for f in first["files"]})
        self.assertEqual({f.get("host") for f in man["files"]}, {"gone"})
        again = json.load(open(os.path.join(self.ev, "public", "evidence-summary.json")))
        self.assertEqual(len(again["files"]), len(public["files"]))
        self.assertEqual(again["bytes_saved"], public["bytes_saved"])
        # A saved copy lost since is an error, never a smaller complete manifest.
        os.remove(self.saved("run/load/ledger.jsonl"))
        man = self.c.collect_evidence(self.ev)
        self.assertFalse(man["complete"])
        self.assertIn("run/load/ledger.jsonl", [e["path"] for e in man["errors"]])

    def test_a_teardown_cut_between_its_two_removals_resumes_on_the_rest(self):
        """The test FoundationDB's directory removed, the connection lost
        before the run directory: the next teardown finds the run directory
        alone (its records and tools intact: removed last), keeps the files
        saved from the other in the manifest and checks them again before it
        removes the rest."""
        self.host.fail_rm = self.c.run_dir
        out = self.c.teardown(self.ev)
        self.assertEqual((out["removed"], out["kept_dirs"]), ([self.c.fdb_data], [self.c.run_dir]))
        self.assertIn("left on the host", out["kept"])
        self.assertNotIn("parent", out)
        self.assertFalse(os.path.isdir(self.c.fdb_data))
        self.host.fail_rm = None
        trace = self.saved("fdb/l/trace.0.xml")
        with open(trace, "rb") as f:
            good = f.read()
        # A saved copy of a file now gone from the host differs: kept.
        with open(trace, "ab") as f:
            f.write(b"x")
        out = self.c.teardown(self.ev)
        self.assertIn("not saved complete", out["kept"])
        self.assertTrue(os.path.isdir(self.c.run_dir))
        self.assertIn("fdb/l/trace.0.xml", [e["path"] for e in self.manifest()["errors"]])
        with open(trace, "wb") as f:
            f.write(good)
        # Changed after this collection, before the removal: kept.
        collect = self.c.collect_evidence

        def tampered(dest, **kw):
            man = collect(dest, **kw)
            with open(trace, "ab") as f:
                f.write(b"x")
            return man

        self.c.collect_evidence = tampered
        out = self.c.teardown(self.ev)
        self.assertIn("no longer matches", out["kept"])
        self.assertTrue(os.path.isdir(self.c.run_dir))
        self.c.collect_evidence = collect
        with open(trace, "wb") as f:
            f.write(good)
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        self.assertEqual((out["removed"], out["absent"]), ([self.c.run_dir], [self.c.fdb_data]))
        self.assertNotIn("kept_dirs", out)
        # The empty parent named by the run goes too.
        self.assertEqual(out["parent"], "removed")
        self.assertFalse(os.path.exists(os.path.dirname(self.c.run_dir)))
        man = self.manifest()
        self.assertTrue(man["complete"])
        self.assertEqual({f["path"] for f in man["files"] if f.get("host") == "gone"},
                         {p for p in self.saved_paths() if p.startswith("fdb/")})

    def test_a_partly_removed_fdb_directory_is_checked_against_the_saved_copies(self):
        """A removal of the test FoundationDB's directory cut mid-way: some
        of its files gone, the marker left."""
        man = self.c.collect_evidence(self.ev)
        self.assertTrue(man["complete"])
        os.remove(self.path("fdb/l/trace.0.xml"))
        trace = self.saved("fdb/l/trace.0.xml")
        with open(trace, "rb") as f:
            good = f.read()
        collect = self.c.collect_evidence

        def tampered(dest, **kw):
            man = collect(dest, **kw)
            with open(trace, "ab") as f:
                f.write(b"x")
            return man

        self.c.collect_evidence = tampered
        out = self.c.teardown(self.ev)
        self.assertKept(out, "no longer matches")
        self.assertIn("fdb/l/trace.0.xml", out["kept"])
        self.c.collect_evidence = collect
        with open(trace, "wb") as f:
            f.write(good)
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        self.assertEqual({f["path"]: f.get("host") for f in self.manifest()["files"]}["fdb/l/trace.0.xml"], "gone")

    def saved_paths(self):
        out = set()
        top = os.path.join(self.ev, "private", "host")
        for d, _, files in os.walk(top):
            out |= {os.path.relpath(os.path.join(d, n), top) for n in files}
        return out

    def test_a_link_to_a_directory_is_named_as_skipped(self):
        os.symlink(os.path.join(self.c.run_dir, "logs"), os.path.join(self.c.run_dir, "loglink"))
        out = self.c.teardown(self.ev)
        self.assertNotIn("kept", out, out)
        man = self.manifest()
        self.assertEqual([(s["path"], s["why"]) for s in man["skipped"]],
                         [("run/loglink", "a link to a directory (not followed)")])
        self.assertNotIn("run/loglink/node0.log", [f["path"] for f in man["files"]])
        self.assertEqual(json.load(open(os.path.join(self.ev, "public", "evidence-summary.json")))["skipped"],
                         ["run/loglink"])

    def test_a_directory_without_this_runs_marker_is_left_and_fails_the_teardown(self):
        """A removal cut mid-way can take the marker with it: such a
        directory is never removed by the tool and the teardown fails."""
        os.remove(self.path("run/.r110-run"))
        out = self.c.teardown(self.ev)
        self.assertEqual((out["removed"], out["kept_dirs"]), ([self.c.fdb_data], [self.c.run_dir]))
        self.assertIn("without its marker", out["kept"])
        self.assertTrue(os.path.isdir(self.c.run_dir))

    def test_no_evidence_directory_removes_nothing(self):
        out = self.c.teardown()
        self.assertKept(out, "no evidence directory")
        self.assertNotIn("inventory", self.host.log)


class Command(unittest.TestCase):
    def test_teardown_and_collect_need_an_evidence_directory(self):
        from r110.tests.test_driver import FakeCluster, NOW, NoContact
        with tempfile.TemporaryDirectory() as d, open(EXAMPLE) as f:
            cfg = json.load(f)
            cfg["local_dir"] = os.path.join(d, "operator")
            p = os.path.join(d, "run.json")
            with open(p, "w") as f2:
                json.dump(cfg, f2)
            digest = sha(p)
            for cmd in ("teardown", "collect"):
                a = os.path.join(d, "approval.json")
                with open(a, "w") as f2:
                    json.dump({"config_sha256": digest, "scope": [cmd], "window_start": "2027-01-01T00:00:00+00:00",
                               "window_end": "2027-01-02T00:00:00+00:00", "approved_by": "approver"}, f2)
                made = []
                with NoContact():
                    rc = driver.main([cmd, p, "--execute", "--approval", a], now=NOW,
                                     cluster_factory=lambda c: made.append(c) or FakeCluster(copy.deepcopy(c)))
                self.assertEqual(rc, 2, cmd)

    def test_the_collection_limits_are_set_from_the_command_line(self):
        from r110.tests.test_driver import FakeCluster, NOW
        with tempfile.TemporaryDirectory() as d, open(EXAMPLE) as f:
            cfg = json.load(f)
            cfg["local_dir"] = os.path.join(d, "operator")
            p = os.path.join(d, "run.json")
            with open(p, "w") as f2:
                json.dump(cfg, f2)
            a = os.path.join(d, "approval.json")
            with open(a, "w") as f2:
                json.dump({"config_sha256": sha(p), "scope": ["collect", "teardown"],
                           "window_start": "2027-01-01T00:00:00+00:00", "window_end": "2027-01-02T00:00:00+00:00",
                           "approved_by": "approver"}, f2)
            flags = ["--log-cap", "256M", "--log-total-cap", "2G", "--collect-deadline", "3600",
                     "--inventory-timeout", "900", "--call-timeout", "300"]
            want = {"log_cap": 256 << 20, "total_cap": 2 << 30, "deadline_s": 3600.0, "inventory_s": 900.0,
                    "call_s": 300.0}
            for cmd in ("collect", "teardown"):
                got = {}

                class Cluster(FakeCluster):
                    def collect_evidence(self, dest, meta=None, **kw):
                        got.update(kw)
                        return {"complete": True, "attempt": 1, "incomplete": [], "errors": []}

                    def teardown(self, dest, **kw):
                        got.update(kw)
                        return {"removed": []}

                rc = driver.main([cmd, p, "--execute", "--approval", a, "--evidence", os.path.join(d, "ev")] + flags,
                                 now=NOW, cluster_factory=lambda c: Cluster(copy.deepcopy(c)))
                self.assertEqual((rc, got), (0, want), cmd)
            for bad in (["--log-cap", "0"], ["--log-cap", "1T"], ["--call-timeout", "-1"], ["--call-timeout", "inf"]):
                with self.assertRaises(SystemExit, msg=bad), contextlib.redirect_stderr(io.StringIO()):
                    driver.main(["collect", p] + bad)


if __name__ == "__main__":
    unittest.main()
