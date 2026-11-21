"""P3: the sampler (plan §2, §9 P3), on local files, a local metrics
server and simulated races; no host."""

import http.server
import json
import os
import tempfile
import threading
import time
import unittest

from r110 import common, sampler

METRICS = """# TYPE arca_node_capacity_bytes gauge
arca_node_capacity_bytes{kind="available"} 123
arca_node_capacity_bytes{kind="allocated"} 456
arca_node_reclaim_retained_bytes 7
other_metric 1
"""


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, mode):
        self.mode = mode
        super().__init__(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}/metrics"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.server.mode == "slow":
            time.sleep(2)
        if self.server.mode == "500":
            self.send_response(500)
            self.end_headers()
            return
        b = METRICS.encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


class Metrics(unittest.TestCase):
    def serve(self, mode):
        s = Server(mode)
        self.addCleanup(lambda: (s.shutdown(), s.server_close()))
        return s

    def test_families_are_kept_with_labels_and_units(self):
        s = self.serve("ok")
        st, v, err = sampler.read_metrics({"url": s.url(), "families": [
            "arca_node_capacity_bytes", "arca_node_reclaim_retained_bytes"]}, 2)
        self.assertEqual(st, "ok", err)
        self.assertEqual(v['arca_node_capacity_bytes{kind="available"}'], {"value": 123.0, "unit": "bytes"})
        self.assertNotIn("other_metric", v)

    def test_an_absent_family_is_partial_not_zero(self):
        s = self.serve("ok")
        st, v, err = sampler.read_metrics({"url": s.url(), "families": [
            "arca_node_capacity_bytes", "arca_node_compaction_backlog_bytes"]}, 2)
        self.assertEqual(st, "partial")
        self.assertNotIn("arca_node_compaction_backlog_bytes", v)
        self.assertIn("arca_node_compaction_backlog_bytes", err)

    def test_timeouts_and_errors_are_told_apart(self):
        self.assertEqual(sampler.read_metrics({"url": self.serve("slow").url(), "families": []}, 0.3)[0], "timeout")
        self.assertEqual(sampler.read_metrics({"url": self.serve("500").url(), "families": []}, 2)[0], "error")
        self.assertEqual(sampler.read_metrics({"url": "http://127.0.0.1:1/metrics", "families": []}, 2)[0], "error")


class Dirs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        os.makedirs(os.path.join(self.d, "sub"))
        for n, size in (("seg-1", 4096), ("seg-2", 100), ("sub/x", 10)):
            with open(os.path.join(self.d, n), "wb") as f:
                f.write(b"\0" * size)

    def tearDown(self):
        self.tmp.cleanup()

    def test_apparent_and_allocated_bytes(self):
        st, v, err = sampler.read_dir({"path": self.d})
        self.assertEqual(st, "ok", err)
        self.assertEqual(v["apparent_bytes"]["value"], 4206)
        self.assertGreater(v["allocated_bytes"]["value"], 0)

    def test_a_file_deleted_during_the_walk_restarts_it(self):
        state = {"done": False}

        def stat(p):
            if p.endswith("seg-1") and not state["done"]:
                state["done"] = True
                os.remove(p)
            return os.lstat(p)

        st, v, err = sampler.read_dir({"path": self.d}, stat)
        self.assertTrue(state["done"])
        self.assertEqual((st, v["apparent_bytes"]["value"]), ("ok", 110))

    def test_vanishing_in_every_walk_is_partial_with_no_value(self):
        def stat(_p):
            raise FileNotFoundError(2, "gone")

        st, v, err = sampler.read_dir({"path": self.d}, stat)
        self.assertEqual((st, v), ("partial", {}))

    def test_other_errors_are_errors(self):
        def stat(_p):
            raise PermissionError(13, "denied")

        st, v, err = sampler.read_dir({"path": self.d}, stat)
        self.assertEqual((st, v), ("error", {}))
        self.assertIn("EACCES", err)

    def test_a_missing_directory_is_an_error(self):
        self.assertEqual(sampler.read_dir({"path": os.path.join(self.d, "nope")})[0], "error")


class Logs(unittest.TestCase):
    def test_counts_are_incremental_whole_lines_and_rotation_is_partial(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "ctl.log")
            r = sampler.LogReader()
            with open(p, "w") as f:
                f.write("x \x1b[32mINFO\x1b[0m reconcile pass deleted=3 unreachable=1\n")
                f.write("reconcile pass deleted=2 unrea")  # being written
            st, v, _ = r.read({"path": p})
            self.assertEqual((st, v["reconcile_deleted"]["value"], v["reconcile_passes"]["value"]), ("ok", 3, 1))
            with open(p, "a") as f:
                f.write("chable=0\n")
            st, v, _ = r.read({"path": p})
            self.assertEqual((v["reconcile_deleted"]["value"], v["reconcile_passes"]["value"]), (5, 2))
            with open(p, "w") as f:
                f.write("reconcile pass deleted=1 unreachable=0\n")
            st, v, err = r.read({"path": p})
            self.assertEqual(st, "partial")
            self.assertIn("shorter", err)


class Records(unittest.TestCase):
    def test_each_sample_is_stamped_and_the_output_is_bounded(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "samples.jsonl")
            cfg = {"run_id": "run-s", "interval": 0.0, "max_out_bytes": 2000,
                   "targets": [{"name": "dir", "kind": "dir", "path": d},
                               {"name": "gone", "kind": "dir", "path": os.path.join(d, "missing")}]}
            s = sampler.Sampler(cfg, out, common.Clock())
            rc = s.run(rounds=1000)
            self.assertEqual(rc, 2)
            self.assertLessEqual(os.path.getsize(out), 2000 + 200)
            lines = [json.loads(x) for x in open(out)]
            self.assertEqual(lines[-1]["sampler_stopped"], "output limit")
            recs = lines[:-1]
            for r in recs:
                for k in ("run_id", "target", "started_wall_ms", "ended_wall_ms", "duration_ms", "status"):
                    self.assertIn(k, r)
                self.assertGreaterEqual(r["ended_wall_ms"], r["started_wall_ms"])
                if r["target"] == "gone":
                    self.assertEqual((r["status"], r["values"]), ("error", {}))

    def test_an_exception_in_a_reader_is_an_error_sample(self):
        s = sampler.Sampler({"run_id": "r", "targets": [{"name": "x", "kind": "weird"}]}, os.devnull)
        self.assertEqual(s.one({"name": "x", "kind": "weird"})["status"], "error")


if __name__ == "__main__":
    unittest.main()
