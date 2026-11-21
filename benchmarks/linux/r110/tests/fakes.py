"""Test doubles: a scripted client, a recording one, a local fake S3."""

import hashlib
import http.server
import threading
import time

from r110 import s3


class Scripted:
    """A client whose every request answers ``answer(method, path, data)``
    (a ``s3.Response``) and is recorded with the clock's time. Shared by
    every worker (one instance), so counts are global."""

    def __init__(self, clock, answer, hold=0.0):
        self.clock = clock
        self.answer = answer
        self.hold = hold
        self.lock = threading.Lock()
        self.sent = []
        self.inflight = 0
        self.max_inflight = 0

    def factory(self, _url):
        return self

    def request(self, method, path, data=None):
        with self.lock:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            self.sent.append((self.clock.monotonic(), method, path, len(data or b"")))
        try:
            if self.hold:
                time.sleep(self.hold)
            return self.answer(method, path, data)
        finally:
            with self.lock:
                self.inflight -= 1


def slow_down(*_):
    return s3.Response(503, b"<Error><Code>SlowDown</Code></Error>", sent=True)


def ok(method, *_):
    return s3.Response(200 if method != "DELETE" else 204, b"", sent=True)


class FakeS3(http.server.ThreadingHTTPServer):
    """A local in-memory S3 subset (PUT, GET, DELETE of objects; bucket PUT;
    ListObjectsV2 of one page). ``corrupt``: keys whose GET returns other
    bytes; ``refuse_puts``: answer every object PUT 503 SlowDown."""

    daemon_threads = True

    def __init__(self):
        self.objects = {}
        self.corrupt = set()
        self.refuse_puts = False
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), _Handler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}"

    def close(self):
        self.shutdown()
        self.server_close()


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def _send(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(n)
        parts = self.path.strip("/").split("/", 1)
        if len(parts) == 1:
            return self._send(200)
        if self.server.refuse_puts:
            return self._send(503, b"<Error><Code>SlowDown</Code></Error>")
        with self.server.lock:
            self.server.objects[parts[1]] = data
        self._send(200)

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = path.strip("/").split("/", 1)
        if len(parts) == 1:
            return self._send(200, b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
        with self.server.lock:
            data = self.server.objects.get(parts[1])
        if data is None:
            return self._send(404)
        if parts[1] in self.server.corrupt:
            data = hashlib.sha256(data).digest()
        self._send(200, data)

    def do_DELETE(self):
        parts = self.path.strip("/").split("/", 1)
        with self.server.lock:
            self.server.objects.pop(parts[1], None)
        self._send(204)
