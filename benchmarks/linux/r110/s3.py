"""The data-plane HTTP client of the R-110 load: anonymous path-style S3
requests (a gateway started with ``--allow-anonymous``, test clusters
only) over ``http.client``. No retry of any kind happens here: one call is
one HTTP attempt, sent only by the load after a budget permit. A broken
connection is dropped and reopened at the next attempt.
"""

import hashlib
import http.client
import time
import urllib.parse


def body(key, version, size):
    """The deterministic body of ``key`` at ``version`` and its sha256."""
    seed = hashlib.sha256(f"{key}\0{version}".encode()).digest()
    out = bytearray()
    while len(out) < size:
        seed = hashlib.sha256(seed).digest()
        out += seed
    data = bytes(out[:size])
    return data, hashlib.sha256(data).hexdigest()


class Response:
    def __init__(self, status, payload, error=None, seconds=0.0, sent=False):
        self.status = status
        self.payload = payload
        self.error = error
        self.seconds = seconds
        # Whether any byte of the request may have reached the server.
        self.sent = sent

    def slow_down(self):
        return self.status == 503 and b"SlowDown" in (self.payload or b"")


class Client:
    def __init__(self, url, timeout=30.0):
        u = urllib.parse.urlsplit(url)
        if u.scheme != "http" or not u.hostname:
            raise ValueError(f"endpoint {url!r}: http://host:port only")
        self.host, self.port = u.hostname, u.port or 80
        self.timeout = timeout
        self._c = None

    def request(self, method, path, data=None):
        t0 = time.monotonic()
        sent = False
        try:
            if self._c is None:
                self._c = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            headers = {"Content-Length": str(len(data))} if data is not None else {}
            sent = True
            self._c.request(method, path, body=data, headers=headers)
            r = self._c.getresponse()
            payload = r.read()
            return Response(r.status, payload, seconds=time.monotonic() - t0, sent=True)
        except (OSError, http.client.HTTPException) as e:
            try:
                if self._c:
                    self._c.close()
            finally:
                self._c = None
            return Response(0, b"", error=f"{type(e).__name__}: {e}",
                            seconds=time.monotonic() - t0, sent=sent)

    def close(self):
        if self._c:
            self._c.close()
            self._c = None
