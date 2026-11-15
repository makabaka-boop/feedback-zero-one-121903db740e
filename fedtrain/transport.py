"""HTTP plumbing shared by the coordinator and the training nodes.

A node and the coordinator each own a small JSON-over-HTTP server.  The
transport is deliberately boring: one ``POST`` per request, a JSON object as
the body and a JSON object as the reply.  A rejected request carries
``{"error": <JSONPath>}`` with HTTP status 400.

Nothing in this module retries: retry policy belongs to the coordinator,
because only the coordinator knows that its operations are idempotent.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import ProtocolError, RemoteError

__all__ = ["post_json", "JsonServer"]


def post_json(url, body, timeout=5.0):
    """POST ``body`` as JSON and return the parsed reply.

    Raises :class:`RemoteError` if the peer answered with an error object and
    lets every transport failure (refused connection, timeout) propagate.
    """
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Connection": "close"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except ValueError:
            payload = {"error": "$"}
        if isinstance(payload, dict) and isinstance(payload.get("error"), str):
            raise RemoteError(payload["error"], status=exc.code) from None
        raise RemoteError("$", status=exc.code) from None
    payload = json.loads(raw.decode("utf-8"))
    if isinstance(payload, dict) and "error" in payload:
        raise RemoteError(payload["error"])
    return payload


class JsonServer:
    """A threaded HTTP server dispatching ``POST <path>`` to ``handler``."""

    def __init__(self, handler, port=0, host="127.0.0.1"):
        self.handler = handler
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):  # keep test output readable
                pass

            def _reply(self, status, reply):
                payload = json.dumps(reply).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw.decode("utf-8"))
                except ValueError:
                    self._reply(400, {"error": "$"})
                    return
                try:
                    reply = outer.handler(self.path, body)
                except ProtocolError as exc:
                    self._reply(400, exc.body())
                    return
                except Exception as exc:  # defensive: never leak a traceback
                    self._reply(500, {"error": "$", "detail": repr(exc)})
                    return
                self._reply(200, reply)

            def do_GET(self):
                self._reply(200, {"ok": True})

        self.httpd = ThreadingHTTPServer((host, port), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def port(self):
        return self.httpd.server_address[1]

    @property
    def address(self):
        return "http://%s:%d" % self.httpd.server_address[:2]

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
