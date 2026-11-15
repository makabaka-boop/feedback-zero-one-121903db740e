"""The node's HTTP face: ``compute``, ``decide`` and ``revoke``.

The node only ever reads its own target array; it learns the global model only
through the ``w`` the coordinator sends with each ``compute``.  Rejections
follow the protocol convention ``{"error": <JSONPath>}`` (HTTP 400).
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from .errors import ProtocolError
from .node import Node
from .transport import JsonServer

__all__ = ["NodeServer"]


class NodeServer:
    def __init__(self, node, port=0, host="127.0.0.1"):
        if not isinstance(node, Node):
            raise TypeError("node must be a Node")
        self.node = node
        self.server = JsonServer(self._dispatch, port=port, host=host)

    def handle(self, path, body):
        if not isinstance(body, dict):
            raise ProtocolError("$", "the body must be a JSON object")

        if path == "/snapshot":
            return self.node.snapshot()

        if path == "/compute":
            if "round" not in body:
                raise ProtocolError("$.round", "missing field")
            if "w" not in body:
                raise ProtocolError("$.w", "missing field")
            return self.node.compute(body["round"], body["w"])

        if path == "/decide":
            if "round" not in body:
                raise ProtocolError("$.round", "missing field")
            if "included" not in body:
                raise ProtocolError("$.included", "missing field")
            return self.node.decide(body["round"], body["included"])

        if path == "/revoke":
            if "round" not in body:
                raise ProtocolError("$.round", "missing field")
            return self.node.revoke(body["round"])

        raise ProtocolError("$", "unknown endpoint")

    def _dispatch(self, path, body):
        return self.handle(path, body)

    @property
    def address(self):
        return self.server.address

    @property
    def port(self):
        return self.server.port

    def start(self):
        self.server.start()
        return self

    def stop(self):
        self.server.stop()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description="run one training node over HTTP")
    parser.add_argument("--id", required=True, help="node identifier")
    parser.add_argument(
        "--y",
        action="append",
        required=True,
        metavar="P/Q",
        help="one target value; repeat for a longer array",
    )
    parser.add_argument("--db", required=True, help="private SQLite file")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args(argv)

    node = Node(args.id, args.y, args.db)
    server = NodeServer(node, port=args.port, host=args.host).start()
    print(json.dumps({"id": node.ident, "address": server.address}), flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    server.stop()
    node.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
