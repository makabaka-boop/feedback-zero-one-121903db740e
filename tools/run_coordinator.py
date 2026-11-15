#!/usr/bin/env python3
"""Run a coordinator as its own process.

    run_coordinator.py --db PATH --node ID=URL [--node ID=URL ...] [--port N]

Prints one JSON line with the bound address, then serves until SIGTERM or
SIGKILL.  The database is the coordinator's only memory, so a SIGKILL loses
nothing that was committed.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from fedtrain.coordinator import Coordinator, CoordinatorServer


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument(
        "--node", action="append", required=True, metavar="ID=URL",
        help="register one node; repeat for each node",
    )
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args(argv)

    nodes = {}
    for entry in args.node:
        ident, sep, url = entry.partition("=")
        if not sep or not ident or not url:
            parser.error("--node expects ID=URL, got %r" % entry)
        nodes[ident] = url

    coordinator = Coordinator(args.db, nodes, timeout=args.timeout)
    server = CoordinatorServer(coordinator, port=args.port, host=args.host).start()
    print(json.dumps({"address": server.address}), flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    server.stop()
    coordinator.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
