#!/usr/bin/env python3
"""Run one training node as its own process.

    run_node.py --id ID --db PATH --y P/Q [--y P/Q ...] [--port N]

Prints one JSON line with the bound address, then serves until SIGTERM or
SIGKILL.  Only the node's private database is authoritative; the target array
is written there once, on the first start.
"""

from __future__ import annotations

import sys

from fedtrain.node_server import main

if __name__ == "__main__":
    sys.exit(main())
