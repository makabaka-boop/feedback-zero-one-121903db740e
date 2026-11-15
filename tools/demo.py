#!/usr/bin/env python3
"""Run the acceptance example and print each step.

    python3 -m tools.demo

Starts one node with ``y = ["1/4"]`` and a coordinator, opens and seals two
rounds with that node selected, and shows the global model after each round.
The expected output is ``(0/1, 0/1)`` then ``(1/1, -1/1)``.
"""

from __future__ import annotations

import os
import sys
import tempfile

from fedtrain.cluster import Cluster


def main():
    root = tempfile.mkdtemp(prefix="fedtrain-demo-")
    cluster = Cluster({"n1": ["1/4"]}, root)
    try:
        for name in ("r1", "r2"):
            print("open  %s -> %s" % (name, cluster.open(name)))
            print("seal  %s -> %s" % (name, cluster.seal(name, ["n1"])))
            model = cluster.model()
            print("model       -> w=%s m=%s" % (model["w"], model["m"]))
            print("node state  -> %s" % cluster.node_state("n1"))
            print()
    finally:
        cluster.stop()
        import shutil

        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
