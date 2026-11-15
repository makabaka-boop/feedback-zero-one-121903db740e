"""The private cursor indexes the target array modulo its length."""

import unittest

from tests.harness import ClusterCase


class MultiElementTargets(ClusterCase):
    targets = {"n": ["1/1", "2/1"]}

    def test_cursor_walks_the_array_and_wraps(self):
        cluster = self.start()
        node = cluster.nodes["n"]
        model = {"w": "0/1"}

        # Round 1: c = 0, so y[0] = 1, g = -1 -> q = -1, r = 0.
        first = cluster.open("r1")
        self.assertEqual(first["state"], "collecting")
        self.assertEqual(node.compute(1, "0/1"), {"round": 1, "q": "-1/1",
                                                  "residual": "0/1"})
        cluster.seal("r1", ["n"])
        self.assertEqual(node.cursor(), 1)
        # m1 = mean(-1) = -1, w1 = 1.
        self.assertEqual(cluster.model(), {"w": "1/1", "m": "-1/1"})

        # Round 2: c = 1, so y[1] = 2, g = 1 - 2 = -1 -> q = -1, r = 0.
        cluster.open("r2")
        self.assertEqual(node.compute(2, "1/1"), {"round": 2, "q": "-1/1",
                                                  "residual": "0/1"})
        cluster.seal("r2", ["n"])
        self.assertEqual(node.cursor(), 2)
        # m2 = -1/2 + -1 = -3/2, w2 = 1 - (-3/2) = 5/2.
        self.assertEqual(cluster.model(), {"w": "5/2", "m": "-3/2"})

        # Round 3: c = 2, which wraps back to y[0] = 1.  g = 5/2 - 1 = 3/2,
        # an exact tie, so q = 2 and the residual becomes -1/2.
        cluster.open("r3")
        self.assertEqual(node.compute(3, "5/2"), {"round": 3, "q": "2/1",
                                                  "residual": "-1/2"})
        self.assertEqual(node.cursor(), 2)  # unchanged until the decision

    def test_excluded_round_does_not_move_the_cursor(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", [])
        self.assertEqual(cluster.nodes["n"].cursor(), 0)
        # Round 2 still reads y[0].
        cluster.open("r2")
        self.assertEqual(cluster.nodes["n"].compute(2, "0/1")["q"], "-1/1")


class CursorMonotonic(unittest.TestCase):
    def test_cursor_only_advances_on_inclusion(self):
        import os
        import shutil
        import tempfile

        from fedtrain.node import Node

        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, True)
        node = Node("x", ["0/1", "0/1", "0/1"], os.path.join(root, "x.db"))
        self.addCleanup(node.close)
        node.compute(1, "0/1")
        node.decide(1, True)
        self.assertEqual(node.cursor(), 1)
        node.compute(2, "0/1")
        node.decide(2, False)
        self.assertEqual(node.cursor(), 1)
        node.compute(3, "0/1")
        node.decide(3, True)
        self.assertEqual(node.cursor(), 2)
        self.assertEqual(node.cursor() % 3, 2)


if __name__ == "__main__":
    unittest.main()
