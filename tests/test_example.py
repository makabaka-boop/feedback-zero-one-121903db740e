"""The acceptance example: one node, y = ["1/4"], two consecutive inclusions."""

import unittest

from tests.harness import ClusterCase


class SingleNodeExample(ClusterCase):
    targets = {"n1": ["1/4"]}

    def test_two_rounds(self):
        cluster = self.start()

        first = cluster.open("r1")
        self.assertEqual(first, {"round": 1, "state": "collecting",
                                 "ready": ["n1"], "members": None})
        sealed = cluster.seal("r1", ["n1"])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(sealed["members"], ["n1"])
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})

        second = cluster.open("r2")
        self.assertEqual(second["round"], 2)
        self.assertEqual(second["state"], "collecting")
        self.assertEqual(second["ready"], ["n1"])
        self.assertIsNone(second["members"])
        sealed2 = cluster.seal("r2", ["n1"])
        self.assertEqual(sealed2["state"], "done")
        self.assertEqual(cluster.model(), {"w": "1/1", "m": "-1/1"})

        state = cluster.node_state("n1")
        self.assertEqual(state["cursor"], 2)
        # round 2: g + r = -1/4 + -1/4 = -1/2, rounded away from zero to -1,
        # so the candidate residual is -1/2 - (-1) = 1/2.
        self.assertEqual(state["residual"], "1/2")


if __name__ == "__main__":
    unittest.main()
