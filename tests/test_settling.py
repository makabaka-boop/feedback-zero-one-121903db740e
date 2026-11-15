"""``settling``: a round stays unfinished until every node confirms its decision."""

import os
import unittest

from fedtrain.cluster import Cluster
from fedtrain.errors import RemoteError
from fedtrain.transport import post_json
from tests.harness import ClusterCase


class UnavailableNode(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/6"]}

    def test_seal_returns_settling_when_a_node_cannot_be_reached(self):
        down = {"b": False}

        def flaky(url, body, timeout=5.0):
            if down["b"] and url.startswith(self._node_b_url):
                raise OSError("connection refused")
            return post_json(url, body, timeout=timeout)

        cluster = Cluster(self.targets, os.path.join(self.root, "run"), fetch=flaky)
        self.addCleanup(cluster.stop)
        self._node_b_url = cluster.node_servers["b"].address

        cluster.open("r1")
        down["b"] = True
        sealed = cluster.seal("r1", ["a", "b"])
        # a has applied its decision, b has not even been reached, so the round
        # is not done yet.
        self.assertEqual(sealed["state"], "settling")
        self.assertEqual(sealed["members"], ["a", "b"])
        self.assertEqual(cluster.node_state("a")["cursor"], 1)
        self.assertEqual(cluster.node_state("b")["cursor"], 0)  # never advanced

        # The node comes back; the next status re-delivers the decision.
        down["b"] = False
        status = cluster.status("r1")
        self.assertEqual(status["state"], "done")
        self.assertEqual(cluster.node_state("b")["cursor"], 1)
        # Both candidates were 0: q(a) = q(b) = 0, so m' = 0 and w' = 0.
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})

    def test_a_new_round_is_refused_while_settling(self):
        down = {"b": False}

        def flaky(url, body, timeout=5.0):
            if down["b"] and url.startswith(self._node_b_url):
                raise OSError("connection refused")
            return post_json(url, body, timeout=timeout)

        cluster = Cluster(self.targets, os.path.join(self.root, "run"), fetch=flaky)
        self.addCleanup(cluster.stop)
        self._node_b_url = cluster.node_servers["b"].address
        cluster.open("r1")
        down["b"] = True
        self.assertEqual(cluster.seal("r1", ["a", "b"])["state"], "settling")
        with self.assertRaises(RemoteError) as caught:
            cluster.open("r2")
        self.assertEqual(caught.exception.path, "$.id")
        down["b"] = False
        self.assertEqual(cluster.status("r1")["state"], "done")
        self.assertEqual(cluster.open("r2")["round"], 2)

    def test_settling_round_freezes_ready(self):
        down = {"b": False}

        def flaky(url, body, timeout=5.0):
            if down["b"] and url.startswith(self._node_b_url):
                raise OSError("connection refused")
            return post_json(url, body, timeout=timeout)

        cluster = Cluster(self.targets, os.path.join(self.root, "run"), fetch=flaky)
        self.addCleanup(cluster.stop)
        self._node_b_url = cluster.node_servers["b"].address
        cluster.open("r1")
        down["b"] = True
        sealed = cluster.seal("r1", ["a"])
        # b never confirmed, so the round is held in settling.
        self.assertEqual(sealed["state"], "settling")
        # The frozen ready list is exactly the candidates stored before the
        # seal.  b never heard about the decision, so its stored candidate is
        # still there and a repeated compute simply replays it.
        self.assertEqual(cluster.status("r1")["ready"], ["a", "b"])
        replay = cluster.nodes["b"].compute(1, "0/1")
        self.assertEqual(replay["round"], 1)
        self.assertEqual(cluster.status("r1")["ready"], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
