"""Faults: duplicate deliveries, lost replies, restarts and hard kills.

Every failure mode here is one the protocol explicitly has to survive:
communication may repeat, replies may be lost, and a process may be killed
right after a decision has been written to disk.
"""

import os
import unittest

from fedtrain.cluster import Cluster
from tests.harness import ClusterCase


class FlakyFetch:
    """Wraps the real transport; can drop replies and repeat deliveries."""

    def __init__(self, fetch, drop_after_send=0, repeat=1):
        self.fetch = fetch
        self.drop_after_send = drop_after_send  # drop the reply *after* it landed
        self.repeat = repeat
        self.calls = []

    def __call__(self, url, body, timeout=5.0):
        self.calls.append((url, dict(body)))
        reply = None
        error = None
        for _ in range(self.repeat):
            try:
                reply = self.fetch(url, body, timeout=timeout)
            except Exception as exc:  # replay the same call on failure
                error = exc
                continue
        if error is not None and reply is None:
            raise error
        return reply


class Duplicates(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/4"]}

    def test_a_repeated_compute_returns_the_stored_candidate(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        first = node.compute(1, "0/1")
        second = node.compute(1, "0/1")
        self.assertEqual(first, second)
        self.assertEqual(node.cursor(), 0)

    def test_a_repeated_decision_advances_once(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        node.compute(1, "0/1")
        first = node.decide(1, True)
        self.assertTrue(first["applied"])
        self.assertEqual(node.cursor(), 1)
        second = node.decide(1, True)
        self.assertFalse(second["applied"])
        self.assertEqual(second["reason"], "replay")
        self.assertEqual(node.cursor(), 1)

    def test_a_repeated_seal_only_applies_the_model_once(self):
        cluster = self.start()
        cluster.open("r1")
        first = cluster.seal("r1", ["a"])
        # m' = 0 + 0 = 0 for the first round, and a second seal must not
        # apply the update again.
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})
        for _ in range(3):
            self.assertEqual(cluster.seal("r1", ["a"]), first)
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})
        self.assertEqual(cluster.node_state("a")["cursor"], 1)

    def test_a_duplicated_open_does_not_create_a_second_round(self):
        cluster = self.start()
        for _ in range(3):
            self.assertEqual(cluster.open("r1")["round"], 1)
        self.assertEqual(cluster.status("r1")["round"], 1)

    def test_an_excluded_decision_replayed_never_advances(self):
        cluster = self.start()
        node = cluster.nodes["b"]
        node.compute(1, "0/1")
        for _ in range(3):
            node.decide(1, False)
        self.assertEqual(node.cursor(), 0)
        self.assertEqual(node.pending_rounds(), [])


class LostReplies(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/6"]}

    def test_lost_node_replies_are_retried_by_the_next_status(self):
        """A node reply that never comes back is retried, not lost."""
        from fedtrain.transport import post_json

        node_ports = set()
        lost = set()

        def lossy(url, body, timeout=5.0):
            reply = post_json(url, body, timeout=timeout)
            # Only node calls lose a reply, and only the first time each time:
            # the retry must then recover the value the node already stored.
            if any(":%d" % port in url for port in node_ports):
                key = (url, str(body.get("round")))
                if key not in lost:
                    lost.add(key)
                    raise OSError("reply lost")
            return reply

        cluster = Cluster(self.targets, os.path.join(self.root, "run"), fetch=lossy)
        self.addCleanup(cluster.stop)
        node_ports.update(s.server.port for s in cluster.node_servers.values())
        status = cluster.open("r1")
        self.assertEqual(status["ready"], [])
        # The nodes did compute; the retry through status must pick the values up.
        again = cluster.status("r1")
        self.assertEqual(again["ready"], ["a", "b"])

    def test_lost_decision_replies_are_retried(self):
        from fedtrain.transport import post_json

        state = {"drop_next_decide_reply": 0}
        node_ports = set()

        def flaky(url, body, timeout=5.0):
            reply = post_json(url, body, timeout=timeout)
            if (
                any(":%d" % port in url for port in node_ports)
                and url.endswith("/decide")
                and state["drop_next_decide_reply"]
            ):
                state["drop_next_decide_reply"] -= 1
                raise OSError("reply lost")
            return reply

        cluster = Cluster(self.targets, os.path.join(self.root, "run"), fetch=flaky)
        self.addCleanup(cluster.stop)
        node_ports.update(s.server.port for s in cluster.node_servers.values())
        cluster.open("r1")
        state["drop_next_decide_reply"] = 1
        sealed = cluster.seal("r1", ["a", "b"])
        # One decision reply was dropped, so round 1 is still settling.
        self.assertIn(sealed["state"], ("settling", "done"))
        converged = cluster.status("r1")
        self.assertEqual(converged["state"], "done")
        for ident in ("a", "b"):
            self.assertEqual(cluster.node_state(ident)["cursor"], 1)


class Cancellation(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/4"]}

    def test_an_early_revocation_blocks_a_late_computation(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        # The cancellation arrives before the candidate was ever computed.
        node.revoke(1)
        with self.assertRaises(Exception) as caught:
            node.compute(1, "0/1")
        self.assertEqual(caught.exception.path, "$.round")
        self.assertEqual(node.cursor(), 0)
        self.assertEqual(node.pending_rounds(), [])

    def test_a_revocation_after_a_candidate_discards_it(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        node.compute(1, "0/1")
        self.assertEqual(node.pending_rounds(), [1])
        node.revoke(1)
        self.assertEqual(node.pending_rounds(), [])
        node.revoke(1)  # replaying a revocation is harmless
        self.assertEqual(node.cursor(), 0)

    def test_a_late_decision_after_a_revocation_does_not_advance(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        node.compute(1, "0/1")
        node.revoke(1)
        result = node.decide(1, True)
        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "revoked")
        self.assertEqual(node.cursor(), 0)

    def test_cancelling_a_round_through_seal_revokes_every_node(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", [])
        with self.assertRaises(Exception) as caught:
            cluster.nodes["b"].compute(1, "0/1")
        self.assertEqual(caught.exception.path, "$.round")


class Restart(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/6"]}

    def test_a_restarted_coordinator_resumes_its_round(self):
        root = os.path.join(self.root, "run")
        cluster = Cluster(self.targets, root)
        first = cluster.open("r1")
        self.assertEqual(first["state"], "collecting")
        coordinator_db = cluster.coordinator.db
        registry = {
            ident: cluster.node_servers[ident].address for ident in self.targets
        }
        # Keep the nodes alive; replace only the coordinator.
        cluster.coordinator.close()
        cluster.server.stop()

        from fedtrain.coordinator import Coordinator, CoordinatorServer

        revived = Coordinator(os.path.join(root, "coordinator.db"), registry)
        cluster.coordinator = revived
        cluster.server = CoordinatorServer(revived).start()
        self.addCleanup(cluster.stop)

        resumed = cluster.status("r1")
        self.assertEqual(resumed["round"], 1)
        self.assertEqual(resumed["state"], "collecting")
        sealed = cluster.seal("r1", ["a"])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})

    def test_a_restarted_node_keeps_cursor_and_residual(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["a", "b"])
        before = cluster.node_state("a")
        self.assertEqual(before["cursor"], 1)

        from fedtrain.node import Node
        from fedtrain.node_server import NodeServer

        db_path = os.path.join(self.root, "run", "nodes", "a.db")
        cluster.nodes["a"].close()
        cluster.node_servers["a"].stop()
        revived = Node("a", ["1/4"], db_path)
        cluster.nodes["a"] = revived
        cluster.node_servers["a"] = NodeServer(revived).start()
        cluster.coordinator.db.execute(
            "UPDATE nodes SET url = ? WHERE id = 'a'",
            (cluster.node_servers["a"].address,),
        )
        cluster.coordinator.db.commit()

        self.assertEqual(cluster.node_state("a"), before)
        second = cluster.open("r2")
        self.assertEqual(second["round"], 2)
        sealed = cluster.seal("r2", ["a", "b"])
        self.assertEqual(sealed["state"], "done")
        # Round 1 leaves m = 0 and both nodes at cursor 1 with residual -1/4,
        # -1/6.  Round 2: a rounds -1/2 away from zero to -1, b rounds -1/3 to
        # 0, so m2 = 0/2 + mean(-1, 0) = -1/2 and w2 = 0 - (-1/2) = 1/2.
        self.assertEqual(cluster.model(), {"w": "1/2", "m": "-1/2"})

    def test_a_node_refuses_to_start_with_a_different_target_array(self):
        cluster = self.start()
        db_path = os.path.join(self.root, "run", "nodes", "a.db")
        cluster.nodes["a"].close()
        from fedtrain.node import Node

        with self.assertRaises(RuntimeError):
            Node("a", ["9/9"], db_path)


class BadConfiguration(ClusterCase):
    targets = {"a": ["1/4"], "b": ["1/6"]}

    def test_a_changed_registry_is_refused_on_restart(self):
        root = os.path.join(self.root, "run")
        cluster = Cluster(self.targets, root)
        cluster.stop()
        with self.assertRaises(Exception):
            Cluster({"a": ["1/4"]}, root)


if __name__ == "__main__":
    unittest.main()
