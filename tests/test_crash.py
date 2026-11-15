"""Hard kills with real processes: committed state must survive SIGKILL.

The acceptance requirement is specifically that a decision written to disk is
not lost when the process dies immediately afterwards.  These tests spawn the
coordinator and nodes as separate interpreters, do the work, ``SIGKILL`` them
without any chance to clean up, restart them on the same databases and check
that the federation resumes exactly where it stopped.
"""

import os
import sqlite3
import unittest

from fedtrain.transport import post_json
from tests.harness import ClusterCase
from tests.procs import CoordinatorProcess, NodeProcess


class HardKill(ClusterCase):
    def _db(self, *parts):
        return os.path.join(self.root, *parts)

    def test_committed_decision_survives_a_coordinator_kill(self):
        node_a = NodeProcess("a", ["1/4"], self._db("a.db"))
        self.addCleanup(node_a.stop)
        coord = CoordinatorProcess(self._db("c.db"), {"a": node_a.address})
        self.addCleanup(coord.stop)

        self.assertEqual(post_json(coord.address + "/open", {"id": "r1"})["state"],
                         "collecting")
        sealed = post_json(coord.address + "/seal", {"id": "r1", "members": ["a"]})
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(post_json(coord.address + "/model", {}),
                         {"w": "0/1", "m": "0/1"})

        coord.kill()

        # Restart on the same database and the same node; the model and the
        # round are still there.
        revived = CoordinatorProcess(self._db("c.db"), {"a": node_a.address})
        self.addCleanup(revived.stop)
        self.assertEqual(post_json(revived.address + "/model", {}),
                         {"w": "0/1", "m": "0/1"})
        status = post_json(revived.address + "/status", {"id": "r1"})
        self.assertEqual(status["round"], 1)
        self.assertEqual(status["state"], "done")

        # And a new round can proceed from the persisted model.
        second = post_json(revived.address + "/open", {"id": "r2"})
        self.assertEqual(second["round"], 2)
        post_json(revived.address + "/seal", {"id": "r2", "members": ["a"]})
        self.assertEqual(post_json(revived.address + "/model", {}),
                         {"w": "1/1", "m": "-1/1"})

    def test_a_node_killed_after_its_decision_keeps_the_advance(self):
        node_a = NodeProcess("a", ["1/4"], self._db("a.db"))
        self.addCleanup(node_a.stop)
        coord = CoordinatorProcess(self._db("c.db"), {"a": node_a.address})
        self.addCleanup(coord.stop)

        post_json(coord.address + "/open", {"id": "r1"})
        post_json(coord.address + "/seal", {"id": "r1", "members": ["a"]})
        self.assertEqual(post_json(node_a.address + "/snapshot", {})["cursor"], 1)

        node_a.kill()

        revived = NodeProcess("a", ["1/4"], self._db("a.db"))
        self.addCleanup(revived.stop)
        snapshot = post_json(revived.address + "/snapshot", {})
        self.assertEqual(snapshot["cursor"], 1)
        self.assertEqual(snapshot["residual"], "-1/4")
        # A replay of the same decision must not advance a second time.
        reply = post_json(revived.address + "/decide",
                          {"round": 1, "included": True})
        self.assertFalse(reply["applied"])
        self.assertEqual(post_json(revived.address + "/snapshot", {})["cursor"], 1)

    def test_a_kill_between_publish_and_acknowledgement_still_converges(self):
        node_a = NodeProcess("a", ["1/4"], self._db("a.db"))
        self.addCleanup(node_a.stop)
        coord = CoordinatorProcess(self._db("c.db"), {"a": node_a.address})
        self.addCleanup(coord.stop)

        post_json(coord.address + "/open", {"id": "r1"})
        post_json(coord.address + "/seal", {"id": "r1", "members": ["a"]})

        # Simulate the crash window: the model was published but the round is
        # still 'settling' because no acknowledgement was recorded.
        db = sqlite3.connect(self._db("c.db"))
        db.execute("UPDATE rounds SET state = 'settling' WHERE round = 1")
        db.execute("DELETE FROM ack")
        db.commit()
        db.close()
        self.assertEqual(post_json(coord.address + "/model", {}),
                         {"w": "0/1", "m": "0/1"})

        coord.kill()

        revived = CoordinatorProcess(self._db("c.db"), {"a": node_a.address})
        self.addCleanup(revived.stop)
        # The restart re-sends the decision; the node recognises the replay and
        # confirms, so the round completes without moving the model again.
        status = post_json(revived.address + "/status", {"id": "r1"})
        self.assertEqual(status["state"], "done")
        self.assertEqual(post_json(revived.address + "/model", {}),
                         {"w": "0/1", "m": "0/1"})
        self.assertEqual(post_json(node_a.address + "/snapshot", {})["cursor"], 1)

    def test_a_target_array_is_written_once_even_across_a_kill(self):
        node_a = NodeProcess("a", ["1/4"], self._db("a.db"))
        self.addCleanup(node_a.stop)
        post_json(node_a.address + "/compute", {"round": 1, "w": "0/1"})
        node_a.kill()

        # Restarting with a different array must fail loudly rather than
        # silently changing the node's targets.
        with self.assertRaises(Exception):
            NodeProcess("a", ["1/2"], self._db("a.db"))


if __name__ == "__main__":
    unittest.main()
