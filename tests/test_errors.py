"""Rejections: bad types, unknown references, parameter mismatches, conflicts."""

import unittest

import fedtrain
from fedtrain.errors import ProtocolError
from tests.harness import ClusterCase


class BodyShape(ClusterCase):
    targets = {"a": ["0/1"]}

    def test_non_object_bodies_are_rejected(self):
        cluster = self.start()
        server = cluster.server
        for body in (None, [], "text", 7):
            with self.subTest(body=body):
                with self.assertRaises(ProtocolError) as caught:
                    server.handle("/open", body)
                self.assertEqual(caught.exception.path, "$")
                self.assertEqual(caught.exception.body(), {"error": "$"})

    def test_missing_fields_are_reported_by_path(self):
        cluster = self.start()
        cases = [
            ("/open", {}, "$.id"),
            ("/status", {}, "$.id"),
            ("/seal", {}, "$.id"),
            ("/seal", {"id": "r1"}, "$.members"),
        ]
        for path, body, expected in cases:
            with self.subTest(path=path, body=body):
                with self.assertRaises(ProtocolError) as caught:
                    cluster.server.handle(path, body)
                self.assertEqual(caught.exception.path, expected)

    def test_unknown_endpoint_is_rejected(self):
        cluster = self.start()
        with self.assertRaises(ProtocolError) as caught:
            cluster.server.handle("/nope", {})
        self.assertEqual(caught.exception.path, "$")


class Identifiers(ClusterCase):
    targets = {"a": ["0/1"]}

    def test_bad_identifiers_are_rejected_at_dollar_dot_id(self):
        cluster = self.start()
        for bad in ("", "a-b", "a_b", "a b", "ünïcode", 5, None, ["a"], True):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    cluster.server.handle("/open", {"id": bad})
                self.assertEqual(caught.exception.path, "$.id")

    def test_status_of_an_unknown_round_is_rejected(self):
        cluster = self.start()
        with self.assertRaises(ProtocolError) as caught:
            cluster.status("never")
        self.assertEqual(caught.exception.path, "$.id")

    def test_seal_of_an_unknown_round_is_rejected(self):
        cluster = self.start()
        with self.assertRaises(ProtocolError) as caught:
            cluster.seal("never", ["a"])
        self.assertEqual(caught.exception.path, "$.id")


class SealArguments(ClusterCase):
    targets = {"a": ["0/1"], "b": ["0/1"]}

    def test_members_must_be_an_array(self):
        cluster = self.start()
        cluster.open("r1")
        for bad in (None, "a", {"a": 1}, 3):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    cluster.seal("r1", bad)
                self.assertEqual(caught.exception.path, "$.members")

    def test_invalid_identifiers_inside_members_are_indexed(self):
        cluster = self.start()
        cluster.open("r1")
        for index, bad in enumerate((["a", ""], ["a", 5], ["a", "x_y"])):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    cluster.seal("r1", bad)
                self.assertEqual(caught.exception.path, "$.members[1]")

    def test_a_rejected_seal_leaves_no_trace(self):
        cluster = self.start()
        before = cluster.open("r1")
        for bad in (["a", "a"], ["a", "zzz"], ["a", 7]):
            with self.assertRaises(ProtocolError):
                cluster.seal("r1", bad)
        self.assertEqual(cluster.status("r1"), before)
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})
        # The round is still open and can be sealed properly.
        self.assertEqual(cluster.seal("r1", ["a"])["state"], "done")


class NodeArguments(ClusterCase):
    targets = {"a": ["0/1"]}

    def test_round_must_be_a_positive_integer(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        for bad in (0, -1, "1", 1.0, True, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    node.compute(bad, "0/1")
                self.assertEqual(caught.exception.path, "$.round")
            with self.subTest(bad=bad, op="decide"):
                with self.assertRaises(ProtocolError) as caught:
                    node.decide(bad, True)
                self.assertEqual(caught.exception.path, "$.round")

    def test_w_must_be_a_pq_string(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        for bad in ("", "0", "1/0", "x/y", 0, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    node.compute(1, bad)
                self.assertEqual(caught.exception.path, "$.w")

    def test_included_must_be_a_boolean(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        node.compute(1, "0/1")
        for bad in (1, 0, "true", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError) as caught:
                    node.decide(1, bad)
                self.assertEqual(caught.exception.path, "$.included")

    def test_a_different_w_for_the_same_round_is_a_mismatch(self):
        cluster = self.start()
        node = cluster.nodes["a"]
        node.compute(1, "0/1")
        with self.assertRaises(ProtocolError) as caught:
            node.compute(1, "1/1")
        self.assertEqual(caught.exception.path, "$.w")
        # The originally persisted candidate survives untouched.
        self.assertEqual(node.compute(1, "0/1"), {"round": 1, "q": "0/1",
                                                  "residual": "0/1"})

    def test_an_included_decision_without_a_candidate_is_rejected(self):
        cluster = self.start()
        with self.assertRaises(ProtocolError) as caught:
            cluster.nodes["a"].decide(9, True)
        self.assertEqual(caught.exception.path, "$.round")
        self.assertEqual(cluster.node_state("a")["cursor"], 0)


class FrozenRound(ClusterCase):
    targets = {"a": ["0/1"]}

    def test_computing_for_a_decided_round_is_rejected(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["a"])
        with self.assertRaises(ProtocolError) as caught:
            cluster.nodes["a"].compute(1, "0/1")
        self.assertEqual(caught.exception.path, "$.round")
        self.assertEqual(cluster.node_state("a")["cursor"], 1)


if __name__ == "__main__":
    unittest.main()
