"""Protocol semantics: status transitions, sealing, exclusion and cancellation."""

import os
import unittest

from tests.harness import ClusterCase


class StatusTransitions(ClusterCase):
    targets = {"a": ["0/1"], "b": ["0/1"], "c": ["0/1"]}

    def test_open_starts_at_round_one_and_collects(self):
        cluster = self.start()
        status = cluster.open("r1")
        self.assertEqual(status["round"], 1)
        self.assertEqual(status["state"], "collecting")
        self.assertEqual(status["ready"], ["a", "b", "c"])
        self.assertIsNone(status["members"])

    def test_same_id_returns_the_same_state(self):
        cluster = self.start()
        first = cluster.open("r1")
        again = cluster.open("r1")
        self.assertEqual(first, again)
        self.assertEqual(again["round"], 1)
        # Re-opening is idempotent: no extra round, no extra collection.
        self.assertEqual(cluster.status("r1"), first)

    def test_new_id_is_refused_until_the_previous_round_is_done(self):
        cluster = self.start()
        cluster.open("r1")
        with self.assertRaises(Exception) as caught:
            cluster.open("r2")
        self.assertEqual(caught.exception.path, "$.id")
        self.assertEqual(cluster.status("r1")["round"], 1)
        self.assertEqual(cluster.status("r1")["state"], "collecting")

    def test_new_id_admitted_after_done_and_numbering_increments(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["a"])
        self.assertEqual(cluster.status("r1")["state"], "done")
        second = cluster.open("r2")
        self.assertEqual(second["round"], 2)
        self.assertEqual(second["state"], "collecting")

    def test_round_numbers_restart_from_one_on_a_fresh_database(self):
        cluster = self.start()
        self.assertEqual(cluster.open("first")["round"], 1)


class Sealing(ClusterCase):
    targets = {"a": ["0/1"], "b": ["0/1"], "c": ["0/1"]}

    def test_seal_freezes_members_and_model(self):
        cluster = self.start()
        cluster.open("r1")
        sealed = cluster.seal("r1", ["a", "b"])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(sealed["members"], ["a", "b"])
        self.assertEqual(sealed["ready"], ["a", "b", "c"])

    def test_seal_members_are_reported_in_ascii_order(self):
        cluster = self.start()
        cluster.open("r1")
        sealed = cluster.seal("r1", ["c", "a"])
        self.assertEqual(sealed["members"], ["a", "c"])

    def test_same_members_in_another_order_replays(self):
        cluster = self.start()
        cluster.open("r1")
        first = cluster.seal("r1", ["a", "b"])
        second = cluster.seal("r1", ["b", "a"])
        self.assertEqual(first, second)
        self.assertEqual(cluster.status("r1")["state"], "done")

    def test_different_members_are_rejected_without_side_effects(self):
        cluster = self.start()
        cluster.open("r1")
        first = cluster.seal("r1", ["a", "b"])
        with self.assertRaises(Exception) as caught:
            cluster.seal("r1", ["a"])
        self.assertEqual(caught.exception.path, "$.members")
        self.assertEqual(cluster.status("r1"), first)
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})

    def test_duplicate_members_are_rejected(self):
        cluster = self.start()
        cluster.open("r1")
        with self.assertRaises(Exception) as caught:
            cluster.seal("r1", ["a", "a"])
        self.assertEqual(caught.exception.path, "$.members[1]")
        self.assertEqual(cluster.status("r1")["state"], "collecting")

    def test_unknown_member_is_rejected(self):
        cluster = self.start()
        cluster.open("r1")
        with self.assertRaises(Exception) as caught:
            cluster.seal("r1", ["a", "z"])
        self.assertEqual(caught.exception.path, "$.members[1]")

    def test_member_without_a_candidate_is_rejected(self):
        cluster = self.start()
        cluster.open("r1")
        # A node that never contributed a candidate cannot be selected.
        cluster.coordinator.db.execute(
            "DELETE FROM ready WHERE round = 1 AND node = 'b'"
        )
        with self.assertRaises(Exception) as caught:
            cluster.seal("r1", ["a", "b"])
        self.assertEqual(caught.exception.path, "$.members[1]")

    def test_empty_members_cancel_without_changing_the_model(self):
        cluster = self.start()
        cluster.open("r1")
        before = cluster.model()
        sealed = cluster.seal("r1", [])
        self.assertEqual(sealed["members"], [])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(cluster.model(), before)
        # No node advanced.
        for ident in ("a", "b", "c"):
            self.assertEqual(cluster.node_state(ident)["cursor"], 0)

    def test_round_after_cancel_is_admitted(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", [])
        second = cluster.open("r2")
        self.assertEqual(second["round"], 2)


class Exclusion(ClusterCase):
    targets = {"a": ["1/2"], "b": ["1/2"], "c": ["1/2"]}

    def test_excluded_node_does_not_advance(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["a", "b"])
        # m' = 0/2 + mean(-1, -1) = -1, w' = 0 - (-1) = 1
        self.assertEqual(cluster.model(), {"w": "1/1", "m": "-1/1"})
        for ident in ("a", "b"):
            state = cluster.node_state(ident)
            self.assertEqual(state["cursor"], 1)
            self.assertEqual(state["residual"], "1/2")
        excluded = cluster.node_state("c")
        self.assertEqual(excluded["cursor"], 0)
        self.assertEqual(excluded["residual"], "0/1")

    def test_excluded_node_contributes_again_once_selected(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["a", "b"])
        cluster.open("r2")
        # Round 2 with w = 1: a and b have residual 1/2 and cursor 1, so
        # g + r = 1; c is still at cursor 0 with residual 0, g = 1/2.
        # Selecting [a, c]: q(a) = 1, q(c) = 1 (half rounded away from zero).
        sealed = cluster.seal("r2", ["a", "c"])
        self.assertEqual(sealed["state"], "done")
        # m2 = m1/2 + mean(1, 1) = -1/2 + 1 = 1/2, w2 = 1 - 1/2 = 1/2
        self.assertEqual(cluster.model(), {"w": "1/2", "m": "1/2"})
        self.assertEqual(cluster.node_state("a")["cursor"], 2)
        self.assertEqual(cluster.node_state("c")["cursor"], 1)
        # b was excluded in round 2 and keeps its round 1 state.
        self.assertEqual(cluster.node_state("b")["cursor"], 1)
        self.assertEqual(cluster.node_state("b")["residual"], "1/2")


class ArithmeticAcrossRounds(ClusterCase):
    targets = {"neg": ["-1/4"], "pos": ["1/4"]}

    def test_positive_half_rounds_away_from_zero(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["pos"])
        # Round 1: g = -1/4, r = 0, so q = 0 and r' = -1/4.
        self.assertEqual(cluster.node_state("pos")["residual"], "-1/4")
        self.assertEqual(cluster.model(), {"w": "0/1", "m": "0/1"})

        cluster.open("r2")
        cluster.seal("r2", ["pos"])
        # Round 2: g + r = -1/2, which is a tie; away from zero gives -1.
        self.assertEqual(cluster.model(), {"w": "1/1", "m": "-1/1"})

    def test_negative_target_flips_the_tie_direction(self):
        cluster = self.start()
        cluster.open("r1")
        cluster.seal("r1", ["neg"])
        self.assertEqual(cluster.node_state("neg")["residual"], "1/4")

        cluster.open("r2")
        cluster.seal("r2", ["neg"])
        # Round 2: g + r = +1/2, away from zero gives +1, and m' = 1.
        self.assertEqual(cluster.model(), {"w": "-1/1", "m": "1/1"})


class ModelExactness(ClusterCase):
    targets = {"a": ["1/3"], "b": ["2/3"], "c": ["1/6"]}

    def test_mean_is_exact_and_normalised(self):
        cluster = self.start()
        cluster.open("r1")
        # Round 1 with w = 0: q(a) = round(-1/3) = 0, q(b) = round(-2/3) = -1,
        # q(c) = round(-1/6) = 0.  mean = -1/3, so m' = -1/3 and w' = 1/3 --
        # a value no float could represent exactly as p/q.
        sealed = cluster.seal("r1", ["a", "b", "c"])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(cluster.model(), {"w": "1/3", "m": "-1/3"})

    def test_half_of_m_is_exact_and_ties_round_away(self):
        cluster = self.start()
        cluster.open("r1")
        # q(a) = 0, q(b) = -1: mean = -1/2, m' = -1/2, w' = 1/2.
        cluster.seal("r1", ["a", "b"])
        self.assertEqual(cluster.model(), {"w": "1/2", "m": "-1/2"})

        cluster.open("r2")
        # Round 2 with w = 1/2: a (cursor 1) gives g + r = 1/6 + -1/3 = -1/6,
        # b (cursor 1) gives g + r = -1/6 + 1/3 = 1/6; both round to q = 0.
        # m2 = m1/2 + mean(0, 0) = -1/4, w2 = 1/2 - (-1/4) = 3/4.
        sealed = cluster.seal("r2", ["a", "b"])
        self.assertEqual(sealed["state"], "done")
        self.assertEqual(cluster.model(), {"w": "3/4", "m": "-1/4"})


if __name__ == "__main__":
    unittest.main()
