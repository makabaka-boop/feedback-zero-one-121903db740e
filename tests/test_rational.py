"""Exact rational arithmetic: parsing, normalisation and half-away rounding."""

import unittest

from fedtrain import rational
from fedtrain.rational import BadValue, fmt, parse, round_half_away_from_zero


class ParseTests(unittest.TestCase):
    def test_accepts_canonical_and_reduces(self):
        self.assertEqual(parse("1/4"), (1, 4))
        self.assertEqual(fmt(parse("2/4")), "1/2")
        self.assertEqual(fmt(parse("4/2")), "2/1")
        self.assertEqual(fmt(parse("0/7")), "0/1")
        self.assertEqual(fmt(parse("-6/4")), "-3/2")

    def test_rejects_malformed(self):
        for text in ["", "1", "1/", "/2", "a/b", "1/0", "1/-2", "1/2/3", "1.5/2",
                     "+1/2", " 1/2", "1/2 ", "1 / 2", "1//2", "1/2\n", True, 1, None,
                     "٣/4", "1/٤", "1e3/2", "0x1/2", "1/2\r"]:
            with self.subTest(text=text):
                with self.assertRaises(BadValue):
                    parse(text)


class ArithmeticTests(unittest.TestCase):
    def test_add_half_mean(self):
        self.assertEqual(fmt(rational.add((1, 2), (1, 3))), "5/6")
        self.assertEqual(fmt(rational.half((3, 2))), "3/4")
        self.assertEqual(fmt(rational.mean([(1, 3), (2, 3)])), "1/2")
        self.assertEqual(fmt(rational.mean([(1, 1)])), "1/1")

    def test_round_half_away_from_zero(self):
        cases = {
            (1, 2): 1, (-1, 2): -1, (3, 2): 2, (-3, 2): -2,
            (5, 2): 3, (-5, 2): -3, (1, 3): 0, (-1, 3): 0,
            (2, 3): 1, (-2, 3): -1, (0, 1): 0, (7, 2): 4, (-7, 2): -4,
            (1, 4): 0, (-1, 4): 0, (3, 4): 1, (-3, 4): -1, (2, 1): 2,
        }
        for value, expected in cases.items():
            with self.subTest(value=fmt(value)):
                self.assertEqual(round_half_away_from_zero(value), expected)


if __name__ == "__main__":
    unittest.main()
