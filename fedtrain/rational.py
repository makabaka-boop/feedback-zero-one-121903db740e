"""Exact rational arithmetic on ``p/q`` strings.

A value is a pair of Python ints ``(p, q)`` in lowest terms with ``q > 0``.
``p`` may be negative or zero; ``q`` must be strictly positive.  Every
entry point validates and normalises, so equal values compare equal.

Conventions for errors are decided by the callers: :class:`BadValue` is a
plain :class:`ValueError` and carries no protocol knowledge.
"""

from math import gcd

__all__ = [
    "BadValue",
    "parse",
    "num",
    "fmt",
    "add",
    "total",
    "half",
    "mean",
    "scale",
    "round_half_away_from_zero",
]


class BadValue(ValueError):
    """The text is not a well-formed p/q rational."""


def num(p, q=1):
    """Normalise ``p/q``.  Raises BadValue when ``q`` is zero."""
    if q == 0:
        raise BadValue("denominator must be non-zero")
    if q < 0:
        p, q = -p, -q
    g = gcd(abs(p), q)
    return (p // g, q // g)


def _int_token(tok):
    """ASCII digits, optionally preceded by a minus sign; nothing else.

    ``str.isdigit`` alone would also accept non-ASCII digits such as ``٣``,
    and ``int`` would happily convert them, so the ASCII check is explicit.
    A leading ``+`` is not part of the canonical form and is rejected.
    """
    if tok[:1] == "-":
        tok = tok[1:]
    return tok != "" and tok.isascii() and tok.isdigit()


def parse(text):
    """Parse a ``p/q`` string into a normalised pair.

    Raises BadValue unless the text is exactly ``<integer>/<positive integer>``.
    """
    if not isinstance(text, str):
        raise BadValue("a value must be a string")
    if text.count("/") != 1:
        raise BadValue("a value must contain exactly one '/'")
    ps, qs = text.split("/")
    if not _int_token(ps) or not _int_token(qs):
        raise BadValue("p must be an integer and q a positive integer")
    if qs.startswith("-") or int(qs) <= 0:
        raise BadValue("q must be a positive integer")
    return num(int(ps), int(qs))


def parse_integer_pq(text):
    """Parse a p/q string and require it to denote an integer (useful for tests)."""
    v = parse(text)
    if v[1] != 1:
        raise BadValue("must be an integer")
    return v[0]


def fmt(v):
    return "%d/%d" % (v[0], v[1])


def add(a, b):
    return num(a[0] * b[1] + b[0] * a[1], a[1] * b[1])


def total(values):
    acc = (0, 1)
    for v in values:
        acc = add(acc, v)
    return acc


def half(v):
    return num(v[0], 2 * v[1])


def mean(values):
    """Exact mean of a non-empty sequence."""
    if not values:
        raise ValueError("mean of no values")
    s = total(values)
    return num(s[0], s[1] * len(values))


def scale(v, k):
    return num(v[0] * k, v[1])


def round_half_away_from_zero(v):
    """Round the exact rational ``v`` to an int; exact halves go away from zero."""
    n, d = v
    if n >= 0:
        return (2 * n + d) // (2 * d)
    return -((-2 * n + d) // (2 * d))
