"""A training node: a private target array, a private cursor and residual.

Each node owns its own SQLite database::

    config   -- the immutable identifier and target array
    state    -- ``cursor`` (a non-negative integer) and ``resid`` (a p/q value)
    pending  -- the candidate computed for a round, keyed by round number
    resolved -- the decision this node has applied for a round
    revoked  -- rounds cancelled before this node could be advanced

The node never sees the global model.  It answers ``compute`` with the
rounded candidate ``q`` for a round, applies ``decide`` messages and records
``revoke`` messages so that a cancellation arriving before a computation
blocks that computation when it finally shows up.

All three operations are idempotent: a repeated ``compute`` returns the value
already persisted for that round, a repeated ``decide`` reports the outcome
already applied, and a repeated ``revoke`` changes nothing.  That is what lets
the coordinator retry after a lost reply.
"""

from __future__ import annotations

import json
import threading

from . import rational
from .errors import ProtocolError
from .store import atomic, connect, execute_all

__all__ = ["Node", "valid_id", "is_round"]

_ID_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)


def valid_id(value):
    """Identifiers are non-empty ASCII alphanumeric strings."""
    return isinstance(value, str) and value != "" and all(c in _ID_CHARS for c in value)


def is_round(value):
    """Round numbers are positive integers; booleans do not count."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


class Node:
    def __init__(self, ident, targets, db_path):
        if not valid_id(ident):
            raise ValueError("node identifier must be a non-empty ASCII alphanumeric string")
        if not isinstance(targets, (list, tuple)) or not targets:
            raise ValueError("the target array must be a non-empty array")
        self.ident = ident
        self.targets = [rational.parse(t) for t in targets]
        self.db = connect(db_path)
        self.lock = threading.RLock()
        self._init_schema()
        self._seed()

    # --------------------------------------------------------------- storage
    def _init_schema(self):
        execute_all(
            self.db,
            [
                "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS pending (round INTEGER PRIMARY KEY, "
                "w TEXT NOT NULL, q TEXT NOT NULL, resid TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS resolved (round INTEGER PRIMARY KEY, "
                "included INTEGER NOT NULL)",
                "CREATE TABLE IF NOT EXISTS revoked (round INTEGER PRIMARY KEY)",
            ],
        )

    def _seed(self):
        """Write the immutable configuration once; a later start must agree."""
        wanted = [rational.fmt(v) for v in self.targets]
        with atomic(self.db):
            row = self.db.execute(
                "SELECT value FROM config WHERE key = 'targets'"
            ).fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO config(key, value) VALUES('id', ?)", (self.ident,)
                )
                self.db.execute(
                    "INSERT INTO config(key, value) VALUES('targets', ?)",
                    (json.dumps(wanted),),
                )
            else:
                stored = json.loads(row[0])
                ident = self.db.execute(
                    "SELECT value FROM config WHERE key = 'id'"
                ).fetchone()[0]
                if stored != wanted:
                    raise RuntimeError("target array disagrees with stored state")
                if ident != self.ident:
                    raise RuntimeError("identifier disagrees with stored state")
            self.db.execute(
                "INSERT OR IGNORE INTO state(key, value) VALUES('cursor', '0')"
            )
            self.db.execute(
                "INSERT OR IGNORE INTO state(key, value) VALUES('resid', '0/1')"
            )

    def _state(self, key):
        return self.db.execute(
            "SELECT value FROM state WHERE key = ?", (key,)
        ).fetchone()[0]

    # ------------------------------------------------------------- accessors
    def cursor(self):
        return int(self._state("cursor"))

    def residual(self):
        return self._state("resid")

    def snapshot(self):
        return {
            "id": self.ident,
            "cursor": self.cursor(),
            "residual": self.residual(),
            "targets": [rational.fmt(v) for v in self.targets],
        }

    def pending_rounds(self):
        return [
            r[0]
            for r in self.db.execute("SELECT round FROM pending ORDER BY round")
        ]

    # --------------------------------------------------------------- compute
    def compute(self, rnd, w):
        """Compute and persist the candidate for round ``rnd`` under model ``w``.

        Returns ``{"round", "q", "residual"}``.  ``q`` is the integer the
        coordinator aggregates; ``residual`` is ``g + r - q``.
        """
        if not is_round(rnd):
            raise ProtocolError("$.round", "round must be a positive integer")
        try:
            wv = rational.parse(w)
        except rational.BadValue:
            raise ProtocolError("$.w", "w must be a p/q string")

        with self.lock, atomic(self.db):
            if self.db.execute(
                "SELECT 1 FROM revoked WHERE round = ?", (rnd,)
            ).fetchone():
                raise ProtocolError("$.round", "the round was revoked")
            if self.db.execute(
                "SELECT 1 FROM resolved WHERE round = ?", (rnd,)
            ).fetchone():
                raise ProtocolError("$.round", "the round is already decided")

            row = self.db.execute(
                "SELECT w, q, resid FROM pending WHERE round = ?", (rnd,)
            ).fetchone()
            if row is not None:
                if row[0] != rational.fmt(wv):
                    raise ProtocolError("$.w", "round parameter mismatch")
                return {"round": rnd, "q": row[1], "residual": row[2]}

            y = self.targets[self.cursor() % len(self.targets)]
            g = rational.add(wv, (-y[0], y[1]))          # g = w - y[c mod len]
            exact = rational.add(g, rational.parse(self._state("resid")))  # g + r
            q = rational.round_half_away_from_zero(exact)
            resid = rational.add(exact, (-q, 1))         # g + r - q
            self.db.execute(
                "INSERT INTO pending(round, w, q, resid) VALUES(?, ?, ?, ?)",
                (rnd, rational.fmt(wv), rational.fmt((q, 1)), rational.fmt(resid)),
            )
            return {"round": rnd, "q": rational.fmt((q, 1)), "residual": rational.fmt(resid)}

    # ---------------------------------------------------------------- decide
    def decide(self, rnd, included):
        """Apply the coordinator's decision for round ``rnd``.

        Included rounds advance ``cursor`` and replace ``residual`` atomically;
        excluded rounds only discard the candidate.  Replays are no-ops.
        """
        if not is_round(rnd):
            raise ProtocolError("$.round", "round must be a positive integer")
        if not isinstance(included, bool):
            raise ProtocolError("$.included", "included must be a boolean")

        with self.lock, atomic(self.db):
            if self.db.execute(
                "SELECT 1 FROM revoked WHERE round = ?", (rnd,)
            ).fetchone():
                return {"round": rnd, "included": included, "applied": False,
                        "reason": "revoked"}
            row = self.db.execute(
                "SELECT included FROM resolved WHERE round = ?", (rnd,)
            ).fetchone()
            if row is not None:
                return {"round": rnd, "included": bool(row[0]), "applied": False,
                        "reason": "replay"}
            if included:
                cand = self.db.execute(
                    "SELECT resid FROM pending WHERE round = ?", (rnd,)
                ).fetchone()
                if cand is None:
                    raise ProtocolError("$.round", "no candidate for this round")
                self.db.execute(
                    "UPDATE state SET value = CAST(value AS INTEGER) + 1 WHERE key = 'cursor'"
                )
                self.db.execute(
                    "UPDATE state SET value = ? WHERE key = 'resid'", (cand[0],)
                )
            self.db.execute("DELETE FROM pending WHERE round = ?", (rnd,))
            self.db.execute(
                "INSERT INTO resolved(round, included) VALUES(?, ?)",
                (rnd, 1 if included else 0),
            )
            return {"round": rnd, "included": included, "applied": True}

    # ---------------------------------------------------------------- revoke
    def revoke(self, rnd):
        """Record a cancellation; a later computation for the round is blocked."""
        if not is_round(rnd):
            raise ProtocolError("$.round", "round must be a positive integer")
        with self.lock, atomic(self.db):
            row = self.db.execute(
                "SELECT included FROM resolved WHERE round = ?", (rnd,)
            ).fetchone()
            if row is not None:
                return {"round": rnd, "revoked": True, "applied": False,
                        "reason": "already resolved"}
            exists = self.db.execute(
                "SELECT 1 FROM revoked WHERE round = ?", (rnd,)
            ).fetchone()
            self.db.execute("DELETE FROM pending WHERE round = ?", (rnd,))
            self.db.execute("INSERT OR IGNORE INTO revoked(round) VALUES(?)", (rnd,))
            return {"round": rnd, "revoked": True, "applied": exists is None}

    def close(self):
        self.db.close()
