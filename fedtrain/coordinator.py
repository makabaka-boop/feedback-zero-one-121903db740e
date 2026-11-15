"""The coordinator: admission, candidate collection, sealing and decisions.

Persistent state lives in its own SQLite file::

    config  -- the global model ``w`` and ``m`` as p/q strings
    nodes   -- the identifier -> base URL registry (written once; URLs may be
               refreshed on a restart, identifiers may not change)
    rounds  -- one row per round: name, state, frozen members, the model the
               round started from and the model it published
    ready   -- one row per (round, node) candidate the coordinator has stored
    ack     -- one row per (round, node) confirmation of the round's decision
    revoked -- one row per (round, node) notified that the round was cancelled

A round opens under a client supplied identifier, collects candidates, is
sealed with a duplicate-free subset of the *ready* nodes, publishes the new
model and the member list in a single transaction, then asks every node to
apply its decision.  The round becomes ``done`` only once every registered
node has confirmed.  A cancelled round (empty member list) instead tells the
nodes to revoke, which blocks any candidate computation that arrives late.

Every operation is idempotent, so a client can retry a request whose reply was
lost, a coordinator can retry a node call whose reply was lost, and a restarted
process resumes the round it was in.
"""

from __future__ import annotations

import json
import threading

from . import rational
from .errors import ProtocolError, StartupError
from .node import valid_id
from .store import atomic, connect, execute_all
from .transport import post_json

__all__ = [
    "Coordinator",
    "CoordinatorServer",
    "COLLECTING",
    "SETTLING",
    "DONE",
]

COLLECTING = "collecting"
SETTLING = "settling"
DONE = "done"


class Coordinator:
    def __init__(self, db_path, nodes, fetch=None, timeout=5.0):
        """``nodes`` maps every identifier to that node's HTTP base URL.

        The registry is written on the first start.  On later starts the set of
        identifiers must match exactly; the URLs may differ (a restarted node
        may listen on another port).
        """
        if not isinstance(nodes, dict) or not nodes:
            raise ValueError("the node registry must be a non-empty mapping")
        registry = {}
        for ident, url in nodes.items():
            if not valid_id(ident):
                raise ValueError("invalid node identifier %r" % (ident,))
            if not isinstance(url, str) or not url:
                raise ValueError("invalid base URL for %r" % (ident,))
            registry[ident] = url.rstrip("/")

        self.db = connect(db_path)
        self.lock = threading.RLock()
        self._fetch = fetch or post_json
        self._timeout = timeout
        self._init_schema()
        self._seed(registry)

    # --------------------------------------------------------------- storage
    def _init_schema(self):
        execute_all(
            self.db,
            [
                "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, "
                "value TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, "
                "url TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS rounds (round INTEGER PRIMARY KEY, "
                "name TEXT NOT NULL UNIQUE, state TEXT NOT NULL, members TEXT, "
                "w0 TEXT NOT NULL, m0 TEXT NOT NULL, w1 TEXT, m1 TEXT)",
                "CREATE TABLE IF NOT EXISTS ready (round INTEGER NOT NULL, "
                "node TEXT NOT NULL, q TEXT NOT NULL, PRIMARY KEY (round, node))",
                "CREATE TABLE IF NOT EXISTS ack (round INTEGER NOT NULL, "
                "node TEXT NOT NULL, PRIMARY KEY (round, node))",
                "CREATE TABLE IF NOT EXISTS revoked (round INTEGER NOT NULL, "
                "node TEXT NOT NULL, PRIMARY KEY (round, node))",
            ],
        )

    def _seed(self, registry):
        with atomic(self.db):
            stored = {
                row["id"]: row["url"]
                for row in self.db.execute("SELECT id, url FROM nodes")
            }
            if not stored:
                for ident, url in registry.items():
                    self.db.execute(
                        "INSERT INTO nodes(id, url) VALUES(?, ?)", (ident, url)
                    )
            else:
                if set(stored) != set(registry):
                    raise StartupError(
                        "the node identifiers disagree with stored state"
                    )
                for ident, url in registry.items():
                    if stored[ident] != url:
                        self.db.execute(
                            "UPDATE nodes SET url = ? WHERE id = ?", (url, ident)
                        )
            for key, value in (("w", "0/1"), ("m", "0/1")):
                self.db.execute(
                    "INSERT OR IGNORE INTO config(key, value) VALUES(?, ?)",
                    (key, value),
                )

    def _node_ids(self):
        return [
            row["id"] for row in self.db.execute("SELECT id FROM nodes ORDER BY id")
        ]

    def _url(self, ident):
        return self.db.execute(
            "SELECT url FROM nodes WHERE id = ?", (ident,)
        ).fetchone()["url"]

    def _cfg(self, key):
        return self.db.execute(
            "SELECT value FROM config WHERE key = ?", (key,)
        ).fetchone()["value"]

    def close(self):
        self.db.close()

    # ---------------------------------------------------------------- queries
    def model(self):
        """The complete global model, ``{"w": "p/q", "m": "p/q"}``."""
        with self.lock:
            return {"w": self._cfg("w"), "m": self._cfg("m")}

    def status(self, name):
        """The status object of the round opened under ``name``."""
        with self.lock:
            rnd = self._round_of(name)
            self._advance(rnd)
            return self._status(rnd)

    def _round_of(self, name):
        if not valid_id(name):
            raise ProtocolError(
                "$.id", "identifiers are non-empty ASCII alphanumeric strings"
            )
        row = self.db.execute(
            "SELECT round FROM rounds WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise ProtocolError("$.id", "unknown round identifier")
        return row["round"]

    def _status(self, rnd):
        row = self.db.execute("SELECT * FROM rounds WHERE round = ?", (rnd,)).fetchone()
        ready = sorted(
            r["node"]
            for r in self.db.execute(
                "SELECT node FROM ready WHERE round = ?", (rnd,)
            )
        )
        members = None if row["members"] is None else sorted(json.loads(row["members"]))
        return {
            "round": row["round"],
            "state": row["state"],
            "ready": ready,
            "members": members,
        }

    # ------------------------------------------------------------------ open
    def open(self, name):
        """Open (or resume) the round identified by ``name`` and collect candidates."""
        with self.lock:
            if not valid_id(name):
                raise ProtocolError(
                    "$.id", "identifiers are non-empty ASCII alphanumeric strings"
                )
            existing = self.db.execute(
                "SELECT round FROM rounds WHERE name = ?", (name,)
            ).fetchone()
            if existing is not None:
                rnd = existing["round"]
                self._advance(rnd)
                return self._status(rnd)

            latest = self.db.execute(
                "SELECT round, state FROM rounds ORDER BY round DESC LIMIT 1"
            ).fetchone()
            if latest is not None and latest["state"] != DONE:
                raise ProtocolError(
                    "$.id", "a new round is admitted only after the previous one is done"
                )
            rnd = 1 if latest is None else latest["round"] + 1
            with atomic(self.db):
                self.db.execute(
                    "INSERT INTO rounds(round, name, state, members, w0, m0) "
                    "VALUES(?, ?, 'collecting', NULL, ?, ?)",
                    (rnd, name, self._cfg("w"), self._cfg("m")),
                )
            self._collect(rnd)
            return self._status(rnd)

    # -------------------------------------------------------------- progress
    def _advance(self, rnd):
        row = self.db.execute("SELECT * FROM rounds WHERE round = ?", (rnd,)).fetchone()
        if row["state"] == COLLECTING:
            self._collect(rnd)
        elif row["state"] == SETTLING:
            if json.loads(row["members"]):
                self._push_decisions(rnd)
            else:
                self._push_revocations(rnd)

    def _collect(self, rnd):
        """Ask every node for its candidate for ``rnd``; persist what arrives."""
        w = self.db.execute(
            "SELECT w0 FROM rounds WHERE round = ?", (rnd,)
        ).fetchone()["w0"]
        for ident in self._node_ids():
            try:
                reply = self._ask(ident, "/compute", {"round": rnd, "w": w})
            except (ProtocolError, OSError, ValueError):
                continue
            if not isinstance(reply, dict) or not isinstance(reply.get("q"), str):
                continue
            try:
                q = rational.parse(reply["q"])
            except rational.BadValue:
                continue
            if q[1] != 1:
                # A candidate must be an integer; anything else is unusable.
                continue
            with atomic(self.db):
                self.db.execute(
                    "INSERT OR IGNORE INTO ready(round, node, q) VALUES(?, ?, ?)",
                    (rnd, ident, rational.fmt(q)),
                )

    def _push_decisions(self, rnd):
        row = self.db.execute("SELECT * FROM rounds WHERE round = ?", (rnd,)).fetchone()
        if row["state"] != SETTLING:
            return
        members = set(json.loads(row["members"]))
        for ident in self._node_ids():
            if self.db.execute(
                "SELECT 1 FROM ack WHERE round = ? AND node = ?", (rnd, ident)
            ).fetchone() is not None:
                continue
            try:
                self._ask(
                    ident, "/decide", {"round": rnd, "included": ident in members}
                )
            except (ProtocolError, OSError, ValueError):
                continue
            with atomic(self.db):
                self.db.execute(
                    "INSERT OR IGNORE INTO ack(round, node) VALUES(?, ?)", (rnd, ident)
                )
        self._finish_if_confirmed(rnd)

    def _push_revocations(self, rnd):
        row = self.db.execute("SELECT * FROM rounds WHERE round = ?", (rnd,)).fetchone()
        if row["state"] != SETTLING:
            return
        for ident in self._node_ids():
            if self.db.execute(
                "SELECT 1 FROM revoked WHERE round = ? AND node = ?", (rnd, ident)
            ).fetchone() is not None:
                continue
            try:
                self._ask(ident, "/revoke", {"round": rnd})
            except (ProtocolError, OSError, ValueError):
                continue
            with atomic(self.db):
                self.db.execute(
                    "INSERT OR IGNORE INTO revoked(round, node) VALUES(?, ?)",
                    (rnd, ident),
                )
        self._finish_if_confirmed(rnd)

    def _finish_if_confirmed(self, rnd):
        """A round is done once every node has confirmed its decision or revocation."""
        with atomic(self.db):
            row = self.db.execute(
                "SELECT state, members FROM rounds WHERE round = ?", (rnd,)
            ).fetchone()
            if row["state"] != SETTLING:
                return
            table = "ack" if json.loads(row["members"]) else "revoked"
            missing = self.db.execute(
                "SELECT COUNT(*) AS n FROM nodes WHERE id NOT IN "
                "(SELECT node FROM %s WHERE round = ?)" % table,
                (rnd,),
            ).fetchone()["n"]
            if missing == 0:
                self.db.execute(
                    "UPDATE rounds SET state = 'done' WHERE round = ?", (rnd,)
                )

    def _ask(self, ident, path, body):
        return self._fetch(self._url(ident) + path, body, timeout=self._timeout)

    # ------------------------------------------------------------------ seal
    def seal(self, name, members):
        """Seal round ``name`` with ``members``; publish the model and decisions."""
        with self.lock:
            rnd = self._round_of(name)
            row = self.db.execute(
                "SELECT * FROM rounds WHERE round = ?", (rnd,)
            ).fetchone()
            chosen = self._validate_members(members)

            if row["state"] != COLLECTING:
                sealed = (
                    None if row["members"] is None else sorted(json.loads(row["members"]))
                )
                if sealed == sorted(chosen):
                    self._advance(rnd)
                    return self._status(rnd)
                raise ProtocolError(
                    "$.members", "the round is already sealed with other members"
                )

            ready = set(
                r["node"]
                for r in self.db.execute(
                    "SELECT node FROM ready WHERE round = ?", (rnd,)
                )
            )
            for index, ident in enumerate(members):
                if ident not in ready:
                    raise ProtocolError(
                        "$.members[%d]" % index,
                        "no candidate has been received from this node",
                    )

            if not members:
                with atomic(self.db):
                    self.db.execute(
                        "UPDATE rounds SET state = 'settling', members = '[]' "
                        "WHERE round = ?",
                        (rnd,),
                    )
                self._push_revocations(rnd)
                return self._status(rnd)

            qs = [
                rational.parse(
                    self.db.execute(
                        "SELECT q FROM ready WHERE round = ? AND node = ?",
                        (rnd, ident),
                    ).fetchone()["q"]
                )
                for ident in members
            ]
            m1 = rational.add(rational.half(rational.parse(row["m0"])), rational.mean(qs))
            w1 = rational.add(rational.parse(row["w0"]), (-m1[0], m1[1]))

            with atomic(self.db):
                self.db.execute(
                    "UPDATE rounds SET state = 'settling', members = ?, w1 = ?, "
                    "m1 = ? WHERE round = ?",
                    (
                        json.dumps(sorted(members)),
                        rational.fmt(w1),
                        rational.fmt(m1),
                        rnd,
                    ),
                )
                self.db.execute(
                    "UPDATE config SET value = ? WHERE key = 'w'", (rational.fmt(w1),)
                )
                self.db.execute(
                    "UPDATE config SET value = ? WHERE key = 'm'", (rational.fmt(m1),)
                )
            self._push_decisions(rnd)
            return self._status(rnd)

    def _validate_members(self, members):
        if not isinstance(members, list):
            raise ProtocolError("$.members", "members must be an array")
        seen = set()
        for index, ident in enumerate(members):
            if not valid_id(ident):
                raise ProtocolError(
                    "$.members[%d]" % index, "invalid node identifier"
                )
            if ident in seen:
                raise ProtocolError(
                    "$.members[%d]" % index, "duplicate node identifier"
                )
            seen.add(ident)
        known = set(self._node_ids())
        for index, ident in enumerate(members):
            if ident not in known:
                raise ProtocolError("$.members[%d]" % index, "unknown node")
        return list(members)


class CoordinatorServer:
    """The coordinator's HTTP face: ``open``, ``seal``, ``status`` and ``model``."""

    def __init__(self, coordinator, port=0, host="127.0.0.1"):
        from .transport import JsonServer

        self.coordinator = coordinator
        self.server = JsonServer(self._dispatch, port=port, host=host)

    def handle(self, path, body):
        """Dispatch one request; raises :class:`ProtocolError` on bad input."""
        if not isinstance(body, dict):
            raise ProtocolError("$", "the body must be a JSON object")

        if path == "/model":
            return self.coordinator.model()

        if path == "/status":
            if "id" not in body:
                raise ProtocolError("$.id", "missing field")
            return self.coordinator.status(body["id"])

        if path == "/open":
            if "id" not in body:
                raise ProtocolError("$.id", "missing field")
            return self.coordinator.open(body["id"])

        if path == "/seal":
            if "id" not in body:
                raise ProtocolError("$.id", "missing field")
            if "members" not in body:
                raise ProtocolError("$.members", "missing field")
            return self.coordinator.seal(body["id"], body["members"])

        raise ProtocolError("$", "unknown endpoint")

    def _dispatch(self, path, body):
        return self.handle(path, body)

    @property
    def address(self):
        return self.server.address

    def start(self):
        self.server.start()
        return self

    def stop(self):
        self.server.stop()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
