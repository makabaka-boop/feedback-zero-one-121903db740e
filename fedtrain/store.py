"""SQLite persistence helpers shared by the coordinator and the nodes.

Each process owns its own database file.  Durability settings are chosen so
that a hard kill (``SIGKILL``) immediately after a commit cannot lose that
commit: WAL journaling, ``synchronous=FULL``, and every state transition is a
single explicit transaction.  Connections are shared between HTTP handler
threads, so callers must serialise access with their own lock.
"""

from __future__ import annotations

import os
import sqlite3

__all__ = ["connect", "atomic", "execute_all"]


def connect(path):
    """Open (creating if needed) a private SQLite database in durable mode."""
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    db = sqlite3.connect(
        path, timeout=30.0, isolation_level=None, check_same_thread=False
    )
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=30000")
    db.row_factory = sqlite3.Row
    return db


class atomic:
    """Context manager running a ``BEGIN IMMEDIATE`` ... ``COMMIT`` transaction."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.execute("BEGIN IMMEDIATE")
        return self.db

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.db.execute("COMMIT")
        else:
            try:
                self.db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        return False


def execute_all(db, statements):
    """Run DDL statements as one transaction.

    ``sqlite3.Connection.executescript`` issues an implicit COMMIT first, which
    would break a surrounding explicit transaction, so the statements are run
    one by one instead.
    """
    with atomic(db):
        for statement in statements:
            db.execute(statement)
