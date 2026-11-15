"""The public façade: one federation, the four protocol operations.

``init`` builds the federation once from a mapping of node identifier to that
node's non-empty target array; ``open``, ``seal``, ``status`` and ``model``
then expose the protocol.  A :class:`Service` instance can be used directly
when several federations are needed (the tests do this); the module level
functions operate on the single federation created by :func:`init`.

Nodes are real HTTP servers listening on the loopback interface, and every
component persists itself in its own SQLite file under the federation root::

    <root>/coordinator.db
    <root>/nodes/<id>.db

so a restart of any single component resumes where it stopped.
"""

from __future__ import annotations

import os
import tempfile

from .cluster import Cluster
from .node import valid_id

__all__ = ["Service", "init", "open", "seal", "status", "model", "close", "reset"]


class Service:
    """A federation of HTTP nodes plus its coordinator."""

    def __init__(self, targets, root=None, fetch=None):
        if not isinstance(targets, dict) or not targets:
            raise ValueError("the configuration must map identifiers to target arrays")
        self.root = os.path.abspath(root) if root is not None else tempfile.mkdtemp(
            prefix="fedtrain-"
        )
        self.cluster = Cluster(targets, self.root, fetch=fetch)

    # ------------------------------------------------------- protocol surface
    def open(self, round_id):
        """Open (or resume) the round identified by ``round_id``."""
        return self.cluster.open(round_id)

    def seal(self, round_id, members):
        """Seal ``round_id`` with ``members``; publish the model and decisions."""
        return self.cluster.seal(round_id, members)

    def status(self, round_id):
        """Report ``{round, state, ready, members}`` for ``round_id``."""
        return self.cluster.status(round_id)

    def model(self):
        """Report the complete global model ``{w, m}``."""
        return self.cluster.model()

    # --------------------------------------------------------------- helpers
    def close(self):
        self.cluster.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


_state = {"service": None, "targets": None, "root": None}


def init(targets, root=None, fetch=None):
    """Initialise the federation once and return it.

    A repeated call with the same configuration returns the existing service.
    A call with a different configuration is refused: the protocol requires a
    single initialisation.
    """
    if not isinstance(targets, dict) or not targets:
        raise ValueError("the configuration must map identifiers to target arrays")
    normalised = {}
    for ident, y in targets.items():
        if not valid_id(ident):
            raise ValueError("invalid node identifier %r" % (ident,))
        if not isinstance(y, (list, tuple)) or not y:
            raise ValueError("the target array for %r must be non-empty" % (ident,))
        normalised[ident] = list(y)

    if _state["service"] is not None:
        if normalised != _state["targets"] or (
            root is not None and os.path.abspath(root) != _state["root"]
        ):
            raise RuntimeError("the federation is already initialised")
        return _state["service"]

    service = Service(normalised, root=root, fetch=fetch)
    _state.update(
        service=service, targets=normalised, root=service.root
    )
    return service


def _service():
    if _state["service"] is None:
        raise RuntimeError("call init(targets) first")
    return _state["service"]


def open(round_id):  # noqa: A001 - the protocol names this operation "open"
    return _service().open(round_id)


def seal(round_id, members):
    return _service().seal(round_id, members)


def status(round_id):
    return _service().status(round_id)


def model():
    return _service().model()


def close():
    if _state["service"] is not None:
        _state["service"].close()
        _state.update(service=None, targets=None, root=None)


def reset():
    """Drop the current federation (intended for tests)."""
    close()
