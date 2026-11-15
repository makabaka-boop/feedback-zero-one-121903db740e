"""The single error type shared by coordinator and nodes.

A rejected request is answered with ``{"error": <JSONPath>}`` where the
JSONPath points at the offending member of the request body, measured from
the root ``$``.  Array indices are zero based.

``ProtocolError`` is raised wherever a request is validated.  ``RemoteError``
is what a caller sees when that rejection travelled over HTTP; it carries the
same ``path`` so callers can treat both uniformly.
"""

from __future__ import annotations

__all__ = ["ProtocolError", "RemoteError", "StartupError"]


class ProtocolError(ValueError):
    """A request violated the protocol; ``path`` is the JSONPath to blame."""

    def __init__(self, path, detail=""):
        self.path = path
        self.detail = detail
        super().__init__("%s: %s" % (path, detail) if detail else path)

    def body(self):
        return {"error": self.path}


class RemoteError(ProtocolError):
    """A rejection that arrived over HTTP; the path is unchanged."""

    def __init__(self, path, detail="", status=None):
        self.status = status
        super().__init__(path, detail)


class StartupError(RuntimeError):
    """Persistent state disagrees with the configuration the process was given."""
