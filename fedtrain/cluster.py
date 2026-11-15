"""An in-process cluster: the intended shape of the whole system.

The configuration is exactly what the protocol describes -- a mapping from
every node identifier (a non-empty ASCII alphanumeric string) to that node's
non-empty target array ``y`` (p/q strings).  Each node gets its own database
file under ``root/nodes/<id>.db`` and the coordinator gets
``root/coordinator.db``; nothing is shared between them.
"""

from __future__ import annotations

import os

from .coordinator import Coordinator, CoordinatorServer
from .node import Node, valid_id
from .node_server import NodeServer
from .transport import post_json

__all__ = ["Cluster"]


class Cluster:
    def __init__(self, targets, root, fetch=None):
        """``targets`` maps each identifier to its target array."""
        if not isinstance(targets, dict) or not targets:
            raise ValueError("the configuration must map identifiers to target arrays")
        for ident, y in targets.items():
            if not valid_id(ident):
                raise ValueError("invalid node identifier %r" % (ident,))
            if not isinstance(y, (list, tuple)) or not y:
                raise ValueError("the target array for %r must be non-empty" % (ident,))
        self.targets = targets
        self.root = os.path.abspath(root)
        os.makedirs(os.path.join(self.root, "nodes"), exist_ok=True)

        self.nodes = {}
        self.node_servers = {}
        self.coordinator = None
        self.server = None
        try:
            for ident, y in targets.items():
                self.nodes[ident] = Node(
                    ident, list(y), os.path.join(self.root, "nodes", ident + ".db")
                )
                self.node_servers[ident] = NodeServer(self.nodes[ident]).start()

            registry = {
                ident: server.address for ident, server in self.node_servers.items()
            }
            self.coordinator = Coordinator(
                os.path.join(self.root, "coordinator.db"), registry, fetch=fetch
            )
            self.server = CoordinatorServer(self.coordinator).start()
        except Exception:
            # A refused configuration must not leave servers or handles behind.
            self._shutdown()
            raise
        self.fetch = fetch or post_json

    # ------------------------------------------------------------- lifecycle
    @property
    def address(self):
        return self.server.address

    def call(self, path, body):
        return self.fetch(self.address + path, body, timeout=5.0)

    def open(self, ident):
        return self.call("/open", {"id": ident})

    def seal(self, ident, members):
        return self.call("/seal", {"id": ident, "members": members})

    def status(self, ident):
        return self.call("/status", {"id": ident})

    def model(self):
        return self.call("/model", {})

    # --------------------------------------------------------------- helpers
    def node_state(self, ident):
        return self.nodes[ident].snapshot()

    def stop(self):
        self._shutdown()

    def _shutdown(self):
        if self.server is not None:
            self.server.stop()
            self.server = None
        for server in self.node_servers.values():
            server.stop()
        if self.coordinator is not None:
            self.coordinator.close()
            self.coordinator = None
        for node in self.nodes.values():
            node.close()
