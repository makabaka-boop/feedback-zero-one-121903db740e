"""Shared test harness: an isolated cluster on loopback with throwaway databases."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from fedtrain.cluster import Cluster


class ClusterCase(unittest.TestCase):
    targets = None  # subclasses set {"id": ["p/q", ...]}

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="fedtrain-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.cluster = None

    def start(self, targets=None):
        self.cluster = Cluster(targets or self.targets, os.path.join(self.root, "run"))
        self.addCleanup(self._stop)
        return self.cluster

    def _stop(self):
        if self.cluster is not None:
            self.cluster.stop()
            self.cluster = None
