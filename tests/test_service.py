"""The module-level façade: initialisation happens exactly once."""

import os
import shutil
import tempfile
import unittest

import fedtrain


class Initialisation(unittest.TestCase):
    def setUp(self):
        fedtrain.reset()
        self.addCleanup(fedtrain.reset)
        self.root = tempfile.mkdtemp(prefix="fedtrain-init-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def test_init_returns_a_service_and_the_operations_use_it(self):
        service = fedtrain.init({"a": ["1/4"]}, root=self.root)
        self.assertIsInstance(service, fedtrain.Service)
        self.assertEqual(fedtrain.open("r1")["round"], 1)
        self.assertEqual(fedtrain.seal("r1", ["a"])["state"], "done")
        self.assertEqual(fedtrain.model(), {"w": "0/1", "m": "0/1"})
        self.assertEqual(fedtrain.status("r1")["state"], "done")

    def test_repeated_init_with_the_same_configuration_is_idempotent(self):
        first = fedtrain.init({"a": ["1/4"]}, root=self.root)
        second = fedtrain.init({"a": ["1/4"]}, root=self.root)
        self.assertIs(first, second)
        self.assertEqual(fedtrain.open("r1")["round"], 1)

    def test_repeated_init_with_another_configuration_is_refused(self):
        fedtrain.init({"a": ["1/4"]}, root=self.root)
        with self.assertRaises(RuntimeError):
            fedtrain.init({"b": ["1/4"]}, root=self.root)
        with self.assertRaises(RuntimeError):
            fedtrain.init({"a": ["1/2"]}, root=self.root)
        with self.assertRaises(RuntimeError):
            fedtrain.init({"a": ["1/4"]}, root=os.path.join(self.root, "other"))

    def test_operations_require_init(self):
        with self.assertRaises(RuntimeError):
            fedtrain.open("r1")
        with self.assertRaises(RuntimeError):
            fedtrain.model()

    def test_bad_configuration_is_refused(self):
        for bad in ({}, {"": ["1/4"]}, {"a": []}, {"a b": ["1/4"]}, {"a": None},
                    {"a": "1/4"}, []):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    fedtrain.init(bad, root=self.root)

    def test_each_component_persists_in_its_own_file(self):
        service = fedtrain.init({"a": ["1/4"], "b": ["1/4"]}, root=self.root)
        fedtrain.open("r1")
        fedtrain.seal("r1", ["a"])
        for name in ("coordinator.db", "nodes/a.db", "nodes/b.db"):
            self.assertTrue(os.path.exists(os.path.join(service.root, name)), name)
        # The node databases are genuinely separate files.
        self.assertNotEqual(
            os.stat(os.path.join(service.root, "nodes", "a.db")).st_ino,
            os.stat(os.path.join(service.root, "nodes", "b.db")).st_ino,
        )


if __name__ == "__main__":
    unittest.main()
