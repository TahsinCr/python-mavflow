from __future__ import annotations

import unittest

from eventflow import EventBus

import mavflow
from mavflow import MavRuntime
from mavflow.contracts import MavHeader, MavMessage


class PackageLayoutTests(unittest.TestCase):
    def test_public_package_identity_and_runtime_exports(self) -> None:
        self.assertEqual(mavflow.__version__, "1.0.0")
        self.assertIn("MavRuntime", mavflow.__all__)
        self.assertIs(mavflow.MavRuntime, MavRuntime)

    def test_public_contracts_are_exported_from_contracts_module(self) -> None:
        self.assertIs(mavflow.MavHeader, MavHeader)
        self.assertIs(mavflow.MavMessage, MavMessage)

    def test_runtime_events_use_eventflow_bus(self) -> None:
        runtime = MavRuntime()
        try:
            self.assertIsInstance(runtime.messages, EventBus)
            self.assertIsInstance(runtime.errors, EventBus)
        finally:
            runtime.close()


if __name__ == "__main__":
    unittest.main()
