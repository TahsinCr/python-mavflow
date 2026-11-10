from __future__ import annotations

import unittest
from typing import Any

from eventflow import EventBus

from mavflow.client import MavClient
from mavflow.endpoint import MavEndpoint
from mavflow.message import MavMessageEnvelope


class _Connection:
    def __init__(self, endpoint: MavEndpoint) -> None:
        self.endpoint = endpoint
        self.errors = EventBus[Exception]()
        self.is_connected = False
        self.raw = object()
        self.mavlink = object()
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def send(self, message: Any) -> None:
        self.calls.append(("send", (message,), {}))

    def send_named(self, message_name: str, **parameters: Any) -> None:
        self.calls.append(("send_named", (message_name,), parameters))

    def request_message_rate(self, message_id: int, frequency_hz: float, **targets: Any) -> int:
        self.calls.append(("request_message_rate", (message_id, frequency_hz), targets))
        return 25_000

    def call_mav(self, method_name: str, *args: Any, **kwargs: Any) -> str:
        self.calls.append(("call_mav", (method_name, *args), kwargs))
        return "mav-result"

    def call_raw(self, method_name: str, *args: Any, **kwargs: Any) -> str:
        self.calls.append(("call_raw", (method_name, *args), kwargs))
        return "raw-result"


class _Router:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.messages = EventBus[MavMessageEnvelope]()
        self.errors = EventBus[Any]()
        self.running = False
        self.calls: list[str] = []

    def start(self) -> None:
        self.calls.append("start")
        self.running = True

    def stop(self) -> None:
        self.calls.append("stop")
        self.running = False


class MavClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.endpoint = MavEndpoint.udp("127.0.0.1", 14550)
        self.connection = _Connection(self.endpoint)
        self.router = _Router(self.connection)
        self.client = MavClient(
            connection=self.connection,  # type: ignore[arg-type]
            router=self.router,  # type: ignore[arg-type]
        )

    def test_client_exposes_native_components_and_delegates_transport_calls(self) -> None:
        self.assertIs(self.client.endpoint, self.endpoint)
        self.assertIs(self.client.messages, self.router.messages)
        self.assertIs(self.client.raw, self.connection.raw)
        self.assertIs(self.client.mavlink, self.connection.mavlink)
        self.assertFalse(self.client.is_connected)

        self.client.configure_endpoint(MavEndpoint.tcp("localhost", 5760))
        self.assertEqual(self.client.endpoint, MavEndpoint.tcp("localhost", 5760))
        message = object()
        self.client.send(message)
        self.client.send_named("HEARTBEAT", custom_mode=4)
        self.assertEqual(
            self.client.request_message_rate(33, 40.0, target_system=7),
            25_000,
        )
        self.assertEqual(self.client.call_mav("heartbeat_send", 1, custom=2), "mav-result")
        self.assertEqual(self.client.call_raw("set_mode", "AUTO"), "raw-result")
        self.assertEqual(
            [name for name, _, _ in self.connection.calls],
            ["send", "send_named", "request_message_rate", "call_mav", "call_raw"],
        )
        self.assertEqual(self.connection.calls[1][2], {"custom_mode": 4})
        self.assertEqual(
            self.connection.calls[2][2],
            {"target_system": 7, "target_component": None},
        )

    def test_client_rejects_endpoint_and_router_connection_mismatches(self) -> None:
        with self.assertRaises(ValueError):
            MavClient(endpoint=MavEndpoint.tcp("localhost", 5760), connection=self.connection)  # type: ignore[arg-type]

        other_connection = _Connection(MavEndpoint.tcp("localhost", 5760))
        with self.assertRaises(ValueError):
            MavClient(
                connection=self.connection,  # type: ignore[arg-type]
                router=_Router(other_connection),  # type: ignore[arg-type]
            )

    def test_client_context_manages_router_and_endpoint_changes_require_stopped_state(self) -> None:
        with self.client as entered:
            self.assertIs(entered, self.client)
            self.assertTrue(self.router.running)
            with self.assertRaises(RuntimeError):
                self.client.configure_endpoint(self.endpoint)

        self.assertEqual(self.router.calls, ["start", "stop"])
        self.assertFalse(self.router.running)


if __name__ == "__main__":
    unittest.main()
