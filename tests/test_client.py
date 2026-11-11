from __future__ import annotations

import threading
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

    def test_endpoint_configuration_waits_for_client_start_transition(self) -> None:
        start_entered = threading.Event()
        release_start = threading.Event()
        configure_started = threading.Event()
        configure_finished = threading.Event()
        errors: list[Exception] = []

        class BlockingRouter(_Router):
            def start(self) -> None:
                self.calls.append("start")
                start_entered.set()
                if not release_start.wait(1.0):
                    raise TimeoutError("test did not release router startup")
                self.running = True

        connection = _Connection(self.endpoint)
        router = BlockingRouter(connection)
        client = MavClient(
            connection=connection,  # type: ignore[arg-type]
            router=router,  # type: ignore[arg-type]
        )
        next_endpoint = MavEndpoint.tcp("localhost", 5760)
        starter = threading.Thread(target=client.start)

        def configure() -> None:
            configure_started.set()
            try:
                client.configure_endpoint(next_endpoint)
            except Exception as exc:
                errors.append(exc)
            finally:
                configure_finished.set()

        starter.start()
        self.assertTrue(start_entered.wait(1.0))
        configurer = threading.Thread(target=configure)
        configurer.start()
        self.assertTrue(configure_started.wait(1.0))
        configuration_was_serialized = not configure_finished.wait(0.05)
        release_start.set()
        starter.join(1.0)
        configurer.join(1.0)

        self.assertTrue(configuration_was_serialized)
        self.assertFalse(starter.is_alive())
        self.assertFalse(configurer.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIs(client.endpoint, self.endpoint)
        client.stop()

    def test_lifecycle_calls_from_router_thread_fail_before_client_lock(self) -> None:
        operations = (
            ("start", lambda: self.client.start()),
            ("stop", lambda: self.client.stop()),
            (
                "configure_endpoint",
                lambda: self.client.configure_endpoint(MavEndpoint.tcp("localhost", 5760)),
            ),
        )
        for operation, call in operations:
            with self.subTest(operation=operation):
                completed = threading.Event()
                errors: list[Exception] = []

                def invoke_from_router() -> None:
                    self.router._thread = threading.current_thread()
                    try:
                        call()
                    except Exception as exc:
                        errors.append(exc)
                    finally:
                        completed.set()

                self.client._lifecycle_lock.acquire()
                thread = threading.Thread(target=invoke_from_router, daemon=True)
                try:
                    thread.start()
                    completed_before_unlock = completed.wait(0.1)
                finally:
                    self.client._lifecycle_lock.release()
                thread.join(1.0)
                self.router._thread = None

                self.assertTrue(completed_before_unlock)
                self.assertFalse(thread.is_alive())
                self.assertEqual(len(errors), 1)
                self.assertRegex(str(errors[0]), "router receive thread")
                self.router.running = False
                self.connection.endpoint = self.endpoint


if __name__ == "__main__":
    unittest.main()
