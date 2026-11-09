from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from typing import Any

from eventflow import EventBus, EventFilter
from eventflow.engine import EventEngine

from mavflow import (
    MavApplicationPacket,
    MavEndpoint,
    MavRouterStats,
    MavRuntime,
)
from mavflow._compatibility import ExceptionGroup


class FakeConnection:
    def __init__(self) -> None:
        self.errors = EventBus[Exception]()
        self.target_system = 1
        self.target_component = 1


class FakeRouter:
    def __init__(self) -> None:
        self.messages = EventBus[Any](
            history=32,
            routing_key=lambda event: event.message_type,
        )
        self.errors = EventBus[Any]()

    @property
    def stats(self) -> MavRouterStats:
        return MavRouterStats(False, 0, 0, 0, 0, 0, None, None, None)


class FakeClient:
    def __init__(self, calls: list[str]) -> None:
        self.endpoint = MavEndpoint()
        self.connection = FakeConnection()
        self.router = FakeRouter()
        self.events = EventEngine()
        self.events.add("mavlink.messages", self.router.messages)
        self.events.add("mavlink.connection.errors", self.connection.errors)
        self.events.add("mavlink.router.errors", self.router.errors)
        self.is_connected = False
        self.calls = calls
        self.sent: list[Any] = []

    def start(self) -> None:
        self.calls.append("client:start")
        self.is_connected = True

    def stop(self) -> None:
        self.calls.append("client:stop")
        self.is_connected = False

    def send(self, message: Any) -> None:
        self.sent.append(message)

    def send_named(self, message_name: str, **parameters: Any) -> None:
        self.sent.append((message_name, parameters))


class FakePeer:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.packets = EventBus[MavApplicationPacket]()
        self.changed = EventBus[Any]()
        self.errors = EventBus[Exception]()
        self.alive = True
        self.sent: list[tuple[str, Any]] = []

    def start(self) -> None:
        self.calls.append("peer:start")

    def stop(self) -> None:
        self.calls.append("peer:stop")

    def send(self, packet_type: str, payload: Any = None) -> MavApplicationPacket:
        self.sent.append((packet_type, payload))
        return MavApplicationPacket(packet_type, payload or {})

    def request(self, *_args: Any, **_kwargs: Any) -> str:
        return "response"


class FakeDispatcher:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.handled = EventBus[Any]()
        self.unhandled = EventBus[Any]()
        self.errors = EventBus[Exception]()
        self.handlers: dict[str, Any] = {}

    def start(self) -> None:
        self.calls.append("dispatcher:start")

    def stop(self) -> None:
        self.calls.append("dispatcher:stop")

    def register(self, packet_type: str, handler: Any, *, replace: bool = False) -> Any:
        self.handlers[packet_type] = handler
        return EventBus[None]().subscribe(lambda _event: None)


class FailingStopPeer(FakePeer):
    def __init__(self, calls: list[str]) -> None:
        super().__init__(calls)
        self.failures = 1

    def stop(self) -> None:
        super().stop()
        if self.failures:
            self.failures -= 1
            raise TimeoutError("peer still stopping")


class MavRuntimeTests(unittest.TestCase):
    def test_runtime_owns_lifecycle_in_dependency_order(self) -> None:
        calls: list[str] = []
        client = FakeClient(calls)
        peer = FakePeer(calls)
        dispatcher = FakeDispatcher(calls)
        runtime = MavRuntime(
            client=client,  # type: ignore[arg-type]
            peer=peer,  # type: ignore[arg-type]
            dispatcher=dispatcher,  # type: ignore[arg-type]
        )

        runtime.start()
        runtime.stop()

        self.assertEqual(
            calls,
            [
                "client:start",
                "peer:start",
                "dispatcher:start",
                "dispatcher:stop",
                "peer:stop",
                "client:stop",
            ],
        )
        runtime.close()

    def test_simple_message_and_application_facade(self) -> None:
        calls: list[str] = []
        client = FakeClient(calls)
        peer = FakePeer(calls)
        dispatcher = FakeDispatcher(calls)
        runtime = MavRuntime(
            client=client,  # type: ignore[arg-type]
            peer=peer,  # type: ignore[arg-type]
            dispatcher=dispatcher,  # type: ignore[arg-type]
        )
        received: list[Any] = []
        runtime.messages.once(received.append, routing_key="HEARTBEAT")

        first = SimpleNamespace(message_type="HEARTBEAT", message="first")
        second = SimpleNamespace(message_type="HEARTBEAT", message="second")
        latest = SimpleNamespace(message_type="ATTITUDE", message="latest")
        waited = SimpleNamespace(message_type="GPS_RAW_INT", message="waited")
        client.router.messages.publish(first)
        client.router.messages.publish(second)
        client.router.messages.publish(latest)
        publisher = threading.Timer(0.01, lambda: client.router.messages.publish(waited))
        publisher.start()
        received_wait = runtime.messages.wait_for(routing_key="GPS_RAW_INT", timeout=1.0)
        publisher.join(timeout=1.0)
        packet = runtime.notify("mission.status", {"ready": True})
        runtime.handle("mission.start", lambda _packet: None)

        self.assertEqual(received, [first])
        self.assertEqual(packet.packet_type, "mission.status")
        self.assertIn("mission.start", dispatcher.handlers)
        self.assertEqual(runtime.request("mission.start"), "response")
        self.assertEqual(received_wait, waited)
        history = runtime.messages.history
        self.assertIsNotNone(history)
        latest_event = history.latest(
            EventFilter(predicate=lambda event: event.message_type == "ATTITUDE")
        )
        self.assertEqual(latest_event, latest)
        runtime.close()

    def test_runtime_exposes_the_native_message_bus_and_engine(self) -> None:
        calls: list[str] = []
        client = FakeClient(calls)
        runtime = MavRuntime(client=client)  # type: ignore[arg-type]
        self.assertIs(runtime.messages, client.router.messages)
        self.assertIs(runtime.events, client.events)
        self.assertIs(runtime.events.channel("mavlink.messages"), runtime.messages)
        self.assertFalse(hasattr(runtime, "errors"))
        runtime.close()

    def test_component_errors_remain_on_named_eventflow_channels(self) -> None:
        calls: list[str] = []
        client = FakeClient(calls)
        runtime = MavRuntime(client=client)  # type: ignore[arg-type]
        errors: list[Any] = []
        runtime.events.channel("mavlink.connection.errors").subscribe(errors.append)

        error = RuntimeError("transport failed")
        client.connection.errors.publish(error)
        runtime.start()
        state = runtime.state
        runtime.stop()

        self.assertIs(errors[0], error)
        self.assertTrue(state.running)
        self.assertTrue(state.connected)
        self.assertFalse(state.application_enabled)
        runtime.close()

    def test_application_role_builds_default_stack_without_connecting(self) -> None:
        runtime = MavRuntime(
            MavEndpoint.udp("127.0.0.1", 14550),
            application_role="vehicle",
        )

        self.assertTrue(runtime.application_enabled)
        self.assertIsNotNone(runtime.channel)
        self.assertIsNotNone(runtime.peer)
        self.assertIsNotNone(runtime.dispatcher)
        runtime.close()

    def test_failed_shutdown_is_reported_and_can_be_retried(self) -> None:
        calls: list[str] = []
        client = FakeClient(calls)
        peer = FailingStopPeer(calls)
        dispatcher = FakeDispatcher(calls)
        runtime = MavRuntime(
            client=client,  # type: ignore[arg-type]
            peer=peer,  # type: ignore[arg-type]
            dispatcher=dispatcher,  # type: ignore[arg-type]
        )
        runtime.start()

        with self.assertRaises(ExceptionGroup):
            runtime.stop()
        self.assertIn("client:stop", calls)

        runtime.close()
        self.assertFalse(runtime.events.running)
        self.assertEqual(calls.count("peer:stop"), 2)
