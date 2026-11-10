from __future__ import annotations

import threading
import time
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
from mavflow.application import MavApplicationCodec
from mavflow.message import MavMessageEnvelope


class FakeConnection:
    def __init__(self) -> None:
        self.errors = EventBus[Exception]()
        self.target_system = 1
        self.target_component = 1


class FakeRouter:
    def __init__(self) -> None:
        self.messages = EventBus[Any](history=32)
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

    @property
    def messages(self) -> EventBus[Any]:
        return self.router.messages

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
        self.requests: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def start(self) -> None:
        self.calls.append("peer:start")

    def stop(self) -> None:
        self.calls.append("peer:stop")

    def send(self, packet_type: str, payload: Any = None) -> MavApplicationPacket:
        self.sent.append((packet_type, payload))
        return MavApplicationPacket(packet_type, payload or {})

    def request(self, *_args: Any, **_kwargs: Any) -> str:
        self.requests.append((_args, _kwargs))
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

    def test_start_failure_rolls_back_components_in_reverse_order(self) -> None:
        calls: list[str] = []

        class FailingStartDispatcher(FakeDispatcher):
            def start(self) -> None:
                self.calls.append("dispatcher:start")
                raise OSError("dispatcher startup failed")

        runtime = MavRuntime(
            client=FakeClient(calls),  # type: ignore[arg-type]
            peer=FakePeer(calls),  # type: ignore[arg-type]
            dispatcher=FailingStartDispatcher(calls),  # type: ignore[arg-type]
        )

        with self.assertRaisesRegex(OSError, "dispatcher startup failed"):
            runtime.start()

        self.assertEqual(
            calls,
            [
                "client:start",
                "peer:start",
                "dispatcher:start",
                "peer:stop",
                "client:stop",
            ],
        )
        self.assertFalse(runtime.running)
        runtime.close()

    def test_start_and_stop_are_serialized_across_the_full_transition(self) -> None:
        calls: list[str] = []
        entered = threading.Event()
        release = threading.Event()
        stop_started = threading.Event()
        stop_finished = threading.Event()
        failures: list[Exception] = []

        class BlockingClient(FakeClient):
            def start(self) -> None:
                calls.append("client:start:entered")
                entered.set()
                if not release.wait(1.0):
                    raise TimeoutError("test did not release runtime start")
                self.is_connected = True
                calls.append("client:start:complete")

        client = BlockingClient(calls)
        runtime = MavRuntime(client=client)  # type: ignore[arg-type]

        def start() -> None:
            try:
                runtime.start()
            except Exception as exc:
                failures.append(exc)

        def stop() -> None:
            stop_started.set()
            try:
                runtime.stop()
            except Exception as exc:
                failures.append(exc)
            finally:
                stop_finished.set()

        starter = threading.Thread(target=start)
        stopper = threading.Thread(target=stop)
        starter.start()
        self.assertTrue(entered.wait(1.0))
        stopper.start()
        self.assertTrue(stop_started.wait(1.0))
        self.assertFalse(stop_finished.wait(0.05))
        release.set()
        starter.join(1.0)
        stopper.join(1.0)

        self.assertFalse(starter.is_alive())
        self.assertFalse(stopper.is_alive())
        self.assertFalse(failures)
        self.assertLess(calls.index("client:start:complete"), calls.index("client:stop"))
        self.assertFalse(runtime.running)
        runtime.close()

    def test_request_requires_and_forwards_to_application_peer(self) -> None:
        calls: list[str] = []
        peer = FakePeer(calls)
        runtime = MavRuntime(
            client=FakeClient(calls),  # type: ignore[arg-type]
            peer=peer,  # type: ignore[arg-type]
        )

        self.assertEqual(
            runtime.request(
                "camera.capture",
                {"mode": "still"},
                response_types="system.ack",
                timeout=1.5,
            ),
            "response",
        )
        self.assertEqual(
            peer.requests,
            [
                (
                    ("camera.capture", {"mode": "still"}),
                    {"response_types": "system.ack", "timeout": 1.5},
                )
            ],
        )
        runtime.close()

        no_peer_runtime = MavRuntime(client=FakeClient([]))  # type: ignore[arg-type]
        with self.assertRaisesRegex(RuntimeError, "not configured"):
            no_peer_runtime.request("camera.capture")
        no_peer_runtime.close()

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
        runtime.messages.once(
            received.append,
            event_filter=EventFilter(predicate=lambda event: event.message_type == "HEARTBEAT"),
        )

        first = SimpleNamespace(message_type="HEARTBEAT", message="first")
        second = SimpleNamespace(message_type="HEARTBEAT", message="second")
        latest = SimpleNamespace(message_type="ATTITUDE", message="latest")
        waited = SimpleNamespace(message_type="GPS_RAW_INT", message="waited")
        client.router.messages.publish(first)
        client.router.messages.publish(second)
        client.router.messages.publish(latest)
        publisher = threading.Timer(0.01, lambda: client.router.messages.publish(waited))
        publisher.start()
        received_wait = runtime.messages.wait_for(
            event_filter=EventFilter(predicate=lambda event: event.message_type == "GPS_RAW_INT"),
            timeout=1.0,
        )
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
            MavEndpoint.udp(
                "127.0.0.1",
                14550,
                expected_system=7,
                expected_component=3,
            ),
            application_role="vehicle",
        )

        self.assertTrue(runtime.application_enabled)
        self.assertIsNotNone(runtime.channel)
        self.assertIsNotNone(runtime.peer)
        self.assertIsNotNone(runtime.dispatcher)
        assert runtime.peer is not None
        assert runtime.channel is not None
        self.assertEqual(runtime.peer._resolve_target(runtime.peer._target_system), 7)
        self.assertEqual(runtime.peer._resolve_target(runtime.peer._target_component), 3)
        self.assertEqual(runtime.channel._source_systems, frozenset({7}))
        self.assertEqual(runtime.channel._source_components, frozenset({3}))
        runtime.close()

    def test_application_role_accepts_explicit_peer_and_source_identity(self) -> None:
        runtime = MavRuntime(
            client=FakeClient([]),  # type: ignore[arg-type]
            application_role="gcs",
            peer_options={"target_system": 7, "target_component": 3},
            channel_options={"source_systems": (7, 8), "source_components": 3},
        )

        assert runtime.peer is not None
        assert runtime.channel is not None
        self.assertEqual(runtime.peer._resolve_target(runtime.peer._target_system), 7)
        self.assertEqual(runtime.peer._resolve_target(runtime.peer._target_component), 3)
        self.assertEqual(runtime.channel._source_systems, frozenset({7, 8}))
        self.assertEqual(runtime.channel._source_components, frozenset({3}))
        runtime.close()

    def test_dynamic_peer_identity_blocks_unexpected_dispatcher_sources(self) -> None:
        client = FakeClient([])
        client.connection.target_system = 7
        client.connection.target_component = 3
        runtime = MavRuntime(client=client, application_role="gcs")  # type: ignore[arg-type]
        assert runtime.channel is not None
        assert runtime.dispatcher is not None
        self.assertIsNone(runtime.channel._source_systems)
        self.assertIsNone(runtime.channel._source_components)
        handled: list[int] = []
        completed = threading.Event()

        def handle(packet: MavApplicationPacket) -> None:
            handled.append(packet.source_system or 0)
            completed.set()

        runtime.dispatcher.register("camera.capture", handle)
        runtime.start()

        def publish(source_system: int, source_component: int, packet_id: int) -> None:
            fragment = MavApplicationCodec.encode(
                MavApplicationPacket("camera.capture", packet_id=packet_id)
            )[0]
            message = SimpleNamespace(
                payload=list(fragment),
                target_network=runtime.channel.network_id,
                message_type=runtime.channel.message_type,
                target_system=0,
                target_component=0,
            )
            client.router.messages.publish(
                MavMessageEnvelope(
                    sequence=packet_id,
                    message=message,
                    message_type="V2_EXTENSION",
                    source_system=source_system,
                    source_component=source_component,
                    message_id=123,
                    received_monotonic=time.monotonic(),
                )
            )

        try:
            publish(7, 3, 101)
            self.assertTrue(completed.wait(1.0))
            self.assertEqual(handled, [7])
            completed.clear()
            publish(8, 1, 102)
            self.assertFalse(completed.wait(0.05))
            self.assertEqual(handled, [7])
            client.connection.target_system = 0
            client.connection.target_component = 0
            publish(7, 3, 103)
            self.assertFalse(completed.wait(0.05))
            self.assertEqual(handled, [7])
        finally:
            runtime.stop()

    def test_explicit_channel_allowlists_define_dispatch_sources(self) -> None:
        client = FakeClient([])
        client.connection.target_system = 7
        client.connection.target_component = 3
        runtime = MavRuntime(
            client=client,  # type: ignore[arg-type]
            application_role="gcs",
            channel_options={"source_systems": (7, 8), "source_components": (1, 3)},
        )
        assert runtime.peer is not None
        assert runtime.dispatcher is not None
        handled: list[tuple[int | None, int | None]] = []
        completed = threading.Event()

        def handle(packet: MavApplicationPacket) -> None:
            handled.append((packet.source_system, packet.source_component))
            completed.set()

        runtime.dispatcher.register("camera.capture", handle)
        runtime.start()

        runtime.peer.packets.publish(
            MavApplicationPacket(
                "camera.capture",
                source_system=8,
                source_component=1,
            )
        )

        try:
            self.assertTrue(completed.wait(1.0))
            self.assertEqual(handled, [(8, 1)])
        finally:
            runtime.stop()

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
            runtime.close()
        self.assertIn("client:stop", calls)
        self.assertTrue(runtime.events.running)

        runtime.close()
        self.assertFalse(runtime.events.running)
        self.assertEqual(calls.count("peer:stop"), 2)
