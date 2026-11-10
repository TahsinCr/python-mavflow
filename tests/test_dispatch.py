from __future__ import annotations

import threading
import unittest
from typing import Any

from eventflow import EventBus

from mavflow.application import MavApplicationPacket
from mavflow.dispatch import MavApplicationDispatcher, MavApplicationHandlerRegistry


class _DispatchPeer:
    def __init__(self) -> None:
        self.packets = EventBus[MavApplicationPacket]()
        self.sent: list[tuple[str, dict[str, Any], dict[str, int]]] = []
        self.send_error: Exception | None = None
        self.response_sent = threading.Event()

    def send(
        self,
        packet_type: str,
        payload: Any,
        **targets: int,
    ) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((packet_type, dict(payload), targets))
        self.response_sent.set()


class HandlerRegistryTests(unittest.TestCase):
    def test_replacement_and_stale_subscription_cancellation_preserve_current_handler(self) -> None:
        registry = MavApplicationHandlerRegistry()

        def first_handler(_packet: MavApplicationPacket) -> None:
            return None

        def replacement(_packet: MavApplicationPacket) -> None:
            return None

        first = registry.register(" Camera.Capture ", first_handler)

        with self.assertRaises(ValueError):
            registry.register("camera.capture", replacement)
        with self.assertRaises(ValueError):
            registry.register("system.ping", replacement)
        with self.assertRaises(TypeError):
            registry.register("camera.other", object())  # type: ignore[arg-type]

        second = registry.register("CAMERA.CAPTURE", replacement, replace=True)
        first.cancel()
        self.assertIs(registry.resolve(" camera.capture "), replacement)
        second.cancel()
        self.assertIsNone(registry.resolve("camera.capture"))


class DispatcherContractTests(unittest.TestCase):
    def test_dispatch_reports_stopped_and_unhandled_requests(self) -> None:
        peer = _DispatchPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        errors: list[Exception] = []
        unhandled: list[MavApplicationPacket] = []
        dispatcher.errors.subscribe(errors.append)
        dispatcher.unhandled.subscribe(unhandled.append)
        request = MavApplicationPacket(
            "camera.unknown",
            packet_id=50,
            source_system=12,
            source_component=4,
            expects_response=True,
        )

        self.assertFalse(dispatcher.dispatch(request))
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(str(errors[0]), "MAVLink application dispatcher is not running")

        dispatcher.start()
        self.addCleanup(dispatcher.stop)
        self.assertFalse(dispatcher.dispatch(request))

        self.assertEqual(unhandled, [request])
        self.assertEqual(len(peer.sent), 1)
        response_type, payload, targets = peer.sent[0]
        self.assertEqual(response_type, "system.error")
        self.assertEqual(payload["request_id"], 50)
        self.assertEqual(payload["error_type"], "UnsupportedPacket")
        self.assertEqual(targets, {"target_system": 12, "target_component": 4})

    def test_handler_mapping_is_returned_and_correlated_in_response(self) -> None:
        peer = _DispatchPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        handled: list[Any] = []
        completed = threading.Event()
        dispatcher.handled.subscribe(lambda event: (handled.append(event), completed.set()))
        dispatcher.register("camera.status", lambda _packet: {"ready": True})
        dispatcher.start()
        self.addCleanup(dispatcher.stop)
        request = MavApplicationPacket(
            "camera.status",
            packet_id=61,
            source_system=7,
            source_component=2,
            expects_response=True,
        )

        peer.response_sent.clear()
        self.assertTrue(dispatcher.dispatch(request))
        self.assertTrue(completed.wait(1.0))
        self.assertTrue(peer.response_sent.wait(1.0))
        self.assertTrue(handled[0].result.accepted)
        self.assertEqual(handled[0].result.payload, {"ready": True})
        self.assertEqual(peer.sent[0][0], "system.ack")
        self.assertEqual(peer.sent[0][1]["request_id"], 61)
        self.assertEqual(peer.sent[0][1]["ready"], True)
        self.assertEqual(peer.sent[0][2], {"target_system": 7, "target_component": 2})

    def test_handler_exception_and_invalid_result_are_reported_as_failed_responses(self) -> None:
        peer = _DispatchPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        handled: list[Any] = []
        errors: list[Exception] = []
        completed = threading.Event()
        dispatcher.handled.subscribe(lambda event: (handled.append(event), completed.set()))
        dispatcher.errors.subscribe(errors.append)

        def raises(_packet: MavApplicationPacket) -> None:
            raise ValueError("camera unavailable")

        dispatcher.register("camera.failure", raises)
        dispatcher.register("camera.invalid", lambda _packet: 42)  # type: ignore[arg-type,return-value]
        dispatcher.start()
        self.addCleanup(dispatcher.stop)

        for index, packet_type in enumerate(("camera.failure", "camera.invalid"), start=1):
            with self.subTest(packet_type=packet_type):
                completed.clear()
                peer.response_sent.clear()
                self.assertTrue(
                    dispatcher.dispatch(
                        MavApplicationPacket(
                            packet_type, packet_id=70 + index, expects_response=True
                        )
                    )
                )
                self.assertTrue(completed.wait(1.0))
                self.assertTrue(peer.response_sent.wait(1.0))
                result = handled[-1].result
                self.assertFalse(result.accepted)
                self.assertEqual(peer.sent[-1][0], "system.error")
                self.assertEqual(peer.sent[-1][1]["request_id"], 70 + index)

        self.assertEqual(len(errors), 2)
        self.assertIsInstance(errors[0], ValueError)
        self.assertIsInstance(errors[1], TypeError)

    def test_response_send_failures_are_published_without_losing_dispatch_result(self) -> None:
        peer = _DispatchPeer()
        peer.send_error = OSError("transport failed")
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        errors: list[Exception] = []
        dispatcher.errors.subscribe(errors.append)
        dispatcher.start()
        self.addCleanup(dispatcher.stop)

        self.assertFalse(
            dispatcher.dispatch(
                MavApplicationPacket("camera.unknown", packet_id=90, expects_response=True)
            )
        )
        self.assertEqual(errors, [peer.send_error])


if __name__ == "__main__":
    unittest.main()
