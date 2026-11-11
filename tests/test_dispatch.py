from __future__ import annotations

import threading
import unittest
from typing import Any
from unittest.mock import Mock, patch

from eventflow import EventBus

from mavflow.application import (
    MavApplicationCodec,
    MavApplicationPacket,
    MavApplicationProtocolError,
)
from mavflow.dispatch import MavApplicationDispatcher, MavApplicationHandlerRegistry


class _DispatchPeer:
    def __init__(self) -> None:
        self.packets = EventBus[MavApplicationPacket]()
        self.sent: list[tuple[str, dict[str, Any], dict[str, int]]] = []
        self.send_thread_ids: list[int] = []
        self.send_error: Exception | None = None
        self.response_sent = threading.Event()

    @staticmethod
    def _accepts_dispatch_packet(_packet: MavApplicationPacket) -> bool:
        return True

    def send(
        self,
        packet_type: str,
        payload: Any,
        **targets: int,
    ) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((packet_type, dict(payload), targets))
        self.send_thread_ids.append(threading.get_ident())
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
        self.assertTrue(peer.response_sent.wait(1.0))

        self.assertEqual(unhandled, [request])
        self.assertEqual(len(peer.sent), 1)
        response_type, payload, targets = peer.sent[0]
        self.assertEqual(response_type, "system.error")
        self.assertEqual(payload["request_id"], 50)
        self.assertEqual(payload["error_type"], "UnsupportedPacket")
        self.assertEqual(targets, {"target_system": 12, "target_component": 4})
        self.assertNotEqual(peer.send_thread_ids[0], threading.get_ident())

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
            raise ValueError(
                "Database connection failed: postgresql://user:password@internal-host/db"
            )

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
                self.assertEqual(result.message, "Application handler failed")
                self.assertEqual(peer.sent[-1][0], "system.error")
                self.assertEqual(peer.sent[-1][1]["request_id"], 70 + index)
                self.assertNotIn("postgresql://", repr(peer.sent[-1][1]))

        self.assertEqual(len(errors), 2)
        self.assertIsInstance(errors[0], ValueError)
        self.assertIn("postgresql://", str(errors[0]))
        self.assertIsInstance(errors[1], TypeError)

    def test_oversized_handler_response_is_reported_as_a_failed_result(self) -> None:
        peer = _DispatchPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        handled: list[Any] = []
        errors: list[Exception] = []
        completed = threading.Event()
        dispatcher.handled.subscribe(lambda event: (handled.append(event), completed.set()))
        dispatcher.errors.subscribe(errors.append)
        dispatcher.register("camera.large", lambda _packet: {"data": "x" * 20_000})
        dispatcher.start()
        self.addCleanup(dispatcher.stop)

        self.assertTrue(
            dispatcher.dispatch(
                MavApplicationPacket("camera.large", packet_id=89, expects_response=True)
            )
        )
        self.assertTrue(completed.wait(1.0))
        self.assertTrue(peer.response_sent.wait(1.0))

        self.assertFalse(handled[0].result.accepted)
        self.assertEqual(peer.sent[0][0], "system.error")
        self.assertEqual(peer.sent[0][1]["request_id"], 89)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], MavApplicationProtocolError)

    def test_response_preflight_includes_final_metadata_at_packet_boundary(self) -> None:
        maximum = MavApplicationCodec.max_packet_bytes

        def response_fits(size: int, *, include_metadata: bool) -> bool:
            candidate: dict[str, Any] = {"data": "x" * size}
            if include_metadata:
                candidate.update({"request_id": 89, "accepted": True, "message": ""})
            try:
                MavApplicationCodec.encode(
                    MavApplicationPacket("system.ack", candidate, packet_id=0xFFFFFFFF)
                )
            except MavApplicationProtocolError:
                return False
            return True

        low, high = 0, maximum
        while low < high:
            middle = (low + high + 1) // 2
            if response_fits(middle, include_metadata=True):
                low = middle
            else:
                high = middle - 1
        payload_size = low + 16
        payload = {"data": "x" * payload_size}
        self.assertTrue(response_fits(payload_size, include_metadata=False))
        final_payload = {
            **payload,
            "request_id": 89,
            "accepted": True,
            "message": "",
        }
        self.assertFalse(response_fits(payload_size, include_metadata=True))
        with self.assertRaises(MavApplicationProtocolError):
            MavApplicationCodec.encode(
                MavApplicationPacket("system.ack", final_payload, packet_id=0xFFFFFFFF)
            )

        peer = _DispatchPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        handled: list[Any] = []
        errors: list[Exception] = []
        completed = threading.Event()
        dispatcher.handled.subscribe(lambda event: (handled.append(event), completed.set()))
        dispatcher.errors.subscribe(errors.append)
        dispatcher.register("camera.boundary", lambda _packet: payload)
        dispatcher.start()
        self.addCleanup(dispatcher.stop)

        self.assertTrue(
            dispatcher.dispatch(
                MavApplicationPacket(
                    "camera.boundary",
                    packet_id=89,
                    source_system=7,
                    source_component=3,
                    expects_response=True,
                )
            )
        )
        self.assertTrue(completed.wait(2.0))
        self.assertTrue(peer.response_sent.wait(2.0))

        self.assertFalse(handled[0].result.accepted)
        self.assertEqual(peer.sent[0][0], "system.error")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], MavApplicationProtocolError)

    def test_dispatcher_subscription_failure_shuts_down_created_work_pool(self) -> None:
        class FailingBus:
            def subscribe(self, _callback: Any) -> None:
                raise OSError("dispatcher subscription failed")

        peer = _DispatchPeer()
        peer.packets = FailingBus()  # type: ignore[assignment]
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        work_pool = Mock()

        with patch("mavflow.dispatch.BoundedWorkPool", return_value=work_pool):
            with self.assertRaisesRegex(OSError, "dispatcher subscription failed"):
                dispatcher.start()

        work_pool.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        self.assertIsNone(dispatcher._work_pool)
        self.assertIsNone(dispatcher._subscription)

    def test_response_send_failures_are_published_without_losing_dispatch_result(self) -> None:
        peer = _DispatchPeer()
        peer.send_error = OSError("transport failed")
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        errors: list[Exception] = []
        error_received = threading.Event()

        def receive_error(error: Exception) -> None:
            errors.append(error)
            error_received.set()

        dispatcher.errors.subscribe(receive_error)
        dispatcher.start()
        self.addCleanup(dispatcher.stop)

        self.assertFalse(
            dispatcher.dispatch(
                MavApplicationPacket("camera.unknown", packet_id=90, expects_response=True)
            )
        )
        self.assertTrue(error_received.wait(1.0))
        self.assertEqual(errors, [peer.send_error])

    def test_unsupported_response_worker_can_stop_its_dispatcher(self) -> None:
        class StoppingPeer(_DispatchPeer):
            def __init__(self) -> None:
                super().__init__()
                self.dispatcher: MavApplicationDispatcher | None = None
                self.stop_errors: list[Exception] = []

            def send(
                self,
                packet_type: str,
                payload: Any,
                **targets: int,
            ) -> None:
                assert self.dispatcher is not None
                try:
                    self.dispatcher.stop()
                except Exception as exc:
                    self.stop_errors.append(exc)
                super().send(packet_type, payload, **targets)

        peer = StoppingPeer()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        peer.dispatcher = dispatcher
        dispatcher.start()

        self.assertFalse(
            dispatcher.dispatch(
                MavApplicationPacket(
                    "camera.unsupported",
                    packet_id=91,
                    expects_response=True,
                )
            )
        )
        self.assertTrue(peer.response_sent.wait(1.0))

        self.assertEqual(peer.stop_errors, [])
        self.assertEqual(peer.sent[0][0], "system.error")
        self.assertIsNone(dispatcher._work_pool)


if __name__ == "__main__":
    unittest.main()
