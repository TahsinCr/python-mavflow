from __future__ import annotations

import queue
import threading
import time
import unittest
from typing import Any

from eventflow import AsyncEventQueue, EventBus, EventFilter

from mavflow.application import MavApplicationPacket
from mavflow.dispatch import MavApplicationDispatcher
from mavflow.message import MavMessageEnvelope
from mavflow.peer import MavApplicationPeer
from mavflow.router import MavMessageRouter


class _Message:
    def __init__(self, message_type: str, value: int = 0) -> None:
        self._message_type = message_type
        self.value = value

    def get_type(self) -> str:
        return self._message_type


class _RestartableConnection:
    def __init__(self) -> None:
        self.inbox: queue.Queue[_Message] = queue.Queue()
        self.start_count = 0
        self.stop_count = 0

    def start(self) -> None:
        self.start_count += 1

    def stop(self) -> None:
        self.stop_count += 1

    def receive(self, *, blocking: bool = False, timeout: float | None = None) -> Any:
        del blocking
        try:
            return self.inbox.get(timeout=timeout)
        except queue.Empty:
            return None

    def evaluate_condition(self, _condition: str) -> bool:
        return True


class _StuckConnection(_RestartableConnection):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def receive(self, *, blocking: bool = False, timeout: float | None = None) -> None:
        del blocking, timeout
        self.entered.set()
        self.release.wait()
        return None


class RouterLifecycleTests(unittest.TestCase):
    def test_router_can_start_stop_and_restart(self) -> None:
        connection = _RestartableConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            stop_timeout=0.2,
        )

        router.start()
        router.stop()
        router.start()
        boundary = router.sequence
        publication = threading.Timer(
            0.01,
            lambda: connection.inbox.put(_Message("HEARTBEAT", 9)),
        )
        publication.start()
        envelope = router.messages.wait_for(
            event_filter=EventFilter(
                predicate=lambda item: item.message_type == "HEARTBEAT" and item.sequence > boundary
            ),
            timeout=1.0,
        )
        publication.join(timeout=1.0)
        router.stop()

        self.assertIsNotNone(envelope)
        self.assertEqual(envelope.message.value, 9)
        self.assertEqual(connection.start_count, 2)
        self.assertEqual(connection.stop_count, 2)
        self.assertFalse(router.running)

    def test_router_keeps_connection_open_when_receive_thread_is_stuck(self) -> None:
        connection = _StuckConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            stop_timeout=0.05,
        )
        router.start()
        self.assertTrue(connection.entered.wait(1.0))

        with self.assertRaisesRegex(TimeoutError, "receive thread"):
            router.stop()

        self.assertEqual(connection.stop_count, 0)
        self.assertTrue(router.running)
        with self.assertRaisesRegex(RuntimeError, "durduruluyor"):
            router.start()

        connection.release.set()
        router.stop()
        self.assertEqual(connection.stop_count, 1)
        self.assertFalse(router.running)

    def test_router_rejects_restart_until_direct_stop_finishes_connection_cleanup(self) -> None:
        class BlockingStopConnection(_RestartableConnection):
            def __init__(self) -> None:
                super().__init__()
                self.stop_entered = threading.Event()
                self.release_stop = threading.Event()

            def stop(self) -> None:
                self.stop_entered.set()
                if not self.release_stop.wait(1.0):
                    raise TimeoutError("test connection stop was not released")
                super().stop()

        connection = BlockingStopConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            stop_timeout=0.2,
        )
        router.start()
        stop_errors: list[Exception] = []

        def stop_router() -> None:
            try:
                router.stop()
            except Exception as exc:
                stop_errors.append(exc)

        stopping = threading.Thread(target=stop_router)
        stopping.start()
        self.assertTrue(connection.stop_entered.wait(1.0))
        with self.assertRaisesRegex(RuntimeError, "durduruluyor"):
            router.start()
        connection.release_stop.set()
        stopping.join(1.0)

        self.assertFalse(stopping.is_alive())
        self.assertEqual(stop_errors, [])
        router.start()
        router.stop()


class AsyncEventQueueIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_eventflow_queue_capacity_includes_all_router_envelopes(self) -> None:
        router = MavMessageRouter(_RestartableConnection())  # type: ignore[arg-type]
        queue = AsyncEventQueue(
            router.messages,
            capacity=1,
            overflow="drop_oldest",
        )
        try:
            router.messages.publish(MavMessageEnvelope.wrap(1, _Message("HEARTBEAT", 1)))
            router.messages.publish(MavMessageEnvelope.wrap(2, _Message("ATTITUDE", 0)))
            router.messages.publish(MavMessageEnvelope.wrap(3, _Message("HEARTBEAT", 2)))

            received = await queue.receive()
            self.assertEqual(received.message.value, 2)
            self.assertEqual(received.message_type, "HEARTBEAT")
            self.assertEqual(queue.stats().dropped, 2)
        finally:
            await queue.aclose()
            router.messages.close()
            router.errors.close()

    async def test_unrelated_telemetry_can_evict_a_heartbeat_from_eventflow_queue(self) -> None:
        router = MavMessageRouter(_RestartableConnection())  # type: ignore[arg-type]
        queue = AsyncEventQueue(
            router.messages,
            capacity=1,
            overflow="drop_oldest",
        )
        try:
            router.messages.publish(MavMessageEnvelope.wrap(1, _Message("HEARTBEAT", 1)))
            router.messages.publish(MavMessageEnvelope.wrap(2, _Message("ATTITUDE", 2)))

            received = await queue.receive()

            self.assertEqual(received.message_type, "ATTITUDE")
            self.assertEqual(received.message.value, 2)
            self.assertEqual(queue.stats().dropped, 1)
        finally:
            await queue.aclose()
            router.messages.close()
            router.errors.close()


class _ChannelStub:
    def __init__(self) -> None:
        self.packets = EventBus[MavApplicationPacket]()
        self.errors = EventBus[Exception]()
        self.sent: list[MavApplicationPacket] = []
        self.sent_targets: list[dict[str, Any]] = []
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False

    def send(
        self,
        packet_type: str,
        payload: Any = None,
        *,
        packet_id: int | None = None,
        expects_response: bool = False,
        **_targets: Any,
    ) -> MavApplicationPacket:
        packet = MavApplicationPacket(
            packet_type,
            dict(payload or {}),
            packet_id=packet_id or len(self.sent) + 1,
            expects_response=expects_response,
        )
        self.sent.append(packet)
        self.sent_targets.append(_targets)
        return packet


class PeerLifecycleTests(unittest.TestCase):
    def _peer(self, channel: _ChannelStub, **options: Any) -> MavApplicationPeer:
        peer_options: dict[str, Any] = {
            "heartbeat_interval": 0.02,
            "heartbeat_timeout": 0.2,
            "stop_timeout": 0.1,
        }
        peer_options.update(options)
        return MavApplicationPeer(
            channel,  # type: ignore[arg-type]
            role="onboard",
            **peer_options,
        )

    def test_concurrent_start_and_stop_are_serialized(self) -> None:
        class BlockingStartChannel(_ChannelStub):
            def __init__(self) -> None:
                super().__init__()
                self.start_entered = threading.Event()
                self.release_start = threading.Event()

            def start(self) -> None:
                self.start_entered.set()
                if not self.release_start.wait(1.0):
                    raise TimeoutError("channel start was not released")
                super().start()

        channel = BlockingStartChannel()
        peer = self._peer(channel)
        start_errors: list[Exception] = []
        stop_errors: list[Exception] = []
        stop_entered = threading.Event()
        stop_completed = threading.Event()

        def start_peer() -> None:
            try:
                peer.start()
            except Exception as exc:
                start_errors.append(exc)

        def stop_peer() -> None:
            stop_entered.set()
            try:
                peer.stop()
            except Exception as exc:
                stop_errors.append(exc)
            finally:
                stop_completed.set()

        starting = threading.Thread(target=start_peer)
        starting.start()
        self.assertTrue(channel.start_entered.wait(1.0))
        stopping = threading.Thread(target=stop_peer)
        stopping.start()
        self.assertTrue(stop_entered.wait(1.0))
        channel.release_start.set()
        starting.join(1.0)
        stopping.join(1.0)

        self.assertFalse(starting.is_alive())
        self.assertFalse(stopping.is_alive())
        self.assertTrue(stop_completed.is_set())
        self.assertEqual(start_errors, [])
        self.assertEqual(stop_errors, [])
        self.assertFalse(peer.running)
        self.assertFalse(channel.started)
        self.assertEqual(peer._subscriptions, [])
        self.assertIsNone(peer._monitor_thread)

    def test_channel_stop_failure_is_retried_by_idempotent_peer_stop(self) -> None:
        class RetryStopChannel(_ChannelStub):
            def __init__(self) -> None:
                super().__init__()
                self.stop_calls = 0

            def stop(self) -> None:
                self.stop_calls += 1
                if self.stop_calls == 1:
                    raise OSError("channel stop failed")
                super().stop()

        channel = RetryStopChannel()
        peer = self._peer(channel)
        peer.start()

        with self.assertRaisesRegex(OSError, "channel stop failed"):
            peer.stop()
        self.assertFalse(peer.running)
        self.assertTrue(channel.started)

        peer.stop()

        self.assertEqual(channel.stop_calls, 2)
        self.assertFalse(channel.started)
        self.assertFalse(peer._channel_cleanup_pending)

    def test_stop_wakes_pending_request_and_prevents_later_packets(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel)
        peer.start()
        sent = threading.Event()
        errors: list[Exception] = []

        def request() -> None:
            try:
                peer.request(
                    "camera.capture",
                    response_types="system.ack",
                    timeout=2.0,
                    on_sent=lambda _packet: sent.set(),
                )
            except Exception as exc:
                errors.append(exc)

        requester = threading.Thread(target=request)
        requester.start()
        self.assertTrue(sent.wait(1.0))
        peer.stop()
        requester.join(1.0)
        sent_count = len(channel.sent)
        time.sleep(0.05)

        self.assertFalse(requester.is_alive())
        self.assertIsInstance(errors[0], ConnectionError)
        self.assertEqual(len(channel.sent), sent_count)
        self.assertFalse(peer.running)

    def test_stuck_monitor_thread_is_reported_and_retained(self) -> None:
        channel = _ChannelStub()
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def transport_available() -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                return True
            entered.set()
            release.wait()
            return True

        peer = self._peer(channel, transport_available=transport_available)
        peer.start()
        self.assertTrue(entered.wait(1.0))

        with self.assertRaisesRegex(TimeoutError, "monitor thread"):
            peer.stop()

        stopped_state = peer.state
        sent_count = len(channel.sent)
        self.assertIsNotNone(peer._monitor_thread)
        self.assertFalse(peer.running)
        with self.assertRaisesRegex(RuntimeError, "still stopping"):
            peer.start()
        release.set()
        peer.stop()
        self.assertIsNone(peer._monitor_thread)
        self.assertEqual(peer.state, stopped_state)
        self.assertEqual(len(channel.sent), sent_count)

    def test_request_accepts_only_correlated_response(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel, target_system=7, target_component=2)
        peer.start()
        sent = threading.Event()
        requests: list[MavApplicationPacket] = []
        responses: list[Any] = []

        def request() -> None:
            responses.append(
                peer.request(
                    "camera.capture",
                    response_types="system.ack",
                    timeout=1.0,
                    on_sent=lambda packet: (requests.append(packet), sent.set()),
                )
            )

        requester = threading.Thread(target=request)
        requester.start()
        self.assertTrue(sent.wait(1.0))
        channel.packets.publish(
            MavApplicationPacket(
                "system.ack",
                {"request_id": requests[0].packet_id + 1},
            )
        )
        self.assertTrue(requester.is_alive())
        channel.packets.publish(
            MavApplicationPacket(
                "system.ack",
                {"request_id": requests[0].packet_id, "accepted": True},
                source_system=7,
                source_component=3,
            )
        )
        self.assertTrue(requester.is_alive())
        channel.packets.publish(
            MavApplicationPacket(
                "system.ack",
                {"request_id": requests[0].packet_id, "accepted": True},
                source_system=8,
                source_component=2,
            )
        )
        self.assertTrue(requester.is_alive())
        channel.packets.publish(
            MavApplicationPacket(
                "system.ack",
                {"request_id": requests[0].packet_id, "accepted": True},
                source_system=7,
                source_component=2,
            )
        )
        requester.join(1.0)
        peer.stop()

        self.assertFalse(requester.is_alive())
        self.assertEqual(responses[0].response.payload["accepted"], True)

    def test_unexpected_peer_cannot_change_liveness_or_trigger_a_pong(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel, target_system=17, target_component=3)
        peer.start()
        channel.packets.publish(
            MavApplicationPacket(
                "system.heartbeat",
                packet_id=101,
                source_system=18,
                source_component=3,
            )
        )
        channel.packets.publish(
            MavApplicationPacket(
                "system.ping",
                packet_id=102,
                source_system=18,
                source_component=3,
            )
        )
        channel.packets.publish(
            MavApplicationPacket(
                "system.heartbeat",
                packet_id=104,
                source_system=17,
                source_component=4,
            )
        )

        self.assertFalse(peer.alive)
        self.assertFalse(any(packet.packet_type == "system.pong" for packet in channel.sent))
        channel.packets.publish(
            MavApplicationPacket(
                "system.heartbeat",
                packet_id=103,
                source_system=17,
                source_component=3,
            )
        )
        self.assertTrue(peer.alive)
        peer.stop()

    def test_request_ignores_matching_id_with_unexpected_response_type(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel)
        peer.start()
        sent = threading.Event()
        requests: list[MavApplicationPacket] = []
        responses: list[Any] = []
        errors: list[Exception] = []

        def request() -> None:
            try:
                responses.append(
                    peer.request(
                        "camera.capture",
                        response_types="system.ack",
                        timeout=1.0,
                        on_sent=lambda packet: (requests.append(packet), sent.set()),
                    )
                )
            except Exception as exc:
                errors.append(exc)

        requester = threading.Thread(target=request)
        requester.start()
        self.assertTrue(sent.wait(1.0))
        request_id = requests[0].packet_id
        channel.packets.publish(MavApplicationPacket("system.error", {"request_id": request_id}))
        self.assertTrue(requester.is_alive())
        channel.packets.publish(
            MavApplicationPacket("system.ack", {"request_id": request_id, "accepted": True})
        )
        requester.join(1.0)
        peer.stop()

        self.assertFalse(requester.is_alive())
        self.assertFalse(errors)
        self.assertTrue(responses[0].response.payload["accepted"])

    def test_peer_answers_ping_and_records_probe_round_trip(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel)
        peer.start()
        channel.packets.publish(
            MavApplicationPacket(
                "system.ping",
                {"health": "ready"},
                packet_id=303,
                source_system=21,
                source_component=5,
            )
        )

        pong_index = next(
            index
            for index, packet in enumerate(channel.sent)
            if packet.packet_type == "system.pong"
        )
        pong = channel.sent[pong_index]
        self.assertEqual(pong.payload["request_id"], 303)
        self.assertEqual(pong.payload["role"], "onboard")
        self.assertEqual(
            channel.sent_targets[pong_index],
            {"target_system": 21, "target_component": 5},
        )

        probe = peer.probe()
        self.assertEqual(probe.packet_type, "system.ping")
        channel.packets.publish(
            MavApplicationPacket("system.pong", {"request_id": probe.packet_id})
        )
        round_trip_ms = peer.state.round_trip_ms
        self.assertIsNotNone(round_trip_ms)
        assert round_trip_ms is not None
        self.assertGreaterEqual(round_trip_ms, 0.0)
        self.assertNotIn(probe.packet_id, peer._probes)
        peer.stop()

    def test_request_validation_and_channel_start_failure_leave_peer_stopped(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel)
        with self.assertRaises(ConnectionError):
            peer.request("camera.capture", response_types="system.ack", timeout=0.1)
        with self.assertRaises(ValueError):
            peer.request("camera.capture", response_types="system.ack", timeout=0)
        for timeout in (float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                peer.request("camera.capture", response_types="system.ack", timeout=timeout)
        for options in (
            {"heartbeat_interval": float("nan")},
            {"heartbeat_timeout": float("inf")},
            {"stop_timeout": float("inf")},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self._peer(_ChannelStub(), **options)

        class FailingChannel(_ChannelStub):
            def start(self) -> None:
                raise OSError("channel start failed")

        failing_channel = FailingChannel()
        failed_peer = self._peer(failing_channel)
        with self.assertRaisesRegex(OSError, "channel start failed"):
            failed_peer.start()
        self.assertFalse(failed_peer.running)
        failing_channel.packets.publish(MavApplicationPacket("system.heartbeat"))
        self.assertEqual(failed_peer.state.packets_in, 0)

    def test_second_subscription_failure_cancels_the_first_and_resets_peer(self) -> None:
        class TrackedSubscription:
            cancelled = False

            def cancel(self) -> None:
                self.cancelled = True

        class Bus:
            def __init__(self, fail: bool = False) -> None:
                self.fail = fail
                self.subscription: TrackedSubscription | None = None

            def subscribe(self, _callback: Any) -> TrackedSubscription:
                if self.fail:
                    raise OSError("second subscription failed")
                self.subscription = TrackedSubscription()
                return self.subscription

        channel = _ChannelStub()
        packets = Bus()
        channel.packets = packets  # type: ignore[assignment]
        channel.errors = Bus(fail=True)  # type: ignore[assignment]
        peer = self._peer(channel)

        with self.assertRaisesRegex(OSError, "second subscription failed"):
            peer.start()

        self.assertFalse(peer.running)
        assert packets.subscription is not None
        self.assertTrue(packets.subscription.cancelled)
        self.assertFalse(channel.started)

    def test_request_timeout_is_an_absolute_deadline_for_late_responses(self) -> None:
        class LateResponseChannel(_ChannelStub):
            def send(self, *args: Any, **kwargs: Any) -> MavApplicationPacket:
                request = super().send(*args, **kwargs)
                if kwargs.get("expects_response"):
                    time.sleep(0.05)
                    self.packets.publish(
                        MavApplicationPacket(
                            "system.ack",
                            {"request_id": request.packet_id},
                            packet_id=90,
                            source_system=7,
                            source_component=3,
                        )
                    )
                return request

        channel = LateResponseChannel()
        peer = self._peer(channel)
        peer.start()
        self.addCleanup(peer.stop)

        with self.assertRaisesRegex(TimeoutError, "deadline expired"):
            peer.request(
                "camera.capture",
                response_types="system.ack",
                timeout=0.02,
                target_system=7,
                target_component=3,
            )

    def test_request_timeout_and_liveness_expiry(self) -> None:
        channel = _ChannelStub()
        peer = self._peer(channel)
        peer.start()

        with self.assertRaises(TimeoutError):
            peer.request(
                "camera.capture",
                response_types="system.ack",
                timeout=0.02,
            )

        channel.packets.publish(MavApplicationPacket("system.heartbeat"))
        self.assertTrue(peer.alive)
        deadline = time.monotonic() + 0.5
        while peer.alive and time.monotonic() < deadline:
            time.sleep(0.01)
        peer.stop()
        self.assertFalse(peer.alive)

    def test_request_timeout_cannot_interrupt_a_blocking_transport_send(self) -> None:
        class BlockingSendChannel(_ChannelStub):
            def __init__(self) -> None:
                super().__init__()
                self.send_entered = threading.Event()
                self.release_send = threading.Event()

            def send(self, *args: Any, **kwargs: Any) -> MavApplicationPacket:
                self.send_entered.set()
                if not self.release_send.wait(1.0):
                    raise TimeoutError("transport send was not released")
                return super().send(*args, **kwargs)

        channel = BlockingSendChannel()
        peer = self._peer(channel, target_system=7, target_component=3)
        peer.start()
        errors: list[Exception] = []

        def request() -> None:
            try:
                peer.request(
                    "camera.capture",
                    response_types="system.ack",
                    timeout=0.02,
                )
            except Exception as exc:
                errors.append(exc)

        requester = threading.Thread(target=request)
        requester.start()
        self.assertTrue(channel.send_entered.wait(1.0))
        time.sleep(0.03)
        self.assertTrue(requester.is_alive())
        channel.release_send.set()
        requester.join(1.0)

        self.assertFalse(requester.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TimeoutError)
        peer.stop()


class _PeerStub:
    def __init__(self) -> None:
        self.packets = EventBus[MavApplicationPacket]()
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def send(self, packet_type: str, payload: Any, **_targets: Any) -> None:
        self.sent.append((packet_type, dict(payload)))

    @staticmethod
    def _accepts_dispatch_packet(_packet: MavApplicationPacket) -> bool:
        return True


class DispatcherLifecycleTests(unittest.TestCase):
    def test_handler_can_stop_dispatcher_without_joining_itself(self) -> None:
        peer = _PeerStub()
        dispatcher = MavApplicationDispatcher(peer)  # type: ignore[arg-type]
        finished = threading.Event()
        errors: list[BaseException] = []

        def handler(_packet: MavApplicationPacket) -> dict[str, bool]:
            try:
                dispatcher.stop()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                finished.set()
            return {"done": True}

        dispatcher.register("system.stop", handler)
        dispatcher.start()
        self.assertTrue(
            dispatcher.dispatch(
                MavApplicationPacket(
                    "system.stop",
                    packet_id=99,
                    expects_response=True,
                )
            )
        )

        self.assertTrue(finished.wait(1.0))
        self.assertFalse(errors)
        self.assertFalse(dispatcher.running)
        self.assertFalse(peer.sent)

    def test_stop_suppresses_running_handler_and_cancels_pending_work(self) -> None:
        peer = _PeerStub()
        dispatcher = MavApplicationDispatcher(
            peer,  # type: ignore[arg-type]
            workers=1,
            max_pending=1,
        )
        entered = threading.Event()
        release = threading.Event()
        errors: list[Exception] = []
        dispatcher.errors.subscribe(errors.append)

        def handler(_packet: MavApplicationPacket) -> dict[str, bool]:
            entered.set()
            release.wait()
            return {"done": True}

        dispatcher.register("camera.capture", handler)
        dispatcher.start()
        first = MavApplicationPacket("camera.capture", packet_id=101, expects_response=True)
        second = MavApplicationPacket("camera.capture", packet_id=102, expects_response=True)
        third = MavApplicationPacket("camera.capture", packet_id=103, expects_response=True)
        self.assertTrue(dispatcher.dispatch(first))
        self.assertTrue(entered.wait(1.0))
        self.assertTrue(dispatcher.dispatch(second))
        self.assertFalse(dispatcher.dispatch(third))

        stopper = threading.Thread(target=dispatcher.stop)
        stopper.start()
        time.sleep(0.02)
        self.assertTrue(stopper.is_alive())
        release.set()
        stopper.join(1.0)

        self.assertFalse(stopper.is_alive())
        response_ids = [payload["request_id"] for _, payload in peer.sent]
        self.assertIn(103, response_ids)
        self.assertNotIn(101, response_ids)
        self.assertNotIn(102, response_ids)
        busy_response = next(payload for _, payload in peer.sent if payload["request_id"] == 103)
        self.assertEqual(busy_response["error_type"], "DispatcherBusy")
        self.assertEqual(len(errors), 1)
        self.assertRegex(str(errors[0]), "queue is full")

        dispatcher.start()
        handled = threading.Event()
        dispatcher.handled.subscribe(lambda _result: handled.set())
        self.assertTrue(dispatcher.dispatch(MavApplicationPacket("camera.capture", packet_id=104)))
        self.assertTrue(handled.wait(1.0))
        dispatcher.stop()


if __name__ == "__main__":
    unittest.main()
