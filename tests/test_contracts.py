from __future__ import annotations

import math
import queue
import threading
import unittest
from typing import Any

from eventflow import EventFilter

from mavflow.connection import MavConnection
from mavflow.endpoint import MavEndpoint
from mavflow.message import MavMessageEnvelope
from mavflow.remote_log import (
    REMOTE_LOG_MAX_BATCH_RECORDS,
    REMOTE_LOG_MAX_DETAILS_BYTES,
    MavRemoteLogBatch,
    MavRemoteLogLevel,
    MavRemoteLogRecord,
)
from mavflow.router import MavMessageRouter


class EndpointTests(unittest.TestCase):
    def test_endpoint_normalizes_supported_transport_forms(self) -> None:
        self.assertEqual(MavEndpoint("127.0.0.1:5760").uri, "tcp:127.0.0.1:5760")
        self.assertEqual(MavEndpoint.tcp("localhost", 5760).uri, "tcp:localhost:5760")
        self.assertEqual(MavEndpoint.udp("0.0.0.0", 14550).uri, "udp:0.0.0.0:14550")
        serial = MavEndpoint.serial(" /dev/ttyUSB0 ", baud=57600)
        self.assertEqual((serial.uri, serial.baud), ("/dev/ttyUSB0", 57600))

    def test_endpoint_rejects_invalid_values(self) -> None:
        for uri in ("udp:127.0.0.1:0", "tcp:localhost:65536", "udp:host:not-a-port"):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                MavEndpoint(uri)
        with self.assertRaises(ValueError):
            MavEndpoint(source_system=256)
        with self.assertRaises(ValueError):
            MavEndpoint(heartbeat_timeout=0)


class _Header:
    def __init__(self, system: int, component: int, sequence: int = 0) -> None:
        self.srcSystem = system
        self.srcComponent = component
        self.seq = sequence


class _RichMessage:
    def __init__(
        self,
        message_type: str,
        *,
        message_id: int,
        system: int,
        component: int,
        value: int = 0,
        sequence: int = 0,
    ) -> None:
        self._type = message_type
        self._id = message_id
        self._header = _Header(system, component, sequence)
        self.value = value

    def get_type(self) -> str:
        return self._type

    def get_msgId(self) -> int:
        return self._id

    def get_srcSystem(self) -> int:
        return self._header.srcSystem

    def get_srcComponent(self) -> int:
        return self._header.srcComponent

    def get_seq(self) -> int:
        return self._header.seq


class _MavSender:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def send(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("send", args, kwargs))

    def command_long_send(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("command_long_send", args, kwargs))

    def heartbeat_send(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("heartbeat_send", args, kwargs))


class _RawConnection:
    def __init__(self, heartbeats: list[Any]) -> None:
        self.heartbeats = heartbeats
        self.closed = 0
        self.mav = _MavSender()
        self.target_system = 7
        self.target_component = 1
        self.messages = {"HEARTBEAT": object()}
        self.recv_kwargs: dict[str, Any] | None = None

    def wait_heartbeat(self, *, timeout: float) -> Any:
        del timeout
        return self.heartbeats.pop(0) if self.heartbeats else None

    def probably_vehicle_heartbeat(self, heartbeat: Any) -> bool:
        return heartbeat == "vehicle"

    def recv_match(self, **kwargs: Any) -> str:
        self.recv_kwargs = kwargs
        return "message"

    def set_mode(self, mode: str) -> str:
        return mode

    def close(self) -> None:
        self.closed += 1


class _MavConstants:
    MAV_CMD_SET_MESSAGE_INTERVAL = 511
    MAV_TYPE_GCS = 6
    MAV_AUTOPILOT_INVALID = 8
    MAV_STATE_ACTIVE = 4


class _MavutilStub:
    mavlink = _MavConstants()

    @staticmethod
    def evaluate_condition(condition: str, messages: dict[str, Any]) -> bool:
        return condition == "HEARTBEAT" and "HEARTBEAT" in messages


class ConnectionTests(unittest.TestCase):
    def test_connection_lifecycle_filters_nonvehicle_heartbeat_and_serializes_calls(self) -> None:
        raw = _RawConnection(["companion", "vehicle"])
        connection = MavConnection(
            MavEndpoint(heartbeat_timeout=0.1),
            connection_factory=lambda _endpoint: raw,
            mavutil_module=_MavutilStub(),
        )
        changes: list[bool] = []
        connection.connection_changed.subscribe(changes.append)

        connection.start()
        connection.start()
        self.assertTrue(connection.is_connected)
        self.assertEqual(connection.target_system, 7)
        self.assertTrue(connection.evaluate_condition("HEARTBEAT"))
        self.assertEqual(
            connection.receive(
                message_types=["HEARTBEAT", "GLOBAL_POSITION_INT"],
                condition=" HEARTBEAT ",
                blocking=True,
                timeout=0.5,
            ),
            "message",
        )
        self.assertEqual(
            raw.recv_kwargs,
            {
                "blocking": True,
                "timeout": 0.5,
                "type": ["HEARTBEAT", "GLOBAL_POSITION_INT"],
                "condition": "HEARTBEAT",
            },
        )
        self.assertEqual(connection.request_message_rate(33, 10.0), 100_000)
        self.assertEqual(connection.call_raw("set_mode", "AUTO"), "AUTO")
        self.assertEqual(connection.sent_messages, 2)
        connection.stop()
        connection.stop()

        self.assertEqual(changes, [True, False])
        self.assertEqual(raw.closed, 1)

    def test_connect_failure_closes_partial_transport_and_publishes_error(self) -> None:
        raw = _RawConnection([])
        connection = MavConnection(
            MavEndpoint(heartbeat_timeout=0.01),
            connection_factory=lambda _endpoint: raw,
        )
        errors: list[Exception] = []
        connection.errors.subscribe(errors.append)

        with self.assertRaises(TimeoutError):
            connection.start()

        self.assertFalse(connection.is_connected)
        self.assertEqual(raw.closed, 1)
        self.assertIsInstance(errors[-1], TimeoutError)

    def test_invalid_send_method_is_reported(self) -> None:
        raw = _RawConnection(["vehicle"])
        connection = MavConnection(
            MavEndpoint(heartbeat_timeout=0.1),
            connection_factory=lambda _endpoint: raw,
        )
        errors: list[Exception] = []
        connection.errors.subscribe(errors.append)
        connection.start()

        with self.assertRaises(ValueError):
            connection.call_mav("missing_send")

        self.assertIsInstance(errors[-1], ValueError)
        connection.stop()


class _RouterConnection:
    def __init__(self) -> None:
        self.inbox: queue.Queue[_RichMessage] = queue.Queue()
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False

    def receive(self, *, blocking: bool, timeout: float | None) -> Any:
        del blocking
        try:
            return self.inbox.get(timeout=timeout)
        except queue.Empty:
            return None

    def evaluate_condition(self, condition: str) -> bool:
        return condition == "ready"


class RouterContractTests(unittest.TestCase):
    def test_router_publishes_keyed_envelopes_with_eventflow_history(self) -> None:
        connection = _RouterConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            stop_timeout=0.2,
        )
        received: list[MavMessageEnvelope] = []
        router.messages.subscribe(
            received.append,
            routing_key="HEARTBEAT",
            predicate=lambda envelope: envelope.source_system == 3,
        )
        router.start()
        connection.inbox.put(
            _RichMessage("HEARTBEAT", message_id=0, system=2, component=1, value=1)
        )
        connection.inbox.put(
            _RichMessage("HEARTBEAT", message_id=0, system=3, component=1, value=2)
        )
        waiter = threading.Timer(
            0.01,
            lambda: router.messages.publish(
                MavMessageEnvelope.wrap(
                    3,
                    _RichMessage("ATTITUDE", message_id=30, system=3, component=1, value=4),
                )
            ),
        )
        waiter.start()
        matched = router.messages.wait_for(routing_key="ATTITUDE", timeout=1.0)
        waiter.join(timeout=1.0)
        history = router.messages.history
        self.assertIsNotNone(history)

        self.assertIsNotNone(matched)
        self.assertEqual(matched.message.value, 4)
        self.assertEqual([envelope.message.value for envelope in received], [2])
        heartbeat_filter = EventFilter(
            predicate=lambda envelope: envelope.message_type == "HEARTBEAT"
        )
        latest = history.latest(heartbeat_filter)
        self.assertIsNotNone(latest)
        self.assertEqual(latest.message.value, 2)
        self.assertEqual(len(history.query(heartbeat_filter)), 2)
        self.assertIsNone(router.messages.wait_for(routing_key="UNKNOWN", timeout=0.02))
        self.assertFalse(waiter.is_alive())
        router.stop()


class RemoteLogTests(unittest.TestCase):
    def _record(self, sequence: int = 1, **changes: Any) -> MavRemoteLogRecord:
        values = {
            "sequence": sequence,
            "source": "mission",
            "action": "started",
            "message": "Mission started",
            **changes,
        }
        return MavRemoteLogRecord(**values)

    def test_remote_log_is_domain_only_detached_and_round_trips(self) -> None:
        details = {"nested": {"values": [1]}}
        record = self._record(details=details, level=MavRemoteLogLevel.ERROR)
        details["nested"]["values"].append(2)  # type: ignore[index,union-attr]

        self.assertFalse(hasattr(record.level, "yki_severity"))
        self.assertEqual(record.level.priority, 40)
        self.assertEqual(record.to_payload()["details"], {"nested": {"values": [1]}})
        with self.assertRaises(TypeError):
            record.details["new"] = True  # type: ignore[index]
        with self.assertRaises(AttributeError):
            record.details["nested"]["values"].append(2)  # type: ignore[union-attr]
        copied = self._record(2, details=record.details)
        self.assertEqual(copied.to_payload()["details"], record.to_payload()["details"])
        batch = MavRemoteLogBatch("session-001", (record, copied))
        restored = MavRemoteLogBatch.from_payload(batch.to_payload())
        self.assertEqual(restored.to_payload(), batch.to_payload())

    def test_remote_log_validates_details_order_and_version(self) -> None:
        with self.assertRaises(ValueError):
            self._record(details={"value": math.nan})
        with self.assertRaises(ValueError):
            self._record(details={"text": "x" * REMOTE_LOG_MAX_DETAILS_BYTES})
        with self.assertRaises(ValueError):
            MavRemoteLogBatch("session-001", (self._record(2), self._record(1)))
        with self.assertRaisesRegex(ValueError, "protocol version"):
            MavRemoteLogBatch.from_payload({"version": 99, "session": "session-001", "records": []})

    def test_remote_log_payload_shapes_and_optional_text_are_normalized(self) -> None:
        record = MavRemoteLogRecord.from_payload(
            {
                "seq": 1,
                "source": " flight ",
                "action": " Started ",
                "message": " Ready ",
                "level": "INFO",
                "at": 1.0,
                "details": {},
                "device_id": "  ",
                "correlation_id": " job-7 ",
            }
        )

        self.assertEqual(
            (record.source, record.action, record.message), ("FLIGHT", "started", "Ready")
        )
        self.assertIsNone(record.device_id)
        self.assertEqual(record.correlation_id, "job-7")
        for payload in ([], {"details": []}, {"level": "not-a-level"}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                MavRemoteLogRecord.from_payload(payload)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            MavRemoteLogBatch.from_payload(
                {"version": 1, "session": "session-001", "records": "bad"}
            )

    def test_remote_log_batch_enforces_count_timestamp_and_encoded_size_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "1.."):
            MavRemoteLogBatch("session-001", ())
        too_many = tuple(
            self._record(sequence) for sequence in range(1, REMOTE_LOG_MAX_BATCH_RECORDS + 2)
        )
        with self.assertRaisesRegex(ValueError, "records"):
            MavRemoteLogBatch("session-001", too_many)
        with self.assertRaisesRegex(ValueError, "timestamp"):
            MavRemoteLogBatch("session-001", (self._record(),), created_at=math.nan)

        oversized = tuple(
            self._record(sequence, details={"text": "x" * 2_500}) for sequence in range(1, 6)
        )
        with self.assertRaisesRegex(ValueError, "encoded bytes"):
            MavRemoteLogBatch("session-001", oversized)


if __name__ == "__main__":
    unittest.main()
