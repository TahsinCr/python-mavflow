"""Loopback tests that exercise the required pymavlink UDP transport."""

from __future__ import annotations

import os
import socket
import threading
import time
import unittest

from eventflow import EventFilter
from pymavlink import mavutil

from mavflow.connection import MavConnection
from mavflow.endpoint import MavEndpoint
from mavflow.router import MavMessageRouter


class PymavlinkUdpIntegrationTests(unittest.TestCase):
    def test_connection_and_router_receive_real_udp_heartbeat(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = int(probe.getsockname()[1])

        endpoint = MavEndpoint(
            f"udpin:127.0.0.1:{port}",
            heartbeat_timeout=2.0,
        )
        connection = MavConnection(endpoint)
        router = MavMessageRouter(
            connection,
            poll_timeout=0.02,
            stop_timeout=1.0,
        )
        sender = mavutil.mavlink_connection(
            f"udpout:127.0.0.1:{port}",
            source_system=1,
            source_component=1,
        )
        stop_sender = threading.Event()

        def send_heartbeats() -> None:
            while not stop_sender.is_set():
                sender.mav.heartbeat_send(
                    mavutil.mavlink.MAV_TYPE_QUADROTOR,
                    mavutil.mavlink.MAV_AUTOPILOT_ARDUPILOTMEGA,
                    0,
                    0,
                    mavutil.mavlink.MAV_STATE_ACTIVE,
                )
                stop_sender.wait(0.05)

        sender_thread = threading.Thread(target=send_heartbeats, daemon=True)
        sender_thread.start()
        boundary = router.sequence
        next_envelope = EventFilter(
            predicate=lambda envelope: (
                envelope.message_type == "HEARTBEAT" and envelope.sequence > boundary
            )
        )
        try:
            router.start()
            envelope = router.messages.wait_for(
                event_filter=next_envelope,
                timeout=2.0,
            )
            self.assertIsNotNone(envelope)
            message = envelope.message
            self.assertEqual(message.get_srcSystem(), 1)
            self.assertEqual(message.get_srcComponent(), 1)
        finally:
            stop_sender.set()
            sender_thread.join(1.0)
            if router.running or connection.is_connected:
                router.stop()
            sender.close()

    def test_mavlink2_signing_rejects_unsigned_and_wrong_key_heartbeats(self) -> None:
        previous_mavlink = mavutil.mavlink
        previous_dialect = mavutil.current_dialect
        previous_mavlink20 = os.environ.get("MAVLINK20")
        receiver: MavConnection | None = None
        senders: list[object] = []
        connect_thread: threading.Thread | None = None
        connect_errors: list[Exception] = []

        try:
            os.environ["MAVLINK20"] = "1"
            mavutil.set_dialect("ardupilotmega")
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.bind(("127.0.0.1", 0))
                port = int(probe.getsockname()[1])

            raw_ready = threading.Event()

            def create_transport(endpoint: MavEndpoint) -> object:
                raw = mavutil.mavlink_connection(
                    endpoint.uri,
                    source_system=endpoint.source_system,
                    source_component=endpoint.source_component,
                    dialect=endpoint.dialect,
                )
                raw_ready.set()
                return raw

            secret = b"a" * 32
            receiver = MavConnection(
                MavEndpoint(
                    f"udpin:127.0.0.1:{port}",
                    expected_system=7,
                    expected_component=3,
                    heartbeat_timeout=2.0,
                    signing_key=secret,
                ),
                connection_factory=create_transport,  # type: ignore[arg-type]
            )

            def connect() -> None:
                try:
                    receiver.start()
                except Exception as exc:
                    connect_errors.append(exc)

            connect_thread = threading.Thread(target=connect, daemon=True)
            connect_thread.start()
            self.assertTrue(raw_ready.wait(1.0), "pymavlink UDP listener did not start")

            def make_sender(signing_key: bytes | None) -> object:
                sender = mavutil.mavlink_connection(
                    f"udpout:127.0.0.1:{port}",
                    source_system=7,
                    source_component=3,
                    dialect="ardupilotmega",
                )
                if signing_key is not None:
                    sender.setup_signing(signing_key, sign_outgoing=True)
                senders.append(sender)
                return sender

            def send_for(sender: object, duration: float) -> None:
                deadline = time.monotonic() + duration
                while time.monotonic() < deadline and connect_thread.is_alive():
                    sender.mav.heartbeat_send(
                        mavutil.mavlink.MAV_TYPE_QUADROTOR,
                        mavutil.mavlink.MAV_AUTOPILOT_ARDUPILOTMEGA,
                        0,
                        0,
                        mavutil.mavlink.MAV_STATE_ACTIVE,
                    )
                    time.sleep(0.01)

            unsigned_sender = make_sender(None)
            send_for(unsigned_sender, 0.15)
            self.assertTrue(connect_thread.is_alive(), "unsigned heartbeat was accepted")

            wrong_key_sender = make_sender(b"b" * 32)
            send_for(wrong_key_sender, 0.15)
            self.assertTrue(connect_thread.is_alive(), "wrong-key heartbeat was accepted")

            valid_sender = make_sender(secret)
            send_for(valid_sender, 0.5)
            connect_thread.join(1.0)
            self.assertFalse(connect_thread.is_alive(), "valid signed heartbeat was not accepted")
            self.assertEqual(connect_errors, [])
            self.assertTrue(receiver.is_connected)
        finally:
            if connect_thread is not None:
                connect_thread.join(2.0)
            if receiver is not None and receiver.is_connected:
                receiver.stop()
            for sender in senders:
                sender.close()
            mavutil.mavlink = previous_mavlink
            mavutil.current_dialect = previous_dialect
            if previous_mavlink20 is None:
                os.environ.pop("MAVLINK20", None)
            else:
                os.environ["MAVLINK20"] = previous_mavlink20


if __name__ == "__main__":
    unittest.main()
