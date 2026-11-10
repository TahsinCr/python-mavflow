"""Loopback tests that exercise the required pymavlink UDP transport."""

from __future__ import annotations

import socket
import threading
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


if __name__ == "__main__":
    unittest.main()
