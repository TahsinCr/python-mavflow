from __future__ import annotations

import queue
import time
import unittest
from typing import Any

from mavflow.router import MavMessageRouter


class _SequencedMessage:
    def __init__(self, sequence: int, source_system: int = 4, source_component: int = 1) -> None:
        self._sequence = sequence
        self._source_system = source_system
        self._source_component = source_component

    def get_type(self) -> str:
        return "HEARTBEAT"

    def get_seq(self) -> int:
        return self._sequence

    def get_srcSystem(self) -> int:
        return self._source_system

    def get_srcComponent(self) -> int:
        return self._source_component


class _RouterConnection:
    def __init__(self) -> None:
        self.inbox: queue.Queue[Any] = queue.Queue()
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    def receive(self, *, blocking: bool, timeout: float) -> Any | None:
        try:
            value = self.inbox.get(timeout=timeout) if blocking else self.inbox.get_nowait()
        except queue.Empty:
            return None
        if isinstance(value, Exception):
            raise value
        return value


class RouterQualityTests(unittest.TestCase):
    def _wait_for(self, predicate: Any, timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertTrue(predicate(), "router did not reach the expected state")

    def test_sequence_wrap_gaps_and_reordering_are_counted_correctly(self) -> None:
        connection = _RouterConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            stop_timeout=0.2,
        )
        router.start()
        self.addCleanup(router.stop)
        for sequence in (254, 255, 0, 1, 4, 200):
            connection.inbox.put(_SequencedMessage(sequence))

        self._wait_for(lambda: router.stats.received_messages == 6)
        stats = router.stats

        self.assertEqual(stats.sequence, 6)
        self.assertEqual(stats.estimated_dropped_messages, 2)
        self.assertEqual(stats.delivery_quality, 75)

    def test_receive_errors_recover_and_subscriber_errors_are_counted(self) -> None:
        connection = _RouterConnection()
        router = MavMessageRouter(
            connection,  # type: ignore[arg-type]
            poll_timeout=0.01,
            error_backoff=0.005,
            stop_timeout=0.2,
        )
        errors: list[Any] = []
        router.errors.subscribe(errors.append)

        def failing_subscriber(_envelope: Any) -> None:
            raise RuntimeError("consumer failed")

        router.messages.subscribe(failing_subscriber, routing_key="HEARTBEAT")
        router.start()
        self.addCleanup(router.stop)
        connection.inbox.put(OSError("radio temporarily unavailable"))
        connection.inbox.put(_SequencedMessage(1))
        connection.inbox.put(_SequencedMessage(2))

        self._wait_for(
            lambda: (
                router.stats.receive_errors == 1
                and router.stats.received_messages == 2
                and router.stats.dispatch_errors == 2
            )
        )

        self.assertEqual(len(errors), 3)
        self.assertEqual(
            [error.phase for error in errors],
            ["receive", "dispatch", "dispatch"],
        )
        self.assertTrue(router.running)


if __name__ == "__main__":
    unittest.main()
