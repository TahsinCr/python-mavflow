"""Measure single-reader Mavlink router delivery throughput with pyperf."""

from __future__ import annotations

import queue
import threading
import time

from eventflow import EventFilter

from mavflow import MavMessageRouter

from ._runner import create_runner, positive_integer


class _Message:
    def __init__(self, sequence: int, message_type: str) -> None:
        self.sequence = sequence
        self.message_type = message_type

    def get_type(self) -> str:
        return self.message_type

    def get_seq(self) -> int:
        return self.sequence & 0xFF

    def get_msgId(self) -> int:
        return 0

    def get_srcSystem(self) -> int:
        return 1

    def get_srcComponent(self) -> int:
        return 1


class _Connection:
    def __init__(self, messages: queue.Queue[_Message], release: threading.Event) -> None:
        self._messages = messages
        self._release = release

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def receive(self, *, blocking: bool = False, timeout: float | None = None) -> _Message | None:
        del blocking
        if not self._release.wait(timeout=timeout):
            return None
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def evaluate_condition(self, _condition: str) -> bool:
        return True


def _sample(messages_per_batch: int, subscriber_count: int, matching_ratio: int) -> float:
    messages: queue.Queue[_Message] = queue.Queue(maxsize=messages_per_batch)
    for sequence in range(messages_per_batch):
        matches = ((sequence + 1) * matching_ratio // 100) > (sequence * matching_ratio // 100)
        message_type = "HEARTBEAT" if matches else "ATTITUDE"
        messages.put(_Message(sequence, message_type))
    release = threading.Event()
    connection = _Connection(messages, release)
    router = MavMessageRouter(connection, poll_timeout=0.002, stop_timeout=2.0)  # type: ignore[arg-type]
    delivered = 0
    expected_callbacks = (messages_per_batch * matching_ratio // 100) * subscriber_count
    callbacks_complete = threading.Event()
    processing_complete = threading.Event()
    final_sequence = router.sequence + messages_per_batch

    def receive(_envelope: object) -> None:
        nonlocal delivered
        delivered += 1
        if delivered == expected_callbacks:
            callbacks_complete.set()

    def receive_final(_envelope: object) -> None:
        processing_complete.set()

    for _ in range(subscriber_count):
        router.messages.subscribe(
            receive,
            event_filter=EventFilter(
                predicate=lambda envelope: envelope.message_type == "HEARTBEAT"
            ),
        )
    router.messages.subscribe(
        receive_final,
        event_filter=EventFilter(predicate=lambda envelope: envelope.sequence == final_sequence),
    )
    router.start()
    started = time.perf_counter()
    release.set()
    try:
        if not processing_complete.wait(30.0):
            raise TimeoutError(f"Router did not process all {messages_per_batch} messages")
        if expected_callbacks and not callbacks_complete.wait(30.0):
            raise TimeoutError(f"Router delivered {delivered} of {expected_callbacks} callbacks")
        elapsed = time.perf_counter() - started
    finally:
        router.stop()
    if delivered != expected_callbacks:
        raise RuntimeError(f"Router delivered {delivered} of {expected_callbacks} callbacks")
    return elapsed


def _time_batches(
    loops: int,
    messages_per_batch: int,
    subscriber_count: int,
    matching_ratio: int,
) -> float:
    return sum(_sample(messages_per_batch, subscriber_count, matching_ratio) for _ in range(loops))


def main() -> None:
    runner = create_runner("benchmarks.benchmark_router")
    runner.argparser.add_argument(
        "--messages",
        type=positive_integer,
        default=1_000,
        help="messages routed in each timed batch",
    )
    runner.argparser.add_argument(
        "--subscribers",
        type=positive_integer,
        default=1,
        help="EventFlow callbacks subscribed to HEARTBEAT",
    )
    runner.argparser.add_argument(
        "--matching-ratio",
        type=int,
        choices=(0, 10, 50, 100),
        default=100,
        help="percentage of routed messages matching HEARTBEAT subscribers",
    )
    args = runner.parse_args()
    runner.bench_time_func(
        "router_receive_and_dispatch",
        _time_batches,
        args.messages,
        args.subscribers,
        args.matching_ratio,
        inner_loops=args.messages,
        metadata={
            "transport": "preloaded in-memory fake connection",
            "message_types": "HEARTBEAT,ATTITUDE",
            "messages_per_batch": args.messages,
            "subscriber_count": args.subscribers,
            "matching_ratio_percent": args.matching_ratio,
            "matching_messages_per_batch": args.messages * args.matching_ratio // 100,
            "effective_matching_ratio_percent": (args.messages * args.matching_ratio // 100)
            / args.messages
            * 100,
            "completion_observer_count": 1,
            "timed_region": "router start through final envelope dispatch; setup excluded",
        },
    )


if __name__ == "__main__":
    main()
