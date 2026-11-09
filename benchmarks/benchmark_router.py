"""Measure single-reader Mavlink router delivery throughput with pyperf."""

from __future__ import annotations

import queue
import threading
import time

from mavflow import MavMessageRouter

from ._runner import create_runner, positive_integer


class _Message:
    def __init__(self, sequence: int) -> None:
        self.sequence = sequence

    def get_type(self) -> str:
        return "HEARTBEAT"

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


def _sample(messages_per_batch: int) -> float:
    messages: queue.Queue[_Message] = queue.Queue(maxsize=messages_per_batch)
    for sequence in range(messages_per_batch):
        messages.put(_Message(sequence))
    release = threading.Event()
    connection = _Connection(messages, release)
    router = MavMessageRouter(connection, poll_timeout=0.002, stop_timeout=2.0)  # type: ignore[arg-type]
    delivered = 0
    completed = threading.Event()

    def receive(_envelope: object) -> None:
        nonlocal delivered
        delivered += 1
        if delivered == messages_per_batch:
            completed.set()

    router.messages.subscribe(receive, routing_key="HEARTBEAT")
    router.start()
    started = time.perf_counter()
    release.set()
    try:
        if not completed.wait(30.0):
            raise TimeoutError(f"Router delivered {delivered} of {messages_per_batch} messages")
        elapsed = time.perf_counter() - started
    finally:
        router.stop()
    if delivered != messages_per_batch:
        raise RuntimeError(f"Router delivered {delivered} of {messages_per_batch} messages")
    return elapsed


def _time_batches(loops: int, messages_per_batch: int) -> float:
    return sum(_sample(messages_per_batch) for _ in range(loops))


def main() -> None:
    runner = create_runner("benchmarks.benchmark_router")
    runner.argparser.add_argument(
        "--messages",
        type=positive_integer,
        default=1_000,
        help="messages routed in each timed batch",
    )
    args = runner.parse_args()
    runner.bench_time_func(
        "router_receive_and_dispatch",
        _time_batches,
        args.messages,
        inner_loops=args.messages,
        metadata={
            "transport": "preloaded in-memory fake connection",
            "message_type": "HEARTBEAT",
            "messages_per_batch": args.messages,
            "timed_region": "router start through final callback; setup excluded",
        },
    )


if __name__ == "__main__":
    main()
