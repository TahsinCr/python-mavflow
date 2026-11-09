"""Single-reader MAVLink reception with EventFlow message delivery."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, cast

from eventflow import EventBus

from .connection import MavConnection
from .message import MavMessageEnvelope


@dataclass(frozen=True, slots=True)
class MavRouterStats:
    running: bool
    sequence: int
    received_messages: int
    receive_errors: int
    dispatch_errors: int
    estimated_dropped_messages: int
    delivery_quality: int | None
    started_monotonic: float | None
    last_message_monotonic: float | None


@dataclass(frozen=True, slots=True)
class MavRouterError:
    phase: str
    error: Exception
    envelope: MavMessageEnvelope | None = None


class MavMessageRouter:
    """Own one MAVLink receive thread and publish typed envelopes."""

    def __init__(
        self,
        connection: MavConnection,
        *,
        history_limit: int = 512,
        poll_timeout: float = 0.25,
        error_backoff: float = 0.1,
        stop_timeout: float = 2.0,
    ) -> None:
        if history_limit <= 0:
            raise ValueError("history_limit pozitif olmalı")
        if poll_timeout <= 0:
            raise ValueError("poll_timeout pozitif olmalı")
        if error_backoff < 0:
            raise ValueError("error_backoff negatif olamaz")
        if stop_timeout <= 0:
            raise ValueError("stop_timeout pozitif olmalı")
        self.connection = connection
        self.messages = EventBus[MavMessageEnvelope](
            history=history_limit,
            routing_key=lambda envelope: envelope.message_type,
        )
        self.errors = EventBus[MavRouterError]()
        self._poll_timeout = poll_timeout
        self._error_backoff = error_backoff
        self._stop_timeout = float(stop_timeout)
        self._condition = threading.Condition(threading.RLock())
        self._lifecycle_lock = threading.RLock()
        self._sequence = 0
        self._received_messages = 0
        self._receive_errors = 0
        self._dispatch_errors = 0
        self._estimated_dropped_messages = 0
        self._source_sequences: dict[tuple[int, int], int] = {}
        self._delivery_window: deque[tuple[int, int]] = deque(maxlen=256)
        self._started_monotonic: float | None = None
        self._last_message_monotonic: float | None = None
        self._running = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        with self._condition:
            return self._running

    @property
    def sequence(self) -> int:
        """Return the latest sequence number assigned to a received message."""
        with self._condition:
            return self._sequence

    @property
    def stats(self) -> MavRouterStats:
        with self._condition:
            return MavRouterStats(
                running=self._running,
                sequence=self._sequence,
                received_messages=self._received_messages,
                receive_errors=self._receive_errors,
                dispatch_errors=self._dispatch_errors,
                estimated_dropped_messages=self._estimated_dropped_messages,
                delivery_quality=self._delivery_quality(),
                started_monotonic=self._started_monotonic,
                last_message_monotonic=self._last_message_monotonic,
            )

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._thread and self._thread.is_alive():
                if self._stop_event.is_set():
                    raise RuntimeError("MAVLink router hâlâ durduruluyor")
                return
            self.connection.start()
            self._stop_event.clear()
            with self._condition:
                self._source_sequences.clear()
                self._delivery_window.clear()
                self._estimated_dropped_messages = 0
                self._running = True
                self._started_monotonic = time.monotonic()
            self._thread = threading.Thread(
                target=self._read_loop,
                name="MavRouter",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop_event.set()
            thread = self._thread
        if thread is threading.current_thread():
            raise RuntimeError("MAVLink router kendi receive thread'inden durdurulamaz")
        if thread is not None:
            thread.join(timeout=self._stop_timeout)
            if thread.is_alive():
                raise TimeoutError(
                    "MAVLink router receive thread'i zamanında durmadı; "
                    "bağlantı güvenlik için açık bırakıldı"
                )

        try:
            self.connection.stop()
        finally:
            with self._lifecycle_lock:
                if self._thread is thread:
                    self._thread = None
            with self._condition:
                self._running = False
                self._condition.notify_all()

    close = stop

    def _read_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    message = self.connection.receive(
                        blocking=True,
                        timeout=self._poll_timeout,
                    )
                except Exception as exc:
                    if self._stop_event.is_set():
                        return
                    with self._condition:
                        self._receive_errors += 1
                    self.errors.publish(MavRouterError("receive", exc))
                    if self._stop_event.wait(self._error_backoff):
                        return
                    continue
                if message is None:
                    continue
                with self._condition:
                    self._sequence += 1
                    envelope = MavMessageEnvelope.wrap(self._sequence, message)
                    self._received_messages += 1
                    self._record_delivery(envelope)
                    self._last_message_monotonic = envelope.received_monotonic
                self._dispatch(envelope)
        finally:
            with self._condition:
                self._running = False
                self._condition.notify_all()

    def _record_delivery(self, envelope: MavMessageEnvelope) -> None:
        source_system = envelope.source_system
        source_component = envelope.source_component
        sequence = self._message_sequence(envelope.message)
        if source_system is None or source_component is None or sequence is None:
            return
        key = (source_system, source_component)
        previous = self._source_sequences.get(key)
        self._source_sequences[key] = sequence
        if previous is None:
            self._delivery_window.append((1, 0))
            return
        delta = (sequence - previous) & 0xFF
        # Backward jumps larger than 64 indicate restart or reordering, not loss.
        dropped = delta - 1 if 1 < delta <= 64 else 0
        self._estimated_dropped_messages += dropped
        self._delivery_window.append((1, dropped))

    def _delivery_quality(self) -> int | None:
        if not self._delivery_window:
            return None
        received = sum(item[0] for item in self._delivery_window)
        dropped = sum(item[1] for item in self._delivery_window)
        total = received + dropped
        return round(received / total * 100.0) if total else None

    @staticmethod
    def _message_sequence(message: Any) -> int | None:
        getter = getattr(message, "get_seq", None)
        if callable(getter):
            try:
                return int(cast(Any, getter())) & 0xFF
            except (TypeError, ValueError):
                return None
        header = getattr(message, "_header", None)
        value = getattr(header, "seq", None)
        try:
            return int(cast(Any, value)) & 0xFF if value is not None else None
        except (TypeError, ValueError):
            return None

    def _dispatch(self, envelope: MavMessageEnvelope) -> None:
        for error in self.messages.publish(envelope).errors:
            with self._condition:
                self._dispatch_errors += 1
            self.errors.publish(MavRouterError("dispatch", error, envelope))
