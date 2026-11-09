from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from eventflow import EventBus, Subscription

from .application import MavApplicationPacket
from .peer import MavApplicationPeer


@dataclass(frozen=True, slots=True)
class MavApplicationResult:
    """Transport-neutral result returned by an application packet handler."""

    accepted: bool = True
    message: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)
    response_type: str = ""

    def __post_init__(self) -> None:
        response_type = str(self.response_type).strip().lower()
        if not response_type:
            response_type = "system.ack" if self.accepted else "system.error"
        object.__setattr__(self, "accepted", bool(self.accepted))
        object.__setattr__(self, "message", str(self.message))
        object.__setattr__(
            self,
            "payload",
            MavApplicationPacket._freeze_payload_value(self.payload),
        )
        object.__setattr__(self, "response_type", response_type)

    @classmethod
    def success(
        cls,
        payload: Mapping[str, Any] | None = None,
        *,
        message: str = "Command accepted",
    ) -> MavApplicationResult:
        return cls(True, message, dict(payload or {}))

    @classmethod
    def failure(
        cls,
        message: str,
        payload: Mapping[str, Any] | None = None,
    ) -> MavApplicationResult:
        return cls(False, message, dict(payload or {}))


@dataclass(frozen=True, slots=True)
class MavApplicationDispatch:
    """Record a handled request and the result produced for it."""

    packet: MavApplicationPacket
    result: MavApplicationResult


MavApplicationHandler = Callable[
    [MavApplicationPacket],
    MavApplicationResult | Mapping[str, Any] | None,
]


class MavApplicationHandlerRegistry:
    """Keep packet handlers independent from a channel or peer lifecycle."""

    def __init__(self) -> None:
        self._handlers: dict[
            str,
            tuple[object, MavApplicationHandler],
        ] = {}
        self._lock = threading.RLock()

    @property
    def packet_types(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._handlers))

    def register(
        self,
        packet_type: str,
        handler: MavApplicationHandler,
        *,
        replace: bool = False,
    ) -> Subscription:
        normalized = _normalize_packet_type(packet_type)
        if normalized in MavApplicationDispatcher.RESERVED_TYPES:
            raise ValueError(f"Reserved application packet type: {normalized}")
        if not callable(handler):
            raise TypeError("Application packet handler must be callable")
        token = object()
        with self._lock:
            if normalized in self._handlers and not replace:
                raise ValueError(f"Application packet handler is already registered: {normalized}")
            self._handlers[normalized] = (token, handler)

        def cancel() -> None:
            with self._lock:
                current = self._handlers.get(normalized)
                if current is not None and current[0] is token:
                    self._handlers.pop(normalized, None)

        return Subscription(id(token), cancel)

    def resolve(self, packet_type: str) -> MavApplicationHandler | None:
        with self._lock:
            entry = self._handlers.get(_normalize_packet_type(packet_type))
            return entry[1] if entry is not None else None


class MavApplicationDispatcher:
    """Route application packets without blocking the MAVLink receive thread.

    Handlers are registered by packet type. Notifications are handled without
    a reply, while packets sent through ``MavApplicationPeer.request``
    receive a correlated ``system.ack`` or ``system.error`` automatically.
    """

    RESERVED_TYPES = MavApplicationPeer.LIVENESS_TYPES | frozenset({"system.ack", "system.error"})

    def __init__(
        self,
        peer: MavApplicationPeer,
        *,
        workers: int = 1,
        max_pending: int = 64,
        thread_name: str = "MavApplicationDispatch",
        handlers: MavApplicationHandlerRegistry | None = None,
    ) -> None:
        if int(workers) <= 0:
            raise ValueError("Application dispatcher worker count must be positive")
        if int(max_pending) <= 0:
            raise ValueError("Application dispatcher queue limit must be positive")
        self._peer = peer
        self._workers = int(workers)
        self._thread_name = str(thread_name).strip() or "MavApplicationDispatch"
        self._capacity = threading.BoundedSemaphore(int(max_pending))
        self._handlers = handlers or MavApplicationHandlerRegistry()
        self._subscription: Subscription | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._generation = 0
        self._lock = threading.RLock()
        self._worker_context = threading.local()

        self.handled = EventBus[MavApplicationDispatch]()
        self.unhandled = EventBus[MavApplicationPacket]()
        self.errors = EventBus[Exception]()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._subscription is not None

    @property
    def packet_types(self) -> tuple[str, ...]:
        return self._handlers.packet_types

    def register(
        self,
        packet_type: str,
        handler: MavApplicationHandler,
        *,
        replace: bool = False,
    ) -> Subscription:
        return self._handlers.register(packet_type, handler, replace=replace)

    def start(self) -> None:
        with self._lock:
            if self._subscription is not None:
                return
            self._generation += 1
            self._executor = ThreadPoolExecutor(
                max_workers=self._workers,
                thread_name_prefix=self._thread_name,
            )
            self._subscription = self._peer.packets.subscribe(self._on_packet)

    def _on_packet(self, packet: MavApplicationPacket) -> None:
        self.dispatch(packet)

    def stop(self) -> None:
        with self._lock:
            subscription = self._subscription
            self._subscription = None
            executor = self._executor
            self._executor = None
            self._generation += 1
        if subscription is not None:
            subscription.cancel()
        if executor is not None:
            called_from_worker = bool(getattr(self._worker_context, "active", False))
            executor.shutdown(
                wait=not called_from_worker,
                cancel_futures=True,
            )

    def dispatch(self, packet: MavApplicationPacket) -> bool:
        """Queue one packet for its registered handler."""

        if packet.packet_type in self.RESERVED_TYPES:
            return False
        with self._lock:
            executor = self._executor
            generation = self._generation
        if executor is None:
            self.errors.publish(RuntimeError("MAVLink application dispatcher is not running"))
            return False
        handler = self._handlers.resolve(packet.packet_type)
        if handler is None:
            self.unhandled.publish(packet)
            if packet.expects_response:
                self._respond(
                    packet,
                    MavApplicationResult.failure(
                        f"Unsupported application packet type: {packet.packet_type}",
                        {"error_type": "UnsupportedPacket"},
                    ),
                )
            return False
        if not self._capacity.acquire(blocking=False):
            error = RuntimeError("MAVLink application dispatcher queue is full")
            self.errors.publish(error)
            if packet.expects_response:
                self._respond(
                    packet,
                    MavApplicationResult.failure(
                        str(error),
                        {"error_type": "DispatcherBusy"},
                    ),
                )
            return False
        try:
            future = executor.submit(self._execute, packet, handler, generation)
        except Exception as exc:
            self._capacity.release()
            self.errors.publish(exc)
            if packet.expects_response and self._is_generation_active(generation):
                self._respond(
                    packet,
                    MavApplicationResult.failure(
                        str(exc),
                        {"error_type": exc.__class__.__name__},
                    ),
                )
            return False
        future.add_done_callback(lambda _future: self._capacity.release())
        return True

    def _execute(
        self,
        packet: MavApplicationPacket,
        handler: MavApplicationHandler,
        generation: int,
    ) -> None:
        self._worker_context.active = True
        try:
            self._handle(packet, handler, generation)
        finally:
            self._worker_context.active = False

    def _handle(
        self,
        packet: MavApplicationPacket,
        handler: MavApplicationHandler,
        generation: int,
    ) -> None:
        error: Exception | None = None
        try:
            returned = handler(packet)
            if isinstance(returned, MavApplicationResult):
                result = returned
            elif returned is None:
                result = MavApplicationResult.success()
            elif isinstance(returned, Mapping):
                result = MavApplicationResult.success(returned)
            else:
                raise TypeError(
                    "Application packet handlers must return a result, mapping, or None"
                )
        except Exception as exc:
            error = exc
            result = MavApplicationResult.failure(
                str(exc),
                {"error_type": exc.__class__.__name__},
            )
        if not self._is_generation_active(generation):
            return
        if error is not None:
            self.errors.publish(error)
        self.handled.publish(MavApplicationDispatch(packet, result))
        if packet.expects_response:
            self._respond(packet, result)

    def _respond(
        self,
        request: MavApplicationPacket,
        result: MavApplicationResult,
    ) -> None:
        payload = MavApplicationPacket._copy_payload_value(result.payload)
        payload.update(
            {
                "request_id": request.packet_id,
                "accepted": result.accepted,
                "message": result.message,
            }
        )
        try:
            self._peer.send(
                result.response_type,
                payload,
                target_system=int(request.source_system or 0),
                target_component=int(request.source_component or 0),
            )
        except Exception as exc:
            self.errors.publish(exc)

    def _is_generation_active(self, generation: int) -> bool:
        with self._lock:
            return (
                self._subscription is not None
                and self._executor is not None
                and self._generation == generation
            )


def _normalize_packet_type(packet_type: str) -> str:
    normalized = str(packet_type).strip().lower()
    if not normalized:
        raise ValueError("Application packet type cannot be empty")
    return normalized


__all__ = [
    "MavApplicationDispatch",
    "MavApplicationDispatcher",
    "MavApplicationHandler",
    "MavApplicationHandlerRegistry",
    "MavApplicationResult",
]
