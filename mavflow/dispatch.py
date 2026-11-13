from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from eventflow import BoundedWorkPool, EventBus, Subscription

from .application import MavApplicationCodec, MavApplicationPacket
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
        handlers: MavApplicationHandlerRegistry | None = None,
    ) -> None:
        self._peer = peer
        self._workers = workers
        self._max_pending = max_pending
        self._handlers = handlers or MavApplicationHandlerRegistry()
        self._subscription: Subscription | None = None
        self._work_pool: BoundedWorkPool | None = None
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
            self._work_pool = BoundedWorkPool(
                max_workers=self._workers,
                max_pending=self._max_pending,
            )
            try:
                self._subscription = self._peer.packets.subscribe(self._on_packet)
            except Exception:
                work_pool = self._work_pool
                self._work_pool = None
                self._generation += 1
                if work_pool is not None:
                    work_pool.shutdown(wait=True, cancel_futures=True)
                raise

    def _on_packet(self, packet: MavApplicationPacket) -> None:
        self.dispatch(packet)

    def stop(self) -> None:
        """Cancel queued work and wait for running handlers to return.

        Python cannot safely interrupt a synchronous handler that is already
        running; handlers must bound their own blocking I/O and loops.
        """
        with self._lock:
            subscription = self._subscription
            self._subscription = None
            work_pool = self._work_pool
            self._work_pool = None
            self._generation += 1
        if subscription is not None:
            subscription.cancel()
        if work_pool is not None:
            called_from_worker = bool(getattr(self._worker_context, "active", False))
            work_pool.shutdown(
                wait=not called_from_worker,
                cancel_futures=True,
            )

    def dispatch(self, packet: MavApplicationPacket) -> bool:
        """Queue one packet for its registered handler."""

        if not self._peer._accepts_dispatch_packet(packet):
            return False
        if packet.packet_type in self.RESERVED_TYPES:
            return False
        with self._lock:
            work_pool = self._work_pool
            generation = self._generation
        if work_pool is None:
            self.errors.publish(RuntimeError("MAVLink application dispatcher is not running"))
            return False
        handler = self._handlers.resolve(packet.packet_type)
        if handler is None:
            self.unhandled.publish(packet)
            if packet.expects_response:
                result = MavApplicationResult.failure(
                    f"Unsupported application packet type: {packet.packet_type}",
                    {"error_type": "UnsupportedPacket"},
                )
                try:
                    work_pool.submit(
                        self._run_in_worker,
                        self._respond_if_active_generation,
                        packet,
                        result,
                        generation,
                    )
                except Exception as exc:
                    self.errors.publish(exc)
            return False
        try:
            work_pool.submit(self._execute, packet, handler, generation)
        except Exception as exc:
            busy = isinstance(exc, RuntimeError) and self._is_generation_active(generation)
            error = RuntimeError("MAVLink application dispatcher queue is full") if busy else exc
            self.errors.publish(error)
            # Do not send a saturation response on the router thread. The
            # caller observes the bounded-pool error and its request deadline.
            return False
        return True

    def _execute(
        self,
        packet: MavApplicationPacket,
        handler: MavApplicationHandler,
        generation: int,
    ) -> None:
        self._run_in_worker(self._handle, packet, handler, generation)

    def _run_in_worker(self, operation: Callable[..., None], *args: Any) -> None:
        previous_active = bool(getattr(self._worker_context, "active", False))
        self._worker_context.active = True
        try:
            operation(*args)
        finally:
            self._worker_context.active = previous_active

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
                "Application handler failed",
                {"error_type": "HandlerError"},
            )
        if packet.expects_response:
            try:
                response_payload = self._response_payload(packet, result)
                MavApplicationCodec.encode(
                    MavApplicationPacket(
                        result.response_type,
                        response_payload,
                        packet_id=0xFFFFFFFF,
                    )
                )
            except Exception as exc:
                if error is None:
                    error = exc
                result = MavApplicationResult.failure(
                    "Application handler returned an invalid response",
                    {"error_type": "InvalidResponse"},
                )
                response_payload = self._response_payload(packet, result)
        else:
            response_payload = None
        if not self._is_generation_active(generation):
            return
        if error is not None:
            self.errors.publish(error)
        self.handled.publish(MavApplicationDispatch(packet, result))
        if packet.expects_response:
            self._respond(packet, result, response_payload)

    @staticmethod
    def _response_payload(
        request: MavApplicationPacket,
        result: MavApplicationResult,
    ) -> dict[str, Any]:
        payload = MavApplicationPacket._copy_payload_value(result.payload)
        payload.update(
            {
                "request_id": request.packet_id,
                "accepted": result.accepted,
                "message": result.message,
            }
        )
        return payload

    def _respond(
        self,
        request: MavApplicationPacket,
        result: MavApplicationResult,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        response_payload = (
            payload if payload is not None else self._response_payload(request, result)
        )
        try:
            self._peer.send(
                result.response_type,
                response_payload,
                target_system=int(request.source_system or 0),
                target_component=int(request.source_component or 0),
            )
        except Exception as exc:
            self.errors.publish(exc)

    def _respond_if_active_generation(
        self,
        request: MavApplicationPacket,
        result: MavApplicationResult,
        generation: int,
    ) -> None:
        if self._is_generation_active(generation):
            self._respond(request, result)

    def _is_generation_active(self, generation: int) -> bool:
        with self._lock:
            return (
                self._subscription is not None
                and self._work_pool is not None
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
