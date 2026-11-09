"""High-level MAVLink runtime that composes the reusable low-level pieces."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from eventflow import EventBus, Subscription

from ._compatibility import ExceptionGroup
from .application import MavApplicationChannel, MavApplicationPacket
from .client import MavClient
from .dispatch import (
    MavApplicationDispatcher,
    MavApplicationHandler,
)
from .endpoint import MavEndpoint
from .filter import MavMessageFilter, MessagePredicate, MessageTypeInput
from .peer import MavApplicationPeer, MavApplicationResponse
from .router import MavRouterStats


@dataclass(frozen=True, slots=True)
class MavRuntimeError:
    source: str
    error: Exception


@dataclass(frozen=True, slots=True)
class MavRuntimeState:
    running: bool
    connected: bool
    application_enabled: bool
    peer_alive: bool
    router: MavRouterStats


class MavRuntime:
    """Offer one lifecycle and a small API for telemetry and application data."""

    def __init__(
        self,
        endpoint: MavEndpoint | None = None,
        *,
        client: MavClient | None = None,
        application_role: str | None = None,
        channel: MavApplicationChannel | None = None,
        peer: MavApplicationPeer | None = None,
        dispatcher: MavApplicationDispatcher | None = None,
        workers: int = 1,
        max_pending: int = 64,
        channel_options: Mapping[str, Any] | None = None,
        peer_options: Mapping[str, Any] | None = None,
    ) -> None:
        if client is not None and endpoint is not None and client.endpoint != endpoint:
            raise ValueError("MAVLink runtime endpoint and client endpoint do not match")
        self.client = client or MavClient(endpoint or MavEndpoint())
        self.connection = self.client.connection
        self.router = self.client.router

        role = str(application_role).strip().lower() if application_role else ""
        if channel is not None and peer is None and not role:
            raise ValueError("A custom application channel requires a role or peer")
        if dispatcher is not None and peer is None:
            raise ValueError("A custom dispatcher requires an application peer")

        if peer is None and role:
            channel = channel or MavApplicationChannel(
                self.client,
                **dict(channel_options or {}),
            )
            peer = MavApplicationPeer(
                channel,
                role=role,
                target_system=lambda: self.connection.target_system,
                target_component=lambda: self.connection.target_component,
                transport_available=lambda: self.client.is_connected,
                **dict(peer_options or {}),
            )
        self.channel = channel
        self.peer = peer
        self.dispatcher = dispatcher or (
            MavApplicationDispatcher(
                peer,
                workers=workers,
                max_pending=max_pending,
            )
            if peer is not None
            else None
        )

        self.errors = EventBus[MavRuntimeError](history=100)
        self._error_subscriptions: list[Subscription] = []
        self._running = False
        self._cleanup_pending = False
        self._closed = False
        self._lock = threading.RLock()
        self._bridge_errors()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def application_enabled(self) -> bool:
        return self.peer is not None and self.dispatcher is not None

    @property
    def messages(self) -> EventBus[Any]:
        return self.router.messages

    @property
    def packets(self) -> EventBus[MavApplicationPacket]:
        if self.peer is None:
            raise RuntimeError("MAVLink application channel is not configured")
        return self.peer.packets

    @property
    def state(self) -> MavRuntimeState:
        return MavRuntimeState(
            running=self.running,
            connected=self.client.is_connected,
            application_enabled=self.application_enabled,
            peer_alive=bool(self.peer is not None and self.peer.alive),
            router=self.router.stats,
        )

    def start(self) -> None:
        """Start transport, application peer, and dispatcher in safe order."""

        with self._lock:
            if self._closed:
                raise RuntimeError("MAVLink runtime is closed")
            if self._cleanup_pending:
                raise RuntimeError("MAVLink runtime cleanup must complete before restart")
            if self._running:
                return
        started_stops: list[Callable[[], None]] = []
        try:
            self.client.start()
            started_stops.append(self.client.stop)
            if self.peer is not None:
                self.peer.start()
                started_stops.append(self.peer.stop)
            elif self.channel is not None:
                self.channel.start()
                started_stops.append(self.channel.stop)
            if self.dispatcher is not None:
                self.dispatcher.start()
                started_stops.append(self.dispatcher.stop)
        except Exception as exc:
            self._publish_error("start", exc)
            cleanup_failed = False
            for stop_component in reversed(started_stops):
                try:
                    stop_component()
                except Exception as stop_error:
                    cleanup_failed = True
                    self._publish_error("rollback", stop_error)
            with self._lock:
                self._cleanup_pending = cleanup_failed
            raise
        with self._lock:
            self._running = True

    def stop(self) -> None:
        """Stop every owned layer in reverse order and preserve all errors."""

        with self._lock:
            if not self._running and not self._cleanup_pending:
                return
            self._running = False
            self._cleanup_pending = True
        stop_callbacks: list[Callable[[], None]] = []
        if self.dispatcher is not None:
            stop_callbacks.append(self.dispatcher.stop)
        if self.peer is not None:
            stop_callbacks.append(self.peer.stop)
        elif self.channel is not None:
            stop_callbacks.append(self.channel.stop)
        stop_callbacks.append(self.client.stop)

        errors: list[Exception] = []
        for stop_component in stop_callbacks:
            try:
                stop_component()
            except Exception as exc:
                errors.append(exc)
                self._publish_error("stop", exc)
        if errors:
            raise ExceptionGroup("MAVLink runtime shutdown failed", errors)
        with self._lock:
            self._cleanup_pending = False

    def close(self) -> None:
        """Stop owned services and permanently release runtime subscriptions."""

        with self._lock:
            if self._closed:
                return
        self.stop()
        with self._lock:
            self._closed = True
            subscriptions = tuple(self._error_subscriptions)
            self._error_subscriptions.clear()
        for subscription in subscriptions:
            subscription.cancel()
        self.errors.close()

    def reconnect(self) -> None:
        self.stop()
        self.start()

    def subscribe(
        self,
        message_types: MavMessageFilter | MessageTypeInput,
        callback: Callable[[Any], None],
        *,
        predicate: MessagePredicate | None = None,
    ) -> Subscription:
        message_filter = (
            message_types
            if isinstance(message_types, MavMessageFilter)
            else MavMessageFilter(
                message_types=message_types,
                predicate=predicate,
            )
        )
        if isinstance(message_types, MavMessageFilter) and predicate is not None:
            raise ValueError("Use a MavMessageFilter or predicate, not both")
        return self.client.subscribe(callback, message_filter)

    on = subscribe

    def once(
        self,
        message_types: MavMessageFilter | MessageTypeInput,
        callback: Callable[[Any], None],
        *,
        predicate: MessagePredicate | None = None,
    ) -> Subscription:
        """Handle the first matching MAVLink message and then unsubscribe."""

        lock = threading.Lock()
        subscription: Subscription | None = None
        consumed = False

        def receive(message: Any) -> None:
            nonlocal consumed
            with lock:
                if consumed:
                    return
                consumed = True
                current = subscription
            if current is not None:
                current.cancel()
            callback(message)

        subscription = self.subscribe(
            message_types,
            receive,
            predicate=predicate,
        )
        with lock:
            if consumed:
                subscription.cancel()
        return subscription

    def wait_for(
        self,
        message_types: MavMessageFilter | MessageTypeInput,
        *,
        predicate: MessagePredicate | None = None,
        timeout: float = 3.0,
        after_sequence: int | None = None,
    ) -> Any:
        return self.client.wait_for(
            message_types,
            predicate=predicate,
            timeout=timeout,
            after_sequence=after_sequence,
        )

    def latest(
        self,
        message_filter: MavMessageFilter | MessageTypeInput | None = None,
    ) -> Any | None:
        return self.client.latest(message_filter)

    def send(self, message: Any) -> None:
        self.client.send(message)

    def send_named(self, message_name: str, **parameters: Any) -> None:
        self.client.send_named(message_name, **parameters)

    def handle(
        self,
        packet_type: str,
        handler: MavApplicationHandler,
        *,
        replace: bool = False,
    ) -> Subscription:
        if self.dispatcher is None:
            raise RuntimeError("MAVLink application dispatcher is not configured")
        return self.dispatcher.register(packet_type, handler, replace=replace)

    def notify(
        self,
        packet_type: str,
        payload: Mapping[str, Any] | None = None,
    ) -> MavApplicationPacket:
        if self.peer is None:
            raise RuntimeError("MAVLink application peer is not configured")
        return self.peer.send(packet_type, payload)

    def request(
        self,
        packet_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        response_types: str | tuple[str, ...] | frozenset[str] = (
            "system.ack",
            "system.error",
        ),
        timeout: float = 3.0,
    ) -> MavApplicationResponse:
        if self.peer is None:
            raise RuntimeError("MAVLink application peer is not configured")
        return self.peer.request(
            packet_type,
            payload,
            response_types=response_types,
            timeout=timeout,
        )

    def _bridge_errors(self) -> None:
        sources = (
            ("connection", getattr(self.connection, "errors", None)),
            ("router", getattr(self.router, "errors", None)),
            ("channel", getattr(self.channel, "errors", None)),
            ("peer", getattr(self.peer, "errors", None)),
            ("dispatcher", getattr(self.dispatcher, "errors", None)),
        )
        for name, events in sources:
            if events is None:
                continue
            self._error_subscriptions.append(
                events.subscribe(lambda error, source=name: self._publish_error(source, error))
            )

    def _publish_error(self, source: str, error: Any) -> None:
        if not isinstance(error, Exception):
            error = getattr(error, "error", RuntimeError(str(error)))
        if not self.errors.closed:
            self.errors.publish(MavRuntimeError(source, error))

    def __enter__(self) -> MavRuntime:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
