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
from .message import MavMessageEnvelope
from .peer import MavApplicationPeer, MavApplicationResponse
from .router import MavRouterStats


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
            peer_config = dict(peer_options or {})
            peer_config.setdefault(
                "target_system",
                self.client.endpoint.expected_system
                if self.client.endpoint.expected_system is not None
                else lambda: self.connection.target_system,
            )
            peer_config.setdefault(
                "target_component",
                self.client.endpoint.expected_component
                if self.client.endpoint.expected_component is not None
                else lambda: self.connection.target_component,
            )
            peer_config.setdefault("transport_available", lambda: self.client.is_connected)
            if channel is None:
                resolved_channel_options = dict(channel_options or {})
                for channel_key, peer_key in (
                    ("source_systems", "target_system"),
                    ("source_components", "target_component"),
                ):
                    peer_target = peer_config[peer_key]
                    if (
                        channel_key not in resolved_channel_options
                        and isinstance(peer_target, int)
                        and not isinstance(peer_target, bool)
                        and peer_target != 0
                    ):
                        resolved_channel_options[channel_key] = peer_target
                channel = MavApplicationChannel(self.client, **resolved_channel_options)
            peer = MavApplicationPeer(channel, role=role, **peer_config)
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

        self.events = self.client.events
        if self.channel is not None:
            self.events.add("mavlink.application.errors", self.channel.errors)
        if self.peer is not None:
            self.events.add("mavlink.application.packets", self.peer.packets)
            self.events.add("mavlink.peer.changed", self.peer.changed)
            self.events.add("mavlink.peer.errors", self.peer.errors)
        elif self.channel is not None:
            self.events.add("mavlink.application.packets", self.channel.packets)
        if self.dispatcher is not None:
            self.events.add("mavlink.dispatcher.handled", self.dispatcher.handled)
            self.events.add("mavlink.dispatcher.unhandled", self.dispatcher.unhandled)
            self.events.add("mavlink.dispatcher.errors", self.dispatcher.errors)
        self._running = False
        self._cleanup_pending = False
        self._closed = False
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def application_enabled(self) -> bool:
        return self.peer is not None and self.dispatcher is not None

    @property
    def messages(self) -> EventBus[MavMessageEnvelope]:
        return self.router.messages

    @property
    def packets(self) -> EventBus[MavApplicationPacket]:
        if self.peer is not None:
            return self.peer.packets
        if self.channel is not None:
            return self.channel.packets
        raise RuntimeError("MAVLink application channel is not configured")

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

        with self._lifecycle_lock:
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
            except Exception:
                cleanup_failed = False
                for stop_component in reversed(started_stops):
                    try:
                        stop_component()
                    except Exception:
                        cleanup_failed = True
                with self._lock:
                    self._cleanup_pending = cleanup_failed
                raise
            with self._lock:
                self._running = True

    def stop(self) -> None:
        """Stop every owned layer in reverse order and preserve all errors."""

        with self._lifecycle_lock:
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
            if errors:
                raise ExceptionGroup("MAVLink runtime shutdown failed", errors)
            with self._lock:
                self._cleanup_pending = False

    def close(self) -> None:
        """Stop owned services and close the EventFlow channel engine."""

        with self._lifecycle_lock:
            with self._lock:
                if self._closed:
                    return
            self.stop()
            self.events.stop()
            with self._lock:
                self._closed = True

    def reconnect(self) -> None:
        with self._lifecycle_lock:
            self.stop()
            self.start()

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

    def __enter__(self) -> MavRuntime:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
