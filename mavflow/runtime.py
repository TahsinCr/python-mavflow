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
        if peer is not None:
            peer_channel = getattr(peer, "_channel", None)
            if peer_channel is not None:
                if channel is None:
                    channel = peer_channel
                elif channel is not peer_channel:
                    raise ValueError("MAVLink runtime peer and channel do not match")
        if client is None:
            client = MavClient(endpoint or MavEndpoint()) if channel is None else channel._client
        if endpoint is not None and client.endpoint != endpoint:
            raise ValueError("MAVLink runtime endpoint and client endpoint do not match")
        if channel is not None and getattr(channel, "_client", client) is not client:
            raise ValueError("MAVLink runtime channel uses a different client")
        self.client = client
        self.connection = self.client.connection
        self.router = self.client.router
        self._endpoint_peer_identity: dict[str, int] = {}

        role = str(application_role).strip().lower() if application_role else ""
        if channel is not None and peer is None and not role:
            raise ValueError("A custom application channel requires a role or peer")
        if dispatcher is not None and peer is None:
            raise ValueError("A custom dispatcher requires an application peer")
        if dispatcher is not None and getattr(dispatcher, "_peer", peer) is not peer:
            raise ValueError("MAVLink runtime dispatcher and peer do not match")

        if peer is None and role:
            peer_config = dict(peer_options or {})
            for endpoint_field, peer_field in (
                ("expected_system", "target_system"),
                ("expected_component", "target_component"),
            ):
                expected_identity = getattr(self.client.endpoint, endpoint_field)
                if peer_field not in peer_config and expected_identity is not None:
                    self._endpoint_peer_identity[endpoint_field] = expected_identity
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
        self._starting_thread: threading.Thread | None = None

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

        self._reject_managed_thread_lifecycle()
        with self._lifecycle_lock:
            with self._lock:
                if self._closed:
                    raise RuntimeError("MAVLink runtime is closed")
                if self._cleanup_pending:
                    raise RuntimeError("MAVLink runtime cleanup must complete before restart")
                if self._running:
                    return
            self._starting_thread = threading.current_thread()
            cleanup_callbacks: list[Callable[[], None]] = []
            try:
                with self.client._lifecycle_lock:
                    for endpoint_field, expected_identity in self._endpoint_peer_identity.items():
                        if getattr(self.client.endpoint, endpoint_field) != expected_identity:
                            raise RuntimeError(
                                "MAVLink endpoint peer identity changed after runtime creation; "
                                "create a new runtime for the new peer"
                            )
                    cleanup_callbacks.append(self.client.stop)
                    self.client.start()
                if self.peer is not None:
                    cleanup_callbacks.append(self.peer.stop)
                    self.peer.start()
                elif self.channel is not None:
                    cleanup_callbacks.append(self.channel.stop)
                    self.channel.start()
                if self.dispatcher is not None:
                    self.dispatcher.start()
                    cleanup_callbacks.append(self.dispatcher.stop)
            except Exception:
                cleanup_failed = False
                for stop_component in reversed(cleanup_callbacks):
                    try:
                        stop_component()
                    except Exception:
                        cleanup_failed = True
                with self._lock:
                    self._cleanup_pending = cleanup_failed
                raise
            else:
                with self._lock:
                    self._running = True
            finally:
                self._starting_thread = None

    def _reject_managed_thread_lifecycle(self) -> None:
        current_thread = threading.current_thread()
        if self._starting_thread is current_thread:
            raise RuntimeError("MAVLink runtime lifecycle is not allowed from a startup callback")
        if getattr(self.client, "_starting_thread", None) is current_thread:
            raise RuntimeError(
                "MAVLink runtime lifecycle is not allowed from a client startup callback"
            )
        if getattr(self.router, "_thread", None) is current_thread:
            raise RuntimeError(
                "MAVLink runtime lifecycle is not allowed from its router receive thread"
            )
        if getattr(self.peer, "_monitor_thread", None) is current_thread:
            raise RuntimeError(
                "MAVLink runtime lifecycle is not allowed from its peer monitor thread"
            )
        worker_context = getattr(self.dispatcher, "_worker_context", None)
        if worker_context is not None and getattr(worker_context, "active", False):
            raise RuntimeError(
                "MAVLink runtime lifecycle is not allowed from a dispatcher worker thread"
            )

    def stop(self) -> None:
        """Stop owned layers; lifecycle calls from managed callbacks are rejected."""

        self._reject_managed_thread_lifecycle()
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

        self._reject_managed_thread_lifecycle()
        with self._lifecycle_lock:
            with self._lock:
                if self._closed:
                    return
            self.stop()
            self.events.stop()
            with self._lock:
                self._closed = True

    def reconnect(self) -> None:
        self._reject_managed_thread_lifecycle()
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
