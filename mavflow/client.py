from __future__ import annotations

from typing import Any

from eventflow import EventBus
from eventflow.engine import EventEngine

from .connection import MavConnection
from .endpoint import MavEndpoint
from .router import MavMessageRouter


class MavClient:
    """Combine connection and router lifecycles behind one modular interface."""

    def __init__(
        self,
        endpoint: MavEndpoint | None = None,
        *,
        connection: MavConnection | None = None,
        router: MavMessageRouter | None = None,
    ) -> None:
        if connection is None and router is not None:
            connection = router.connection
        if connection is None:
            connection = MavConnection(endpoint or MavEndpoint())
        elif endpoint is not None and connection.endpoint != endpoint:
            raise ValueError("MavClient endpoint ve connection endpoint uyuşmuyor")
        if router is not None and router.connection is not connection:
            raise ValueError("MavClient router farklı bir connection kullanıyor")
        self.connection = connection
        self.router = router or MavMessageRouter(connection)
        self.events = EventEngine()
        self.events.add("mavlink.messages", self.router.messages)
        self.events.add("mavlink.connection.errors", self.connection.errors)
        self.events.add("mavlink.router.errors", self.router.errors)

    @property
    def endpoint(self) -> MavEndpoint:
        return self.connection.endpoint

    @property
    def raw(self) -> Any:
        return self.connection.raw

    @property
    def is_connected(self) -> bool:
        return self.connection.is_connected

    @property
    def mavlink(self) -> Any:
        return self.connection.mavlink

    @property
    def messages(self) -> EventBus[Any]:
        return self.router.messages

    def start(self) -> None:
        self.router.start()

    def stop(self) -> None:
        self.router.stop()

    def configure_endpoint(self, endpoint: MavEndpoint) -> None:
        """Change transport settings of a closed client for its next connection."""
        if self.router.running or self.connection.is_connected:
            raise RuntimeError("Çalışan MAVLink istemcisi yeniden yapılandırılamaz")
        self.connection.endpoint = endpoint

    close = stop

    def send(self, message: Any) -> None:
        self.connection.send(message)

    def send_named(self, message_name: str, **parameters: Any) -> None:
        self.connection.send_named(message_name, **parameters)

    def request_message_rate(
        self,
        message_id: int,
        frequency_hz: float,
        *,
        target_system: int | None = None,
        target_component: int | None = None,
    ) -> int:
        return self.connection.request_message_rate(
            message_id,
            frequency_hz,
            target_system=target_system,
            target_component=target_component,
        )

    def call_mav(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        return self.connection.call_mav(method_name, *args, **kwargs)

    def call_raw(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        return self.connection.call_raw(method_name, *args, **kwargs)

    def __enter__(self) -> MavClient:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()
