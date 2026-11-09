from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class MavMessageEnvelope:
    """Once-extracted routing metadata and the raw pymavlink message."""

    sequence: int
    message: Any
    message_type: str
    source_system: int | None
    source_component: int | None
    message_id: int | None
    received_monotonic: float

    @classmethod
    def wrap(cls, sequence: int, message: Any) -> MavMessageEnvelope:
        get_type = getattr(message, "get_type", None)
        if not callable(get_type):
            raise TypeError("MAVLink mesajı get_type() sağlamalı")
        message_type = str(get_type()).strip().upper()
        if not message_type:
            raise ValueError("MAVLink mesaj tipi boş olamaz")

        header = getattr(message, "_header", None)
        get_source_system = getattr(message, "get_srcSystem", None)
        source_system = (
            get_source_system()
            if callable(get_source_system)
            else getattr(header, "srcSystem", None)
        )
        get_source_component = getattr(message, "get_srcComponent", None)
        source_component = (
            get_source_component()
            if callable(get_source_component)
            else getattr(header, "srcComponent", None)
        )
        get_message_id = getattr(message, "get_msgId", None)
        message_id = get_message_id() if callable(get_message_id) else None
        return cls(
            sequence=sequence,
            message=message,
            message_type=message_type,
            source_system=int(cast(Any, source_system)) if source_system is not None else None,
            source_component=(
                int(cast(Any, source_component)) if source_component is not None else None
            ),
            message_id=int(cast(Any, message_id)) if message_id is not None else None,
            received_monotonic=time.monotonic(),
        )

    def to_dict(self, *, include_payload: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "sequence": self.sequence,
            "message_type": self.message_type,
            "source_system": self.source_system,
            "source_component": self.source_component,
            "message_id": self.message_id,
            "received_monotonic": self.received_monotonic,
        }
        if include_payload:
            serializer = getattr(self.message, "to_dict", None)
            payload["payload"] = serializer() if callable(serializer) else repr(self.message)
        return payload
