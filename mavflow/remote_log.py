from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from ._compatibility import StrEnum

REMOTE_LOG_PACKET_TYPE = "logs.push"
REMOTE_LOG_PROTOCOL_VERSION = 1
REMOTE_LOG_MAX_BATCH_RECORDS = 16
REMOTE_LOG_MAX_BATCH_BYTES = 12_000
REMOTE_LOG_MAX_DETAILS_BYTES = 3_000

_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{4,96}$")


class MavRemoteLogLevel(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    @property
    def priority(self) -> int:
        return {
            MavRemoteLogLevel.DEBUG: 10,
            MavRemoteLogLevel.INFO: 20,
            MavRemoteLogLevel.SUCCESS: 25,
            MavRemoteLogLevel.WARNING: 30,
            MavRemoteLogLevel.ERROR: 40,
            MavRemoteLogLevel.CRITICAL: 50,
        }[self]


@dataclass(frozen=True, slots=True)
class MavRemoteLogRecord:
    """One transport-safe structured log emitted by an onboard application."""

    sequence: int
    source: str
    action: str
    message: str
    level: MavRemoteLogLevel = MavRemoteLogLevel.INFO
    emitted_at: float = field(default_factory=time.time)
    details: Mapping[str, Any] = field(default_factory=dict)
    device_id: str | None = None
    correlation_id: str | None = None

    @staticmethod
    def _freeze_details_value(value: Any) -> Any:
        """Detach and recursively freeze a value stored in log details."""
        if isinstance(value, Mapping):
            return MappingProxyType(
                {
                    deepcopy(key): MavRemoteLogRecord._freeze_details_value(item)
                    for key, item in value.items()
                }
            )
        if isinstance(value, (tuple, list)):
            return tuple(MavRemoteLogRecord._freeze_details_value(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return frozenset(MavRemoteLogRecord._freeze_details_value(item) for item in value)
        return deepcopy(value)

    @staticmethod
    def _copy_details_value(value: Any) -> Any:
        """Return a detached mutable representation of a log details value."""
        if isinstance(value, Mapping):
            return {
                deepcopy(key): MavRemoteLogRecord._copy_details_value(item)
                for key, item in value.items()
            }
        if isinstance(value, tuple):
            return [MavRemoteLogRecord._copy_details_value(item) for item in value]
        if isinstance(value, list):
            return [MavRemoteLogRecord._copy_details_value(item) for item in value]
        if isinstance(value, (set, frozenset)):
            return {MavRemoteLogRecord._copy_details_value(item) for item in value}
        return deepcopy(value)

    def __post_init__(self) -> None:
        sequence = int(self.sequence)
        source = str(self.source).strip().upper()
        action = str(self.action).strip().lower()
        message = str(self.message).strip()
        emitted_at = float(self.emitted_at)
        details = self._copy_details_value(self.details)
        if sequence <= 0:
            raise ValueError("Remote log sequence must be positive")
        if not source or len(source) > 64:
            raise ValueError("Remote log source must contain 1..64 characters")
        if not action or len(action) > 96:
            raise ValueError("Remote log action must contain 1..96 characters")
        if not message or len(message) > 1_024:
            raise ValueError("Remote log message must contain 1..1024 characters")
        if not math.isfinite(emitted_at) or emitted_at <= 0:
            raise ValueError("Remote log timestamp must be a positive finite value")
        try:
            details_json = json.dumps(
                details,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Remote log details must be JSON compatible: {exc}") from exc
        if len(details_json) > REMOTE_LOG_MAX_DETAILS_BYTES:
            raise ValueError(
                f"Remote log details exceed {REMOTE_LOG_MAX_DETAILS_BYTES} encoded bytes"
            )
        device_id = _optional_text(self.device_id, maximum=96)
        correlation_id = _optional_text(self.correlation_id, maximum=128)
        object.__setattr__(self, "sequence", sequence)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "message", message)
        object.__setattr__(self, "level", MavRemoteLogLevel(self.level))
        object.__setattr__(self, "emitted_at", emitted_at)
        object.__setattr__(self, "details", self._freeze_details_value(details))
        object.__setattr__(self, "device_id", device_id)
        object.__setattr__(self, "correlation_id", correlation_id)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "seq": self.sequence,
            "source": self.source,
            "action": self.action,
            "message": self.message,
            "level": self.level.value,
            "at": self.emitted_at,
            "details": self._copy_details_value(self.details),
        }
        if self.device_id is not None:
            payload["device_id"] = self.device_id
        if self.correlation_id is not None:
            payload["correlation_id"] = self.correlation_id
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> MavRemoteLogRecord:
        if not isinstance(payload, Mapping):
            raise ValueError("Remote log record payload must be an object")
        details = payload.get("details", {})
        if not isinstance(details, Mapping):
            raise ValueError("Remote log details must be an object")
        return cls(
            sequence=int(payload.get("seq", 0)),
            source=str(payload.get("source", "")),
            action=str(payload.get("action", "")),
            message=str(payload.get("message", "")),
            level=MavRemoteLogLevel(str(payload.get("level", "info")).lower()),
            emitted_at=float(payload.get("at", 0.0)),
            details=dict(details),
            device_id=_payload_optional_text(payload.get("device_id")),
            correlation_id=_payload_optional_text(payload.get("correlation_id")),
        )


@dataclass(frozen=True, slots=True)
class MavRemoteLogBatch:
    """An ordered log batch acknowledged as one application request."""

    session_id: str
    records: tuple[MavRemoteLogRecord, ...]
    created_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        session_id = str(self.session_id).strip()
        records = tuple(self.records)
        created_at = float(self.created_at)
        if not _SESSION_ID_PATTERN.fullmatch(session_id):
            raise ValueError("Remote log session ID is invalid")
        if not 1 <= len(records) <= REMOTE_LOG_MAX_BATCH_RECORDS:
            raise ValueError(
                f"Remote log batch must contain 1..{REMOTE_LOG_MAX_BATCH_RECORDS} records"
            )
        if any(
            current.sequence >= following.sequence
            for current, following in zip(records, records[1:], strict=False)
        ):
            raise ValueError("Remote log records must be ordered by unique sequence")
        if not math.isfinite(created_at) or created_at <= 0:
            raise ValueError("Remote log batch timestamp must be positive")
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "records", records)
        object.__setattr__(self, "created_at", created_at)
        payload_size = len(
            json.dumps(
                self.to_payload(),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        if payload_size > REMOTE_LOG_MAX_BATCH_BYTES:
            raise ValueError(f"Remote log batch exceeds {REMOTE_LOG_MAX_BATCH_BYTES} encoded bytes")

    @property
    def first_sequence(self) -> int:
        return self.records[0].sequence

    @property
    def last_sequence(self) -> int:
        return self.records[-1].sequence

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": REMOTE_LOG_PROTOCOL_VERSION,
            "session": self.session_id,
            "created_at": self.created_at,
            "records": [record.to_payload() for record in self.records],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> MavRemoteLogBatch:
        if not isinstance(payload, Mapping):
            raise ValueError("Remote log batch payload must be an object")
        version = int(payload.get("version", 0))
        if version != REMOTE_LOG_PROTOCOL_VERSION:
            raise ValueError(f"Unsupported remote log protocol version: {version}")
        values = payload.get("records")
        if not isinstance(values, list):
            raise ValueError("Remote log records must be a list")
        if len(values) > REMOTE_LOG_MAX_BATCH_RECORDS:
            raise ValueError(
                f"Remote log batch cannot contain more than {REMOTE_LOG_MAX_BATCH_RECORDS} records"
            )
        return cls(
            session_id=str(payload.get("session", "")),
            records=tuple(MavRemoteLogRecord.from_payload(value) for value in values),
            created_at=float(payload.get("created_at", 0.0)),
        )


def _optional_text(value: object, *, maximum: int) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if len(normalized) > maximum:
        raise ValueError(f"Remote log metadata exceeds {maximum} characters")
    return normalized


def _payload_optional_text(value: object) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "MavRemoteLogBatch",
    "MavRemoteLogLevel",
    "MavRemoteLogRecord",
    "REMOTE_LOG_MAX_BATCH_BYTES",
    "REMOTE_LOG_MAX_BATCH_RECORDS",
    "REMOTE_LOG_MAX_DETAILS_BYTES",
    "REMOTE_LOG_PACKET_TYPE",
    "REMOTE_LOG_PROTOCOL_VERSION",
]
