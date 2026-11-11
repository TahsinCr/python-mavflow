from __future__ import annotations

import json
import math
import re
import secrets
import struct
import threading
import time
import zlib
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from eventflow import EventBus, EventFilter, Subscription

from .client import MavClient
from .message import MavMessageEnvelope

V2_EXTENSION_PAYLOAD_SIZE = 249
DEFAULT_APPLICATION_NETWORK = 77
DEFAULT_APPLICATION_MESSAGE_TYPE = 42_000

_MAGIC = b"MA"
_VERSION = 1
_HEADER = struct.Struct("!2sBBIHHHI")
_CHUNK_SIZE = V2_EXTENSION_PAYLOAD_SIZE - _HEADER.size
_MAX_FRAGMENT_COUNT = 72
_MAX_PACKET_BYTES = _CHUNK_SIZE * _MAX_FRAGMENT_COUNT
_MAX_PACKET_TYPE_LENGTH = 96
_MAX_JSON_DEPTH = 64
_JSON_DEPTH_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|[{}\[\]]')


class MavApplicationProtocolError(ValueError):
    """Report a malformed or incompatible application-channel packet."""


def _normalize_source_id(value: int | None, name: str) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    if not 0 <= normalized <= 255:
        raise ValueError(f"{name} 0..255 aralığında olmalı")
    return normalized


def _normalize_source_ids(
    values: int | tuple[int, ...] | None,
    name: str,
) -> frozenset[int] | None:
    if values is None:
        return None
    candidates = values if isinstance(values, tuple) else (values,)
    normalized = frozenset(int(value) for value in candidates)
    if any(not 0 <= value <= 255 for value in normalized):
        raise ValueError(f"{name} 0..255 aralığında olmalı")
    return normalized


def _normalize_allowed_sources(
    values: tuple[tuple[int, int], ...] | None,
) -> frozenset[tuple[int, int]] | None:
    if values is None:
        return None
    normalized: set[tuple[int, int]] = set()
    for pair in values:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise ValueError("allowed_sources her biri (system, component) olan çiftler içermeli")
        system = _normalize_source_id(pair[0], "allowed_sources system")
        component = _normalize_source_id(pair[1], "allowed_sources component")
        if system is None or component is None:
            raise ValueError("allowed_sources kimlik çiftleri None içeremez")
        normalized.add((system, component))
    return frozenset(normalized)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON nesnesinde yinelenen anahtar: {key}")
        result[key] = value
    return result


def _validate_json_depth(body: str) -> None:
    if body.count("{") + body.count("[") <= _MAX_JSON_DEPTH:
        return

    depth = 0
    for match in _JSON_DEPTH_TOKEN.finditer(body):
        token = match.group()
        if token.startswith('"'):
            continue
        if token in "{[":
            depth += 1
            if depth > _MAX_JSON_DEPTH:
                raise MavApplicationProtocolError(
                    f"Uygulama JSON iç içe geçme sınırını aştı: {_MAX_JSON_DEPTH}"
                )
        else:
            depth -= 1
            if depth < 0:
                raise MavApplicationProtocolError("Uygulama JSON iç içe geçmesi geçersiz")


def _validate_json_unicode(value: object) -> None:
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            if current.isascii():
                continue
            if any(0xD800 <= ord(character) <= 0xDFFF for character in current):
                raise MavApplicationProtocolError(
                    "JSON strings cannot contain unpaired Unicode surrogate code points"
                )
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)


def _raise_invalid_json_constant(value: str) -> None:
    raise ValueError(f"Geçersiz JSON sabiti: {value}")


@dataclass(frozen=True, slots=True)
class MavApplicationPacket:
    """Extensible application packet independent from MAVLink transport."""

    packet_type: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    packet_id: int = field(default_factory=lambda: secrets.randbits(32) or 1)
    sent_at: float = field(default_factory=time.time)
    source_system: int | None = None
    source_component: int | None = None
    expects_response: bool = False

    @staticmethod
    def _freeze_payload_value(value: Any) -> Any:
        """Detach and recursively freeze a value stored in a packet payload."""
        if isinstance(value, Mapping):
            return MappingProxyType(
                {
                    deepcopy(key): MavApplicationPacket._freeze_payload_value(item)
                    for key, item in value.items()
                }
            )
        if isinstance(value, (tuple, list)):
            return tuple(MavApplicationPacket._freeze_payload_value(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return frozenset(MavApplicationPacket._freeze_payload_value(item) for item in value)
        return deepcopy(value)

    @staticmethod
    def _copy_payload_value(value: Any) -> Any:
        """Return a detached mutable representation of a packet payload value."""
        if isinstance(value, Mapping):
            return {
                deepcopy(key): MavApplicationPacket._copy_payload_value(item)
                for key, item in value.items()
            }
        if isinstance(value, tuple):
            return [MavApplicationPacket._copy_payload_value(item) for item in value]
        if isinstance(value, list):
            return [MavApplicationPacket._copy_payload_value(item) for item in value]
        if isinstance(value, (set, frozenset)):
            return {MavApplicationPacket._copy_payload_value(item) for item in value}
        return deepcopy(value)

    def __post_init__(self) -> None:
        normalized_type = self.packet_type.strip().lower()
        if not normalized_type:
            raise ValueError("Uygulama paket tipi boş olamaz")
        if len(normalized_type) > _MAX_PACKET_TYPE_LENGTH:
            raise ValueError(
                f"Uygulama paket tipi en fazla {_MAX_PACKET_TYPE_LENGTH} karakter olabilir"
            )
        if not 0 < int(self.packet_id) <= 0xFFFFFFFF:
            raise ValueError("Uygulama paket kimliği 1..2^32-1 aralığında olmalı")
        sent_at = float(self.sent_at)
        if not math.isfinite(sent_at) or sent_at <= 0:
            raise ValueError("Uygulama paket zamanı pozitif ve sonlu olmalı")
        if not isinstance(self.payload, Mapping):
            raise ValueError("Uygulama paket payload değeri nesne olmalı")
        payload = self._copy_payload_value(self.payload)
        try:
            json.dumps(payload, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Uygulama paket payload değeri JSON uyumlu olmalı: {exc}") from exc
        source_system = _normalize_source_id(self.source_system, "source_system")
        source_component = _normalize_source_id(
            self.source_component,
            "source_component",
        )
        object.__setattr__(self, "packet_type", normalized_type)
        object.__setattr__(self, "payload", self._freeze_payload_value(payload))
        object.__setattr__(self, "packet_id", int(self.packet_id))
        object.__setattr__(self, "sent_at", sent_at)
        object.__setattr__(self, "source_system", source_system)
        object.__setattr__(self, "source_component", source_component)
        object.__setattr__(self, "expects_response", bool(self.expects_response))

    def to_dict(self) -> dict[str, Any]:
        return {
            "packet_type": self.packet_type,
            "payload": self._copy_payload_value(self.payload),
            "packet_id": self.packet_id,
            "sent_at": self.sent_at,
            "source_system": self.source_system,
            "source_component": self.source_component,
            "expects_response": self.expects_response,
        }


@dataclass(frozen=True, slots=True)
class _Fragment:
    packet_id: int
    index: int
    count: int
    checksum: int
    chunk: bytes


@dataclass(slots=True)
class _Assembly:
    count: int
    checksum: int
    created_monotonic: float
    fragments: dict[int, bytes] = field(default_factory=dict)
    byte_count: int = 0


class MavApplicationCodec:
    """Safely fragment JSON packets across V2_EXTENSION's 249-byte payload."""

    max_packet_bytes = _MAX_PACKET_BYTES
    chunk_size = _CHUNK_SIZE

    @classmethod
    def encode(cls, packet: MavApplicationPacket) -> tuple[bytes, ...]:
        try:
            json_body = json.dumps(
                {
                    "type": packet.packet_type,
                    "payload": packet._copy_payload_value(packet.payload),
                    "sent_at": packet.sent_at,
                    "reply": packet.expects_response,
                },
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
            _validate_json_depth(json_body)
            body = json_body.encode("utf-8")
        except MavApplicationProtocolError:
            raise
        except (TypeError, ValueError) as exc:
            raise MavApplicationProtocolError(
                f"Uygulama paketi JSON olarak kodlanamadı: {exc}"
            ) from exc
        if not body:
            raise MavApplicationProtocolError("Uygulama paketi boş olamaz")
        if len(body) > cls.max_packet_bytes:
            raise MavApplicationProtocolError(
                f"Uygulama paketi çok büyük: {len(body)} > {cls.max_packet_bytes} bayt"
            )
        count = max(1, (len(body) + cls.chunk_size - 1) // cls.chunk_size)
        checksum = zlib.crc32(body) & 0xFFFFFFFF
        fragments: list[bytes] = []
        for index in range(count):
            chunk = body[index * cls.chunk_size : (index + 1) * cls.chunk_size]
            fragments.append(
                _HEADER.pack(
                    _MAGIC,
                    _VERSION,
                    0,
                    packet.packet_id,
                    index,
                    count,
                    len(chunk),
                    checksum,
                )
                + chunk
            )
        return tuple(fragments)

    @classmethod
    def decode_fragment(cls, payload: bytes | bytearray | list[int] | tuple[int, ...]) -> _Fragment:
        if len(payload) > V2_EXTENSION_PAYLOAD_SIZE:
            raise MavApplicationProtocolError(
                f"Uygulama fragmenti en fazla {V2_EXTENSION_PAYLOAD_SIZE} bayt olabilir"
            )
        raw = bytes(payload)
        if len(raw) < _HEADER.size:
            raise MavApplicationProtocolError("Uygulama fragment başlığı eksik")
        magic, version, flags, packet_id, index, count, chunk_length, checksum = (
            _HEADER.unpack_from(raw)
        )
        if magic != _MAGIC:
            raise MavApplicationProtocolError("Uygulama fragment imzası geçersiz")
        if version != _VERSION:
            raise MavApplicationProtocolError(f"Desteklenmeyen uygulama protokol sürümü: {version}")
        if flags != 0:
            raise MavApplicationProtocolError("Uygulama fragment ayrılmış flag alanı geçersiz")
        if not 0 < packet_id <= 0xFFFFFFFF:
            raise MavApplicationProtocolError("Uygulama paket kimliği geçersiz")
        if not 1 <= count <= _MAX_FRAGMENT_COUNT or index >= count:
            raise MavApplicationProtocolError("Uygulama fragment sırası geçersiz")
        if (
            chunk_length == 0
            or chunk_length > cls.chunk_size
            or _HEADER.size + chunk_length > len(raw)
            or (index < count - 1 and chunk_length != cls.chunk_size)
        ):
            raise MavApplicationProtocolError("Uygulama fragment uzunluğu geçersiz")
        if any(raw[_HEADER.size + chunk_length :]):
            raise MavApplicationProtocolError("Uygulama fragment padding alanı sıfır olmalı")
        return _Fragment(
            packet_id,
            index,
            count,
            checksum,
            raw[_HEADER.size : _HEADER.size + chunk_length],
        )

    @staticmethod
    def decode_packet(
        packet_id: int,
        body: bytes,
        *,
        source_system: int | None = None,
        source_component: int | None = None,
    ) -> MavApplicationPacket:
        if len(body) > _MAX_PACKET_BYTES:
            raise MavApplicationProtocolError(
                f"Uygulama paketi çok büyük: {len(body)} > {_MAX_PACKET_BYTES} bayt"
            )
        try:
            json_body = body.decode("utf-8")
            _validate_json_depth(json_body)
            decoded = json.loads(
                json_body,
                parse_constant=lambda value: _raise_invalid_json_constant(value),
                object_pairs_hook=_reject_duplicate_json_keys,
            )
            _validate_json_unicode(decoded)
        except MavApplicationProtocolError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
            raise MavApplicationProtocolError(f"Uygulama paket gövdesi çözülemedi: {exc}") from exc
        if not isinstance(decoded, dict):
            raise MavApplicationProtocolError("Uygulama paket gövdesi nesne olmalı")
        packet_type = decoded.get("type")
        payload = decoded.get("payload", {})
        sent_at = decoded.get("sent_at", 0.0)
        reply = decoded.get("reply", False)
        if not isinstance(packet_type, str):
            raise MavApplicationProtocolError("Uygulama paket tipi metin olmalı")
        if not isinstance(payload, dict):
            raise MavApplicationProtocolError("Uygulama paket payload değeri nesne olmalı")
        if isinstance(sent_at, bool) or not isinstance(sent_at, int | float):
            raise MavApplicationProtocolError("Uygulama paket zamanı sayısal olmalı")
        try:
            normalized_sent_at = float(sent_at)
        except (OverflowError, ValueError) as exc:
            raise MavApplicationProtocolError("Uygulama paket zamanı geçersiz") from exc
        if not math.isfinite(normalized_sent_at):
            raise MavApplicationProtocolError("Uygulama paket zamanı sonlu olmalı")
        if not isinstance(reply, bool):
            raise MavApplicationProtocolError("Uygulama reply alanı boolean olmalı")
        try:
            return MavApplicationPacket(
                packet_type,
                payload,
                packet_id,
                normalized_sent_at,
                source_system,
                source_component,
                reply,
            )
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            raise MavApplicationProtocolError(f"Uygulama paket gövdesi geçersiz: {exc}") from exc


class MavApplicationAssembler:
    """Reassemble packets and suppress recent duplicate packet IDs per source.

    Duplicate suppression is in-memory, bounded, and lasts at most
    ``fragment_timeout``; cache pressure can evict entries sooner. It is not
    durable request deduplication or an exactly-once delivery guarantee.
    """

    def __init__(
        self,
        *,
        fragment_timeout: float = 10.0,
        max_inflight_assemblies: int = 256,
        max_inflight_bytes: int = _MAX_PACKET_BYTES * 4,
        max_inflight_assemblies_per_source: int = 32,
        max_inflight_bytes_per_source: int | None = None,
        max_completed_packets: int = 1_024,
    ) -> None:
        if not math.isfinite(fragment_timeout) or fragment_timeout <= 0:
            raise ValueError("Fragment zaman aşımı pozitif ve sonlu olmalı")
        if max_inflight_assemblies <= 0:
            raise ValueError("In-flight assembly sınırı pozitif olmalı")
        if max_inflight_bytes <= 0:
            raise ValueError("In-flight byte sınırı pozitif olmalı")
        if max_inflight_assemblies_per_source <= 0:
            raise ValueError("Kaynak başına in-flight assembly sınırı pozitif olmalı")
        if max_completed_packets <= 0:
            raise ValueError("Tamamlanan paket geçmişi sınırı pozitif olmalı")
        source_byte_limit = (
            min(int(max_inflight_bytes), _MAX_PACKET_BYTES)
            if max_inflight_bytes_per_source is None
            else int(max_inflight_bytes_per_source)
        )
        if source_byte_limit <= 0:
            raise ValueError("Kaynak başına in-flight byte sınırı pozitif olmalı")
        self._fragment_timeout = float(fragment_timeout)
        self._max_inflight_assemblies = int(max_inflight_assemblies)
        self._max_inflight_bytes = int(max_inflight_bytes)
        self._max_inflight_assemblies_per_source = int(max_inflight_assemblies_per_source)
        self._max_inflight_bytes_per_source = source_byte_limit
        self._max_completed_packets = int(max_completed_packets)
        self._assemblies: dict[tuple[int, int, int], _Assembly] = {}
        self._completed: dict[tuple[int, int, int], float] = {}
        self._inflight_bytes = 0
        self._source_assembly_counts: dict[tuple[int, int], int] = {}
        self._source_inflight_bytes: dict[tuple[int, int], int] = {}
        self._next_prune_at = 0.0
        self._lock = threading.RLock()

    def accept(
        self,
        payload: bytes | bytearray | list[int] | tuple[int, ...],
        *,
        source_system: int | None = None,
        source_component: int | None = None,
    ) -> MavApplicationPacket | None:
        fragment = MavApplicationCodec.decode_fragment(payload)
        source_key = (source_system or 0, source_component or 0)
        key = (*source_key, fragment.packet_id)
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            if key in self._completed:
                return None
            assembly = self._assemblies.get(key)
            if assembly is None:
                if len(self._assemblies) >= self._max_inflight_assemblies:
                    raise MavApplicationProtocolError("Çok fazla tamamlanmamış uygulama paketi var")
                if (
                    self._source_assembly_counts.get(source_key, 0)
                    >= self._max_inflight_assemblies_per_source
                ):
                    raise MavApplicationProtocolError(
                        "Bir MAVLink kaynağı çok fazla tamamlanmamış uygulama paketi açtı"
                    )
                assembly = _Assembly(fragment.count, fragment.checksum, now)
                self._assemblies[key] = assembly
                self._source_assembly_counts[source_key] = (
                    self._source_assembly_counts.get(source_key, 0) + 1
                )
            elif assembly.count != fragment.count or assembly.checksum != fragment.checksum:
                self._discard_assembly(key)
                raise MavApplicationProtocolError(
                    "Aynı kimlikte uyuşmayan uygulama fragmentleri alındı"
                )
            previous = assembly.fragments.get(fragment.index)
            if previous is not None and previous != fragment.chunk:
                self._discard_assembly(key)
                raise MavApplicationProtocolError("Tekrarlanan fragment içeriği uyuşmuyor")
            if previous is None:
                if self._inflight_bytes + len(fragment.chunk) > self._max_inflight_bytes:
                    self._discard_assembly(key)
                    raise MavApplicationProtocolError(
                        "Tamamlanmamış uygulama paketleri byte sınırını aştı"
                    )
                if (
                    self._source_inflight_bytes.get(source_key, 0) + len(fragment.chunk)
                    > self._max_inflight_bytes_per_source
                ):
                    self._discard_assembly(key)
                    raise MavApplicationProtocolError(
                        "Bir MAVLink kaynağı in-flight byte sınırını aştı"
                    )
                assembly.fragments[fragment.index] = fragment.chunk
                assembly.byte_count += len(fragment.chunk)
                self._inflight_bytes += len(fragment.chunk)
                self._source_inflight_bytes[source_key] = self._source_inflight_bytes.get(
                    source_key, 0
                ) + len(fragment.chunk)
            if len(assembly.fragments) != assembly.count:
                return None
            body = b"".join(assembly.fragments[index] for index in range(assembly.count))
            self._discard_assembly(key)
            if zlib.crc32(body) & 0xFFFFFFFF != assembly.checksum:
                raise MavApplicationProtocolError("Uygulama paketi CRC doğrulamasını geçemedi")
            packet = MavApplicationCodec.decode_packet(
                fragment.packet_id,
                body,
                source_system=source_system,
                source_component=source_component,
            )
            self._completed[key] = now
            while len(self._completed) > self._max_completed_packets:
                self._completed.pop(next(iter(self._completed)))
            return packet

    def clear(self) -> None:
        with self._lock:
            self._assemblies.clear()
            self._completed.clear()
            self._inflight_bytes = 0
            self._source_assembly_counts.clear()
            self._source_inflight_bytes.clear()
            self._next_prune_at = 0.0

    def _discard_assembly(self, key: tuple[int, int, int]) -> None:
        assembly = self._assemblies.pop(key, None)
        if assembly is not None:
            self._inflight_bytes -= assembly.byte_count
            source_key = (key[0], key[1])
            source_count = self._source_assembly_counts[source_key] - 1
            if source_count:
                self._source_assembly_counts[source_key] = source_count
            else:
                self._source_assembly_counts.pop(source_key)
            source_bytes = self._source_inflight_bytes.get(source_key, 0) - assembly.byte_count
            if source_bytes:
                self._source_inflight_bytes[source_key] = source_bytes
            else:
                self._source_inflight_bytes.pop(source_key, None)

    def _prune(self, now: float) -> None:
        if now < self._next_prune_at:
            return
        deadline = now - self._fragment_timeout
        expired_assemblies = tuple(
            key
            for key, assembly in self._assemblies.items()
            if assembly.created_monotonic < deadline
        )
        for key in expired_assemblies:
            self._discard_assembly(key)
        expired_completed = tuple(
            key for key, completed_at in self._completed.items() if completed_at < deadline
        )
        for key in expired_completed:
            self._completed.pop(key, None)
        self._next_prune_at = now + min(self._fragment_timeout, 1.0)


class MavApplicationChannel:
    """Targeted bidirectional application channel over an existing MAVLink router.

    It opens no additional serial reader and shares the MAVLink connection and
    V2_EXTENSION target system/component fields with flight messages.
    """

    def __init__(
        self,
        client: MavClient,
        *,
        network_id: int = DEFAULT_APPLICATION_NETWORK,
        message_type: int = DEFAULT_APPLICATION_MESSAGE_TYPE,
        target_system: int = 0,
        target_component: int = 0,
        local_system: int | None = None,
        local_component: int | None = None,
        source_systems: int | tuple[int, ...] | None = None,
        source_components: int | tuple[int, ...] | None = None,
        allowed_sources: tuple[tuple[int, int], ...] | None = None,
        fragment_timeout: float = 10.0,
        max_inflight_assemblies: int = 256,
        max_inflight_bytes: int = _MAX_PACKET_BYTES * 4,
        max_inflight_assemblies_per_source: int = 32,
        max_inflight_bytes_per_source: int | None = None,
        max_completed_packets: int = 1_024,
    ) -> None:
        for name, value, maximum in (
            ("network_id", network_id, 255),
            ("target_system", target_system, 255),
            ("target_component", target_component, 255),
        ):
            if not 0 <= int(value) <= maximum:
                raise ValueError(f"{name} 0..{maximum} aralığında olmalı")
        if not 0 <= int(message_type) <= 65535:
            raise ValueError("V2_EXTENSION message_type 0..65535 aralığında olmalı")
        self._client = client
        self.network_id = int(network_id)
        self.message_type = int(message_type)
        self.target_system = int(target_system)
        self.target_component = int(target_component)
        self._local_system_override = _normalize_source_id(local_system, "local_system")
        self._local_component_override = _normalize_source_id(local_component, "local_component")
        self.packets = EventBus[MavApplicationPacket]()
        self.errors = EventBus[Exception]()
        self._assembler = MavApplicationAssembler(
            fragment_timeout=fragment_timeout,
            max_inflight_assemblies=max_inflight_assemblies,
            max_inflight_bytes=max_inflight_bytes,
            max_inflight_assemblies_per_source=max_inflight_assemblies_per_source,
            max_inflight_bytes_per_source=max_inflight_bytes_per_source,
            max_completed_packets=max_completed_packets,
        )
        self._source_systems = _normalize_source_ids(source_systems, "source_systems")
        self._source_components = _normalize_source_ids(source_components, "source_components")
        self._allowed_sources = _normalize_allowed_sources(allowed_sources)
        self._subscription: Subscription | None = None
        self._running = False
        self._lock = threading.RLock()

    @property
    def local_system(self) -> int:
        if self._local_system_override is not None:
            return self._local_system_override
        return self._client.endpoint.source_system

    @property
    def local_component(self) -> int:
        if self._local_component_override is not None:
            return self._local_component_override
        return self._client.endpoint.source_component

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._assembler.clear()
            self._running = True
            try:
                self._subscription = self._client.messages.subscribe(
                    self._on_message,
                    event_filter=EventFilter(predicate=self._matches_channel),
                )
            except Exception:
                self._running = False
                raise

    def stop(self) -> None:
        with self._lock:
            self._running = False
            subscription = self._subscription
            self._subscription = None
            if subscription is not None:
                subscription.cancel()
            self._assembler.clear()

    def send(
        self,
        packet_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        packet_id: int | None = None,
        target_system: int | None = None,
        target_component: int | None = None,
        expects_response: bool = False,
    ) -> MavApplicationPacket:
        packet = MavApplicationPacket(
            packet_type,
            dict(payload or {}),
            packet_id if packet_id is not None else (secrets.randbits(32) or 1),
            expects_response=expects_response,
        )
        system = self.target_system if target_system is None else int(target_system)
        component = self.target_component if target_component is None else int(target_component)
        if not 0 <= system <= 255 or not 0 <= component <= 255:
            raise ValueError("Hedef system/component 0..255 aralığında olmalı")
        for fragment in MavApplicationCodec.encode(packet):
            padded = list(fragment) + [0] * (V2_EXTENSION_PAYLOAD_SIZE - len(fragment))
            self._client.call_mav(
                "v2_extension_send",
                self.network_id,
                system,
                component,
                self.message_type,
                padded,
            )
        return packet

    def _matches_channel(self, envelope: MavMessageEnvelope) -> bool:
        if envelope.message_type != "V2_EXTENSION":
            return False
        message = envelope.message
        try:
            if (
                self._source_systems is not None
                and envelope.source_system not in self._source_systems
            ):
                return False
            if (
                self._source_components is not None
                and envelope.source_component not in self._source_components
            ):
                return False
            if (
                self._allowed_sources is not None
                and (
                    envelope.source_system or 0,
                    envelope.source_component or 0,
                )
                not in self._allowed_sources
            ):
                return False
            if int(message.target_network) != self.network_id:
                return False
            if int(message.message_type) != self.message_type:
                return False
            if int(message.target_system) not in {0, self.local_system}:
                return False
            return int(message.target_component) in {0, self.local_component}
        except (AttributeError, TypeError, ValueError):
            return False

    def _on_message(self, envelope: MavMessageEnvelope) -> None:
        with self._lock:
            if not self._running:
                return
            message = envelope.message
            try:
                packet = self._assembler.accept(
                    message.payload,
                    source_system=envelope.source_system,
                    source_component=envelope.source_component,
                )
            except Exception as exc:
                errors = (exc,)
            else:
                errors = self.packets.publish(packet).errors if packet is not None else ()
            for error in errors:
                self.errors.publish(error)
