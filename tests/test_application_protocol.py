from __future__ import annotations

import math
import threading
import time
import unittest
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from eventflow import EventBus

from mavflow import application as application_module
from mavflow.application import (
    MavApplicationAssembler,
    MavApplicationChannel,
    MavApplicationCodec,
    MavApplicationPacket,
    MavApplicationProtocolError,
)
from mavflow.dispatch import MavApplicationResult
from mavflow.endpoint import MavEndpoint
from mavflow.message import MavMessageEnvelope


class ApplicationPacketTests(unittest.TestCase):
    def test_payload_is_detached_and_serializes_to_a_copy(self) -> None:
        original = {"nested": {"values": [1, 2]}}
        packet = MavApplicationPacket("Camera.Capture", original)
        original["nested"]["values"].append(3)  # type: ignore[index,union-attr]

        self.assertEqual(packet.packet_type, "camera.capture")
        self.assertIsInstance(packet.payload, Mapping)
        self.assertEqual(packet.to_dict()["payload"], {"nested": {"values": [1, 2]}})
        serialized = packet.to_dict()
        serialized["payload"]["nested"]["values"].append(4)
        self.assertEqual(packet.to_dict()["payload"], {"nested": {"values": [1, 2]}})
        with self.assertRaises(TypeError):
            packet.payload["new"] = True  # type: ignore[index]
        with self.assertRaises(AttributeError):
            packet.payload["nested"]["values"].append(4)  # type: ignore[union-attr]
        copied = MavApplicationPacket("copy", packet.payload)
        self.assertEqual(copied.to_dict()["payload"], packet.to_dict()["payload"])

    def test_application_result_payload_is_detached(self) -> None:
        original = {"nested": {"accepted": True}}
        result = MavApplicationResult.success(original)
        original["nested"]["accepted"] = False

        self.assertTrue(result.payload["nested"]["accepted"])
        with self.assertRaises(TypeError):
            result.payload["nested"] = {}  # type: ignore[index]

    def test_packet_validates_time_sources_type_and_json_payload(self) -> None:
        for sent_at in (0.0, -1.0, math.nan, math.inf, -math.inf):
            with self.subTest(sent_at=sent_at), self.assertRaises(ValueError):
                MavApplicationPacket("status", sent_at=sent_at)

        for source in (-1, 256):
            with self.subTest(source_system=source), self.assertRaises(ValueError):
                MavApplicationPacket("status", source_system=source)
            with self.subTest(source_component=source), self.assertRaises(ValueError):
                MavApplicationPacket("status", source_component=source)

        with self.assertRaises(ValueError):
            MavApplicationPacket("x" * 97)
        with self.assertRaises(ValueError):
            MavApplicationPacket("status", payload={"value": math.nan})
        with self.assertRaises(ValueError):
            MavApplicationPacket("status", payload={"value": object()})

    def test_packet_rejects_invalid_identity_and_payload_shape(self) -> None:
        for packet_type, packet_id, payload in (
            ("  ", 1, {}),
            ("status", 0, {}),
            ("status", -1, {}),
            ("status", 0x1_0000_0000, {}),
            ("status", 1, []),
        ):
            with self.subTest(packet_type=packet_type, packet_id=packet_id, payload=payload):
                with self.assertRaises(ValueError):
                    MavApplicationPacket(packet_type, payload, packet_id=packet_id)  # type: ignore[arg-type]


class ApplicationCodecTests(unittest.TestCase):
    def test_single_fragment_round_trip(self) -> None:
        packet = MavApplicationPacket("status.update", {"ready": True}, packet_id=7)
        fragments = MavApplicationCodec.encode(packet)

        self.assertEqual(len(fragments), 1)
        decoded = MavApplicationAssembler().accept(
            fragments[0], source_system=4, source_component=191
        )
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.packet_id, 7)
        self.assertEqual(decoded.payload, {"ready": True})
        self.assertEqual(decoded.source_system, 4)
        self.assertEqual(decoded.source_component, 191)

    def test_multiple_out_of_order_fragments_round_trip(self) -> None:
        packet = MavApplicationPacket("data.large", {"text": "x" * 2_000})
        fragments = MavApplicationCodec.encode(packet)
        assembler = MavApplicationAssembler()
        decoded = None

        for fragment in reversed(fragments):
            decoded = assembler.accept(fragment)

        self.assertGreater(len(fragments), 1)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.payload["text"], "x" * 2_000)

    def test_identical_duplicate_fragment_is_idempotent(self) -> None:
        packet = MavApplicationPacket("data.large", {"text": "x" * 1_000})
        fragments = MavApplicationCodec.encode(packet)
        assembler = MavApplicationAssembler()

        self.assertIsNone(assembler.accept(fragments[0]))
        self.assertIsNone(assembler.accept(fragments[0]))
        decoded = None
        for fragment in fragments[1:]:
            decoded = assembler.accept(fragment)

        self.assertIsNotNone(decoded)
        self.assertIsNone(assembler.accept(fragments[-1]))

    def test_conflicting_duplicate_fragment_is_rejected(self) -> None:
        packet = MavApplicationPacket("data.large", {"text": "x" * 1_000})
        first = MavApplicationCodec.encode(packet)[0]
        conflicting = bytearray(first)
        conflicting[-1] ^= 0x01
        assembler = MavApplicationAssembler()
        assembler.accept(first)

        with self.assertRaisesRegex(MavApplicationProtocolError, "Tekrarlanan"):
            assembler.accept(conflicting)

    def test_crc_mismatch_is_rejected(self) -> None:
        packet = MavApplicationPacket("status", {"ready": True})
        corrupted = bytearray(MavApplicationCodec.encode(packet)[0])
        corrupted[-1] ^= 0x01

        with self.assertRaisesRegex(MavApplicationProtocolError, "CRC"):
            MavApplicationAssembler().accept(corrupted)

    def test_fragment_timeout_discards_incomplete_assembly(self) -> None:
        packet = MavApplicationPacket("data.large", {"text": "x" * 1_000})
        fragments = MavApplicationCodec.encode(packet)
        assembler = MavApplicationAssembler(fragment_timeout=0.01)
        assembler.accept(fragments[0])
        key = (0, 0, packet.packet_id)
        first_created = assembler._assemblies[key].created_monotonic

        time.sleep(0.02)
        self.assertIsNone(assembler.accept(fragments[1]))

        self.assertGreater(assembler._assemblies[key].created_monotonic, first_created)
        self.assertNotIn(0, assembler._assemblies[key].fragments)

    def test_assembler_rejects_nonfinite_fragment_timeouts(self) -> None:
        for timeout in (math.nan, math.inf, -math.inf):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                MavApplicationAssembler(fragment_timeout=timeout)

    def test_maximum_encoded_packet_boundary(self) -> None:
        low = 0
        high = MavApplicationCodec.max_packet_bytes * 2
        while low + 1 < high:
            middle = (low + high) // 2
            packet = MavApplicationPacket(
                "data",
                {"text": "x" * middle},
                packet_id=1,
                sent_at=1.0,
            )
            try:
                MavApplicationCodec.encode(packet)
            except MavApplicationProtocolError:
                high = middle
            else:
                low = middle

        accepted = MavApplicationPacket("data", {"text": "x" * low}, packet_id=1, sent_at=1.0)
        rejected = MavApplicationPacket("data", {"text": "x" * high}, packet_id=1, sent_at=1.0)
        self.assertLessEqual(
            len(MavApplicationCodec.encode(accepted)),
            72,
        )
        with self.assertRaisesRegex(MavApplicationProtocolError, "çok büyük"):
            MavApplicationCodec.encode(rejected)

    def test_malformed_json_and_nonfinite_constant_are_rejected(self) -> None:
        with self.assertRaises(MavApplicationProtocolError):
            MavApplicationCodec.decode_packet(1, b"{")
        with self.assertRaises(MavApplicationProtocolError):
            MavApplicationCodec.decode_packet(
                1,
                b'{"type":"status","payload":{"value":NaN},"sent_at":1}',
            )

    def test_invalid_json_does_not_reserve_the_completed_packet_id(self) -> None:
        packet_id = 903
        body = b'{"type":"status","payload":'
        fragment = (
            application_module._HEADER.pack(
                application_module._MAGIC,
                application_module._VERSION,
                0,
                packet_id,
                0,
                1,
                len(body),
                application_module.zlib.crc32(body) & 0xFFFFFFFF,
            )
            + body
        )
        assembler = MavApplicationAssembler()

        with self.assertRaises(MavApplicationProtocolError):
            assembler.accept(fragment, source_system=7, source_component=3)

        valid = MavApplicationCodec.encode(
            MavApplicationPacket("status", {"ready": True}, packet_id=packet_id)
        )[0]
        decoded = assembler.accept(valid, source_system=7, source_component=3)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.payload, {"ready": True})

    def test_codec_rejects_lone_surrogate_as_a_protocol_error(self) -> None:
        packet = MavApplicationPacket("status", {"value": "\ud800"})

        with self.assertRaises(MavApplicationProtocolError) as raised:
            MavApplicationCodec.encode(packet)

        self.assertIsInstance(raised.exception.__cause__, UnicodeEncodeError)

    def test_json_decoder_rejects_duplicate_keys_invalid_utf8_and_excessive_depth(self) -> None:
        malformed_bodies = (
            b'{"type":"status","payload":{},"sent_at":1,"sent_at":2}',
            b"\xff",
            b'{"type":"status","payload":' + b"[" * 65 + b"0" + b"]" * 65 + b',"sent_at":1}',
            b"]" + b'{"type":"status","payload":{}}',
        )
        for body in malformed_bodies:
            with self.subTest(body=body[:48]), self.assertRaises(MavApplicationProtocolError):
                MavApplicationCodec.decode_packet(1, body)

    def test_json_depth_scanner_ignores_brackets_inside_escaped_strings(self) -> None:
        text = '[]{}\\"' * 80
        packet = MavApplicationPacket("status.update", {"text": text}, packet_id=902)
        assembler = MavApplicationAssembler()
        decoded = None
        for fragment in MavApplicationCodec.encode(packet):
            decoded = assembler.accept(fragment)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.payload["text"], text)

    def test_codec_enforces_the_same_json_depth_limit_for_encoding_and_decoding(self) -> None:
        at_limit: Any = "value"
        for _ in range(62):
            at_limit = [at_limit]
        packet = MavApplicationPacket("status.depth", {"data": at_limit}, packet_id=903)
        assembler = MavApplicationAssembler()
        decoded = None
        for fragment in MavApplicationCodec.encode(packet):
            decoded = assembler.accept(fragment)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.payload, packet.payload)

        beyond_limit: Any = "value"
        for _ in range(63):
            beyond_limit = [beyond_limit]
        too_deep = MavApplicationPacket("status.depth", {"data": beyond_limit}, packet_id=904)
        with self.assertRaisesRegex(MavApplicationProtocolError, "nesting|iç içe"):
            MavApplicationCodec.encode(too_deep)

    def test_codec_round_trips_payload_size_and_shape_matrix(self) -> None:
        sizes = (128, 512, 2_048, 8_192, 16_000)
        shapes = ("flat", "brackets", "nested")
        for payload_bytes in sizes:
            for shape in shapes:
                with self.subTest(payload_bytes=payload_bytes, shape=shape):
                    if shape == "brackets":
                        value: Any = ("[]{}" * (payload_bytes // 4)) + "[]{}"[: payload_bytes % 4]
                    elif shape == "nested":
                        depth = min(62, max(1, payload_bytes // 256))
                        value = "x" * max(1, payload_bytes - 11 * depth)
                        for _ in range(depth):
                            value = {"nested": value}
                    else:
                        value = "x" * payload_bytes

                    packet = MavApplicationPacket(
                        "benchmark.payload", {"data": value}, packet_id=905
                    )
                    assembler = MavApplicationAssembler()
                    decoded = None
                    for fragment in MavApplicationCodec.encode(packet):
                        decoded = assembler.accept(fragment)

                    self.assertIsNotNone(decoded)
                    self.assertEqual(decoded.payload, packet.payload)

    def test_unsupported_protocol_version_is_rejected(self) -> None:
        packet = MavApplicationPacket("status")
        fragment = bytearray(MavApplicationCodec.encode(packet)[0])
        fragment[2] = application_module._VERSION + 1

        with self.assertRaisesRegex(MavApplicationProtocolError, "sürümü"):
            MavApplicationCodec.decode_fragment(fragment)

    def test_fragment_decoder_rejects_invalid_header_fields_and_lengths(self) -> None:
        header = application_module._HEADER
        fields = [application_module._MAGIC, application_module._VERSION, 0, 1, 0, 1, 1, 0]
        valid = header.pack(*fields) + b"x"
        invalid_fragments = [
            b"short",
            header.pack(b"BAD!", *fields[1:]) + b"x",
            header.pack(fields[0], fields[1], 0, 0, *fields[4:]) + b"x",
            header.pack(fields[0], fields[1], 0, 1, 0, 0, 1, 0) + b"x",
            header.pack(
                fields[0],
                fields[1],
                0,
                1,
                0,
                application_module._MAX_FRAGMENT_COUNT + 1,
                1,
                0,
            )
            + b"x",
            header.pack(fields[0], fields[1], 0, 1, 1, 1, 1, 0) + b"x",
            header.pack(fields[0], fields[1], 0, 1, 0, 1, 2, 0) + b"x",
            header.pack(
                fields[0],
                fields[1],
                0,
                1,
                0,
                1,
                MavApplicationCodec.chunk_size + 1,
                0,
            ),
        ]

        self.assertEqual(MavApplicationCodec.decode_fragment(valid).chunk, b"x")
        for fragment in invalid_fragments:
            with self.subTest(fragment=fragment[: application_module._HEADER.size]):
                with self.assertRaises(MavApplicationProtocolError):
                    MavApplicationCodec.decode_fragment(fragment)

    def test_packet_decoder_rejects_non_object_and_invalid_fields(self) -> None:
        invalid_bodies = (
            b"[]",
            b'{"type":"status","payload":[]} ',
            b'{"type":"","payload":{},"sent_at":1}',
            b'{"type":"status","payload":{},"sent_at":"invalid"}',
            b'{"type":42,"payload":{},"sent_at":1}',
            b'{"type":"status","payload":{},"sent_at":true}',
            b'{"type":"status","payload":{},"sent_at":1,"reply":"false"}',
            b'{"type":"status","payload":{},"sent_at":' + b"9" * 2_000 + b"}",
            b"\xff",
        )
        for body in invalid_bodies:
            with self.subTest(body=body), self.assertRaises(MavApplicationProtocolError):
                MavApplicationCodec.decode_packet(1, body)
        fragment = bytearray(MavApplicationCodec.encode(MavApplicationPacket("status"))[0])
        fragment[3] = 1
        with self.assertRaisesRegex(MavApplicationProtocolError, "flag"):
            MavApplicationCodec.decode_fragment(fragment)

    def test_fragment_decoder_requires_full_nonfinal_chunks_and_zero_padding(self) -> None:
        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("data.large", {"text": "x" * 1_000})
        )[0]
        magic, version, flags, packet_id, index, count, chunk_length, checksum = (
            application_module._HEADER.unpack_from(fragment)
        )
        shortened = (
            application_module._HEADER.pack(
                magic,
                version,
                flags,
                packet_id,
                index,
                count,
                chunk_length - 1,
                checksum,
            )
            + fragment[application_module._HEADER.size : -1]
        )
        empty = application_module._HEADER.pack(
            magic, version, flags, packet_id, index, count, 0, checksum
        )

        for malformed in (shortened, empty, fragment + b"\x01"):
            with (
                self.subTest(length=len(malformed)),
                self.assertRaises(MavApplicationProtocolError),
            ):
                MavApplicationCodec.decode_fragment(malformed)

    def test_fragment_and_packet_decoders_enforce_public_size_limits(self) -> None:
        packet = MavApplicationPacket("status", packet_id=1)
        fragment = MavApplicationCodec.encode(packet)[0]
        padded_to_limit = fragment + bytes(
            application_module.V2_EXTENSION_PAYLOAD_SIZE - len(fragment)
        )

        self.assertEqual(
            MavApplicationCodec.decode_fragment(padded_to_limit).packet_id,
            1,
        )
        with self.assertRaisesRegex(MavApplicationProtocolError, "en fazla 249"):
            MavApplicationCodec.decode_fragment(padded_to_limit + b"\x00")

        with self.assertRaisesRegex(MavApplicationProtocolError, "çok büyük"):
            MavApplicationCodec.decode_packet(
                1,
                bytes(application_module._MAX_PACKET_BYTES + 1),
            )

    def test_completed_packet_deduplication_expires_and_is_process_local(self) -> None:
        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("command.execute", {"operation": "capture"}, packet_id=901)
        )[0]
        assembler = MavApplicationAssembler(fragment_timeout=0.01)

        with patch(
            "mavflow.application.time.monotonic",
            side_effect=(1.0, 1.0, 1.02, 1.02),
        ):
            self.assertIsNotNone(assembler.accept(fragment, source_system=7, source_component=3))
            self.assertIsNone(assembler.accept(fragment, source_system=7, source_component=3))
            self.assertIsNotNone(assembler.accept(fragment, source_system=7, source_component=3))
            self.assertIsNotNone(
                MavApplicationAssembler().accept(fragment, source_system=7, source_component=3)
            )

    def test_assembler_isolates_sources_and_discards_conflicting_packet_identity(self) -> None:
        first_packet = MavApplicationPacket(
            "data.large", {"text": "x" * 1_000}, packet_id=77, sent_at=1.0
        )
        second_packet = MavApplicationPacket(
            "data.large", {"text": "y" * 1_000}, packet_id=77, sent_at=1.0
        )
        first_fragments = MavApplicationCodec.encode(first_packet)
        second_fragments = MavApplicationCodec.encode(second_packet)

        assembler = MavApplicationAssembler()
        self.assertIsNone(assembler.accept(first_fragments[0], source_system=4))
        self.assertIsNone(assembler.accept(second_fragments[0], source_system=5))
        first_decoded = None
        second_decoded = None
        for fragment in first_fragments[1:]:
            first_decoded = assembler.accept(fragment, source_system=4)
        for fragment in second_fragments[1:]:
            second_decoded = assembler.accept(fragment, source_system=5)
        self.assertEqual(first_decoded.payload["text"], "x" * 1_000)
        self.assertEqual(second_decoded.payload["text"], "y" * 1_000)

        conflicting = MavApplicationAssembler()
        self.assertIsNone(conflicting.accept(first_fragments[0], source_system=4))
        with self.assertRaisesRegex(MavApplicationProtocolError, "uyuşmayan"):
            conflicting.accept(second_fragments[0], source_system=4)
        self.assertEqual(conflicting._inflight_bytes, 0)
        self.assertFalse(conflicting._assemblies)

    def test_inflight_assembly_count_is_bounded(self) -> None:
        assembler = MavApplicationAssembler(max_inflight_assemblies=2)
        fragments = [
            MavApplicationCodec.encode(
                MavApplicationPacket(
                    "data.large",
                    {"text": "x" * 1_000},
                    packet_id=packet_id,
                )
            )[0]
            for packet_id in (1, 2, 3)
        ]

        self.assertIsNone(assembler.accept(fragments[0]))
        self.assertIsNone(assembler.accept(fragments[1]))
        with self.assertRaisesRegex(MavApplicationProtocolError, "Çok fazla"):
            assembler.accept(fragments[2])

    def test_inflight_fragment_bytes_are_bounded(self) -> None:
        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("data.large", {"text": "x" * 1_000})
        )[0]
        assembler = MavApplicationAssembler(max_inflight_bytes=8)

        with self.assertRaisesRegex(MavApplicationProtocolError, "byte sınırını"):
            assembler.accept(fragment)

    def test_inflight_assemblies_are_bounded_per_source(self) -> None:
        assembler = MavApplicationAssembler(
            max_inflight_assemblies=4,
            max_inflight_assemblies_per_source=1,
        )
        fragments = [
            MavApplicationCodec.encode(
                MavApplicationPacket(
                    "data.large",
                    {"text": "x" * 1_000},
                    packet_id=packet_id,
                )
            )[0]
            for packet_id in (10, 11)
        ]

        self.assertIsNone(assembler.accept(fragments[0], source_system=4, source_component=1))
        with self.assertRaisesRegex(MavApplicationProtocolError, "kaynağı"):
            assembler.accept(fragments[1], source_system=4, source_component=1)
        self.assertIsNone(assembler.accept(fragments[1], source_system=5, source_component=1))
        self.assertEqual(len(assembler._assemblies), 2)

    def test_inflight_bytes_are_bounded_per_source(self) -> None:
        assembler = MavApplicationAssembler(
            max_inflight_bytes=2_000,
            max_inflight_bytes_per_source=300,
            max_inflight_assemblies_per_source=4,
        )
        fragments = [
            MavApplicationCodec.encode(
                MavApplicationPacket(
                    "data.large",
                    {"text": "x" * 1_000},
                    packet_id=packet_id,
                )
            )[0]
            for packet_id in (20, 21)
        ]

        self.assertIsNone(assembler.accept(fragments[0], source_system=4, source_component=1))
        with self.assertRaisesRegex(MavApplicationProtocolError, "kaynağı"):
            assembler.accept(fragments[1], source_system=4, source_component=1)
        self.assertIsNone(assembler.accept(fragments[1], source_system=5, source_component=1))
        self.assertEqual(
            assembler._source_inflight_bytes[(4, 1)],
            len(assembler._assemblies[(4, 1, 20)].fragments[0]),
        )

    def test_completed_packet_history_is_bounded(self) -> None:
        assembler = MavApplicationAssembler(max_completed_packets=2)
        fragments = [
            MavApplicationCodec.encode(MavApplicationPacket("status", packet_id=packet_id))[0]
            for packet_id in (1, 2, 3)
        ]

        for fragment in fragments:
            self.assertIsNotNone(assembler.accept(fragment))

        self.assertEqual(len(assembler._completed), 2)
        self.assertNotIn((0, 0, 1), assembler._completed)


class _ApplicationClient:
    def __init__(self) -> None:
        self.endpoint = MavEndpoint(source_system=42, source_component=3)
        self.messages = EventBus[MavMessageEnvelope](history=16)
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def call_mav(self, method_name: str, *args: object) -> None:
        self.calls.append((method_name, args))

    def publish_fragment(
        self,
        payload: bytes,
        *,
        source_system: int = 8,
        source_component: int = 9,
        target_network: int = 14,
        message_type: int = 420,
        target_system: int = 42,
        target_component: int = 3,
    ) -> None:
        message = SimpleNamespace(
            payload=list(payload),
            target_network=target_network,
            message_type=message_type,
            target_system=target_system,
            target_component=target_component,
        )
        self.messages.publish(
            MavMessageEnvelope(
                sequence=1,
                message=message,
                message_type="V2_EXTENSION",
                source_system=source_system,
                source_component=source_component,
                message_id=123,
                received_monotonic=time.monotonic(),
            )
        )


class ApplicationChannelTests(unittest.TestCase):
    def test_channel_filters_routes_and_clears_incomplete_assemblies(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(
            client,  # type: ignore[arg-type]
            network_id=14,
            message_type=420,
            source_systems=(8,),
            source_components=(9,),
        )
        packets: list[MavApplicationPacket] = []
        errors: list[Exception] = []
        channel.packets.subscribe(packets.append)
        channel.errors.subscribe(errors.append)
        packet = MavApplicationPacket("data.large", {"text": "x" * 1_000}, packet_id=501)
        fragments = MavApplicationCodec.encode(packet)

        channel.start()
        channel.start()
        client.publish_fragment(fragments[0], source_system=7)
        client.publish_fragment(fragments[0], target_network=13)
        client.publish_fragment(fragments[0], target_system=99)
        self.assertFalse(errors)
        client.publish_fragment(fragments[0])
        self.assertFalse(packets)
        channel.stop()
        self.assertFalse(channel.running)
        channel.start()
        for fragment in fragments[1:]:
            client.publish_fragment(fragment)
        self.assertFalse(packets)
        client.publish_fragment(fragments[0])

        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0].packet_id, 501)
        self.assertEqual(packets[0].payload["text"], "x" * 1_000)
        channel.stop()

    def test_channel_allowed_source_pairs_do_not_form_a_cross_product(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(
            client,  # type: ignore[arg-type]
            network_id=14,
            message_type=420,
            allowed_sources=((7, 3), (8, 4)),
        )
        packets: list[MavApplicationPacket] = []
        channel.packets.subscribe(packets.append)
        channel.start()

        for packet_id, source in enumerate(((7, 3), (8, 4), (7, 4), (8, 3)), start=1):
            fragment = MavApplicationCodec.encode(
                MavApplicationPacket("status.update", {"id": packet_id}, packet_id=packet_id)
            )[0]
            client.publish_fragment(
                fragment,
                source_system=source[0],
                source_component=source[1],
            )

        channel.stop()
        self.assertEqual([packet.packet_id for packet in packets], [1, 2])

    def test_default_local_identity_tracks_endpoint_reconfiguration(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(client, network_id=14, message_type=420)  # type: ignore[arg-type]
        packets: list[MavApplicationPacket] = []
        channel.packets.subscribe(packets.append)
        client.endpoint = MavEndpoint(source_system=55, source_component=6)
        channel.start()

        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("status.update", {}, packet_id=903)
        )[0]
        client.publish_fragment(fragment, target_system=55, target_component=6)

        self.assertEqual((channel.local_system, channel.local_component), (55, 6))
        self.assertEqual([packet.packet_id for packet in packets], [903])
        channel.stop()

    def test_channel_stop_waits_for_an_inflight_packet_callback(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(client, network_id=14, message_type=420)  # type: ignore[arg-type]
        entered = threading.Event()
        release = threading.Event()
        stopped = threading.Event()
        packets: list[MavApplicationPacket] = []

        def block(packet: MavApplicationPacket) -> None:
            packets.append(packet)
            entered.set()
            if not release.wait(1.0):
                raise TimeoutError("test did not release the packet callback")

        channel.packets.subscribe(block)
        channel.start()
        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("status.update", {}, packet_id=904)
        )[0]
        publisher = threading.Thread(target=lambda: client.publish_fragment(fragment))
        publisher.start()
        self.assertTrue(entered.wait(1.0))

        stopper = threading.Thread(target=lambda: (channel.stop(), stopped.set()))
        stopper.start()
        self.assertFalse(stopped.wait(0.05))
        release.set()
        publisher.join(1.0)
        stopper.join(1.0)

        self.assertFalse(publisher.is_alive())
        self.assertFalse(stopper.is_alive())
        self.assertTrue(stopped.is_set())
        client.publish_fragment(fragment)
        self.assertEqual([packet.packet_id for packet in packets], [904])

    def test_repeated_channel_start_stop_keeps_delivery_single(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(client, network_id=14, message_type=420)  # type: ignore[arg-type]
        packets: list[MavApplicationPacket] = []
        channel.packets.subscribe(packets.append)

        for _ in range(1_000):
            channel.start()
            channel.stop()
        channel.start()
        fragment = MavApplicationCodec.encode(
            MavApplicationPacket("status.update", {}, packet_id=905)
        )[0]
        client.publish_fragment(fragment)

        self.assertEqual([packet.packet_id for packet in packets], [905])
        channel.stop()

    def test_channel_publishes_protocol_errors_for_matching_frames(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(client, network_id=14, message_type=420)
        errors: list[Exception] = []
        channel.errors.subscribe(errors.append)
        channel.start()
        client.publish_fragment(b"bad frame")

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], MavApplicationProtocolError)
        channel.stop()

    def test_packet_subscriber_failures_are_reported_without_recursive_errors(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(client, network_id=14, message_type=420)
        errors: list[Exception] = []
        channel.errors.subscribe(errors.append)

        def fail(_packet: MavApplicationPacket) -> None:
            raise RuntimeError("subscriber failed")

        channel.packets.subscribe(fail)
        channel.start()
        fragment = MavApplicationCodec.encode(MavApplicationPacket("status"))[0]
        client.publish_fragment(fragment)

        self.assertEqual(len(errors), 1)
        self.assertEqual(str(errors[0]), "subscriber failed")
        channel.stop()

    def test_channel_send_fragments_and_pads_mavlink_extension_payloads(self) -> None:
        client = _ApplicationClient()
        channel = MavApplicationChannel(
            client,  # type: ignore[arg-type]
            network_id=14,
            message_type=420,
            target_system=5,
            target_component=6,
        )
        packet = channel.send(
            "Data.Large",
            {"text": "x" * 1_000},
            packet_id=78,
            target_system=8,
            target_component=9,
            expects_response=True,
        )
        fragments = MavApplicationCodec.encode(packet)

        self.assertEqual(packet.packet_id, 78)
        self.assertTrue(packet.expects_response)
        self.assertEqual(len(client.calls), len(fragments))
        for (method_name, args), fragment in zip(client.calls, fragments, strict=True):
            padded = cast(list[int], args[4])
            self.assertEqual(method_name, "v2_extension_send")
            self.assertEqual(args[:4], (14, 8, 9, 420))
            self.assertEqual(len(padded), 249)
            self.assertEqual(padded[: len(fragment)], list(fragment))

    def test_channel_rejects_invalid_destination_and_protocol_configuration(self) -> None:
        client = _ApplicationClient()
        with self.assertRaises(ValueError):
            MavApplicationChannel(client, network_id=256)  # type: ignore[arg-type]
        channel = MavApplicationChannel(client)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            channel.send("status", target_system=256)
        with self.assertRaises(ValueError):
            MavApplicationChannel(client, allowed_sources=((7, 3), (256, 1)))  # type: ignore[arg-type]
        self.assertFalse(client.calls)


if __name__ == "__main__":
    unittest.main()
