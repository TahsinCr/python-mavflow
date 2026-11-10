from __future__ import annotations

import math
import time
import unittest
from collections.abc import Mapping
from types import SimpleNamespace
from typing import cast

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
            b"\xff",
        )
        for body in invalid_bodies:
            with self.subTest(body=body), self.assertRaises(MavApplicationProtocolError):
                MavApplicationCodec.decode_packet(1, body)

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
        self.messages = EventBus[MavMessageEnvelope](
            history=16,
            routing_key=lambda envelope: envelope.message_type,
        )
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
        self.assertFalse(client.calls)


if __name__ == "__main__":
    unittest.main()
