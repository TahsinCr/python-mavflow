from __future__ import annotations

import unittest
from types import SimpleNamespace

from mavflow.message import MavMessageEnvelope


class MessageEnvelopeTests(unittest.TestCase):
    def test_wrap_normalizes_message_type_and_reads_header_fallbacks(self) -> None:
        message = SimpleNamespace(
            get_type=lambda: " heartbeat ",
            _header=SimpleNamespace(srcSystem=17, srcComponent=3),
        )

        envelope = MavMessageEnvelope.wrap(9, message)

        self.assertEqual(envelope.sequence, 9)
        self.assertEqual(envelope.message_type, "HEARTBEAT")
        self.assertEqual(envelope.source_system, 17)
        self.assertEqual(envelope.source_component, 3)
        self.assertIsNone(envelope.message_id)

    def test_wrap_rejects_missing_or_empty_message_type(self) -> None:
        with self.assertRaises(TypeError):
            MavMessageEnvelope.wrap(1, object())
        with self.assertRaises(ValueError):
            MavMessageEnvelope.wrap(1, SimpleNamespace(get_type=lambda: "  "))

    def test_to_dict_includes_payload_only_when_requested_and_supports_repr_fallback(self) -> None:
        serialized_message = SimpleNamespace(
            get_type=lambda: "STATUS",
            to_dict=lambda: {"ready": True},
        )
        envelope = MavMessageEnvelope.wrap(1, serialized_message)

        self.assertNotIn("payload", envelope.to_dict())
        self.assertEqual(envelope.to_dict(include_payload=True)["payload"], {"ready": True})

        plain_message = SimpleNamespace(get_type=lambda: "STATUS")
        plain_envelope = MavMessageEnvelope.wrap(2, plain_message)
        self.assertEqual(
            plain_envelope.to_dict(include_payload=True)["payload"],
            repr(plain_message),
        )


if __name__ == "__main__":
    unittest.main()
