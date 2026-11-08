"""Encode and reassemble a Mavflow application packet without a vehicle link."""

from __future__ import annotations

from mavflow import MavlinkApplicationAssembler, MavlinkApplicationCodec, MavlinkApplicationPacket


def main() -> None:
    packet = MavlinkApplicationPacket(
        "vehicle.health",
        {"ready": True, "battery_percent": 84, "note": "example"},
        packet_id=42,
    )
    fragments = MavlinkApplicationCodec.encode(packet)
    assembler = MavlinkApplicationAssembler()
    decoded = None
    for fragment in fragments:
        decoded = assembler.accept(fragment, source_system=1, source_component=1)

    if decoded is None:
        raise RuntimeError("The example packet did not reassemble")
    print(f"Encoded and reassembled {len(fragments)} fragment(s): {decoded.to_dict()}")


if __name__ == "__main__":
    main()
