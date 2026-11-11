"""Measure application packet encode/reassembly performance with pyperf."""

from __future__ import annotations

from typing import Any

from mavflow import MavApplicationAssembler, MavApplicationCodec, MavApplicationPacket

from ._runner import create_runner, positive_integer


def _round_trip(packet: MavApplicationPacket) -> MavApplicationPacket:
    assembler = MavApplicationAssembler()
    decoded = None
    for fragment in MavApplicationCodec.encode(packet):
        decoded = assembler.accept(fragment, source_system=1, source_component=1)
    if decoded is None:
        raise RuntimeError("Benchmark packet did not reassemble")
    return decoded


def _payload_value(payload_bytes: int, shape: str) -> Any:
    if shape == "brackets":
        return ("[]{}" * (payload_bytes // 4)) + "[]{}"[: payload_bytes % 4]
    if shape == "nested":
        depth = min(62, max(1, payload_bytes // 256))
        value: Any = "x" * max(1, payload_bytes - 11 * depth)
        for _ in range(depth):
            value = {"nested": value}
        return value
    return "x" * payload_bytes


def main() -> None:
    runner = create_runner("benchmarks.benchmark_codec")
    runner.argparser.add_argument(
        "--payload-bytes",
        type=positive_integer,
        default=2_048,
        help="number of payload bytes encoded in each operation",
    )
    runner.argparser.add_argument(
        "--payload-shape",
        choices=("flat", "brackets", "nested"),
        default="flat",
        help="JSON payload shape to measure",
    )
    args = runner.parse_args()
    packet = MavApplicationPacket(
        "benchmark.payload",
        {"data": _payload_value(args.payload_bytes, args.payload_shape)},
        packet_id=42,
    )
    runner.bench_func(
        "application_encode_and_reassemble",
        _round_trip,
        packet,
        metadata={
            "packet_type": packet.packet_type,
            "payload_bytes": args.payload_bytes,
            "payload_shape": args.payload_shape,
            "operation": "encode all fragments and reassemble one packet",
        },
    )


if __name__ == "__main__":
    main()
