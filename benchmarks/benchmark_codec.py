"""Measure application packet encode/reassembly performance with pyperf."""

from __future__ import annotations

from mavflow import MavlinkApplicationAssembler, MavlinkApplicationCodec, MavlinkApplicationPacket

from ._runner import create_runner, positive_integer


def _round_trip(packet: MavlinkApplicationPacket) -> MavlinkApplicationPacket:
    assembler = MavlinkApplicationAssembler()
    decoded = None
    for fragment in MavlinkApplicationCodec.encode(packet):
        decoded = assembler.accept(fragment, source_system=1, source_component=1)
    if decoded is None:
        raise RuntimeError("Benchmark packet did not reassemble")
    return decoded


def main() -> None:
    runner = create_runner("benchmarks.benchmark_codec")
    runner.argparser.add_argument(
        "--payload-bytes",
        type=positive_integer,
        default=2_048,
        help="number of payload bytes encoded in each operation",
    )
    args = runner.parse_args()
    packet = MavlinkApplicationPacket(
        "benchmark.payload",
        {"data": "x" * args.payload_bytes},
        packet_id=42,
    )
    runner.bench_func(
        "application_encode_and_reassemble",
        _round_trip,
        packet,
        metadata={
            "packet_type": packet.packet_type,
            "payload_bytes": args.payload_bytes,
            "operation": "encode all fragments and reassemble one packet",
        },
    )


if __name__ == "__main__":
    main()
