"""Receive one MAVLink heartbeat through an asyncio channel."""

from __future__ import annotations

import argparse
import asyncio

from eventflow import AsyncEventQueue

from mavflow import MavEndpoint, MavRuntime


async def receive_heartbeat(host: str, port: int, timeout: float) -> int:
    runtime = MavRuntime(MavEndpoint.udp(host, port))
    try:
        async with AsyncEventQueue(
            runtime.messages,
            routing_key="HEARTBEAT",
            capacity=64,
            overflow="drop_oldest",
        ) as events:
            runtime.start()
            try:
                envelope = await asyncio.wait_for(events.receive(), timeout=timeout)
            except asyncio.TimeoutError:
                print(f"No heartbeat received from {host}:{port} within {timeout:g}s")
                return 1
            print(envelope.message)
            return 0
    finally:
        runtime.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="UDP bind address")
    parser.add_argument("--port", type=int, default=14550, help="UDP port")
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="seconds to wait for a heartbeat",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    raise SystemExit(asyncio.run(receive_heartbeat(args.host, args.port, args.timeout)))


if __name__ == "__main__":
    main()
