"""Receive one MAVLink heartbeat through an asyncio channel."""

from __future__ import annotations

import argparse
import asyncio
import math

from eventflow import AsyncEventQueue

from mavflow import MavEndpoint, MavRuntime


async def receive_heartbeat(
    runtime: MavRuntime,
    host: str,
    port: int,
    timeout: float,
) -> int:
    async with AsyncEventQueue(
        runtime.messages,
        capacity=64,
        overflow="drop_oldest",
    ) as events:
        deadline = asyncio.get_running_loop().time() + timeout
        try:
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                envelope = await asyncio.wait_for(events.receive(), timeout=remaining)
                if envelope.message_type == "HEARTBEAT":
                    break
        except asyncio.TimeoutError:
            print(f"No heartbeat received from {host}:{port} within {timeout:g}s")
            return 1
        print(envelope.message)
        return 0


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
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be positive and finite")
    runtime = MavRuntime(MavEndpoint.udp(args.host, args.port))
    try:
        runtime.start()
        exit_code = asyncio.run(receive_heartbeat(runtime, args.host, args.port, args.timeout))
    finally:
        runtime.close()
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
