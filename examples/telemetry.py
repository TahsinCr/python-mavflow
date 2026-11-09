"""Receive and print MAVLink heartbeat messages from a UDP endpoint."""

from __future__ import annotations

import argparse

from mavflow import MavEndpoint, MavRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="UDP bind address")
    parser.add_argument("--port", type=int, default=14550, help="UDP port")
    parser.add_argument("--timeout", type=float, default=10.0, help="Heartbeat wait timeout")
    args = parser.parse_args()

    endpoint = MavEndpoint.udp(args.host, args.port)
    runtime = MavRuntime(endpoint)
    subscription = runtime.messages.subscribe(
        lambda envelope: print(envelope.message),
        routing_key="HEARTBEAT",
    )
    try:
        runtime.start()
        first = runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=args.timeout)
        print("First heartbeat:", first.message if first is not None else None)
    finally:
        subscription.cancel()
        runtime.close()


if __name__ == "__main__":
    main()
