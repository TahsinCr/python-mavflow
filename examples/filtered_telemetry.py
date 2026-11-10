"""Print position and attitude messages from a selected MAVLink system."""

from __future__ import annotations

import argparse
import time

from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="UDP bind address")
    parser.add_argument("--port", type=int, default=14550, help="UDP port")
    parser.add_argument("--system", type=int, default=1, help="source system ID")
    parser.add_argument("--timeout", type=float, default=10.0, help="heartbeat timeout")
    parser.add_argument(
        "--seconds",
        type=float,
        default=30.0,
        help="seconds to print filtered messages after the first heartbeat",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if args.seconds < 0:
        parser.error("--seconds cannot be negative")

    endpoint = MavEndpoint.udp(args.host, args.port)
    runtime = MavRuntime(endpoint)

    subscriptions = [
        runtime.messages.subscribe(
            lambda envelope: print(envelope.message),
            event_filter=EventFilter(
                predicate=lambda envelope, selected_type=message_type: (
                    envelope.message_type == selected_type and envelope.source_system == args.system
                )
            ),
        )
        for message_type in ("GLOBAL_POSITION_INT", "ATTITUDE")
    ]
    try:
        runtime.start()
        heartbeat = runtime.messages.wait_for(
            event_filter=EventFilter(
                predicate=lambda envelope: (
                    envelope.message_type == "HEARTBEAT" and envelope.source_system == args.system
                )
            ),
            timeout=args.timeout,
        )
        if heartbeat is None:
            print(f"No heartbeat received from system {args.system}")
            return 1
        print(f"Listening to system {args.system} for {args.seconds:g}s (Ctrl+C to stop)")
        time.sleep(args.seconds)
    finally:
        for subscription in subscriptions:
            subscription.cancel()
        runtime.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
