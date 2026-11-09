"""Wait for a MAVLink message and inspect the router's retained history."""

from __future__ import annotations

import argparse

from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="UDP bind address")
    parser.add_argument("--port", type=int, default=14550, help="UDP port")
    parser.add_argument(
        "--message-type",
        default="HEARTBEAT",
        help="MAVLink message name to wait for",
    )
    parser.add_argument("--limit", type=int, default=20, help="maximum history entries")
    parser.add_argument("--timeout", type=float, default=10.0, help="message wait timeout")
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")

    endpoint = MavEndpoint.udp(args.host, args.port)
    with MavRuntime(endpoint) as runtime:
        envelope = runtime.messages.wait_for(
            routing_key=args.message_type.upper(),
            timeout=args.timeout,
        )
        if envelope is None:
            print(f"No {args.message_type} message received before the timeout")
            return 1

        message_filter = EventFilter(
            predicate=lambda item: item.message_type == args.message_type.upper()
        )
        history = runtime.messages.history
        if history is None:
            raise RuntimeError("MAVLink message history is not configured")
        most_recent = history.latest(message_filter)
        print("First matching message:", envelope.message)
        print("Most recent:", most_recent.message if most_recent is not None else None)
        print(f"Recent {args.message_type} history:")
        for entry in history.query(message_filter, limit=args.limit):
            print(entry.message)
        print("Router stats:", runtime.state.router)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
