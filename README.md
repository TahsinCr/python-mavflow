[![Version: v2.0][version-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/CHANGELOG.md)
[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![MIT License][license-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE)

**English** | [Türkçe](https://github.com/TahsinCr/python-mavflow/blob/main/README-TR.md)

# Mavflow

[Repository](https://github.com/TahsinCr/python-mavflow) · [Releases](https://github.com/TahsinCr/python-mavflow/releases) · [Source package](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink) · [MIT License](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE)

Mavflow provides Python tools for MAVLink endpoints, connections, message
routing, telemetry subscriptions, bounded asyncio delivery, and an optional
application packet protocol.

The package was extracted from the MAVLink layer of
[Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core).
Mavflow is maintained as a standalone library; its source project is not needed
to install or use it. The transport, routing, application protocol, tests, and
examples live in this repository.

Mavflow focuses on reusable MAVLink infrastructure. Vehicle mission logic and
application-specific behavior belong to the projects built on top of it.

## Contents

- [Getting started](#getting-started)
  - [Install](#install)
  - [Version](#version)
  - [Receive your first heartbeat](#first-heartbeat)
  - [Run an example](#run-an-example)
  - [Choose an API layer](#choose-api-layer)
- [Core concepts](#core-concepts)
  - [Endpoints and connections](#endpoints)
  - [One reader and the message router](#router)
  - [Filtering and message history](#filtering)
  - [Lifecycle and composition](#lifecycle)
  - [Event delivery](#event-delivery)
- [Working with telemetry](#telemetry)
  - [Subscribe to messages](#subscribe)
  - [Wait for a message and inspect history](#wait-and-history)
- [Async applications](#async-applications)
- [Application packets](#application-packets)
  - [Encode and reassemble packets](#codec)
  - [Peers, requests, and handlers](#peers-and-requests)
  - [Remote log models](#remote-logs)
- [Troubleshooting](#troubleshooting)
- [Security and operational boundaries](#operational-boundaries)
- [Composing the stack](#composition)
- [Lifecycle and reference](#reference)
  - [Defaults](#defaults)
  - [Component responsibilities](#components)
  - [Public types and errors](#public-types)
- [Development and license](#development)

<a id="getting-started"></a>
## Getting started

<a id="install"></a>
### Install

Python 3.10 or newer is required. From a checkout, install Mavflow into the
active environment:

~~~bash
python -m pip install .
~~~

After the PyPI release is available, install the distribution with:

~~~bash
python -m pip install mavflow
~~~

The distribution and import package are both named `mavflow`. Its required
dependencies, `flowevent` and `pymavlink`, are installed automatically.
`flowevent` supplies event buses and subscription handles; Mavflow uses those
types directly.

For editable development, install the development tools:

~~~bash
python -m pip install -e ".[dev]"
~~~

The `dev` extra provides build, coverage, pyperf, Pyright, Ruff, and Twine.

<a id="version"></a>
### Version

This breaking API line is **v2.0**. Message subscriptions, waiting, filtering,
and retained history now use EventFlow directly through `runtime.messages`;
Mavflow no longer maintains parallel subscription or history APIs.

~~~python
from mavflow import __version__

print(__version__)  # 2.0
~~~

<a id="first-heartbeat"></a>
### Receive your first heartbeat

This async example listens on UDP port 14550 and awaits a heartbeat through
EventFlow's bounded `AsyncEventQueue`. The MAVLink router remains the only
transport reader; no extra receive thread is created for async consumers.

~~~python
import asyncio

from eventflow import AsyncEventQueue
from mavflow import MavEndpoint, MavRuntime


async def main() -> None:
    runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
    async with AsyncEventQueue(
        runtime.messages,
        routing_key="HEARTBEAT",
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:
        runtime.start()
        try:
            envelope = await asyncio.wait_for(heartbeats.receive(), timeout=10.0)
        except asyncio.TimeoutError:
            print("No heartbeat received before the timeout")
        else:
            print("First heartbeat:", envelope.message)
        finally:
            runtime.stop()
    runtime.close()


asyncio.run(main())
~~~

For synchronous code, call `runtime.messages.wait_for(routing_key="HEARTBEAT")`;
it returns a `MavMessageEnvelope`, whose `.message` is the original pymavlink
message. The runtime owns transport start and stop.

<a id="run-an-example"></a>
### Run an example

Install Mavflow first, then run these commands from the repository root. Each
telemetry example expects a vehicle, simulator, or MAVLink router to send data
to the selected endpoint.

~~~bash
python -m examples.telemetry_async --host 0.0.0.0 --port 14550 --timeout 10
python -m examples.telemetry --host 0.0.0.0 --port 14550 --timeout 10
python -m examples.filtered_telemetry --system 1 --seconds 30
python -m examples.message_history --message-type HEARTBEAT --limit 20
python -m examples.application_packets
~~~

`telemetry_async` demonstrates asyncio consumption, while `telemetry` shows the
synchronous runtime. `filtered_telemetry` selects position and attitude
messages from one source system; `message_history` waits for a message and
prints the router's retained history and counters. `application_packets`
demonstrates packet encoding and reassembly without opening a vehicle link.

<a id="choose-api-layer"></a>
### Choose an API layer

| Component | Use it for |
| --- | --- |
| `MavConnection` | Opening a `pymavlink` transport and sending or receiving raw MAVLink messages. |
| `MavMessageRouter` | Owning the single receive loop and publishing typed message envelopes. |
| `MavClient` | Composing a connection, router, and EventFlow named channels. |
| `MavRuntime` | Owning the client lifecycle and optional application services. |
| EventFlow `EventBus` | Subscribing, filtering, waiting for, and querying received envelopes. |
| EventFlow `AsyncEventQueue` | Consuming a synchronous bus from asyncio with bounded buffering. |

Use `MavRuntime` for the normal application entry point. EventFlow is a direct
runtime dependency: import its `EventBus`, `EventFilter`, `EventEngine`, and
`AsyncEventQueue` from `eventflow` and use the objects exposed by Mavflow.

<a id="core-concepts"></a>
## Core concepts

<a id="endpoints"></a>
### Endpoints and connections

`MavEndpoint` is an immutable description of a connection. The helpers
`tcp(host, port)`, `udp(host, port)`, and `serial(device, baud=...)` build
common endpoint forms:

~~~python
from mavflow import MavEndpoint

udp = MavEndpoint.udp("0.0.0.0", 14550)
tcp = MavEndpoint.tcp("192.0.2.10", 5760)
serial = MavEndpoint.serial("/dev/ttyUSB0", baud=57600)
~~~

An endpoint also carries the local system and component IDs, MAVLink dialect,
reconnect setting, and heartbeat timeout. Its default URI is
`udp:127.0.0.1:14550`. Use the address and transport mode expected by the
vehicle, radio, simulator, or router.

`MavConnection` owns the underlying `pymavlink` connection. Importing
`mavflow` does not open a device or load the transport module; `pymavlink` is
loaded when a real connection is created. The connection provides lifecycle
methods, raw message access, send helpers, and vehicle target information.
At this low level, `MavConnection.receive()` forwards pymavlink's native message
type filter (`str`, `list[str]`, or `set[str]`) and condition expression. Keep
the router as the only reader while it is active; subscribe through
`runtime.messages` for routed telemetry.

<a id="router"></a>
### One reader and the message router

`MavMessageRouter` owns the receive loop for a connection. It reads each
incoming message once, wraps it in `MavMessageEnvelope`, and publishes that
envelope once through an EventFlow `EventBus`. The bus indexes subscriptions by
`message_type`, so a keyed subscription skips callbacks for unrelated MAVLink
types while preserving registration order.

This single-reader ownership keeps transport reads and routing in one place.
Router callbacks run synchronously during delivery, so keep them short or
submit expensive work to a bounded worker. `MavClient` and `MavRuntime` expose
the same message bus directly as `messages` and the native EventFlow engine as
`events`.

<a id="filtering"></a>
### Filtering and message history

Subscribe with EventFlow routing keys and predicates. A routing key selects a
message type through the router's indexed bus; `EventFilter` or `predicate`
handles source IDs and application-specific conditions:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
source_one = EventFilter(predicate=lambda envelope: envelope.source_system == 1)
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    routing_key="GLOBAL_POSITION_INT",
    event_filter=source_one,
)
runtime.start()
try:
    runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
finally:
    subscription.cancel()
    runtime.close()
~~~

The envelope also carries `source_component`, `message_id`, sequence, and
receive time. A native MAVLink condition can be used inside an EventFlow
predicate by calling `runtime.connection.evaluate_condition(condition)`.

<a id="lifecycle"></a>
### Lifecycle and composition

`MavRuntime` composes a client and, when requested, an application channel,
peer, and dispatcher. `start()` starts the transport and owned application
services in dependency order. `stop()` shuts them down in reverse order and
collects shutdown errors. A runtime can be used as a synchronous context
manager, as in the first example.

Create separate runtimes for separate connections. A runtime can also accept
an existing `MavClient` when an application needs to share or configure
the lower-level connection and router.

<a id="event-delivery"></a>
### Event delivery

Mavflow uses `flowevent` for event buses and subscription handles. Runtime
message, packet, and error events use that library's contracts directly; Mavflow
does not implement a second event bus.

A subscription returned by `subscribe()` or `handle()` has an explicit
`cancel()` lifecycle. Cancel subscriptions when their work is complete, or
manage them with the surrounding service lifetime.

<a id="telemetry"></a>
## Working with telemetry

<a id="subscribe"></a>
### Subscribe to messages

`runtime.messages` is the EventFlow `EventBus` itself. Its `subscribe()` and
`once()` methods return native EventFlow subscriptions, and callbacks receive a
`MavMessageEnvelope`:

~~~python
from mavflow import MavEndpoint, MavRuntime

runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
subscription = runtime.messages.subscribe(
    lambda envelope: print(envelope.message),
    routing_key="HEARTBEAT",
)
runtime.start()
try:
    first = runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
    print("First heartbeat:", first.message if first is not None else None)
finally:
    subscription.cancel()
    runtime.close()
~~~

Use `EventFilter` for extra conditions, or the direct `event_type` and
`predicate` subscription arguments. `routing_key` is indexed by EventFlow
before the predicate runs, so telemetry subscriptions do not scan unrelated
message types.

<a id="wait-and-history"></a>
### Wait for a message and inspect history

Use EventFlow `wait_for()` for one future message. The bounded EventFlow history
is available directly on the bus and supports the same native filters:

~~~python
from eventflow import EventFilter

from mavflow import MavEndpoint, MavRuntime

with MavRuntime(MavEndpoint.udp("0.0.0.0", 14550)) as runtime:
    envelope = runtime.messages.wait_for(routing_key="HEARTBEAT", timeout=10.0)
    history = runtime.messages.history
    heartbeat_filter = EventFilter(predicate=lambda item: item.message_type == "HEARTBEAT")
    recent = history.query(heartbeat_filter, limit=20) if history is not None else ()
    latest = history.latest(heartbeat_filter) if history is not None else None
    router_state = runtime.state.router
~~~

`wait_for()` returns `None` after its timeout. History contains envelopes in
publication order and is bounded by `history_limit` (512 by default); it is
in-memory, not a durable telemetry log. Router statistics include receive and
dispatch errors, estimated dropped messages, and delivery quality.

<a id="async-applications"></a>
## Async applications

MAVLink transport reads remain owned by the synchronous router. Use EventFlow's
`AsyncEventQueue` to receive the same envelopes in asyncio. It applies the bus
filter before consuming bounded queue capacity; `drop_oldest` retains the
newest telemetry when a consumer falls behind.

~~~python
import asyncio

from eventflow import AsyncEventQueue
from mavflow import MavEndpoint, MavRuntime


async def main() -> None:
    runtime = MavRuntime(MavEndpoint.udp("0.0.0.0", 14550))
    async with AsyncEventQueue(
        runtime.messages,
        routing_key="HEARTBEAT",
        capacity=64,
        overflow="drop_oldest",
    ) as heartbeats:
        runtime.start()
        try:
            while True:
                envelope = await heartbeats.receive()
                print(envelope.message)
        finally:
            runtime.stop()
    runtime.close()


asyncio.run(main())
~~~

The queue owns no MAVLink reader and creates no asyncio task per publication.
Its `stats()` reports queue depth and dropped events. Cancel the consuming task
normally; the `async with` block removes the EventFlow subscription.

<a id="application-packets"></a>
## Application packets

The optional application protocol carries JSON-compatible Mavflow packets over
MAVLink `V2_EXTENSION` messages. It is separate from ordinary telemetry
routing; applications that only need standard MAVLink messages do not need to
enable it.

<a id="codec"></a>
### Encode and reassemble packets

`MavApplicationPacket` stores a packet type, JSON-compatible payload,
packet ID, timestamp, source IDs, and response flag. `MavApplicationCodec`
encodes a packet into bounded fragments, adds a checksum, and validates
fragments during decode. `MavApplicationAssembler` reassembles fragments
while bounding incomplete packet count, memory, and lifetime.

The codec can be used without opening a vehicle connection:

~~~python
from mavflow import (
    MavApplicationAssembler,
    MavApplicationCodec,
    MavApplicationPacket,
)

packet = MavApplicationPacket(
    "vehicle.health",
    {"ready": True, "battery_percent": 84},
    packet_id=42,
)
fragments = MavApplicationCodec.encode(packet)

assembler = MavApplicationAssembler()
decoded = None
for fragment in fragments:
    decoded = assembler.accept(fragment, source_system=1, source_component=1)

if decoded is None:
    raise RuntimeError("The packet was not fully reassembled")

print(decoded.to_dict())
~~~

See [examples/application_packets.py](examples/application_packets.py) for the
complete example. The codec rejects malformed JSON, invalid checksums,
unsupported protocol versions, and packets outside its configured bounds.

<a id="peers-and-requests"></a>
### Peers, requests, and handlers

Set `application_role` when constructing a runtime to enable the application
channel, peer, and dispatcher:

~~~python
from mavflow import MavEndpoint, MavRuntime


def read_health(_request: object) -> dict[str, object]:
    return {"ready": True, "battery_percent": 84}


endpoint = MavEndpoint.udp("0.0.0.0", 14550)
runtime = MavRuntime(endpoint, application_role="vehicle")
handler = runtime.handle("vehicle.health.query", read_health)

try:
    with runtime:
        # The remote peer can now request vehicle.health.query.
        ...
finally:
    handler.cancel()
~~~

A handler receives a `MavApplicationPacket`. It can return a mapping, a
`MavApplicationResult`, or `None`. If the request expects a response,
Mavflow sends the handler result to the requesting peer. Handler registration
returns a cancellable subscription.

Use `notify()` for a one-way application packet and `request()` when the caller
needs a correlated response. Requests carry an ID, accept response type filters,
and raise `TimeoutError` when no matching response arrives before the deadline:

~~~python
with MavRuntime(endpoint, application_role="ground-station") as link:
    response = link.request(
        "vehicle.health.query",
        {},
        response_types="system.ack",
        timeout=3.0,
    )
    print(response.response.payload)
~~~

The default response types are `system.ack` and `system.error`. The peer also
tracks remote liveness and transport availability. Configure request deadlines
and dispatcher capacity for the timing and workload of the application.

<a id="remote-logs"></a>
### Remote log models

`MavRemoteLogRecord` and `MavRemoteLogBatch` describe application log
records that can be converted to and from protocol payloads. They provide
validated record and batch models; they do not configure a Python logging
handler or persist logs locally.

<a id="troubleshooting"></a>
## Troubleshooting

| Symptom | Checks |
| --- | --- |
| `wait_for()` returns `None` or an async receive times out. | Confirm the sender targets the configured UDP/TCP endpoint, the vehicle or simulator is transmitting, and the requested message type is present. Increase the timeout only when the link needs more time to start. |
| An async receive is waiting or queue capacity is reached. | Check `AsyncEventQueue.stats()` and its configured overflow policy. Route only the message types you need, process events faster, or increase `capacity` if memory permits. |
| An application `request()` times out. | Check that the remote peer is running, the packet type and response type match the registered handler, and the selected system/component IDs route to that peer. |

EventFlow `wait_for()` returns `None` when its timeout expires. Async queue
receives raise `asyncio.TimeoutError`; application requests raise
`TimeoutError`. Handle these separately when one task uses more than one API.

<a id="operational-boundaries"></a>
## Security and operational boundaries

Mavflow provides MAVLink transport and protocol models. It does not decide
vehicle policy, command authorization, mission behavior, or how an application
stores telemetry.

MAVLink history, async queue capacity, application fragment assembly, and
handler work are bounded in-memory resources. EventFlow owns message history,
subscriptions, queue overflow behavior, and bounded worker execution; Mavflow
keeps only protocol-specific state such as the single reader, packet
reassembly, request correlation, and MAVLink loss estimates. `runtime.stop()`
keeps EventFlow channels available for a later restart; `runtime.close()` also
closes the EventFlow engine.

<a id="composition"></a>
## Composing the stack

The core telemetry path is `MavEndpoint` → `MavConnection` →
`MavMessageRouter` → `MavClient` → `MavRuntime`. Each layer has
one job: describe the address, own the transport, own the receive loop,
provide a synchronous facade, and coordinate application lifecycle.

An application role adds the packet channel, peer state, and bounded
dispatcher on top of that same client and router:

~~~python
from mavflow import MavClient, MavEndpoint, MavRuntime

endpoint = MavEndpoint.udp("0.0.0.0", 14550)
client = MavClient(endpoint)
runtime = MavRuntime(
    client=client,
    application_role="ground-station",
    workers=2,
    max_pending=128,
)

with runtime:
    runtime.notify("mission.status", {"state": "ready"})
~~~

The transport and router remain shared. Creating an application role does not
start another receive loop for the same connection. Use a separate runtime
when an application needs an independent transport or lifecycle.

<a id="reference"></a>
## Lifecycle and reference

<a id="defaults"></a>
### Defaults

| Setting | Default | Notes |
| --- | --- | --- |
| Endpoint URI | `udp:127.0.0.1:14550` | Local default endpoint. |
| Endpoint system/component | `255 / 0` | Local MAVLink source IDs. |
| Endpoint dialect | `ardupilotmega` | Used by `pymavlink`. |
| Heartbeat timeout | `10 seconds` | Connection setting. |
| Async channel capacity | `128 messages` | Oldest queued message is dropped on overflow. |
| Application role | Disabled | Set `application_role` to create a peer and dispatcher. |
| Application dispatcher | `1 worker, 64 pending jobs` | Runtime defaults when an application role is enabled. |

<a id="components"></a>
### Component responsibilities

| Component | Responsibility |
| --- | --- |
| `MavEndpoint` | Normalize a TCP, UDP, or serial connection description. |
| `MavConnection` | Own the `pymavlink` transport, lifecycle, and raw send/receive operations. |
| `MavMessageRouter` | Own one receive loop and publish keyed message envelopes. |
| `MavClient` | Compose a connection, router, and EventFlow engine. |
| `MavRuntime` | Start and stop the client and optional application services as one unit. |
| `MavApplicationChannel` | Carry application fragments over MAVLink `V2_EXTENSION`. |
| `MavApplicationPeer` | Send notifications and correlated requests and track peer liveness. |
| `MavApplicationDispatcher` | Run registered packet handlers through EventFlow's bounded worker pool. |

### Runtime and EventFlow APIs

| Access | Behavior |
| --- | --- |
| `runtime.messages` | Direct EventFlow `EventBus[MavMessageEnvelope]`; supports `subscribe`, `once`, `wait_for`, and retained history. |
| `runtime.events` | Direct EventFlow `EventEngine` owning named MAVLink and application event channels. |
| `runtime.start()` / `stop()` / `close()` | Start, stop for restart, or stop and close all EventFlow channels. |
| `runtime.notify(packet_type, payload=None)` | Send a one-way application packet; requires an application role. |
| `runtime.request(packet_type, payload=None, ...)` | Send a correlated application request and wait for a response. |
| `runtime.handle(packet_type, handler)` | Register an application handler; returns a cancellable registration. |

Use EventFlow channels such as `mavlink.connection.errors`,
`mavlink.router.errors`, `mavlink.application.packets`, and
`mavlink.dispatcher.errors` for component events. Mavflow does not create a
second aggregate error bus.

<a id="public-types"></a>
### Public types and errors

Mavflow exports transport, envelope, application protocol, peer, dispatcher,
and remote-log types from the `mavflow` package. Its event primitives come
from the direct `flowevent` dependency: import `EventBus`, `EventFilter`,
`EventEngine`, `AsyncEventQueue`, and `BoundedWorkPool` from `eventflow`.

| Area | Mavflow public names |
| --- | --- |
| Transport and routing | `MavEndpoint`, `MavConnection`, `MavMessageRouter`, `MavMessageEnvelope`, `MavHeader`, `MavMessage`, `MavRouterStats` |
| Runtime | `MavClient`, `MavRuntime`, `MavRuntimeState` |
| Application protocol | `MavApplicationPacket`, `MavApplicationCodec`, `MavApplicationAssembler`, `MavApplicationChannel`, `MavApplicationPeer`, `MavApplicationDispatcher`, `MavApplicationHandlerRegistry`, `MavApplicationResult`, `MavApplicationResponse` |
| Remote logs | `MavRemoteLogLevel`, `MavRemoteLogRecord`, `MavRemoteLogBatch` |

`MavHeader` and `MavMessage` are defined in `mavflow.contracts` and
re-exported from `mavflow`. Component error values remain on their own named
EventFlow channels; `MavRouterError`, `MavUnavailableError`, and
`MavApplicationProtocolError` are importable from `mavflow`.

<a id="development"></a>
## Development and license

The project supports Python 3.10 and newer and uses setuptools as its build
backend. From the project root, install development dependencies and run the
checks:

~~~bash
python -m pip install -e ".[dev]"
python run_tests.py
python -m coverage run --branch -m unittest discover -s tests -v
python -m coverage report --fail-under=75
python -m ruff check .
python -m ruff format --check .
python -m pyright
python -m build
python -m twine check dist/*
~~~

The test suite includes a real `pymavlink` UDP loopback check. For that test
alone, run `python -m unittest tests.test_pymavlink_integration -v`.

The benchmark suite uses [pyperf](https://pyperf.readthedocs.io/) for
calibration, worker processes, and JSON result suites. Install the `dev` extra,
then run a benchmark module from the repository root:

~~~bash
python -m benchmarks.benchmark_router --output router-run.json
python -m benchmarks.benchmark_codec --output codec-run.json
python -m pyperf show --metadata --stats router-run.json
python -m pyperf compare_to --table benchmarks/results/router.json router-run.json
~~~

Use a fresh output filename for each run; pyperf does not overwrite an
existing result file. The router benchmark measures single-reader delivery
through a preloaded in-memory connection. The codec benchmark measures
application packet encoding and reassembly. `--fast` is for CI smoke checks;
use the default calibration for local measurements. Results are specific to
their recorded interpreter, platform, source fingerprint, and workload. They
are not deployment guarantees. See
[benchmarks/results/README.md](benchmarks/results/README.md) for the workload
definitions and comparison guidance.

The `CHANGELOG` records the extraction from
[Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink).

Copyright © 2026 TahsinCr. Mavflow is licensed under the MIT License. See
[LICENSE](https://github.com/TahsinCr/python-mavflow/blob/main/LICENSE).

[version-shield]: https://img.shields.io/badge/version-v2.0-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
