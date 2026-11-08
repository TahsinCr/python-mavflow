[![Version: v1.0.0][version-shield]](https://github.com/TahsinCr/python-mavflow/blob/main/CHANGELOG.md)
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

The installed package exposes its version as `mavflow.__version__`:

~~~python
from mavflow import __version__

print(__version__)  # 1.0.0
~~~

<a id="first-heartbeat"></a>
### Receive your first heartbeat

This example listens on UDP port 14550, waits for a heartbeat, and then
observes subsequent heartbeat messages. A vehicle or simulator must send
MAVLink packets to the configured endpoint.

~~~python
from mavflow import MavlinkEndpoint, MavlinkRuntime


def show_heartbeat(message: object) -> None:
    print(message)


endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
with MavlinkRuntime(endpoint) as link:
    subscription = link.subscribe("HEARTBEAT", show_heartbeat)
    try:
        first = link.wait_for("HEARTBEAT", timeout=10.0)
        print("First heartbeat:", first)
    finally:
        subscription.cancel()
~~~

The runtime starts the connection and router when the context is entered, then
stops them when it exits. `wait_for()` returns the first matching message or
`None` when the timeout expires. See
[examples/telemetry.py](examples/telemetry.py) for a command-line version.

<a id="choose-api-layer"></a>
### Choose an API layer

| Component | Use it for |
| --- | --- |
| `MavlinkConnection` | Opening a `pymavlink` transport and sending or receiving raw MAVLink messages. |
| `MavlinkMessageRouter` | Owning the receive loop, filtering messages, and keeping bounded message history. |
| `MavlinkClient` | Using a connection and router through one synchronous facade. |
| `MavlinkRuntime` | Owning the client lifecycle and optionally composing application peers and handlers. |
| `MavlinkAsyncChannel` | Delivering routed messages to an asyncio event loop through a bounded queue. |

Use `MavlinkRuntime` for the normal application entry point. Compose the lower
level types directly when an application needs to own a transport or router
itself.

<a id="core-concepts"></a>
## Core concepts

<a id="endpoints"></a>
### Endpoints and connections

`MavlinkEndpoint` is an immutable description of a connection. The helpers
`tcp(host, port)`, `udp(host, port)`, and `serial(device, baud=...)` build
common endpoint forms:

~~~python
from mavflow import MavlinkEndpoint

udp = MavlinkEndpoint.udp("0.0.0.0", 14550)
tcp = MavlinkEndpoint.tcp("192.0.2.10", 5760)
serial = MavlinkEndpoint.serial("/dev/ttyUSB0", baud=57600)
~~~

An endpoint also carries the local system and component IDs, MAVLink dialect,
reconnect setting, and heartbeat timeout. Its default URI is
`udp:127.0.0.1:14550`. Use the address and transport mode expected by the
vehicle, radio, simulator, or router.

`MavlinkConnection` owns the underlying `pymavlink` connection. Importing
`mavflow` does not open a device or load the transport module; `pymavlink` is
loaded when a real connection is created. The connection provides lifecycle
methods, raw message access, send helpers, and vehicle target information.

<a id="router"></a>
### One reader and the message router

`MavlinkMessageRouter` owns the receive loop for a connection. It reads each
incoming message once, records the message and sequence state, then delivers
matching messages to active subscriptions.

This single-reader ownership keeps transport reads and routing in one place.
Router callbacks run as part of message delivery, so keep them short. Move
blocking work to an application-owned worker instead of holding up the receive
loop.

The router exposes current statistics, the latest matching message, a bounded
history, and a wait operation. `MavlinkClient` combines one connection and one
router so callers do not have to wire those pieces together manually.

<a id="filtering"></a>
### Filtering and message history

`MavlinkMessageFilter` can match message types, source system IDs, source
component IDs, message IDs, a message condition, or a predicate. Combine
criteria when a callback should receive only a subset of the telemetry:

~~~python
from mavflow import MavlinkMessageFilter

position_filter = MavlinkMessageFilter.for_types(
    "GLOBAL_POSITION_INT",
    source_systems=1,
)
~~~

Pass the filter to `subscribe()` to register a callback. A type filter can also
be supplied directly to `wait_for()`,
`latest()`, or the router's `history()` method. Use `limit` with `history()` to
retrieve only the most recent matching messages. History is in memory and
bounded; it is not a durable telemetry log.

<a id="lifecycle"></a>
### Lifecycle and composition

`MavlinkRuntime` composes a client and, when requested, an application channel,
peer, and dispatcher. `start()` starts the transport and owned application
services in dependency order. `stop()` shuts them down in reverse order and
collects shutdown errors. A runtime can be used as a synchronous context
manager, as in the first example.

Create separate runtimes for separate connections. A runtime can also accept
an existing `MavlinkClient` when an application needs to share or configure
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

`MavlinkRuntime.subscribe()` accepts a message type or a filter and a callback.
The returned subscription can be cancelled independently of the runtime:

~~~python
from mavflow import MavlinkEndpoint, MavlinkMessageFilter, MavlinkRuntime


def report_position(message: object) -> None:
    print(message)


position_filter = MavlinkMessageFilter.for_types(
    "GLOBAL_POSITION_INT",
    source_systems=1,
)

with MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550)) as link:
    position_subscription = link.subscribe(position_filter, report_position)
    heartbeat_subscription = link.subscribe("HEARTBEAT", print)
    try:
        link.wait_for("HEARTBEAT", timeout=10.0)
    finally:
        position_subscription.cancel()
        heartbeat_subscription.cancel()
~~~

Message filters can also restrict source components and message IDs, or apply
a condition or predicate to a received message. The message object is provided
by `pymavlink`; use its generated message accessors for fields such as source
system, source component, and message type.

<a id="wait-and-history"></a>
### Wait for a message and inspect history

Use `wait_for()` when code needs one matching message as a value. Use `latest()`
for the most recent match already observed, or `history()` to inspect retained
messages:

~~~python
from mavflow import MavlinkEndpoint, MavlinkRuntime

with MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550)) as link:
    first_heartbeat = link.wait_for("HEARTBEAT", timeout=10.0)
    most_recent = link.latest("HEARTBEAT")
    recent_messages = link.router.history("HEARTBEAT", limit=20)
    router_state = link.state.router
~~~

`wait_for()` returns `None` after its timeout. `latest()` also returns `None`
until a matching message has arrived. History contains message envelopes and
is read in publication order. Router statistics include received messages,
receive and dispatch errors, estimated dropped messages, and delivery quality.

Use `send()` or `send_named()` when sending a MAVLink message through the
client or runtime. The method name and arguments follow the generated
`pymavlink` dialect API.

<a id="async-applications"></a>
## Async applications

MAVLink transport reads remain owned by the synchronous router. Use
`MavlinkAsyncChannel` to forward selected messages into an asyncio event loop.
The channel has a bounded queue; when it fills, it drops the oldest queued
message and increments `dropped_messages`.

Start the channel from inside its running event loop. Stop it before stopping
the runtime so it can cancel its router subscription cleanly:

~~~python
import asyncio

from mavflow import MavlinkAsyncChannel, MavlinkEndpoint, MavlinkRuntime


async def main() -> None:
    runtime = MavlinkRuntime(MavlinkEndpoint.udp("0.0.0.0", 14550))
    channel = MavlinkAsyncChannel(
        runtime.router,
        "HEARTBEAT",
        maxsize=64,
    )
    channel.start()
    try:
        runtime.start()
        message = await channel.receive(timeout=10.0)
        print(message)
    finally:
        channel.stop()
        runtime.stop()


asyncio.run(main())
~~~

`receive(timeout=...)` raises a timeout exception when no message arrives
before the deadline. `receive_nowait()` reads an already queued message and
raises `asyncio.QueueEmpty` when the queue is empty. The channel exposes its
pending count and drop count so applications can monitor bounded delivery.

<a id="application-packets"></a>
## Application packets

The optional application protocol carries JSON-compatible Mavflow packets over
MAVLink `V2_EXTENSION` messages. It is separate from ordinary telemetry
routing; applications that only need standard MAVLink messages do not need to
enable it.

<a id="codec"></a>
### Encode and reassemble packets

`MavlinkApplicationPacket` stores a packet type, JSON-compatible payload,
packet ID, timestamp, source IDs, and response flag. `MavlinkApplicationCodec`
encodes a packet into bounded fragments, adds a checksum, and validates
fragments during decode. `MavlinkApplicationAssembler` reassembles fragments
while bounding incomplete packet count, memory, and lifetime.

The codec can be used without opening a vehicle connection:

~~~python
from mavflow import (
    MavlinkApplicationAssembler,
    MavlinkApplicationCodec,
    MavlinkApplicationPacket,
)

packet = MavlinkApplicationPacket(
    "vehicle.health",
    {"ready": True, "battery_percent": 84},
    packet_id=42,
)
fragments = MavlinkApplicationCodec.encode(packet)

assembler = MavlinkApplicationAssembler()
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
from mavflow import MavlinkEndpoint, MavlinkRuntime


def read_health(_request: object) -> dict[str, object]:
    return {"ready": True, "battery_percent": 84}


endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
runtime = MavlinkRuntime(endpoint, application_role="vehicle")
handler = runtime.handle("vehicle.health.query", read_health)

try:
    with runtime:
        # The remote peer can now request vehicle.health.query.
        ...
finally:
    handler.cancel()
~~~

A handler receives a `MavlinkApplicationPacket`. It can return a mapping, a
`MavlinkApplicationResult`, or `None`. If the request expects a response,
Mavflow sends the handler result to the requesting peer. Handler registration
returns a cancellable subscription.

Use `notify()` for a one-way application packet and `request()` when the caller
needs a correlated response. Requests carry an ID, accept response type filters,
and raise `TimeoutError` when no matching response arrives before the deadline:

~~~python
with MavlinkRuntime(endpoint, application_role="ground-station") as link:
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

`MavlinkRemoteLogRecord` and `MavlinkRemoteLogBatch` describe application log
records that can be converted to and from protocol payloads. They provide
validated record and batch models; they do not configure a Python logging
handler or persist logs locally.

<a id="operational-boundaries"></a>
## Security and operational boundaries

Mavflow provides MAVLink transport and reusable routing primitives. It does not
decide vehicle policy, command authorization, mission behavior, or how an
application stores telemetry.

Router history, the asyncio channel, application fragment assembly, and
dispatcher work are bounded in-memory resources. They are cleared with their
owning lifecycle and are not durable queues. Monitor router statistics,
channel drops, peer state, and runtime/application error events when operating
a long-running service.

MAVLink endpoints and network access must match the vehicle and deployment
configuration. Applications remain responsible for their transport security,
command policy, and validation of business-level payloads.

<a id="composition"></a>
## Composing the stack

The core telemetry path is `MavlinkEndpoint` → `MavlinkConnection` →
`MavlinkMessageRouter` → `MavlinkClient` → `MavlinkRuntime`. Each layer has
one job: describe the address, own the transport, own the receive loop,
provide a synchronous facade, and coordinate application lifecycle.

An application role adds the packet channel, peer state, and bounded
dispatcher on top of that same client and router:

~~~python
from mavflow import MavlinkClient, MavlinkEndpoint, MavlinkRuntime

endpoint = MavlinkEndpoint.udp("0.0.0.0", 14550)
client = MavlinkClient(endpoint)
runtime = MavlinkRuntime(
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
| `MavlinkEndpoint` | Normalize a TCP, UDP, or serial connection description. |
| `MavlinkConnection` | Own the `pymavlink` transport, lifecycle, and raw send/receive operations. |
| `MavlinkMessageRouter` | Own one receive loop, filter callbacks, sequence state, and bounded history. |
| `MavlinkClient` | Compose a connection and router behind a synchronous facade. |
| `MavlinkRuntime` | Start and stop the client and optional application services as one unit. |
| `MavlinkAsyncChannel` | Bridge filtered router delivery into a bounded asyncio queue. |
| `MavlinkApplicationChannel` | Carry application fragments over MAVLink `V2_EXTENSION`. |
| `MavlinkApplicationPeer` | Send notifications and correlated requests and track peer liveness. |
| `MavlinkApplicationDispatcher` | Run registered packet handlers with bounded worker capacity. |

### Runtime methods

| Method | Behavior |
| --- | --- |
| `start()` / `stop()` | Start or stop the runtime and its owned services. |
| `subscribe(filter, callback)` | Route matching MAVLink messages to a synchronous callback. |
| `once(filter, callback)` | Register a callback for one matching message. |
| `wait_for(filter, timeout=None)` | Return one matching message or `None` after timeout. |
| `latest(filter=None)` | Return the latest matching message or `None`. |
| `notify(packet_type, payload=None)` | Send a one-way application packet; requires an application role. |
| `request(packet_type, payload=None, ...)` | Send a correlated application request and wait for a response. |
| `handle(packet_type, handler)` | Register an application handler; returns a cancellable subscription. |

<a id="public-types"></a>
### Public types and errors

The package exports these public names:

| Area | Public names |
| --- | --- |
| Transport and routing | `MavlinkEndpoint`, `MavlinkConnection`, `MavlinkMessageRouter`, `MavlinkMessageFilter`, `MavlinkMessageEnvelope`, `MavlinkHeader`, `MavlinkMessage`, `MavlinkRouterStats`, `MessageCache` |
| Runtime and async | `MavlinkClient`, `MavlinkRuntime`, `MavlinkRuntimeState`, `MavlinkAsyncChannel` |
| Application protocol | `MavlinkApplicationPacket`, `MavlinkApplicationCodec`, `MavlinkApplicationAssembler`, `MavlinkApplicationChannel`, `MavlinkApplicationPeer`, `MavlinkApplicationDispatcher`, `MavlinkApplicationHandlerRegistry`, `MavlinkApplicationResult`, `MavlinkApplicationResponse` |
| Remote logs | `MavlinkRemoteLogLevel`, `MavlinkRemoteLogRecord`, `MavlinkRemoteLogBatch` |

`MavlinkHeader` and `MavlinkMessage` are defined in `mavflow.contracts` and
re-exported from `mavflow`.

Key error types include `MavlinkUnavailableError`, `MavlinkRouterError`,
`MavlinkRuntimeError`, and `MavlinkApplicationProtocolError`. Import the
specific error classes from `mavflow` when an application needs to handle them.

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

[version-shield]: https://img.shields.io/badge/version-v1.0.0-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
