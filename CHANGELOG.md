# Changelog

All notable changes to Mavflow are documented here.

## [Unreleased]

## [v2.0] - 2026-11-13

### Fixed

- Reject runtime `start()` and `reconnect()` calls from managed dispatcher,
  reader, monitor, and startup threads before taking the runtime lifecycle lock;
  apply the same early guard to direct client and peer lifecycle entry points.
- Reject remote-log text fields that cannot be encoded as UTF-8 when the record
  is created.
- Retry peer and channel cleanup through the runtime when application startup
  fails before completing.
- Reject peer shutdown reentry from its startup state callback before changing
  runtime-owned component state.
- Return stable error codes for unexpected handler failures and invalid handler
  responses without exposing exception class names to remote peers.
- Serialize runtime peer-identity validation with client startup and reject
  reentrant client lifecycle calls from a connection startup callback.
- Reject decoded JSON strings containing unpaired Unicode surrogates so decode
  and UTF-8 re-encoding accept the same wire-format values.
- Run the decoded-value Unicode walk only when the JSON body contains a `\u`
  escape, keeping the normal decode path free of an extra Python tree traversal.

### Added

- Support explicit system/component source pairs through application channel
  `allowed_sources` while preserving the independent source allowlists.
- Document best-effort request delivery, in-memory duplicate suppression,
  payload wire-time limits, callback lifecycle restrictions, and dispatcher
  saturation behavior in both README languages.
- Measure router callback fan-out across subscriber counts and 0%, 10%, 50%,
  and 100% message-filter match ratios in the pyperf router benchmark; include
  the MAVLink sequence path in benchmark messages.
- Configure optional MAVLink 2 signing on `MavEndpoint` before heartbeat
  discovery and keep signing keys out of endpoint representations.
- Filter initial heartbeat discovery by optional expected system/component IDs
  and use those IDs as the runtime peer's default target.
- Use the released EventFlow 1.3.2 API directly: message-type filters use
  `EventFilter` predicates, and `AsyncEventQueue` buffers all envelope types.

### Changed

- Require integer packet and source identities; reject booleans, floats, and
  numeric strings instead of silently converting them.
- Submit automatic responses for unknown application requests to EventFlow's
  bounded dispatcher pool; pool saturation now reports an error and leaves the
  caller to its request deadline instead of sending on the router thread.
- Route envelopes from the extracted [Vehicle Autonomy Core MAVLink source](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink)
  through EventFlow's `EventBus` and expose
  EventFlow's native `EventEngine`, `EventFilter`, `EventHistory`, and
  `AsyncEventQueue` APIs directly. Mavflow 2.0 removes its duplicate message
  filter, cache, async channel, and subscription/wait facades without
  compatibility aliases.
- Run application handlers with EventFlow's `BoundedWorkPool`; dispatcher
  `max_pending` now means waiting jobs in addition to active workers and accepts
  zero pending slots.
- Forward `MavConnection.receive(message_types=...)` directly to pymavlink's
  native type filter (`str`, `list[str]`, or `set[str]`); type names are passed
  unchanged and should use pymavlink's message-name casing.

### Removed

- Remove `MavMessageFilter`, `MessageCache`, `MavAsyncChannel`,
  `MavRuntimeError`, and custom `subscribe`/`wait_for`/history facades. Consume
  `MavMessageEnvelope` values from `runtime.messages` or named `runtime.events`
  channels instead.
- Remove duplicate peer packet publication; the peer exposes the application
  channel's EventFlow packet bus directly.
- Remove dispatcher thread-name configuration because EventFlow 1.3.2 owns
  worker naming through `BoundedWorkPool`.

### Fixed

- Forward the codec payload shape to pyperf worker processes so they execute the
  workload recorded in the suite metadata.
- Send a fixed safe message for unexpected application handler exceptions while
  retaining the original exception on the local dispatcher error bus.
- Decode completed application packets before recording their IDs for duplicate
  suppression, strictly validate correlated request IDs, and enforce public
  fragment and packet size limits.
- Publish connection startup errors after releasing the lifecycle lock and roll
  back a router connection when its receive thread cannot start; retain failed
  cleanup for a later stop retry.
- Mark every EventFlow dispatcher worker path so shutdown from an unsupported-
  request response cannot wait for its own worker.
- Reject runtime startup when a default peer's endpoint-derived identity was
  changed after runtime construction.
- Cover lone-surrogate encoding failures in the protocol-error contract.
- Correct the 8N1 serial lower-bound calculation for the five-byte
  `V2_EXTENSION` routing/message-type fields in both README languages.
- Require the published EventFlow 1.3.2 release and install it through normal
  package resolution in local environments and GitHub Actions.
- Validate application peer responses against the destination system and
  component IDs, and ignore unrelated peers for liveness and probe state.
- Allow runtime `peer_options` to specify an expected peer system/component
  before starting the connection.
- Keep router processing alive when an envelope or delivery-quality update
  fails, and report those failures through the router error bus.
- Serialize runtime lifecycle transitions and leave the EventFlow engine open
  when component cleanup has not completed.
- Reject malformed application packet fields, nonzero reserved header flags,
  and oversized remote-log batches before constructing their records.
- Bound fragment assemblies per MAVLink source and ignore out-of-order packets
  when estimating sequence loss; resynchronize after ambiguous large forward
  jumps instead of leaving the sequence baseline stale.
- Validate finite timeout values and handler response payloads before reporting
  them as handled.
- Apply known peer system/component IDs to the default application channel's
  source allowlist and keep explicitly supplied channel source filters intact.
- Reject IPv6 endpoints unsupported by pymavlink's built-in TCP/UDP transports
  and reject signing when the active transport does not use MAVLink 2.
- Roll back partial peer and dispatcher startup, prevent direct router
  restart during connection cleanup, and count request send time against its
  monotonic deadline.
- Recheck application packet source identity before dispatching handlers,
  including dynamic runtime targets, and reject callable targets whose peer
  identity is not yet known.
- Serialize direct peer start/stop transitions and retry application-channel
  cleanup after a failed stop.
- Retain transports whose connection-start rollback could not close them, so a
  later stop or connect can retry cleanup without reporting a false connection.
- Deliver connection state events in transition order across concurrent
  connect/stop calls.
- Raise the branch coverage gate from 75% to 85% and document that request
  deadlines cannot interrupt a synchronous transport send already in progress.
- Use the correct MAVLink message IDs in router benchmark fakes and report the
  measured router interval accurately.
- Preflight the complete application response, including correlation metadata,
  before sending it.
- Keep application channel local IDs aligned with endpoint reconfiguration,
  serialize packet-ID allocation with pending request/probe registration, and
  wait for in-flight channel callbacks during stop.
- Reject runtime stop/close calls from the router receive callback before
  tearing down any owned component.
- Keep client cleanup under runtime ownership when startup fails, and reject
  runtime lifecycle reentry from peer monitor and startup callbacks before
  partial shutdown.
- Serialize client start, stop, and endpoint configuration; reject stale
  dispatcher generations before sending unsupported-request responses.
- Roll back router startup when reader thread construction or start fails.
- Enforce the JSON nesting limit on both encode and decode, using a fast
  delimiter-count path for shallow payloads and string-aware scanning otherwise.
- Send peer pongs and automatic dispatcher replies through existing worker
  threads rather than performing transport I/O on the router callback path.
- Reject duplicate JSON object keys, excessive nesting, malformed fragment
  lengths, and non-zero fragment padding before application dispatch.
- Refresh the six pyperf router and codec reference suites against the fifth
  audit's fixed source revision; archive the previous baseline and document
  benchmark stability limits.
- Document that pymavlink condition expressions must come from trusted code,
  and that `is_connected` reports an open raw connection rather than peer health.

## [v1.0] - 2026-11-09

### Added

- Prepare Mavflow v1.0 as a standalone Python package for MAVLink transport,
  message routing, application packets, peer coordination, and remote log models.
- Extract the MAVLink package from [Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink).
- Use [EventFlow](https://github.com/TahsinCr/python-eventflow) for synchronous
  event delivery and subscription lifecycle.
- Add the source project's MAVLink protocol, lifecycle, application protocol,
  and real `pymavlink` UDP loopback test.
- Add Mavflow examples and pyperf-based router/codec benchmark entry points.
- Add async-first quick-start documentation and runnable asyncio, filtered
  telemetry, and message-history examples in English and Turkish.
- Declare `pymavlink` as a required runtime dependency for MAVLink transport.

### Changed

- Rename public `Mavlink*` classes and types to the shorter `Mav*` names.
- Upgrade GitHub Actions checkout and Python setup to Node.js 24-compatible releases.
- Use the `backports.strenum` and `exceptiongroup` packages for Python 3.10
  compatibility instead of local implementations; use built-ins on Python 3.11+.
- Keep component lifecycle management in Mavflow instead of inheriting the
  lifecycle base copied from Vehicle Autonomy Core.
- Expose MAVLink structural protocols from the public `mavflow.contracts`
  module and keep payload immutability behavior with its owning data models.
- Remove the copied private `_contracts.py` helper module.
- Make the `mavflow` package importable without the Vehicle Autonomy Core source
  tree; defer importing `pymavlink` until a real transport is opened.
- Replace copied EventFlow documentation, examples, tests, benchmark records,
  and API snapshots with Mavflow-specific material.
- Use pyperf calibration, worker processes, JSON suites, and shared source/workload
  metadata instead of hand-timed benchmark reports.
