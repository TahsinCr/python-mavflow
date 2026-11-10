# Changelog

All notable changes to Mavflow are documented here.

## [Unreleased]

### Added

- Measure router callback fan-out with a configurable number of matching
  EventFlow subscribers in the pyperf router benchmark.
- Configure optional MAVLink 2 signing on `MavEndpoint` before heartbeat
  discovery and keep signing keys out of endpoint representations.
- Filter initial heartbeat discovery by optional expected system/component IDs
  and use those IDs as the runtime peer's default target.
- Use the released EventFlow 1.3.2 API directly: message-type filters use
  `EventFilter` predicates, and `AsyncEventQueue` buffers all envelope types.

### Changed

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
  when estimating sequence loss.
- Validate finite timeout values and handler response payloads before reporting
  them as handled.
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
