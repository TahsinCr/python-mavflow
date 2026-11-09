# Changelog

All notable changes to Mavflow are documented here.

## [Unreleased]

### Changed

- Route envelopes from the extracted [Vehicle Autonomy Core MAVLink source](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink)
  through EventFlow's indexed `EventBus` and expose
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

### Fixed

- Install EventFlow 1.4.0 from its pinned upstream commit in GitHub Actions while
  that required version is not yet available on PyPI.

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
