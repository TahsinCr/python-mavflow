# Changelog

All notable changes to Mavflow are documented here.

## [Unreleased]

### Added

- Prepare Mavflow 1.0.0 as a standalone Python package for MAVLink transport,
  message routing, application packets, peer coordination, and remote log models.
- Extract the MAVLink package from [Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core/tree/main/mavlink).
- Use [EventFlow](https://github.com/TahsinCr/python-eventflow) for synchronous
  event delivery and subscription lifecycle.
- Add the source project's MAVLink protocol, lifecycle, application protocol,
  and real `pymavlink` UDP loopback test.
- Add Mavflow examples and pyperf-based router/codec benchmark entry points.
- Declare `pymavlink` as a required runtime dependency for MAVLink transport.

### Changed

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
