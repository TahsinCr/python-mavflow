# Mavflow pyperf suites

These files are pyperf JSON suites. Each suite records calibrated samples,
interpreter and platform details, plus Mavflow's source revision, working-tree
state, source fingerprint, and workload metadata.

## Reproduce a measurement

Run from the project root and choose a fresh output path. Pyperf refuses to
overwrite an existing file; `--append` is only appropriate for more samples of
the same source revision, workload, and environment.

```bash
python -m benchmarks.benchmark_router --output router-run.json
python -m benchmarks.benchmark_codec --output codec-run.json
```

Use `--fast` only for CI smoke checks. Inspect a suite with
`python -m pyperf show --metadata --stats router-run.json`. Compare results only
when source fingerprints, interpreter, platform, and workload metadata match:

```bash
python -m pyperf compare_to --table benchmarks/results/router.json router-run.json
```

## Current reference workloads

| Suite | Workload | Mean ± standard deviation |
| --- | --- | ---: |
| `router.json` | 1,000 messages, 1 subscriber, 100% HEARTBEAT matches | 14.0 ± 0.8 µs |
| `router-no-match.json` | 1,000 messages, 10 subscribers, 0% HEARTBEAT matches | 17.6 ± 1.1 µs |
| `router-filtered.json` | 1,000 messages, 10 subscribers, 10% HEARTBEAT matches | 19.1 ± 1.1 µs |
| `router-half-match.json` | 1,000 messages, 10 subscribers, 50% HEARTBEAT matches | 23.0 ± 1.7 µs |
| `router-all-match.json` | 1,000 messages, 10 subscribers, 100% HEARTBEAT matches | 26.4 ± 1.8 µs |
| `codec.json` | Encode and reassemble one 2,048-byte application packet | 96.0 ± 8.1 µs |

All six suites were generated with `--rigorous` from source revision
`6244d3f6e848d4d2c113731311bcaf0b4ab2d2b1`, with
`mavflow_source_dirty=false` and source fingerprint
`5636e578dd3c23ad5f6a6905d5c183d8b54bca90633ff5093646c723e8ac7f9a`. The
environment was CPython 3.14.9, pyperf 2.11.0, Arch Linux 7.2.14, and an Intel
Core i7-12700H. Each suite contains 120 calibrated values. Pyperf flagged all
five router suites as unstable because the samples did not establish 95%
certainty within 1% variation; it also flagged the codec suite for variation
and outliers. Treat the means as machine-specific observations, not a
performance guarantee or a stable router comparison.

The router benchmark uses the sequence-quality path and correct MAVLink message
IDs. Its timed interval starts when a preloaded in-memory queue is released and
ends after the final envelope reaches its observer; queue construction, router
startup, and shutdown are excluded. It does not measure a serial device, UDP
stack, or autopilot. The codec workload encodes and reassembles one fixed-size
application packet per operation.

The previous six reference suites are retained under
[`archive/pre-m6-audit-67d4946/`](archive/pre-m6-audit-67d4946/). Their codec
mean was 154 ± 1 µs for the same 2,048-byte flat workload. They came from
source revision `222c78f5ffcec1417bd90b6c51139299342c5824`, fingerprint
`e677d7af6ae82398f786c1e9a19aa8cc912f9d5ae79f6901f40b3f792f3f3480`, and the
same interpreter, platform, and CPU listed above. `pyperf compare_to` reports
the current 96.0 µs mean as 1.60x faster, but the current suite is unstable;
treat that difference as provisional rather than a confirmed gain.
Three clean-source repeats measured 87.9 ± 9.3, 92.9 ± 12.6, and 96.0 ± 8.1 µs;
all repeat files are kept in
[`archive/m6-codec-repeats-6244d3f/`](archive/m6-codec-repeats-6244d3f/).

The codec payload-size/shape smoke matrix covered 128, 512, 2,048, 8,192, and
16,000 bytes with `flat`, `brackets`, and `nested` values. All 15 worker suites
reported the requested workload and clean source revision. These used `--fast`
to validate argument forwarding and metadata; they are not timing evidence.

The earlier six suites are also retained under
[`archive/pre-m5-audit-1ccd2b5/`](archive/pre-m5-audit-1ccd2b5/). They were
generated from revision `1ccd2b5d1120c7aa917c3d50ba301c15e2c7a8b7` with source
fingerprint `b02a28fa1ed1623f9959dd854b978c4d9f331a7862d9f0493485510161662c04`.
Their source and benchmark workload differ from later suites.
