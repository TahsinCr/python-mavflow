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
| `router.json` | 1,000 messages, 1 subscriber, 100% HEARTBEAT matches | 14.2 ± 1.3 µs |
| `router-no-match.json` | 1,000 messages, 10 subscribers, 0% HEARTBEAT matches | 17.1 ± 0.9 µs |
| `router-filtered.json` | 1,000 messages, 10 subscribers, 10% HEARTBEAT matches | 18.5 ± 1.2 µs |
| `router-half-match.json` | 1,000 messages, 10 subscribers, 50% HEARTBEAT matches | 22.3 ± 0.9 µs |
| `router-all-match.json` | 1,000 messages, 10 subscribers, 100% HEARTBEAT matches | 25.7 ± 1.4 µs |
| `codec.json` | Encode and reassemble one 2,048-byte application packet | 154 ± 1 µs |

All six suites were generated from source revision
`222c78f5ffcec1417bd90b6c51139299342c5824`, with `mavflow_source_dirty=false`
and source fingerprint
`e677d7af6ae82398f786c1e9a19aa8cc912f9d5ae79f6901f40b3f792f3f3480`. The
environment was CPython 3.14.9, pyperf 2.11.0, Arch Linux 7.2.14, and an Intel
Core i7-12700H. Router suites used `--rigorous`, but pyperf still reported that
the results may be unstable because the samples did not establish 95% certainty
within 1% variation. The codec suite did not report instability. Treat all
numbers as machine-specific workload records; the router measurements do not
establish a performance improvement or general performance guarantee.

The router benchmark uses the sequence-quality path and correct MAVLink message
IDs. Its timed interval starts when a preloaded in-memory queue is released and
ends after the final envelope reaches its observer; queue construction, router
startup, and shutdown are excluded. It does not measure a serial device, UDP
stack, or autopilot. The codec workload encodes and reassembles one fixed-size
application packet per operation.

The previous six reference suites are retained under
[`archive/pre-m5-audit-1ccd2b5/`](archive/pre-m5-audit-1ccd2b5/). They were
generated from revision `1ccd2b5d1120c7aa917c3d50ba301c15e2c7a8b7` with source
fingerprint `b02a28fa1ed1623f9959dd854b978c4d9f331a7862d9f0493485510161662c04`.
The source and benchmark workload changed since then, so these archived files
are not a direct before/after comparison with the current suites.
