# Mavflow pyperf suites

Files in this directory are pyperf JSON suites. Pyperf records calibrated
samples, interpreter and platform details; the shared Mavflow runner adds the
source revision, dirty state, source fingerprint, and workload metadata.
Results from the copied EventFlow suite were removed.

Run from the project root to create a suite. Pyperf refuses to overwrite an
existing JSON file. Use `--append` only to add more samples from the same source,
workload, and environment; use a new file after changing code or workload:

```bash
python -m benchmarks.benchmark_router --output router-run.json
python -m benchmarks.benchmark_codec --output codec-run.json
```

These commands create fresh files in the project root. Pick new names for later
runs; keep the reference suites in this directory unchanged unless you intend
to replace them after reviewing the new measurements.

Current reference workloads:

| Suite | Workload |
| --- | --- |
| `router.json` | 1,000 messages, 1 subscriber, 100% HEARTBEAT matches. |
| `router-no-match.json` | 1,000 messages, 10 subscribers, 0% HEARTBEAT matches. |
| `router-filtered.json` | 1,000 messages, 10 subscribers, 10% HEARTBEAT matches. |
| `router-half-match.json` | 1,000 messages, 10 subscribers, 50% HEARTBEAT matches. |
| `router-all-match.json` | 1,000 messages, 10 subscribers, 100% HEARTBEAT matches. |
| `codec.json` | 2,048-byte application packet encode and reassembly. |

The refreshed router benchmark now reports the correct MAVLink message IDs,
calls `get_seq()`, and measures the sequence-quality path. Its timed region
starts when the preloaded queue is released and ends after final delivery. The
previous router suite omitted the sequence path, so those timings are not a
direct before/after comparison. The six refreshed pyperf suites use source
revision `1ccd2b5d1120c7aa917c3d50ba301c15e2c7a8b7` and fingerprint
`b02a28fa1ed1623f9959dd854b978c4d9f331a7862d9f0493485510161662c04`. Each run
reported host instability; treat these as workload records, not evidence of a
performance gain.

The router workload times delivery through Mavflow's single-reader router from a
preloaded in-memory queue. Its timed interval begins when the queue is released
and ends after the final envelope reaches its observer; queue construction,
router startup, and shutdown are outside the timed region. It does not measure a
serial device, UDP stack, or autopilot. The codec workload encodes and
reassembles one fixed-size application packet per operation.

Inspect metadata and statistics with `python -m pyperf show --metadata --stats
router-run.json`. Compare a new router run with the stored suite using `python -m
pyperf compare_to --table benchmarks/results/router.json router-run.json`. Use matching source fingerprints,
interpreters, platforms, and workload metadata when interpreting changes.
`--fast` is reserved for CI smoke checks; use the default pyperf calibration for
measurements. Microbenchmarks are not general performance guarantees.
