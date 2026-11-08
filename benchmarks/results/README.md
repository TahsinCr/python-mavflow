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

The router workload times delivery through Mavflow's single-reader router from a
preloaded in-memory queue; queue construction, router startup, and shutdown are
outside its timed region. It does not measure a serial device, UDP stack, or
autopilot. The codec workload encodes and reassembles one fixed-size application
packet per operation.

Inspect metadata and statistics with `python -m pyperf show --metadata --stats
router-run.json`. Compare a new router run with the stored suite using `python -m
pyperf compare_to --table benchmarks/results/router.json router-run.json`. Use matching source fingerprints,
interpreters, platforms, and workload metadata when interpreting changes.
`--fast` is reserved for CI smoke checks; use the default pyperf calibration for
measurements. Microbenchmarks are not general performance guarantees.
