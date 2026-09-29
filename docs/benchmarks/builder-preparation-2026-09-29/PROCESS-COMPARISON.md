# Candidate preparation: threads versus processes

This is a diagnostic experiment for deciding whether process isolation deserves
an implementation trial. It does not change the production execution model or
prove that any observed improvement comes only from Python's interpreter lock.

`scripts/benchmark_preparation_execution.py` compares the same candidate source
with four threads sharing one interpreter and four fresh spawned interpreters.
It reuses the fixture and filesystem comparison helpers in the frozen
`scripts/benchmark_environment_preparation.py`; stage both scripts together.
The original benchmark and its retained receipts are unchanged.

```sh
python3 benchmark_preparation_execution.py \
  --source-root /path/to/frozen/candidate \
  --work-root /work \
  --modules 2000 --repeats 2 \
  --output /work/preparation-process-comparison-r1
```

Use the candidate's Python environment, with its declared dependencies, and a
fresh output path. Run on one otherwise idle host. The default order is
threads/processes/processes/threads. Each arm has a fresh supervisor interpreter;
the process arm uses `multiprocessing`'s explicit `spawn` context, not inherited
runtime objects from `fork`. Each worker imports the pinned candidate, obtains
its own temporary tree, materializes all four synthetic layers, and uses
`consume_private_diffs=True` when squashing them.

Both arms receive phase commands over local pipes. All workers finish extraction
before the untimed materialized-tree snapshots start; all snapshots finish before
squashing starts. Output and overlay validation occur only after all timed
squashing finishes. No validator competes with another publication's timed work.
This staged comparison isolates contention within each operation; it does not
reproduce production's arbitrary overlap between different phases.

## Read the report

`summary.json` retains every arm, module fingerprints, fixture identity, worker
timings, exact tree comparisons, capability probes, and averaged measurements.
`exact_semantics_equal` must be true. Runtime or source fingerprint differences
also fail that gate. Each trial must report
`workers_reaped_and_trees_removed: true`; Linux root qualification must have no
capability skips in any output. Comparison inherits the original helper's
documented exclusions, including generated-whiteout modification time.

- `startup_to_ready_wall_seconds` includes worker creation, candidate imports,
  scratch initialization, and readiness. The outer supervisor interpreter's
  own startup is excluded equally in both arms.
- `preparation_wall_seconds` adds the extraction and squash stage wall times,
  including their bounded control-message overhead.
- `startup_plus_preparation_wall_seconds` measures fresh-worker startup plus
  those stages. For a conservative cold offload estimate, compare this process
  value against **thread preparation alone**, because the production node
  already imported its runtime.
- `cleanup_and_reap_wall_seconds` measures scratch cleanup and worker shutdown.
  Add it when evaluating a design that owns cleanup inside each invocation.
- `preparation_cpu_seconds` sums worker CPU inside timed phases: per-thread CPU
  for the shared interpreter and per-process CPU for spawned workers. Startup,
  supervisor IPC, validation, and cleanup CPU are outside this number.
- `whole_trial_including_validation_seconds` includes deliberate validation
  overhead and is not a proposed production-latency estimate.

The benchmark uses shared warm fixture bytes and new output trees; it does not
drop caches. It performs no registry, network, Docker, BuildKit, `mkfs.erofs`,
signing, or production operations. It does not measure a persistent process pool
or its memory, cancellation, crash-recovery, and admission behavior. A result
favoring processes supports a bounded implementation trial; production latency
still needs its own qualification.

Each arm has a configurable total timeout (180 seconds by default). Only the
benchmark's own spawned children may be terminated on failure; successful arms
require clean exits. Temporary worker trees are removed before success, and
fixture blobs are removed after the full comparison passes. A failed comparison
retains its output fixture for diagnosis.

The eight-module smoke passed for both modes with identical candidate source and
filesystem outputs, all children reaped, and Ruff clean. Full-size local and
Linux-host results are separate receipts and should be added only after their
completion gates pass.

## Local completed comparison

[The 2,000-module ABBA receipt](local-thread-process-2000.json) passed exact
source and output comparison across all 16 publications. It used the deployed
candidate module fingerprints `174d8cb8…` and `fd7c7a91…`. Every worker exited and
removed its temporary tree. Local trusted-opacity support was unavailable
(`EPERM`); the other capability probes ran.

| Mean of two trials per mode | Four threads | Four spawned processes |
| --- | ---: | ---: |
| Preparation stage wall time | 10.439 s | 2.128 s |
| Worker startup/import to readiness | 0.162 s | 0.206 s |
| Startup plus preparation | 10.600 s | 2.334 s |
| Preparation CPU | 15.433 CPU-s | 6.945 CPU-s |
| Cleanup and worker reap | 1.750 s | 0.427 s |

Even including fresh process startup, this fixture takes substantially less
time than the already-imported thread arm's preparation alone. The result is
consistent with significant overhead from sharing one Python interpreter across
these filesystem-heavy operations. It does not isolate the GIL from every other
interpreter or filesystem effect, nor measure registry/EROFS publication. A
matched Linux builder run is recorded below; a production offload implementation
still requires separate runtime and representative-load qualification.

## Linux root comparison

[The completed Linux receipt](execution-model-linux.json) used Python 3.14.4,
Linux 7.0.0-30, UID/GID 0, and the same candidate fingerprints as the local run.
The four ABBA trials produced 16 equivalent publication outputs, with no source
mismatches, no capability skips, and every child reaped and temporary tree
removed. User xattrs, real whiteouts, lower-layer whiteouts and trusted opacity
were all exercised.

| Mean of two trials per mode | Four threads | Four spawned processes |
| --- | ---: | ---: |
| Materialization stage wall time | 4.012 s | 0.939 s |
| Squash stage wall time | 3.428 s | 0.829 s |
| Preparation stage wall time | 7.439 s | 1.768 s |
| Worker startup/import to readiness | 0.176 s | 0.233 s |
| Startup plus preparation | 7.616 s | 2.001 s |
| Preparation CPU | 14.632 CPU-s | 5.970 CPU-s |
| Cleanup and worker reap | 1.194 s | 0.397 s |

The conservative comparison is **2.001 seconds for fresh-process startup plus
preparation versus 7.439 seconds of thread preparation**, a 73.1% reduction for
this fixture. Cleanup is shown separately and must be included if it blocks the
production request. This comparison does not isolate the interpreter lock from
other interpreter/filesystem effects, and excludes registry transfer, BuildKit,
EROFS creation and signing.

The retained receipt SHA-256 is
`b67d4b3b3632280379d2c9c7003f0f8abbe22f103e64c7c54f9157c98aa6bf23`.
Its fixture-helper fingerprint matches the archived source:
`e55dbc76764785f75b19d7bcab5c4d35ce21ba7283220c8451e8ac507d190432`.
The benchmark script fingerprint at review was
`2e3210abdc55da47481aaede7c07c392dbf906a5b90fd9c5f542b8787eb259b4`.

The bounded production candidate should start a child only after selective
missing-layer work is identified, with existing build admission limiting the
number of children. Authenticated extraction and consuming private scratch
trees can run in that child; cached-group lookup, locking, final publication and
signing keep their existing correctness contracts. All-cache-hit paths should
avoid child startup. Child deadlines, termination/reaping, failure propagation,
scratch cleanup, numeric timing transport and unchanged fallback behavior need
their own tests. The diagnostic alone does not qualify any of these runtime
properties.
