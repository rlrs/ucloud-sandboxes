# Wake-tail attribution and reconstructible mount inputs

Status: both optimizations qualified for rc26 release. Production deployment
and post-release verification are recorded below when complete.

## Attribution

Full sampling matched all 160 wakes in `attributed16`, including the slow tail.
The same existing harness forced 16 concurrent guests through ten park/wake
cycles on worker 12401342: 128 MiB resident heaps, 16 MiB dirty pages per cycle,
2 GiB guest limits, three-second model waits, and the existing uploaded tool
confirmation. One warmup cycle is excluded from latency percentiles.

The worst guest continuation took 6.35 seconds. Of that, 3.32 seconds elapsed
after the simulated model-ready deadline while forced parking was still
finishing. The worker then spent 2.19 seconds restoring, including 1.52 seconds
in workspace preparation and 248 ms retaining checkpoint storage. This was not
a multi-second relay-dispatch delay. Storage admission queues were negligible.

Additional storage events separated the mount operation into journal transition,
lower preparation, pending-state persistence, source-input writing, device
acquisition, and filesystem mount. On the isolated worker, writing a small
source JSON file averaged 154.50 ms and reached 1071.90 ms. Device acquisition
averaged 2.71 ms and the filesystem mount 10.56 ms. File/directory fsync barriers
on the reconstructible input were a concrete avoidable cost.

## Change and recovery boundary

`_write_runtime_source_config` atomically writes complete input for the native
backend without fsyncing that disposable file and its directory. Create, import
and wake use the same helper. The fenced volume journal is already durable
before that input is consumed and contains the authoritative lower descriptors.
The backend reads the source synchronously and materializes its separate runtime
configuration before returning a device. Source-file identity is not used as
proof of a committed volume or a live device owner.

On service restart, reconciliation uses exact device ownership. On the next
mount, input is regenerated from the journal. Missing or partial old source JSON
therefore cannot silently become authoritative. Published local pins remain a
cache of durable published descriptors. Checkpoint data, ownership journals and
the state transitions that grant execution retain their durability barriers.

The native boundary was checked against pinned AgentENV commit
`771ea55ca80abbfacc85e716ec91c40e82b3398b`, specifically
`storage/ublk-daemon/src/server.rs::handle_create_overlaybd_runtime_device` and
`runtime.rs::materialize_runtime_contents`, plus the repository's owner
identity/transition patches. Source input is converted into the separate runtime
image config, not retained as the device's mutable or authoritative metadata.

## Validation and measured limits

All 38 focused local storage tests and 167 Linux storage/lifecycle tests passed.
The added test removes or corrupts source JSON, reopens the storage service,
reconciles the live owner, parks, and confirms the next mount regenerates the
correct lower descriptors from the journal.

The candidate passed 160 real park/wake cycles and fleet-health checks on the
same worker, with no scenario or cleanup failures:

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Source-input write, mean | 154.50 ms | 0.54 ms |
| Source-input write, maximum | 1071.90 ms | 2.28 ms |
| Guest continuation p95 | 1.934 s | 1.732 s |
| Useful execution p95 | 2.255 s | 2.093 s |
| Useful execution median | 1.560 s | 1.382 s |

Stage timings use `metadata16` (48 cycles) and `optimized16` (160 cycles).
End-to-end comparisons use `single16` and `optimized16`, each 160 cycles with
all guests on 12401342 and full tracing enabled. The paired p95 improvement is
7%; sequential trials have temporal variation, so this is not a fleet-wide or
sustained high-density performance guarantee. Subsecond wake remains unmet.

The initial instrumentation restart also stopped its dependent node service.
It was restarted, but the intervening scale-up spread `profiled16` and `phases16`
over three workers. Those runs are explicitly excluded from the comparison.
The two extra idle test workers (12401351 and 12401352) were removed after
verifying zero routes; subsequent measurements used only the original worker.

The candidate's remaining warm tail is now visible: a 1.91-second continuation
included 1.21 seconds inside worker wake (526 ms in checkpoint source retention)
and 598 ms after worker wake until the guest's continuation was observed. Other
slow wakes spend 260–419 ms in native restore. The latter observation interval
includes guest tool work and an HTTP observation tunnel; it is not all relay
scheduling. These are the next targets, rather than the now-submillisecond
source-input write.

Evidence: [reports, attribution and Linux test output](../benchmarks/wake-attribution-2026-09-24/).
Full trace payloads remain under `/work/ucloud-sandboxes/release/0.5.114rc25/` on
the gateway (`attributed16.traces.json`, `metadata16.traces.json`, and
`optimized16.traces.json`).

## Follow-up: retention journal batching (rc26)

Full tracing on worker 12401431 matched all 160 wakes in each comparison. Both
runs included the mount-input improvement; the second additionally batched the
two retention journal transitions. The same 16 guests, ten cycles and workload
parameters were used. Both passed correctness, cleanup and fleet health checks.

| Measurement | Before retention batching | After |
| --- | ---: | ---: |
| Prepare retention journal, mean | 31.88 ms | 15.58 ms |
| Ready retention journal, mean | 24.83 ms | 13.03 ms |
| Inode sync, mean | 149.36 ms | 139.40 ms |
| Guest continuation p95, warm cycles | 2.582 s | 2.287 s |
| Useful execution p95, warm cycles | 2.949 s | 2.686 s |

The allocator now uses the existing `DurableSqliteBatch` for retention writes.
Independent owners share a FULL commit without holding the allocator-wide lock
through disk I/O. Per-owner cross-process mutation leases, unique project IDs,
exact inode/digest checks, and both durable transition barriers remain intact.
The global capacity claim still precedes retention; inode reassignment is still
fsynced; physical retirement still precedes releasing the claim. A persistent
batch connection rejects replaced journals and use after fork.

194 Linux regression tests passed, including concurrent owners, independent-reader
visibility before quota assignment, injected failure of either commit, restart
recovery, and the existing retention/deletion/migration tests. The native canary
adds actual XFS quota, reflink restore and guest tool execution coverage.

This is one matched pair, not a fleet-wide latency guarantee. Required memory
inode sync remains substantial (about 474 ms p95 after batching). The subsecond
objective is still unmet. Detailed reports are in
`docs/benchmarks/retention-batching-2026-09-24/`.
