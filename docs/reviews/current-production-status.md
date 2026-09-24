# Production source and qualification status — 24 September 2026

This page identifies the current baseline. Earlier candidate reviews and logs
are retained as historical evidence, including failures; their release-status
statements must be read in the context of their candidate version.

## Deployed baseline

Production runs **0.5.114rc24**, deployed from `5bc348f` on September 24.
It removes restore-slot contention from resident growth admission, separates
cached memory-placement reads from allocator I/O, and replaces unread exec-event
eviction with output backpressure. SDK **0.4.27** is published from `dc62af7`;
Verifiers main `9226b1f` pins it. Upgrade client environments before large-stdin
exec workloads. Production now allows **zero to ten workers**, with the existing
80% memory-utilization target. Native runtime, OS and storage closure are unchanged.

All 123 targeted Linux server tests and 58 SDK client/duplex tests passed. SDK CI
passed the complete suite on Python 3.10 and 3.13. The live managed-profile test
passed 16 concurrent creates and 48 forced park/wake cycles, plus separate large
stdin/stdout/stderr and delayed-reader checks. This is a correctness result:
useful-execution p95 was 2.52s, so the subsecond SLO remains unmet. See the
[release receipt and remaining bottleneck](wake-exec-release-2026-09-24.md).

The preceding **0.5.114rc23** (`46df583`) added narrowly retryable relay/heartbeat
backpressure and parking-burst recovery corrections. Its historical evidence is
in [overload recovery](overload-recovery-2026-09-24.md).

The preceding **0.5.114rc22** baseline has server Python recorded in commit
`62ad20b`. Every staged server Python file was compared byte-for-byte with the
frozen rc22 package before that commit. The qualified source manifest was also
verified against the retained test snapshot with no discrepancies. Package and
lock metadata now consistently identify rc22; no final 0.5.114 release is claimed.

The assembled changes include PostgreSQL relay authority, asynchronous gateway
response handling, memory/workspace separation, physical and backing-capacity
growth admission, and memory-aware autoscaling. Immutable environments remain
opt-in and disabled in production. Production allows zero to ten workers.

The frozen baseline passed **1,748 Linux tests against real PostgreSQL**, with
12 environment skips. See the [qualification record](../benchmarks/autoscaler-memory-2026-09-23/qualification.json)
and [deployment record](../benchmarks/autoscaler-memory-2026-09-23/deployment.json).
These existing full-suite results were not rerun merely to commit identical code.

The latest [production load tests](../benchmarks/autoscale-load-2026-09-23/README.md)
show a warm 256-guest pass, a cold 256-guest startup timeout before observed
rebalancing, and a correct 512-guest run with substantial transient parking/I/O
latency. They do not establish sustained subsecond wake under memory pressure.

## Deferred work

Changes beyond rc22 to background retirement of restored artifacts, resident
reclaim action ranking, and reclaim telemetry are recorded separately. They are
not deployed or qualified by rc22's full-suite or production results. Their
focused Linux regression results are supporting evidence only; loaded pressure
qualification remains necessary before deployment. Committing this work does not
change production or imply that the performance objective is complete.

## Commit organization

The coupled implementation, native runtime patches, test contracts and load
harness form one qualified baseline commit. Documentation and successful/failed
qualification evidence form a separate commit. Subsequent reclaim experiments
and their tests form another commit so that they can be reviewed or reverted
without changing the production baseline.
