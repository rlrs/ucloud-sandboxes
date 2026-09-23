# Production source and qualification status — 23 September 2026

This page identifies the current baseline. Earlier candidate reviews and logs
are retained as historical evidence, including failures; their release-status
statements must be read in the context of their candidate version.

## Deployed baseline

Production runs **0.5.114rc22**, with server Python recorded in commit
`62ad20b`. Every staged server Python file was compared byte-for-byte with the
frozen rc22 package before that commit. The qualified source manifest was also
verified against the retained test snapshot with no discrepancies. Package and
lock metadata now consistently identify rc22; no final 0.5.114 release is claimed.

The assembled changes include PostgreSQL relay authority, asynchronous gateway
response handling, memory/workspace separation, physical and backing-capacity
growth admission, and memory-aware autoscaling. Immutable environments remain
opt-in and disabled in production. Production allows zero to eight workers.

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
