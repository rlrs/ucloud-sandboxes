# Release 0.5.77: release park waits and isolate wake dispatch

Commit `4d4af6c15cc64578fa3e84b1cfcec07445ec2f4e` was pushed to
`codex/checkpoint-publication-efficiency`. Wheel SHA-256:
`a5f23de93c5bf71cddbeb7264ab4293cca02e93161b6a10273f16b823f21cd55`.
No SDK or Verifiers upgrade is required.

## Changes

- Replaced the relay's 16-thread/16-slot park HTTP dispatcher with asynchronous
  lifecycle HTTP. Waiting on network I/O no longer reserves an executor thread.
- Worker warm retention returns an internal, explicitly retryable `park_deferred`
  response with its remaining delay. The original start time survives retries,
  pressure is reevaluated on retry, and neither gateway nor worker HTTP execution
  remains held throughout the grace period. Generation and durable wake fences
  still reject a stale park after a response has superseded it.
- Deferred PostgreSQL lifecycle operations release their claims and keep their
  next attempt durable. Wake dispatch has admission independent of park backlog;
  each class retains excess work in the durable queue. Worker-local checkpoint
  and restore admission remain in force.
- The load generator renews inference leases while deliberately waiting for
  parking. Renewal failure aborts before response commit; finishing/cancelling
  the wait cancels the renewal task.
- PostgreSQL latency histograms now use subsecond bucket boundaries. Previously,
  the default first bucket was five seconds, making millisecond p95 estimates
  meaningless. The earlier run's sums/counts remain useful: mean enqueue body
  time was approximately 50 ms, dispatch completion 27 ms, and several mean pool
  waits 21–26 ms. The connection maximum was not increased speculatively.

## Validation and deployment

**1,202 tests ran, 10 skipped, no failures**, on Linux with real PostgreSQL, in 104.799 s.
Regression coverage includes a full park budget not blocking a wake, durable
claim release on deferral, concurrent async park/wake transport, prompt worker
HTTP deferral, retry deadline preservation, wake fencing after restart, and load
harness lease renewal/loss. Ruff and diff checks passed. The exact wheel installed
in a clean Linux venv and passed the installed-package verifier.

The initial test staging omitted a historical qualification JSON fixture; after
copying it, the full suite passed. This was a test-input omission, not a runtime
failure. Existing unclosed-SQLite ResourceWarnings remain in the suite output.

Installed gateway/relay at **12:23:51 UTC**, then all five workers: 12398499,
12398500, 12398539, 12398541, 12397503. All verified 0.5.77. Both future node bundles
passed validation on worker kernel `7.0.0-30-generic`. Exact installed-package
comparison matched 110 files. Autoscaling resumed after worker upgrades.
PostgreSQL authority and the SQLite cutover fence were preserved; a backup ran
before the restart. The earlier user sandbox on worker 12398541 remains preserved.

## Production load results

See the JSON artifacts in
[`release-0.5.77-2026-09-22/`](../benchmarks/release-0.5.77-2026-09-22/).

The natural 256×8 run completed 2,048/2,048 cycles correctly but regressed:
1,792 measured cycles had wake p95 **8.656 s** and response-ready-to-verified-exec
p95 **10.517 s**, against the earlier 1.504 s / 2.177 s baseline. The forced-parking
256×3 test also completed correctly (768/768), with 512 measured wake p95 **4.655 s**.
Its end-to-end metric includes deliberate waiting for parking, so is not directly
comparable with natural response delivery. This release was not accepted as a
performance improvement; investigation and a follow-up fix continued immediately.

A gateway CPU profile found repeated SQLite condition broadcasts among waiting
writers, plus unnecessary owner inventory reads for warm wake shadow planning.
Worker sampling showed little I/O pressure. A separate profiled load run is for
hotspot identification only; its latencies include substantial profiler overhead.
