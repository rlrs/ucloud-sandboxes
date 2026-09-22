# Release 0.5.79: coalesce routing lifecycle bursts

Commit `181ca7245f6af0050af25f850e7e35ef862188b0`; no SDK or Verifiers update is required.

## Change and evidence

Gateway routing now coalesces writes for 5 ms instead of 1 ms. Concurrent lifecycle
transitions join fewer FULL-synchronous commits, with the original per-operation
savepoints, generation checks and durable acknowledgements. This does not change
request admission or worker journal timing. It follows the notification-storm and
warm-wake shadow-planning fixes in 0.5.78 and the async relay dispatch in 0.5.77.

An isolated benchmark ran on the production Linux gateway, using a separate
temporary routing database on the same local filesystem. With 128 concurrent
writers and 512 requests × four lifecycle transitions, the 1 ms setting took
2.508 s wall / 2.853 s CPU and p95 1.641 s per request. The 5 ms setting took
1.220 s wall / 1.425 s CPU and p95 0.473 s. Commit counts including 128 seed
inserts fell from 531 to 223. Production request tests follow below.

## Validation and rollout

Full Linux suite with PostgreSQL: **1,205 tests, 10 skipped, no failures** in
109.958 s. Ruff and diff checks passed. The exact wheel passed installed-package
verification in a clean Linux venv. Both future worker bundles validated on
production worker kernel 7.0.0-30-generic. All accepted responses still wait for
commit; failed operations and failed batches retain rollback coverage.

Gateway/relay installed at **12:58:39 UTC**, with 110 package files matched to
the wheel. Workers 12398499, 12398500, 12398539, 12398541 and 12397503 all verified
0.5.79, then autoscaling resumed. PostgreSQL authority, backups and the SQLite
cutover fence were preserved. The existing unrouted user sandbox on the draining
worker was retained.

## Production measurement

The clean natural 256×8 run completed **2,048/2,048** cycles with no workload
errors. Across 1,792 measured cycles, wake p50 was 0.973 s, p95 **2.113 s**,
p99 2.690 s; response-ready-to-verified-exec p95 was **3.157 s**.
This improves the 0.5.77 result (8.656 s / 10.517 s) and 0.5.78 result
(3.773 s / 4.694 s), but is still above the earlier 0.5.75 baseline
(1.504 s / 2.177 s). The **0.8 s target is not achieved**. The additional
async park activity in this release has not yet been made as cheap as the
previous transport, which admitted only 16 park HTTP operations at a time.
Do not describe the rollout as an overall latency win over that earlier baseline.

Gateway and relay CPU remained substantial on the two-vCPU host. Connection
churn is another measured hotspot, but the gateway intentionally disables
keep-alive because idle connections occupy request threads. Enabling it safely
requires idle-connection multiplexing or a different HTTP serving architecture;
that change is not included here.

The final clean forced-parking 256×3 run completed **768/768** cycles with no
workload or cleanup errors. Across 512 measured restores, wake p50 was 1.487 s,
p95 **4.225 s**, p99 5.343 s; usable-exec p95 was 5.171 s. Its full end-to-end
metric includes deliberately withholding model responses until parking and
must not be compared with the natural-run SLO. No profiler was attached.

Final checks found zero gateway sandbox routes and reservations, zero relay
inflight requests and pending deliveries, fresh 0.5.79 worker heartbeats, and
healthy public gateway/relay endpoints. The preserved unrouted old user sandbox
remains on its draining worker. Autoscaling is active.

Wheel SHA-256: `383669a545d177c24aaa050f9b77336cb00b4045024f4e861622c50c2b3c82e3`.

The implementation is deployed and correct on these workloads. Further gateway
coordination optimization is still required; sub-0.8-second performance and an
overall latency improvement over 0.5.75 have not been demonstrated.
