# Release 0.5.83: parking churn and gateway poll contention

Runtime commit `997159d`. Deployed normally after the user reported that the
production workload had failed. No live hotpatch was applied. Preflight found
no routed sandboxes, in-flight relay requests, or pending relay deliveries.
The gateway, relay, and autoscaler had remained running with zero restarts;
this evidence does not establish the external runner's terminal failure reason.

## Live findings

Before deployment, a three-minute sample of 308 completed relay requests had
response-ready-to-wake-completed median 5.813 s, p95 126.775 s, maximum 166.730 s.
Another sample recorded 114 program transitions carrying an error across 111
requests: 87 memory-pressure, 26 CPU-pressure/load, and one lifecycle-busy event.
These are internal retry/error observations, not 111 proven terminal client failures.

The gateway health probe took 1.991 s. Its heartbeat handlers repeatedly attempted
to write a response after the sender had timed out. Postgres showed 17 idle
client connections and no active lock waits in the sampled snapshot.

A 40-sample Python stack profile found 4,064 thread samples waiting to record
managed-process responses under `RoutingStore._lock` (about 102 blocked threads
per sample). Heartbeat reconciliation and warmup checks waited behind the same
lock; a create held the placement lock while waiting for routing, also blocking
wake placement. A second profile of the routing-lock owner found it awaiting
writer admission in managed-process updates in 28/40 samples and in image-warmup
reads in 9/40. These diagnostic samples can perturb latency and are not a clean
performance comparison.

Worker `12399418` had about 81 GiB available RAM but two vmstat intervals showed
70–71% I/O wait. Native-backend and gVisor per-process I/O counters dominated;
these account for multiple layers and must not be summed as physical disk
throughput. gVisor held numerous `hibernate-*/pages.img` checkpoint descriptors.
Several sandbox cgroups reported memory PSI despite abundant host headroom.
This fits repeated park/restore work and cgroup reclaim, rather than sustained
exhaustion of host RAM alone. Publication Python work was a smaller contributor
in the sampled interval.

The sampled snapshot-inventory missing-route errors all referred to the known
preserved orphan `515e821fed87424badfa64c47fcee5ab` on `12398541`, not new lost routes.

## Changes

1. `WarmParkPolicy` now divides Linux memory PSI percentages by 100. Previously
   1% PSI multiplied the retention budget by zero and immediately triggered
   parking even with ample headroom. With 80% memory available and a 15-second
   maximum, 1% PSI now retains a 14.85-second budget. Low memory, queued demand,
   large sandbox footprint, and draining still reduce or eliminate retention.
2. Unchanged managed-process status polls use a single read snapshot of the
   current route generation and cached record. Poll-only `updated_at` changes
   no longer cause durable writes. Changed records retain transactional
   generation, identity, sequence, and terminal-state fences, without holding
   the fleet projection lock while waiting for writer admission/commit.
3. Image-warmup listing filters expired rows in a read query. Normal pruning
   still removes expired rows; heartbeat listing no longer takes a write
   transaction or the fleet projection lock.

Admission thresholds were not relaxed in this release. The fixes address the
unnecessary checkpoint activity and serialized routing work that amplify
pressure. Residual pressure-based wake retries remain an issue to measure under
the user's full workload; this release is not proof of the 0.8-second load SLO.

## Validation and deployment

The Linux targeted suites passed 200 tests: 94 routing/overload/managed-process/
wake-owner/startup tests and 106 gateway/node/direct/lifecycle-batch/routing-pool/
stateful/wake-batch tests. New tests exercise progress while another thread
holds the projection lock, read-only unchanged polls, deleted-generation
fencing, terminal-state monotonicity, and PSI units. Ruff and diff checks passed.

On Linux, 512 unchanged managed-process polls at concurrency 32 changed from
512 commits, 4.344 s total, and 534 ms p95 to zero commits, 0.129 s total, and
10.3 ms p95. This isolates the polling path, not end-to-end wake performance.

Both future node bundles passed validation on production kernel 7.0.0-30-generic.
The gateway installed 0.5.83 at 20:08:08 UTC; all 110 installed package files
matched the wheel. Postgres authority was preserved and backed up. The current
workers `12399418`, `12399419`, `12399420`, and `12399423`, plus builder `12399413`,
were drained only after empty inventories/no active work, updated, restarted,
and reopened. All four native storage backend PIDs remained unchanged. Their
v0.2.2 artifact is unchanged; no writable storage migration was required.

Old workers `12397503` and `12398541` remain intact on 0.5.81 with preserved
inventory and are excluded by version-aware placement. Autoscaling resumed
with the new bundle root. The final public workload results and service checks
are recorded below and in the accompanying evidence directory.

Evidence: [`release-0.5.83-2026-09-22`](../benchmarks/release-0.5.83-2026-09-22).

## Production verification after deployment

The public relay workload completed 64 sandboxes × 3 cycles (192 total) with no
workload or cleanup errors. Each sandbox retained 512 MiB, dirtied 128 MiB per
cycle, and used 20-second simulated model waits with 5-second jitter. All 128
measured cycles after warmup observed actual parking, with placements across
all four updated workers.

- Commit-and-wake p95: **0.673 s**, maximum 0.844 s.
- Response-ready-to-usable execution p95: **1.695 s**, maximum 2.009 s.
- Public gateway health p95 during the test: **36.7 ms**, maximum 51.7 ms
  across 51 successful probes.

Memory contents, files, and guest tool execution were verified. The configured
0.8-second end-to-end usable-execution SLO still failed, although the measured
commit-and-wake p95 was below 0.8 seconds. This is a 64-sandbox test, not a
256/512 qualification or a controlled before/after comparison with the user's
failed application workload. No profiler ran during these latency measurements.
