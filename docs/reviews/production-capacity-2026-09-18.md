# Production capacity investigation — September 18, 2026

Scope: DFM Pretraining, deployment `live-ucloud-20260824a`. This follows the
[production health and provisioning fixes](production-health-2026-09-18.md).

## Capacity use

Retained worker heartbeats for **06:50–08:05 UTC**, before the qualification
sandboxes, show:

| Measurement | Observed value |
| --- | ---: |
| Worker heartbeats / distinct worker jobs | 486 / 8 |
| Time-weighted CPU utilization | 25.94% |
| Time-weighted memory utilization | 6.37% |
| Observed available / consumed CPU hours | 102.09 / 26.48 |
| Mean CPU in samples with active sandboxes | 30.26% |
| 95th percentile CPU in samples with active sandboxes | about 75% |
| Maximum simultaneous sandboxes observed on one 32-vCPU worker | 68 |

These are sampled utilization estimates, not billing totals. Each heartbeat is
weighted until the next heartbeat, capped at 40 seconds; a worker's final sample
gets 20 seconds. Missing intervals and bootstrap time are not extrapolated.
The fleet has substantial average spare capacity, especially memory, but CPU
peaks mean that average utilization alone cannot justify a fourfold reduction
in workers. The scheduler is already multiplexing sandboxes beyond nominal
CPU allocations. Worker loss and request latency also need to constrain tuning.

No retained image-materialization or storage queue pressure explains the low
average utilization. There were no observed parking/restoring operations or
snapshot publications/compactions in the workload window. There were no
storage-detach attempts in its retained autoscaler cycles. Retained completed
migrations were older, from September 2–3. These observations do not prove that
all historical workload runs behaved the same way.

## The supplied Verifiers integration does not request parking

Reviewed [`rlrs/verifiers-ucloud` at `fed54c688896057ddde95bf864babd614150bdb8`](https://github.com/rlrs/verifiers-ucloud/tree/fed54c688896057ddde95bf864babd614150bdb8),
dated August 29. The user cautioned that it may differ from the deployed runner.

- `src/verifiers_ucloud/runtime.py:114` uses `SandboxSpec.benchmark(...)`.
  The pinned SDK is **0.4.15**; that factory uses the `linux_host` profile and
  leaves `parkable=False` and `managed_process=False`. The server does not
  automatically opt these sandboxes into parking.
- `src/verifiers_ucloud/interception.py:72` opens a generic relay rollout
  session with rollout and worker IDs, without binding it to a sandbox.
  It does not use the managed agent rollout registration path.
- Consequently, this version supplies neither parkable sandboxes nor the
  sandbox-bound model-wait lifecycle needed for coordinated parking. This is
  consistent with production's absence of parking and program signals; the
  exact deployed integration revision remains unverified.

Production has `program_aware_autoscaling_enabled=false`, and all inspected
program signals were zero. Enabling that flag alone would not improve this
runner. A safe integration change must define park points around model waits,
bind each rollout to its sandbox, and preserve background-process behavior.
Simply setting `parkable=True` risks freezing background work: production's
ordinary inactivity timer is only one second. SDK 0.4.15's managed-process mode
also requires the `container` profile, whereas this integration uses
`linux_host`; this is not a one-flag migration.

## Packing and consolidation at the start of investigation

New sandbox placement considers cached or in-flight images, create headroom,
layer transfer cost, active creates, resource slack, and disk. This encourages
reuse, but it was not a fleet consolidation controller. At that time, normal wakes stayed on
the existing worker; relocation occurs for detached or pressured placements.
The autoscaler can detach eligible published parked sandboxes and stop idle
workers. It does not proactively move running sandboxes to empty a lightly
used node. Migration destination ordering also favors cache/reservations and
free disk rather than explicitly minimizing the number of occupied workers.

Current worker policy is 0–6 nodes, 300-second idle scale-down, live CPU target
70%, memory target 80%, and create concurrency target eight per worker with
one node of create headroom. CPU-pressure and create-pressure flags appeared
in 56 and 48 retained cycles respectively; these are repeated cycle decisions,
not counts of nodes created. No policy threshold was changed from these
aggregate measurements alone.

## Parking race repaired

An isolated parkable sandbox reproduced a concrete failure: after the first
idle park, its next exec returned HTTP 400, `sandbox lifecycle transition is in
progress`. Wake did not refresh the inactivity timestamp, so the one-second
idle timer could immediately park the restored sandbox before exec acquired
its activity lease. A timer observation taken before wake could also remain
stale while waiting for the lifecycle lock.

`DirectSandboxService.wake()` now marks activity both after restore and when
already running. Background `park()` rechecks inactivity under the sandbox
lock. Explicit parking remains available immediately after activity.

A regression test covers successful and idempotent wake, the stale timer, and
explicit park. **60 relevant tests passed**, plus Ruff and `git diff --check`.

Deployment retained the original source and configuration. The empty
qualification worker `12395565` was fenced against admission, verified empty,
patched, restarted, and reopened. The patched source SHA-256 is
`b51aca36060a5c30f3b168801b726b63b39c4617667bcc3e5cb369ba28703d92`.
Future workers use the immutable bundle directory
`/work/ucloud-sandboxes/release/0.5.33-parking-20260918`, whose bundle SHA-256 is
`de0284178ad551f68adf6e7efcdf171ccbd30d16374b7abfafb0dd2126fe00ef`.
Its nested agent archive and manifest were updated and validated; storage and
sandbox runtime binaries were retained. Autoscaler configuration now points
to that directory. Builder bundles are unchanged. This remains a targeted
0.5.33 hotfix and should be incorporated into the next versioned release.

A live test on that worker passed three automatic idle park/restore cycles,
with wake-and-exec latencies of **1.021, 0.598, and 0.562 seconds**. It then
published and detached the parked sandbox, leaving a durable manifest and
`worker_state=detached`. The next exec restored and reattached it in **0.836
seconds**. A UUID created only in the primary process's memory survived every
restore, alongside its advancing counter. Final deletion succeeded.

The detach/restore qualification reused the same worker; it does not validate
cross-worker migration or sustained throughput. The future bundle was checked
structurally and cryptographically, but this test ran on the patched existing
worker rather than a fresh boot from that bundle.

Two test-protocol details matter: a gateway route can briefly lag a node's
automatic park until the next heartbeat, so immediate detach first used an
explicit idempotent park to synchronize the route. Also, waking while snapshot
publication is pending can return the explicit retryable
`snapshot_publication_pending` 503. The test retries only that pre-exec
condition, rather than repeating arbitrary failed commands. The original
wake-race HTTP 400 did not recur in the patched worker's completed cycles.

Successful steps are retained on the gateway in
`/work/ucloud-sandboxes/release/health-20260918/parking-smoke.json`.
The isolated sandbox was deleted, and the temporary capacity reservation was
removed after qualification.

Final verification at **09:08:48 UTC (11:08 CEST)** found all four core
services active, all four telemetry backends healthy, no sandbox routes, no
pending sandbox/build demand, and no preparation reservations. The report
still recorded one background snapshot-publication error alongside four
successes during the qualification window. No corresponding sampled error
trace or journal exception was available, so its cause is not established.
The completed end-to-end test is evidence of successful restore, not a claim
that all publication errors have been eliminated.

## Follow-up priorities

1. Update and qualify the actual Verifiers integration for sandbox-bound
   model-wait handling, with explicit tests for background jobs and the chosen
   runtime profile. Confirm the deployed revision before rollout.
2. Measure worker count, CPU, model-wait fraction, and provisioning/exec tail
   latency during a representative run with that integration.
3. Evaluate the wake-time consolidation improvement below under real load;
   extend it if workers remain sparsely occupied. Include migration cost, cache locality,
   destination headroom, and anti-churn thresholds.
4. Tune warm/create headroom and idle grace only against those latency and
   utilization measurements. Memory is abundant; CPU peaks remain meaningful.

Investigation evidence was captured in the local `/private/tmp/prod-capacity-*`
logs and gateway `health-20260918` directory. No representative production
workload was running during the controlled parking qualification.

## Consolidation improvement deployed

After the initial investigation, the user requested an improvement. Production
now enables `policy.parked_wake_consolidation_enabled`. A safely published
park on a lightly used worker can wake on an already occupied worker, even
when the original worker could run it. The source can then empty through
natural park/wake cycles and become eligible for the existing fenced idle
scale-down. This addresses repeated local wakes keeping sparse nodes alive.

Optional migration is conservative: the destination must have the exact image
cached, fresh complete telemetry/inventory, at least as many active sandboxes,
and headroom for the full waking shape under the CPU/memory targets. Storage
errors, queues, in-flight creates/wakes, missing observations, or another
active migration prevent consolidation. Moves follow a strictly decreasing
immutable job/node order, with a 60-second gateway cooldown. Normal local wake
is the fallback. Existing migration journaling and placement locks preserve
identity on retries. The policy does not force running work to park, and it
will not improve a workload whose sandboxes never park.

The knob defaults to false for new and legacy configurations; production was
explicitly enabled. See [scaling policy](../scaling-policy.md#consolidating-on-wake)
for the exact admission rules and cooldown/restart behavior.

Validation: **224 tests passed** across consolidation, control plane,
configuration, CLI, routing, correctness regressions, policy, reconciliation,
and program scheduling. Additional destination-in-flight and migration-retry
checks passed, as did Ruff and `git diff --check`.

The live two-worker check first placed an isolated park on worker `12395623`,
with a separate active sandbox on worker `12395622`. Its next exec caused an
automatic consolidation to `12395622`. The primary process retained UUID
`e578eba69b5c452eb9f5e8b67944fe2c` and its counter advanced from 3 to 4, proving
process restoration rather than recreation. Migration, restore, and exec
startup took **0.943 seconds**. Both test sandboxes and their preparation
reservation were deleted. This demonstrates the actual cross-worker path;
it does not establish cost savings or tail latency under representative load.

Fresh boot testing also found that the previous parking bundle's directory
was mode 0700 because the administrative shell's umask restricted its creation.
The autoscaler service user could not resolve the package file, causing init
failures. The directory is now explicitly 0755, and package path/hash access
was verified as `ucloud`. Both qualification workers subsequently booted from
that bundle. No runtime archive or snapshot format change was required.

Gateway source/config backups, source hashes, and successful smoke steps are
retained under `/work/ucloud-sandboxes/release/health-20260918/consolidation/`.
Gateway and autoscaler were restarted; health-failure rollback was prepared
and not needed. These hotfixes are included in the versioned 0.5.34 release.

Final check at **09:28 UTC (11:28 CEST)**: gateway health returned HTTP 200;
all four core services and telemetry backends were healthy; sandbox routes,
active migrations, preparation reservations, and pending sandbox/build demand
were empty. The consolidation event recorded the expected source/destination.
The report still includes operation errors from the test window, which
included cold-image warmup and deliberately terminated keeper execs; it is not
a zero-error production-load verification. The two now-empty workers remain
subject to normal idle scale-down. The booted worker's installed parking-fix
source hash matched the validated bundle.
