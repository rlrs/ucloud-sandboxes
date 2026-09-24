# Latest production run: recovery and renewed parking bursts

Read-only investigation on 2026-09-24, production rc22. The substantial agent run
was September 23 23:00–23:47 UTC (September 24 01:00–01:47 Copenhagen).
The fleet is idle now; a later single-worker qualification session is separate.
No production configuration, code, or services changed during this investigation.

## Finding

Performance recovered between bursts. The evidence does not support a fleet that
remained permanently overloaded after scale-up. Instead, synchronized response
bursts generated conservative memory-growth reservations, pressure-driven parking,
and slow lifecycle completion. Some requests then waited tens of seconds even
while their own worker had abundant available RAM. This is a real policy and
recovery problem, not simply insufficient worker count.

The previous worker `sandbox.wake` histogram measures actual restores, excluding
resident no-op continuations and time before the restore span. It is not a
representative latency distribution for all relay wakes. Retained relay state
transitions provide a better end-to-end coordination measurement:

| Wake start UTC | Completed requests | p95 seconds | >10 seconds |
|---|---:|---:|---:|
| 23:20–23:25 | 2,895 | 0.517 | 0 |
| 23:25–23:30 | 3,398 | 0.491 | 0 |
| 23:30–23:35 | 2,833 | 3.983 | 65 |
| 23:35–23:40 | 3,348 | 0.444 | 17 |
| 23:40–23:45 | 2,445 | 0.479 | 0 |
| 23:45–23:50 | 389 | 16.264 | 24 |

These use unique request IDs and wake_started_at → wake_completed_at. They exclude
failed/incomplete requests and do not measure first useful guest execution.
Retention starts at 23:19:51, so the first partial window is omitted here. Tail
failures remain important even when p95 is low. The final window contains the end
of the run and is not a sustained-load recovery qualification.

## Actual placement and resource evidence

The eighth worker became ready at 23:33:46. Its assigned sandbox count rose from
6 at 23:34:55 to 33 at 23:44:05; the other workers held 37–46 at that latter point.
This establishes that additional capacity was used, not that existing workloads
were migrated or that every worker had equal demand.

Near 23:44, retained per-worker heartbeats show approximately 65–73 GiB
MemAvailable, not just scheduler bookkeeping. Around the late burst, worker
12400857 had 65.4 GiB available at 23:45:16 and 65.0 GiB at 23:45:38. Its available
RAM backing was about 73.5 GiB. The latter sample shows one checkpoint in flight,
zero pending demand, only 10.5 GiB admitted growth exposure, and I/O PSI 23.73%.
The reclaim target had already returned to zero, but capture was still running.

At 23:45:19, the newest worker 12400860 had ten checkpoints in flight despite
70.8 GiB MemAvailable. At 23:45:41 it still had four in flight, 73.0 GiB available,
and I/O PSI 37.5%. Several other workers similarly developed I/O pressure while
CPU usage dropped. Available memory is not proof that *unreserved* growth
headroom existed at the instant each checkpoint was authorized.

## Trigger and why the lag persists

The retained completed-request records contain 168 response-ready transitions
in 23:34:47–48 and 201 in 23:45:14–16. These are synchronized response bursts,
not a steady workload. Configured guest limits include 2, 4, and 8 GiB.

`DirectSandboxService._growth_remaining` reserves the configured memory bound
minus observed residency for an active managed primary. Safe waits release that
growth exposure; admitting a response reactivates it. `warm_park_demand` feeds
admitted exposure plus the next eligible queued owner into parking decisions.
Thus many small resident heaps can temporarily consume nearly all *reserved*
headroom without consuming physical RAM. This protects against real growth and
must not simply be removed.

A captured example at 23:34:51 on 12400857 shows 65.0 GiB available, 57.7 GiB
admitted exposure and 7.7 GiB next demand; the parking reason is `queued_demand`.
At 23:34:59, worker 12400848 shows 16 checkpoints in flight and about 13.9 GiB
projected reclaim, but the current reclaim target is only 0.88 GiB. Its reason is
`memory_reclaim`: memory PSI is 12.52%, I/O PSI 7.68%, with 66.7 GiB available.
The target can shrink after captures start; captures are not canceled afterward.

The current policy also allows memory PSI alone to trigger full checkpointing
when I/O PSI is below 20%. This does not require a physical/backing byte deficit.
By the time I/O PSI crosses 20%, many captures can already be running. The new
(un-deployed) I/O backpressure patch prevents additional captures at high I/O
pressure, but cannot prevent every initial burst or undo admitted captures.

Once capture starts, wake must respect lifecycle exclusivity. Spare RAM later
does not bypass a capture already holding the sandbox's lifecycle authority.
Two sampled 60-second HTTP failures explicitly show `sandbox lifecycle is busy`
at the direct-service lock, followed by BrokenPipe after the gateway times out.

## Late slow wake: directly measured

Trace `ba31e40e6e688fae01737d1683f9dfa6`, sandbox
`1dcaec2de70241a2a64820c9a9c232f6`, worker 12400857:

- Gateway request: 23:45:15.415–23:46:00.471, 45.056 seconds.
- Worker handler: 23:45:16.572–23:46:00.461, 43.890 seconds.
- Actual restore span starts at 23:45:56.371: 39.800 seconds after handler entry.
- Restore: 4.087 seconds, including runsc 3.656 seconds, cleanup 0.169 seconds.
- Restore admission timer: only 5.8 milliseconds.
- Relay durable state independently records a 44.269-second wake.

The missing 39.8 seconds is before restore admission/execution, on the worker,
not a 40-second gateway or Postgres queue. The code has managed-growth admission
and lifecycle coordination before this span. The concurrent checkpoint and
separate lifecycle-lock failures strongly support checkpoint/coordination delay,
but there is no span splitting growth admission from lifecycle waiting in this
specific successful request. Do not claim its exact 39.8-second attribution is
proven.

The latest park samples also show very different capture times: approximately
1.6–2.0 seconds on 12400843/51, 23.9 seconds on 12400844, and 40.1 seconds on
12400860. Sampled earlier failures hit the 60-second runsc checkpoint timeout.

## Smallest useful follow-up

1. Avoid full hibernation on PSI alone when physical/backing headroom covers
   admitted and next demand; prefer the existing cheaper reclamation path.
2. For forecast-only, transient deficits, give active continuations a bounded
   chance to return to safe waits before launching expensive captures. Keep
   memory admission guarantees and continue immediately on genuine byte pressure.
3. Recheck need after waiting for lifecycle authority, before beginning capture;
   never cancel a partially committed checkpoint without a proper protocol.
4. Instrument managed-growth admission and lifecycle waiting separately. Test a
   synchronized response burst with small heaps and 4/8 GiB bounds, then hold the
   workload active to verify recovery. Existing steady 2 GiB synthetic shapes
   miss this important reservation-to-residency mismatch.

The earlier 76 Postgres pool timeouts are a separate control-path problem. They
occurred around 23:34 and do not explain the traced late worker-side delay.
Adding more nodes could reduce per-node exposure, but is not a complete fix for
this burst-triggered capture behavior.

Evidence: `docs/benchmarks/overload-recovery-2026-09-24/` contains compact retained
heartbeats, sampled traces, and computed relay wake windows. Heartbeats are
sampled (often 20–25 seconds apart); they do not capture every decision instant.

## Implemented correction and qualification

The parking policy now gives forecast-only deficits a one-second settling window.
If such a deficit persists, it admits one reclaim candidate at a time, observes
the result, and reassesses. This applies only while physical RAM/backing headroom
is healthy. Actual low or unknown memory/backing evidence bypasses the grace;
measured-byte parallel reclaim remains available when physical pressure requires
it (subject to existing I/O backpressure). Admission accounting and durable
growth reservations are unchanged.

Memory PSI can trigger checkpointing only near the existing memory reserve,
including admitted/next demand, rather than with abundant unreserved headroom.
The runtime rechecks pressure after acquiring lifecycle authority before starting
capture. No in-progress checkpoint is canceled. Wake traces now expose separate
`sandbox.wake.growth_admission` and `sandbox.wake.lifecycle_wait` spans.

The one-second window is a bounded anti-burst policy, not an admission timeout or
capacity cap; its production tradeoff remains to be measured. Persistent
forecast-only pressure may reclaim more slowly, deliberately avoiding speculative
parallel disk work while RAM remains available.

Linux qualification passed 210 tests in 10.875 seconds, with source hashes checked
against this working tree. Coverage includes transient/persistent bursts, a
32-agent per-worker response wave, immediate RAM/backing-shortage reclamation,
recovered pressure while acquiring lifecycle authority, memory-growth admission,
and stale wake/park fences. An assembled-service regression verifies that a queued
continuation cannot overspend growth headroom and resumes without checkpointing
when a peer reaches a safe wait. Runtime/pressure fixtures are simulated; this is
not a native gVisor load run or proof of production p95. The change was subsequently deployed as rc23; see the deployment receipt below.


## Deployment

Deployed `0.5.114rc23` from `46df583` to the idle production gateway at
2026-09-24 05:25 UTC. Both sandbox and builder bundles were rebuilt from rc22,
with all native/OS/kernel/storage members verified byte-identical. All 142 package
files matched the committed-source wheel after installation. PostgreSQL remains
the relay authority; policy and sandbox configuration are unchanged. Gateway,
relay, registry, and autoscaler are active. See the
[receipt](../benchmarks/overload-recovery-2026-09-24/deployment-result.json).

The post-deployment SDK 0.4.26 smoke ran four sandboxes through three forced
park/resume cycles each on freshly provisioned worker 12400891. All 12 cycles and
primary exits passed, with no operation, cleanup, or fleet-health failures.
Excluding the first cycle, continuation p95 was 0.634 seconds and useful tool
completion p95 was 0.883 seconds (eight measurements). The harness correctly
reported `slo_passed=false`: its fleet-health gate lacked observation beyond the
30-second placement grace. This short run is correctness evidence, not sustained
load qualification. After cleanup, routes and relay inflight/delivery-pending
counts were zero; all four services were active and the worker reported rc23,
open admission, and zero sandboxes. See the [smoke result](../benchmarks/overload-recovery-2026-09-24/deployment-smoke.json).
