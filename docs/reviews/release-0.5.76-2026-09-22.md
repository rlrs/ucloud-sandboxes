# Release 0.5.76: restore heartbeats behind a migration fence

Commit `c6c2a1925fd50e0e1c4de147ceef5d166aaca06b` is pushed to
`codex/checkpoint-publication-efficiency`. Wheel SHA-256:
`51b568f9208e02be02808330e7b77d9fa94174efee79edf8748edd57e14b86dd`.
No SDK or Verifiers upgrade is needed.

## Problem and fix

Worker **12397503** served health but disconnected inventory and heartbeat
requests. An expired sandbox retained a migration registration. Inventory's
opportunistic expiry cleanup tried ordinary deletion, which correctly rejected
that unfenced operation. The exception escaped through the heartbeat handler.

`try_delete` now defers when migration ownership requires the fenced deletion
path. It checks durable registration state while holding the sandbox lifecycle
lock. Ordinary owned sandboxes and already-authorized deletion retain their
existing behavior. No migration record or sandbox was discarded to restore health.

A regression test reproduces expiry with a moving-out registration, verifies
inventory remains available and ownership unchanged, then verifies normal deletion
works after abort returns ownership. **68 Linux tests passed** in the provisioner
and node-runtime suites. Ruff and diff checks passed.

## Deployment

Installed gateway/relay at **11:41:54 UTC**, then upgraded all five workers:
12398499, 12398500, 12398539, 12398541, 12397503. All advertised 0.5.76 and healthy
node-agent services. Worker 12397503's heartbeat recovered; all five had fresh
heartbeats afterward. The preserved-orphan worker 12398541 remains draining.
Autoscaling resumed after rollout. PostgreSQL remains authoritative, and the
registry's legacy executable and qualification executables use the same release.

Future worker and builder bundles both passed validation on Linux worker kernel
`7.0.0-30-generic`. Exact installed-wheel verification matched 110 package files.
PostgreSQL backup ran before the brief gateway/relay restart. Serving artifacts
and rollback files are retained in `/work/ucloud-sandboxes/release/0.5.76`.

The final 4-agent / 3-cycle smoke passed all 12 cycles and cleanup: wake p95
0.130 s and response-ready-to-verified-exec p95 0.713 s.

## Load qualification and outstanding issue

The immediately preceding 0.5.75 rollout passed natural **64 × 8** and **256 × 8**
managed-agent cycles through the real public SDK/relay path. The 256 run missed
the 0.8-second target: wake p95 **1.504 s**, verified execution p95 **2.177 s**.
See [full results](release-0.5.75-2026-09-22.md).

The 0.5.76 **256 × 3 wait-for-park test did not pass**. It completed 112 first-round
cycles before a model-response commit received HTTP 409, `request lease has expired`.
There were no post-warmup measured cycles, so no p95 qualification can be claimed.
All test resources were cleaned up; one active-delete 409 retried successfully.

This exposed an interaction between the artificial test and a real scheduling
bottleneck. The harness withholds ready responses until it observes parking, but
its request lease is only 120 seconds (parking observation allows up to 180).
The relay still has a **16-thread/16-slot park dispatcher**. Worker warm retention
can occupy each park HTTP attempt for up to 15 seconds, serializing batches of
park requests. A 256-request burst can therefore exceed the test lease even
without slow disk restore. Natural traffic usually commits sooner and cancels
queued parks, which explains why the natural 256 run can succeed.

The next change should make park notification asynchronous and release dispatch
admission during warm retention, while retaining generation/migration fences,
durable lifecycle claims, cancellation and worker-local I/O admission. Simply
raising the 16-thread constant would hide rather than remove that dependency.
The forced-path harness should renew its worker lease while deliberately waiting
for parking, instead of interpreting expiry of its own held lease as a production
inference failure. Both need regression coverage and another load run.

Raw results and telemetry are in
[`release-0.5.76-2026-09-22/`](../benchmarks/release-0.5.76-2026-09-22/).
The service rollout is complete; **the 256-way latency/forced-parking target is not**.
