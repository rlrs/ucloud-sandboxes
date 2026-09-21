# Wake capacity and relay latency follow-up

The 21:12–21:22 UTC production workload saturated worker 12396266's configured
128 active ublk-device slots. At 21:16:42 it reported 128 active devices, 16
additional reusable idle devices, about 13% memory use, and disk reservations at
69% of advertised capacity. Trace `2fd8a245c7fb94f908d8b7e20dc49f46` records an
EnsureMounted rejection because device capacity was exhausted. Its relay
`POST /worker/respond` lasted 50.668 seconds, of which only 0.320 seconds was
inside the wake notifier; the rest preceded the wake attempt. This establishes
a separate relay-side delay but does not identify that historical delay's cause
by itself. Both workers and the gateway remained responsive. The workload later
removed all routes, pending creates, and pending relay deliveries.

## Changes

The relay previously shared asyncio's default thread executor between blocking
lifecycle HTTP calls and SQLite journal operations. A queued journal operation
holds the relay's shared state lock, so saturation can delay response commits,
leases, and unrelated rollouts. A dedicated single-worker journal executor
removes that dependency while preserving serialized commits, context propagation,
and cancellation fencing. A regression fills the default executor with a
blocked lifecycle call and verifies a durable response and another rollout can
still complete before the lifecycle call is released.

The gateway already rejected a worker whose reported device count was full.
However, it skipped device reservations for a waking sandbox still represented
as parked in the heartbeat. Local wake requests also reserve zero additional
disk, so reducing advertised disk to zero did not enforce those *unobserved*
reservations. Local wakes now check reported owners plus reserved device slots.
Parked snapshots themselves do not reserve active devices; wakes do until a
heartbeat reports them live. Migration selection includes pending destination
imports in the same accounting, preventing competing migrations from claiming
the last slot.

Explicit relay parks need not publish every checkpoint. When the owner cannot
admit a wake and the checkpoint is not portable, the gateway now requests
asynchronous publication outside its placement locks. The new internal endpoint
checks the exact sandbox generation and requires an already parked lifecycle;
it cannot park running work. At most sixteen background publications are queued
or active per node, retaining the storage service's separate publication limit.
Retries deduplicate an existing publication or consume its cached descriptor.
The gateway validates that descriptor against the complete sandbox identity and
current placement before storing it, so it can migrate on the next request
without waiting for a heartbeat. Transport failures preserve the safe retry
response. Worker device, disk, CPU/RAM and fleet limits remain unchanged.

## Validation scope

Regressions cover executor saturation, wake and migration device reservations,
publication outside the placement lock, immediate descriptor reuse, stale state
fences, generation checks, and bounded publication work. These isolated checks
do not establish 256-way production throughput or eliminate all possible relay
latency. The implementation does not raise the node's configured device limit;
it makes spare capacity on other workers usable for blocked local wakes.

## Production deployment

Server 0.5.36, commit `a61ad702c2dfc910cefbaf946567c52e2e2e88c4`,
was deployed at 2026-09-18 21:43:47 UTC. The gateway, relay, and autoscaler
were restarted while there were no sandbox routes, pending creates, capacity
reservations, or pending relay deliveries. All 90 installed package files
matched the release wheel. Future workers use the new sandbox and builder
bundles. SDK 0.4.20 remains the current client release.

Release artifact SHA-256 values:

- Wheel: `5bdaf0b966df6af753641d6b961bb28d60be9bd81d1b04f50a349fc1214cd7d4`
- Sandbox bundle: `4f8afde15a45ae2ebae96446c4a1b4122688f11d9f9ef90ba303efa61893f5b6`
- Builder bundle: `0b5cd21c37a79acd301755e996028b381ba70074841fae00f9f7944af5ee5675`

The canonical check passed 892 server tests (6 skipped), 99 SDK tests, lint,
shell checks, Go tests, package builds, and installed-wheel checks.
[Server CI](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35397996057)
also passed. Worker 12396276 reported 0.5.36; its actual service environment
loaded the new bundle, and hashes for direct_service.py, node_agent.py,
node_runtime.py, and cli.py matched the committed source. Its startup limit
remained eight.

A bounded production qualification created one managed Python sandbox, started
a persistent counter process, and explicitly parked it without a portable
checkpoint. It then created 128 additional managed sandboxes at concurrency
eight on worker 12396276. Those 128 creations completed in 25.8 seconds after
the initial worker/image startup. The worker's heartbeat confirmed exactly
128 active devices against a maximum of 128.

After worker 12396279 became ready, a normal SDK file read woke the parked
sandbox through publication and cross-worker migration in **7.078 seconds**.
The Python process retained its UUID and counter. There were 153 public gateway
health probes with no failures and a maximum latency of 130 ms throughout the
156-second test, including cleanup. Safe retries included initial node/image
readiness and checkpoint publication. All 129 test sandbox deletions and the
test capacity-reservation deletion succeeded.

The machine-readable summary is
[wake-capacity-2026-09-18-smoke.json](wake-capacity-2026-09-18-smoke.json).
The complete test script and cleanup records remain under
`/work/ucloud-sandboxes/release/0.5.36/` on the gateway. This verifies recovery
from the real device ceiling with one migration, using a small managed Python
image. It does not measure 256 simultaneous cold creates or many simultaneous
checkpoint migrations, and the creation measurement is not a before/after
benchmark.

Final production readback showed no sandbox routes, pending creates, prepared
capacity, or pending relay deliveries. All three workers observed during the
qualification reported 0.5.36 and zero active sandboxes. The destination
worker's running package hashes also matched the committed source.
