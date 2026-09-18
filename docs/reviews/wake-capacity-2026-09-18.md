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
