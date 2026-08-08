# Scaling policy

UCloud VM startup can be slow or capacity-constrained, so the autoscaler should
not behave like a normal fast container scheduler. The policy should balance
two risks:

- Creating too few VMs and leaving sandbox requests waiting behind a slow VM
  boot.
- Creating too many VMs while UCloud is already queueing jobs.

The current policy is intentionally tunable rather than a single fixed answer.

## Recommended starting point

Use scale-to-zero with a strict in-flight cap until we have real VM startup
data:

```json
{
  "policy": {
    "min_nodes": 0,
    "max_nodes": 10,
    "warm_resources": {
      "vcpu": 0,
      "memory_mb": 0,
      "disk_mb": 0
    },
    "max_create_per_cycle": 1,
    "max_stop_per_cycle": 1,
    "max_provisioning_nodes": 2,
    "provisioning_capacity_weight": 0.75,
    "stale_provisioning_after_seconds": 1800,
    "stale_provisioning_capacity_weight": 0.0,
    "unreachable_stop_after_seconds": 1800,
    "scale_down_idle_seconds": 600,
    "builder_scale_down_idle_seconds": 900,
    "live_pressure_enabled": true,
    "live_pressure_window_seconds": 60,
    "live_pressure_min_samples": 3,
    "live_pressure_fresh_seconds": 30,
    "target_cpu_utilization": 0.70,
    "target_memory_utilization": 0.80,
    "max_memory_psi_full_avg10": 5.0,
    "target_storage_queue_utilization": 0.75,
    "create_pressure_enabled": true,
    "create_pressure_window_seconds": 30,
    "create_pressure_min_samples": 2,
    "create_pressure_fresh_seconds": 15,
    "create_target_concurrency_per_node": 8,
    "create_pressure_max_headroom_nodes": 1,
    "pressure_scale_down_cooldown_seconds": 300,
    "provisioning_latency_lookback_seconds": 604800,
    "provisioning_scale_down_multiplier": 2.0,
    "program_aware_autoscaling_enabled": false,
    "model_wait_capacity_weight": 0.10,
    "model_wait_max_headroom_nodes": 1,
    "default_node_resources": {
      "vcpu": 32,
      "memory_mb": 98304,
      "disk_mb": 1449984
    }
  }
}
```

This keeps no idle VM by default, submits at most one new VM per reconciliation
cycle, and uses the configured homogeneous storage-native node shape for
planning: 32 vCPU, 96 GiB RAM, and 1,449,984 MiB of hard sandbox
capacity on a 2-TB worker after bounded image storage, swap, cache, and
headroom. A heartbeat replaces this estimate with measured capacity as soon as
the node is ready.

## Static safety and live feedback

Requested disk, CPU, and memory are still the hard placement model. Disk is
never overallocated. Live feedback only decides when the pool should retain an
additional ready node around that hard capacity.

With `live_pressure_enabled`, the controller reads indexed recent heartbeat
events from `metrics.sqlite`. Three pressure samples in the default 60-second
window trigger one additional node when no node is already provisioning.
Pressure is any configured CPU, memory, full-memory PSI, or storage-operation
queue target being crossed by a node with active work. Idle host cache or an
idle node's background state cannot trigger it.

`target_cpu_utilization` and `target_memory_utilization` are the clearest
density/latency tradeoff. Lower targets retain more headroom. The PSI and
storage queue limits catch cases that average utilization misses.

Create pressure is an amplifier for resident host pressure, not an independent
reason to buy capacity. Two sampled `gateway_busy` rejections in the default
30-second window prove that all gateway create slots are occupied, but another
VM is requested only when the ordinary sustained CPU, memory, PSI, storage, or
image-materialization queue independently proves that it can help. The default burst
is capped at one node beyond hard resource demand. Operators can raise
`create_pressure_max_headroom_nodes` after observing a workload where measured
node-side queues justify a wider burst. Already-provisioning nodes count toward
the target, preventing repeated scale-up every cycle.

Placement still prefers an existing immutable image copy. At eight concurrent
creates on that node it may spill to another ready node, using registry-layer
cost estimates to decide whether another image transfer is cheaper than the
queue. This prevents a cached image from pinning an entire diverse cold-start
burst to one image-materialization queue.

Node heartbeats expose active sandbox creates plus active, waiting, and maximum
image-materialization operations. Queue pressure is `waiting / concurrency`:
occupied slots are productive capacity and cannot trigger scale-up without a queue.
Gateway saturation can widen a confirmed burst but cannot manufacture pressure
from healthy cold creates.

Pending or active image builds keep one small runnable sandbox shape warm (1
vCPU, 512 MiB memory, and 1 GiB disk). They do not reserve an entire pristine
sandbox node, and a prepared builder without build work does not create any
sandbox-node demand. Exact client capacity preparations and creates continue to
drive exact hard-disk demand.

`pressure_scale_down_cooldown_seconds` supplies hysteresis after the last
pressure sample. Independently, measured submit-to-first-heartbeat p95
multiplied by `provisioning_scale_down_multiplier` becomes a lower bound on the
idle grace. With a 70-second p95 and multiplier `2`, an idle node is retained
for at least 140 seconds even if `scale_down_idle_seconds` is lower.

The policy does not predict the next tool call for each parked sandbox.
Instead, the relay supplies an exact `ready_to_wake` signal and a deliberately
coarse aggregate `model_wait` leading signal. The latter is bounded and
disabled for action by default, as described below.

## Program-aware wake demand

The gateway persists each relay-bound request in one of four generation-fenced
phases: `model_wait`, `ready_to_wake`, `waking`, or `acting`. `ready_to_wake`
is exact hard demand because the response is already committed and the sandbox
must run. `model_wait` is only a weighted, bounded headroom signal for requests
still executing on a model worker.

The scheduler always records its wake plan and metrics. With
`program_aware_autoscaling_enabled=false`, that plan is shadow-only. Enabling
the setting allows its hard and weighted demand to create or retain nodes; it
does not change route ownership, disk accounting, or generation fences.

The deployed systemd service exposes every live-feedback setting through
`/etc/ucloud-sandboxes/autoscaler.env`. The corresponding variables are
`UCLOUD_LIVE_PRESSURE_ENABLED`, `UCLOUD_LIVE_PRESSURE_WINDOW_SECONDS`,
`UCLOUD_LIVE_PRESSURE_MIN_SAMPLES`, `UCLOUD_LIVE_PRESSURE_FRESH_SECONDS`,
`UCLOUD_CREATE_PRESSURE_ENABLED`, `UCLOUD_CREATE_PRESSURE_WINDOW_SECONDS`,
`UCLOUD_CREATE_PRESSURE_MIN_SAMPLES`, `UCLOUD_CREATE_PRESSURE_FRESH_SECONDS`,
`UCLOUD_CREATE_TARGET_CONCURRENCY_PER_NODE`,
`UCLOUD_CREATE_PRESSURE_MAX_HEADROOM_NODES`,
`UCLOUD_TARGET_CPU_UTILIZATION`, `UCLOUD_TARGET_MEMORY_UTILIZATION`,
`UCLOUD_MAX_MEMORY_PSI_FULL_AVG10`,
`UCLOUD_TARGET_STORAGE_QUEUE_UTILIZATION`,
`UCLOUD_PRESSURE_SCALE_DOWN_COOLDOWN_SECONDS`,
`UCLOUD_PROVISIONING_LATENCY_LOOKBACK_SECONDS`, and
`UCLOUD_PROVISIONING_SCALE_DOWN_MULTIPLIER`. Program-aware settings use
`UCLOUD_PROGRAM_AWARE_AUTOSCALING_ENABLED`,
`UCLOUD_MODEL_WAIT_CAPACITY_WEIGHT`, and
`UCLOUD_MODEL_WAIT_MAX_HEADROOM_NODES`. Changes take effect after an
autoscaler service restart. Set the corresponding enabled variable to `false`
for a shadow rollout: metrics and the dashboard continue to update, while that
feedback neither creates nor retains nodes.

## Knobs

`warm_resources` keeps a standing resource-shaped buffer ready when
scale-to-zero latency is unacceptable. It reserves CPU, RAM, and disk separately
and should stay at zero unless there is a measured latency SLO that justifies
always-on cost.

Prepared-capacity signals are the burst-oriented alternative. `POST
/v1/capacity/prepare` records a scale-up reservation equal to `count *
per-sandbox resources`; the autoscaler treats it like pending sandbox demand.
Use this when a runner knows a batch is about to start and wants VM scale-up to
begin before the first `POST /v1/sandboxes`. Each newly reserved sandbox with
the same resource shape and, when specified, image atomically claims one unit
from the prepared count. This keeps the reservation alive through slow VM
boots without double-counting the sandboxes it was created for. Unclaimed
units expire at the TTL or can be canceled with `DELETE
/v1/capacity/prepare/<id>`.
For parkable sandboxes, the prepare request must also set `parkable: true`.
The gateway converts writable `disk_mb` to the full hard hibernation
reservation before recording demand, keeping both autoscaling and the later
exact claim aligned with sandbox admission.
If the prepare payload includes `image`, the gateway also tries to pull that
image onto enough ready sandbox-node capacity for the requested sandbox count.
If no suitable node is ready yet, the gateway records a transient image warmup
work item with the same prepare id and TTL. Capacity is claimed by matching
sandbox reservations; the image warmup is tracked independently so claiming
capacity does not cancel an in-flight pull. It is completed when heartbeating
sandbox nodes with the image can fit the requested `count * resources`, or it
expires with the prepare TTL.

Failed sandbox creates are not durable queue entries. When `POST /v1/sandboxes`
cannot fit on a ready node, the gateway records a short-lived pending scale-up
signal and returns `503`; callers must retry creation. The executing autoscaler
loop consumes these pending signals after a reconciliation cycle, and unconsumed
signals expire after a short TTL from the last failed create attempt. This keeps
abandoned client attempts from holding VMs alive as phantom demand.

If a create was already placed on a node but the gateway has not yet observed
completion, retries with the same sandbox id return a retryable in-progress
response instead of adding another scale-up signal or choosing a different node.

Disk is credited only from the storage-native physical-capacity report in a
fresh complete heartbeat. A node without storage-native authority receives no
sandbox placement.

`max_provisioning_nodes` caps queued or booting provider instances. Keep this
low while the provider reports scarce machines, otherwise the autoscaler can
submit redundant instances that all wait in the same provider queue.

`provisioning_capacity_weight` controls how much queued or booting VM capacity
counts toward pending demand. `1.0` is optimistic. Values around `0.5` to
`0.75` are safer when startup latency is high and variable.

`stale_provisioning_after_seconds` and
`stale_provisioning_capacity_weight` reduce the credited capacity of a VM that
has itself been provisioning too long. Backlog age does not make a newly
submitted VM stale. With the default stale weight of `0.0`, a stale queued,
suspended, or booting VM contributes no projected capacity, but it still counts
against the hard provider and `max_provisioning_nodes` limits until the adapter
reports it final. This prevents duplicate submissions from bypassing the cap
while a billed or provider-visible job still exists.

This weighting applies only to the initial pre-start `SUSPENDED` state. A
post-start suspension is destructive node loss, contributes neither capacity
nor a provider-limit slot to replacement planning, and is terminated directly.
Provider lifecycle evidence and the operation journal keep that classification
latched if inventory later reports the destroyed instance as running again.

`unreachable_stop_after_seconds` is a separate, conservative eviction lease for
a running VM whose heartbeat has disappeared. After the lease expires, the VM
is eligible for provider termination only when it owns no gateway routes and
its last complete heartbeat inventory was empty, or when it never produced a
heartbeat at all. Set it to `0` to disable unreachable-node eviction. Fresh
nodes continue to use the normal drain-token handshake described below.

`scale_down_idle_seconds` prevents the controller from stopping a VM immediately
after its last sandbox exits. The control plane records when a heartbeat first
reports zero active sandboxes and counts the grace from that idle transition,
not from VM boot time. Keep this short when cost matters. It delays scale-down;
it does not require a warm pool.

## Fenced provider operations and drain-before-stop

The supported topology has one control host. Every mutating autoscaler process
therefore contends on one process-lifetime POSIX lock beside
`<state_dir>/autoscaler-state.sqlite`. The kernel releases the lock if the
process exits; there is no renewable wall-clock leader lease or renewal thread.
The SQLite file retains the provider-operation ambiguity journal and durable
drain desired state. `autoscaler-loop` is the only mutating entry point, and
`autoscaler-loop --once` runs one operational cycle. `reconcile` is always
read-only.

No executing autoscaler sends a provider terminate request directly from a
scale-down decision. While holding the local controller lock it first writes a durable
drain intent for the deployment and immutable UCloud job ID. The intent contains
a random incarnation token. Restarts, replacement leaders, and later one-shot
invocations adopt the same intent and token; they do not generate a new token
for each attempt.

Every cycle counterfactually evaluates active drains with their admission
reopened. If current demand and policy still select the same node, the leader
posts `{"token":"...","draining":true}` to the `/v1/drain` endpoint at the node
URL from its heartbeat. If the node's capacity is now needed, it first persists
the intent as `canceling`, then posts the exact token with `"draining":false`.
An ambiguous undrain remains `canceling`, can never authorize a stop, and is
retried after restart or leader handoff; the intent retires only after the node
returns an exact admission-open acknowledgement. A later scale-down allocates a
new token and stop-journal incarnation. Provider stop calls that have already
started are irreversible and are not canceled.

A failed or ambiguous drain request leaves the intent active and does not create
a stop operation. A successful HTTP response is not an acknowledgement that the
VM is safe to stop. The autoscaler waits for a
fresh, gateway-receipt-stamped heartbeat that proves all of the following:

- the node is draining, admission is closed, and the drain token matches;
- the runtime inventory is complete and empty;
- the drain activity epoch equals the current activity epoch;
- active sandbox and build counts are zero; and
- used, sandbox-reserved, and build-reserved resources are all zero.

Only then is a provider stop written to the operation journal, and that journal
is the only autoscaler terminate path. Consequently, two
`autoscaler-loop --once --execute-stops` cycles are normally required: the first
persists and posts the drain, while the later cycle observes the fresh heartbeat
acknowledgement and performs the journaled terminate. Draining or
admission-closed nodes remain in the provider pool count but contribute no ready
or projected free placement capacity. Final UCloud jobs retire their drain
intents.

There is one bounded exception for a node that cannot participate in the drain
protocol at all. After `unreachable_stop_after_seconds`, an owned running VM may
receive a durable unreachable-stop proof when it has no gateway routes and its
last complete heartbeat inventory was empty, or when it never emitted a
heartbeat. This proof permits the same journaled provider termination without a
node acknowledgement. It does not apply to a fresh node, an incomplete last
inventory, or any node with retained route ownership.

The journal moves an operation from `prepared` to `uncertain` before making the
provider call. A crash or timeout leaves that same operation uncertain. A
subsequent exhaustive inventory marks
the operation recovered when every target is explicitly final; otherwise the
idempotent terminate call is retried. Absence from inventory is never treated as
proof that a job is final.

Create operations similarly remain a visibility guard only until one exhaustive
inventory has observed their target job IDs. They are then settled durably, so a
completed job that later ages out of provider history cannot block new capacity
forever. If the same deterministic planning slot is needed again, it receives a
new journal incarnation. Settled and definitely failed audit rows are compacted
to a bounded recent history; a small slot table retains the next incarnation.

Builder VMs are scaled separately from sandbox resources. Pending image builds
record count-based, one-shot scale-up signals in the gateway route file.
Executing autoscaler cycles consume these signals after reacting; image-build
callers retry `POST /v1/images/build` once a builder is ready. `POST
/v1/builders/prepare` records the same kind of one-shot scale-up signal for
known upcoming build bursts; it asks for `count` builder VMs, is consumed after
an executing reconciliation cycle, and can be canceled with
`DELETE /v1/builders/prepare/<id>` before it is consumed. The autoscaler creates
up to `--max-builder-nodes` builder-only VMs for
`max(1 if pending_builds else 0, prepared_builder_count)`, and stops idle
builder VMs after `builder_scale_down_idle_seconds` once pending builds are
zero and prepared builder signals have been consumed. Keep this grace longer
than sandbox idle grace because image builds often arrive in bursts and builder
startup is comparatively expensive. Builder nodes must carry
`ucloud-sandboxes/builder=true` and must not carry
`ucloud-sandboxes/node=true`.

Pending, prepared, and active image-build work also acts as a transient sandbox
warm-capacity signal. During those autoscaler cycles, the sandbox pool adds one
default sandbox node worth of desired resources. This is not stored as prepared
capacity and is not durable demand; it exists only while build activity is
present or while the one-shot build signal is waiting to be consumed. The goal is
to avoid scaling sandbox nodes to zero while a builder is preparing an image that
will likely be launched shortly afterward.

## Direct-runtime placement and wake

The direct runtime has no fixed CPU or memory overcommit multiplier. Each node
advertises physical CPU and RAM, but resident sandbox CPU and memory limits are
not treated as additive lifetime reservations. They are limits, while actual
usage is bursty. Parked and running sandboxes retain their exact hard disk
reservation on their current node. Disk is never overallocated.

This produces two independent capacity questions:

1. Can the sandbox's lifetime disk reservation remain on this node?
2. Can its individual CPU and memory limit fit on one node, and does that node
   currently have enough live headroom to admit more work?

The first answer determines durable placement. The second is re-evaluated on
create and wake using actual CPU, available RAM plus swap, load, and memory PSI.
A parked sandbox wakes on its owning node when its individual shape fits and
live pressure allows admission. Otherwise the gateway may migrate its verified
storage-native snapshot to a fitting node before wake. The node independently
applies the same pressure brake and reserves concurrent creates and wakes
against physical CPU/RAM, so stale heartbeats and simultaneous requests cannot
produce an unbounded activation burst.

Failed create and wake attempts persist individual request shapes as pending
demand. The autoscaler consumes disk additively and bin-packs exact disk shapes.
CPU and memory contribute the largest individual shape and are reusable within
a dynamic-admission node; sustained live pressure adds a headroom node.

Closing admission and selecting placement can race. A node that has durably
closed its admission gate returns the structured, retryable
`node_admission_closed` error before provisioning begins. The gateway treats
that response as definitive rather than ambiguous: it removes the provisional
route, restores the exact request shape as pending demand, and allows the SDK
retry to select another node. Timeouts and generic 5xx responses remain
identity-fenced because the original create may have reached provisioning.

Scale-down requires a proven empty node or a completed storage-native migration:

1. persist a drain incarnation and close node admission;
2. wait for running ownership to be deleted and migrate each parked
   `storage-native-v1` route to a surviving node that advertises both
   `storage-native-v1` and `sandbox-migrate-storage-native-v1`;
3. require a fresh complete empty inventory and zero resource ownership after
   migration;
4. journal and execute the provider stop.

The autoscaler derives the gateway base from `--init-heartbeat-url`, or accepts
`--gateway-control-url` explicitly. It starts at most
`--max-storage-native-migrations-per-cycle` drain migrations. A node is not a
stop candidate when any parked route lacks the canonical storage schema,
manifest digest, or a fitting migration destination.

## Initial operating stance

Until we have measurements, prefer:

- Scale-to-zero by default.
- One VM create per cycle.
- Two provisioning VMs max.
- Prepared-capacity signals for known near-term bursts.
- Standing warm resources only for a measured latency SLO that justifies the cost.
- CPU and memory overcommit factors of `1.0` for direct-runtime nodes, with
  live-pressure dynamic admission instead of fixed limit aggregation.
- No disk overcommit by default.

The controller records VM lifecycle events into the indexed SQLite metrics
database:
submission, observed UCloud state changes, init attempt durations, first
heartbeat, and first sandbox placement. `GET /v1/metrics` exposes these under
`vm_lifecycle`, including `submit_to_running_ms`,
`ucloud_created_to_running_ms`, `running_to_first_init_attempt_ms`, separate
successful package-stage and remote-init durations,
`first_init_attempt_to_first_heartbeat_ms`, and
`first_heartbeat_to_first_sandbox_ms`. The running-to-init interval includes
the controller's polling delay because UCloud does not publish a distinct
SSH-ready timestamp. Those measurements should drive later tuning more than
fixed guesses. The default metrics endpoint is optimized for dashboard polling
with a bounded recent event window and cached registry status; use `?full=true`
when doing offline performance analysis that needs the larger event window and
fresh registry metadata.
