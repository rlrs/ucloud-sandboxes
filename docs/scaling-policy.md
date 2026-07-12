# Scaling policy

Date: 2026-06-28

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
    "scale_down_idle_seconds": 600,
    "builder_scale_down_idle_seconds": 900,
    "default_node_resources": {
      "vcpu": 32,
      "memory_mb": 65536,
      "disk_mb": 204800
    }
  }
}
```

This keeps no idle VM by default, submits at most one new VM per reconciliation
cycle, and uses a coarse sandbox node shape by default: one 32-vCPU VM with
roughly 64 GiB RAM as the planning fallback and 200 GB advertised Docker
writable-layer capacity. The ten-minute idle grace avoids churn after the last
sandbox exits; set it to `0` for immediate scale-down.

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

Disk is only credited from node heartbeats that advertise `disk-quota`, which is
derived from a passing runtime conformance probe. Nodes without that capability
can still contribute CPU and memory, but their free disk is treated as zero for
hard disk demand.

`max_provisioning_nodes` caps queued or booting VM jobs. Keep this low while
UCloud reports scarce machines, otherwise the autoscaler can submit redundant
jobs that all wait in the same provider queue.

`provisioning_capacity_weight` controls how much queued or booting VM capacity
counts toward pending demand. `1.0` is optimistic. Values around `0.5` to
`0.75` are safer when startup latency is high and variable.

`stale_provisioning_after_seconds` and
`stale_provisioning_capacity_weight` reduce the credited capacity of a VM that
has itself been provisioning too long. Backlog age does not make a newly
submitted VM stale. With the default stale weight of `0.0`, a stale queued,
suspended, or booting VM contributes no projected capacity, but it still counts
against the hard provider and `max_provisioning_nodes` limits until UCloud
reports it final. This prevents duplicate submissions from bypassing the cap
while a billed or provider-visible job still exists.

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
read-only and rejects its legacy mutation flags.

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

## Initial operating stance

Until we have measurements, prefer:

- Scale-to-zero by default.
- One VM create per cycle.
- Two provisioning VMs max.
- Prepared-capacity signals for known near-term bursts.
- Standing warm resources only for a measured latency SLO that justifies the cost.
- CPU overcommit of `2.0` for sandbox nodes.
- Conservative memory overcommit of `1.2` for sandbox nodes.
- No disk overcommit by default.

The controller records VM lifecycle events into the metrics JSONL stream:
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
