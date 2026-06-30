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
    "stale_provisioning_capacity_weight": 0.25,
    "scale_down_idle_seconds": 300,
    "builder_scale_down_idle_seconds": 900,
    "default_node_resources": {
      "vcpu": 16,
      "memory_mb": 32768,
      "disk_mb": 204800
    }
  }
}
```

This keeps no idle VM by default, submits at most one new VM per reconciliation
cycle, and uses a coarse sandbox node shape by default: one 16-vCPU VM with
roughly 32 GB RAM as the planning fallback and 200 GB advertised Docker
writable-layer capacity. The short idle grace is only there to avoid immediate
churn after the last sandbox exits; set it to `0` for immediate scale-down.

## Knobs

`warm_resources` keeps a standing resource-shaped buffer ready when
scale-to-zero latency is unacceptable. It reserves CPU, RAM, and disk separately
and should stay at zero unless there is a measured latency SLO that justifies
always-on cost.

Prepared-capacity signals are the burst-oriented alternative. `POST
/v1/capacity/prepare` adds expiring resource demand equal to `count *
per-sandbox resources`; the autoscaler treats it like pending sandbox demand
for scale-up, but no sandbox ids, callers, or nodes are reserved. Use this when
a runner knows a batch is about to start and wants VM scale-up to begin before
the first `POST /v1/sandboxes`. The signal expires by TTL or can be canceled
with `DELETE /v1/capacity/prepare/<id>`, so it does not block scale-to-zero
forever.
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
has been provisioning too long. The same discount applies when
`--oldest-pending-seconds` exceeds the stale threshold, because old unscheduled
demand means the in-flight VMs have not actually relieved the backlog yet. This
lets the autoscaler react to stuck jobs without immediately ignoring every
queued VM.

`scale_down_idle_seconds` prevents the controller from stopping a VM immediately
after its last sandbox exits. The control plane records when a heartbeat first
reports zero active sandboxes and counts the grace from that idle transition,
not from VM boot time. Keep this short when cost matters. It delays scale-down;
it does not require a warm pool.

Builder VMs are scaled separately from sandbox resources. Pending image builds
record count-based demand in the gateway route file. The autoscaler creates up
to `--max-builder-nodes` builder-only VMs when that count is nonzero, and stops
idle builder VMs after `builder_scale_down_idle_seconds` once the pending build
count is zero. Keep this longer than sandbox idle grace because image builds
often arrive in bursts and builder startup is comparatively expensive. Builder
nodes must carry `ucloud-sandboxes/builder=true` and must not carry
`ucloud-sandboxes/node=true`.

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

The controller should record job creation time, start time, first heartbeat
time, and first sandbox placement time. Those measurements should drive later
tuning more than fixed guesses.
