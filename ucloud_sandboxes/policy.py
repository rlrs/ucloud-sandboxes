from __future__ import annotations

from datetime import datetime
import math

from .capabilities import DISK_QUOTA_CAPABILITY, has_capability
from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    ScaleAction,
    ScaleDecision,
    ScalePolicy,
    utc_now,
)


def evaluate_scale(
    nodes: list[SandboxNode],
    demand: SandboxDemand,
    policy: ScalePolicy,
    *,
    now: datetime | None = None,
) -> ScaleDecision:
    if now is None:
        now = utc_now()
    incompatible_stop_candidates = _incompatible_stop_candidates(
        nodes,
        now=now,
    )[: max(0, policy.max_stop_per_cycle)]
    pool_nodes = [node for node in nodes if _counts_as_pool_node(node, policy, now, 0)]
    # Incompatible nodes consume real provider slots but contribute no usable
    # capacity. Keeping those sets separate prevents version drift from opening
    # the hard limits and causing a replacement stampede.
    capacity_nodes = [node for node in pool_nodes if node.agent_version_compatible]
    ready_nodes = [node for node in capacity_nodes if node.is_schedulable]

    oldest_pending_seconds = max(0, demand.oldest_pending_seconds)
    provisioning_nodes = [
        node
        for node in pool_nodes
        if _counts_as_active_provisioning(
            node,
            policy,
            now,
            oldest_pending_seconds,
        )
    ]
    total_nodes = len(pool_nodes)

    demand_resources = demand.desired_resources
    desired_resources = _add_resources(demand_resources, policy.warm_resources)
    projected_free_resources = _projected_free_resources(
        capacity_nodes,
        policy,
        now,
        oldest_pending_seconds,
    )
    resource_deficit = _resource_deficit(
        desired_resources,
        projected_free_resources,
    )
    reasons: list[str] = []
    actions: list[ScaleAction] = []

    if incompatible_stop_candidates:
        job_ids = tuple(node.job_id for node in incompatible_stop_candidates)
        reason = "idle sandbox node(s) have incompatible agent version"
        actions.append(
            ScaleAction(
                kind="stop",
                count=len(job_ids),
                job_ids=job_ids,
                reason=reason,
            )
        )
        reasons.append(reason)

    if total_nodes < policy.min_nodes:
        missing_nodes = policy.min_nodes - total_nodes
        create_count = min(
            missing_nodes,
            _create_budget(policy, total_nodes, len(provisioning_nodes), actions),
        )
        if create_count > 0:
            reason = f"below min_nodes={policy.min_nodes}"
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        else:
            reason = _create_limit_reason(
                policy,
                total_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append(f"cannot satisfy min_nodes={policy.min_nodes}: {reason}")

    if _has_resource_demand(desired_resources) and _has_resource_deficit(
        resource_deficit
    ):
        needed_nodes = _nodes_for_resource_deficit(resource_deficit, policy)
        create_count = min(
            needed_nodes,
            _create_budget(policy, total_nodes, len(provisioning_nodes), actions),
        )
        if create_count > 0:
            reason = (
                "projected free resources "
                f"{_resource_label(projected_free_resources)} below desired "
                f"{_resource_label(desired_resources)}"
            )
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        else:
            reason = _create_limit_reason(
                policy,
                total_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append(
                    "cannot create for resource deficit "
                    f"{_resource_label(resource_deficit)}: {reason}"
                )

    planned_creates = _planned_creates(actions)
    if planned_creates == 0 and not _has_resource_deficit(resource_deficit):
        excess_nodes = total_nodes - policy.min_nodes
        stop_budget = max(0, policy.max_stop_per_cycle - _planned_stops(actions))
        if excess_nodes > 0 and stop_budget > 0:
            stop_candidates = _stop_candidates(
                ready_nodes,
                policy,
                now,
                required_resources=desired_resources,
                max_count=min(excess_nodes, stop_budget),
            )
            if stop_candidates:
                job_ids = tuple(node.job_id for node in stop_candidates)
                reason = _stop_reason(
                    ready_nodes,
                    policy,
                    required_resources=desired_resources,
                    job_ids=job_ids,
                )
                actions.append(
                    ScaleAction(
                        kind="stop",
                        count=len(job_ids),
                        job_ids=job_ids,
                        reason=reason,
                    )
                )
                reasons.append(reason)

    if not actions and not reasons:
        reasons.append("current pool matches demand and policy")

    return ScaleDecision(
        actions=tuple(actions),
        ready_nodes=len(ready_nodes),
        provisioning_nodes=len(provisioning_nodes),
        total_nodes=total_nodes,
        pending_resources=demand.pending_resources,
        prepared_resources=demand.prepared_resources,
        desired_resources=desired_resources,
        projected_free_resources=projected_free_resources,
        resource_deficit=resource_deficit,
        reasons=tuple(reasons),
    )


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


def _planned_creates(actions: list[ScaleAction]) -> int:
    return sum(action.count for action in actions if action.kind == "create")


def _planned_stops(actions: list[ScaleAction]) -> int:
    return sum(action.count for action in actions if action.kind == "stop")


def _create_budget(
    policy: ScalePolicy,
    total_nodes: int,
    provisioning_nodes: int,
    actions: list[ScaleAction],
) -> int:
    planned = _planned_creates(actions)
    limits = [
        max(0, policy.max_nodes - total_nodes - planned),
        max(0, policy.max_create_per_cycle - planned),
    ]
    if policy.max_provisioning_nodes > 0:
        limits.append(
            max(0, policy.max_provisioning_nodes - provisioning_nodes - planned)
        )
    return min(limits)


def _create_limit_reason(
    policy: ScalePolicy,
    total_nodes: int,
    provisioning_nodes: int,
    actions: list[ScaleAction],
) -> str:
    planned = _planned_creates(actions)
    if total_nodes + planned >= policy.max_nodes:
        return f"max_nodes={policy.max_nodes} reached"
    if planned >= policy.max_create_per_cycle:
        return f"max_create_per_cycle={policy.max_create_per_cycle} reached"
    if (
        policy.max_provisioning_nodes > 0
        and provisioning_nodes + planned >= policy.max_provisioning_nodes
    ):
        return f"max_provisioning_nodes={policy.max_provisioning_nodes} reached"
    return ""


def _projected_free_resources(
    nodes: list[SandboxNode],
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> ResourceQuantity:
    total = ResourceQuantity()
    for node in nodes:
        if node.job.is_final:
            continue
        if node.heartbeat is not None:
            if node.is_schedulable:
                total = total + _security_adjusted_resources(
                    node,
                    node.heartbeat.free_resources,
                )
            elif node.is_provisioning:
                effective = node.heartbeat.effective_resources
                if _has_resource_demand(effective):
                    total = total + _scale_resources(
                        _security_adjusted_resources(node, effective),
                        _provisioning_weight(
                            node,
                            policy,
                            now,
                            oldest_pending_seconds,
                        ),
                    )
                else:
                    total = total + _weighted_estimated_node_resources(
                        node,
                        policy,
                        now,
                        oldest_pending_seconds,
                    )
            continue
        if node.is_provisioning:
            total = total + _weighted_estimated_node_resources(
                node,
                policy,
                now,
                oldest_pending_seconds,
            )
    return total


def _weighted_estimated_node_resources(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> ResourceQuantity:
    return _scale_resources(
        _security_adjusted_resources(node, _estimated_node_resources(node, policy)),
        _provisioning_weight(node, policy, now, oldest_pending_seconds),
    )


def _estimated_node_resources(
    node: SandboxNode,
    policy: ScalePolicy,
) -> ResourceQuantity:
    vcpu = float(node.job.cpu or 0)
    memory_mb = int((node.job.memory_gb or 0) * 1024)
    disk_mb = int((node.job.disk_gb or 0) * 1024)
    if vcpu <= 0:
        vcpu = policy.default_node_resources.vcpu
    if memory_mb <= 0:
        memory_mb = policy.default_node_resources.memory_mb
    if disk_mb <= 0:
        disk_mb = policy.default_node_resources.disk_mb
    return ResourceQuantity(
        vcpu=vcpu,
        memory_mb=memory_mb,
        disk_mb=disk_mb,
    )


def _provisioning_weight(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> float:
    weight = _clamp_ratio(policy.provisioning_capacity_weight)
    stale_after = max(0, policy.stale_provisioning_after_seconds)
    if stale_after <= 0:
        return weight
    provisioning_age = _provisioning_age_seconds(node, now) or 0.0
    age_seconds = max(provisioning_age, float(max(0, oldest_pending_seconds)))
    if age_seconds >= stale_after:
        return min(weight, _clamp_ratio(policy.stale_provisioning_capacity_weight))
    return weight


def _counts_as_pool_node(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> bool:
    del policy, now, oldest_pending_seconds
    if node.job.is_final:
        return False
    # Capacity weighting and hard provider limits are separate concerns. A stale
    # provisioning job may contribute no projected resources, but it is still a
    # live VM and must count against max_nodes until UCloud reports it final.
    return True


def _counts_as_active_provisioning(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> bool:
    del policy, now, oldest_pending_seconds
    # max_provisioning_nodes is a hard in-flight job limit, not a measure of the
    # capacity currently credited to that job.
    return node.job.state in {"IN_QUEUE", "SUSPENDED"} or (
        node.job.state == "RUNNING" and not node.heartbeat_fresh
    )


def _provisioning_age_seconds(node: SandboxNode, now: datetime) -> float | None:
    reference = (
        node.job.started_at if node.job.state == "RUNNING" else node.job.created_at
    )
    if reference is None:
        return None
    return max(0.0, (now - reference).total_seconds())


def _clamp_ratio(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _scale_resources(value: ResourceQuantity, weight: float) -> ResourceQuantity:
    weight = _clamp_ratio(weight)
    return ResourceQuantity(
        vcpu=value.vcpu * weight,
        memory_mb=int(value.memory_mb * weight),
        disk_mb=int(value.disk_mb * weight),
    )


def _add_resources(
    left: ResourceQuantity,
    right: ResourceQuantity,
) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=max(0.0, left.vcpu) + max(0.0, right.vcpu),
        memory_mb=max(0, left.memory_mb) + max(0, right.memory_mb),
        disk_mb=max(0, left.disk_mb) + max(0, right.disk_mb),
    )


def _subtract_resources(
    left: ResourceQuantity,
    right: ResourceQuantity,
) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=max(0.0, left.vcpu - right.vcpu),
        memory_mb=max(0, left.memory_mb - right.memory_mb),
        disk_mb=max(0, left.disk_mb - right.disk_mb),
    )


def _stop_candidates(
    ready_nodes: list[SandboxNode],
    policy: ScalePolicy,
    now: datetime,
    *,
    required_resources: ResourceQuantity,
    max_count: int,
) -> list[SandboxNode]:
    if max_count <= 0:
        return []
    candidates: list[SandboxNode] = []
    remaining_free_resources = _ready_free_resources(ready_nodes, policy)
    for node in ready_nodes:
        if len(candidates) >= max_count:
            break
        if not node.is_idle:
            continue
        if not _past_idle_grace(node, policy, now):
            continue
        node_free_resources = _node_free_resources(node, policy)
        after_resources = _subtract_resources(
            remaining_free_resources, node_free_resources
        )
        if not required_resources.fits_within(after_resources):
            continue
        candidates.append(node)
        remaining_free_resources = after_resources
    return candidates


def _incompatible_stop_candidates(
    nodes: list[SandboxNode],
    *,
    now: datetime,
) -> list[SandboxNode]:
    candidates: list[SandboxNode] = []
    for node in nodes:
        if node.job.is_final or node.agent_version_compatible:
            continue
        if node.job.state in {"IN_QUEUE", "SUSPENDED"}:
            candidates.append(node)
            continue
        if node.job.state == "RUNNING" and node.heartbeat_fresh and node.is_idle:
            candidates.append(node)
    return sorted(
        candidates,
        key=lambda node: (
            node.job.started_at or node.job.created_at or now,
            node.job_id,
        ),
    )


def _stop_reason(
    ready_nodes: list[SandboxNode],
    policy: ScalePolicy,
    *,
    required_resources: ResourceQuantity,
    job_ids: tuple[str, ...],
) -> str:
    if _has_resource_demand(required_resources):
        remaining = _ready_free_resources(
            [node for node in ready_nodes if node.job_id not in set(job_ids)],
            policy,
        )
        return (
            "idle resources remain above desired demand after stopping "
            f"{', '.join(job_ids)}: remaining={_resource_label(remaining)}, "
            f"desired={_resource_label(required_resources)}"
        )
    return (
        "idle node exceeds min_nodes="
        f"{policy.min_nodes} with no pending resource demand"
    )


def _ready_free_resources(
    ready_nodes: list[SandboxNode],
    policy: ScalePolicy,
) -> ResourceQuantity:
    total = ResourceQuantity()
    for node in ready_nodes:
        total = total + _node_free_resources(node, policy)
    return total


def _node_free_resources(
    node: SandboxNode,
    policy: ScalePolicy,
) -> ResourceQuantity:
    if node.heartbeat is None:
        return _security_adjusted_resources(
            node, _estimated_node_resources(node, policy)
        )
    free = node.heartbeat.free_resources
    if _has_resource_demand(free):
        return _security_adjusted_resources(node, free)
    return _security_adjusted_resources(node, _estimated_node_resources(node, policy))


def _security_adjusted_resources(
    node: SandboxNode,
    resources: ResourceQuantity,
) -> ResourceQuantity:
    if resources.disk_mb <= 0 or _node_has_disk_quota(node):
        return resources
    return ResourceQuantity(
        vcpu=resources.vcpu,
        memory_mb=resources.memory_mb,
        disk_mb=0,
    )


def _node_has_disk_quota(node: SandboxNode) -> bool:
    if node.heartbeat is None:
        return node.is_provisioning
    return node.heartbeat is not None and has_capability(
        node.heartbeat.capabilities,
        DISK_QUOTA_CAPABILITY,
    )


def _past_idle_grace(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
) -> bool:
    idle_seconds = max(0, policy.scale_down_idle_seconds)
    if idle_seconds == 0:
        return True
    reference = (
        node.heartbeat.idle_since
        if node.heartbeat is not None and node.heartbeat.idle_since is not None
        else node.heartbeat.updated_at
        if node.heartbeat is not None and node.active_sandboxes == 0
        else node.job.started_at or node.job.created_at
    )
    if reference is None:
        return False
    return (now - reference).total_seconds() >= idle_seconds


def _resource_deficit(
    demand: ResourceQuantity,
    projected_free: ResourceQuantity,
) -> ResourceQuantity:
    return ResourceQuantity(
        vcpu=max(0.0, demand.vcpu - projected_free.vcpu),
        memory_mb=max(0, demand.memory_mb - projected_free.memory_mb),
        disk_mb=max(0, demand.disk_mb - projected_free.disk_mb),
    )


def _has_resource_demand(value: ResourceQuantity) -> bool:
    return value.vcpu > 0 or value.memory_mb > 0 or value.disk_mb > 0


def _has_resource_deficit(value: ResourceQuantity) -> bool:
    return value.vcpu > 0 or value.memory_mb > 0 or value.disk_mb > 0


def _nodes_for_resource_deficit(deficit: ResourceQuantity, policy: ScalePolicy) -> int:
    defaults = policy.default_node_resources
    counts = [1]
    if deficit.vcpu > 0 and defaults.vcpu > 0:
        counts.append(_ceil_div_float(deficit.vcpu, defaults.vcpu))
    if deficit.memory_mb > 0 and defaults.memory_mb > 0:
        counts.append(_ceil_div(deficit.memory_mb, defaults.memory_mb))
    if deficit.disk_mb > 0 and defaults.disk_mb > 0:
        counts.append(_ceil_div(deficit.disk_mb, defaults.disk_mb))
    return max(counts)


def _ceil_div_float(value: float, divisor: float) -> int:
    return int(math.ceil(value / divisor))


def _resource_label(value: ResourceQuantity) -> str:
    return f"{value.vcpu:g}vcpu/{value.memory_mb}MB/{value.disk_mb}MB"
