from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import math

from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    DYNAMIC_ACTIVE_ADMISSION_CAPABILITY,
    has_capability,
)
from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    SandboxPlacementRequest,
    LiveScaleSignals,
    ProgramScaleSignals,
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
    live_signals: LiveScaleSignals | None = None,
    program_signals: ProgramScaleSignals | None = None,
) -> ScaleDecision:
    if now is None:
        now = utc_now()
    stop_budget = max(0, policy.max_stop_per_cycle)
    unreachable_stop_candidates = _unreachable_stop_candidates(
        nodes,
        policy,
        now=now,
    )[:stop_budget]
    unreachable_job_ids = {
        node.job_id for node in unreachable_stop_candidates
    }
    incompatible_stop_candidates = _incompatible_stop_candidates(
        [node for node in nodes if node.job_id not in unreachable_job_ids],
        now=now,
    )[: max(0, stop_budget - len(unreachable_stop_candidates))]
    pool_nodes = [node for node in nodes if _counts_as_pool_node(node, policy, now, 0)]
    # A booting node can temporarily have no usable version label. It remains
    # unschedulable, but receives normal time-decaying provisioning credit so
    # transient metadata lag or a failed bootstrap cannot create replacement
    # VMs while the first VM is already billable. A ready incompatible node
    # contributes no capacity.
    capacity_nodes = [
        node
        for node in pool_nodes
        if node.agent_version_compatible or node.is_provisioning
    ]
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
    unreachable_nodes = [
        node
        for node in pool_nodes
        if _counts_as_unreachable(
            node,
            policy,
            now,
            oldest_pending_seconds,
        )
    ]
    total_nodes = len(pool_nodes)
    effective_scale_down_idle_seconds = policy.scale_down_idle_seconds
    if (
        policy.live_pressure_enabled
        and live_signals is not None
        and live_signals.provisioning_p95_seconds is not None
    ):
        effective_scale_down_idle_seconds = max(
            effective_scale_down_idle_seconds,
            int(
                math.ceil(
                    live_signals.provisioning_p95_seconds
                    * max(0.0, policy.provisioning_scale_down_multiplier)
                )
            ),
        )
    effective_policy = replace(
        policy,
        scale_down_idle_seconds=effective_scale_down_idle_seconds,
    )
    pressure_scale_up = _live_pressure_requires_capacity(
        policy,
        live_signals,
    ) and not any(node.is_idle for node in ready_nodes)
    create_pressure_scale_up = _create_pressure_requires_capacity(
        policy,
        live_signals,
    )

    maximum_request = policy.schedulable_node_resources
    demand_placement_requests = tuple(
        request
        for request in demand.placement_requests
        if request.resources.fits_within(maximum_request)
    )
    unschedulable_placements = len(demand.placement_requests) - len(
        demand_placement_requests
    )
    if demand.placement_requests:
        demand_resources = _placement_request_resources(
            demand_placement_requests,
            dynamic=policy.dynamic_active_admission_enabled,
            include_disk=True,
        )
    else:
        # Older state backends cannot reconstruct individual shapes. Retain
        # conservative aggregation rather than guessing at request boundaries.
        demand_resources = demand.desired_resources
    program_placement_requests: tuple[SandboxPlacementRequest, ...] = ()
    if (
        policy.program_aware_autoscaling_enabled
        and program_signals is not None
    ):
        program_placement_requests = tuple(
            request
            for request in program_signals.ready_placement_requests
            if request.resources.fits_within(maximum_request)
        )
        unschedulable_placements += len(
            program_signals.ready_placement_requests
        ) - len(program_placement_requests)
        if program_signals.ready_placement_requests:
            program_resources = _add_resources(
                _placement_request_resources(
                    program_placement_requests,
                    dynamic=policy.dynamic_active_admission_enabled,
                    include_disk=False,
                ),
                program_signals.weighted_model_wait_resources,
            )
        else:
            program_resources = (
                _dynamic_program_resources(program_signals)
                if policy.dynamic_active_admission_enabled
                else program_signals.effective_resources
            )
        demand_resources = _add_resources(
            demand_resources,
            program_resources,
        )
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
    placement_requests = (
        *demand_placement_requests,
        *program_placement_requests,
    )
    placement_nodes = _nodes_for_unplaced_requests(
        capacity_nodes,
        placement_requests,
        policy,
        now=now,
        oldest_pending_seconds=oldest_pending_seconds,
    )
    reasons: list[str] = []
    actions: list[ScaleAction] = []

    if unschedulable_placements > 0:
        reasons.append(
            f"{unschedulable_placements} placement request(s) exceed the "
            "configured schedulable node shape and were excluded from create demand"
        )

    if demand.suppressed_pending_count > 0:
        reasons.append(
            f"{demand.suppressed_pending_count} non-capacity pending failure(s) "
            "excluded from fleet demand"
        )

    if unreachable_stop_candidates:
        job_ids = tuple(node.job_id for node in unreachable_stop_candidates)
        reason = "unreachable empty sandbox node(s) exceeded the eviction lease"
        actions.append(
            ScaleAction(
                kind="stop",
                count=len(job_ids),
                job_ids=job_ids,
                reason=reason,
            )
        )
        reasons.append(reason)

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

    if placement_nodes > 0 or (
        _has_resource_demand(desired_resources)
        and _has_resource_deficit(resource_deficit)
    ):
        deficit_nodes = (
            _nodes_for_resource_deficit(resource_deficit, policy)
            if _has_resource_deficit(resource_deficit)
            else 0
        )
        # A node already planned to restore ``min_nodes`` contributes the same
        # default schedulable shape as a resource-deficit create.  Count it
        # once; otherwise loss of the final node produces one replacement for
        # the minimum and a second replacement for the exact same demand.
        needed_nodes = max(
            0,
            max(deficit_nodes, placement_nodes) - _planned_creates(actions),
        )
        create_count = min(
            needed_nodes,
            _create_budget(policy, total_nodes, len(provisioning_nodes), actions),
        )
        if create_count > 0:
            if placement_nodes > deficit_nodes:
                reason = (
                    f"{placement_nodes} additional node(s) required because "
                    "pending sandbox shapes do not fit any single projected node"
                )
            else:
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

    if (
        pressure_scale_up
        # Create saturation is a more specific interpretation of this same
        # live-pressure sample and owns its bounded headroom calculation
        # below. Letting the generic branch act first bypasses
        # create_pressure_max_headroom_nodes on every later cycle.
        and not create_pressure_scale_up
        and _planned_creates(actions) == 0
        and len(provisioning_nodes) == 0
        and ready_nodes
    ):
        create_count = min(
            1,
            _create_budget(policy, total_nodes, len(provisioning_nodes), actions),
        )
        if create_count > 0:
            reason = _live_pressure_reason(policy, live_signals)
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)

    if create_pressure_scale_up:
        assert live_signals is not None
        baseline_nodes = max(0, policy.min_nodes)
        if _has_resource_demand(desired_resources):
            baseline_nodes = max(
                baseline_nodes,
                _nodes_for_resource_deficit(desired_resources, policy),
            )
        elif total_nodes > 0:
            baseline_nodes = max(baseline_nodes, 1)
        pipeline_nodes = _ceil_div(
            max(1, live_signals.sandbox_create_limit),
            max(1, policy.create_target_concurrency_per_node),
        )
        target_nodes = min(
            policy.max_nodes,
            max(
                baseline_nodes,
                min(
                    pipeline_nodes,
                    baseline_nodes
                    + max(0, policy.create_pressure_max_headroom_nodes),
                ),
            ),
        )
        needed_nodes = max(
            0,
            target_nodes - total_nodes - _planned_creates(actions),
        )
        create_count = min(
            needed_nodes,
            _create_budget(policy, total_nodes, len(provisioning_nodes), actions),
        )
        if create_count > 0:
            reason = (
                "sandbox create pipeline saturated at "
                f"{live_signals.sandbox_create_limit} concurrent request(s); "
                f"targeting {target_nodes} temporary node(s) after "
                f"{live_signals.sandbox_create_rejections} recent rejection(s)"
            )
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        elif target_nodes > total_nodes + _planned_creates(actions):
            reason = _create_limit_reason(
                policy,
                total_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append(
                    "cannot create temporary sandbox-create headroom: " + reason
                )

    planned_creates = _planned_creates(actions)
    if (
        planned_creates == 0
        and not _has_resource_deficit(resource_deficit)
        and placement_nodes == 0
    ):
        excess_nodes = total_nodes - policy.min_nodes
        stop_budget = max(0, policy.max_stop_per_cycle - _planned_stops(actions))
        latest_capacity_pressure_age = _latest_capacity_pressure_age(
            policy,
            live_signals,
        )
        pressure_cooldown = bool(
            (policy.live_pressure_enabled or policy.create_pressure_enabled)
            and live_signals is not None
            and latest_capacity_pressure_age is not None
            and latest_capacity_pressure_age
            < policy.pressure_scale_down_cooldown_seconds
        )
        if pressure_cooldown:
            reasons.append(
                "recent live pressure retains ready capacity during cooldown"
            )
        elif excess_nodes > 0 and stop_budget > 0:
            stop_candidates = _stop_candidates(
                ready_nodes,
                effective_policy,
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
        unreachable_nodes=len(unreachable_nodes),
        pending_resources=demand.pending_resources,
        suppressed_pending_resources=demand.suppressed_pending_resources,
        pending_count=demand.pending_count,
        suppressed_pending_count=demand.suppressed_pending_count,
        prepared_resources=demand.prepared_resources,
        desired_resources=desired_resources,
        projected_free_resources=projected_free_resources,
        resource_deficit=resource_deficit,
        reasons=tuple(reasons),
        live_signals=live_signals,
        program_signals=program_signals,
        pressure_scale_up=pressure_scale_up,
        create_pressure_scale_up=create_pressure_scale_up,
        effective_scale_down_idle_seconds=effective_scale_down_idle_seconds,
    )


def _live_pressure_requires_capacity(
    policy: ScalePolicy,
    signals: LiveScaleSignals | None,
) -> bool:
    if not policy.live_pressure_enabled or signals is None:
        return False
    age = signals.latest_pressure_age_seconds
    return bool(
        signals.pressure_samples >= policy.live_pressure_min_samples
        and age is not None
        and age <= policy.live_pressure_fresh_seconds
    )


def _create_pressure_requires_capacity(
    policy: ScalePolicy,
    signals: LiveScaleSignals | None,
) -> bool:
    if not policy.create_pressure_enabled or signals is None:
        return False
    age = signals.latest_create_pressure_age_seconds
    return bool(
        signals.create_pressure_samples >= policy.create_pressure_min_samples
        and signals.sandbox_create_limit > 0
        and age is not None
        and age <= policy.create_pressure_fresh_seconds
        # Gateway saturation says callers are waiting, but not that another VM
        # would help. Require the ordinary sustained node-pressure proof before
        # treating it as a burst-capacity signal. Gateway pressure can then
        # accelerate/magnify a real backlog without reacting to healthy cold
        # creates merely occupying request slots.
        and _live_pressure_requires_capacity(policy, signals)
    )


def _latest_capacity_pressure_age(
    policy: ScalePolicy,
    signals: LiveScaleSignals | None,
) -> int | None:
    if signals is None:
        return None
    ages: list[int] = []
    if (
        policy.live_pressure_enabled
        and signals.latest_pressure_age_seconds is not None
    ):
        ages.append(signals.latest_pressure_age_seconds)
    # Create pressure is only an amplifier for live node pressure. Its raw age
    # must not retain otherwise idle capacity after a harmless gateway burst;
    # the corroborating live-pressure age already supplies the cooldown when
    # the combined signal was actionable.
    return min(ages) if ages else None


def _live_pressure_reason(
    policy: ScalePolicy,
    signals: LiveScaleSignals | None,
) -> str:
    if signals is None:
        return "sustained live node pressure exceeds target headroom"
    values: list[str] = []
    if (
        signals.cpu_utilization is not None
        and signals.cpu_utilization >= policy.target_cpu_utilization
    ):
        values.append(f"cpu={signals.cpu_utilization:.0%}")
    if (
        signals.memory_utilization is not None
        and signals.memory_utilization >= policy.target_memory_utilization
    ):
        values.append(f"memory={signals.memory_utilization:.0%}")
    if (
        signals.memory_psi_full_avg10 is not None
        and signals.memory_psi_full_avg10 >= policy.max_memory_psi_full_avg10
    ):
        values.append(f"memory-psi={signals.memory_psi_full_avg10:g}")
    if (
        signals.storage_queue_utilization is not None
        and signals.storage_queue_utilization
        >= policy.target_storage_queue_utilization
    ):
        values.append(f"storage-queue={signals.storage_queue_utilization:.0%}")
    if (
        signals.rootfs_export_queue_utilization is not None
        and signals.rootfs_export_queue_utilization
        >= policy.target_storage_queue_utilization
    ):
        values.append(
            f"rootfs-export={signals.rootfs_export_queue_utilization:.0%}"
        )
    suffix = f" ({', '.join(values)})" if values else ""
    return (
        f"sustained live pressure across {signals.pressure_samples} sample(s)"
        f"{suffix}"
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
                total = total + _projected_provisioning_resources(
                    node,
                    policy,
                    now,
                    oldest_pending_seconds,
                )
            continue
        if node.is_provisioning:
            total = total + _projected_provisioning_resources(
                node,
                policy,
                now,
                oldest_pending_seconds,
            )
    return total


def _nodes_for_unplaced_requests(
    nodes: list[SandboxNode],
    requests: tuple[SandboxPlacementRequest, ...],
    policy: ScalePolicy,
    *,
    now: datetime,
    oldest_pending_seconds: int,
) -> int:
    """Bin-pack accepted request shapes so aggregate free space cannot lie."""

    if not requests:
        return 0
    bins: list[tuple[str, ResourceQuantity, bool]] = []
    for node in nodes:
        if node.job.is_final:
            continue
        if node.heartbeat is not None and node.is_schedulable:
            bins.append(
                (
                    node.job_id,
                    _security_adjusted_resources(
                        node, node.heartbeat.free_resources
                    ),
                    has_capability(
                        node.heartbeat.capabilities,
                        DYNAMIC_ACTIVE_ADMISSION_CAPABILITY,
                    ),
                )
            )
        elif node.is_provisioning:
            available = _projected_provisioning_resources(
                node,
                policy,
                now,
                oldest_pending_seconds,
            )
            if not _has_resource_demand(available):
                continue
            bins.append(
                (
                    node.job_id,
                    available,
                    policy.dynamic_active_admission_enabled,
                )
            )
    default_bin = policy.schedulable_node_resources

    def pressure(
        placement: SandboxPlacementRequest,
    ) -> tuple[float, int, float]:
        request = placement.resources
        ratios = (
            request.vcpu / default_bin.vcpu if default_bin.vcpu > 0 else 0.0,
            request.memory_mb / default_bin.memory_mb
            if default_bin.memory_mb > 0
            else 0.0,
            request.disk_mb / default_bin.disk_mb
            if default_bin.disk_mb > 0
            else 0.0,
        )
        return max(ratios), request.memory_mb + request.disk_mb, request.vcpu

    missing = 0
    for placement in sorted(requests, key=pressure, reverse=True):
        requested = placement.resources
        excluded = set(placement.excluded_job_ids)
        fitting: list[tuple[int, str, ResourceQuantity, bool]] = []
        for index, (job_id, available, dynamic_active) in enumerate(bins):
            if job_id in excluded:
                continue
            available_for_request = available
            if job_id == placement.owned_job_id and placement.owned_disk_mb > 0:
                available_for_request = replace(
                    available,
                    disk_mb=available.disk_mb + placement.owned_disk_mb,
                )
            if requested.fits_within(available_for_request):
                fitting.append(
                    (index, job_id, available_for_request, dynamic_active)
                )
        if fitting:
            index, job_id, available, dynamic_active = min(
                fitting,
                key=lambda item: (
                    item[2].disk_mb - requested.disk_mb,
                    item[2].memory_mb - requested.memory_mb,
                    item[2].vcpu - requested.vcpu,
                ),
            )
            bins[index] = (
                job_id,
                _subtract_dynamic_resources(available, requested)
                if dynamic_active
                else _subtract_resources(available, requested),
                dynamic_active,
            )
            continue
        missing += 1
        bins.append(
            (
                "",
                (
                    _subtract_dynamic_resources(default_bin, requested)
                    if policy.dynamic_active_admission_enabled
                    else _subtract_resources(default_bin, requested)
                )
                if requested.fits_within(default_bin)
                else ResourceQuantity(),
                policy.dynamic_active_admission_enabled,
            )
        )
    return missing


def _placement_request_resources(
    requests: tuple[SandboxPlacementRequest, ...],
    *,
    dynamic: bool,
    include_disk: bool,
) -> ResourceQuantity:
    """Aggregate exact schedulable shapes without weakening hard disk ownership."""

    if not dynamic:
        total = ResourceQuantity()
        for request in requests:
            resources = request.resources
            total = total + ResourceQuantity(
                vcpu=resources.vcpu,
                memory_mb=resources.memory_mb,
                disk_mb=resources.disk_mb if include_disk else 0,
            )
        return total
    return ResourceQuantity(
        vcpu=max(
            (item.resources.vcpu for item in requests),
            default=0.0,
        ),
        memory_mb=max(
            (item.resources.memory_mb for item in requests),
            default=0,
        ),
        disk_mb=(
            sum(item.resources.disk_mb for item in requests)
            if include_disk
            else 0
        ),
    )


def _dynamic_program_resources(
    signals: ProgramScaleSignals,
) -> ResourceQuantity:
    """Keep predictive headroom without adding every ready CPU/RAM limit."""

    if signals.ready_placement_requests:
        ready = ResourceQuantity(
            vcpu=max(
                (
                    item.resources.vcpu
                    for item in signals.ready_placement_requests
                ),
                default=0.0,
            ),
            memory_mb=max(
                (
                    item.resources.memory_mb
                    for item in signals.ready_placement_requests
                ),
                default=0,
            ),
        )
    else:
        # Preserve conservative behavior for program-state backends that do
        # not expose exact ready-to-wake shapes.
        ready = signals.ready_to_wake_resources
    return _add_resources(ready, signals.weighted_model_wait_resources)


def _projected_provisioning_resources(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> ResourceQuantity:
    weight = _provisioning_weight(
        node,
        policy,
        now,
        oldest_pending_seconds,
    )
    if node.heartbeat is not None and _has_resource_demand(
        node.heartbeat.effective_resources
    ):
        return _scale_resources(
            _security_adjusted_resources(node, node.heartbeat.free_resources),
            weight,
        )
    return _scale_resources(
        _security_adjusted_resources(node, _estimated_node_resources(node, policy)),
        weight,
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
    physical = ResourceQuantity(
        vcpu=vcpu,
        memory_mb=memory_mb,
        disk_mb=disk_mb,
    )
    estimated = physical.scaled(
        cpu=max(0.0, policy.cpu_overcommit),
        memory=max(0.0, policy.memory_overcommit),
        disk=min(1.0, max(0.0, policy.disk_overcommit)),
    )
    maximum = policy.schedulable_node_resources
    return ResourceQuantity(
        vcpu=min(estimated.vcpu, maximum.vcpu),
        memory_mb=min(estimated.memory_mb, maximum.memory_mb),
        disk_mb=min(estimated.disk_mb, maximum.disk_mb),
    )


def _provisioning_weight(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> float:
    del oldest_pending_seconds
    weight = _clamp_ratio(policy.provisioning_capacity_weight)
    stale_after = max(0, policy.stale_provisioning_after_seconds)
    if stale_after <= 0:
        return weight
    provisioning_age = _provisioning_age_seconds(node, now)
    if provisioning_age is None:
        return min(weight, _clamp_ratio(policy.stale_provisioning_capacity_weight))
    if provisioning_age >= stale_after:
        return min(weight, _clamp_ratio(policy.stale_provisioning_capacity_weight))
    return weight


def _counts_as_pool_node(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> bool:
    del policy, now, oldest_pending_seconds
    if (
        node.job.is_final
        or node.job.is_unexpectedly_suspended
        or node.permanently_lost
    ):
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
    del oldest_pending_seconds
    # max_provisioning_nodes is a hard in-flight job limit, not a measure of the
    # capacity currently credited to that job. A RUNNING VM that previously
    # heartbeated is unreachable, not provisioning. A VM that never reached its
    # first heartbeat receives only a bounded bootstrap grace period.
    if not node.is_provisioning:
        return False
    if node.job.state != "RUNNING":
        return True
    stale_after = max(0, policy.stale_provisioning_after_seconds)
    age = _provisioning_age_seconds(node, now)
    return bool(stale_after <= 0 or age is None or age < stale_after)


def _counts_as_unreachable(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> bool:
    return bool(
        node.job.state == "RUNNING"
        and not node.heartbeat_fresh
        and not _counts_as_active_provisioning(
            node,
            policy,
            now,
            oldest_pending_seconds,
        )
    )


def _provisioning_age_seconds(node: SandboxNode, now: datetime) -> float | None:
    reference = (
        (node.job.started_at or node.job.created_at)
        if node.job.state == "RUNNING"
        else node.job.created_at
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


def _subtract_dynamic_resources(
    left: ResourceQuantity,
    right: ResourceQuantity,
) -> ResourceQuantity:
    """Consume hard disk while retaining reusable dynamic CPU/RAM headroom."""

    return ResourceQuantity(
        vcpu=left.vcpu,
        memory_mb=left.memory_mb,
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
        if node.job.state == "IN_QUEUE" or node.job.is_initially_suspended:
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


def unreachable_node_stop_ready(
    node: SandboxNode,
    policy: ScalePolicy,
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether an unreachable node has conservative provider-stop proof.

    A fresh node must always use the drain-token handshake.  This fallback is
    only for a running VM that has exceeded its heartbeat lease, owns no known
    sandbox routes, and whose last complete inventory was empty.  A VM that
    never emitted a heartbeat cannot have admitted gateway-managed work.
    """

    if now is None:
        now = utc_now()
    timeout_seconds = max(0, policy.unreachable_stop_after_seconds)
    if (
        timeout_seconds <= 0
        or node.job.is_final
        or node.job.state != "RUNNING"
        or node.heartbeat_fresh
        or node.active_sandboxes != 0
    ):
        return False
    reference = unreachable_node_reference(node)
    if reference is None or (now - reference).total_seconds() < timeout_seconds:
        return False
    heartbeat = node.heartbeat
    if heartbeat is None:
        return True
    return bool(
        heartbeat.inventory_complete
        and not heartbeat.inventory
        and heartbeat.active_workloads == 0
        and heartbeat.used_resources == ResourceQuantity()
        and heartbeat.reserved_resources == ResourceQuantity()
        and heartbeat.build_reserved_resources == ResourceQuantity()
    )


def unreachable_node_reference(node: SandboxNode) -> datetime | None:
    heartbeat = node.heartbeat
    if heartbeat is not None:
        return heartbeat.freshness_at
    return node.job.started_at or node.job.created_at


def _unreachable_stop_candidates(
    nodes: list[SandboxNode],
    policy: ScalePolicy,
    *,
    now: datetime,
) -> list[SandboxNode]:
    return sorted(
        [
            node
            for node in nodes
            if unreachable_node_stop_ready(node, policy, now=now)
        ],
        key=lambda node: (
            unreachable_node_reference(node) or now,
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
    if node.is_provisioning:
        return True
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
    defaults = policy.schedulable_node_resources
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
