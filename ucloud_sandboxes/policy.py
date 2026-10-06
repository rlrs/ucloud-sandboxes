from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import math

from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    RUNTIME_CPU_CAPABILITY_PREFIX,
    has_capability,
)
from .consolidation import consolidation_rank
from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxNode,
    SandboxPlacementRequest,
    LiveScaleSignals,
    ScaleAction,
    ScaleDecision,
    ScalePolicy,
    SOFT_DRAIN_LABEL,
    is_soft_drained,
    retiring_reason,
    utc_now,
)
from .resource_admission import (
    dynamic_request_fits,
    node_storage_pressure_allows,
    reserve_dynamic_resources,
    reusable_dynamic_resources,
)


def evaluate_scale(
    nodes: list[SandboxNode],
    demand: SandboxDemand,
    policy: ScalePolicy,
    *,
    now: datetime | None = None,
    live_signals: LiveScaleSignals | None = None,
) -> ScaleDecision:
    if now is None:
        now = utc_now()
    stop_budget = max(0, policy.max_stop_per_cycle)
    unreachable_stop_candidates = _unreachable_stop_candidates(
        nodes,
        policy,
        now=now,
    )[:stop_budget]
    unreachable_job_ids = {node.job_id for node in unreachable_stop_candidates}
    incompatible_candidates = incompatible_stop_candidates(
        [node for node in nodes if node.job_id not in unreachable_job_ids],
        now=now,
        max_cpu_check_ms=policy.max_cpu_check_ms,
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
        if (node.agent_version_compatible or node.is_provisioning)
        # A retiring host (repeated reboots, a slow VM) keeps serving what it
        # holds while it retires; demand is planned without it.
        and not retiring_reason(node.heartbeat, now, max_cpu_check_ms=policy.max_cpu_check_ms)
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
    # A RUNNING provider job with an expired worker heartbeat is billable but
    # not useful capacity. Keep it in total_nodes for accounting and eventual
    # eviction, while allowing one-for-one replacement inside max_nodes.
    available_pool_nodes = max(0, total_nodes - len(unreachable_nodes))
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
    backlog_scale_up = _startup_backlog_requires_capacity(
        policy, demand, ready_nodes, live_signals
    )
    create_pressure_scale_up = backlog_scale_up or _create_pressure_requires_capacity(
        policy,
        live_signals,
    )
    pending_backlog_nodes = (
        _ceil_div(
            max(0, demand.pending_count),
            max(1, policy.create_target_concurrency_per_node),
        )
        if pressure_scale_up and demand.pending_count > 0
        else 0
    )

    maximum_request = policy.default_node_resources
    all_demand_placement_requests = (
        *demand.placement_requests,
        *demand.prepared_placement_requests,
    )
    placement_requests = tuple(
        request
        for request in all_demand_placement_requests
        if request.resources.fits_within(maximum_request)
    )
    unschedulable_placements = sum(
        request.count
        for request in all_demand_placement_requests
        if not request.resources.fits_within(maximum_request)
    )
    pending_resources = (
        _placement_request_resources(
            tuple(
                request
                for request in demand.placement_requests
                if request.resources.fits_within(maximum_request)
            ),
        )
        if demand.placement_requests
        else demand.pending_resources
    )
    prepared_resources = _placement_request_resources(
        tuple(
            request
            for request in demand.prepared_placement_requests
            if request.resources.fits_within(maximum_request)
        ),
    )
    # A pending cold start is future resident demand, not a permanent charge
    # for its sandbox limit. Once assigned, its transient worker reservation
    # replaces this promise; once running, measured working memory replaces it.
    pending_resources = replace(pending_resources, memory_mb=(
        _cold_memory_forecast(demand.placement_requests, maximum_request, policy)
        if demand.placement_requests else math.ceil(
            pending_resources.memory_mb / policy.target_memory_utilization)))
    prepared_resources = replace(prepared_resources, memory_mb=
        _cold_memory_forecast(demand.prepared_placement_requests, maximum_request, policy))
    resident_forecast = sum(_resident_memory_forecast(node, policy, now) for node in ready_nodes)
    demand_resources = _add_resources(pending_resources, prepared_resources)
    demand_resources = replace(demand_resources,
        memory_mb=demand_resources.memory_mb + resident_forecast)
    forecast_node_memory = _forecast_node_memory(nodes, policy)
    desired_resources = _add_resources(demand_resources, policy.warm_resources)
    soft_drain = plan_soft_drain(
        nodes,
        policy,
        now,
        required_resources=desired_resources,
        placement_requests=placement_requests,
        oldest_pending_seconds=oldest_pending_seconds,
        pending_count=demand.pending_count,
        allow_select=False,
    )

    def scale_up_capacity(withheld: str):
        # Creates avoid a soft-drained worker, so its capacity must not
        # hide demand from scale-up either.
        scale_nodes = [node for node in capacity_nodes if node.job_id != withheld]
        projected = _projected_free_resources(
            scale_nodes,
            policy,
            now,
            oldest_pending_seconds,
        )
        return projected, _subtract_resources(desired_resources, projected), (
            _nodes_for_unplaced_requests(
                scale_nodes,
                placement_requests,
                policy,
                now=now,
                oldest_pending_seconds=oldest_pending_seconds,
            )
        )

    projected_free_resources, resource_deficit, placement_nodes = (
        scale_up_capacity(soft_drain.job_id)
    )
    soft_drain_released = False
    if soft_drain.job_id and not soft_drain.retiring and (
        placement_nodes > 0
        or (_has_resources(desired_resources) and _has_resources(resource_deficit))
    ):
        # Reopening the drained worker is cheaper than buying a new one.
        soft_drain_released = True
        soft_drain = SoftDrainPlan(
            clear_job_ids=(*soft_drain.clear_job_ids, soft_drain.job_id),
            reason=f"{soft_drain.job_id} is needed for current demand",
        )
        projected_free_resources, resource_deficit, placement_nodes = (
            scale_up_capacity("")
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

    if incompatible_candidates:
        job_ids = tuple(node.job_id for node in incompatible_candidates)
        reason = "idle sandbox node(s) have incompatible agent version or rebooted repeatedly"
        actions.append(
            ScaleAction(
                kind="stop",
                count=len(job_ids),
                job_ids=job_ids,
                reason=reason,
            )
        )
        reasons.append(reason)

    if available_pool_nodes < policy.min_nodes:
        missing_nodes = policy.min_nodes - available_pool_nodes
        create_count = min(
            missing_nodes,
            _create_budget(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            ),
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
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append(f"cannot satisfy min_nodes={policy.min_nodes}: {reason}")

    if placement_nodes > 0 or (
        _has_resources(desired_resources) and _has_resources(resource_deficit)
    ):
        deficit_nodes = (
            _nodes_for_resource_deficit(resource_deficit, policy, memory_capacity_mb=forecast_node_memory)
            if _has_resources(resource_deficit)
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
            _create_budget(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            ),
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
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append(
                    "cannot create for resource deficit "
                    f"{_resource_label(resource_deficit)}: {reason}"
                )

    if pending_backlog_nodes > 0:
        # Pending create/wake rows are requests that the current fleet already
        # failed to admit. Nominal CPU and RAM limits remain reusable; only a
        # simultaneous real-pressure signal turns the retry backlog into
        # additional pipeline capacity.
        target_nodes = min(
            policy.max_nodes,
            available_pool_nodes + pending_backlog_nodes,
        )
        needed_nodes = max(
            0,
            target_nodes - available_pool_nodes - _planned_creates(actions),
        )
        create_count = min(
            needed_nodes,
            _create_budget(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            ),
        )
        if create_count > 0:
            reason = (
                f"{demand.pending_count} pressure-confirmed pending request(s) "
                f"require {pending_backlog_nodes} additional create pipeline "
                f"node(s); targeting {target_nodes} node(s)"
            )
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        elif target_nodes > available_pool_nodes + _planned_creates(actions):
            reason = _create_limit_reason(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            )
            if reason:
                reasons.append("cannot create for pending request backlog: " + reason)

    if (
        pressure_scale_up
        # Create saturation is a more specific interpretation of this same
        # live-pressure sample and owns its rejection-wave calculation below.
        # Letting the generic branch act first would undercount a sustained
        # gateway backlog on every later cycle.
        and not create_pressure_scale_up
        and ready_nodes
    ):
        pressure_target = min(policy.max_nodes, len(ready_nodes) + policy.max_create_per_cycle)
        create_count = min(
            max(0, pressure_target - available_pool_nodes - _planned_creates(actions)),
            _create_budget(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            ),
        )
        if create_count > 0:
            reason = _live_pressure_reason(policy, live_signals)
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        else:
            reason = _create_limit_reason(
                policy, available_pool_nodes, len(provisioning_nodes), actions
            )
            if reason:
                reasons.append("cannot create for live pressure: " + reason)

    if create_pressure_scale_up and not any(node.is_idle for node in ready_nodes):
        assert live_signals is not None
        baseline_nodes = max(len(ready_nodes), policy.min_nodes)
        if _has_resources(desired_resources):
            baseline_nodes = max(
                baseline_nodes,
                _nodes_for_resource_deficit(desired_resources, policy, memory_capacity_mb=forecast_node_memory),
            )
        elif available_pool_nodes > 0:
            baseline_nodes = max(baseline_nodes, 1)
        pipeline_nodes = _ceil_div(
            max(1, live_signals.sandbox_create_limit),
            max(1, policy.create_target_concurrency_per_node),
        )
        if backlog_scale_up:
            pipeline_nodes = max(
                pipeline_nodes,
                len(ready_nodes)
                + _ceil_div(
                    demand.pending_count,
                    max(1, policy.create_target_concurrency_per_node),
                ),
            )
        # Saturation on an already larger fleet still warrants its configured
        # headroom; an absolute pipeline count must not suppress live pressure.
        pipeline_nodes = max(pipeline_nodes, baseline_nodes + min(
            policy.max_create_per_cycle, policy.create_pressure_max_headroom_nodes))
        target_nodes = min(
            policy.max_nodes,
            max(
                baseline_nodes,
                min(
                    pipeline_nodes,
                    baseline_nodes + max(0, policy.create_pressure_max_headroom_nodes),
                ),
            ),
        )
        needed_nodes = max(
            0,
            target_nodes - available_pool_nodes - _planned_creates(actions),
        )
        create_count = min(
            needed_nodes,
            _create_budget(
                policy,
                available_pool_nodes,
                len(provisioning_nodes),
                actions,
            ),
        )
        if create_count > 0:
            reason = (
                (
                    f"{demand.pending_count} capacity request(s) queued for "
                    f"{demand.oldest_capacity_pending_seconds}s; targeting "
                    f"{target_nodes} temporary startup node(s)"
                )
                if backlog_scale_up
                else (
                    "sandbox create pipeline saturated at "
                    f"{live_signals.sandbox_create_limit} concurrent request(s); "
                    f"targeting {target_nodes} temporary node(s) after "
                    f"{live_signals.sandbox_create_rejections} recent rejection(s)"
                )
            )
            actions.append(
                ScaleAction(kind="create", count=create_count, reason=reason)
            )
            reasons.append(reason)
        elif target_nodes > available_pool_nodes + _planned_creates(actions):
            reason = _create_limit_reason(
                policy,
                available_pool_nodes,
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
        and not _has_resources(resource_deficit)
        and placement_nodes == 0
    ):
        excess_nodes = total_nodes - policy.min_nodes
        stop_budget = max(0, policy.max_stop_per_cycle - planned_stops(actions))
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
                placement_nodes=capacity_nodes,
                placement_requests=placement_requests,
                oldest_pending_seconds=oldest_pending_seconds,
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
        # A busy worker is surplus by memory claims long before it is surplus
        # for create throughput. Consolidating it in a rollout step's tail left
        # the next step one worker short until a new one provisioned, so new
        # creates hold selection off for the same idle time that stops wait.
        recent_creates = bool(
            live_signals is not None and live_signals.latest_schedule_age_seconds is not None
            and live_signals.latest_schedule_age_seconds < effective_policy.scale_down_idle_seconds
        )
        if not pressure_cooldown and not recent_creates and not soft_drain.job_id and not soft_drain_released:
            selected = plan_soft_drain(
                nodes,
                policy,
                now,
                required_resources=desired_resources,
                placement_requests=placement_requests,
                oldest_pending_seconds=oldest_pending_seconds,
                pending_count=demand.pending_count,
                excluded_job_ids=_planned_stop_job_ids(actions),
            )
            soft_drain = replace(
                selected,
                clear_job_ids=tuple(
                    dict.fromkeys((*soft_drain.clear_job_ids, *selected.clear_job_ids))
                ),
            )
    if soft_drain.reason:
        reasons.append(f"soft-drain: {soft_drain.reason}")

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
        pressure_scale_up=pressure_scale_up,
        create_pressure_scale_up=create_pressure_scale_up,
        effective_scale_down_idle_seconds=effective_scale_down_idle_seconds,
        soft_drain_job_id=soft_drain.job_id,
        soft_drain_selected=soft_drain.selected,
        soft_drain_clear_job_ids=soft_drain.clear_job_ids,
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


def _startup_backlog_requires_capacity(
    policy: ScalePolicy,
    demand: SandboxDemand,
    ready_nodes: list[SandboxNode],
    signals: LiveScaleSignals | None,
) -> bool:
    """A sustained capacity queue can justify bounded startup headroom.

    Count durable capacity demands, not HTTP retries or warm reservations.
    Another VM cannot help a short burst or a pool with an unused ready worker.
    The existing headroom and provisioning caps still bound the purchase.
    """

    return bool(
        policy.create_pressure_enabled
        and demand.pending_count >= max(1, policy.create_target_concurrency_per_node)
        and demand.oldest_capacity_pending_seconds
        >= max(1, policy.create_pressure_window_seconds)
        and ready_nodes
        and not any(node.is_idle for node in ready_nodes)
        and signals is not None
        and signals.latest_observation_age_seconds is not None
        and signals.latest_observation_age_seconds <= policy.live_pressure_fresh_seconds
    )


def _latest_capacity_pressure_age(
    policy: ScalePolicy,
    signals: LiveScaleSignals | None,
) -> int | None:
    if signals is None:
        return None
    ages: list[int] = []
    if policy.live_pressure_enabled and signals.latest_pressure_age_seconds is not None:
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
        signals.io_psi_full_avg10 is not None
        and signals.io_psi_full_avg10 >= policy.max_io_psi_full_avg10
    ):
        values.append(f"io-psi={signals.io_psi_full_avg10:g}")
    if (
        signals.storage_queue_utilization is not None
        and signals.storage_queue_utilization >= policy.target_storage_queue_utilization
    ):
        values.append(f"storage-queue={signals.storage_queue_utilization:.0%}")
    if (
        signals.image_materialization_queue_utilization is not None
        and signals.image_materialization_queue_utilization
        >= policy.target_storage_queue_utilization
    ):
        values.append(
            "image-materialization="
            f"{signals.image_materialization_queue_utilization:.0%}"
        )
    suffix = f" ({', '.join(values)})" if values else ""
    return (
        f"sustained live pressure across {signals.pressure_samples} sample(s){suffix}"
    )


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


def _planned_creates(actions: list[ScaleAction]) -> int:
    return sum(action.count for action in actions if action.kind == "create")


def _planned_stop_job_ids(actions: list[ScaleAction]) -> tuple[str, ...]:
    return tuple(
        job_id for action in actions if action.kind == "stop" for job_id in action.job_ids
    )


def planned_stops(actions: list[ScaleAction]) -> int:
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


def _physical_memory(node: SandboxNode) -> int:
    heartbeat = node.heartbeat
    if heartbeat is None:
        return 0
    metrics = heartbeat.runtime_metrics
    actual = metrics.memory_total_mb if metrics is not None else 0
    return min(heartbeat.total_resources.memory_mb, actual) if actual > 0 else heartbeat.total_resources.memory_mb


def _forecast_node_memory(nodes: list[SandboxNode], policy: ScalePolicy) -> int:
    observed = [_physical_memory(node) for node in nodes
        if node.is_schedulable and node.heartbeat is not None
        and node.heartbeat.total_resources.memory_mb == policy.default_node_resources.memory_mb
        and node.heartbeat.total_resources.vcpu == policy.default_node_resources.vcpu
        and node.heartbeat.runtime_metrics is not None
        and node.heartbeat.runtime_metrics.memory_total_mb > 0]
    return min([policy.default_node_resources.memory_mb, *observed])


def _cold_memory_forecast(
    requests: tuple[SandboxPlacementRequest, ...], maximum: ResourceQuantity,
    policy: ScalePolicy,
) -> int:
    return sum(min(maximum.memory_mb, math.ceil(
        item.resources.memory_mb / policy.target_memory_utilization)) * item.count
        for item in requests if item.resources.fits_within(maximum))


def _resident_memory_forecast(node: SandboxNode, policy: ScalePolicy, now: datetime) -> int:
    heartbeat = node.heartbeat
    if heartbeat is None:
        return 0
    active_entries = [item for item in heartbeat.inventory
                      if item.state in {"running", "creating", "waking"}]
    transition_bounds = sum(item.resources.memory_mb for item in active_entries
                            if item.state in {"creating", "waking"})
    promises = (max(heartbeat.reserved_resources.memory_mb, transition_bounds)
                + heartbeat.build_reserved_resources.memory_mb)
    metrics = heartbeat.runtime_metrics
    measured = 0
    if not node.is_idle or heartbeat.inventory:
        if (metrics is not None
                and (metrics.memory_total_mb > 0 or metrics.memory_working_set_mb > 0)
                and 0 <= (now - metrics.collected_at).total_seconds() <= policy.live_pressure_fresh_seconds):
            measured = max(metrics.memory_working_set_mb,
                           max(0, metrics.memory_total_mb - metrics.memory_available_mb))
        elif (not heartbeat.inventory_complete
              or node.active_sandboxes > len(active_entries)):
            # Unknown resident demand consumes existing physical credit, but
            # does not buy another worker by itself. Incomplete inventory must
            # not turn an occupied worker into apparently free RAM.
            return max(_physical_memory(node),
                       math.ceil(promises / policy.target_memory_utilization))
        else:
            # Missing telemetry is not evidence of a cheap owner. Known active
            # inventory gives a conservative fallback; parked bounds never do.
            measured = sum(item.resources.memory_mb for item in active_entries
                           if item.state == "running")
    # A promise can have partially materialized between heartbeats. Counting its
    # full bound temporarily errs toward earlier provisioning, not admission;
    # completion clears it. No long-lived parked or running limit is reserved.
    return math.ceil((measured + promises) / policy.target_memory_utilization)


def _forecast_free_resources(node: SandboxNode) -> ResourceQuantity:
    heartbeat = node.heartbeat
    assert heartbeat is not None
    free = _security_adjusted_resources(node, heartbeat.free_resources)
    physical = _security_adjusted_resources(node, heartbeat.total_resources)
    return replace(free, memory_mb=min(physical.memory_mb, _physical_memory(node)))


def _projected_free_resources(
    nodes: list[SandboxNode],
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
) -> ResourceQuantity:
    total = ResourceQuantity()
    forecast_memory = _forecast_node_memory(nodes, policy)
    for node in nodes:
        if node.job.is_final:
            continue
        if node.heartbeat is not None:
            if node.is_schedulable:
                available = _security_adjusted_resources(
                    node,
                    node.heartbeat.free_resources,
                )
                physical = _security_adjusted_resources(
                    node,
                    node.heartbeat.total_resources,
                )
                total = total + replace(
                    reusable_dynamic_resources(available, physical),
                    memory_mb=min(physical.memory_mb, _physical_memory(node)),
                )
            elif node.is_provisioning:
                total = total + _projected_provisioning_resources(
                    node,
                    policy,
                    now,
                    oldest_pending_seconds,
                    memory_capacity_mb=forecast_memory,
                )
            continue
        if node.is_provisioning:
            total = total + _projected_provisioning_resources(
                node,
                policy,
                now,
                oldest_pending_seconds,
                memory_capacity_mb=forecast_memory,
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
    """Bin-pack shapes, batching repeated demand and stopping beyond fleet size."""

    if not requests:
        return 0
    bins: list[tuple[str, ResourceQuantity, ResourceQuantity]] = []
    for node in nodes:
        if node.job.is_final:
            continue
        if node.heartbeat is not None and node.is_schedulable:
            available = _security_adjusted_resources(
                node,
                node.heartbeat.free_resources,
            )
            total = _security_adjusted_resources(
                node,
                node.heartbeat.total_resources,
            )
            bins.append(
                (
                    node.job_id,
                    reusable_dynamic_resources(available, total),
                    total,
                )
            )
        elif node.is_provisioning:
            available = _projected_provisioning_resources(
                node,
                policy,
                now,
                oldest_pending_seconds,
            )
            if not _has_resources(available):
                continue
            bins.append(
                (
                    node.job_id,
                    available,
                    policy.default_node_resources,
                )
            )
    default_bin = policy.default_node_resources

    def pressure(
        placement: SandboxPlacementRequest,
    ) -> tuple[float, int, float]:
        request = placement.resources
        ratios = (
            request.vcpu / default_bin.vcpu if default_bin.vcpu > 0 else 0.0,
            request.memory_mb / default_bin.memory_mb
            if default_bin.memory_mb > 0
            else 0.0,
            request.disk_mb / default_bin.disk_mb if default_bin.disk_mb > 0 else 0.0,
        )
        return max(ratios), request.memory_mb + request.disk_mb, request.vcpu

    missing = 0
    for placement in sorted(requests, key=pressure, reverse=True):
        requested = placement.resources
        excluded = set(placement.excluded_job_ids)
        remaining = placement.count
        while remaining > 0:
            fitting: list[tuple[int, str, ResourceQuantity, ResourceQuantity]] = []
            for index, (job_id, available, total) in enumerate(bins):
                if job_id in excluded:
                    continue
                if dynamic_request_fits(requested, available, total):
                    fitting.append((index, job_id, available, total))
            if fitting:
                index, job_id, available, total = min(
                    fitting,
                    key=lambda item: (
                        item[2].disk_mb - requested.disk_mb,
                        item[2].memory_mb - requested.memory_mb,
                        item[2].vcpu - requested.vcpu,
                    ),
                )
                # CPU/RAM are reusable; only disk is reserved per placement.
                # Consume identical demand together instead of expanding a
                # reservation into one planner iteration per future sandbox.
                batch = (
                    min(remaining, available.disk_mb // requested.disk_mb)
                    if requested.disk_mb > 0
                    else remaining
                )
                bins[index] = (
                    job_id,
                    reserve_dynamic_resources(
                        available, replace(requested, disk_mb=requested.disk_mb * batch)
                    ),
                    total,
                )
                remaining -= batch
                continue
            missing += 1
            # Additional hypothetical nodes cannot change this cycle's create
            # budget, or the answer to whether existing capacity suffices.
            if missing > policy.max_nodes:
                return missing
            bins.append(
                (
                    "",
                    reserve_dynamic_resources(default_bin, requested)
                    if dynamic_request_fits(requested, default_bin, default_bin)
                    else ResourceQuantity(),
                    default_bin,
                )
            )
            remaining -= 1
    return missing


def _placement_request_resources(
    requests: tuple[SandboxPlacementRequest, ...],
) -> ResourceQuantity:
    """Aggregate exact schedulable shapes without weakening hard disk ownership."""

    return ResourceQuantity(
        vcpu=max(
            (item.resources.vcpu for item in requests),
            default=0.0,
        ),
        memory_mb=max(
            (item.resources.memory_mb for item in requests),
            default=0,
        ),
        disk_mb=sum(item.resources.disk_mb * item.count for item in requests),
    )


def _projected_provisioning_resources(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    oldest_pending_seconds: int,
    *, memory_capacity_mb: int | None = None,
) -> ResourceQuantity:
    weight = _provisioning_weight(node, policy, now, oldest_pending_seconds)
    if node.heartbeat is not None and node.heartbeat.resources_known:
        available = _security_adjusted_resources(node, node.heartbeat.free_resources)
        if memory_capacity_mb is not None:
            available = replace(available, memory_mb=min(
                _security_adjusted_resources(node, node.heartbeat.total_resources).memory_mb,
                _physical_memory(node), memory_capacity_mb))
    else:
        available = _security_adjusted_resources(node, _estimated_node_resources(node, policy))
        if memory_capacity_mb is not None:
            available = replace(available, memory_mb=min(available.memory_mb, memory_capacity_mb))
    return _scale_resources(available, weight)


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
    maximum = policy.default_node_resources
    return ResourceQuantity(
        vcpu=min(physical.vcpu, maximum.vcpu),
        memory_mb=min(physical.memory_mb, maximum.memory_mb),
        disk_mb=min(physical.disk_mb, maximum.disk_mb),
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
    if node.job.is_final or node.job.is_lost or node.permanently_lost:
        return False
    # Capacity weighting and hard provider limits are separate concerns. A stale
    # provisioning job may contribute no projected resources, but it is still a
    # live instance and must count against max_nodes until the provider reports
    # it final.
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
    if not node.job.is_running:
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
        (node.job.is_running or node.job.is_unavailable)
        and (not node.heartbeat_fresh or node.job.is_unavailable)
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
        if node.job.is_running
        else node.job.created_at
    )
    if reference is None:
        return None
    return max(0.0, (now - reference).total_seconds())


def _clamp_ratio(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _scale_resources(value: ResourceQuantity, weight: float) -> ResourceQuantity:
    weight = _clamp_ratio(weight)
    return value.scaled(cpu=weight, memory=weight, disk=weight)


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
    placement_nodes: list[SandboxNode],
    placement_requests: tuple[SandboxPlacementRequest, ...],
    oldest_pending_seconds: int,
) -> list[SandboxNode]:
    if max_count <= 0:
        return []
    candidates: list[SandboxNode] = []
    remaining_free_resources = _reported_free_resources(ready_nodes)
    remaining_placement_nodes = list(placement_nodes)
    for node in ready_nodes:
        if len(candidates) >= max_count:
            break
        if not node.is_idle:
            continue
        idle_seconds = policy.scale_down_idle_seconds
        if is_soft_drained(node.heartbeat):
            # Selection already proved surplus; an emptied worker only waits
            # out brief churn instead of the full idle grace.
            idle_seconds = min(idle_seconds, _SOFT_DRAIN_IDLE_SECONDS)
        if not past_idle_grace(node, idle_seconds=idle_seconds, now=now):
            continue
        removal = _removal_fits(
            node,
            policy,
            now,
            remaining_free_resources=remaining_free_resources,
            remaining_placement_nodes=remaining_placement_nodes,
            required_resources=required_resources,
            placement_requests=placement_requests,
            oldest_pending_seconds=oldest_pending_seconds,
        )
        if removal is None:
            continue
        candidates.append(node)
        remaining_free_resources, remaining_placement_nodes = removal
    return candidates


_SOFT_DRAIN_IDLE_SECONDS = 60


def _reported_free_resources(nodes: list[SandboxNode]) -> ResourceQuantity:
    # Scale-down may only rely on capacity explicitly reported by surviving
    # nodes. Estimates are useful for scale-up projections, but are not safe
    # evidence for a destructive removal decision.
    total = ResourceQuantity()
    for node in nodes:
        if node.heartbeat is not None and node.heartbeat.resources_known:
            total = total + _forecast_free_resources(node)
    return total


def _removal_fits(
    node: SandboxNode,
    policy: ScalePolicy,
    now: datetime,
    *,
    remaining_free_resources: ResourceQuantity,
    remaining_placement_nodes: list[SandboxNode],
    required_resources: ResourceQuantity,
    placement_requests: tuple[SandboxPlacementRequest, ...],
    oldest_pending_seconds: int,
) -> tuple[ResourceQuantity, list[SandboxNode]] | None:
    """Return the fleet left after removing ``node`` if demand still fits."""

    node_free_resources = (
        _forecast_free_resources(node)
        if node.heartbeat is not None and node.heartbeat.resources_known
        else ResourceQuantity()
    )
    after_resources = _subtract_resources(remaining_free_resources, node_free_resources)
    if not required_resources.fits_within(after_resources):
        return None
    after_nodes = [
        current for current in remaining_placement_nodes if current.job_id != node.job_id
    ]
    exact_after_nodes = [
        current
        for current in after_nodes
        if current.is_schedulable
        and current.heartbeat is not None
        and current.heartbeat.resources_known
    ]
    if _nodes_for_unplaced_requests(
        exact_after_nodes,
        placement_requests,
        policy,
        now=now,
        oldest_pending_seconds=oldest_pending_seconds,
    ):
        return None
    return after_resources, after_nodes


@dataclass(frozen=True)
class SoftDrainPlan:
    job_id: str = ""
    selected: bool = False
    clear_job_ids: tuple[str, ...] = ()
    reason: str = ""
    retiring: bool = False  # Held for repeated reboots, never for demand.


def _soft_drain_sandboxes(node: SandboxNode) -> int:
    heartbeat = node.heartbeat
    # Parked routes do not count as active compute, but each one is a move.
    return max(node.active_sandboxes, len(heartbeat.inventory) if heartbeat else 0)


def plan_soft_drain(
    nodes: list[SandboxNode],
    policy: ScalePolicy,
    now: datetime,
    *,
    required_resources: ResourceQuantity,
    placement_requests: tuple[SandboxPlacementRequest, ...] = (),
    oldest_pending_seconds: int = 0,
    pending_count: int = 0,
    allow_select: bool = True,
    excluded_job_ids: tuple[str, ...] = (),
) -> SoftDrainPlan:
    """Keep, clear or choose the one surplus worker to empty via parked moves.

    A worker is surplus when the stop math (reported free resources and exact
    placement shapes) still fits current demand without it. With
    ``allow_select`` false an existing selection is only re-validated.
    """

    pool = [node for node in nodes if _counts_as_pool_node(node, policy, now, 0)]
    labelled = [node for node in pool if is_soft_drained(node.heartbeat)]
    if not policy.drain_on_park_enabled:
        return SoftDrainPlan(
            clear_job_ids=tuple(node.job_id for node in labelled),
            reason="drain on park is disabled" if labelled else "",
        )
    # A failing host empties whatever demand says, and takes the one slot.
    if retiring := [node for node in pool
                    if retiring_reason(node.heartbeat, now, max_cpu_check_ms=policy.max_cpu_check_ms)]:
        chosen = min(retiring, key=lambda node: (not is_soft_drained(node.heartbeat), node.job_id))
        return SoftDrainPlan(
            job_id=chosen.job_id, selected=not is_soft_drained(chosen.heartbeat), retiring=True,
            clear_job_ids=tuple(n.job_id for n in labelled if n.job_id != chosen.job_id),
            reason=f"{chosen.job_id} "
            + retiring_reason(chosen.heartbeat, now, max_cpu_check_ms=policy.max_cpu_check_ms),
        )
    capacity_nodes = [
        node for node in pool if node.agent_version_compatible or node.is_provisioning
    ]
    ready_nodes = [node for node in capacity_nodes if node.is_schedulable]
    ready_job_ids = {node.job_id for node in ready_nodes}
    reported_free = _reported_free_resources(ready_nodes)
    excess = len(pool) > policy.min_nodes

    def surplus(node: SandboxNode) -> bool:
        heartbeat = node.heartbeat
        if heartbeat is None or not heartbeat.resources_known:
            return False
        # Its parks move onto the survivors and keep holding their disk.
        held = _subtract_resources(
            _security_adjusted_resources(node, heartbeat.total_resources),
            _security_adjusted_resources(node, heartbeat.free_resources),
        )
        return _removal_fits(
            node,
            policy,
            now,
            remaining_free_resources=reported_free,
            remaining_placement_nodes=capacity_nodes,
            required_resources=replace(
                required_resources,
                disk_mb=max(0, required_resources.disk_mb) + held.disk_mb,
            ),
            placement_requests=placement_requests,
            oldest_pending_seconds=oldest_pending_seconds,
        ) is not None

    def serves_pending(node: SandboxNode) -> bool:
        if pending_count <= 0:
            return False
        if not placement_requests or node.heartbeat is None:
            return True
        total = _security_adjusted_resources(node, node.heartbeat.total_resources)
        available = reusable_dynamic_resources(
            _security_adjusted_resources(node, node.heartbeat.free_resources), total,
        )
        return any(
            node.job_id not in request.excluded_job_ids
            and dynamic_request_fits(request.resources, available, total)
            for request in placement_requests
        )

    def cpu_identity(node: SandboxNode) -> str | None:
        advertised = [
            value for value in (node.heartbeat.capabilities if node.heartbeat else ())
            if value.startswith(RUNTIME_CPU_CAPABILITY_PREFIX)
        ]
        return advertised[0] if len(advertised) == 1 else None

    def has_move_peer(node: SandboxNode) -> bool:
        # Checkpoints import only onto the same CPU feature set; draining a
        # worker whose parks fit nowhere would hold the slot without progress.
        identity = cpu_identity(node)
        return identity is None or any(
            other.job_id != node.job_id and cpu_identity(other) in (identity, None)
            for other in ready_nodes
        )

    kept = ""
    clear: list[str] = []
    reasons: list[str] = []
    for node in sorted(
        labelled, key=lambda item: (item.heartbeat.labels[SOFT_DRAIN_LABEL], item.job_id),
    ):
        if kept:
            clear.append(node.job_id)
            reasons.append(f"{node.job_id} exceeds one soft-drained worker")
        elif node.job_id not in ready_job_ids:
            # Stopping, unreachable or closed: it takes no creates, and still
            # occupies the single drain slot until it returns or is gone.
            kept = node.job_id
        elif not excess or not surplus(node):
            clear.append(node.job_id)
            reasons.append(f"{node.job_id} is no longer surplus")
        elif serves_pending(node):
            clear.append(node.job_id)
            reasons.append(f"{node.job_id} can serve pending demand")
        elif not has_move_peer(node):
            clear.append(node.job_id)
            reasons.append(f"no other worker shares {node.job_id}'s CPU features")
        else:
            kept = node.job_id
    if kept or not allow_select or not excess or pending_count > 0:
        return SoftDrainPlan(
            job_id=kept, clear_job_ids=tuple(clear), reason="; ".join(reasons),
        )
    excluded = set(excluded_job_ids)
    candidates = [
        node
        for node in ready_nodes
        if node.job_id not in excluded
        and not node.is_idle
        and node.heartbeat is not None
        and node.heartbeat.resources_known
    ]
    # Idle workers already scale down normally; a lone busy worker has nowhere
    # to move its parks.
    eligible = [
        node for node in candidates if has_move_peer(node) and surplus(node)
    ] if len(candidates) > 1 else []
    if not eligible:
        return SoftDrainPlan(clear_job_ids=tuple(clear), reason="; ".join(reasons))
    fewest = min(_soft_drain_sandboxes(node) for node in eligible)
    chosen = max(
        (node for node in eligible if _soft_drain_sandboxes(node) == fewest),
        key=lambda node: consolidation_rank(node.heartbeat),
    )
    reasons.append(
        f"selected {chosen.job_id} ({fewest} sandbox(es)); demand fits without it"
    )
    return SoftDrainPlan(
        job_id=chosen.job_id,
        selected=True,
        clear_job_ids=tuple(clear),
        reason="; ".join(reasons),
    )


def incompatible_stop_candidates(
    nodes: list[SandboxNode],
    *,
    now: datetime,
    max_cpu_check_ms: int = 0,
) -> list[SandboxNode]:
    candidates: list[SandboxNode] = []
    for node in nodes:
        retiring = retiring_reason(node.heartbeat, now, max_cpu_check_ms=max_cpu_check_ms) is not None
        if node.job.is_final or (node.agent_version_compatible and not retiring):
            continue
        if node.job.is_provisioning and not node.agent_version_compatible:
            candidates.append(node)
        elif node.job.is_running and node.heartbeat_fresh and node.is_idle:
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

    A fresh node must always use the drain-token handshake. This path is
    only for a running VM that has exceeded its heartbeat lease, owns no known
    sandbox routes, and whose last complete inventory was empty.  A VM that
    never emitted a heartbeat cannot have admitted gateway-managed work.
    """

    if now is None:
        now = utc_now()
    timeout_seconds = max(0, policy.unreachable_stop_after_seconds)
    reference = unreachable_node_reference(node)
    if (
        timeout_seconds <= 0
        or not node.job.is_running
        or node.heartbeat_fresh
        or reference is None
        or (now - reference).total_seconds() < timeout_seconds
        or node.active_sandboxes != 0
    ):
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
        [node for node in nodes if unreachable_node_stop_ready(node, policy, now=now)],
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
    if _has_resources(required_resources):
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
    if node.heartbeat.resources_known:
        return _forecast_free_resources(node)
    return _security_adjusted_resources(node, _estimated_node_resources(node, policy))


def _security_adjusted_resources(
    node: SandboxNode,
    resources: ResourceQuantity,
) -> ResourceQuantity:
    if node.heartbeat is not None and not node_storage_pressure_allows(
        node.heartbeat, ResourceQuantity()
    ):
        return ResourceQuantity()
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


def past_idle_grace(
    node: SandboxNode,
    *,
    idle_seconds: int,
    now: datetime,
) -> bool:
    idle_seconds = max(0, idle_seconds)
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


def _has_resources(value: ResourceQuantity) -> bool:
    return value.vcpu > 0 or value.memory_mb > 0 or value.disk_mb > 0


def _nodes_for_resource_deficit(
    deficit: ResourceQuantity, policy: ScalePolicy, *, memory_capacity_mb: int | None = None,
) -> int:
    defaults = policy.default_node_resources
    counts = [1]
    if deficit.vcpu > 0 and defaults.vcpu > 0:
        counts.append(_ceil_div_float(deficit.vcpu, defaults.vcpu))
    memory_capacity = defaults.memory_mb if memory_capacity_mb is None else memory_capacity_mb
    if deficit.memory_mb > 0 and memory_capacity > 0:
        counts.append(_ceil_div(deficit.memory_mb, memory_capacity))
    if deficit.disk_mb > 0 and defaults.disk_mb > 0:
        counts.append(_ceil_div(deficit.disk_mb, defaults.disk_mb))
    return max(counts)


def _ceil_div_float(value: float, divisor: float) -> int:
    return int(math.ceil(value / divisor))


def _resource_label(value: ResourceQuantity) -> str:
    return f"{value.vcpu:g}vcpu/{value.memory_mb}MB/{value.disk_mb}MB"
