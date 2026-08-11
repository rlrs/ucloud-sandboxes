from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from .autoscaler_state import PROVIDER_OPERATION_LABEL
from .config import DeploymentConfig
from .deployment import (
    AGENT_VERSION_LABEL,
    CREATE_INDEX_LABEL,
    DEPLOYMENT_LABEL,
    BUILDER_LABEL,
    INIT_VERSION_LABEL,
    NODE_LABEL,
    RECONCILE_CYCLE_LABEL,
    RECONCILE_LABEL,
    DEFAULT_INIT_VERSION,
    package_version,
)
from .networking import stable_hostname
from .models import (
    ResourceQuantity,
    SandboxNode,
    ScaleAction,
    ScaleDecision,
    ScalePolicy,
    utc_now,
)
from .policy import incompatible_stop_candidates, unreachable_node_stop_ready
from .providers.base import InstanceCreateIntent


def with_provider_operation_label(
    intent: InstanceCreateIntent,
    operation_id: str,
    *,
    deployment_id: str | None = None,
) -> InstanceCreateIntent:
    operation_id = str(operation_id).strip()
    if not operation_id:
        raise ValueError("operation_id is required")
    labels = dict(intent.labels)
    existing = labels.get(PROVIDER_OPERATION_LABEL)
    if existing and existing != operation_id:
        raise ValueError("intent already has a different provider operation label")
    labels[PROVIDER_OPERATION_LABEL] = operation_id
    if deployment_id is not None:
        deployment_id = str(deployment_id).strip()
        if not deployment_id:
            raise ValueError("deployment_id is required when supplied")
        existing_deployment = labels.get(DEPLOYMENT_LABEL)
        if existing_deployment and existing_deployment != deployment_id:
            raise ValueError("intent already has a different deployment label")
        labels[DEPLOYMENT_LABEL] = deployment_id
    return intent.with_labels(labels)


def build_create_intents(
    config: DeploymentConfig,
    decision: ScaleDecision,
    *,
    role: Literal["sandbox", "builder"],
    seed_prefix: str | None = None,
) -> list[InstanceCreateIntent]:
    if decision.creates <= 0:
        return []

    seed_role, node_prefix, name_prefix, role_label = {
        "sandbox": ("", "sandbox-node", "ucloud-sandbox-node", NODE_LABEL),
        "builder": (
            "-builder",
            "sandbox-builder",
            "ucloud-sandbox-builder",
            BUILDER_LABEL,
        ),
    }[role]
    cycle_seed = stable_hostname(seed_prefix or uuid4().hex[:10], prefix="")
    intents: list[InstanceCreateIntent] = []
    for index in range(1, decision.creates + 1):
        seed = f"{cycle_seed}{seed_role}-{index}"
        hostname = stable_hostname(seed, prefix=node_prefix)
        intents.append(
            InstanceCreateIntent(
                seed=seed,
                role=role,
                name=stable_hostname(seed, prefix=name_prefix),
                node_id=hostname,
                node_url=f"http://{hostname}:8090",
                labels={
                    role_label: "true",
                    RECONCILE_LABEL: "true",
                    RECONCILE_CYCLE_LABEL: cycle_seed,
                    CREATE_INDEX_LABEL: str(index),
                    DEPLOYMENT_LABEL: config.deployment_id,
                    AGENT_VERSION_LABEL: package_version(),
                    INIT_VERSION_LABEL: DEFAULT_INIT_VERSION,
                },
            )
        )
    return intents


def evaluate_builder_scale(
    builder_nodes: list[SandboxNode],
    *,
    pending_builds: int,
    prepared_builders: int = 0,
    policy: ScalePolicy,
    max_builder_nodes: int = 1,
    now: datetime | None = None,
) -> ScaleDecision:
    if now is None:
        now = utc_now()
    stop_budget = max(0, policy.max_stop_per_cycle)
    unreachable_stop_candidates = [
        node
        for node in builder_nodes
        if unreachable_node_stop_ready(node, policy, now=now)
    ][:stop_budget]
    unreachable_job_ids = {node.job_id for node in unreachable_stop_candidates}
    incompatible_candidates = incompatible_stop_candidates(
        [node for node in builder_nodes if node.job_id not in unreachable_job_ids],
        now=now,
    )[: max(0, stop_budget - len(unreachable_stop_candidates))]
    pool_nodes = [
        node
        for node in builder_nodes
        if not node.job.is_final and not node.job.is_lost and not node.permanently_lost
    ]
    ready_nodes = [node for node in pool_nodes if node.is_schedulable]
    provisioning_nodes = [node for node in pool_nodes if node.is_provisioning]
    total_nodes = len(pool_nodes)
    max_builder_nodes = max(0, max_builder_nodes)
    pending_builds = max(0, pending_builds)
    prepared_builders = max(0, prepared_builders)
    desired_nodes = max(1 if pending_builds > 0 else 0, prepared_builders)
    desired_nodes = min(desired_nodes, max_builder_nodes)
    actions: list[ScaleAction] = []
    reasons: list[str] = []

    if unreachable_stop_candidates:
        job_ids = tuple(node.job_id for node in unreachable_stop_candidates)
        reason = "unreachable empty builder node(s) exceeded the eviction lease"
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
        reason = "idle builder node(s) have incompatible agent version"
        actions.append(
            ScaleAction(
                kind="stop",
                count=len(job_ids),
                job_ids=job_ids,
                reason=reason,
            )
        )
        reasons.append(reason)

    if desired_nodes > 0:
        if total_nodes < desired_nodes:
            create_count = min(
                desired_nodes - total_nodes,
                max(0, max_builder_nodes - total_nodes),
                max(0, policy.max_create_per_cycle),
            )
            if create_count > 0:
                reason = (
                    f"builder demand needs {desired_nodes} builder node(s) "
                    f"({pending_builds} pending build(s), "
                    f"{prepared_builders} prepared builder(s))"
                )
                actions.append(
                    ScaleAction(kind="create", count=create_count, reason=reason)
                )
                reasons.append(reason)
        elif (
            total_nodes >= max_builder_nodes
            and len(ready_nodes) + len(provisioning_nodes) < desired_nodes
        ):
            reasons.append(f"max_builder_nodes={max_builder_nodes} reached")
        else:
            reasons.append(
                f"builder capacity exists for demand ({pending_builds} pending build(s), "
                f"{prepared_builders} prepared builder(s))"
            )
    else:
        stop_budget = max(0, policy.max_stop_per_cycle - _planned_stops(actions))
        stop_candidates = [
            node
            for node in ready_nodes
            if node.is_idle
            and _past_idle_grace(
                node,
                idle_seconds=policy.builder_scale_down_idle_seconds,
                now=now,
            )
        ][:stop_budget]
        if stop_candidates:
            job_ids = tuple(node.job_id for node in stop_candidates)
            reason = "idle builder node(s) exceed pending image build demand"
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
        reasons.append("builder pool matches demand and policy")

    return ScaleDecision(
        actions=tuple(actions),
        ready_nodes=len(ready_nodes),
        provisioning_nodes=len(provisioning_nodes),
        total_nodes=total_nodes,
        pending_resources=ResourceQuantity(),
        desired_resources=ResourceQuantity(),
        projected_free_resources=ResourceQuantity(),
        resource_deficit=ResourceQuantity(),
        reasons=tuple(reasons),
    )


def node_drain_ready(node: SandboxNode, token: str) -> bool:
    """Return whether a heartbeat proves that one drain incarnation is empty.

    This is intentionally stricter than :attr:`SandboxNode.is_idle`.  A stop
    decision is only safe after node admission is durably closed and one fresh,
    complete inventory snapshot acknowledges the same activity revision.
    """

    token = str(token).strip()
    heartbeat = node.heartbeat
    if not token or heartbeat is None or not node.heartbeat_fresh:
        return False
    if (
        not heartbeat.draining
        or heartbeat.admission_open
        or heartbeat.drain_token != token
        or not heartbeat.inventory_complete
        or heartbeat.drain_activity_epoch != heartbeat.activity_epoch
    ):
        return False
    if heartbeat.inventory or heartbeat.active_workloads != 0:
        return False
    return all(
        quantity == ResourceQuantity()
        for quantity in (
            heartbeat.used_resources,
            heartbeat.reserved_resources,
            heartbeat.build_reserved_resources,
        )
    )


def _planned_stops(actions: list[ScaleAction]) -> int:
    return sum(action.count for action in actions if action.kind == "stop")


def partition_safe_stop_job_ids(
    nodes: list[Any],
    requested_job_ids: tuple[str, ...],
    *,
    deployment_id: str,
    ownership_label: str = NODE_LABEL,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if not deployment_id:
        return (), requested_job_ids

    owned_job_ids = {
        node.job.id
        for node in nodes
        if node.job.labels.get(DEPLOYMENT_LABEL) == deployment_id
        and node.job.labels.get(ownership_label) == "true"
    }
    safe: list[str] = []
    blocked: list[str] = []
    for job_id in requested_job_ids:
        if job_id in owned_job_ids:
            safe.append(job_id)
        else:
            blocked.append(job_id)
    return tuple(safe), tuple(blocked)


def _past_idle_grace(
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
