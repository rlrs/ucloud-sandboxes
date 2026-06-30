from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from .config import AutoscalerConfig
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
from .models import ResourceQuantity, SandboxNode, ScaleAction, ScaleDecision, ScalePolicy, utc_now
from .vm_submit import (
    DEFAULT_VM_DISK_GB,
    VmApplicationRef,
    VmProductRef,
    VmSubmissionOptions,
    VmTimeAllocation,
    bulk_submission_payload,
)


@dataclass(frozen=True)
class VmNodeSubmissionDefaults:
    private_network_id: str | None
    product: VmProductRef = VmProductRef()
    application: VmApplicationRef = VmApplicationRef()
    disk_gb: int = DEFAULT_VM_DISK_GB
    time_allocation: VmTimeAllocation = VmTimeAllocation()
    ssh_enabled: bool = False
    allow_duplicate_job: bool = False
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class VmCreateIntent:
    seed: str
    node_id: str
    node_url: str
    options: VmSubmissionOptions

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "nodeId": self.node_id,
            "nodeUrl": self.node_url,
            "name": self.options.name,
            "hostname": self.options.hostname,
            "privateNetworkId": self.options.private_network_id,
            "publicLinkId": self.options.public_link_id,
            "publicLinkPort": (
                self.options.public_link_port if self.options.public_link_id else None
            ),
            "payloadItem": self.options.job_item(),
        }


def build_vm_create_intents(
    config: AutoscalerConfig,
    decision: ScaleDecision,
    defaults: VmNodeSubmissionDefaults,
    *,
    seed_prefix: str | None = None,
) -> list[VmCreateIntent]:
    count = create_count_from_decision(decision)
    if count <= 0:
        return []

    cycle_seed = stable_hostname(seed_prefix or uuid4().hex[:10], prefix="")
    intents: list[VmCreateIntent] = []
    for index in range(1, count + 1):
        seed = f"{cycle_seed}-{index}"
        hostname = stable_hostname(seed, prefix=config.node_hostname_prefix)
        name = stable_hostname(seed, prefix=config.job_name_prefix.rstrip("-"))
        labels = dict(defaults.labels)
        labels.setdefault(NODE_LABEL, "true")
        labels.setdefault(RECONCILE_LABEL, "true")
        labels[RECONCILE_CYCLE_LABEL] = cycle_seed
        labels[CREATE_INDEX_LABEL] = str(index)
        if config.deployment_id:
            labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
        labels.setdefault(AGENT_VERSION_LABEL, package_version())
        labels.setdefault(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION)
        options = VmSubmissionOptions(
            name=name,
            hostname=hostname,
            private_network_id=defaults.private_network_id,
            product=defaults.product,
            application=defaults.application,
            disk_gb=defaults.disk_gb,
            time_allocation=defaults.time_allocation,
            ssh_enabled=defaults.ssh_enabled,
            allow_duplicate_job=defaults.allow_duplicate_job,
            labels=labels,
        )
        intents.append(
            VmCreateIntent(
                seed=seed,
                node_id=hostname,
                node_url=f"http://{hostname}:8090",
                options=options,
            )
        )
    return intents


def build_builder_vm_create_intents(
    config: AutoscalerConfig,
    decision: ScaleDecision,
    defaults: VmNodeSubmissionDefaults,
    *,
    seed_prefix: str | None = None,
) -> list[VmCreateIntent]:
    count = create_count_from_decision(decision)
    if count <= 0:
        return []

    cycle_seed = stable_hostname(seed_prefix or uuid4().hex[:10], prefix="")
    intents: list[VmCreateIntent] = []
    for index in range(1, count + 1):
        seed = f"{cycle_seed}-builder-{index}"
        hostname = stable_hostname(seed, prefix="sandbox-builder")
        name = stable_hostname(seed, prefix="ucloud-sandbox-builder")
        labels = dict(defaults.labels)
        labels.pop(NODE_LABEL, None)
        labels.setdefault(BUILDER_LABEL, "true")
        labels.setdefault(RECONCILE_LABEL, "true")
        labels[RECONCILE_CYCLE_LABEL] = cycle_seed
        labels[CREATE_INDEX_LABEL] = str(index)
        if config.deployment_id:
            labels.setdefault(DEPLOYMENT_LABEL, config.deployment_id)
        labels.setdefault(AGENT_VERSION_LABEL, package_version())
        labels.setdefault(INIT_VERSION_LABEL, DEFAULT_INIT_VERSION)
        options = VmSubmissionOptions(
            name=name,
            hostname=hostname,
            private_network_id=defaults.private_network_id,
            product=defaults.product,
            application=defaults.application,
            disk_gb=defaults.disk_gb,
            time_allocation=defaults.time_allocation,
            ssh_enabled=defaults.ssh_enabled,
            allow_duplicate_job=defaults.allow_duplicate_job,
            labels=labels,
        )
        intents.append(
            VmCreateIntent(
                seed=seed,
                node_id=hostname,
                node_url=f"http://{hostname}:8090",
                options=options,
            )
        )
    return intents


def evaluate_builder_scale(
    builder_nodes: list[SandboxNode],
    *,
    pending_builds: int,
    policy: ScalePolicy,
    max_builder_nodes: int = 1,
    now: datetime | None = None,
) -> ScaleDecision:
    if now is None:
        now = utc_now()
    relevant_nodes = [node for node in builder_nodes if not node.job.is_final]
    ready_nodes = [node for node in relevant_nodes if node.is_ready]
    provisioning_nodes = [node for node in relevant_nodes if node.is_provisioning]
    total_nodes = len(relevant_nodes)
    max_builder_nodes = max(0, max_builder_nodes)
    pending_builds = max(0, pending_builds)
    actions: list[ScaleAction] = []
    reasons: list[str] = []

    if pending_builds > 0:
        if not ready_nodes and not provisioning_nodes and total_nodes < max_builder_nodes:
            create_count = min(
                1,
                max(0, max_builder_nodes - total_nodes),
                max(0, policy.max_create_per_cycle),
            )
            if create_count > 0:
                reason = f"{pending_builds} pending image build(s) need builder capacity"
                actions.append(ScaleAction(kind="create", count=create_count, reason=reason))
                reasons.append(reason)
        elif total_nodes >= max_builder_nodes and not ready_nodes and not provisioning_nodes:
            reasons.append(f"max_builder_nodes={max_builder_nodes} reached")
        else:
            reasons.append("builder capacity exists for pending image builds")
    else:
        stop_candidates = [
            node
            for node in ready_nodes
            if node.is_idle
            and _past_idle_grace(
                node,
                idle_seconds=policy.builder_scale_down_idle_seconds,
                now=now,
            )
        ][: max(0, policy.max_stop_per_cycle)]
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


def create_count_from_decision(decision: ScaleDecision) -> int:
    return sum(action.count for action in decision.actions if action.kind == "create")


def stop_job_ids_from_decision(decision: ScaleDecision) -> tuple[str, ...]:
    job_ids: list[str] = []
    for action in decision.actions:
        if action.kind == "stop":
            job_ids.extend(action.job_ids)
    return tuple(job_ids)


def partition_safe_stop_job_ids(
    nodes: list[Any],
    requested_job_ids: tuple[str, ...],
    *,
    deployment_id: str,
    allow_unlabeled: bool = False,
    ownership_label: str = NODE_LABEL,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if allow_unlabeled:
        return requested_job_ids, ()
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


def bulk_payload_from_create_intents(
    intents: list[VmCreateIntent],
) -> dict[str, Any]:
    return bulk_submission_payload([intent.options for intent in intents])
