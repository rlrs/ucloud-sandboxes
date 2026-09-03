from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import time
from typing import Any, Callable, Sequence

from ...models import ProviderInstance
from ...providers.base import (
    DestructiveInstanceLoss,
    InstanceBootstrapAccess,
    InstanceCreateIntent,
    ProviderError,
    ProviderMutationResult,
)
from .api import SessionStore, UCloudClient, UCloudError, UCloudHttpError
from .bootstrap import bootstrap_access
from .payloads import (
    DEFAULT_VM_DISK_GB,
    VmApplicationRef,
    VmProductRef,
    VmSubmissionOptions,
    VmTimeAllocation,
    bulk_submission_payload,
)
from .models import instance_from_payload


@dataclass(frozen=True)
class UCloudCreateProfile:
    private_network_id: str | None
    require_private_network: bool = True
    product: VmProductRef = VmProductRef()
    application: VmApplicationRef = VmApplicationRef()
    disk_gb: int = DEFAULT_VM_DISK_GB
    time_allocation: VmTimeAllocation = VmTimeAllocation()
    ssh_enabled: bool = False
    allow_duplicate_job: bool = False


class UCloudProvider:
    """Translate the provider-neutral autoscaler contract to SDU UCloud."""

    kind = "ucloud"
    # UCloud may report a job RUNNING again after destroying and replacing its
    # guest. The ordered update history is therefore required to establish
    # continuity; the current state alone is not authoritative.
    requires_continuity_history = True
    # A UCloud guest that disappears after reaching RUNNING cannot be recovered.
    # Once its heartbeat lease expires, retaining the provider job cannot preserve
    # its sandbox inventory and must not block replacement capacity.
    unreachable_lease_expiry_loss = DestructiveInstanceLoss(
        reason="ucloud_unreachable_lease_expired",
        evidence_kind="unreachable_lease_expired",
        required_evidence_fields=(
            "unreachableLeaseExpired",
            "unreachableReference",
        ),
        evidence=(("unreachableLeaseExpired", True),),
    )
    _post_start_instance_loss = DestructiveInstanceLoss(
        reason="post_start_suspension",
        evidence_kind="post_start_suspension",
        required_evidence_fields=("postStartSuspensionObserved",),
        evidence=(("postStartSuspensionObserved", True),),
    )
    destructive_instance_losses = (_post_start_instance_loss,)
    _active_job_states = ("IN_QUEUE", "RUNNING", "SUSPENDED")

    def __init__(
        self,
        project_id: str,
        client: UCloudClient | None = None,
        *,
        session_file: str | Path | None = None,
        client_factory: Callable[[SessionStore], UCloudClient] = UCloudClient,
        sandbox_profile: UCloudCreateProfile,
        builder_profile: UCloudCreateProfile,
        deployment_id: str = "",
        full_inventory_refresh_seconds: float = 300.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not project_id.strip():
            raise ValueError("UCloud project id is required")
        self.scope_id = project_id.strip()
        self._client = client
        self._session_file = Path(session_file) if session_file is not None else None
        self._client_factory = client_factory
        self._profiles = {
            "sandbox": sandbox_profile,
            "builder": builder_profile,
        }
        self._deployment_id = deployment_id.strip()
        self._full_inventory_refresh_seconds = max(
            1.0, float(full_inventory_refresh_seconds)
        )
        self._monotonic = monotonic
        self._inventory_by_id: dict[str, ProviderInstance] = {}
        self._recent_terminal_by_id: dict[str, ProviderInstance] = {}
        self._last_full_inventory_at: float | None = None
        self._force_full_inventory = True
        self._awaiting_instance_ids: set[str] = set()
        self._discovery_retry_until = 0.0

    @property
    def client(self) -> UCloudClient:
        if self._client is None:
            if self._session_file is None:
                raise ValueError(
                    "UCloud session file is required for provider API calls"
                )
            self._client = self._client_factory(SessionStore(self._session_file))
        return self._client

    def list_instances(self) -> list[ProviderInstance]:
        try:
            now = self._monotonic()
            if self._full_inventory_due(now):
                payloads = self.client.browse_all_jobs(
                    self.scope_id,
                    include_application=False,
                )
                self._replace_from_full_inventory(payloads)
                self._last_full_inventory_at = now
                self._force_full_inventory = False
            elif self._inventory_by_id:
                payloads = []
                for state in self._active_job_states:
                    payloads.extend(
                        self.client.browse_all_jobs(
                            self.scope_id,
                            include_application=False,
                            filter_state=state,
                        )
                    )
                self._replace_active_inventory(payloads)
        except UCloudError as exc:
            raise ProviderError(str(exc)) from exc
        return [
            *self._inventory_by_id.values(),
            *self._recent_terminal_by_id.values(),
        ]

    def _full_inventory_due(self, now: float) -> bool:
        if self._awaiting_instance_ids and now <= self._discovery_retry_until:
            return True
        if now > self._discovery_retry_until:
            self._awaiting_instance_ids.clear()
        return bool(
            self._force_full_inventory
            or self._last_full_inventory_at is None
            or now - self._last_full_inventory_at
            >= self._full_inventory_refresh_seconds
        )

    def _replace_from_full_inventory(self, payloads: list[dict[str, Any]]) -> None:
        previous_active_ids = set(self._inventory_by_id)
        observed = self._decode_managed_inventory(payloads)
        self._awaiting_instance_ids.difference_update(item.id for item in observed)
        self._inventory_by_id = {
            item.id: item for item in observed if not item.is_final
        }
        self._recent_terminal_by_id = {
            item.id: item
            for item in observed
            if item.is_final and item.id in previous_active_ids
        }

    def _replace_active_inventory(self, payloads: list[dict[str, Any]]) -> None:
        previous = self._inventory_by_id
        observed = self._decode_managed_inventory(payloads)
        active = {item.id: item for item in observed if not item.is_final}
        vanished_ids = set(previous) - set(active)
        terminals = dict(self._recent_terminal_by_id)
        for instance_id in vanished_ids:
            try:
                terminal = self.retrieve_instance(instance_id, include_updates=False)
            except ProviderError:
                # Preserve the last non-terminal observation until a full census
                # can safely establish whether the job vanished or the narrow
                # provider query was transiently incomplete.
                active[instance_id] = previous[instance_id]
                continue
            if terminal.is_final:
                terminals[instance_id] = terminal
            else:
                active[instance_id] = terminal
        self._inventory_by_id = active
        self._recent_terminal_by_id = terminals

    def _decode_managed_inventory(
        self, payloads: list[dict[str, Any]]
    ) -> list[ProviderInstance]:
        return [
            self.decode_instance(item)
            for item in payloads
            if self._payload_is_managed(item)
        ]

    def _payload_is_managed(self, payload: dict[str, Any]) -> bool:
        if not self._deployment_id:
            return True
        specification = payload.get("specification")
        labels = specification.get("labels") if isinstance(specification, dict) else None
        return bool(
            isinstance(labels, dict)
            and labels.get("ucloud-sandboxes/deployment") == self._deployment_id
            and (
                labels.get("ucloud-sandboxes/node") == "true"
                or labels.get("ucloud-sandboxes/builder") == "true"
            )
        )

    def decode_instance(self, payload: dict[str, Any]) -> ProviderInstance:
        return instance_from_payload(payload)

    def retrieve_instance(
        self,
        instance_id: str,
        *,
        include_updates: bool = True,
    ) -> ProviderInstance:
        try:
            payload = self.client.retrieve_job(
                self.scope_id,
                instance_id,
                include_updates=include_updates,
            )
        except UCloudError as exc:
            raise ProviderError(str(exc)) from exc
        return self.decode_instance(payload)

    def bootstrap_access(
        self,
        instance: ProviderInstance,
    ) -> InstanceBootstrapAccess:
        return bootstrap_access(instance)

    def instance_is_eligible(self, instance: ProviderInstance) -> bool:
        private_network_ids = {
            profile.private_network_id
            for profile in self._profiles.values()
            if profile.private_network_id
        }
        return not private_network_ids or bool(
            private_network_ids.intersection(instance.private_network_ids)
        )

    def destructive_instance_loss(
        self,
        instance: ProviderInstance,
    ) -> DestructiveInstanceLoss | None:
        if not instance.is_lost:
            return None
        return replace(
            self._post_start_instance_loss,
            evidence=(("postStartSuspensionObserved", True),),
        )

    def render_create_request(
        self,
        intents: Sequence[InstanceCreateIntent],
    ) -> dict[str, Any]:
        options: list[VmSubmissionOptions] = []
        for intent in intents:
            profile = self._profiles[intent.role]
            if profile.require_private_network and not profile.private_network_id:
                raise ValueError(
                    "UCloud private network id is required for autoscaled nodes"
                )
            options.append(
                VmSubmissionOptions(
                    name=intent.name,
                    hostname=intent.node_id,
                    private_network_id=profile.private_network_id,
                    product=profile.product,
                    application=profile.application,
                    disk_gb=profile.disk_gb,
                    time_allocation=profile.time_allocation,
                    ssh_enabled=profile.ssh_enabled,
                    allow_duplicate_job=profile.allow_duplicate_job,
                    labels=intent.labels,
                )
            )
        return bulk_submission_payload(options)

    def create(self, request: dict[str, Any]) -> ProviderMutationResult:
        try:
            response = self.client.submit_jobs(self.scope_id, request)
        except UCloudHttpError as exc:
            return _http_error_result(exc)
        except Exception as exc:
            return ProviderMutationResult(status="uncertain", error=str(exc))
        instance_ids = _response_instance_ids(response)
        if _response_is_rejection(response):
            return ProviderMutationResult(
                status="rejected",
                response=response,
                error="UCloud explicitly rejected the create operation",
            )
        if len(instance_ids) == 1:
            self._force_full_inventory = True
            self._awaiting_instance_ids.update(instance_ids)
            self._discovery_retry_until = self._monotonic() + 60.0
            return ProviderMutationResult(
                status="accepted",
                instance_ids=instance_ids,
                response=response,
            )
        return ProviderMutationResult(
            status="uncertain",
            response=response,
            error="UCloud response did not prove whether the create operation applied",
        )

    def terminate(
        self,
        instance_ids: tuple[str, ...],
    ) -> ProviderMutationResult:
        try:
            response = self.client.terminate_jobs(self.scope_id, instance_ids)
        except UCloudHttpError as exc:
            return _http_error_result(exc)
        except Exception as exc:
            return ProviderMutationResult(status="uncertain", error=str(exc))
        response_ids = _response_instance_ids(response)
        if _response_is_rejection(response):
            return ProviderMutationResult(
                status="rejected",
                response=response,
                error="UCloud explicitly rejected the terminate operation",
            )
        if instance_ids and set(instance_ids).issubset(response_ids):
            return ProviderMutationResult(
                status="accepted",
                instance_ids=instance_ids,
                response=response,
            )
        return ProviderMutationResult(
            status="uncertain",
            response=response,
            error="UCloud response did not prove whether the terminate operation applied",
        )


def _http_error_result(exc: UCloudHttpError) -> ProviderMutationResult:
    response = {"status": exc.status, "payload": exc.payload}
    if 400 <= exc.status < 500 and exc.status not in {408, 425, 429}:
        return ProviderMutationResult(
            status="rejected",
            response=response,
            error=str(exc),
        )
    return ProviderMutationResult(
        status="uncertain",
        response=response,
        error=str(exc),
    )


def _response_instance_ids(response: dict[str, Any]) -> tuple[str, ...]:
    raw = response.get("responses")
    if not isinstance(raw, list):
        return ()
    values: list[str] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        instance_id = item.get("id")
        if instance_id not in (None, ""):
            values.append(str(instance_id))
    return tuple(values)


def _response_is_rejection(response: dict[str, Any]) -> bool:
    responses = response.get("responses")
    if not isinstance(responses, list) or not responses:
        return False
    return all(
        isinstance(item, dict)
        and not item.get("id")
        and any(item.get(key) not in (None, "") for key in ("error", "why", "message"))
        for item in responses
    )
