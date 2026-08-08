from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from ...models import ProviderInstance
from ...providers.base import (
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

    def __init__(
        self,
        project_id: str,
        client: UCloudClient | None = None,
        *,
        session_file: str | Path | None = None,
        client_factory: Callable[[SessionStore], UCloudClient] = UCloudClient,
        sandbox_profile: UCloudCreateProfile,
        builder_profile: UCloudCreateProfile,
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

    @classmethod
    def from_session_file(
        cls,
        project_id: str,
        session_file: str | Path,
        *,
        sandbox_profile: UCloudCreateProfile,
        builder_profile: UCloudCreateProfile,
    ) -> "UCloudProvider":
        return cls(
            project_id,
            session_file=session_file,
            sandbox_profile=sandbox_profile,
            builder_profile=builder_profile,
        )

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
            payloads = self.client.browse_all_jobs(
                self.scope_id,
                include_application=False,
            )
        except UCloudError as exc:
            raise ProviderError(str(exc)) from exc
        return [self.decode_instance(item) for item in payloads]

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
