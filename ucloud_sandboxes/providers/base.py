from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Literal, Protocol, Sequence

from ..models import ProviderInstance


ProviderMutationStatus = Literal["accepted", "rejected", "uncertain"]


class ProviderError(RuntimeError):
    """A provider inventory or lookup operation failed."""


@dataclass(frozen=True)
class ProviderConfiguration:
    """Strictly tagged provider-owned configuration.

    The core validates the provider kind and delegates the remaining exact
    schema to that provider. This avoids teaching autoscaler configuration
    about cloud-specific project, network, image, or credential fields.
    """

    kind: str
    scope_id: str = ""
    settings: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: object) -> "ProviderConfiguration":
        if not isinstance(raw, dict):
            raise ValueError("provider configuration must be an object")
        kind = raw.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("provider.kind is required")
        scope_id = raw.get("scope_id", "")
        if not isinstance(scope_id, str):
            raise ValueError("provider.scope_id must be a string")
        return cls(
            kind=kind.strip(),
            scope_id=scope_id.strip(),
            settings={
                key: value
                for key, value in raw.items()
                if key not in {"kind", "scope_id"}
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "scope_id": self.scope_id, **self.settings}

    def with_scope(self, scope_id: str | None) -> "ProviderConfiguration":
        if scope_id in (None, ""):
            return self
        return replace(self, scope_id=str(scope_id).strip())

    def with_setting(self, name: str, value: object | None) -> "ProviderConfiguration":
        if value in (None, ""):
            return self
        settings = dict(self.settings)
        settings[name] = value
        return replace(self, settings=settings)


@dataclass(frozen=True)
class InstanceCreateIntent:
    """Provider-neutral request for one pool node.

    The autoscaler owns identity, role, and operation metadata. A provider
    adapter maps this semantic intent to its native machine, image, network,
    and API payload configuration.
    """

    seed: str
    role: Literal["sandbox", "builder"]
    name: str
    node_id: str
    node_url: str
    labels: dict[str, str] = field(default_factory=dict)

    def with_labels(self, labels: dict[str, str]) -> "InstanceCreateIntent":
        return replace(self, labels=dict(labels))

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "role": self.role,
            "name": self.name,
            "nodeId": self.node_id,
            "nodeUrl": self.node_url,
            "labels": dict(sorted(self.labels.items())),
        }


@dataclass(frozen=True)
class ProviderMutationResult:
    """Normalized result of one provider mutation.

    ``uncertain`` is deliberately distinct from rejection: after a transport
    failure the provider may already have applied the request, so the
    autoscaler must recover through inventory instead of retrying blindly.
    """

    status: ProviderMutationStatus
    instance_ids: tuple[str, ...] = ()
    response: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def __post_init__(self) -> None:
        if self.status not in {"accepted", "rejected", "uncertain"}:
            raise ValueError("unsupported provider mutation status")
        if self.status == "accepted" and not self.instance_ids:
            raise ValueError("accepted provider mutation must identify an instance")


@dataclass(frozen=True)
class InstanceBootstrapAccess:
    """Provider-supplied access details for initializing one instance."""

    instance: ProviderInstance
    command: str | None
    runnable: bool
    reason: str


class ComputeProvider(Protocol):
    """Small compute boundary required by the autoscaler.

    Runtime, routing, storage, and scaling policy intentionally do not belong
    here. Implementations translate provider APIs into this contract.
    """

    kind: str
    scope_id: str

    def list_instances(self) -> list[ProviderInstance]: ...

    def decode_instance(self, payload: dict[str, Any]) -> ProviderInstance: ...

    def retrieve_instance(
        self,
        instance_id: str,
        *,
        include_updates: bool = True,
    ) -> ProviderInstance: ...

    def bootstrap_access(
        self,
        instance: ProviderInstance,
    ) -> InstanceBootstrapAccess: ...

    def instance_is_eligible(self, instance: ProviderInstance) -> bool: ...

    def render_create_request(
        self,
        intents: Sequence[InstanceCreateIntent],
    ) -> dict[str, Any]: ...

    def create(self, request: dict[str, Any]) -> ProviderMutationResult: ...

    def terminate(
        self,
        instance_ids: tuple[str, ...],
    ) -> ProviderMutationResult: ...
