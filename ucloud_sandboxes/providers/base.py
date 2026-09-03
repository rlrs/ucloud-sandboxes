from __future__ import annotations

from dataclasses import dataclass, field, replace
from math import isfinite
from typing import Any, Literal, Mapping, Protocol, Sequence

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
    refresh_recommended: bool = False
    startup_probe_seconds: int = 0


@dataclass(frozen=True)
class DestructiveInstanceLoss:
    """Provider-owned proof that one loss observation cannot recover."""

    reason: str
    evidence_kind: str
    required_evidence_fields: tuple[str, ...]
    evidence: tuple[tuple[str, str | int | float | bool | None], ...] = ()

    def __post_init__(self) -> None:
        reason = self.reason.strip()
        evidence_kind = self.evidence_kind.strip()
        fields = tuple(field.strip() for field in self.required_evidence_fields)
        evidence: list[tuple[str, str | int | float | bool | None]] = []
        evidence_keys: set[str] = set()
        for item in self.evidence:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError("destructive instance loss evidence must be pairs")
            raw_key, value = item
            key = raw_key.strip() if isinstance(raw_key, str) else ""
            if (
                not key
                or key in evidence_keys
                or not isinstance(value, (str, int, float, bool, type(None)))
                or (isinstance(value, float) and not isfinite(value))
            ):
                raise ValueError(
                    "destructive instance loss evidence must be unique JSON scalars"
                )
            evidence_keys.add(key)
            evidence.append((key, value))
        if (
            not reason
            or not evidence_kind
            or not fields
            or any(not field for field in fields)
            or len(fields) != len(set(fields))
        ):
            raise ValueError("destructive instance loss tags cannot be empty")
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "evidence_kind", evidence_kind)
        object.__setattr__(self, "required_evidence_fields", fields)
        object.__setattr__(self, "evidence", tuple(evidence))

    def evidence_payload(self) -> dict[str, str | int | float | bool | None]:
        return dict(self.evidence)

    @property
    def has_complete_evidence(self) -> bool:
        """Return whether every contract field has a concrete observation."""

        evidence = self.evidence_payload()
        return all(
            field in evidence
            and evidence[field] is not None
            and (
                not isinstance(evidence[field], str)
                or bool(str(evidence[field]).strip())
            )
            for field in self.required_evidence_fields
        )

    def observe(
        self,
        evidence: Mapping[str, object],
    ) -> "DestructiveInstanceLoss | None":
        """Validate raw evidence against this provider-declared contract."""

        try:
            observed = replace(
                self,
                evidence=tuple(
                    (field, evidence.get(field))
                    for field in self.required_evidence_fields
                ),
            )
        except ValueError:
            return None
        return observed if self.matches(observed) else None

    def matches(self, observed: "DestructiveInstanceLoss") -> bool:
        """Match an observation without permitting schema or scalar coercion."""

        if (
            self.reason != observed.reason
            or self.evidence_kind != observed.evidence_kind
            or self.required_evidence_fields != observed.required_evidence_fields
            or not observed.has_complete_evidence
        ):
            return False
        actual = observed.evidence_payload()
        return all(
            field in actual
            and type(actual[field]) is type(expected)
            and actual[field] == expected
            for field, expected in self.evidence
        )

    def from_operation_request(
        self,
        request: Mapping[str, object],
    ) -> "DestructiveInstanceLoss | None":
        """Recover and validate this proof from its durable journal payload."""

        nested = request.get("lossEvidence")
        return self.observe(nested) if isinstance(nested, dict) else None

    def operation_request_fields(self, provider_kind: str) -> dict[str, Any]:
        """Serialize the canonical provider-neutral durable proof envelope."""

        if not self.has_complete_evidence:
            raise ValueError("destructive instance loss evidence is incomplete")
        return {
            "providerKind": provider_kind,
            "lossEvidenceKind": self.evidence_kind,
            "lossEvidence": self.evidence_payload(),
        }


class ComputeProvider(Protocol):
    """Small compute boundary required by the autoscaler.

    Runtime, routing, storage, and scaling policy intentionally do not belong
    here. Implementations translate provider APIs into this contract.
    """

    kind: str
    scope_id: str
    requires_continuity_history: bool
    destructive_instance_losses: tuple[DestructiveInstanceLoss, ...]
    unreachable_lease_expiry_loss: DestructiveInstanceLoss | None

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

    def destructive_instance_loss(
        self,
        instance: ProviderInstance,
    ) -> DestructiveInstanceLoss | None: ...

    def render_create_request(
        self,
        intents: Sequence[InstanceCreateIntent],
    ) -> dict[str, Any]: ...

    def create(self, request: dict[str, Any]) -> ProviderMutationResult: ...

    def terminate(
        self,
        instance_ids: tuple[str, ...],
    ) -> ProviderMutationResult: ...
