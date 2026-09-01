from __future__ import annotations

from dataclasses import dataclass, replace
import ipaddress
import re
import shlex
from typing import Any, Callable, Sequence

from ...models import ProviderInstance
from ..base import (
    DestructiveInstanceLoss,
    InstanceBootstrapAccess,
    InstanceCreateIntent,
    ProviderError,
    ProviderMutationResult,
)
from .api import HetznerClient, HetznerError, HetznerHttpError
from .config import HetznerImage
from .models import instance_from_payload


MANAGED_SERVER_LABEL_SELECTOR = "ucloud-sandboxes/reconcile"
_SERVER_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_LABEL_KEY_RE = re.compile(
    r"^([a-z0-9A-Z]((?:[-_.]|[a-z0-9A-Z]){0,253}[a-z0-9A-Z])?/)?"
    r"[a-z0-9A-Z]((?:[-_.]|[a-z0-9A-Z]){0,61}[a-z0-9A-Z])?$"
)
_LABEL_VALUE_RE = re.compile(
    r"^(([a-z0-9A-Z](?:[-_.]|[a-z0-9A-Z]){0,61})?[a-z0-9A-Z]$|$)"
)


@dataclass(frozen=True)
class HetznerCreateProfile:
    server_type: str
    image: HetznerImage
    location: str
    network_id: int
    ssh_key_ids: tuple[int, ...] = ()
    firewall_ids: tuple[int, ...] = ()
    placement_group_id: int | None = None
    enable_ipv4: bool = False
    enable_ipv6: bool = False
    enable_private_egress: bool = False
    private_dns_servers: tuple[str, ...] = ()


class HetznerProvider:
    """Translate autoscaler compute operations to Hetzner Cloud servers."""

    kind = "hetzner"
    requires_continuity_history = False
    destructive_instance_losses: tuple[DestructiveInstanceLoss, ...] = ()
    unreachable_lease_expiry_loss = None

    def __init__(
        self,
        project_name: str,
        client: HetznerClient | None = None,
        *,
        api_token_env: str,
        api_base_url: str,
        ssh_user: str,
        sandbox_profile: HetznerCreateProfile,
        builder_profile: HetznerCreateProfile,
        client_factory: Callable[..., HetznerClient] = HetznerClient,
    ) -> None:
        if not project_name.strip():
            raise ValueError("Hetzner project name is required")
        if sandbox_profile.network_id != builder_profile.network_id:
            raise ValueError("Hetzner role profiles must use one private network")
        self.scope_id = project_name.strip()
        self._client = client
        self._api_token_env = api_token_env
        self._api_base_url = api_base_url
        self._client_factory = client_factory
        self._ssh_user = ssh_user
        self._network_id = sandbox_profile.network_id
        self._profiles = {
            "sandbox": sandbox_profile,
            "builder": builder_profile,
        }

    @property
    def client(self) -> HetznerClient:
        if self._client is None:
            self._client = self._client_factory(
                api_token_env=self._api_token_env,
                base_url=self._api_base_url,
            )
        return self._client

    def list_instances(self) -> list[ProviderInstance]:
        try:
            payloads = self.client.list_servers(
                label_selector=MANAGED_SERVER_LABEL_SELECTOR
            )
        except HetznerError as exc:
            raise ProviderError(str(exc)) from exc
        return [self.decode_instance(item) for item in payloads]

    def decode_instance(self, payload: dict[str, Any]) -> ProviderInstance:
        instance = instance_from_payload(payload)
        private_ip = private_ip_for_network(payload, self._network_id)
        return replace(instance, hostname=private_ip or instance.hostname)

    def retrieve_instance(
        self,
        instance_id: str,
        *,
        include_updates: bool = True,
    ) -> ProviderInstance:
        del include_updates
        try:
            payload = self.client.retrieve_server(instance_id)
        except HetznerError as exc:
            raise ProviderError(str(exc)) from exc
        return self.decode_instance(payload)

    def bootstrap_access(
        self,
        instance: ProviderInstance,
    ) -> InstanceBootstrapAccess:
        address = private_ip_for_network(instance.raw, self._network_id)
        command = (
            f"ssh {shlex.quote(f'{self._ssh_user}@{address}')}" if address else None
        )
        if not instance.is_running:
            return InstanceBootstrapAccess(
                instance=instance,
                command=command,
                runnable=False,
                reason=(
                    "Instance is not running yet; current Hetzner state is "
                    f"{instance.state or 'unknown'}."
                ),
            )
        if not command:
            return InstanceBootstrapAccess(
                instance=instance,
                command=None,
                runnable=False,
                reason="Hetzner has not attached a private IP on the configured network.",
                refresh_recommended=True,
            )
        return InstanceBootstrapAccess(
            instance=instance,
            command=command,
            runnable=True,
            reason="Instance is running and its private address is available.",
        )

    def instance_is_eligible(self, instance: ProviderInstance) -> bool:
        return str(self._network_id) in instance.private_network_ids

    def destructive_instance_loss(
        self,
        instance: ProviderInstance,
    ) -> DestructiveInstanceLoss | None:
        # Hetzner `off`/`stopping` servers retain their local disk and can be
        # powered on again. They are unschedulable, but not proof of data loss.
        del instance
        return None

    def render_create_request(
        self,
        intents: Sequence[InstanceCreateIntent],
    ) -> dict[str, Any]:
        if not intents:
            raise ValueError("at least one Hetzner create intent is required")
        names: set[str] = set()
        servers: list[dict[str, Any]] = []
        for intent in intents:
            profile = self._profiles.get(intent.role)
            if profile is None:
                raise ValueError(f"unsupported Hetzner node role: {intent.role}")
            _validate_server_name(intent.name)
            if intent.name in names:
                raise ValueError(f"duplicate Hetzner server name: {intent.name}")
            names.add(intent.name)
            labels = _validated_labels(intent.labels)
            payload: dict[str, Any] = {
                "name": intent.name,
                "server_type": profile.server_type,
                "image": profile.image,
                "location": profile.location,
                "start_after_create": True,
                "labels": labels,
                "networks": [profile.network_id],
                "public_net": {
                    "enable_ipv4": profile.enable_ipv4,
                    "enable_ipv6": profile.enable_ipv6,
                },
            }
            if profile.ssh_key_ids:
                payload["ssh_keys"] = list(profile.ssh_key_ids)
            if profile.firewall_ids and (profile.enable_ipv4 or profile.enable_ipv6):
                payload["firewalls"] = [
                    {"firewall": firewall_id} for firewall_id in profile.firewall_ids
                ]
            if profile.placement_group_id is not None:
                payload["placement_group"] = profile.placement_group_id
            if profile.enable_private_egress:
                payload["user_data"] = _private_egress_cloud_config(
                    profile.private_dns_servers
                )
            servers.append(payload)
        return {"servers": servers}

    def create(self, request: dict[str, Any]) -> ProviderMutationResult:
        raw_servers = request.get("servers")
        if not isinstance(raw_servers, list) or not raw_servers:
            return ProviderMutationResult(
                status="rejected",
                error="Hetzner create request has no servers",
            )

        created_ids: list[str] = []
        responses: list[dict[str, Any]] = []
        for raw_server in raw_servers:
            if not isinstance(raw_server, dict):
                return _partial_or_rejected(
                    created_ids,
                    responses,
                    "Hetzner create request contains an invalid server",
                )
            try:
                response = self.client.create_server(raw_server)
            except HetznerHttpError as exc:
                return _mutation_error_result(
                    exc,
                    applied_ids=created_ids,
                    responses=responses,
                )
            except Exception as exc:
                return ProviderMutationResult(
                    status="uncertain",
                    instance_ids=tuple(created_ids),
                    response={"responses": responses},
                    error=str(exc),
                )
            summary = _create_response_summary(response)
            responses.append(summary)
            server = response.get("server")
            raw_server_id = server.get("id") if isinstance(server, dict) else None
            server_id = _server_id(raw_server_id)
            if server_id is None:
                return ProviderMutationResult(
                    status="uncertain",
                    instance_ids=tuple(created_ids),
                    response={"responses": responses},
                    error=(
                        "Hetzner response did not identify the created server; "
                        "root password was not retained"
                    ),
                )
            created_ids.append(server_id)
        return ProviderMutationResult(
            status="accepted",
            instance_ids=tuple(created_ids),
            response={"responses": responses},
        )

    def terminate(
        self,
        instance_ids: tuple[str, ...],
    ) -> ProviderMutationResult:
        if not instance_ids:
            return ProviderMutationResult(
                status="rejected",
                error="Hetzner terminate request has no server ids",
            )
        invalid_ids = [value for value in instance_ids if _server_id(value) is None]
        if invalid_ids:
            return ProviderMutationResult(
                status="rejected",
                error=f"Hetzner terminate request has invalid server ids: {invalid_ids!r}",
            )
        deleted_ids: list[str] = []
        responses: list[dict[str, Any]] = []
        for instance_id in instance_ids:
            try:
                response = self.client.delete_server(instance_id)
            except HetznerHttpError as exc:
                if exc.status == 404:
                    deleted_ids.append(instance_id)
                    responses.append(
                        {"server_id": instance_id, "status": 404, "not_found": True}
                    )
                    continue
                return _mutation_error_result(
                    exc,
                    applied_ids=deleted_ids,
                    responses=responses,
                )
            except Exception as exc:
                return ProviderMutationResult(
                    status="uncertain",
                    instance_ids=tuple(deleted_ids),
                    response={"responses": responses},
                    error=str(exc),
                )
            deleted_ids.append(instance_id)
            responses.append(
                {
                    "server_id": instance_id,
                    "action": _action_summary(response.get("action")),
                }
            )
        return ProviderMutationResult(
            status="accepted",
            instance_ids=tuple(deleted_ids),
            response={"responses": responses},
        )


def private_ip_for_network(payload: dict[str, Any], network_id: int) -> str | None:
    private_net = payload.get("private_net")
    if not isinstance(private_net, list):
        return None
    for item in private_net:
        if not isinstance(item, dict) or str(item.get("network")) != str(network_id):
            continue
        value = item.get("ip")
        if not isinstance(value, str):
            continue
        try:
            return str(ipaddress.ip_address(value))
        except ValueError:
            continue
    return None


def _private_egress_cloud_config(dns_servers: tuple[str, ...]) -> str:
    if not dns_servers:
        raise ValueError("Hetzner private egress requires at least one DNS server")
    resolvers = " ".join(
        shlex.quote(f"nameserver {value}") for value in dns_servers
    )
    return f"""#cloud-config
bootcmd:
  - |
      set -eu
      metadata_route="$(ip -4 route show 169.254.169.254/32 | head -n 1)"
      fabric_gateway="$(printf '%s\\n' "$metadata_route" | awk '{{for (i=1; i<=NF; i++) if ($i == "via") print $(i+1)}}')"
      private_interface="$(printf '%s\\n' "$metadata_route" | awk '{{for (i=1; i<=NF; i++) if ($i == "dev") print $(i+1)}}')"
      test -n "$fabric_gateway"
      test -n "$private_interface"
      ip -4 route replace default via "$fabric_gateway" dev "$private_interface"
      rm -f /etc/resolv.conf
      printf '%s\\n' {resolvers} > /etc/resolv.conf
      chmod 0644 /etc/resolv.conf
"""


def _validate_server_name(name: str) -> None:
    if not _SERVER_NAME_RE.fullmatch(name):
        raise ValueError(
            "Hetzner server names must be 1-63 lowercase letters, digits, or "
            "hyphens and must start and end with a letter or digit"
        )


def _server_id(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    parsed = str(value).strip()
    if not parsed.isdigit() or int(parsed) <= 0:
        return None
    return parsed


def _validated_labels(labels: dict[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in labels.items():
        if not isinstance(key, str) or not _LABEL_KEY_RE.fullmatch(key):
            raise ValueError(f"invalid Hetzner label key: {key!r}")
        if not isinstance(value, str) or not _LABEL_VALUE_RE.fullmatch(value):
            raise ValueError(f"invalid Hetzner label value for {key!r}: {value!r}")
        result[key] = value
    return result


def _mutation_error_result(
    exc: HetznerHttpError,
    *,
    applied_ids: Sequence[str],
    responses: Sequence[dict[str, Any]],
) -> ProviderMutationResult:
    response = {
        "responses": list(responses),
        "error": {"status": exc.status, "payload": exc.payload},
    }
    deterministic = 400 <= exc.status < 500 and exc.status not in {408, 425, 429}
    if deterministic and not applied_ids:
        return ProviderMutationResult(
            status="rejected",
            response=response,
            error=str(exc),
        )
    return ProviderMutationResult(
        status="uncertain",
        instance_ids=tuple(applied_ids),
        response=response,
        error=str(exc),
    )


def _partial_or_rejected(
    applied_ids: Sequence[str],
    responses: Sequence[dict[str, Any]],
    error: str,
) -> ProviderMutationResult:
    return ProviderMutationResult(
        status="uncertain" if applied_ids else "rejected",
        instance_ids=tuple(applied_ids),
        response={"responses": list(responses)},
        error=error,
    )


def _create_response_summary(response: dict[str, Any]) -> dict[str, Any]:
    server = response.get("server")
    server_summary: dict[str, Any] = {}
    if isinstance(server, dict):
        for key in ("id", "name", "status"):
            if key in server:
                server_summary[key] = server[key]
    return {
        "server": server_summary,
        "action": _action_summary(response.get("action")),
    }


def _action_summary(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        key: value[key] for key in ("id", "command", "status", "error") if key in value
    }
