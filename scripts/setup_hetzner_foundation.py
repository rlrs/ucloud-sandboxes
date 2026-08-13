#!/usr/bin/env python3
"""Idempotently create the non-billable Hetzner network and firewalls."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ucloud_sandboxes.providers.hetzner import HetznerClient  # noqa: E402


DEFAULT_NAME = "ucloud-sandboxes-production"
DEFAULT_NETWORK_CIDR = "10.42.0.0/16"
DEFAULT_SUBNET_CIDR = "10.42.0.0/24"
DEFAULT_NETWORK_ZONE = "eu-central"
DEFAULT_STATE_FILE = REPO_ROOT / ".hetzner" / "foundation.json"
DEFAULT_PUBLIC_KEY_FILE = REPO_ROOT / ".hetzner" / "ssh" / "gateway-init.pub"
_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--name", default=DEFAULT_NAME)
    result.add_argument("--network-cidr", default=DEFAULT_NETWORK_CIDR)
    result.add_argument("--subnet-cidr", default=DEFAULT_SUBNET_CIDR)
    result.add_argument("--network-zone", default=DEFAULT_NETWORK_ZONE)
    result.add_argument("--public-key-file", type=Path, default=DEFAULT_PUBLIC_KEY_FILE)
    result.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    result.add_argument("--api-token-env", default="HETZNER_API_KEY")
    result.add_argument(
        "--gateway-ssh-source-cidr",
        action="append",
        default=[],
        help=(
            "CIDR allowed to SSH to the gateway; repeat for multiple operator "
            "networks. With none, the gateway firewall exposes only HTTP(S)."
        ),
    )
    result.add_argument(
        "--egress-gateway-ip",
        help=(
            "Private IP of the NAT gateway; adds or verifies the Network "
            "default route used by enable_private_egress workers"
        ),
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not _NAME_RE.fullmatch(args.name):
        raise ValueError("foundation name must be a valid Hetzner resource name")
    public_key = _read_public_key(args.public_key_file)
    gateway_ssh_sources = _ip_networks(args.gateway_ssh_source_cidr)
    gateway_firewall_rules = _gateway_firewall_rules(gateway_ssh_sources)
    labels = {
        "ucloud-sandboxes/foundation": "production",
        "ucloud-sandboxes/managed": "true",
    }
    client = HetznerClient(api_token_env=args.api_token_env)

    network = _existing(client, "/networks", "networks", args.name)
    ssh_key = _existing(client, "/ssh_keys", "ssh_keys", f"{args.name}-init")
    firewall = _existing(client, "/firewalls", "firewalls", f"{args.name}-workers")
    gateway_firewall = _existing(
        client, "/firewalls", "firewalls", f"{args.name}-gateway"
    )
    if network is not None:
        _verify_network(
            network,
            network_cidr=args.network_cidr,
            subnet_cidr=args.subnet_cidr,
            network_zone=args.network_zone,
        )
    egress_gateway_ip = _egress_gateway_ip(
        args.egress_gateway_ip,
        subnet_cidr=args.subnet_cidr,
    )
    if egress_gateway_ip is None:
        egress_route = "not_requested"
    elif network is None:
        egress_route = "create"
    else:
        egress_route = _egress_route_state(network, egress_gateway_ip)
    if (
        ssh_key is not None
        and str(ssh_key.get("public_key") or "").strip() != public_key
    ):
        raise ValueError("existing Hetzner SSH key has different key material")
    if firewall is not None and firewall.get("rules") != []:
        raise ValueError("existing worker firewall is not deny-inbound/allow-outbound")
    gateway_firewall_needs_update = (
        gateway_firewall is not None
        and gateway_firewall.get("rules") != gateway_firewall_rules
    )

    plan = {
        "execute": bool(args.execute),
        "resources": {
            "network": "existing" if network else "create",
            "ssh_key": "existing" if ssh_key else "create",
            "firewall": "existing" if firewall else "create",
            "gateway_firewall": (
                "create"
                if gateway_firewall is None
                else "update"
                if gateway_firewall_needs_update
                else "existing"
            ),
            "egress_route": egress_route,
        },
    }
    if not args.execute:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    if network is None:
        response = client.request_json(
            "POST",
            "/networks",
            json_body={
                "name": args.name,
                "ip_range": args.network_cidr,
                "subnets": [
                    {
                        "type": "cloud",
                        "ip_range": args.subnet_cidr,
                        "network_zone": args.network_zone,
                    }
                ],
                "labels": labels,
                "expose_routes_to_vswitch": False,
            },
        )
        network = _response_object(response, "network")
        _verify_network(
            network,
            network_cidr=args.network_cidr,
            subnet_cidr=args.subnet_cidr,
            network_zone=args.network_zone,
        )
    if egress_gateway_ip is not None:
        egress_route = _egress_route_state(network, egress_gateway_ip)
        if egress_route == "create":
            client.request_json(
                "POST",
                f"/networks/{_positive_id(network, 'network')}/actions/add_route",
                json_body={
                    "destination": "0.0.0.0/0",
                    "gateway": egress_gateway_ip,
                },
            )
            network = _existing(client, "/networks", "networks", args.name)
            if (
                network is None
                or _egress_route_state(network, egress_gateway_ip) != "existing"
            ):
                raise ValueError("Hetzner Network did not retain the egress route")
    if ssh_key is None:
        response = client.request_json(
            "POST",
            "/ssh_keys",
            json_body={
                "name": f"{args.name}-init",
                "public_key": public_key,
                "labels": labels,
            },
        )
        ssh_key = _response_object(response, "ssh_key")
    if firewall is None:
        response = client.request_json(
            "POST",
            "/firewalls",
            json_body={
                "name": f"{args.name}-workers",
                "rules": [],
                "labels": labels,
            },
        )
        firewall = _response_object(response, "firewall")
    if gateway_firewall is None:
        response = client.request_json(
            "POST",
            "/firewalls",
            json_body={
                "name": f"{args.name}-gateway",
                "rules": gateway_firewall_rules,
                "labels": labels,
            },
        )
        gateway_firewall = _response_object(response, "firewall")
    elif gateway_firewall_needs_update:
        gateway_firewall_id = _positive_id(gateway_firewall, "gateway firewall")
        client.request_json(
            "POST",
            f"/firewalls/{gateway_firewall_id}/actions/set_rules",
            json_body={"rules": gateway_firewall_rules},
        )
        gateway_firewall = _existing(
            client,
            "/firewalls",
            "firewalls",
            f"{args.name}-gateway",
        )
        if (
            gateway_firewall is None
            or gateway_firewall.get("rules") != gateway_firewall_rules
        ):
            raise ValueError("Hetzner gateway firewall did not retain ingress rules")

    state = {
        "schema": 1,
        "name": args.name,
        "network_cidr": args.network_cidr,
        "subnet_cidr": args.subnet_cidr,
        "network_zone": args.network_zone,
        "network_id": _positive_id(network, "network"),
        "ssh_key_ids": [_positive_id(ssh_key, "ssh_key")],
        "firewall_ids": [_positive_id(firewall, "firewall")],
        "gateway_firewall_ids": [_positive_id(gateway_firewall, "gateway firewall")],
        "gateway_ssh_source_cidrs": gateway_ssh_sources,
        "private_key_file": str(args.public_key_file.with_suffix("")),
    }
    if egress_gateway_ip is not None:
        state["egress_gateway_ip"] = egress_gateway_ip
    _write_state(args.state_file, state)
    print(json.dumps({**plan, "state": state}, indent=2, sort_keys=True))
    return 0


def _existing(
    client: HetznerClient,
    path: str,
    response_key: str,
    name: str,
) -> dict[str, Any] | None:
    payload = client.request_json(
        "GET", path, params={"name": name, "page": 1, "per_page": 50}
    )
    if not isinstance(payload, dict) or not isinstance(payload.get(response_key), list):
        raise ValueError(f"invalid Hetzner {response_key} list response")
    matches = [item for item in payload[response_key] if isinstance(item, dict)]
    if len(matches) > 1:
        raise ValueError(f"multiple Hetzner resources have name {name!r}")
    return matches[0] if matches else None


def _verify_network(
    network: dict[str, Any],
    *,
    network_cidr: str,
    subnet_cidr: str,
    network_zone: str,
) -> None:
    if network.get("ip_range") != network_cidr:
        raise ValueError("existing Hetzner network has a different IP range")
    subnets = network.get("subnets")
    if not isinstance(subnets, list) or not any(
        isinstance(item, dict)
        and item.get("type") == "cloud"
        and item.get("ip_range") == subnet_cidr
        and item.get("network_zone") == network_zone
        for item in subnets
    ):
        raise ValueError("existing Hetzner network has a different cloud subnet")


def _egress_gateway_ip(value: object, *, subnet_cidr: str) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ValueError("egress gateway IP must be an IPv4 address")
    try:
        address = ipaddress.ip_address(value.strip())
        subnet = ipaddress.ip_network(subnet_cidr)
    except ValueError as exc:
        raise ValueError("egress gateway IP or subnet is invalid") from exc
    if address.version != 4 or subnet.version != 4 or address not in subnet.hosts():
        raise ValueError("egress gateway IP must be a host address in the cloud subnet")
    return str(address)


def _egress_route_state(
    network: dict[str, Any],
    gateway_ip: str,
) -> str:
    routes = network.get("routes")
    if not isinstance(routes, list):
        raise ValueError("existing Hetzner network has invalid routes")
    defaults = [
        item
        for item in routes
        if isinstance(item, dict) and item.get("destination") == "0.0.0.0/0"
    ]
    if not defaults:
        return "create"
    if len(defaults) != 1 or defaults[0].get("gateway") != gateway_ip:
        raise ValueError("existing Hetzner network has a different default route")
    return "existing"


def _response_object(payload: object, key: str) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get(key), dict):
        raise ValueError(f"invalid Hetzner create response for {key}")
    return payload[key]


def _positive_id(payload: dict[str, Any], label: str) -> int:
    value = payload.get("id")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"Hetzner {label} response has no positive id")
    return value


def _read_public_key(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(f"cannot read bootstrap public key: {path}") from exc
    if "\n" in value or "\r" in value or not value.startswith("ssh-ed25519 "):
        raise ValueError("bootstrap public key must be one Ed25519 OpenSSH key")
    return value


def _ip_networks(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError as exc:
            raise ValueError(f"invalid gateway SSH source CIDR: {value}") from exc
        normalized = str(network)
        if normalized not in result:
            result.append(normalized)
    return result


def _gateway_firewall_rules(ssh_sources: list[str]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    if ssh_sources:
        rules.append(
            {
                "direction": "in",
                "protocol": "tcp",
                "port": "22",
                "source_ips": ssh_sources,
                "destination_ips": [],
                "description": "Operator SSH",
            }
        )
    for port, description in (
        ("80", "ACME and HTTPS redirect"),
        ("443", "Sandbox SDK HTTPS"),
    ):
        rules.append(
            {
                "direction": "in",
                "protocol": "tcp",
                "port": port,
                "source_ips": ["0.0.0.0/0", "::/0"],
                "destination_ips": [],
                "description": description,
            }
        )
    return rules


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    os.chmod(path, 0o600)


if __name__ == "__main__":
    raise SystemExit(main())
