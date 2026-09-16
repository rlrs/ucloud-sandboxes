"""Host-owned relay routing. No guest resolver or proxy settings are trusted."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import subprocess
from typing import Mapping, Sequence, TYPE_CHECKING

from .network_policy import RELAY_NAME, SandboxNetworkPolicy

if TYPE_CHECKING:
    from .direct_network import DirectNetworkLease


@dataclass(frozen=True)
class NetworkRelay:
    name: str
    host: str
    port: int

    @classmethod
    def parse(cls, name: str, endpoint: str) -> NetworkRelay:
        # Import locally to keep the wire contract independent of host code.
        from .direct_network import DirectNetworkTcpEgress

        if not isinstance(name, str) or not RELAY_NAME.fullmatch(name):
            raise ValueError("network relay names must match [a-z][a-z0-9-]{0,31}")
        if not isinstance(endpoint, str):
            raise ValueError("network relay endpoints must be HOST:PORT strings")
        parsed = DirectNetworkTcpEgress.parse(endpoint)
        if not parsed.is_dynamic:
            relay_ipv4_addresses((parsed.address,))
        return cls(name, parsed.address, parsed.port)

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def virtual_ip(self) -> str:
        try:
            return str(ipaddress.IPv4Address(self.host))
        except ValueError:
            # RFC 2544 benchmarking space: stable across node, process and DNS
            # changes. Each sandbox selects one relay, so cross-relay hash
            # collisions do not combine authorities.
            slot = int.from_bytes(
                hashlib.sha256(self.name.encode()).digest()[:4], "big"
            )
            return str(ipaddress.IPv4Address("198.18.0.0") + (slot % 131070 + 1))

    @property
    def capability(self) -> str:
        return SandboxNetworkPolicy.relay_only(self.name).capability


def parse_network_relays(raw: object) -> dict[str, NetworkRelay]:
    if not isinstance(raw, dict) or len(raw) > 64:
        raise ValueError(
            "network_relays must be an object with at most 64 named endpoints"
        )
    return {name: NetworkRelay.parse(name, endpoint) for name, endpoint in raw.items()}


def relay_policy_table(lease: DirectNetworkLease) -> str:
    return f"ucloud_relay_{lease.slot}"


def relay_ipv4_addresses(addresses: Sequence[str]) -> tuple[str, ...]:
    resolved = tuple(sorted({str(ipaddress.IPv4Address(ip)) for ip in addresses}))
    for value in resolved:
        ip = ipaddress.IPv4Address(value)
        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_reserved
        ):
            raise ValueError("relay must resolve to a remote unicast IPv4 endpoint")
        if ip in ipaddress.IPv4Network("100.96.0.0/16"):
            raise ValueError("relay cannot resolve to a sandbox network address")
    return resolved


def relay_policy_rules(
    lease: DirectNetworkLease,
    relay: NetworkRelay,
    addresses: Sequence[str],
) -> str:
    """One atomic nft transaction, including replacement of an existing table.

    The pre-DNAT guard runs after defragmentation and before destination NAT.
    Its DROP verdict is final even when Docker or legacy rules later ACCEPT.
    Post-DNAT checks also revoke old conntrack routes after a DNS handoff.
    """
    table = relay_policy_table(lease)
    interface = lease.host_interface  # generated from a validated numeric slot
    guest = str(ipaddress.IPv4Address(lease.guest_ip))
    resolved = relay_ipv4_addresses(addresses)
    allow_original = (
        (
            f'  iifname "{interface}" ip saddr {guest} ip daddr {relay.virtual_ip} '
            f"tcp dport {relay.port} accept\n"
        )
        if resolved
        else ""
    )
    allow_forward = (
        (
            f'  iifname "{interface}" ip saddr {guest} ip daddr {{ {", ".join(resolved)} }} '
            f"tcp dport {relay.port} accept\n"
        )
        if resolved
        else ""
    )
    allow_reply = (
        (
            f'  oifname "{interface}" ip saddr {{ {", ".join(resolved)} }} '
            f"tcp sport {relay.port} ct state established accept\n"
        )
        if resolved
        else ""
    )
    # Pick deterministically from the current A records. Existing flows retain
    # their conntrack translation while their endpoint remains authorized.
    dnat = (
        (
            f'  iifname "{interface}" ip daddr {relay.virtual_ip} tcp dport {relay.port} '
            f"dnat ip to {resolved[0]}:{relay.port}\n"
        )
        if resolved
        else ""
    )
    return (
        f"add table inet {table}\ndelete table inet {table}\n"
        f"table inet {table} {{\n"
        " chain guard {\n  type filter hook prerouting priority -150; policy accept;\n"
        + allow_original
        + f'  iifname "{interface}" drop\n }}\n'
        + " chain destination {\n  type nat hook prerouting priority -110; policy accept;\n"
        + dnat
        + " }\n"
        + " chain local {\n  type filter hook input priority -150; policy accept;\n"
        + f'  iifname "{interface}" drop\n }}\n'
        + " chain transit {\n  type filter hook forward priority -150; policy accept;\n"
        + allow_forward
        + f'  iifname "{interface}" drop\n'
        + allow_reply
        + f'  oifname "{interface}" drop\n }}\n'
        + " chain host_output {\n  type filter hook output priority -150; policy accept;\n"
        + f'  oifname "{interface}" drop\n }}\n'
        + "}\n"
    )


def apply_nft(script: str) -> None:
    result = subprocess.run(
        ("nft", "-f", "-"),
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=15,
    )
    if result.returncode:
        from .direct_network import DirectNetworkError

        raise DirectNetworkError(
            f"relay firewall transaction failed: {result.stderr.strip()}"
        )


def relay_hosts(
    relays: Mapping[str, NetworkRelay], policy: SandboxNetworkPolicy
) -> dict[str, str]:
    if policy.egress != "relay":
        return {}
    relay = relays[policy.relay]
    return {relay.host: relay.virtual_ip}
