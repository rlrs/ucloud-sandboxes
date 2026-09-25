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


# One shared table: every packet pays a constant number of hash lookups keyed
# by interface, however many relay-only sandboxes the node hosts. Per-sandbox
# base chains made each packet walk one guard per sandbox.
RELAY_TABLE = "ucloud_relay"
# Set on forwarded packets the transit guard authorized, so a single iptables
# ACCEPT can let them past legacy private-address and Docker DROP rules. The
# kernel scrubs skb marks when a packet crosses from the sandbox netns.
RELAY_FORWARD_MARK = 0x01000000
_RELAY_CHAIN_KINDS = ("guard", "nat", "egress", "ingress")
_RELAY_MAPS = (
    ("guard_by_iif", "guard"),
    ("nat_by_iif", "nat"),
    ("egress_by_iif", "egress"),
    ("ingress_by_oif", "ingress"),
)


def legacy_relay_policy_table(slot: int) -> str:
    """Per-sandbox table name used before the shared table."""
    return f"ucloud_relay_{int(slot)}"


def relay_table_rules() -> str:
    """Replace the shared table's skeleton; callers append every sandbox.

    The pre-DNAT guard runs after defragmentation and before destination NAT.
    Its DROP verdict is final even when Docker or legacy rules later ACCEPT.
    Post-DNAT checks also revoke old conntrack routes after a DNS handoff.
    """
    t = RELAY_TABLE
    return (
        f"add table inet {t}\ndelete table inet {t}\n"
        f"table inet {t} {{\n"
        " set relay_interfaces { type ifname; }\n"
        + "".join(f" map {name} {{ type ifname : verdict; }}\n" for name, _ in _RELAY_MAPS)
        + " chain guard {\n  type filter hook prerouting priority -150; policy accept;\n"
        "  iifname vmap @guard_by_iif\n }\n"
        " chain destination {\n  type nat hook prerouting priority -110; policy accept;\n"
        "  iifname vmap @nat_by_iif\n }\n"
        " chain local {\n  type filter hook input priority -150; policy accept;\n"
        "  iifname @relay_interfaces drop\n }\n"
        " chain transit {\n  type filter hook forward priority -150; policy accept;\n"
        "  iifname vmap @egress_by_iif\n"
        "  oifname vmap @ingress_by_oif\n }\n"
        " chain host_output {\n  type filter hook output priority -150; policy accept;\n"
        "  oifname @relay_interfaces drop\n }\n"
        "}\n"
    )


def _relay_chain(lease: DirectNetworkLease, kind: str) -> str:
    return f"s{int(lease.slot)}_{kind}"


def _relay_elements(lease: DirectNetworkLease, verb: str) -> str:
    t = RELAY_TABLE
    interface = lease.host_interface  # generated from a validated numeric slot
    lines = [f'{verb} element inet {t} relay_interfaces {{ "{interface}" }}\n']
    for name, kind in _RELAY_MAPS:
        value = f' : jump {_relay_chain(lease, kind)}' if verb == "add" else ""
        lines.append(f'{verb} element inet {t} {name} {{ "{interface}"{value} }}\n')
    return "".join(lines)


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
    """Idempotent statements that replace one sandbox's rules in the table.

    They apply atomically in one nft transaction, whatever that sandbox's
    previous rules were, and fail without effect if the table is missing.
    Without addresses the sandbox keeps only its DROP rules.
    """
    t = RELAY_TABLE
    guest = str(ipaddress.IPv4Address(lease.guest_ip))
    resolved = relay_ipv4_addresses(addresses)
    targets = ", ".join(resolved)
    port = int(relay.port)
    rules = {
        "guard": [
            f"ip saddr {guest} ip daddr {relay.virtual_ip} tcp dport {port} accept",
            "drop",
        ],
        # Pick deterministically from the current A records. Existing flows
        # retain their conntrack translation while their endpoint remains
        # authorized.
        "nat": [
            f"ip daddr {relay.virtual_ip} tcp dport {port} dnat ip to {resolved[0]}:{port}"
        ] if resolved else [],
        "egress": [
            f"ip saddr {guest} ip daddr {{ {targets} }} tcp dport {port} "
            f"meta mark set meta mark or {RELAY_FORWARD_MARK:#x} accept",
            "drop",
        ],
        "ingress": [
            f"ip saddr {{ {targets} }} tcp sport {port} ct state established accept",
            "drop",
        ],
    }
    if not resolved:
        for kind in ("guard", "egress", "ingress"):
            rules[kind] = ["drop"]
    script = []
    for kind in _RELAY_CHAIN_KINDS:
        chain = _relay_chain(lease, kind)
        script.append(f"add chain inet {t} {chain}\nflush chain inet {t} {chain}\n")
        script.extend(f"add rule inet {t} {chain} {rule}\n" for rule in rules[kind])
    return "".join(script) + _relay_elements(lease, "add")


def relay_policy_removal(lease: DirectNetworkLease) -> str:
    """Idempotently remove one sandbox; fails without effect if the table is missing."""
    t = RELAY_TABLE
    chains = [_relay_chain(lease, kind) for kind in _RELAY_CHAIN_KINDS]
    legacy = legacy_relay_policy_table(lease.slot)
    # Create before deleting so the transaction succeeds whatever is present.
    return (
        f"add table inet {legacy}\ndelete table inet {legacy}\n"
        "".join(f"add chain inet {t} {chain}\n" for chain in chains)
        + _relay_elements(lease, "add")
        + _relay_elements(lease, "delete")
        + "".join(
            f"flush chain inet {t} {chain}\ndelete chain inet {t} {chain}\n"
            for chain in chains
        )
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
