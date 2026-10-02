from __future__ import annotations

from contextlib import contextmanager
import ctypes
from dataclasses import dataclass, replace
import errno
import fcntl
import hashlib
import ipaddress
import json
import logging
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import tempfile
import threading
import time
from typing import Callable, Mapping, Sequence

from . import phase_timings
from .network_policy import SandboxNetworkPolicy
from .relay_network import (
    RELAY_FORWARD_MARK,
    NetworkRelay,
    apply_nft,
    legacy_relay_policy_table,
    parse_network_relays,
    relay_hosts,
    relay_ipv4_addresses,
    relay_policy_removal,
    relay_policy_rules,
    relay_table_rules,
)


_LOG = logging.getLogger(__name__)


NETWORK_STATE_VERSION = 1
NETWORK_CIDR = ipaddress.IPv4Network("100.96.0.0/16")
MAX_NETWORK_SLOTS = (NETWORK_CIDR.num_addresses // 2) - 1
# UCloud private/public-link networking uses a 1420-byte path MTU. Leaving the
# veth default at 1500 allows small requests through but black-holes larger TLS
# records when upstream ICMP fragmentation feedback is filtered.
NETWORK_MTU = 1420
# Pre-created netns+veth pairs a node keeps for new leases: one burst of the
# 32 concurrent creates the create gate measures.
NETWORK_POOL_SIZE = 32
# The refill defers to in-flight ensures for at most this long per pair, so a
# sustained create stream still refills slowly instead of never.
_POOL_YIELD_SECONDS = 1.0
_POOL_RETRY_SECONDS = 5.0
DEFAULT_EGRESS_RESOLVE_INTERVAL_SECONDS = 2.0
DENIED_DESTINATIONS = (
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.168.0.0/16",
)


class DirectNetworkError(RuntimeError):
    pass


def _write_durably(path: Path, payload: object, *, sync_directory: bool = True) -> None:
    """Replace ``path`` with compact JSON; fsync the file, then (by default) its directory."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if sync_directory:
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class DirectNetworkLease:
    sandbox_id: str
    sandbox_generation: int
    slot: int
    namespace: str
    namespace_path: Path
    host_interface: str
    host_ip: str
    guest_ip: str


@dataclass(frozen=True)
class DirectNetworkTcpEgress:
    address: str
    port: int

    @classmethod
    def parse(cls, value: str) -> DirectNetworkTcpEgress:
        raw_address, separator, raw_port = value.rpartition(":")
        if not separator or not raw_address or not raw_port:
            raise ValueError(
                "direct network TCP egress must use the IPv4:port form"
            )
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError(
                "direct network TCP egress must use the HOST:port form"
            ) from exc
        if not 1 <= port <= 65535:
            raise ValueError("direct network TCP egress port must be in 1..65535")
        try:
            address = str(ipaddress.IPv4Address(raw_address))
        except ValueError:
            address = raw_address.rstrip(".").lower()
            if (
                len(address) > 253
                or not re.fullmatch(
                    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
                    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*",
                    address,
                )
            ):
                raise ValueError(
                    "direct network TCP egress must use the IPv4-or-DNS:port form"
                )
        return cls(address=address, port=port)

    def endpoint(self) -> str:
        return f"{self.address}:{self.port}"

    @property
    def is_dynamic(self) -> bool:
        try:
            ipaddress.IPv4Address(self.address)
        except ValueError:
            return True
        return False


class DirectNetworkManager:
    """Crash-durable owner of direct-runtime netns/veth/NAT slots.

    A background thread keeps ``pool_size`` slots in the durable ``pool``,
    each with a configured pair in namespace ``ucloud-pool-<slot>``. A slot is
    in ``leases`` or ``pool``, never both, and moves in the write that records
    its lease, so a pair has one owner. A pooled name a crash leaves behind
    belongs to the slot's lease: its next ensure attaches it, or release drops it.
    """

    def __init__(
        self,
        state_path: Path,
        *,
        namespace_root: Path = Path("/run/netns"),
        allowed_tcp_egress: Sequence[str] = (),
        network_relays: Mapping[str, str] | None = None,
        nft_runner: Callable[[str], None] | None = None,
        runner: Callable[[Sequence[str]], None] | None = None,
        resolver: Callable[[str], Sequence[str]] | None = None,
        ip_batch_runner: Callable[[Sequence[str], str], None] | None = None,
        resolve_interval_seconds: float = DEFAULT_EGRESS_RESOLVE_INTERVAL_SECONDS,
        pool_size: int = 0,
    ) -> None:
        if not state_path.is_absolute() or not namespace_root.is_absolute():
            raise ValueError("direct network paths must be absolute")
        if resolve_interval_seconds <= 0:
            raise ValueError("direct network resolve interval must be positive")
        if not 0 <= pool_size <= 1024:
            raise ValueError("direct network pool size must be in 0..1024")
        self.state_path = state_path
        self.lock_path = state_path.with_suffix(state_path.suffix + ".lock")
        self.pool_size = pool_size
        # Only slots this process configured while owning the pool are handed
        # out; other durable pool slots are rechecked first.
        self._pool_ready: set[int] = set()
        self._pool_guard = threading.Lock()
        self._pool_wake, self._pool_stop = threading.Event(), threading.Event()
        self._pool_thread: threading.Thread | None = None
        self._foreground = 0
        self.egress_state_path = state_path.with_suffix(
            state_path.suffix + ".egress.json"
        )
        self.namespace_root = namespace_root
        self.allowed_tcp_egress = tuple(
            dict.fromkeys(
                DirectNetworkTcpEgress.parse(value)
                for value in allowed_tcp_egress
            )
        )
        self.relays = parse_network_relays(
            {} if network_relays is None else network_relays
        )
        self.nft_runner = nft_runner or apply_nft
        self._relay_resolution: dict[str, tuple[float, tuple[str, ...]]] = {}
        # Per-sandbox statements last applied by this process, keyed by slot.
        # Deltas are only attempted once this process has rebuilt the table.
        self._relay_applied: dict[int, str] = {}
        self._relay_table_ready = False
        self.runner = runner or self._run
        self.ip_batch_runner = ip_batch_runner or (
            self._run_ip_batch if runner is None else self._split_ip_batch)
        self._host_rules_observed_at = float("-inf")
        self.resolver = resolver or self._resolve_ipv4
        self.resolve_interval_seconds = float(resolve_interval_seconds)
        self._egress_guard = threading.Lock()
        self._resolved_tcp_egress = self._load_egress_state()

    @property
    def has_dynamic_tcp_egress(self) -> bool:
        return bool(self.relays) or any(
            endpoint.is_dynamic for endpoint in self.allowed_tcp_egress
        )

    def reconcile(self) -> None:
        """Reconcile host rules and refresh DNS-backed exact egress exceptions."""
        if self.relays:
            # Verify nft userspace and kernel support before advertising relay
            # capabilities, even when the node has no active sandboxes.
            probe = f"ucloud_relay_probe_{os.getpid()}"
            self.nft_runner(
                f"add table inet {probe}\n"
                f"add chain inet {probe} nat {{ type nat hook prerouting priority -110; }}\n"
                f"add map inet {probe} m {{ type ifname : verdict; }}\n"
                f"delete table inet {probe}\n"
            )
        # Restore restrictions before any broad legacy forwarding rules.
        self._refresh_relay_policies(force=True)
        self._ensure_host_rules()
        self._refresh_relay_policies(force=True)

    def refresh_tcp_egress(self) -> None:
        """Refresh only dynamic exact egress rules after initial reconciliation."""
        self._refresh_relay_policies()
        self._reconcile_tcp_egress()

    def ensure(
        self,
        sandbox_id: str,
        sandbox_generation: int,
        *,
        avoid_guest_ips: Sequence[str] = (),
        network_policy: SandboxNetworkPolicy = SandboxNetworkPolicy(),
        host_rules_ready: bool = False,
    ) -> DirectNetworkLease:
        requested_at = time.monotonic()
        self.validate_policy(network_policy)
        if sandbox_generation < 0:
            raise ValueError("sandbox generation cannot be negative")
        avoided = {
            str(ipaddress.IPv4Address(item))
            for item in avoid_guest_ips
        }
        if any(ipaddress.IPv4Address(item) not in NETWORK_CIDR for item in avoided):
            raise ValueError("avoided guest IP is outside the direct network")
        key = self._key(sandbox_id, sandbox_generation)
        with self._lease_locked(key), self._foreground_work():
            pooled = False
            with self._locked():
                state = self._load()
                slot = state["leases"].get(key)
                if slot is None:
                    slot = self._claim_pooled(state, avoided)
                    pooled = slot is not None
                    used = {int(item) for item in state["leases"].values()}
                    used.update(state["pool"])
                    slot = slot or next(
                        (candidate for candidate in range(1, MAX_NETWORK_SLOTS + 1)
                         if candidate not in used
                         and self._lease(
                             sandbox_id,
                             sandbox_generation,
                             candidate,
                         ).guest_ip not in avoided),
                        None,
                    )
                    if slot is None:
                        raise DirectNetworkError("direct network slot capacity is exhausted")
                    # One durable write moves a pooled slot to this lease.
                    state["leases"][key] = slot
                    if network_policy.egress == "relay":
                        state.setdefault("policies", {})[key] = network_policy.to_dict()
                    self._store(state)
                stored_policy = SandboxNetworkPolicy.from_dict(
                    state.get("policies", {}).get(key, {})
                )
                if stored_policy != network_policy:
                    raise DirectNetworkError(
                        "network policy is immutable for a sandbox generation"
                    )
                lease = self._lease(sandbox_id, sandbox_generation, int(slot))
                if lease.guest_ip in avoided:
                    raise DirectNetworkError(
                        "existing direct network lease reuses a forbidden guest IP"
                    )
                if not host_rules_ready and self._host_rules_observed_at < requested_at:
                    # Requests already queued share this fresh reconciliation.
                    # No TTL: a later request must take a new kernel snapshot.
                    observed_at = time.monotonic()
                    self._host_rules_observed_at = float("-inf")
                    with phase_timings.phase("network_host_rules"):  # Deliberate per-create cost.
                        self._ensure_host_rules()
                    self._host_rules_observed_at = observed_at
                if network_policy.egress == "relay":
                    addresses = self._resolve_relay(network_policy.relay)
                    self._install_relay_policy(lease, network_policy, addresses, state)
                    if not addresses:
                        raise DirectNetworkError(
                            "relay has no usable IPv4 address; egress is blocked"
                        )
            # A pair the pool configured needs only its name; any other lease,
            # and a handoff a crash interrupted, is checked and repaired.
            adopted = self._adopt_pooled(lease)
            if not (pooled and adopted and self._interface_present(lease.host_interface)):
                self._ensure_kernel_lease(lease)
            return lease

    def start_pool(self) -> None:
        """Start the low-priority refill, or the trim of a disabled pool."""
        if self._pool_thread is not None and self._pool_thread.is_alive():
            return
        if self.pool_size == 0:
            with self._locked():
                if not self._load()["pool"]:
                    return
        self._pool_stop.clear()
        self._pool_thread = threading.Thread(
            target=self._pool_loop, name="ucloud-direct-network-pool", daemon=True
        )
        self._pool_thread.start()

    def stop_pool(self) -> None:
        """Stop refilling. Pooled slots stay durable and are rechecked on start."""
        self._pool_stop.set()
        self._pool_wake.set()
        if self._pool_thread is not None:
            self._pool_thread.join(timeout=10)
        self._pool_thread = None

    @contextmanager
    def _foreground_work(self):
        with self._pool_guard:
            self._foreground += 1
        try:
            yield
        finally:
            with self._pool_guard:
                self._foreground -= 1

    def _claim_pooled(self, state: dict, avoided: set[str]) -> int | None:
        """Move one configured slot out of ``state["pool"]``."""
        with self._pool_guard:
            # A ready slot no longer pooled was leased by an older release.
            self._pool_ready.intersection_update(state["pool"])
            slot = min((item for item in self._pool_ready
                        if self._pool_lease(item).guest_ip not in avoided), default=None)
            if slot is not None:
                self._pool_ready.discard(slot)
                state["pool"].remove(slot)
                self._pool_wake.set()
        return slot

    def _pool_loop(self) -> None:
        # Low priority by deferral, not SCHED_IDLE: this thread shares the GIL
        # and the state lock with creates, and starving a holder stalls them.
        owner = self.lock_path.with_name(self.lock_path.name + ".pool")
        owner.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with owner.open("a+b") as handle:
            try:
                # One process owns the pool; others treat pooled slots as used.
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                _LOG.warning("another process owns the direct network pool")
                return
            try:
                while not self._pool_stop.is_set():
                    try:
                        progressed = self._refill_one()
                    except Exception:
                        _LOG.exception("direct network pool refill failed; retrying")
                        self._pool_stop.wait(_POOL_RETRY_SECONDS)
                        continue
                    if not progressed and self.pool_size == 0:
                        return
                    if not progressed:
                        self._pool_wake.wait()
                        self._pool_wake.clear()
            finally:
                with self._pool_guard:  # Hand-outs end before another owner starts.
                    self._pool_ready.clear()

    def _refill_one(self) -> bool:
        """Fill, recheck or trim one pooled slot; False once the pool is settled."""
        deadline = time.monotonic() + _POOL_YIELD_SECONDS
        while self._foreground and time.monotonic() < deadline:
            if self._pool_stop.wait(0.01):
                return False
        with self._locked():
            state = self._load()
            pool = state["pool"]
            with self._pool_guard:
                pending = [slot for slot in pool if slot not in self._pool_ready]
                trim = len(pool) > self.pool_size
                if trim:  # Under the state lock, so no hand-out holds it.
                    slot = (pending or pool)[-1]
                    self._pool_ready.discard(slot)
            if not trim and pending:
                slot = pending[0]
            elif not trim:
                used = set(pool).union(state["leases"].values())
                free = (item for item in range(1, MAX_NETWORK_SLOTS + 1) if item not in used)
                if len(pool) == self.pool_size or (slot := next(free, None)) is None:
                    return False
                # Written before any kernel object exists, so none is orphaned.
                state["pool"] = sorted((*pool, slot))
                self._store(state, pool_only=True)
        if not trim:  # Recreated unless complete; configuration is idempotent.
            self._ensure_kernel_lease(self._pool_lease(slot))
            with self._pool_guard:
                self._pool_ready.add(slot)
            return True
        self._cleanup_kernel_lease(self._pool_lease(slot))
        with self._locked():
            state = self._load()
            state["pool"] = [item for item in state["pool"] if item != slot]
            self._store(state, pool_only=True)
        return True

    def release(self, sandbox_id: str, sandbox_generation: int) -> None:
        key = self._key(sandbox_id, sandbox_generation)
        with self._lease_locked(key):
            with self._locked():
                state = self._load()
                raw_slot = state["leases"].get(key)
                if raw_slot is None:
                    return
                lease = self._lease(sandbox_id, sandbox_generation, int(raw_slot))
            # Keep the slot allocated until kernel cleanup finishes. Different
            # incarnations can set up their own namespaces during this work.
            self._cleanup_kernel_lease(lease)
            with self._locked():
                state = self._load()
                if state["leases"].get(key) != raw_slot:
                    raise DirectNetworkError("network lease changed during cleanup")
                if key in state.get("policies", {}):
                    if self._command_ok(
                        ("ip", "link", "show", "dev", lease.host_interface)
                    ):
                        raise DirectNetworkError(
                            "cannot release relay policy while interface exists"
                        )
                    self._remove_relay_policy(lease, key, state)
                    del state["policies"][key]
                del state["leases"][key]
                self._store(state)

    def lease(self, sandbox_id: str, sandbox_generation: int) -> DirectNetworkLease | None:
        key = self._key(sandbox_id, sandbox_generation)
        with self._locked():
            state = self._load()
            raw_slot = state["leases"].get(key)
        if raw_slot is None:
            return None
        return self._lease(sandbox_id, sandbox_generation, int(raw_slot))

    def validate_policy(self, policy: SandboxNetworkPolicy) -> None:
        if not isinstance(policy, SandboxNetworkPolicy):
            raise ValueError("network policy must be a SandboxNetworkPolicy")
        if policy.egress == "relay" and policy.relay not in self.relays:
            raise ValueError(
                f"network relay {policy.relay!r} is not configured on this node"
            )

    def hosts_for_policy(self, policy: SandboxNetworkPolicy) -> dict[str, str]:
        self.validate_policy(policy)
        return relay_hosts(self.relays, policy)

    def _resolve_relay(self, name: str, *, force: bool = False) -> tuple[str, ...]:
        cached = self._relay_resolution.get(name)
        if (
            not force
            and cached
            and time.monotonic() - cached[0] < self.resolve_interval_seconds
        ):
            return cached[1]
        relay = self.relays[name]
        endpoint = DirectNetworkTcpEgress.parse(relay.endpoint)
        try:
            values = self.resolver(relay.host) if endpoint.is_dynamic else (relay.host,)
            addresses = tuple(sorted({str(ipaddress.IPv4Address(ip)) for ip in values}))
            # Validate even when there are no active leases. Reject the entire
            # DNS answer if it contains a forbidden destination.
            relay_ipv4_addresses(addresses)
        except (OSError, ValueError):
            addresses = ()
        if not addresses and (cached is None or cached[1]):
            _LOG.warning(
                "network relay %s has no usable IPv4 address; blocking its egress", name
            )
        elif addresses and cached is not None and not cached[1]:
            _LOG.info("network relay %s resolved again; restoring its egress", name)
        self._relay_resolution[name] = (time.monotonic(), addresses)
        return addresses

    def _install_relay_policy(
        self,
        lease: DirectNetworkLease,
        policy: SandboxNetworkPolicy,
        addresses: tuple[str, ...],
        state: dict,
    ) -> None:
        script = relay_policy_rules(lease, self.relays[policy.relay], addresses)
        if self._apply_relay_delta(script, state):
            self._relay_applied[lease.slot] = script
        self._ensure_relay_forward_accept()

    def _remove_relay_policy(
        self, lease: DirectNetworkLease, key: str, state: dict
    ) -> None:
        remaining = {
            **state,
            "policies": {
                other: raw
                for other, raw in state.get("policies", {}).items()
                if other != key
            },
        }
        if self._apply_relay_delta(relay_policy_removal(lease), remaining):
            self._relay_applied.pop(lease.slot, None)
        # An interface ACCEPT from an earlier release must never outlive the
        # relay policy: a reused slot would bypass private-destination denies.
        legacy = ("FORWARD", "-i", lease.host_interface, "-j", "ACCEPT")
        if self._command_ok(("iptables", "-C", *legacy)):
            self.runner(("iptables", "-D", *legacy))

    def _apply_relay_delta(self, script: str, state: dict) -> bool:
        """Apply per-sandbox statements, else rebuild from durable state.

        Returns whether the delta applied; a rebuild records everything itself.
        """
        if self._relay_table_ready:
            try:
                self.nft_runner(script)
                return True
            except DirectNetworkError as exc:
                # A failed transaction changed nothing. The table may have
                # been removed underneath us, so rebuild it in full.
                _LOG.warning("relay firewall delta failed; rebuilding: %s", exc)
        resolved = {name: self._resolve_relay(name) for name in self.relays}
        fragments, leases, _missing = self._relay_fragments(state, resolved)
        self._rebuild_relay_table(fragments, leases)
        return False

    def _relay_fragments(
        self, state: dict, resolved: Mapping[str, tuple[str, ...]]
    ) -> tuple[dict[int, str], list[DirectNetworkLease], set[str]]:
        fragments: dict[int, str] = {}
        leases: list[DirectNetworkLease] = []
        missing: set[str] = set()
        for key, raw in sorted(state.get("policies", {}).items()):
            policy = SandboxNetworkPolicy.from_dict(raw)
            sandbox_id, generation = key.split("\0")
            lease = self._lease(sandbox_id, int(generation), state["leases"][key])
            leases.append(lease)
            if policy.relay not in self.relays:
                # Configuration removal revokes access, even if a sentry
                # survived the node-agent restart. Keep the durable lease
                # so startup can recover once its relay is restored.
                fragments[lease.slot] = relay_policy_rules(
                    lease, NetworkRelay(policy.relay, "0.0.0.0", 1), ()
                )
                missing.add(policy.relay)
                continue
            fragments[lease.slot] = relay_policy_rules(
                lease, self.relays[policy.relay], resolved[policy.relay]
            )
        return fragments, leases, missing

    def _rebuild_relay_table(
        self, fragments: Mapping[int, str], leases: Sequence[DirectNetworkLease]
    ) -> None:
        # One transaction replaces the shared table and retires any per-sandbox
        # tables from earlier releases, so there is no unguarded moment.
        legacy = "".join(
            f"add table inet {table}\ndelete table inet {table}\n"
            for table in (legacy_relay_policy_table(lease.slot) for lease in leases)
        )
        self._relay_table_ready = False
        self.nft_runner(relay_table_rules() + "".join(fragments.values()) + legacy)
        self._relay_applied = dict(fragments)
        self._relay_table_ready = True
        if leases:
            self._ensure_relay_forward_accept()
            self._remove_legacy_relay_accepts(leases)

    def _refresh_relay_policies(self, *, force: bool = False) -> None:
        # Share the durable lease lock with create/delete so DNS refresh cannot
        # reinstall rules after a slot has been released or reassigned.
        with self._locked():
            state = self._load()
            if not self.relays and not state.get("policies"):
                return  # relay-free nodes never need nftables
            resolved = {
                name: self._resolve_relay(name, force=True) for name in self.relays
            }
            fragments, leases, missing = self._relay_fragments(state, resolved)
            changed = {
                slot: script
                for slot, script in fragments.items()
                if self._relay_applied.get(slot) != script
            }
            if force or not self._relay_table_ready:
                self._rebuild_relay_table(fragments, leases)
            elif changed:
                try:
                    # Every changed sandbox in one atomic transaction.
                    self.nft_runner("".join(changed.values()))
                    self._relay_applied.update(changed)
                except DirectNetworkError as exc:
                    _LOG.warning("relay firewall refresh failed; rebuilding: %s", exc)
                    self._rebuild_relay_table(fragments, leases)
            if missing:
                raise DirectNetworkError(
                    f"active network relays are missing: {sorted(missing)}"
                )

    @staticmethod
    def _relay_forward_accept() -> tuple[str, ...]:
        # The nft prerouting and forward guards have already constrained every
        # packet and marked what they authorized. This one exception permits
        # that traffic past legacy private-address denies and Docker's FORWARD
        # default DROP, independent of the number of sandboxes.
        mark = f"{RELAY_FORWARD_MARK:#x}"
        return (
            "FORWARD", "-s", str(NETWORK_CIDR),
            "-m", "mark", "--mark", f"{mark}/{mark}", "-j", "ACCEPT",
        )

    def _ensure_relay_forward_accept(
        self,
        *,
        snapshot: set[tuple[str, ...]] | None = None,
        move_to_top: bool = False,
    ) -> None:
        rule = self._relay_forward_accept()
        if move_to_top:
            # A DROP was just inserted above it; restore precedence.
            self._run_best_effort(("iptables", "-D", *rule))
            snapshot = None
        self._ensure_iptables(
            ("iptables", "-C", *rule),
            ("iptables", "-I", rule[0], "1", *rule[1:]),
            snapshot=snapshot,
        )

    def _remove_legacy_relay_accepts(
        self, leases: Sequence[DirectNetworkLease]
    ) -> None:
        # Earlier releases installed one FORWARD ACCEPT per relay interface.
        snapshot = self._iptables_snapshot()
        for lease in leases:
            rule = ("FORWARD", "-i", lease.host_interface, "-j", "ACCEPT")
            present = (
                ("filter", "-A", *rule) in snapshot
                if snapshot is not None
                else self._command_ok(("iptables", "-C", *rule))
            )
            if present:
                self.runner(("iptables", "-D", *rule))

    def _ensure_host_rules(self) -> None:
        # Read a fresh kernel snapshot for this reconciliation, never a cached
        # assertion that policy remains installed across requests.
        snapshot = self._iptables_snapshot()
        self.runner(("sysctl", "-q", "-w", "net.ipv4.ip_forward=1"))
        self._ensure_iptables(
            ("iptables", "-C", "INPUT", "-s", str(NETWORK_CIDR), "-j", "DROP"),
            ("iptables", "-I", "INPUT", "1", "-s", str(NETWORK_CIDR), "-j", "DROP"),
            snapshot=snapshot,
        )
        reordered = False
        for destination in DENIED_DESTINATIONS:
            reordered |= self._ensure_iptables(
                (
                    "iptables", "-C", "FORWARD", "-s", str(NETWORK_CIDR),
                    "-d", destination, "-j", "DROP",
                ),
                (
                    "iptables", "-I", "FORWARD", "1", "-s", str(NETWORK_CIDR),
                    "-d", destination, "-j", "DROP",
                ),
                snapshot=snapshot,
            )
        self._reconcile_tcp_egress(snapshot=snapshot)
        if self.relays:
            self._ensure_relay_forward_accept(
                snapshot=snapshot, move_to_top=reordered
            )
        self._ensure_iptables(
            ("iptables", "-C", "FORWARD", "-s", str(NETWORK_CIDR), "-j", "ACCEPT"),
            ("iptables", "-A", "FORWARD", "-s", str(NETWORK_CIDR), "-j", "ACCEPT"),
            snapshot=snapshot,
        )
        self._ensure_iptables(
            (
                "iptables", "-C", "FORWARD", "-d", str(NETWORK_CIDR),
                "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED",
                "-j", "ACCEPT",
            ),
            (
                "iptables", "-A", "FORWARD", "-d", str(NETWORK_CIDR),
                "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED",
                "-j", "ACCEPT",
            ),
            snapshot=snapshot,
        )
        self._ensure_iptables(
            (
                "iptables", "-t", "nat", "-C", "POSTROUTING",
                "-s", str(NETWORK_CIDR), "-j", "MASQUERADE",
            ),
            (
                "iptables", "-t", "nat", "-A", "POSTROUTING",
                "-s", str(NETWORK_CIDR), "-j", "MASQUERADE",
            ),
            snapshot=snapshot,
        )

    def _reconcile_tcp_egress(
        self, *, snapshot: set[tuple[str, ...]] | None = None
    ) -> None:
        # Exact service exceptions sit above the broad private-destination
        # denies. DNS names are resolved on the host and become /32 rules; no
        # resolver or general RFC1918 access is exposed to a sandbox.
        with self._egress_guard:
            previous = self._resolved_tcp_egress
            resolved: dict[DirectNetworkTcpEgress, tuple[str, ...]] = {}
            for endpoint in self.allowed_tcp_egress:
                if not endpoint.is_dynamic:
                    addresses = (endpoint.address,)
                else:
                    try:
                        addresses = tuple(
                            dict.fromkeys(
                                str(ipaddress.IPv4Address(address))
                                for address in self.resolver(endpoint.address)
                            )
                        )
                    except (OSError, ValueError):
                        addresses = previous.get(endpoint, ())
                    if not addresses:
                        raise DirectNetworkError(
                            "direct network could not resolve private egress "
                            f"endpoint {endpoint.endpoint()}"
                        )
                resolved[endpoint] = addresses

            old_rules = {
                (address, endpoint.port)
                for endpoint, addresses in previous.items()
                for address in addresses
            }
            new_rules = {
                (address, endpoint.port)
                for endpoint, addresses in resolved.items()
                for address in addresses
            }

            # Install replacements first so a DNS handoff does not deliberately
            # create a relay outage. Remove only rules previously owned by this
            # manager, preserving unrelated firewall policy.
            for address, port in sorted(new_rules):
                rule = self._tcp_egress_rule(address, port)
                self._ensure_iptables(
                    ("iptables", "-C", "FORWARD", *rule),
                    ("iptables", "-I", "FORWARD", "1", *rule),
                    snapshot=snapshot,
                )
            for address, port in sorted(old_rules - new_rules):
                self._run_best_effort(
                    (
                        "iptables",
                        "-D",
                        "FORWARD",
                        *self._tcp_egress_rule(address, port),
                    )
                )
            if resolved != previous:
                self._store_egress_state(resolved)
            self._resolved_tcp_egress = resolved

    @staticmethod
    def _tcp_egress_rule(address: str, port: int) -> tuple[str, ...]:
        return (
            "-s", str(NETWORK_CIDR),
            "-d", f"{address}/32",
            "-p", "tcp",
            "--dport", str(port),
            "-j", "ACCEPT",
        )

    @staticmethod
    def _resolve_ipv4(host: str) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                item[4][0]
                for item in socket.getaddrinfo(
                    host,
                    None,
                    family=socket.AF_INET,
                    type=socket.SOCK_STREAM,
                )
            )
        )

    def _load_egress_state(
        self,
    ) -> dict[DirectNetworkTcpEgress, tuple[str, ...]]:
        try:
            raw = json.loads(self.egress_state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        try:
            if not isinstance(raw, dict) or raw.get("version") != 1:
                raise ValueError
            endpoints = raw["endpoints"]
            if not isinstance(endpoints, dict):
                raise ValueError
            resolved = {}
            for endpoint_value, addresses in endpoints.items():
                endpoint = DirectNetworkTcpEgress.parse(endpoint_value)
                if (
                    not isinstance(addresses, list)
                    or not addresses
                    or any(not isinstance(address, str) for address in addresses)
                ):
                    raise ValueError
                resolved[endpoint] = tuple(
                    str(ipaddress.IPv4Address(address)) for address in addresses
                )
            return resolved
        except (KeyError, TypeError, ValueError) as exc:
            raise DirectNetworkError(
                "direct network egress state is invalid"
            ) from exc

    def _store_egress_state(
        self,
        resolved: dict[DirectNetworkTcpEgress, tuple[str, ...]],
    ) -> None:
        _write_durably(self.egress_state_path, {
            "version": 1,
            "endpoints": {
                endpoint.endpoint(): list(addresses)
                for endpoint, addresses in sorted(
                    resolved.items(), key=lambda item: item[0].endpoint()
                )
            },
        })

    @staticmethod
    def _iptables_rule_key(command: Sequence[str]) -> tuple[str, ...]:
        words = list(command)
        if words and words[0] == "iptables":
            words.pop(0)
        table = "filter"
        if words[:1] == ["-t"] and len(words) >= 2:
            table = words[1]
            del words[:2]
        if words[:1] in (["-C"], ["-A"]):
            words[0] = "-A"
        # iptables-save makes the implicit TCP matcher explicit. Strip only
        # that redundant module; retain every other predicate verbatim.
        if "-p" in words and words[words.index("-p") + 1:][:1] == ["tcp"]:
            for i in range(len(words) - 1):
                if words[i:i + 2] == ["-m", "tcp"]:
                    del words[i:i + 2]
                    break
        return (table, *words)

    @classmethod
    def _iptables_snapshot(cls) -> set[tuple[str, ...]] | None:
        try:
            result = subprocess.run(
                ("iptables-save",),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode != 0 or not isinstance(result.stdout, str):
            return None
        rules: set[tuple[str, ...]] = set()
        table = None
        try:
            for line in result.stdout.splitlines():
                if line.startswith("*"):
                    if table is not None:
                        return None
                    table = line[1:]
                elif line == "COMMIT":
                    if table is None:
                        return None
                    table = None
                elif line.startswith("-A "):
                    if table is None:
                        return None
                    rules.add(cls._iptables_rule_key(
                        ("-t", table, *shlex.split(line))
                    ))
        except ValueError:
            return None
        return rules if table is None else None

    def _ensure_iptables(
        self,
        check: Sequence[str],
        install: Sequence[str],
        *,
        snapshot: set[tuple[str, ...]] | None = None,
    ) -> bool:
        """Install a missing rule; returns whether it was installed."""
        if snapshot is not None and self._iptables_rule_key(check) in snapshot:
            return False
        if self._command_ok(check):
            return False
        self.runner(tuple(install))
        return True

    def _ensure_kernel_lease(self, lease: DirectNetworkLease) -> None:
        namespace_exists = lease.namespace_path.exists()
        interface_exists = self._interface_present(lease.host_interface)
        if namespace_exists and interface_exists:
            try:
                # Fails without the guest eth0. runsc consumes the external
                # netns wiring at checkpoint, so recreate a partial pair.
                self._configure_kernel_lease(lease)
                return
            except Exception:
                pass
        if namespace_exists or interface_exists:
            self._cleanup_kernel_lease(lease)
        self.namespace_root.mkdir(mode=0o755, parents=True, exist_ok=True)
        try:
            self.runner(("ip", "netns", "add", lease.namespace))
            self.runner(
                (
                    "ip", "link", "add", lease.host_interface, "type", "veth",
                    "peer", "name", "eth0", "netns", lease.namespace,
                )
            )
            self._configure_kernel_lease(lease)
        except Exception:
            self._cleanup_kernel_lease(lease)
            raise

    def _configure_kernel_lease(self, lease: DirectNetworkLease) -> None:
        # ip processes each line synchronously and stops at the first
        # failure. One namespace entry configures the complete guest side.
        self.ip_batch_runner(("ip", "-batch", "-"),
            f"link set dev {lease.host_interface} mtu {NETWORK_MTU} up\n"
            f"address replace {lease.host_ip}/31 dev {lease.host_interface}\n")
        self.ip_batch_runner(("ip", "-n", lease.namespace, "-batch", "-"),
            "link set lo up\n"
            f"link set dev eth0 mtu {NETWORK_MTU} up\n"
            f"address replace {lease.guest_ip}/31 dev eth0\n"
            f"route replace default via {lease.host_ip} dev eth0\n")

    def _split_ip_batch(self, argv: Sequence[str], commands: str) -> None:
        """Give an injected runner each batch line as its own ip command."""
        for line in commands.splitlines():
            self.runner((*argv[:-2], *line.split()))

    def _cleanup_kernel_lease(self, lease: DirectNetworkLease) -> None:
        # Delete the link first: dropping a namespace frees its veth only
        # asynchronously, which could race a recreation of this name.
        self._run_best_effort(("ip", "link", "delete", lease.host_interface))
        self._run_best_effort(("ip", "netns", "delete", lease.namespace))
        pooled = self._pool_lease(lease.slot).namespace_path
        if pooled != lease.namespace_path and pooled.exists():
            # Left by a crash after this lease took the slot from the pool.
            self._detach_namespace(pooled)

    def _pool_lease(self, slot: int) -> DirectNetworkLease:
        name = f"ucloud-pool-{slot}"
        return replace(self._lease("", 0, slot), namespace=name, namespace_path=self.namespace_root / name)

    def _adopt_pooled(self, lease: DirectNetworkLease) -> bool:
        """Name the slot's pooled namespace for ``lease``, then drop the pool's name.

        The pool's name is dropped only once the namespace has another name.
        Otherwise it stays, so cleanup deletes the link before the namespace.
        """
        source = self._pool_lease(lease.slot).namespace_path
        if not source.exists():
            return False
        adopted = not lease.namespace_path.exists()
        if adopted:
            try:
                self._attach_namespace(source, lease.namespace_path)
            except OSError as exc:
                _LOG.warning("could not attach pooled namespace %s: %s", source, exc)
                return False
        elif not os.path.samefile(source, lease.namespace_path):
            return False
        self._detach_namespace(source)
        return adopted

    @staticmethod
    def _attach_namespace(source: Path, target: Path) -> None:
        """Bind ``source``'s namespace at ``target``, as ``ip netns attach`` does."""
        os.close(os.open(target, os.O_RDONLY | os.O_CREAT | os.O_EXCL, 0))
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.mount(os.fsencode(source), os.fsencode(target), None, 4096, None):  # MS_BIND
            code = ctypes.get_errno()
            os.unlink(target)
            raise OSError(code, os.strerror(code), str(target))

    @staticmethod
    def _detach_namespace(path: Path) -> None:
        """Drop one name, as ``ip netns delete``; the namespace lives while named."""
        if ctypes.CDLL(None, use_errno=True).umount2(os.fsencode(path), 2):  # MNT_DETACH
            code = ctypes.get_errno()
            if code not in {errno.EINVAL, errno.ENOENT}:  # unmounted, or gone
                raise OSError(code, os.strerror(code), str(path))
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    @staticmethod
    def _interface_present(name: str) -> bool:
        try:
            socket.if_nametoindex(name)
        except OSError:
            return False
        return True

    def _lease(
        self,
        sandbox_id: str,
        sandbox_generation: int,
        slot: int,
    ) -> DirectNetworkLease:
        if slot < 1 or slot > MAX_NETWORK_SLOTS:
            raise DirectNetworkError("direct network state contains an invalid slot")
        host_ip = NETWORK_CIDR.network_address + (slot * 2)
        guest_ip = host_ip + 1
        digest = hashlib.sha256(
            f"{sandbox_id}\0{sandbox_generation}".encode("utf-8")
        ).hexdigest()[:20]
        namespace = f"ucloud-{digest}"
        return DirectNetworkLease(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            slot=slot,
            namespace=namespace,
            namespace_path=self.namespace_root / namespace,
            host_interface=f"us{slot}h",
            host_ip=str(host_ip),
            guest_ip=str(guest_ip),
        )

    @staticmethod
    def _key(sandbox_id: str, sandbox_generation: int) -> str:
        if not sandbox_id or "\0" in sandbox_id:
            raise ValueError("sandbox id is invalid")
        return f"{sandbox_id}\0{sandbox_generation}"

    def _load(self) -> dict:
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"version": NETWORK_STATE_VERSION, "leases": {}, "pool": []}
        if (
            not isinstance(raw, dict)
            or raw.get("version") != NETWORK_STATE_VERSION
            or not isinstance(raw.get("leases"), dict)
            or any(
                not isinstance(key, str) or not isinstance(value, int)
                for key, value in raw["leases"].items()
            )
        ):
            raise DirectNetworkError("direct network state is invalid")
        policies = raw.get("policies", {})
        if not isinstance(policies, dict) or set(policies) - set(raw["leases"]):
            raise DirectNetworkError("direct network policy state is invalid")
        for policy in policies.values():
            if SandboxNetworkPolicy.from_dict(policy).egress != "relay":
                raise DirectNetworkError("invalid persisted relay policy")
        if len(set(raw["leases"].values())) != len(raw["leases"]):
            raise DirectNetworkError("direct network state double-allocates a slot")
        pool = raw.setdefault("pool", [])
        if not isinstance(pool, list) or len(set(pool)) != len(pool) or any(
            type(slot) is not int or not 1 <= slot <= MAX_NETWORK_SLOTS for slot in pool
        ):
            raise DirectNetworkError("direct network pool state is invalid")
        # Releases before the pool ignore it and may lease a pooled slot.
        raw["pool"] = sorted(set(pool).difference(raw["leases"].values()))
        return raw

    def _store(self, state: dict, *, pool_only: bool = False) -> None:
        # Pool-only writes skip the directory fsync: after an OS crash the name
        # holds a complete later write or the last synced one, with the same
        # leases and policies. Pooled pairs die with the kernel; start rechecks.
        _write_durably(self.state_path, state, sync_directory=not pool_only)

    def _lease_locked(self, key: str):
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        directory = self.lock_path.with_name(self.lock_path.name + ".leases")
        # Retain lock inodes after deletion so existing waiters cannot acquire
        # a different lock from a later operation for the same incarnation.
        return self._locked(directory / (digest + ".lock"))

    @contextmanager
    def _locked(self, path: Path | None = None):
        lock_path = self.lock_path if path is None else path
        lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _run(argv: Sequence[str]) -> None:
        result = subprocess.run(
            tuple(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise DirectNetworkError(
                f"direct network command failed ({result.returncode}): "
                f"{' '.join(argv)}: {detail}"
            )

    @staticmethod
    def _run_ip_batch(argv: Sequence[str], commands: str) -> None:
        result = subprocess.run(tuple(argv), input=commands, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if result.returncode != 0:
            raise DirectNetworkError(f"ip batch failed: {result.stderr.strip()}")

    @staticmethod
    def _command_ok(argv: Sequence[str]) -> bool:
        return subprocess.run(
            tuple(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode == 0

    @staticmethod
    def _run_best_effort(argv: Sequence[str]) -> None:
        subprocess.run(
            tuple(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
