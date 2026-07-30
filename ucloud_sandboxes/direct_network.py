from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Callable, Sequence


NETWORK_STATE_VERSION = 1
NETWORK_CIDR = ipaddress.IPv4Network("100.96.0.0/16")
MAX_NETWORK_SLOTS = (NETWORK_CIDR.num_addresses // 2) - 1
# UCloud private/public-link networking uses a 1420-byte path MTU. Leaving the
# veth default at 1500 allows small requests through but black-holes larger TLS
# records when upstream ICMP fragmentation feedback is filtered.
NETWORK_MTU = 1420
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
            address = str(ipaddress.IPv4Address(raw_address))
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError(
                "direct network TCP egress must use the IPv4:port form"
            ) from exc
        if not 1 <= port <= 65535:
            raise ValueError("direct network TCP egress port must be in 1..65535")
        return cls(address=address, port=port)

    def endpoint(self) -> str:
        return f"{self.address}:{self.port}"


class DirectNetworkManager:
    """Crash-durable owner of direct-runtime netns/veth/NAT slots."""

    def __init__(
        self,
        state_path: Path,
        *,
        namespace_root: Path = Path("/run/netns"),
        allowed_tcp_egress: Sequence[str] = (),
        runner: Callable[[Sequence[str]], None] | None = None,
    ) -> None:
        if not state_path.is_absolute() or not namespace_root.is_absolute():
            raise ValueError("direct network paths must be absolute")
        self.state_path = state_path
        self.lock_path = state_path.with_suffix(state_path.suffix + ".lock")
        self.namespace_root = namespace_root
        self.allowed_tcp_egress = tuple(
            dict.fromkeys(
                DirectNetworkTcpEgress.parse(value)
                for value in allowed_tcp_egress
            )
        )
        self.runner = runner or self._run

    def ensure(
        self,
        sandbox_id: str,
        sandbox_generation: int,
        *,
        avoid_guest_ips: Sequence[str] = (),
    ) -> DirectNetworkLease:
        if sandbox_generation < 0:
            raise ValueError("sandbox generation cannot be negative")
        avoided = {
            str(ipaddress.IPv4Address(item))
            for item in avoid_guest_ips
        }
        if any(ipaddress.IPv4Address(item) not in NETWORK_CIDR for item in avoided):
            raise ValueError("avoided guest IP is outside the direct network")
        key = self._key(sandbox_id, sandbox_generation)
        with self._locked():
            state = self._load()
            slot = state["leases"].get(key)
            if slot is None:
                used = {int(item) for item in state["leases"].values()}
                slot = next(
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
                state["leases"][key] = slot
                self._store(state)
            lease = self._lease(sandbox_id, sandbox_generation, int(slot))
            if lease.guest_ip in avoided:
                raise DirectNetworkError(
                    "existing direct network lease reuses a forbidden guest IP"
                )
            self._ensure_host_rules()
            self._ensure_kernel_lease(lease)
            return lease

    def release(self, sandbox_id: str, sandbox_generation: int) -> None:
        key = self._key(sandbox_id, sandbox_generation)
        with self._locked():
            state = self._load()
            raw_slot = state["leases"].get(key)
            if raw_slot is None:
                return
            lease = self._lease(sandbox_id, sandbox_generation, int(raw_slot))
            self._cleanup_kernel_lease(lease)
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

    def _ensure_host_rules(self) -> None:
        self.runner(("sysctl", "-q", "-w", "net.ipv4.ip_forward=1"))
        self._ensure_iptables(
            ("iptables", "-C", "INPUT", "-s", str(NETWORK_CIDR), "-j", "DROP"),
            ("iptables", "-I", "INPUT", "1", "-s", str(NETWORK_CIDR), "-j", "DROP"),
        )
        for destination in DENIED_DESTINATIONS:
            self._ensure_iptables(
                (
                    "iptables", "-C", "FORWARD", "-s", str(NETWORK_CIDR),
                    "-d", destination, "-j", "DROP",
                ),
                (
                    "iptables", "-I", "FORWARD", "1", "-s", str(NETWORK_CIDR),
                    "-d", destination, "-j", "DROP",
                ),
            )
        # These rules are installed after the private-destination denies with
        # -I, so exact service exceptions sit above the broad denies. The
        # deployment supplies only node-local infrastructure endpoints (for
        # example the model relay); sandboxes do not receive general RFC1918
        # access.
        for endpoint in self.allowed_tcp_egress:
            rule = (
                "-s", str(NETWORK_CIDR),
                "-d", f"{endpoint.address}/32",
                "-p", "tcp",
                "--dport", str(endpoint.port),
                "-j", "ACCEPT",
            )
            self._ensure_iptables(
                ("iptables", "-C", "FORWARD", *rule),
                ("iptables", "-I", "FORWARD", "1", *rule),
            )
        self._ensure_iptables(
            ("iptables", "-C", "FORWARD", "-s", str(NETWORK_CIDR), "-j", "ACCEPT"),
            ("iptables", "-A", "FORWARD", "-s", str(NETWORK_CIDR), "-j", "ACCEPT"),
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
        )

    def _ensure_iptables(
        self,
        check: Sequence[str],
        install: Sequence[str],
    ) -> None:
        result = subprocess.run(
            tuple(check),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode != 0:
            self.runner(tuple(install))

    def _ensure_kernel_lease(self, lease: DirectNetworkLease) -> None:
        namespace_exists = lease.namespace_path.exists()
        interface_exists = self._command_ok(
            ("ip", "link", "show", "dev", lease.host_interface)
        )
        guest_interface_exists = namespace_exists and self._command_ok(
            (
                "ip", "netns", "exec", lease.namespace,
                "ip", "link", "show", "dev", "eth0",
            )
        )
        if namespace_exists and interface_exists and guest_interface_exists:
            try:
                self._configure_kernel_lease(lease)
                return
            except Exception:
                # runsc consumes the external netns wiring when a sandbox is
                # checkpointed. Treat any partially reusable lease as broken
                # and recreate the veth pair before restore.
                self._cleanup_kernel_lease(lease)
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
        self._ensure_lease_mtu(lease)
        self.runner(
            (
                "ip", "address", "replace", f"{lease.host_ip}/31",
                "dev", lease.host_interface,
            )
        )
        self.runner(("ip", "link", "set", lease.host_interface, "up"))
        self.runner(
            (
                "ip", "netns", "exec", lease.namespace,
                "ip", "link", "set", "lo", "up",
            )
        )
        self.runner(
            (
                "ip", "netns", "exec", lease.namespace, "ip", "address",
                "replace", f"{lease.guest_ip}/31", "dev", "eth0",
            )
        )
        self.runner(
            (
                "ip", "netns", "exec", lease.namespace,
                "ip", "link", "set", "eth0", "up",
            )
        )
        self.runner(
            (
                "ip", "netns", "exec", lease.namespace, "ip", "route",
                "replace", "default", "via", lease.host_ip, "dev", "eth0",
            )
        )

    def _ensure_lease_mtu(self, lease: DirectNetworkLease) -> None:
        self.runner(
            (
                "ip", "link", "set", "dev", lease.host_interface,
                "mtu", str(NETWORK_MTU),
            )
        )
        self.runner(
            (
                "ip", "netns", "exec", lease.namespace, "ip", "link", "set",
                "dev", "eth0", "mtu", str(NETWORK_MTU),
            )
        )

    def _cleanup_kernel_lease(self, lease: DirectNetworkLease) -> None:
        self._run_best_effort(("ip", "link", "delete", lease.host_interface))
        self._run_best_effort(("ip", "netns", "delete", lease.namespace))

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
            return {"version": NETWORK_STATE_VERSION, "leases": {}}
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
        if len(set(raw["leases"].values())) != len(raw["leases"]):
            raise DirectNetworkError("direct network state double-allocates a slot")
        return raw

    def _store(self, state: dict) -> None:
        self.state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.",
            dir=self.state_path.parent,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(state, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.state_path)
            directory = os.open(self.state_path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def _locked(self):
        manager = self

        class Lock:
            def __enter__(self):
                manager.lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                self.handle = manager.lock_path.open("a+b")
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
                return self

            def __exit__(self, *_args):
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                self.handle.close()

        return Lock()

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
