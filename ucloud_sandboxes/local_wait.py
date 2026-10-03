"""Node-local model waits (pause-reclaim item 6; docs/node-local-model-waits.md).

The node sees a sandbox's plaintext HTTP/1.1 calls to a private relay
endpoint (``sandbox.network_relays`` IPv4 literals) at the TCP level. A call is
outstanding when the last relay payload went out; its answer is the next
inbound payload. The node pauses a sandbox whose call is outstanding and whose
cgroup has been idle, and thaws it on the answer's first packet, which a
paused gVisor network stack holds until then
(docs/benchmarks/node-local-wake-2026-10-03). Nothing here reads payloads:
nftables logs only the IP and TCP headers of relay flows to one NFLOG group.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import logging
import socket
import struct
import subprocess
import threading
import time

from .sandbox import SandboxBusyError, SandboxStartupBusyError

_LOG = logging.getLogger(__name__)
TABLE = "ucloud_local_wait"
NFLOG_GROUP = 4207
SNAPLEN = 64  # IPv4 and the first 14 bytes of TCP, with IP options.
SETTLE_SECONDS = 0.05  # A call's request went out at least this long ago,
IDLE_SECONDS = 0.05  # and the sandbox used no more than IDLE_USEC of CPU in this window.
IDLE_USEC = 2000
_EPSILON = 1e-6  # Clock arithmetic: a window of exactly 50 ms is a full window.
# An answered call is watched this long, until the next request: any pause
# that lands on it is undone. A status read in progress re-pauses after it
# (keep_paused), and a thaw queued behind that read can lose to it.
ANSWERED_WATCH_SECONDS = 30.0

# Netlink and nfnetlink_log (linux/netfilter/nfnetlink_log.h).
NETLINK_NETFILTER = 12
NFNL_SUBSYS_ULOG = 4
NFULNL_MSG_PACKET, NFULNL_MSG_CONFIG = 0, 1
NFULA_CFG_CMD, NFULA_CFG_MODE, NFULA_CFG_QTHRESH = 1, 2, 5
NFULNL_CFG_CMD_BIND, NFULNL_COPY_PACKET = 1, 2
NFULA_PAYLOAD = 9
NLM_F_REQUEST, NLM_F_ACK = 1, 4
NLMSG_ERROR = 2
_NLMSG = struct.Struct("=IHHII")
_NFGEN = struct.Struct("=BBH")  # family, version, res_id (big endian)
_NLATTR = struct.Struct("=HH")


def relay_endpoints(relays):
    """(IPv4, port) of each relay with a literal private address: the plaintext path."""
    endpoints = set()
    for relay in relays.values():
        try:
            address = ipaddress.IPv4Address(relay.host)
        except ValueError:
            continue  # A DNS name is a TLS ingress; its waits keep the relay's park.
        if address.is_private:
            endpoints.add((str(address), int(relay.port)))
    return tuple(sorted(endpoints))


def nft_script(endpoints, network, *, group=NFLOG_GROUP):
    """Idempotent rules that log relay flows' headers, both directions, before any filter."""
    lines = [f"add table inet {TABLE}",
             f"add chain inet {TABLE} forward {{ type filter hook forward priority -150; policy accept; }}",
             f"flush chain inet {TABLE} forward"]
    log = f"log group {group} snaplen {SNAPLEN} queue-threshold 1"
    for host, port in endpoints:
        lines.append(f"add rule inet {TABLE} forward ip saddr {network} ip daddr {host} tcp dport {port} {log}")
        lines.append(f"add rule inet {TABLE} forward ip saddr {host} tcp sport {port} ip daddr {network} {log}")
    return "\n".join(lines) + "\n"


def install_rules(endpoints, network, *, run=subprocess.run):
    run(["nft", "-f", "-"], input=nft_script(endpoints, network).encode(), check=True, capture_output=True)


def remove_rules(*, run=subprocess.run):
    run(["nft", "delete", "table", "inet", TABLE], capture_output=True)


@dataclass(frozen=True)
class RelayPacket:
    guest: str
    outbound: bool  # Sandbox to relay.
    payload: int
    flags: int  # TCP flags byte.

    @property
    def wakes(self):
        """An answer's data, FIN or RST: a bare ACK need not thaw anyone."""
        return not self.outbound and (self.payload > 0 or self.flags & 0x05)


def parse_packet(packet, network):
    """A logged IPv4/TCP header as a RelayPacket, or None."""
    if len(packet) < 20 or packet[0] >> 4 != 4 or packet[9] != 6:
        return None
    ihl = (packet[0] & 0x0F) * 4
    if len(packet) < ihl + 14:
        return None
    total = struct.unpack_from("!H", packet, 2)[0]
    source, destination = ipaddress.IPv4Address(packet[12:16]), ipaddress.IPv4Address(packet[16:20])
    payload = max(0, total - ihl - (packet[ihl + 12] >> 4) * 4)
    if source in network:
        return RelayPacket(str(source), True, payload, packet[ihl + 13])
    if destination in network:
        return RelayPacket(str(destination), False, payload, packet[ihl + 13])
    return None


def parse_messages(data):
    """The logged packets (NFULA_PAYLOAD) in one netlink read."""
    packets, offset = [], 0
    while offset + _NLMSG.size <= len(data):
        length, kind, _, _, _ = _NLMSG.unpack_from(data, offset)
        if length < _NLMSG.size or offset + length > len(data):
            break
        if kind == (NFNL_SUBSYS_ULOG << 8) | NFULNL_MSG_PACKET:
            cursor, end = offset + _NLMSG.size + _NFGEN.size, offset + length
            while cursor + _NLATTR.size <= end:
                size, attribute = _NLATTR.unpack_from(data, cursor)
                if size < _NLATTR.size:
                    break
                if attribute & 0x7FFF == NFULA_PAYLOAD:
                    packets.append(bytes(data[cursor + _NLATTR.size:cursor + size]))
                cursor += (size + 3) & ~3
        offset += (length + 3) & ~3
    return packets


def _attribute(kind, value):
    return _NLATTR.pack(_NLATTR.size + len(value), kind) + value + b"\0" * (-len(value) % 4)


def _config(group, *attributes, family=socket.AF_UNSPEC, seq=1):
    body = _NFGEN.pack(family, 0, socket.htons(group)) + b"".join(attributes)
    return _NLMSG.pack(_NLMSG.size + len(body), (NFNL_SUBSYS_ULOG << 8) | NFULNL_MSG_CONFIG,
                       NLM_F_REQUEST | NLM_F_ACK, seq, 0) + body


class NflogReader:
    """One NFLOG group: every logged packet at once (queue threshold 1)."""

    def __init__(self, group=NFLOG_GROUP, *, timeout=0.2):
        self.sock = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, NETLINK_NETFILTER)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 ** 2)
        self.sock.bind((0, 0))
        for seq, attribute in enumerate((
                _attribute(NFULA_CFG_CMD, bytes([NFULNL_CFG_CMD_BIND])),
                _attribute(NFULA_CFG_MODE, struct.pack("!IBB", SNAPLEN, NFULNL_COPY_PACKET, 0)),
                _attribute(NFULA_CFG_QTHRESH, struct.pack("!I", 1))), start=1):
            self.sock.send(_config(group, attribute, seq=seq))
            self._ack(seq)
        self.sock.settimeout(timeout)

    def _ack(self, seq):
        data = self.sock.recv(65536)
        length, kind, _, got, _ = _NLMSG.unpack_from(data)
        if kind == NLMSG_ERROR and got == seq and struct.unpack_from("=i", data, _NLMSG.size)[0] != 0:
            raise OSError(-struct.unpack_from("=i", data, _NLMSG.size)[0], "NFLOG configuration refused")

    def read(self):
        try:
            return parse_messages(self.sock.recv(1 << 20))
        except socket.timeout:
            return []

    def close(self):
        self.sock.close()


@dataclass
class Flow:
    """One sandbox's relay traffic: the newest payload in each direction."""
    last_out: float = 0.0
    last_in: float = 0.0
    cpu: list = field(default_factory=list)  # (monotonic time, usage_usec)
    answered_until: float = 0.0  # Thaw any pause that lands on this answered call until then.

    def outstanding(self, now, settle=SETTLE_SECONDS):
        return self.last_out > self.last_in and now - self.last_out >= settle - _EPSILON

    def idle(self, now, window=IDLE_SECONDS, budget=IDLE_USEC):
        """No more than ``budget`` CPU over at least the last ``window``."""
        return (len(self.cpu) >= 2 and now - self.cpu[0][0] >= window - _EPSILON
                and self.cpu[-1][1] - self.cpu[0][1] <= budget)

    def sample(self, now, usage, window=IDLE_SECONDS):
        """Keep the newest sample at least ``window`` old, and every newer one."""
        self.cpu.append((now, usage))
        while len(self.cpu) > 2 and self.cpu[1][0] <= now - window + _EPSILON:
            self.cpu.pop(0)


def cpu_usage_usec(path):
    with open(path) as stream:
        for line in stream:
            if line.startswith("usage_usec "):
                return int(line.split()[1])
    raise ValueError("cgroup cpu.stat has no usage_usec")


@dataclass(frozen=True)
class WaitCandidate:
    """A sandbox the scheduler may pause: its key, guest address and cgroup cpu.stat."""
    key: tuple  # (sandbox_id, generation)
    guest: str
    cpu_stat: str


class LocalWaitScheduler:
    """Pause a sandbox while its relay call is outstanding; thaw it on the answer.

    ``candidates()`` lists the node's waitable sandboxes, ``pause(key)`` and
    ``thaw(key)`` act through the node's own pause tier, ``is_paused(key)``
    reads its markers. Pauses and thaws run on ``executor``, never on the
    packet thread, and at most one per sandbox at a time.
    """

    def __init__(self, *, endpoints, network, candidates, pause, thaw, is_paused, executor,
                 reader=NflogReader, rules=(install_rules, remove_rules), clock=None, tick=0.01,
                 refresh_seconds=1.0):
        self.endpoints, self.network = tuple(endpoints), ipaddress.IPv4Network(network)
        self._candidates, self._pause, self._thaw, self._is_paused = candidates, pause, thaw, is_paused
        self._executor, self._reader_factory, self._rules = executor, reader, rules
        self._clock, self._tick, self._refresh_seconds = clock or time.monotonic, tick, refresh_seconds
        self.flows, self.waits, self._busy, self._logged = {}, {}, set(), {}
        self._guard, self._stop = threading.Lock(), threading.Event()
        self._threads, self._reader, self._refreshed = [], None, float("-inf")

    def start(self):
        self._rules[0](self.endpoints, self.network)
        self._reader = self._reader_factory()
        for target, name in ((self._read_loop, "ucloud-local-wait-packets"), (self._policy_loop, "ucloud-local-wait")):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        return self

    def stop(self):
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads = []
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        self._rules[1]()

    def _read_loop(self):
        while not self._stop.is_set():
            for raw in self._reader.read():
                packet = parse_packet(raw, self.network)
                if packet is not None:
                    self.observe(packet)

    def observe(self, packet, now=None):
        now = self._clock() if now is None else now
        with self._guard:
            flow = self.flows.setdefault(packet.guest, Flow())
            if packet.outbound:
                if packet.payload:
                    flow.last_out = now
                return
            if not packet.wakes:
                return
            flow.last_in = now
            wait = self.waits.get(packet.guest)
        if wait is None:
            return
        with self._guard:
            flow.answered_until = now + ANSWERED_WATCH_SECONDS
        if self._is_paused(wait.key):
            self._submit(wait.key, self._thaw)

    def answered(self, key, now=None):
        """Its call is answered: hibernating it would strand the answer it holds."""
        now = self._clock() if now is None else now
        with self._guard:
            return any(wait.key == key and flow.last_in >= flow.last_out and flow.answered_until > now
                       for guest, wait in self.waits.items() if (flow := self.flows.get(guest)) is not None)

    def _submit(self, key, action):
        with self._guard:
            if key in self._busy:
                return False
            self._busy.add(key)
        self._executor.submit(self._run, key, action)
        return True

    def _run(self, key, action):
        try:
            action(key)
        except (SandboxBusyError, SandboxStartupBusyError):
            pass  # Exec, file or lifecycle activity holds the sandbox: the next tick decides again.
        except Exception:  # noqa: BLE001 - a deletion or a lost race; the next tick decides again
            now = self._clock()
            if now - self._logged.get(key, float("-inf")) >= 10.0:
                self._logged[key] = now
                _LOG.warning("local wait %s of %s failed", "pause" if action is self._pause else "thaw", key,
                             exc_info=True)
        finally:
            with self._guard:
                self._busy.discard(key)
        if action is self._pause and self._is_paused(key):
            with self._guard:
                guest = next((guest for guest, wait in self.waits.items() if wait.key == key), None)
                flow = self.flows.get(guest)
                answered = flow is not None and flow.last_in >= flow.last_out
            if answered:  # The answer raced the pause: thaw now.
                self._submit(key, self._thaw)

    def _policy_loop(self):
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(self._tick)

    def tick(self, now=None):
        now = self._clock() if now is None else now
        if now - self._refreshed >= self._refresh_seconds:
            waits = {candidate.guest: candidate for candidate in self._candidates()}
            with self._guard:
                self.waits = waits
                for guest in self.flows.keys() - waits.keys():
                    self.flows.pop(guest)
            self._refreshed = now
        with self._guard:
            pending = [(self.waits[guest], flow) for guest, flow in self.flows.items()
                       if guest in self.waits and flow.last_out > flow.last_in]
            answered = [self.waits[guest].key for guest, flow in self.flows.items()
                        if guest in self.waits and flow.last_in >= flow.last_out and flow.answered_until > now]
            for flow in self.flows.values():
                if flow.last_out <= flow.last_in:
                    flow.cpu.clear()
                else:
                    flow.answered_until = 0.0  # The next request went out: that answer was consumed.
        for key in answered:  # Never leave an answered call paused; retried every tick.
            if self._is_paused(key):
                self._submit(key, self._thaw)
        for wait, flow in pending:
            try:
                flow.sample(now, cpu_usage_usec(wait.cpu_stat))
            except (OSError, ValueError):
                continue
            if flow.outstanding(now) and flow.idle(now) and not self._is_paused(wait.key):
                self._submit(wait.key, self._pause)
