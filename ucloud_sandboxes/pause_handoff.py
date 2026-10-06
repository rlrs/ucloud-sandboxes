"""The agent's half of runtime/noded's pause tier (docs/rust-node-daemon-plan.md, phase 3a).

With ``--rust-pause-tier`` noded owns pause and thaw, idle pauses, local waits,
paused reclaim and the escalation decision; the agent starts none of those
loops. What crosses the process boundary:

- markers stay the truth, and a thaw's flock on its marker is "thaw in
  progress" (pause_tier.thaw_hold); lifecycle fences are exec_fence's T and A;
- noded calls the agent for an escalation (a Python park) and for growth
  bookkeeping (``/internal/v1/pauses/escalate``, ``/internal/v1/growth/events``);
- small files under ``<state_root>/noded/``, written by atomic rename: the
  agent publishes ``agent-demand.json`` (what paused reclaim bills), and the
  heartbeat adds noded's pause counters from its ``status.json``.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any

from . import pause_tier
from .sandbox import OPERATION_ID_RE, SANDBOX_ID_RE

_LOG = logging.getLogger(__name__)
NODED_DIRECTORY = "noded"
AGENT_DEMAND_FILE = "agent-demand.json"
STATUS_FILE = "status.json"
# agent-demand.json: at least this often, and at most this often when
# admission changes keep waking the publisher. noded's reclaim tick runs at 4 Hz.
DEMAND_INTERVAL_SECONDS = 0.25
DEMAND_MIN_INTERVAL_SECONDS = 0.05
# status.json older than this (noded writes it at least every 250 ms) is stale.
STATUS_MAX_AGE_SECONDS = 2.0
GROWTH_ACTIONS = {"wait": "observe_managed_wait", "activate": "resume_managed_continuation"}
MAX_GROWTH_EVENTS = 4096
_SAFE = getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_FLAGS = os.O_RDONLY | _SAFE


def noded_directory(service: Any) -> Path:
    return service.provisioner.registry.path.parent / NODED_DIRECTORY


def write_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Replace ``path`` by rename from a dot temporary; volatile state, no fsync."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _SAFE, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
    os.replace(temporary, path)


class DemandPublisher:
    """agent-demand.json: ``{seq, admission_open, physical_bytes, ram_backing_bytes}``.

    ``seq`` starts at CLOCK_MONOTONIC nanoseconds and grows by one per write,
    so it also grows across agent restarts within one boot. Woken by any
    admission change, at most every 50 ms and at least every 250 ms.
    """

    def __init__(self, service: Any, path: Path) -> None:
        self.service, self.path = service, path
        self._seq = time.monotonic_ns()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._logged = float("-inf")

    def publish(self) -> None:
        admission_open, demand = self.service.reclaim_demand()
        self._seq += 1
        write_atomic(self.path, {"seq": self._seq, "admission_open": admission_open,
                                 "physical_bytes": demand.physical_bytes,
                                 "ram_backing_bytes": demand.ram_backing_bytes})

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="ucloud-agent-demand", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        changed = self.service._admission_changed
        with changed:
            changed.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._thread = None
        self.path.unlink(missing_ok=True)  # Missing is "unknown" to noded at once.

    def _loop(self) -> None:
        changed = self.service._admission_changed
        while not self._stop.is_set():
            try:
                self.publish()
            except Exception:  # noqa: BLE001 - noded treats a stale file as unknown demand
                now = time.monotonic()
                if now - self._logged >= 60.0:
                    self._logged = now
                    _LOG.warning("agent-demand.json not published", exc_info=True)
            if self._stop.wait(DEMAND_MIN_INTERVAL_SECONDS):
                return
            with changed:
                changed.wait(DEMAND_INTERVAL_SECONDS - DEMAND_MIN_INTERVAL_SECONDS)


def _counters(raw: object) -> dict[str, int] | None:
    if not isinstance(raw, dict) or set(raw) != set(pause_tier.PAUSE_STAT_NAMES):
        return None
    if any(type(value) is not int or value < 0 for value in raw.values()):
        return None
    return dict(raw)


def _sum(*parts: dict[str, int]) -> dict[str, int]:
    total = dict.fromkeys(pause_tier.PAUSE_STAT_NAMES, 0)
    for part in parts:
        for name, value in part.items():
            total[name] = max(total[name], value) if name.endswith("_max") else total[name] + value
    return total


class NodedStatus:
    """noded's pause counters from status.json, added to the agent's own.

    status.json must hold ``seq`` (int), ``session`` (noded's session id) and
    ``pause_stats`` with exactly pause_tier.PAUSE_STAT_NAMES as non-negative
    ints counted from 0 in each session; other keys are ignored. A missing,
    unreadable, invalid or stale file (older than STATUS_MAX_AGE_SECONDS, or
    a lower seq in the same session) adds only what earlier valid reads
    established, so the heartbeat's counters never go backwards; a new
    session's counters add to the previous sessions' last values.
    """

    def __init__(self, path: Path, *, clock=time.time) -> None:
        self.path, self._clock = path, clock
        self._guard = threading.Lock()
        self._session: str | None = None
        self._seq = -1
        self._current: dict[str, int] = {}
        self._previous: dict[str, int] = {}
        self._logged = float("-inf")

    def read(self) -> dict[str, Any] | None:
        """The validated status, or None (missing, stale or invalid)."""
        try:
            descriptor = os.open(self.path, _FLAGS)
        except FileNotFoundError:
            return None
        except OSError as exc:
            return self._invalid(f"unreadable: {exc}")
        try:
            with os.fdopen(descriptor, "rb") as stream:
                modified = os.fstat(stream.fileno()).st_mtime
                data = stream.read(1 << 20)
            raw = json.loads(data)
        except (OSError, ValueError) as exc:
            return self._invalid(f"unreadable: {exc}")
        if self._clock() - modified > STATUS_MAX_AGE_SECONDS:
            return self._invalid("stale")
        if not isinstance(raw, dict):
            return self._invalid("not an object")
        seq, session, counters = raw.get("seq"), raw.get("session"), _counters(raw.get("pause_stats"))
        if type(seq) is not int or seq < 0 or not isinstance(session, str) or not 0 < len(session) <= 128:
            return self._invalid("seq or session is invalid")
        if counters is None:
            return self._invalid("pause_stats is invalid")
        return {"seq": seq, "session": session, "pause_stats": counters}

    def _invalid(self, reason: str) -> None:
        now = time.monotonic()
        if now - self._logged >= 60.0:
            self._logged = now
            _LOG.warning("noded status.json ignored (%s): the heartbeat keeps its last valid counters", reason)
        return None

    def compose(self, own: dict[str, int]) -> dict[str, int]:
        """The agent's counters plus noded's: sums, and a max for ``*_max``."""
        status = self.read()
        with self._guard:
            if status is not None:
                if status["session"] != self._session:
                    self._previous = _sum(self._previous, self._current)
                    self._session, self._seq, self._current = status["session"], -1, {}
                if status["seq"] >= self._seq:
                    self._seq, self._current = status["seq"], status["pause_stats"]
                else:
                    self._invalid("seq went back within a session")
            return _sum(own, self._previous, self._current)


def parse_escalation(raw: object) -> tuple[str, int]:
    if not isinstance(raw, dict) or set(raw) != {"sandbox_id", "generation"}:
        raise ValueError("escalation payload must be {sandbox_id, generation}")
    sandbox_id, generation = raw["sandbox_id"], raw["generation"]
    if not isinstance(sandbox_id, str) or not SANDBOX_ID_RE.fullmatch(sandbox_id):
        raise ValueError("escalation sandbox_id is invalid")
    if type(generation) is not int or generation < 1:
        raise ValueError("escalation generation must be a positive integer")
    return sandbox_id, generation


def parse_growth_events(raw: object) -> list[tuple[str, tuple[str, int], str]]:
    """``{"items": [{action, sandbox_id, generation, request_id}]}`` as
    ``record_local_wait_growth`` items, in order."""
    items = raw.get("items") if isinstance(raw, dict) and set(raw) == {"items"} else None
    if not isinstance(items, list) or len(items) > MAX_GROWTH_EVENTS:
        raise ValueError(f"growth events payload must be {{items: [...]}} with at most {MAX_GROWTH_EVENTS}")
    parsed = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"action", "sandbox_id", "generation", "request_id"}:
            raise ValueError("a growth event must be {action, sandbox_id, generation, request_id}")
        action, sandbox_id, generation, request_id = (
            item["action"], item["sandbox_id"], item["generation"], item["request_id"])
        if action not in GROWTH_ACTIONS:
            raise ValueError("a growth event's action must be wait or activate")
        if not isinstance(sandbox_id, str) or not SANDBOX_ID_RE.fullmatch(sandbox_id):
            raise ValueError("a growth event's sandbox_id is invalid")
        if type(generation) is not int or generation < 1:
            raise ValueError("a growth event's generation must be a positive integer")
        if not isinstance(request_id, str) or not OPERATION_ID_RE.fullmatch(request_id):
            raise ValueError("a growth event's request_id is invalid")
        parsed.append((GROWTH_ACTIONS[action], (sandbox_id, generation), request_id))
    return parsed


def pause_config(service: Any, *, rust_pause_enabled: bool) -> dict[str, Any]:
    """The ``pause`` block of GET /internal/v1/creates/config.

    ``rust_pause_enabled``: the agent started without its local waits, idle
    pauses, paused reclaim and paused sampling; noded owns them.
    """
    from .direct_network import NETWORK_CIDR
    from . import local_wait

    config = service.warden.config
    pause, waits = bool(getattr(config, "pause_tier", False)), bool(getattr(config, "local_model_waits", False))
    network = getattr(service.provisioner, "network_manager", None)
    endpoints = local_wait.relay_endpoints(network.relays) if network is not None else ()
    idle = float(service.idle_park_seconds)
    directory = noded_directory(service)
    return {
        "rust_pause_enabled": bool(rust_pause_enabled),
        "pause_tier": pause,
        "runsc": str(config.runsc),
        "runtime_root": str(config.runtime_root),
        "warden_paused_dir": str(config.runtime_root / "warden-paused"),
        "warden_locks_dir": str(config.runtime_root / "warden-locks"),
        "proc_root": str(getattr(config, "proc_root", "/proc")),
        "command_timeout_seconds": getattr(config, "command_timeout_seconds", 60.0),
        "idle_park_seconds": idle,
        # The agent's idle loop period: min(1, max(0.05, idle / 4)).
        "idle_tick_seconds": min(1.0, max(0.05, idle / 4)) if idle > 0 else None,
        "status_path": str(directory / STATUS_FILE),
        "status_max_age_seconds": STATUS_MAX_AGE_SECONDS,
        "agent_demand_path": str(directory / AGENT_DEMAND_FILE),
        "agent_demand_interval_seconds": DEMAND_INTERVAL_SECONDS,
        "prefetch": {
            "threads": pause_tier.PREFETCH_THREADS,
            "node_threads": pause_tier.PREFETCH_NODE_THREADS,
            "piece_bytes": pause_tier.PREFETCH_PIECE_BYTES,
            "read_bytes": pause_tier.PREFETCH_READ_BYTES,
            "max_bytes": pause_tier.PREFETCH_MAX_BYTES,
            "seconds": pause_tier.PREFETCH_SECONDS,
            "min_swap_bytes": pause_tier.PREFETCH_MIN_SWAP_BYTES,
        },
        "reclaim": {
            "tick_seconds": 0.25,
            "forecast_grace_seconds": 1.0,
            "sample_max_age_seconds": 2.5,
            "swappiness": 200,
            "concurrency": pause_tier.RECLAIM_CONCURRENCY,
            "bytes_per_second": pause_tier.RECLAIM_BYTES_PER_SECOND,
            "window_bytes": pause_tier.RECLAIM_WINDOW_BYTES,
            "stall_bytes": pause_tier.STALL_BYTES,
            "stall_backoff_seconds": pause_tier.STALL_BACKOFF_SECONDS,
            "max_stalls": pause_tier.MAX_STALLS,
            "swap_reserve_fraction": pause_tier.SWAP_RESERVE_FRACTION,
            "zswap_share_of_bound": pause_tier.ZSWAP_SHARE_OF_BOUND,
            "escalation_concurrency": pause_tier.ESCALATION_CONCURRENCY,
            "transfer_break_even_seconds": pause_tier.TRANSFER_BREAK_EVEN_SECONDS,
        },
        "local_waits": {
            "enabled": pause and waits and bool(endpoints),
            "endpoints": [{"ip": host, "port": port} for host, port in endpoints],
            "network": str(NETWORK_CIDR),
            "nflog_group": local_wait.NFLOG_GROUP,
            "nft_table": local_wait.TABLE,
            "snaplen": local_wait.SNAPLEN,
            "settle_seconds": local_wait.SETTLE_SECONDS,
            "idle_seconds": local_wait.IDLE_SECONDS,
            "idle_usec": local_wait.IDLE_USEC,
            "answered_watch_seconds": local_wait.ANSWERED_WATCH_SECONDS,
            "tick_seconds": 0.01,
            "refresh_seconds": 1.0,
            "executor_threads": 8,
            "receive_buffer_bytes": 4 * 1024**2,
            "read_timeout_seconds": 0.2,
            "error_log_seconds": 10.0,
        },
    }
