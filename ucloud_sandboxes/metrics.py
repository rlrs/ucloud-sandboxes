from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import sqlite3
from threading import RLock
import time
from typing import Any
from uuid import uuid4

from .deployment import agent_version_is_schedulable
from .models import (
    LiveScaleSignals,
    NodeHeartbeat,
    ResourceQuantity,
    ScalePolicy,
    parse_iso_datetime,
    utc_now,
)
from .routing import (
    PendingSandboxDemand,
    ProgramRequestState,
    RoutingState,
    SandboxRoute,
)


DEFAULT_RECENT_EVENT_LIMIT = 50
DEFAULT_SCALE_UP_SAMPLE_LIMIT = 200
DEFAULT_VM_LIFECYCLE_LIMIT = 100
DEFAULT_TRACE_SPAN_LIMIT = 250
DEFAULT_TRACE_LIMIT = 50
DEFAULT_PROGRAM_WAKE_PLAN_SAMPLE_LIMIT = 100
DEFAULT_METRICS_MAX_BYTES = 64 * 1024**2
DEFAULT_METRICS_MAX_FILES = 5
DEFAULT_METRICS_MAX_EVENT_BYTES = 1024**2
DEFAULT_METRICS_MAX_EVENTS = 100_000
_METRICS_LOCKS_GUARD = RLock()
_METRICS_LOCKS: dict[Path, RLock] = {}


@dataclass(frozen=True)
class MetricEvent:
    timestamp: str
    kind: str
    data: dict[str, Any]

    @classmethod
    def from_dict(cls, raw: object) -> "MetricEvent | None":
        if not isinstance(raw, dict):
            return None
        timestamp = raw.get("timestamp")
        kind = raw.get("kind")
        data = raw.get("data")
        if not isinstance(timestamp, str) or not timestamp:
            return None
        if not isinstance(kind, str) or not kind:
            return None
        if not isinstance(data, dict):
            data = {}
        return cls(
            timestamp=timestamp,
            kind=kind,
            data={str(key): value for key, value in data.items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "kind": self.kind,
            "data": self.data,
        }


class MetricsStore:
    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = DEFAULT_METRICS_MAX_BYTES,
        max_files: int = DEFAULT_METRICS_MAX_FILES,
        max_event_bytes: int = DEFAULT_METRICS_MAX_EVENT_BYTES,
        max_events: int = DEFAULT_METRICS_MAX_EVENTS,
    ) -> None:
        self.path = path
        self._lock = _metrics_lock(path)
        self._max_bytes = max(1, max_bytes)
        self._max_files = max(1, max_files)
        self._max_event_bytes = max(1, min(max_event_bytes, self._max_bytes))
        self._max_events = max(1, max_events)
        self._sqlite = self.path.suffix.lower() != ".jsonl"
        self._sqlite_connection: sqlite3.Connection | None = None
        self._sqlite_pid = 0
        self._dropped_sqlite_events = 0
        if self._sqlite:
            with self._lock:
                self._sqlite_connect_locked()

    def append(
        self,
        kind: str,
        data: dict[str, Any] | None = None,
        *,
        timestamp: str | None = None,
    ) -> MetricEvent:
        event = MetricEvent(
            timestamp=timestamp or utc_now().isoformat(),
            kind=kind,
            data=data or {},
        )
        line = (json.dumps(event.to_dict(), sort_keys=True) + "\n").encode("utf-8")
        if len(line) > self._max_event_bytes:
            event = MetricEvent(
                timestamp=event.timestamp,
                kind=event.kind,
                data={
                    "metrics_payload_truncated": True,
                    "original_bytes": len(line),
                },
            )
            line = (json.dumps(event.to_dict(), sort_keys=True) + "\n").encode(
                "utf-8"
            )
        if self._sqlite:
            with self._lock:
                connection = self._sqlite_connect_locked()
                try:
                    if self._dropped_sqlite_events:
                        connection.execute(
                            """
                            INSERT INTO metric_events(
                                timestamp, timestamp_epoch, kind, data_json
                            )
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                event.timestamp,
                                _timestamp_epoch(event.timestamp),
                                "metrics_dropped_events",
                                json.dumps(
                                    {
                                        "count": self._dropped_sqlite_events,
                                        "reason": "sqlite_busy",
                                    },
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                            ),
                        )
                    cursor = connection.execute(
                        """
                        INSERT INTO metric_events(
                            timestamp, timestamp_epoch, kind, data_json
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            event.timestamp,
                            _timestamp_epoch(event.timestamp),
                            event.kind,
                            json.dumps(
                                event.data,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        ),
                    )
                    sequence = int(cursor.lastrowid or 0)
                    if sequence > 0 and sequence % 512 == 0:
                        self._prune_sqlite_locked(connection)
                    connection.commit()
                    self._dropped_sqlite_events = 0
                except sqlite3.OperationalError:
                    connection.rollback()
                    # Metrics are observational. A brief writer collision must
                    # never fail a heartbeat, gateway request, or autoscaler
                    # cycle. The next successful transaction records the loss.
                    self._dropped_sqlite_events += 1
            return event
        with _metrics_file_lock(self.path, self._lock):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._rotate_if_needed(len(line))
            fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                os.fchmod(fd, 0o600)
                view = memoryview(line)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise OSError("failed to append metrics event")
                    view = view[written:]
            finally:
                os.close(fd)
        return event

    def load_events(
        self,
        *,
        max_events: int = 1000,
        kinds: tuple[str, ...] = (),
        since_seconds: int | None = None,
    ) -> list[MetricEvent]:
        if self._sqlite:
            return self._load_sqlite_events(
                max_events=max_events,
                kinds=kinds,
                since_seconds=since_seconds,
            )
        result_limit = max_events
        if (kinds or since_seconds is not None) and max_events > 0:
            # JSONL is rollback compatibility only. Scan a bounded superset so
            # callers still get useful filtered results without restoring the
            # old whole-file dashboard behavior.
            max_events = max(1000, max_events * 20)
        # Metrics are observational and may tolerate an incomplete final line.
        # Avoid taking the writer lock while reading: on network-backed state
        # directories a dashboard read can otherwise block every request trace
        # append for seconds and turn overload telemetry into a lock convoy.
        if max_events > 0:
            remaining = max_events
            newest_first = [self.path] + [
                self._rotated_path(index)
                for index in range(1, self._max_files + 1)
            ]
            chunks: list[list[str]] = []
            for path in newest_first:
                if remaining <= 0:
                    break
                try:
                    chunk = _read_recent_lines(path, remaining)
                except OSError:
                    # Rotation can rename a segment between discovery and open.
                    continue
                chunks.append(chunk)
                remaining -= len(chunk)
            lines = [line for chunk in reversed(chunks) for line in chunk]
        else:
            oldest_first = [
                self._rotated_path(index)
                for index in range(self._max_files, 0, -1)
            ] + [self.path]
            lines = []
            for path in oldest_first:
                try:
                    lines.extend(_read_recent_lines(path, max_events))
                except OSError:
                    continue
        events: list[MetricEvent] = []
        for line in lines:
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = MetricEvent.from_dict(parsed)
            if event is not None:
                events.append(event)
        normalized_kinds = {kind for kind in kinds if kind}
        if normalized_kinds:
            events = [event for event in events if event.kind in normalized_kinds]
        if since_seconds is not None:
            cutoff = time.time() - max(0, since_seconds)
            events = [
                event
                for event in events
                if _timestamp_epoch(event.timestamp) >= cutoff
            ]
        if result_limit <= 0:
            return events
        return events[-result_limit:]

    def _sqlite_connect_locked(self) -> sqlite3.Connection:
        pid = os.getpid()
        if self._sqlite_connection is not None and self._sqlite_pid == pid:
            return self._sqlite_connection
        if self._sqlite_connection is not None:
            self._sqlite_connection.close()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=5.0,
            check_same_thread=False,
        )
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=1000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS metric_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                timestamp_epoch REAL NOT NULL,
                kind TEXT NOT NULL,
                data_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS metric_events_kind_sequence
            ON metric_events(kind, sequence DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS metric_events_timestamp
            ON metric_events(timestamp_epoch)
            """
        )
        connection.commit()
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            connection.close()
            raise
        self._sqlite_connection = connection
        self._sqlite_pid = pid
        return connection

    def _load_sqlite_events(
        self,
        *,
        max_events: int,
        kinds: tuple[str, ...],
        since_seconds: int | None,
    ) -> list[MetricEvent]:
        clauses: list[str] = []
        parameters: list[Any] = []
        normalized_kinds = tuple(dict.fromkeys(kind for kind in kinds if kind))
        if normalized_kinds:
            placeholders = ",".join("?" for _ in normalized_kinds)
            clauses.append(f"kind IN ({placeholders})")
            parameters.extend(normalized_kinds)
        if since_seconds is not None:
            clauses.append("timestamp_epoch >= ?")
            parameters.append(time.time() - max(0, since_seconds))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = ""
        if max_events > 0:
            limit = "LIMIT ?"
            parameters.append(max_events)
        query = (
            "SELECT timestamp, kind, data_json FROM metric_events "
            f"{where} ORDER BY sequence DESC {limit}"
        )
        with self._lock:
            rows = self._sqlite_connect_locked().execute(query, parameters).fetchall()
        events: list[MetricEvent] = []
        for timestamp, kind, data_json in reversed(rows):
            try:
                data = json.loads(str(data_json))
            except (TypeError, json.JSONDecodeError):
                continue
            event = MetricEvent.from_dict(
                {"timestamp": timestamp, "kind": kind, "data": data}
            )
            if event is not None:
                events.append(event)
        return events

    def _prune_sqlite_locked(self, connection: sqlite3.Connection) -> None:
        cutoff = connection.execute(
            """
            SELECT sequence FROM metric_events
            ORDER BY sequence DESC
            LIMIT 1 OFFSET ?
            """,
            (self._max_events - 1,),
        ).fetchone()
        if cutoff is not None:
            connection.execute(
                "DELETE FROM metric_events WHERE sequence < ?",
                (int(cutoff[0]),),
            )

    def _rotate_if_needed(self, additional_bytes: int) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        if self.path.stat().st_size + additional_bytes <= self._max_bytes:
            return
        oldest = self._rotated_path(self._max_files)
        try:
            oldest.unlink()
        except FileNotFoundError:
            pass
        for index in range(self._max_files - 1, 0, -1):
            source = self._rotated_path(index)
            if source.exists():
                source.replace(self._rotated_path(index + 1))
        self.path.replace(self._rotated_path(1))

    def _rotated_path(self, index: int) -> Path:
        return self.path.with_name(f"{self.path.name}.{index}")


def _timestamp_epoch(value: str) -> float:
    parsed = parse_iso_datetime(value)
    return parsed.timestamp() if parsed is not None else time.time()


@contextmanager
def _metrics_file_lock(path: Path, local_lock: RLock) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    with local_lock:
        lock_path = path.with_name(path.name + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_recent_lines(
    path: Path,
    max_lines: int,
    *,
    chunk_size: int = 256 * 1024,
) -> list[str]:
    if max_lines <= 0:
        return path.read_text(encoding="utf-8").splitlines()

    chunks: list[bytes] = []
    newline_count = 0
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        while position > 0 and newline_count <= max_lines:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")

    if not chunks:
        return []
    data = b"".join(reversed(chunks))
    return data.decode("utf-8", errors="replace").splitlines()[-max_lines:]


@dataclass
class ActiveTraceSpan:
    trace_id: str
    span_id: str
    parent_span_id: str
    name: str
    started_at: str
    monotonic_started_at: float
    attributes: dict[str, Any]
    status: str = "ok"

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_error(self, error: BaseException | str) -> None:
        self.status = "error"
        self.attributes["error"] = str(error)


class GatewayBusyTraceSampler:
    """Aggregate hot-path gateway admission failures into bounded telemetry."""

    def __init__(
        self,
        store: MetricsStore | None,
        *,
        min_interval_seconds: float = 1.0,
    ) -> None:
        self.store = store
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._lock = RLock()
        self._last_emit_monotonic: float | None = None
        self._pending_rejections = 0

    def record(self, *, trace_id: str, max_concurrent_sandbox_creates: int) -> bool:
        if self.store is None:
            return False
        now_monotonic = time.monotonic()
        with self._lock:
            self._pending_rejections += 1
            elapsed = (
                None
                if self._last_emit_monotonic is None
                else now_monotonic - self._last_emit_monotonic
            )
            if elapsed is not None and elapsed < self.min_interval_seconds:
                return False
            rejected_requests = self._pending_rejections
            self._pending_rejections = 0
            self._last_emit_monotonic = now_monotonic

        finished_at = utc_now().isoformat()
        record_trace_span(
            self.store,
            trace_id=trace_id,
            span_id=uuid4().hex[:16],
            name="gateway.sandbox_create",
            started_at=finished_at,
            finished_at=finished_at,
            duration_ms=0,
            status="error",
            attributes={
                "outcome": "gateway_busy",
                "max_concurrent_sandbox_creates": max_concurrent_sandbox_creates,
                "aggregated_rejections": rejected_requests,
                "sample_interval_seconds": self.min_interval_seconds,
            },
        )
        return True


@contextmanager
def trace_span(
    store: MetricsStore | None,
    trace_id: str,
    name: str,
    *,
    parent_span_id: str = "",
    attributes: dict[str, Any] | None = None,
) -> Any:
    span = ActiveTraceSpan(
        trace_id=trace_id,
        span_id=uuid4().hex[:16],
        parent_span_id=parent_span_id,
        name=name,
        started_at=utc_now().isoformat(),
        monotonic_started_at=time.monotonic(),
        attributes=dict(attributes or {}),
    )
    try:
        yield span
    except Exception as exc:
        span.set_error(exc)
        raise
    finally:
        finished_at = utc_now().isoformat()
        duration_ms = max(0, int((time.monotonic() - span.monotonic_started_at) * 1000))
        record_trace_span(
            store,
            trace_id=span.trace_id,
            span_id=span.span_id,
            parent_span_id=span.parent_span_id,
            name=span.name,
            started_at=span.started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            status=span.status,
            attributes=span.attributes,
        )


def record_trace_span(
    store: MetricsStore | None,
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str = "",
    name: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    status: str = "ok",
    attributes: dict[str, Any] | None = None,
) -> None:
    if store is None:
        return
    store.append(
        "trace_span",
        {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "name": name,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "status": status,
            "attributes": attributes or {},
        },
        timestamp=finished_at,
    )


def record_sandbox_scheduled(
    store: MetricsStore | None,
    *,
    sandbox_id: str,
    route: SandboxRoute,
    resources: ResourceQuantity,
    pending: PendingSandboxDemand | None,
) -> None:
    if store is None:
        return
    now = utc_now()
    pending_created_at = parse_iso_datetime(pending.created_at) if pending else None
    wait_ms = (
        max(0, int((now - pending_created_at).total_seconds() * 1000))
        if pending_created_at is not None
        else None
    )
    store.append(
        "sandbox_scheduled",
        {
            "sandbox_id": sandbox_id,
            "node_id": route.node_id,
            "job_id": route.job_id,
            "resources": resources.to_dict(),
            "had_pending_demand": pending is not None,
            "pending_attempts": pending.attempts if pending is not None else 0,
            "scale_up_wait_ms": wait_ms,
        },
    )


def record_sandbox_pending_deleted(
    store: MetricsStore | None,
    *,
    sandbox_id: str,
    pending: PendingSandboxDemand | None,
) -> None:
    if store is None or pending is None:
        return
    created_at = parse_iso_datetime(pending.created_at)
    age_ms = (
        max(0, int((utc_now() - created_at).total_seconds() * 1000))
        if created_at is not None
        else None
    )
    store.append(
        "sandbox_pending_deleted",
        {
            "sandbox_id": sandbox_id,
            "resources": pending.resources.to_dict(),
            "pending_attempts": pending.attempts,
            "pending_age_ms": age_ms,
        },
    )


def record_autoscaler_cycle(
    store: MetricsStore | None,
    *,
    cycle: int,
    result: dict[str, Any],
) -> None:
    if store is None:
        return
    decision = (
        result.get("decision") if isinstance(result.get("decision"), dict) else {}
    )
    builder_decision = (
        result.get("builderDecision")
        if isinstance(result.get("builderDecision"), dict)
        else {}
    )
    store.append(
        "autoscaler_cycle",
        {
            "cycle": cycle,
            "pending_resources": decision.get("pendingResources", {}),
            "suppressed_pending_resources": decision.get(
                "suppressedPendingResources", {}
            ),
            "pending_count": decision.get("pendingCount", 0),
            "suppressed_pending_count": decision.get(
                "suppressedPendingCount", 0
            ),
            "prepared_resources": decision.get("preparedResources", {}),
            "desired_resources": decision.get("desiredResources", {}),
            "projected_free_resources": decision.get("projectedFreeResources", {}),
            "resource_deficit": decision.get("resourceDeficit", {}),
            "live_signals": decision.get("liveSignals"),
            "program_signals": decision.get("programSignals"),
            "program_wake_plan": _bounded_program_wake_plan(
                result.get("programWakePlan")
            ),
            "effective_policy": result.get("effectivePolicy", {}),
            "pressure_scale_up": bool(decision.get("pressureScaleUp")),
            "create_pressure_scale_up": bool(
                decision.get("createPressureScaleUp")
            ),
            "effective_scale_down_idle_seconds": decision.get(
                "effectiveScaleDownIdleSeconds"
            ),
            "ready_nodes": decision.get("readyNodes", 0),
            "provisioning_nodes": decision.get("provisioningNodes", 0),
            "total_nodes": decision.get("totalNodes", 0),
            "actions": decision.get("actions", []),
            "reasons": decision.get("reasons", []),
            "created_job_ids": result.get("createdJobIds", []),
            "stop_job_ids": result.get("stopJobIds", []),
            "pending_image_builds": result.get("pendingImageBuilds", 0),
            "active_image_builds": result.get("activeImageBuilds", 0),
            "prepared_builder_count": result.get("preparedBuilderCount", 0),
            "build_warm_sandbox_resources": result.get(
                "buildWarmSandboxResources",
                {},
            ),
            "builder_actions": builder_decision.get("actions", []),
            "builder_reasons": builder_decision.get("reasons", []),
        },
    )


def _bounded_program_wake_plan(value: Any) -> dict[str, Any]:
    """Keep autoscaler telemetry useful without persisting an unbounded plan."""

    if not isinstance(value, dict):
        return {}
    placements = (
        value.get("placements")
        if isinstance(value.get("placements"), list)
        else []
    )
    unplaced = (
        value.get("unplaced") if isinstance(value.get("unplaced"), list) else []
    )
    limit = DEFAULT_PROGRAM_WAKE_PLAN_SAMPLE_LIMIT
    return {
        "mode": str(value.get("mode") or "shadow"),
        "queued": max(0, int(value.get("queued") or 0)),
        "placed": max(0, int(value.get("placed") or 0)),
        "unplaced_count": max(0, int(value.get("unplaced_count") or 0)),
        "placements": placements[:limit],
        "unplaced": unplaced[:limit],
        "placements_truncated": max(0, len(placements) - limit),
        "unplaced_truncated": max(0, len(unplaced) - limit),
    }


def record_vm_submitted(
    store: MetricsStore | None,
    *,
    cycle: int,
    job_id: str,
    intent: Any,
) -> None:
    if store is None:
        return
    options = getattr(intent, "options", None)
    labels = getattr(options, "labels", None) or {}
    role = "builder" if labels.get("ucloud-sandboxes/builder") == "true" else "sandbox"
    product = getattr(options, "product", None)
    application = getattr(options, "application", None)
    store.append(
        "vm_submitted",
        {
            "cycle": cycle,
            "job_id": job_id,
            "role": role,
            "node_id": getattr(intent, "node_id", ""),
            "node_url": getattr(intent, "node_url", ""),
            "name": getattr(options, "name", ""),
            "hostname": getattr(options, "hostname", ""),
            "product_id": getattr(product, "id", ""),
            "product_category": getattr(product, "category", ""),
            "application_name": getattr(application, "name", ""),
            "application_version": getattr(application, "version", ""),
            "disk_gb": getattr(options, "disk_gb", None),
        },
    )


def record_vm_observed(
    store: MetricsStore | None,
    *,
    cycle: int,
    node: Any,
) -> None:
    if store is None:
        return
    job = getattr(node, "job", None)
    if job is None:
        return
    store.append(
        "vm_observed",
        {
            "cycle": cycle,
            "job_id": getattr(job, "id", ""),
            "role": _node_role(node),
            "state": getattr(job, "state", ""),
            "name": getattr(job, "name", ""),
            "hostname": getattr(job, "hostname", "") or "",
            "created_at": _iso_or_none(getattr(job, "created_at", None)),
            "started_at": _iso_or_none(getattr(job, "started_at", None)),
            "expires_at": _iso_or_none(getattr(job, "expires_at", None)),
            "latest_note": getattr(job, "latest_note", "") or "",
            "queue_status": getattr(job, "queue_status", "") or "",
            "product_id": getattr(job, "product_id", ""),
            "cpu": getattr(job, "cpu", None),
            "memory_gb": getattr(job, "memory_gb", None),
            "disk_gb": getattr(job, "disk_gb", None),
            "ready": bool(getattr(node, "is_ready", False)),
            "provisioning": bool(getattr(node, "is_provisioning", False)),
            "heartbeat_fresh": bool(getattr(node, "heartbeat_fresh", False)),
        },
    )


def record_vm_init_attempt(
    store: MetricsStore | None,
    *,
    job_id: str,
    node_id: str,
    role: str,
    status: str,
    attempts: int,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    stage_duration_ms: int | None = None,
    run_duration_ms: int | None = None,
    returncode: int | None = None,
    error: str = "",
    skipped: bool = False,
    reason: str = "",
    retry_delay_seconds: int | None = None,
    init_phases_ms: dict[str, int] | None = None,
    init_total_ms: int | None = None,
) -> None:
    if store is None:
        return
    store.append(
        "vm_init_attempt",
        {
            "job_id": job_id,
            "node_id": node_id,
            "role": role,
            "status": status,
            "attempts": attempts,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "stage_duration_ms": stage_duration_ms,
            "run_duration_ms": run_duration_ms,
            "returncode": returncode,
            "error": error,
            "skipped": skipped,
            "reason": reason,
            "retry_delay_seconds": retry_delay_seconds,
            "init_phases_ms": init_phases_ms or {},
            "init_total_ms": init_total_ms,
        },
    )


def record_node_heartbeat(
    store: MetricsStore | None,
    heartbeat: NodeHeartbeat,
    *,
    first: bool = False,
) -> None:
    if store is None:
        return
    effective = heartbeat.effective_resources
    used = heartbeat.used_resources
    store.append(
        "node_heartbeat",
        {
            "node_id": heartbeat.node_id,
            "job_id": heartbeat.job_id,
            "node_url": heartbeat.node_url or "",
            "active_sandboxes": heartbeat.active_sandboxes,
            "active_workloads": heartbeat.active_workloads,
            "active_sandbox_creates": heartbeat.active_sandbox_creates,
            "draining": heartbeat.draining,
            "capabilities": list(heartbeat.capabilities),
            "agent_version": heartbeat.agent_version,
            "deployment_id": heartbeat.deployment_id,
            "init_version": heartbeat.init_version,
            "total_resources": heartbeat.total_resources.to_dict(),
            "effective_resources": effective.to_dict(),
            "used_resources": used.to_dict(),
            "free_resources": heartbeat.free_resources.to_dict(),
            "load": _resource_load(used, effective),
            "actual_usage": (
                heartbeat.runtime_metrics.to_dict()
                if heartbeat.runtime_metrics is not None
                else None
            ),
            "idle_since": heartbeat.idle_since.isoformat()
            if heartbeat.idle_since
            else None,
            "heartbeat_updated_at": heartbeat.updated_at.isoformat(),
        },
    )
    if first:
        store.append(
            "node_first_heartbeat",
            {
                "node_id": heartbeat.node_id,
                "job_id": heartbeat.job_id,
                "heartbeat_updated_at": heartbeat.updated_at.isoformat(),
            },
        )


def build_live_scale_signals(
    events: list[MetricEvent],
    policy: ScalePolicy,
) -> LiveScaleSignals:
    """Reduce recent observations into a deliberately small feedback surface."""

    now = utc_now()
    pressure_cutoff = now.timestamp() - max(1, policy.live_pressure_window_seconds)
    pressure_samples = 0
    latest_pressure_epoch: float | None = None
    latest_cpu: float | None = None
    latest_memory: float | None = None
    latest_psi: float | None = None
    latest_storage_queue: float | None = None
    latest_rootfs_export_queue: float | None = None
    create_cutoff = now.timestamp() - max(
        1, policy.create_pressure_window_seconds
    )
    create_pressure_samples = 0
    latest_create_pressure_epoch: float | None = None
    sandbox_create_rejections = 0
    sandbox_create_limit = 0

    for event in events:
        event_epoch = _timestamp_epoch(event.timestamp)
        if event.kind == "trace_span" and event_epoch >= create_cutoff:
            data = event.data
            attributes = data.get("attributes")
            if (
                data.get("name") == "gateway.sandbox_create"
                and isinstance(attributes, dict)
                and attributes.get("outcome") == "gateway_busy"
            ):
                create_pressure_samples += 1
                sandbox_create_rejections += max(
                    0,
                    _optional_int(attributes.get("aggregated_rejections")) or 0,
                )
                if (
                    latest_create_pressure_epoch is None
                    or event_epoch >= latest_create_pressure_epoch
                ):
                    latest_create_pressure_epoch = event_epoch
                    sandbox_create_limit = max(
                        0,
                        _optional_int(
                            attributes.get("max_concurrent_sandbox_creates")
                        )
                        or 0,
                    )
            continue
        if event.kind != "node_heartbeat":
            continue
        if event_epoch < pressure_cutoff:
            continue
        data = event.data
        actual = data.get("actual_usage")
        if not isinstance(actual, dict):
            continue
        active_workloads = _optional_int(data.get("active_workloads"))
        if active_workloads is None:
            active_workloads = _optional_int(data.get("active_sandboxes"))
        storage_active = _optional_int(actual.get("storage_active_operations")) or 0
        storage_waiting = _optional_int(actual.get("storage_waiting_operations")) or 0
        rootfs_active = (
            _optional_int(actual.get("rootfs_export_active_operations")) or 0
        )
        rootfs_waiting = (
            _optional_int(actual.get("rootfs_export_waiting_operations")) or 0
        )
        if (
            (active_workloads or 0) <= 0
            and storage_active + storage_waiting <= 0
            and rootfs_active + rootfs_waiting <= 0
        ):
            continue

        cpu = _fraction_from_percent(actual.get("cpu_percent"))
        memory = _fraction_from_percent(actual.get("memory_percent"))
        psi = _optional_float(actual.get("memory_psi_full_avg10"))
        storage_limit = (
            _optional_int(actual.get("storage_max_concurrent_operations")) or 0
        )
        storage_queue = (
            min(1.0, (storage_active + storage_waiting) / storage_limit)
            if storage_limit > 0
            else None
        )
        rootfs_limit = (
            _optional_int(
                actual.get("rootfs_export_max_concurrent_operations")
            )
            or 0
        )
        # Occupied export slots are useful work, not queue pressure. Only work
        # waiting behind those slots proves that another pipeline could reduce
        # latency. This distinction matters now that distinct images export
        # concurrently instead of serializing behind one host-wide lock.
        rootfs_queue = (
            min(1.0, rootfs_waiting / rootfs_limit)
            if rootfs_limit > 0
            else None
        )
        is_pressure = any(
            (
                cpu is not None and cpu >= policy.target_cpu_utilization,
                memory is not None and memory >= policy.target_memory_utilization,
                psi is not None and psi >= policy.max_memory_psi_full_avg10,
                storage_queue is not None
                and storage_queue >= policy.target_storage_queue_utilization,
                rootfs_queue is not None
                and rootfs_queue >= policy.target_storage_queue_utilization,
            )
        )
        if not is_pressure:
            continue
        pressure_samples += 1
        if latest_pressure_epoch is None or event_epoch >= latest_pressure_epoch:
            latest_pressure_epoch = event_epoch
            latest_cpu = cpu
            latest_memory = memory
            latest_psi = psi
            latest_storage_queue = storage_queue
            latest_rootfs_export_queue = rootfs_queue

    lifecycle = _vm_lifecycle_summary(events)
    provisioning_values = sorted(
        value / 1000.0
        for item in lifecycle.get("items", [])
        if isinstance(item, dict)
        if (value := _optional_int(item.get("submit_to_first_heartbeat_ms")))
        is not None
    )
    scale_wait_values = sorted(
        value / 1000.0
        for event in events
        if event.kind == "sandbox_scheduled"
        if (value := _optional_int(event.data.get("scale_up_wait_ms"))) is not None
    )
    return LiveScaleSignals(
        window_seconds=max(1, policy.live_pressure_window_seconds),
        pressure_samples=pressure_samples,
        latest_pressure_age_seconds=(
            max(0, int(now.timestamp() - latest_pressure_epoch))
            if latest_pressure_epoch is not None
            else None
        ),
        cpu_utilization=latest_cpu,
        memory_utilization=latest_memory,
        memory_psi_full_avg10=latest_psi,
        storage_queue_utilization=latest_storage_queue,
        rootfs_export_queue_utilization=latest_rootfs_export_queue,
        create_pressure_samples=create_pressure_samples,
        latest_create_pressure_age_seconds=(
            max(0, int(now.timestamp() - latest_create_pressure_epoch))
            if latest_create_pressure_epoch is not None
            else None
        ),
        sandbox_create_rejections=sandbox_create_rejections,
        sandbox_create_limit=sandbox_create_limit,
        provisioning_samples=len(provisioning_values),
        provisioning_p95_seconds=_percentile_float(provisioning_values, 0.95),
        scale_up_wait_samples=len(scale_wait_values),
        scale_up_wait_p95_seconds=_percentile_float(scale_wait_values, 0.95),
    )


def build_metrics_snapshot(
    heartbeats: dict[str, NodeHeartbeat],
    routing_state: RoutingState | None,
    events: list[MetricEvent],
    *,
    heartbeat_ttl_seconds: int,
    exec_session_count: int | None = None,
    program_requests: list[ProgramRequestState] | None = None,
) -> dict[str, Any]:
    now = utc_now()
    heartbeat_items = list(heartbeats.values())
    fresh = [
        heartbeat
        for heartbeat in heartbeat_items
        if heartbeat.is_fresh(now, heartbeat_ttl_seconds)
    ]
    compatible = [
        heartbeat
        for heartbeat in fresh
        if agent_version_is_schedulable(heartbeat.agent_version)
    ]
    sandbox_nodes = [
        heartbeat for heartbeat in fresh if "sandbox" in heartbeat.capabilities
    ]
    builder_nodes = [
        heartbeat for heartbeat in fresh if "image-build" in heartbeat.capabilities
    ]
    schedulable_sandbox_nodes = [
        heartbeat
        for heartbeat in compatible
        if "sandbox" in heartbeat.capabilities
        and not heartbeat.draining
        and heartbeat.admission_open
    ]
    schedulable_builder_nodes = [
        heartbeat for heartbeat in compatible if "image-build" in heartbeat.capabilities
    ]
    routing_state = routing_state or RoutingState({}, {}, {}, {})
    fresh_sandbox_nodes = {heartbeat.node_id: heartbeat for heartbeat in sandbox_nodes}
    routes_on_fresh_nodes = sum(
        1
        for route in routing_state.sandboxes.values()
        if route.node_id in fresh_sandbox_nodes
    )
    active_routes = len(routing_state.sandboxes)
    sandbox_state_counts: dict[str, int] = {}
    for route in routing_state.sandboxes.values():
        state = str(route.state or "unknown").strip().lower() or "unknown"
        sandbox_state_counts[state] = sandbox_state_counts.get(state, 0) + 1
    fresh_resources = _aggregate_node_resources(fresh)
    sandbox_resources = _aggregate_node_resources(sandbox_nodes)
    builder_resources = _aggregate_node_resources(builder_nodes)
    provisional_running = _routes_created_after_heartbeat_count(
        routing_state.sandboxes.values(),
        fresh_sandbox_nodes,
    )
    pending_sandboxes = [
        item for item in routing_state.pending.values() if not item.is_expired(now)
    ]
    prepared_capacity = [
        item for item in routing_state.prepared.values() if not item.is_expired(now)
    ]
    prepared_builders = [
        item
        for item in routing_state.prepared_builders.values()
        if not item.is_expired(now)
    ]
    pending_builds = [
        item for item in routing_state.image_builds.values() if not item.is_expired(now)
    ]
    image_warmups = [
        item for item in routing_state.image_warmups.values() if not item.is_expired(now)
    ]
    scale_events = [
        event
        for event in events
        if event.kind == "sandbox_scheduled"
        and isinstance(event.data.get("scale_up_wait_ms"), int)
    ][-DEFAULT_SCALE_UP_SAMPLE_LIMIT:]
    node_events = [event for event in events if event.kind == "node_heartbeat"][
        -DEFAULT_RECENT_EVENT_LIMIT:
    ]
    scale_values = [int(event.data["scale_up_wait_ms"]) for event in scale_events]
    latest_autoscaler = next(
        (
            event
            for event in reversed(events)
            if event.kind == "autoscaler_cycle"
        ),
        None,
    )
    program_summary = build_program_state_summary(program_requests or [], now=now)
    capacity_pending_sandboxes = [
        item for item in pending_sandboxes if item.is_capacity_demand
    ]
    suppressed_pending_sandboxes = [
        item for item in pending_sandboxes if not item.is_capacity_demand
    ]

    return {
        "generated_at": now.isoformat(),
        "nodes": {
            "total": len(heartbeat_items),
            "fresh": len(fresh),
            "compatible": len(compatible),
            "incompatible": max(0, len(fresh) - len(compatible)),
            "sandbox": len(sandbox_nodes),
            "sandbox_ready": len(schedulable_sandbox_nodes),
            "sandbox_draining": sum(
                1 for heartbeat in sandbox_nodes if heartbeat.draining
            ),
            "sandbox_admission_closed": sum(
                1 for heartbeat in sandbox_nodes if not heartbeat.admission_open
            ),
            "builder": len(builder_nodes),
            "items": [
                _node_metrics(heartbeat, now, heartbeat_ttl_seconds)
                for heartbeat in heartbeat_items
            ],
            "samples": sum(1 for event in events if event.kind == "node_heartbeat"),
            "recent_samples": [event.to_dict() for event in node_events],
        },
        "resources": {
            "fresh": fresh_resources,
            "sandbox": sandbox_resources,
            "builder": builder_resources,
            "schedulable_sandbox": _aggregate_node_resources(schedulable_sandbox_nodes),
            "schedulable_builder": _aggregate_node_resources(schedulable_builder_nodes),
        },
        "sandboxes": {
            "running": sandbox_resources["active_sandboxes"] + provisional_running,
            "active_routes": active_routes,
            "routes_on_fresh_nodes": routes_on_fresh_nodes,
            "provisional_running_routes": provisional_running,
            "stale_routes": max(0, active_routes - routes_on_fresh_nodes),
            "states": dict(sorted(sandbox_state_counts.items())),
            "pending": len(capacity_pending_sandboxes),
            "pending_resources": _sum_pending_resources(
                capacity_pending_sandboxes
            ).to_dict(),
            "oldest_pending_seconds": _oldest_age_seconds(
                capacity_pending_sandboxes
            ),
            "pending_attempts": sum(
                item.attempts for item in capacity_pending_sandboxes
            ),
            "suppressed_pending": len(suppressed_pending_sandboxes),
            "suppressed_pending_resources": _sum_pending_resources(
                suppressed_pending_sandboxes
            ).to_dict(),
            "suppressed_pending_attempts": sum(
                item.attempts for item in suppressed_pending_sandboxes
            ),
        },
        "capacity": {
            "prepared": len(prepared_capacity),
            "prepared_sandboxes": sum(item.count for item in prepared_capacity),
            "prepared_resources": _sum_prepared_resources(prepared_capacity).to_dict(),
            "oldest_prepared_seconds": _oldest_age_seconds(prepared_capacity),
            "next_expiration_seconds": _next_expiration_seconds(prepared_capacity),
            "items": [item.to_dict() for item in prepared_capacity],
        },
        "exec": {
            "sessions": (
                len(routing_state.exec_sessions)
                if exec_session_count is None
                else max(0, int(exec_session_count))
            ),
        },
        "programs": program_summary,
        "images": {
            "pending_builds": len(pending_builds),
            "oldest_pending_build_seconds": _oldest_age_seconds(pending_builds),
            "pending_warmups": len(image_warmups),
            "oldest_warmup_seconds": _oldest_age_seconds(image_warmups),
            "next_warmup_expiration_seconds": _next_expiration_seconds(image_warmups),
            "warmups": [item.to_dict() for item in image_warmups],
        },
        "builders": {
            "prepared": len(prepared_builders),
            "prepared_builders": sum(item.count for item in prepared_builders),
            "oldest_prepared_seconds": _oldest_age_seconds(prepared_builders),
            "next_expiration_seconds": _next_expiration_seconds(prepared_builders),
            "items": [item.to_dict() for item in prepared_builders],
        },
        "scale_up": _scale_up_summary(scale_values, scale_events),
        "autoscaler": (
            {
                "timestamp": latest_autoscaler.timestamp,
                **latest_autoscaler.data,
            }
            if latest_autoscaler is not None
            else None
        ),
        "vm_lifecycle": _vm_lifecycle_summary(events),
        "traces": _trace_snapshot(events),
        "events": {
            "recent": [
                event.to_dict() for event in events[-DEFAULT_RECENT_EVENT_LIMIT:]
            ],
        },
    }


def build_program_state_summary(
    requests: list[ProgramRequestState],
    *,
    now: Any = None,
) -> dict[str, Any]:
    """Build a bounded current-state view and an aging-first shadow queue."""

    now = now or utc_now()
    active = [request for request in requests if not request.is_terminal]
    counts = {state: 0 for state in ("model_wait", "ready_to_wake", "waking", "acting")}
    resources = {
        state: ResourceQuantity()
        for state in ("model_wait", "ready_to_wake", "waking", "acting")
    }
    for request in active:
        if request.state not in counts:
            continue
        counts[request.state] += 1
        resources[request.state] = resources[request.state] + request.resources

    ready = sorted(
        (request for request in active if request.state == "ready_to_wake"),
        key=lambda request: (
            _timestamp_epoch(request.response_ready_at or request.updated_at),
            request.request_id,
        ),
    )
    completed_wait_ms = [
        duration
        for request in active
        if (
            duration := _duration_ms(
                request.accepted_at,
                request.response_ready_at,
            )
        )
        is not None
    ]
    completed_wake_ms = [
        duration
        for request in active
        if (
            duration := _duration_ms(
                request.response_ready_at,
                request.wake_completed_at,
            )
        )
        is not None
    ]
    return {
        "requests": len(active),
        "rollouts": len({request.rollout_id for request in active}),
        "sandboxes": len({request.sandbox_id for request in active}),
        "states": counts,
        "resources": {
            state: quantity.to_dict() for state, quantity in resources.items()
        },
        "oldest_model_wait_seconds": _oldest_program_age_seconds(
            (
                request.accepted_at
                for request in active
                if request.state == "model_wait"
            ),
            now,
        ),
        "oldest_ready_to_wake_seconds": _oldest_program_age_seconds(
            (request.response_ready_at for request in ready),
            now,
        ),
        "model_wait_p50_ms": _percentile(completed_wait_ms, 0.50),
        "model_wait_p95_ms": _percentile(completed_wait_ms, 0.95),
        "response_to_wake_p50_ms": _percentile(completed_wake_ms, 0.50),
        "response_to_wake_p95_ms": _percentile(completed_wake_ms, 0.95),
        "shadow_wake_queue": [
            {
                "position": position,
                "request_id": request.request_id,
                "rollout_id": request.rollout_id,
                "sandbox_id": request.sandbox_id,
                "sandbox_generation": request.sandbox_generation,
                "resources": request.resources.to_dict(),
                "ready_at": request.response_ready_at,
                "age_seconds": max(
                    0,
                    int(
                        now.timestamp()
                        - _timestamp_epoch(
                            request.response_ready_at or request.updated_at
                        )
                    ),
                ),
            }
            for position, request in enumerate(ready[:100], start=1)
        ],
    }


def _node_metrics(
    heartbeat: NodeHeartbeat,
    now: Any,
    heartbeat_ttl_seconds: int,
) -> dict[str, Any]:
    effective = heartbeat.effective_resources
    used = heartbeat.used_resources
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
        "fresh": heartbeat.is_fresh(now, heartbeat_ttl_seconds),
        "agent_version_compatible": agent_version_is_schedulable(
            heartbeat.agent_version
        ),
        "age_seconds": max(0, int((now - heartbeat.updated_at).total_seconds())),
        "active_sandboxes": heartbeat.active_sandboxes,
        "active_image_builds": heartbeat.active_image_builds,
        "active_sandbox_creates": heartbeat.active_sandbox_creates,
        "active_workloads": heartbeat.active_workloads,
        "draining": heartbeat.draining,
        "admission_open": heartbeat.admission_open,
        "capabilities": list(heartbeat.capabilities),
        "agent_version": heartbeat.agent_version,
        "deployment_id": heartbeat.deployment_id,
        "total_resources": heartbeat.total_resources.to_dict(),
        "effective_resources": effective.to_dict(),
        "used_resources": used.to_dict(),
        "free_resources": heartbeat.free_resources.to_dict(),
        "load": _resource_load(used, effective),
        "actual_usage": (
            heartbeat.runtime_metrics.to_dict()
            if heartbeat.runtime_metrics is not None
            else None
        ),
    }


def _aggregate_node_resources(heartbeats: list[NodeHeartbeat]) -> dict[str, Any]:
    effective = ResourceQuantity()
    used = ResourceQuantity()
    free = ResourceQuantity()
    active_sandboxes = 0
    active_image_builds = 0
    active_sandbox_creates = 0
    for heartbeat in heartbeats:
        effective = effective + heartbeat.effective_resources
        used = used + heartbeat.used_resources
        free = free + heartbeat.free_resources
        active_sandboxes += heartbeat.active_sandboxes
        active_image_builds += heartbeat.active_image_builds
        active_sandbox_creates += heartbeat.active_sandbox_creates
    return {
        "nodes": len(heartbeats),
        "active_sandboxes": active_sandboxes,
        "active_image_builds": active_image_builds,
        "active_sandbox_creates": active_sandbox_creates,
        "active_workloads": (
            active_sandboxes + active_image_builds + active_sandbox_creates
        ),
        "effective": effective.to_dict(),
        "used": used.to_dict(),
        "free": free.to_dict(),
        "load": _resource_load(used, effective),
        "actual_usage": _aggregate_actual_usage(heartbeats),
    }


def _routes_created_after_heartbeat_count(
    routes: Any,
    heartbeats_by_node_id: dict[str, NodeHeartbeat],
) -> int:
    count = 0
    for route in routes:
        heartbeat = heartbeats_by_node_id.get(route.node_id)
        if heartbeat is None:
            continue
        route_created_at = parse_iso_datetime(route.created_at)
        if route_created_at is not None and route_created_at > heartbeat.updated_at:
            count += 1
    return count


def _aggregate_actual_usage(heartbeats: list[NodeHeartbeat]) -> dict[str, Any]:
    metrics = [
        heartbeat.runtime_metrics
        for heartbeat in heartbeats
        if heartbeat.runtime_metrics is not None
    ]
    if not metrics:
        return {
            "samples": 0,
            "cpu_vcpu": None,
            "cpu_percent_avg": None,
            "memory_total_mb": 0,
            "memory_used_mb": 0,
            "memory_available_mb": 0,
            "memory_percent": None,
            "swap_total_mb": 0,
            "swap_used_mb": 0,
            "swap_free_mb": 0,
            "memory_psi_some_avg10": None,
            "memory_psi_full_avg10": None,
            "load_average_1m": None,
            "load_average_5m": None,
            "load_average_15m": None,
        }
    cpu_vcpu_values = [item.cpu_vcpu for item in metrics if item.cpu_vcpu is not None]
    cpu_percent_values = [
        item.cpu_percent for item in metrics if item.cpu_percent is not None
    ]
    load_1m_values = [
        item.load_average_1m for item in metrics if item.load_average_1m is not None
    ]
    load_5m_values = [
        item.load_average_5m for item in metrics if item.load_average_5m is not None
    ]
    load_15m_values = [
        item.load_average_15m for item in metrics if item.load_average_15m is not None
    ]
    total_memory = sum(item.memory_total_mb for item in metrics)
    used_memory = sum(item.memory_used_mb for item in metrics)
    available_memory = sum(item.memory_available_mb for item in metrics)
    psi_some_values = [
        item.memory_psi_some_avg10
        for item in metrics
        if item.memory_psi_some_avg10 is not None
    ]
    psi_full_values = [
        item.memory_psi_full_avg10
        for item in metrics
        if item.memory_psi_full_avg10 is not None
    ]
    return {
        "samples": len(metrics),
        "cpu_vcpu": sum(cpu_vcpu_values) if cpu_vcpu_values else None,
        "cpu_percent_avg": _avg(cpu_percent_values),
        "memory_total_mb": total_memory,
        "memory_used_mb": used_memory,
        "memory_available_mb": available_memory,
        "memory_percent": (
            (used_memory / total_memory) * 100.0 if total_memory > 0 else None
        ),
        "swap_total_mb": sum(item.swap_total_mb for item in metrics),
        "swap_used_mb": sum(item.swap_used_mb for item in metrics),
        "swap_free_mb": sum(item.swap_free_mb for item in metrics),
        "memory_psi_some_avg10": _avg(psi_some_values),
        "memory_psi_full_avg10": _avg(psi_full_values),
        "load_average_1m": _avg(load_1m_values),
        "load_average_5m": _avg(load_5m_values),
        "load_average_15m": _avg(load_15m_values),
    }


def _trace_snapshot(events: list[MetricEvent]) -> dict[str, Any]:
    spans = [event for event in events if event.kind == "trace_span"]
    recent_spans = spans[-DEFAULT_TRACE_SPAN_LIMIT:]
    grouped: dict[str, list[MetricEvent]] = {}
    ordered_trace_ids: list[str] = []
    for event in recent_spans:
        trace_id = str(event.data.get("trace_id") or "")
        if not trace_id:
            continue
        if trace_id not in grouped:
            ordered_trace_ids.append(trace_id)
            grouped[trace_id] = []
        grouped[trace_id].append(event)
    recent_traces = [
        _trace_summary(trace_id, grouped[trace_id])
        for trace_id in ordered_trace_ids[-DEFAULT_TRACE_LIMIT:]
    ]
    return {
        "span_count": len(spans),
        "recent_spans": [event.to_dict() for event in recent_spans],
        "recent": recent_traces,
    }


def _trace_summary(trace_id: str, spans: list[MetricEvent]) -> dict[str, Any]:
    sorted_spans = sorted(
        spans,
        key=lambda event: (
            str(event.data.get("started_at") or event.timestamp),
            str(event.data.get("span_id") or ""),
        ),
    )
    root = next(
        (
            event
            for event in sorted_spans
            if not str(event.data.get("parent_span_id") or "")
        ),
        sorted_spans[0] if sorted_spans else None,
    )
    statuses = {str(event.data.get("status") or "ok") for event in sorted_spans}
    total_duration = (
        _optional_int(root.data.get("duration_ms")) if root is not None else None
    )
    if total_duration is None:
        durations = [
            value
            for value in (
                _optional_int(event.data.get("duration_ms")) for event in sorted_spans
            )
            if value is not None
        ]
        total_duration = max(durations) if durations else 0
    return {
        "trace_id": trace_id,
        "name": str(root.data.get("name") or "") if root is not None else "",
        "status": "error" if "error" in statuses else "ok",
        "started_at": str(root.data.get("started_at") or root.timestamp)
        if root is not None
        else "",
        "finished_at": str(root.data.get("finished_at") or root.timestamp)
        if root is not None
        else "",
        "duration_ms": total_duration,
        "span_count": len(sorted_spans),
        "spans": [
            {
                "span_id": str(event.data.get("span_id") or ""),
                "parent_span_id": str(event.data.get("parent_span_id") or ""),
                "name": str(event.data.get("name") or ""),
                "status": str(event.data.get("status") or "ok"),
                "duration_ms": _optional_int(event.data.get("duration_ms")),
                "attributes": event.data.get("attributes") or {},
            }
            for event in sorted_spans
        ],
    }


def _optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _optional_float(raw: object) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if value >= 0 else None


def _fraction_from_percent(raw: object) -> float | None:
    value = _optional_float(raw)
    return min(1.0, value / 100.0) if value is not None else None


def _percentile_float(sorted_values: list[float], quantile: float) -> float | None:
    if not sorted_values:
        return None
    index = int(round((len(sorted_values) - 1) * max(0.0, min(1.0, quantile))))
    return sorted_values[index]


def _metrics_lock(path: Path) -> RLock:
    key = path.resolve()
    with _METRICS_LOCKS_GUARD:
        lock = _METRICS_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _METRICS_LOCKS[key] = lock
        return lock


def _resource_load(
    used: ResourceQuantity, total: ResourceQuantity
) -> dict[str, float | None]:
    return {
        "vcpu": _ratio(used.vcpu, total.vcpu),
        "memory": _ratio(used.memory_mb, total.memory_mb),
        "disk": _ratio(used.disk_mb, total.disk_mb),
    }


def _ratio(used: float, total: float) -> float | None:
    if total <= 0:
        return None
    return used / total


def _avg(values: list[float | int]) -> float | None:
    if not values:
        return None
    return float(sum(values)) / len(values)


def _sum_pending_resources(items: list[Any]) -> ResourceQuantity:
    total = ResourceQuantity()
    for item in items:
        resources = getattr(item, "resources", ResourceQuantity())
        if isinstance(resources, ResourceQuantity):
            total = total + resources
    return total


def _sum_prepared_resources(items: list[Any]) -> ResourceQuantity:
    total = ResourceQuantity()
    for item in items:
        resources = getattr(item, "total_resources", ResourceQuantity())
        if isinstance(resources, ResourceQuantity):
            total = total + resources
    return total


def _oldest_age_seconds(items: list[Any]) -> int:
    now = utc_now()
    oldest = 0
    for item in items:
        created_at = parse_iso_datetime(getattr(item, "created_at", ""))
        if created_at is not None:
            oldest = max(oldest, int((now - created_at).total_seconds()))
    return max(0, oldest)


def _oldest_program_age_seconds(timestamps: Any, now: Any) -> int:
    oldest = 0
    for timestamp in timestamps:
        parsed = parse_iso_datetime(timestamp)
        if parsed is not None:
            oldest = max(oldest, int((now - parsed).total_seconds()))
    return max(0, oldest)


def _next_expiration_seconds(items: list[Any]) -> int | None:
    now = utc_now()
    values: list[int] = []
    for item in items:
        expires_at = parse_iso_datetime(getattr(item, "expires_at", ""))
        if expires_at is not None:
            values.append(max(0, int((expires_at - now).total_seconds())))
    return min(values) if values else None


def _scale_up_summary(
    values: list[int],
    events: list[MetricEvent],
) -> dict[str, Any]:
    if not values:
        return {
            "samples": 0,
            "last_ms": None,
            "avg_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "max_ms": None,
            "recent": [],
        }
    sorted_values = sorted(values)
    return {
        "samples": len(values),
        "last_ms": values[-1],
        "avg_ms": sum(values) / len(values),
        "p50_ms": _percentile(sorted_values, 0.50),
        "p95_ms": _percentile(sorted_values, 0.95),
        "max_ms": max(values),
        "recent": [event.to_dict() for event in events[-DEFAULT_RECENT_EVENT_LIMIT:]],
    }


def _percentile(sorted_values: list[int], quantile: float) -> int:
    if not sorted_values:
        return 0
    index = int(round((len(sorted_values) - 1) * max(0.0, min(1.0, quantile))))
    return sorted_values[index]


def _vm_lifecycle_summary(events: list[MetricEvent]) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    lifecycle_events = [
        event
        for event in events
        if event.kind
        in {
            "vm_submitted",
            "vm_observed",
            "vm_init_attempt",
            "node_heartbeat",
            "node_first_heartbeat",
            "sandbox_scheduled",
        }
    ]
    for event in lifecycle_events:
        data = event.data
        job_id = data.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            continue
        record = records.setdefault(
            job_id,
            {
                "job_id": job_id,
                "role": "",
                "state": "",
                "node_id": "",
                "submitted_at": None,
                "ucloud_created_at": None,
                "ucloud_started_at": None,
                "first_heartbeat_at": None,
                "last_heartbeat_at": None,
                "first_sandbox_scheduled_at": None,
                "last_sandbox_scheduled_at": None,
                "last_activity_at": event.timestamp,
                "init_attempts": [],
            },
        )
        record["last_activity_at"] = max(
            str(record.get("last_activity_at") or ""), event.timestamp
        )
        if event.kind == "vm_submitted":
            record["submitted_at"] = record.get("submitted_at") or event.timestamp
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            _copy_first(record, data, "node_url")
            _copy_first(record, data, "hostname")
            _copy_first(record, data, "product_id")
            _copy_first(record, data, "disk_gb")
        elif event.kind == "vm_observed":
            record["state"] = data.get("state") or record.get("state") or ""
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            _copy_first(record, data, "hostname")
            _copy_first(record, data, "product_id")
            _copy_first(record, data, "disk_gb")
            record["ucloud_created_at"] = data.get("created_at") or record.get(
                "ucloud_created_at"
            )
            record["ucloud_started_at"] = data.get("started_at") or record.get(
                "ucloud_started_at"
            )
            record["latest_note"] = (
                data.get("latest_note") or record.get("latest_note") or ""
            )
            record["ready"] = bool(data.get("ready"))
            record["provisioning"] = bool(data.get("provisioning"))
        elif event.kind == "vm_init_attempt":
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            attempts = record.setdefault("init_attempts", [])
            if isinstance(attempts, list):
                attempts.append(
                    {
                        "status": data.get("status"),
                        "attempts": data.get("attempts"),
                        "started_at": data.get("started_at"),
                        "finished_at": data.get("finished_at"),
                        "duration_ms": data.get("duration_ms"),
                        "stage_duration_ms": data.get("stage_duration_ms"),
                        "run_duration_ms": data.get("run_duration_ms"),
                        "returncode": data.get("returncode"),
                        "retry_delay_seconds": data.get("retry_delay_seconds"),
                        "skipped": data.get("skipped"),
                        "reason": data.get("reason") or "",
                    }
                )
        elif event.kind in {"node_heartbeat", "node_first_heartbeat"}:
            _copy_first(record, data, "node_id")
            heartbeat_at = data.get("heartbeat_updated_at") or event.timestamp
            if not record.get("first_heartbeat_at"):
                record["first_heartbeat_at"] = heartbeat_at
            record["last_heartbeat_at"] = heartbeat_at
        elif event.kind == "sandbox_scheduled":
            scheduled_at = event.timestamp
            if not record.get("first_sandbox_scheduled_at"):
                record["first_sandbox_scheduled_at"] = scheduled_at
                record["first_sandbox_scale_up_wait_ms"] = data.get("scale_up_wait_ms")
            record["last_sandbox_scheduled_at"] = scheduled_at

    items = sorted(
        records.values(),
        key=lambda item: str(item.get("last_activity_at") or ""),
        reverse=True,
    )[:DEFAULT_VM_LIFECYCLE_LIMIT]
    for item in items:
        item["submit_to_running_ms"] = _duration_ms(
            item.get("submitted_at"),
            item.get("ucloud_started_at"),
        )
        item["ucloud_created_to_running_ms"] = _duration_ms(
            item.get("ucloud_created_at"),
            item.get("ucloud_started_at"),
        )
        item["running_to_first_heartbeat_ms"] = _duration_ms(
            item.get("ucloud_started_at"),
            item.get("first_heartbeat_at"),
        )
        item["submit_to_first_heartbeat_ms"] = _duration_ms(
            item.get("submitted_at"),
            item.get("first_heartbeat_at"),
        )
        item["first_heartbeat_to_first_sandbox_ms"] = _duration_ms(
            item.get("first_heartbeat_at"),
            item.get("first_sandbox_scheduled_at"),
        )
        attempts = item.get("init_attempts")
        if isinstance(attempts, list):
            ordered_attempts = sorted(
                attempts,
                key=lambda attempt: str(attempt.get("started_at") or ""),
            )
            first_attempt = ordered_attempts[0] if ordered_attempts else None
            succeeded = [
                attempt
                for attempt in ordered_attempts
                if attempt.get("status") == "succeeded"
                and isinstance(attempt.get("duration_ms"), int)
            ]
            last_success = succeeded[-1] if succeeded else None
            first_init_attempt_at = (
                first_attempt.get("started_at") if first_attempt is not None else None
            )
            item["running_to_first_init_attempt_ms"] = _duration_ms(
                item.get("ucloud_started_at"),
                first_init_attempt_at,
            )
            item["first_init_attempt_to_first_heartbeat_ms"] = _duration_ms(
                first_init_attempt_at,
                item.get("first_heartbeat_at"),
            )
            item["last_successful_init_duration_ms"] = (
                last_success["duration_ms"] if last_success is not None else None
            )
            item["last_successful_package_stage_ms"] = (
                _optional_int(last_success.get("stage_duration_ms"))
                if last_success is not None
                else None
            )
            item["last_successful_remote_init_ms"] = (
                _optional_int(last_success.get("run_duration_ms"))
                if last_success is not None
                else None
            )
            item["init_attempts"] = ordered_attempts[-10:]
    return {
        "samples": len(records),
        "items": items,
        "recent_events": [
            event.to_dict() for event in lifecycle_events[-DEFAULT_RECENT_EVENT_LIMIT:]
        ],
    }


def _copy_first(target: dict[str, Any], source: dict[str, Any], key: str) -> None:
    if target.get(key) in (None, "") and source.get(key) not in (None, ""):
        target[key] = source.get(key)


def _duration_ms(start: object, end: object) -> int | None:
    start_dt = parse_iso_datetime(start)
    end_dt = parse_iso_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return max(0, int((end_dt - start_dt).total_seconds() * 1000))


def _node_role(node: Any) -> str:
    job = getattr(node, "job", None)
    labels = getattr(job, "labels", {}) if job is not None else {}
    if labels.get("ucloud-sandboxes/builder") == "true":
        return "builder"
    return "sandbox"


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
