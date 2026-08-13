from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
from threading import RLock
import time
from typing import Any

from .deployment import BUILDER_LABEL, agent_version_is_schedulable
from .models import (
    LiveScaleSignals,
    NodeHeartbeat,
    ResourceQuantity,
    SandboxNode,
    ScalePolicy,
    parse_iso_datetime,
    utc_now,
)
from .providers.base import InstanceCreateIntent
from .routing import (
    PendingSandboxDemand,
    ProgramRequestState,
    RoutingState,
    SandboxRoute,
    is_portable_parked_route,
)


DEFAULT_RECENT_EVENT_LIMIT = 50
DEFAULT_SCALE_UP_SAMPLE_LIMIT = 200
DEFAULT_VM_LIFECYCLE_LIMIT = 100
DEFAULT_PROGRAM_WAKE_PLAN_SAMPLE_LIMIT = 100
DEFAULT_METRICS_MAX_BYTES = 64 * 1024**2
DEFAULT_METRICS_MAX_EVENT_BYTES = 1024**2
DEFAULT_METRICS_MAX_EVENTS = 100_000
MIN_SQLITE_METRICS_MAX_BYTES = 64 * 1024
_METRICS_LOCKS_GUARD = RLock()
_METRICS_LOCKS: dict[Path, RLock] = {}


@dataclass(frozen=True)
class MetricEvent:
    timestamp: str
    kind: str
    data: dict[str, Any]

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
        max_event_bytes: int = DEFAULT_METRICS_MAX_EVENT_BYTES,
        max_events: int = DEFAULT_METRICS_MAX_EVENTS,
    ) -> None:
        if path.suffix != ".sqlite":
            raise ValueError("metrics path must use the .sqlite suffix")
        self.path = path
        self._lock = _metrics_lock(path)
        self._max_bytes = max(1, max_bytes)
        self._max_event_bytes = max(1, min(max_event_bytes, self._max_bytes))
        self._max_events = max(1, max_events)
        if self._max_bytes < MIN_SQLITE_METRICS_MAX_BYTES:
            raise ValueError(
                "SQLite metrics max_bytes must be at least "
                f"{MIN_SQLITE_METRICS_MAX_BYTES}"
            )
        self._sqlite_connection: sqlite3.Connection | None = None
        self._sqlite_pid = 0
        self._dropped_sqlite_events = 0
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
        payload_bytes = _metric_event_bytes(event)
        if payload_bytes > self._max_event_bytes:
            event = MetricEvent(
                timestamp=event.timestamp,
                kind=event.kind,
                data={
                    "metrics_payload_truncated": True,
                    "original_bytes": payload_bytes,
                },
            )
        with self._lock:
            connection = self._sqlite_connect_locked()
            try:
                stored_events = [event]
                if self._dropped_sqlite_events:
                    stored_events.insert(
                        0,
                        MetricEvent(
                            event.timestamp,
                            "metrics_dropped_events",
                            {
                                "count": self._dropped_sqlite_events,
                                "reason": "sqlite_busy",
                            },
                        ),
                    )
                for stored in stored_events:
                    connection.execute(
                        """
                        INSERT INTO metric_events(
                            timestamp, timestamp_epoch, kind, data_json,
                            payload_bytes
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            stored.timestamp,
                            _timestamp_epoch(stored.timestamp),
                            stored.kind,
                            json.dumps(
                                stored.data,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            _metric_event_bytes(stored),
                        ),
                    )
                self._prune_sqlite_locked(connection)
                connection.commit()
                try:
                    self._reclaim_sqlite_space_locked(connection)
                except sqlite3.OperationalError:
                    pass
                self._dropped_sqlite_events = 0
            except sqlite3.OperationalError:
                connection.rollback()
                self._dropped_sqlite_events += 1
        return event

    def load_events(
        self,
        *,
        max_events: int = 1000,
        kinds: tuple[str, ...] = (),
        since_seconds: int | None = None,
    ) -> list[MetricEvent]:
        return self._load_sqlite_events(
            max_events=max_events,
            kinds=kinds,
            since_seconds=since_seconds,
        )

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
        existing_tables = bool(
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
            ).fetchone()
        )
        if int(connection.execute("PRAGMA auto_vacuum").fetchone()[0]) != 2:
            connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            if existing_tables:
                connection.execute("VACUUM")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=1000")
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        connection.execute(
            f"PRAGMA wal_autocheckpoint={max(1, self._max_bytes // page_size // 8)}"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS metric_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                timestamp_epoch REAL NOT NULL,
                kind TEXT NOT NULL,
                data_json TEXT NOT NULL,
                payload_bytes INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(metric_events)")
        }
        expected_event_columns = {
            "sequence",
            "timestamp",
            "timestamp_epoch",
            "kind",
            "data_json",
            "payload_bytes",
        }
        if columns != expected_event_columns:
            connection.close()
            raise sqlite3.DatabaseError("metrics database schema is invalid")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS metric_store_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                retained_events INTEGER NOT NULL,
                retained_payload_bytes INTEGER NOT NULL
            )
            """
        )
        meta_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(metric_store_meta)")
        }
        if meta_columns != {
            "singleton",
            "retained_events",
            "retained_payload_bytes",
        }:
            connection.close()
            raise sqlite3.DatabaseError("metrics database schema is invalid")
        connection.execute(
            """
            INSERT INTO metric_store_meta(
                singleton, retained_events, retained_payload_bytes
            )
            VALUES (
                1,
                (SELECT count(*) FROM metric_events),
                (SELECT COALESCE(sum(payload_bytes), 0) FROM metric_events)
            )
            ON CONFLICT(singleton) DO UPDATE SET
                retained_events = excluded.retained_events,
                retained_payload_bytes = excluded.retained_payload_bytes
            """
        )
        connection.execute(
            """
            CREATE TRIGGER IF NOT EXISTS metric_events_track_insert
            AFTER INSERT ON metric_events
            BEGIN
                UPDATE metric_store_meta
                SET retained_events = retained_events + 1,
                    retained_payload_bytes = (
                        retained_payload_bytes + NEW.payload_bytes
                    )
                WHERE singleton = 1;
            END
            """
        )
        connection.execute(
            """
            CREATE TRIGGER IF NOT EXISTS metric_events_track_delete
            AFTER DELETE ON metric_events
            BEGIN
                UPDATE metric_store_meta
                SET retained_events = MAX(0, retained_events - 1),
                    retained_payload_bytes = MAX(
                        0,
                        retained_payload_bytes - OLD.payload_bytes
                    )
                WHERE singleton = 1;
            END
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
        self._prune_sqlite_locked(connection)
        connection.commit()
        self._reclaim_sqlite_space_locked(connection)
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
            if not isinstance(data, dict):
                continue
            events.append(MetricEvent(timestamp=timestamp, kind=kind, data=data))
        return events

    def _prune_sqlite_locked(self, connection: sqlite3.Connection) -> None:
        retained = connection.execute(
            """
            SELECT retained_events, retained_payload_bytes
            FROM metric_store_meta
            WHERE singleton = 1
            """
        ).fetchone()
        if retained is None:
            return
        retained_events = max(0, int(retained[0]))
        retained_bytes = max(0, int(retained[1]))
        if retained_events <= self._max_events and retained_bytes <= self._max_bytes:
            return
        evicted: list[tuple[int]] = []
        for sequence, payload_bytes in connection.execute(
            """
            SELECT sequence, payload_bytes
            FROM metric_events
            ORDER BY sequence
            """
        ):
            if (
                retained_events <= self._max_events
                and retained_bytes <= self._max_bytes
            ):
                break
            evicted.append((int(sequence),))
            retained_events -= 1
            retained_bytes = max(0, retained_bytes - max(0, int(payload_bytes)))
        if evicted:
            connection.executemany(
                "DELETE FROM metric_events WHERE sequence = ?",
                evicted,
            )

    def _reclaim_sqlite_space_locked(self, connection: sqlite3.Connection) -> None:
        if _sqlite_storage_bytes(self.path) <= self._max_bytes:
            return
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is not None and int(checkpoint[0]) != 0:
            # A concurrent reader can temporarily pin WAL frames. Logical
            # retention is already bounded; retry physical reclamation on the
            # next append instead of deleting useful rows while the same WAL
            # cannot yet be truncated.
            return
        free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        if free_pages:
            connection.execute(f"PRAGMA incremental_vacuum({free_pages})")
            checkpoint = connection.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if checkpoint is not None and int(checkpoint[0]) != 0:
                return
        attempts = 0
        while _sqlite_storage_bytes(self.path) > self._max_bytes:
            retained = connection.execute(
                "SELECT retained_events FROM metric_store_meta WHERE singleton = 1"
            ).fetchone()
            retained_events = int(retained[0]) if retained is not None else 0
            if retained_events <= 0:
                break
            batch = max(1, min(4096, (retained_events + 9) // 10))
            connection.execute(
                """
                DELETE FROM metric_events
                WHERE sequence IN (
                    SELECT sequence FROM metric_events
                    ORDER BY sequence
                    LIMIT ?
                )
                """,
                (batch,),
            )
            connection.commit()
            free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
            if free_pages:
                connection.execute(f"PRAGMA incremental_vacuum({free_pages})")
            checkpoint = connection.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if checkpoint is not None and int(checkpoint[0]) != 0:
                return
            attempts += 1
            if attempts >= 64:
                break


def _timestamp_epoch(value: str) -> float:
    parsed = parse_iso_datetime(value)
    return parsed.timestamp() if parsed is not None else time.time()


def _metric_event_bytes(event: MetricEvent) -> int:
    return len((json.dumps(event.to_dict(), sort_keys=True) + "\n").encode("utf-8"))


def _sqlite_storage_bytes(path: Path) -> int:
    total = 0
    for candidate in (path, path.with_name(path.name + "-wal")):
        try:
            total += candidate.stat().st_size
        except FileNotFoundError:
            continue
    return total


class GatewayBusySampler:
    """Aggregate admission failures for the autoscaler's local feedback loop."""

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

    def record(self, *, max_concurrent_sandbox_creates: int) -> bool:
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

        self.store.append(
            "sandbox_create_busy",
            {
                "outcome": "gateway_busy",
                "max_concurrent_sandbox_creates": max_concurrent_sandbox_creates,
                "aggregated_rejections": rejected_requests,
                "sample_interval_seconds": self.min_interval_seconds,
            },
        )
        return True


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
            "suppressed_pending_count": decision.get("suppressedPendingCount", 0),
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
            "create_pressure_scale_up": bool(decision.get("createPressureScaleUp")),
            "effective_scale_down_idle_seconds": decision.get(
                "effectiveScaleDownIdleSeconds"
            ),
            "ready_nodes": decision.get("readyNodes", 0),
            "provisioning_nodes": decision.get("provisioningNodes", 0),
            "unreachable_nodes": decision.get("unreachableNodes", 0),
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
        value.get("placements") if isinstance(value.get("placements"), list) else []
    )
    unplaced = value.get("unplaced") if isinstance(value.get("unplaced"), list) else []
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
    intent: InstanceCreateIntent,
) -> None:
    if store is None:
        return
    store.append(
        "vm_submitted",
        {
            "cycle": cycle,
            "job_id": job_id,
            "role": intent.role,
            "node_id": intent.node_id,
            "node_url": intent.node_url,
            "name": intent.name,
            "hostname": intent.node_id,
        },
    )


def record_vm_observed(
    store: MetricsStore | None,
    *,
    cycle: int,
    node: SandboxNode,
) -> None:
    if store is None:
        return
    job = node.job
    store.append(
        "vm_observed",
        {
            "cycle": cycle,
            "job_id": job.id,
            "role": "builder" if job.labels.get(BUILDER_LABEL) == "true" else "sandbox",
            "state": job.state,
            "name": job.name,
            "hostname": job.hostname or "",
            "created_at": _iso_or_none(job.created_at),
            "started_at": _iso_or_none(job.started_at),
            "expires_at": _iso_or_none(job.expires_at),
            "latest_note": job.latest_note or "",
            "queue_status": job.queue_status or "",
            "product_id": job.product_id,
            "cpu": job.cpu,
            "memory_gb": job.memory_gb,
            "disk_gb": job.disk_gb,
            "ready": node.is_ready,
            "provisioning": node.is_provisioning,
            "heartbeat_fresh": node.heartbeat_fresh,
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
    store.append(
        "node_heartbeat",
        _heartbeat_metrics(heartbeat),
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
    latest_image_materialization_queue: float | None = None
    create_cutoff = now.timestamp() - max(1, policy.create_pressure_window_seconds)
    create_pressure_samples = 0
    latest_create_pressure_epoch: float | None = None
    sandbox_create_rejections = 0
    sandbox_create_limit = 0

    for event in events:
        event_epoch = _timestamp_epoch(event.timestamp)
        if event.kind == "sandbox_create_busy" and event_epoch >= create_cutoff:
            attributes = event.data
            if attributes.get("outcome") == "gateway_busy":
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
                        _optional_int(attributes.get("max_concurrent_sandbox_creates"))
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
        materialization_active = (
            _optional_int(actual.get("image_materialization_active_operations")) or 0
        )
        materialization_waiting = (
            _optional_int(actual.get("image_materialization_waiting_operations")) or 0
        )
        if (
            (active_workloads or 0) <= 0
            and storage_active + storage_waiting <= 0
            and materialization_active + materialization_waiting <= 0
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
        materialization_limit = (
            _optional_int(actual.get("image_materialization_max_concurrent_operations"))
            or 0
        )
        # Occupied materialization slots are useful work, not queue pressure.
        # Only queued work proves that another pipeline could reduce latency.
        materialization_queue = (
            min(1.0, materialization_waiting / materialization_limit)
            if materialization_limit > 0
            else None
        )
        is_pressure = any(
            (
                cpu is not None and cpu >= policy.target_cpu_utilization,
                memory is not None and memory >= policy.target_memory_utilization,
                psi is not None and psi >= policy.max_memory_psi_full_avg10,
                storage_queue is not None
                and storage_queue >= policy.target_storage_queue_utilization,
                materialization_queue is not None
                and materialization_queue >= policy.target_storage_queue_utilization,
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
            latest_image_materialization_queue = materialization_queue

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
        image_materialization_queue_utilization=(latest_image_materialization_queue),
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
    portable_parked_routes = sum(
        is_portable_parked_route(route) for route in routing_state.sandboxes.values()
    )
    stale_routes = sum(
        route.node_id not in fresh_sandbox_nodes and not is_portable_parked_route(route)
        for route in routing_state.sandboxes.values()
    )
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
        item
        for item in routing_state.image_warmups.values()
        if not item.is_expired(now)
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
        (event for event in reversed(events) if event.kind == "autoscaler_cycle"),
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
            "portable_parked_routes": portable_parked_routes,
            "provisional_running_routes": provisional_running,
            "stale_routes": stale_routes,
            "states": dict(sorted(sandbox_state_counts.items())),
            "pending": len(capacity_pending_sandboxes),
            "pending_resources": _sum_pending_resources(
                capacity_pending_sandboxes
            ).to_dict(),
            "oldest_pending_seconds": _oldest_age_seconds(capacity_pending_sandboxes),
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
        "model_wait_p50_ms": _percentile(sorted(completed_wait_ms), 0.50),
        "model_wait_p95_ms": _percentile(sorted(completed_wait_ms), 0.95),
        "response_to_wake_p50_ms": _percentile(sorted(completed_wake_ms), 0.50),
        "response_to_wake_p95_ms": _percentile(sorted(completed_wake_ms), 0.95),
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
    return {
        **_heartbeat_metrics(heartbeat),
        "fresh": heartbeat.is_fresh(now, heartbeat_ttl_seconds),
        "agent_version_compatible": agent_version_is_schedulable(
            heartbeat.agent_version
        ),
        "age_seconds": max(0, int((now - heartbeat.updated_at).total_seconds())),
    }


def _heartbeat_metrics(heartbeat: NodeHeartbeat) -> dict[str, Any]:
    total = heartbeat.total_resources
    used = heartbeat.used_resources
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
        "active_sandboxes": heartbeat.active_sandboxes,
        "active_image_builds": heartbeat.active_image_builds,
        "active_sandbox_creates": heartbeat.active_sandbox_creates,
        "active_workloads": heartbeat.active_workloads,
        "draining": heartbeat.draining,
        "admission_open": heartbeat.admission_open,
        "capabilities": list(heartbeat.capabilities),
        "agent_version": heartbeat.agent_version,
        "deployment_id": heartbeat.deployment_id,
        "init_version": heartbeat.init_version,
        "total_resources": total.to_dict(),
        "used_resources": used.to_dict(),
        "free_resources": heartbeat.free_resources.to_dict(),
        "load": _resource_load(used, total),
        "actual_usage": (
            heartbeat.runtime_metrics.to_dict()
            if heartbeat.runtime_metrics is not None
            else None
        ),
        "idle_since": _iso_or_none(heartbeat.idle_since),
        "heartbeat_updated_at": heartbeat.updated_at.isoformat(),
    }


def _aggregate_node_resources(heartbeats: list[NodeHeartbeat]) -> dict[str, Any]:
    total = ResourceQuantity()
    used = ResourceQuantity()
    free = ResourceQuantity()
    active_sandboxes = 0
    active_image_builds = 0
    active_sandbox_creates = 0
    for heartbeat in heartbeats:
        total = total + heartbeat.total_resources
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
        "total": total.to_dict(),
        "used": used.to_dict(),
        "free": free.to_dict(),
        "load": _resource_load(used, total),
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


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
