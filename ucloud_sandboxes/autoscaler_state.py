from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
from threading import Lock, RLock
from typing import Any, Iterable, Iterator
from uuid import NAMESPACE_URL, uuid4, uuid5

from .models import ProviderInstance


PROVIDER_OPERATION_LABEL = "ucloud-sandboxes/provider-operation"
DEPLOYMENT_LABEL = "ucloud-sandboxes/deployment"

OPERATION_KINDS = frozenset({"create", "stop"})
OPERATION_STATES = frozenset({"prepared", "uncertain", "accepted", "settled", "failed"})
PROVIDER_ROLES = frozenset({"sandbox", "builder"})
PROVIDER_OUTCOME_STATES = frozenset(
    {"accepted", "failed", "uncertain", "recovered", "retry", "conflict", "unresolved"}
)
PROVIDER_OUTCOME_SOURCES = frozenset(
    {"inventory-recovery", "prepared-replay", "planned", "journal"}
)
DRAIN_INTENT_STATES = frozenset({"active", "canceling"})

_LOCAL_LOCKS_GUARD = RLock()
_LOCAL_LOCKS: dict[Path, Lock] = {}


class AutoscalerStateError(RuntimeError):
    pass


class OperationConflictError(AutoscalerStateError):
    pass


class OperationStateError(AutoscalerStateError):
    pass


class AutoscalerProcessLock:
    """One process-lifetime autoscaler lock for the supported local topology.

    The kernel owns the lock and releases it if the process exits. Unlike a
    renewable wall-clock lease, a live controller can never silently become a
    stale writer merely because a renewal thread was delayed.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._local_lock: Lock | None = None
        self._file: Any | None = None

    @property
    def held(self) -> bool:
        return self._file is not None

    def acquire(self, *, blocking: bool = False) -> bool:
        if self.held:
            return True
        resolved = self.path.resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        with _LOCAL_LOCKS_GUARD:
            local_lock = _LOCAL_LOCKS.get(resolved)
            if local_lock is None:
                local_lock = Lock()
                _LOCAL_LOCKS[resolved] = local_lock
        if not local_lock.acquire(blocking=blocking):
            return False
        lock_file = None
        try:
            lock_file = resolved.open("a+", encoding="utf-8")
            os.chmod(resolved, 0o600)
            flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            try:
                fcntl.flock(lock_file.fileno(), flags)
            except BlockingIOError:
                lock_file.close()
                return False
            self._local_lock = local_lock
            self._file = lock_file
            return True
        finally:
            if self._file is None:
                if lock_file is not None and not lock_file.closed:
                    lock_file.close()
                local_lock.release()

    def release(self) -> None:
        lock_file = self._file
        local_lock = self._local_lock
        self._file = None
        self._local_lock = None
        if lock_file is not None:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            finally:
                lock_file.close()
        if local_lock is not None:
            local_lock.release()

    def __enter__(self) -> "AutoscalerProcessLock":
        if not self.acquire(blocking=True):
            raise AutoscalerStateError(f"could not acquire autoscaler lock {self.path}")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


@dataclass(frozen=True)
class ProviderOperation:
    operation_id: str
    intent_key: str
    kind: str
    deployment_id: str
    role: str
    state: str
    request: dict[str, Any]
    response: dict[str, Any]
    target_job_ids: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    last_error: str = ""


@dataclass(frozen=True)
class ProviderOperationOutcome:
    operation_id: str
    kind: str
    role: str
    state: str
    job_ids: tuple[str, ...]
    source: str
    error: str = ""

    def __post_init__(self) -> None:
        _required("operation_id", self.operation_id)
        _operation_kind(self.kind)
        _provider_role(self.role)
        if self.state not in PROVIDER_OUTCOME_STATES:
            raise ValueError(f"invalid provider outcome state: {self.state}")
        if self.source not in PROVIDER_OUTCOME_SOURCES:
            raise ValueError(f"invalid provider outcome source: {self.source}")

    @classmethod
    def from_operation(
        cls,
        operation: ProviderOperation,
        *,
        source: str,
        state: str | None = None,
        job_ids: tuple[str, ...] | None = None,
        error: str | None = None,
    ) -> "ProviderOperationOutcome":
        return cls(
            operation_id=operation.operation_id,
            kind=operation.kind,
            role=operation.role,
            state=state or operation.state,
            job_ids=operation.target_job_ids if job_ids is None else job_ids,
            source=source,
            error=operation.last_error if error is None else error,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "operationId": self.operation_id,
            "kind": self.kind,
            "role": self.role,
            "state": self.state,
            "jobIds": list(self.job_ids),
            "source": self.source,
            "error": self.error,
        }


@dataclass(frozen=True)
class DrainIntent:
    deployment_id: str
    job_id: str
    role: str
    token: str
    state: str
    created_at: datetime
    updated_at: datetime


def stable_provider_operation_id(
    deployment_id: str,
    kind: str,
    intent_key: str,
) -> str:
    deployment = _required("deployment_id", deployment_id)
    operation_kind = _operation_kind(kind)
    key = _required("intent_key", intent_key)
    value = uuid5(
        NAMESPACE_URL,
        f"ucloud-sandboxes/provider-operation/{deployment}/{operation_kind}/{key}",
    )
    return f"provider-{value.hex}"


class AutoscalerStateStore:
    """Compact local journal for provider ambiguity and desired drain state."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._ensure_database()

    def process_lock(self) -> AutoscalerProcessLock:
        return AutoscalerProcessLock(self.path.with_name(self.path.name + ".lock"))

    def prepare_drain_intent(
        self,
        *,
        deployment_id: str,
        job_id: str,
        role: str,
        now: datetime | None = None,
    ) -> DrainIntent:
        deployment = _required("deployment_id", deployment_id)
        job = _required("job_id", job_id)
        role_value = _required("role", role)
        now_us = _datetime_to_us(_normalized_now(now))
        with self._transaction() as conn:
            row = self._drain_row(conn, deployment, job, required=False)
            if row is None:
                conn.execute(
                    """
                    INSERT INTO drain_intents (
                        deployment_id, job_id, role, token, state,
                        created_at_us, updated_at_us
                    ) VALUES (?, ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        deployment,
                        job,
                        role_value,
                        f"drain-{uuid4().hex}",
                        now_us,
                        now_us,
                    ),
                )
            else:
                intent = _drain_from_row(row)
                if intent.role != role_value:
                    raise OperationConflictError(f"drain role changed for job {job}")
                if intent.state != "active":
                    raise OperationStateError(
                        f"drain intent for job {job} is {intent.state}"
                    )
                conn.execute(
                    "UPDATE drain_intents SET updated_at_us = ? "
                    "WHERE deployment_id = ? AND job_id = ?",
                    (now_us, deployment, job),
                )
            row = self._drain_row(conn, deployment, job)
        return _drain_from_row(row)

    def pending_drain_intents(
        self,
        *,
        deployment_id: str,
    ) -> list[DrainIntent]:
        deployment = _required("deployment_id", deployment_id)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM drain_intents WHERE deployment_id = ? "
                "ORDER BY created_at_us, job_id",
                (deployment,),
            ).fetchall()
        return [_drain_from_row(row) for row in rows]

    def begin_drain_cancellation(
        self,
        *,
        deployment_id: str,
        job_id: str,
        now: datetime | None = None,
    ) -> DrainIntent:
        deployment = _required("deployment_id", deployment_id)
        job = _required("job_id", job_id)
        now_us = _datetime_to_us(_normalized_now(now))
        with self._transaction() as conn:
            row = self._drain_row(conn, deployment, job)
            intent = _drain_from_row(row)
            if intent.state == "active":
                conn.execute(
                    "UPDATE drain_intents SET state = 'canceling', updated_at_us = ? "
                    "WHERE deployment_id = ? AND job_id = ? AND state = 'active'",
                    (now_us, deployment, job),
                )
            elif intent.state != "canceling":
                raise OperationStateError(
                    f"drain intent for job {job} is {intent.state}"
                )
            row = self._drain_row(conn, deployment, job)
        return _drain_from_row(row)

    def retire_drain_intent(
        self,
        *,
        deployment_id: str,
        job_id: str,
    ) -> DrainIntent | None:
        deployment = _required("deployment_id", deployment_id)
        job = _required("job_id", job_id)
        with self._transaction() as conn:
            row = self._drain_row(conn, deployment, job, required=False)
            if row is None:
                return None
            intent = _drain_from_row(row)
            conn.execute(
                "DELETE FROM drain_intents WHERE deployment_id = ? AND job_id = ?",
                (deployment, job),
            )
        return intent

    def get_drain_intent(
        self,
        deployment_id: str,
        job_id: str,
    ) -> DrainIntent | None:
        with self._connect() as conn:
            row = self._drain_row(conn, str(deployment_id), str(job_id), required=False)
        return _drain_from_row(row) if row is not None else None

    def allocate_operation_intent_key(
        self,
        *,
        deployment_id: str,
        kind: str,
        base_key: str,
        now: datetime | None = None,
    ) -> str:
        """Reserve or reuse one open planning slot without scanning history."""

        deployment = _required("deployment_id", deployment_id)
        operation_kind = _operation_kind(kind)
        base = _required("base_key", base_key)
        now_us = _datetime_to_us(_normalized_now(now))
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT incarnation, state FROM operation_slots "
                "WHERE deployment_id = ? AND kind = ? AND base_key = ?",
                (deployment, operation_kind, base),
            ).fetchone()
            if row is None:
                incarnation = 1
                conn.execute(
                    "INSERT INTO operation_slots "
                    "(deployment_id, kind, base_key, incarnation, state, updated_at_us) "
                    "VALUES (?, ?, ?, 1, 'reserved', ?)",
                    (deployment, operation_kind, base, now_us),
                )
            else:
                incarnation = int(row["incarnation"])
                if str(row["state"]) in {"settled", "failed"}:
                    incarnation += 1
                    conn.execute(
                        "UPDATE operation_slots SET incarnation = ?, state = 'reserved', "
                        "updated_at_us = ? WHERE deployment_id = ? AND kind = ? "
                        "AND base_key = ?",
                        (incarnation, now_us, deployment, operation_kind, base),
                    )
        return base if incarnation == 1 else f"{base}#{incarnation}"

    def prepare_operation(
        self,
        *,
        intent_key: str,
        kind: str,
        deployment_id: str,
        request: dict[str, Any],
        role: str,
        target_job_ids: Iterable[str] = (),
        now: datetime | None = None,
    ) -> ProviderOperation:
        key = _required("intent_key", intent_key)
        operation_kind = _operation_kind(kind)
        deployment = _required("deployment_id", deployment_id)
        operation_role = _provider_role(role)
        request_value = _json_object("request", request)
        job_ids = _job_ids(target_job_ids)
        operation_id = stable_provider_operation_id(deployment, operation_kind, key)
        now_us = _datetime_to_us(_normalized_now(now))
        with self._transaction() as conn:
            row = self._operation_row(conn, operation_id, required=False)
            if row is not None:
                existing = _operation_from_row(row)
                if (
                    existing.intent_key != key
                    or existing.kind != operation_kind
                    or existing.deployment_id != deployment
                    or existing.role != operation_role
                    or existing.request != request_value
                    or existing.target_job_ids != job_ids
                ):
                    raise OperationConflictError(
                        f"provider operation intent changed: {key}"
                    )
                return existing
            conn.execute(
                """
                INSERT INTO provider_operations (
                    operation_id, intent_key, kind, deployment_id, role, state,
                    request_json, response_json, target_job_ids_json,
                    created_at_us, updated_at_us, last_error
                ) VALUES (?, ?, ?, ?, ?, 'prepared', ?, '{}', ?, ?, ?, '')
                """,
                (
                    operation_id,
                    key,
                    operation_kind,
                    deployment,
                    operation_role,
                    _json_dump(request_value),
                    _json_dump(list(job_ids)),
                    now_us,
                    now_us,
                ),
            )
            self._update_slot_state(
                conn, deployment, operation_kind, key, "prepared", now_us
            )
            row = self._operation_row(conn, operation_id)
        return _operation_from_row(row)

    def begin_provider_call(
        self,
        operation_id: str,
        *,
        now: datetime | None = None,
    ) -> ProviderOperation:
        return self._transition_operation(
            operation_id,
            expected_states={"prepared"},
            new_state="uncertain",
            now=now,
        )

    def mark_operation_accepted(
        self,
        operation_id: str,
        *,
        response: dict[str, Any],
        target_job_ids: Iterable[str] = (),
        now: datetime | None = None,
    ) -> ProviderOperation:
        return self._transition_operation(
            operation_id,
            expected_states={"uncertain"},
            new_state="accepted",
            response=response,
            target_job_ids=target_job_ids,
            last_error="",
            now=now,
        )

    def mark_operation_uncertain(
        self,
        operation_id: str,
        *,
        error: str,
        now: datetime | None = None,
    ) -> ProviderOperation:
        return self._transition_operation(
            operation_id,
            expected_states={"uncertain"},
            new_state="uncertain",
            last_error=str(error),
            now=now,
        )

    def mark_operation_failed(
        self,
        operation_id: str,
        *,
        error: str,
        response: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> ProviderOperation:
        return self._transition_operation(
            operation_id,
            expected_states={"uncertain"},
            new_state="failed",
            response=response,
            last_error=str(error),
            now=now,
        )

    def get_operation(self, operation_id: str) -> ProviderOperation | None:
        with self._connect() as conn:
            row = self._operation_row(conn, str(operation_id), required=False)
        return _operation_from_row(row) if row is not None else None

    def list_operations(
        self,
        *,
        kind: str | None = None,
        states: Iterable[str] | None = None,
    ) -> list[ProviderOperation]:
        clauses: list[str] = []
        parameters: list[str] = []
        if kind is not None:
            clauses.append("kind = ?")
            parameters.append(_operation_kind(kind))
        state_values = tuple(dict.fromkeys(str(state) for state in (states or ())))
        if state_values:
            invalid = set(state_values) - OPERATION_STATES
            if invalid:
                raise ValueError(
                    f"invalid provider operation states: {sorted(invalid)}"
                )
            clauses.append("state IN (" + ",".join("?" for _ in state_values) + ")")
            parameters.extend(state_values)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM provider_operations{where} "
                "ORDER BY created_at_us, operation_id",
                tuple(parameters),
            ).fetchall()
        return [_operation_from_row(row) for row in rows]

    def reconcile_provider_inventory(
        self,
        instances: Iterable[ProviderInstance],
        *,
        now: datetime | None = None,
    ) -> list[ProviderOperationOutcome]:
        label_index: dict[tuple[str, str], list[str]] = {}
        observed_job_ids: set[str] = set()
        final_job_ids: set[str] = set()
        for instance in instances:
            operation_id = str(
                instance.labels.get(PROVIDER_OPERATION_LABEL) or ""
            ).strip()
            deployment_id = str(instance.labels.get(DEPLOYMENT_LABEL) or "").strip()
            job_id = str(instance.id or "").strip()
            if not job_id:
                continue
            observed_job_ids.add(job_id)
            if instance.is_final:
                final_job_ids.add(job_id)
            if operation_id and deployment_id:
                label_index.setdefault((operation_id, deployment_id), []).append(job_id)

        results: list[ProviderOperationOutcome] = []
        for operation in self.list_operations(
            kind="create", states={"uncertain", "accepted"}
        ):
            if operation.state == "accepted":
                if operation.target_job_ids and set(operation.target_job_ids).issubset(
                    observed_job_ids
                ):
                    self._transition_operation(
                        operation.operation_id,
                        expected_states={"accepted"},
                        new_state="settled",
                        now=now,
                    )
                continue
            job_ids = _job_ids(
                label_index.get((operation.operation_id, operation.deployment_id), ())
            )
            if len(job_ids) == 1:
                self._transition_operation(
                    operation.operation_id,
                    expected_states={"uncertain"},
                    new_state="settled",
                    response={"recoveredFromJobInventory": True},
                    target_job_ids=job_ids,
                    last_error="",
                    now=now,
                )
                state = "recovered"
            elif len(job_ids) > 1:
                state = "conflict"
            else:
                state = "unresolved"
            results.append(
                ProviderOperationOutcome.from_operation(
                    operation,
                    source="inventory-recovery",
                    state=state,
                    job_ids=job_ids,
                    error="",
                )
            )
        for operation in self.list_operations(
            kind="stop", states={"uncertain", "accepted"}
        ):
            if operation.state == "accepted":
                target_job_ids = set(operation.target_job_ids)
                # A provider-accepted delete may remove an instance from an
                # exhaustive inventory rather than retaining a terminal row
                # (Hetzner does this). The controller can restart after the
                # provider call but before it removes that node's heartbeat.
                # Recover the accepted stop when every target is either final
                # or absent, otherwise a quickly reused private IP remains
                # bound to the deleted job and rejects the replacement node.
                active_targets = target_job_ids & (observed_job_ids - final_job_ids)
                if target_job_ids and not active_targets:
                    self._transition_operation(
                        operation.operation_id,
                        expected_states={"accepted"},
                        new_state="settled",
                        now=now,
                    )
                    results.append(
                        ProviderOperationOutcome.from_operation(
                            operation,
                            source="inventory-recovery",
                            state="recovered",
                            error="",
                        )
                    )
                continue
            all_final = bool(operation.target_job_ids) and set(
                operation.target_job_ids
            ).issubset(final_job_ids)
            response = dict(operation.response)
            response["providerCallStarted"] = True
            if all_final:
                response["recoveredFromFinalJobInventory"] = True
            self._transition_operation(
                operation.operation_id,
                expected_states={"uncertain"},
                new_state="settled" if all_final else "prepared",
                response=response,
                last_error="" if all_final else operation.last_error,
                now=now,
            )
            results.append(
                ProviderOperationOutcome.from_operation(
                    operation,
                    source="inventory-recovery",
                    state="recovered" if all_final else "retry",
                    error="",
                )
            )
        return results

    def compact_terminal_history(self, *, keep: int = 1000) -> int:
        """Bound settled/failed audit rows; slot state preserves incarnation."""

        retain = max(0, int(keep))
        with self._transaction() as conn:
            before = int(
                conn.execute(
                    "SELECT COUNT(*) FROM provider_operations "
                    "WHERE state IN ('settled', 'failed')"
                ).fetchone()[0]
            )
            conn.execute(
                """
                DELETE FROM provider_operations
                WHERE operation_id IN (
                    SELECT operation_id FROM provider_operations
                    WHERE state IN ('settled', 'failed')
                    ORDER BY updated_at_us DESC, operation_id DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (retain,),
            )
            after = int(
                conn.execute(
                    "SELECT COUNT(*) FROM provider_operations "
                    "WHERE state IN ('settled', 'failed')"
                ).fetchone()[0]
            )
        return before - after

    def _transition_operation(
        self,
        operation_id: str,
        *,
        expected_states: Iterable[str],
        new_state: str,
        response: dict[str, Any] | None = None,
        target_job_ids: Iterable[str] | None = None,
        last_error: str | None = None,
        now: datetime | None = None,
    ) -> ProviderOperation:
        if new_state not in OPERATION_STATES:
            raise ValueError(f"invalid provider operation state: {new_state}")
        expected = tuple(dict.fromkeys(str(value) for value in expected_states))
        if not expected or set(expected) - OPERATION_STATES:
            raise ValueError(
                "expected_states contains invalid provider operation states"
            )
        now_us = _datetime_to_us(_normalized_now(now))
        with self._transaction() as conn:
            row = self._operation_row(conn, operation_id)
            operation = _operation_from_row(row)
            if operation.state not in expected:
                raise OperationStateError(
                    f"operation {operation_id} is {operation.state}; expected {expected}"
                )
            response_value = (
                operation.response
                if response is None
                else _json_object("response", response)
            )
            job_ids = (
                operation.target_job_ids
                if target_job_ids is None
                else _job_ids(target_job_ids)
            )
            error_value = (
                operation.last_error if last_error is None else str(last_error)
            )
            conn.execute(
                "UPDATE provider_operations SET state = ?, response_json = ?, "
                "target_job_ids_json = ?, updated_at_us = ?, last_error = ? "
                "WHERE operation_id = ? AND state = ?",
                (
                    new_state,
                    _json_dump(response_value),
                    _json_dump(list(job_ids)),
                    now_us,
                    error_value,
                    operation.operation_id,
                    operation.state,
                ),
            )
            self._update_slot_state(
                conn,
                operation.deployment_id,
                operation.kind,
                operation.intent_key,
                new_state,
                now_us,
            )
            row = self._operation_row(conn, operation_id)
        return _operation_from_row(row)

    @staticmethod
    def _update_slot_state(
        conn: sqlite3.Connection,
        deployment_id: str,
        kind: str,
        intent_key: str,
        state: str,
        now_us: int,
    ) -> None:
        base_key, incarnation = _split_intent_key(intent_key)
        conn.execute(
            """
            INSERT INTO operation_slots (
                deployment_id, kind, base_key, incarnation, state, updated_at_us
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(deployment_id, kind, base_key) DO UPDATE SET
                incarnation = excluded.incarnation,
                state = excluded.state,
                updated_at_us = excluded.updated_at_us
            WHERE operation_slots.incarnation <= excluded.incarnation
            """,
            (deployment_id, kind, base_key, incarnation, state, now_us),
        )

    @staticmethod
    def _operation_row(
        conn: sqlite3.Connection,
        operation_id: str,
        *,
        required: bool = True,
    ) -> sqlite3.Row | None:
        row = conn.execute(
            "SELECT * FROM provider_operations WHERE operation_id = ?",
            (str(operation_id),),
        ).fetchone()
        if row is None and required:
            raise KeyError(f"provider operation not found: {operation_id}")
        return row

    @staticmethod
    def _drain_row(
        conn: sqlite3.Connection,
        deployment_id: str,
        job_id: str,
        *,
        required: bool = True,
    ) -> sqlite3.Row | None:
        row = conn.execute(
            "SELECT * FROM drain_intents WHERE deployment_id = ? AND job_id = ?",
            (deployment_id, job_id),
        ).fetchone()
        if row is None and required:
            raise KeyError(f"drain intent not found: {deployment_id}/{job_id}")
        return row

    def _ensure_database(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_operations (
                    operation_id TEXT PRIMARY KEY,
                    intent_key TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    deployment_id TEXT NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('sandbox', 'builder')),
                    state TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    target_job_ids_json TEXT NOT NULL,
                    created_at_us INTEGER NOT NULL,
                    updated_at_us INTEGER NOT NULL,
                    last_error TEXT NOT NULL,
                    UNIQUE (deployment_id, kind, intent_key)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS provider_operations_state_idx "
                "ON provider_operations (kind, state, updated_at_us)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operation_slots (
                    deployment_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    base_key TEXT NOT NULL,
                    incarnation INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    updated_at_us INTEGER NOT NULL,
                    PRIMARY KEY (deployment_id, kind, base_key)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS drain_intents (
                    deployment_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    created_at_us INTEGER NOT NULL,
                    updated_at_us INTEGER NOT NULL,
                    PRIMARY KEY (deployment_id, job_id)
                )
                """
            )
        self._secure_database_files()

    def _secure_database_files(self) -> None:
        for path in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")):
            try:
                path.chmod(0o600)
            except FileNotFoundError:
                pass

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        self._secure_database_files()
        try:
            yield conn
        finally:
            self._secure_database_files()
            conn.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()


def _operation_from_row(row: sqlite3.Row) -> ProviderOperation:
    state = str(row["state"])
    if state not in OPERATION_STATES:
        raise AutoscalerStateError(f"invalid provider operation state: {state}")
    return ProviderOperation(
        operation_id=str(row["operation_id"]),
        intent_key=str(row["intent_key"]),
        kind=_operation_kind(row["kind"]),
        deployment_id=str(row["deployment_id"]),
        role=_provider_role(row["role"]),
        state=state,
        request=_json_load_object(str(row["request_json"])),
        response=_json_load_object(str(row["response_json"])),
        target_job_ids=tuple(_json_load_list(str(row["target_job_ids_json"]))),
        created_at=_us_to_datetime(int(row["created_at_us"])),
        updated_at=_us_to_datetime(int(row["updated_at_us"])),
        last_error=str(row["last_error"]),
    )


def _drain_from_row(row: sqlite3.Row) -> DrainIntent:
    state = str(row["state"])
    if state not in DRAIN_INTENT_STATES:
        raise AutoscalerStateError(f"invalid drain state: {state}")
    return DrainIntent(
        deployment_id=str(row["deployment_id"]),
        job_id=str(row["job_id"]),
        role=str(row["role"]),
        token=str(row["token"]),
        state=state,
        created_at=_us_to_datetime(int(row["created_at_us"])),
        updated_at=_us_to_datetime(int(row["updated_at_us"])),
    )


def _split_intent_key(intent_key: str) -> tuple[str, int]:
    base, marker, raw_incarnation = intent_key.rpartition("#")
    if marker and raw_incarnation.isdigit() and int(raw_incarnation) >= 2:
        return base, int(raw_incarnation)
    return intent_key, 1


def _normalized_now(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _datetime_to_us(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _us_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)


def _required(label: str, value: object) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise ValueError(f"{label} is required")
    return cleaned


def _operation_kind(value: object) -> str:
    kind = str(value).strip()
    if kind not in OPERATION_KINDS:
        raise ValueError(f"invalid provider operation kind: {kind}")
    return kind


def _provider_role(value: object) -> str:
    role = str(value).strip()
    if role not in PROVIDER_ROLES:
        raise ValueError(f"invalid provider operation role: {role}")
    return role


def _job_ids(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(value for item in values if (value := str(item).strip()))
    )


def _json_object(label: str, value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    _json_dump(value)
    return dict(value)


def _json_dump(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_load_object(value: str) -> dict[str, Any]:
    raw = json.loads(value)
    if not isinstance(raw, dict):
        raise AutoscalerStateError("journal JSON is not an object")
    return raw


def _json_load_list(value: str) -> list[str]:
    raw = json.loads(value)
    if not isinstance(raw, list):
        raise AutoscalerStateError("journal JSON is not a list")
    return [str(item) for item in raw]
