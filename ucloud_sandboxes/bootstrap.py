from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import SandboxNode, utc_now
from .providers.base import InstanceBootstrapAccess
from .vm_init import VmInitOptions


@dataclass(frozen=True)
class VmBootstrapRecord:
    job_id: str
    node_id: str = ""
    role: str = ""
    status: str = ""
    attempts: int = 0
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_access_refresh_at: datetime | None = None
    last_error: str = ""
    retry_delay_seconds: int | None = None

    @classmethod
    def from_dict(cls, raw: object) -> "VmBootstrapRecord":
        if not isinstance(raw, dict):
            raise ValueError("bootstrap record must be a JSON object.")
        expected_fields = {
            "job_id",
            "node_id",
            "role",
            "status",
            "attempts",
            "last_attempt_at",
            "last_success_at",
            "last_access_refresh_at",
            "last_error",
            "retry_delay_seconds",
        }
        if set(raw) != expected_fields:
            raise ValueError("bootstrap record does not match the current schema.")
        job_id = _required_string(raw.get("job_id"), field="job_id")
        attempts = _nonnegative_integer(raw.get("attempts"), field="attempts")
        retry_delay_raw = raw.get("retry_delay_seconds")
        return cls(
            job_id=job_id,
            node_id=_string(raw.get("node_id"), field="node_id"),
            role=_string(raw.get("role"), field="role"),
            status=_string(raw.get("status"), field="status"),
            attempts=attempts,
            last_attempt_at=_parse_iso(
                raw.get("last_attempt_at"),
                field="last_attempt_at",
            ),
            last_success_at=_parse_iso(
                raw.get("last_success_at"),
                field="last_success_at",
            ),
            last_access_refresh_at=_parse_iso(
                raw.get("last_access_refresh_at"),
                field="last_access_refresh_at",
            ),
            last_error=_string(raw.get("last_error"), field="last_error"),
            retry_delay_seconds=(
                _nonnegative_integer(
                    retry_delay_raw,
                    field="retry_delay_seconds",
                )
                if retry_delay_raw is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "node_id": self.node_id,
            "role": self.role,
            "status": self.status,
            "attempts": self.attempts,
            "last_attempt_at": _format_iso(self.last_attempt_at),
            "last_success_at": _format_iso(self.last_success_at),
            "last_access_refresh_at": _format_iso(self.last_access_refresh_at),
            "last_error": self.last_error,
            "retry_delay_seconds": self.retry_delay_seconds,
        }

    def retry_due(self, *, now: datetime, retry_seconds: int) -> bool:
        if self.last_attempt_at is None:
            return True
        delay = (
            self.retry_delay_seconds
            if self.retry_delay_seconds is not None
            else retry_seconds
        )
        return (now - self.last_attempt_at).total_seconds() >= max(0, delay)


@dataclass(frozen=True)
class VmBootstrapIntent:
    job_id: str
    node_id: str
    role: str
    access: InstanceBootstrapAccess
    options: VmInitOptions
    runnable: bool
    reason: str
    previous_attempts: int = 0
    access_refreshed_at: datetime | None = None


def build_vm_bootstrap_intents(
    nodes: list[SandboxNode],
    records: dict[str, VmBootstrapRecord],
    *,
    retry_seconds: int,
    max_per_cycle: int,
    options_for_node: Any,
    access_for_instance: Any,
    refresh_access_for_instance: Any | None = None,
    max_access_refreshes: int | None = None,
    excluded_job_ids: set[str] | frozenset[str] = frozenset(),
    now: datetime | None = None,
) -> list[VmBootstrapIntent]:
    if now is None:
        now = utc_now()
    remaining = max(0, max_per_cycle)
    refreshes_remaining = min(
        remaining,
        max(
            0,
            remaining if max_access_refreshes is None else max_access_refreshes,
        ),
    )
    intents: list[VmBootstrapIntent] = []
    excluded = frozenset(excluded_job_ids)
    # A bounded refresh budget must also make progress through a stable provider
    # inventory. Prefer nodes never refreshed, then the least recently refreshed.
    ordered_nodes = sorted(
        enumerate(nodes),
        key=lambda item: (
            records.get(item[1].job_id) is not None
            and records[item[1].job_id].last_access_refresh_at is not None,
            (
                records[item[1].job_id].last_access_refresh_at
                if records.get(item[1].job_id) is not None
                else None
            )
            or datetime.min.replace(tzinfo=now.tzinfo),
            item[0],
        ),
    )
    for _, node in ordered_nodes:
        if remaining <= 0:
            break
        if node.job_id in excluded:
            continue
        if node.is_ready or not node.job.is_running:
            continue
        role = "builder" if _is_builder(node) else "sandbox"
        options = options_for_node(node, role)
        record = records.get(node.job_id)
        attempts = record.attempts if record is not None else 0
        stale_workloads = max(
            node.active_sandboxes,
            node.heartbeat.active_workloads if node.heartbeat is not None else 0,
        )
        if stale_workloads > 0:
            access = access_for_instance(node.job)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    access=access,
                    options=options,
                    runnable=False,
                    reason="stale node still owns gateway-managed work",
                    previous_attempts=attempts,
                )
            )
            continue
        if record is not None and record.status == "succeeded":
            access = access_for_instance(node.job)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    access=access,
                    options=options,
                    runnable=False,
                    reason="VM init previously succeeded; waiting for heartbeat",
                    previous_attempts=attempts,
                )
            )
            continue
        if record is not None and not record.retry_due(
            now=now, retry_seconds=retry_seconds
        ):
            access = access_for_instance(node.job)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    access=access,
                    options=options,
                    runnable=False,
                    reason="waiting for VM init retry backoff",
                    previous_attempts=attempts,
                )
            )
            continue
        access = access_for_instance(node.job)
        access_refreshed_at: datetime | None = None
        if (
            not access.runnable
            and access.refresh_recommended
            and refresh_access_for_instance is not None
            and refreshes_remaining > 0
        ):
            # Consume before the provider call: failures and still-unready results
            # count against the same per-cycle remote-work budget.
            refreshes_remaining -= 1
            access_refreshed_at = now
            access = refresh_access_for_instance(node.job)
        intent = VmBootstrapIntent(
            job_id=node.job_id,
            node_id=options.normalized_node_id(),
            role=role,
            access=access,
            options=options,
            runnable=access.runnable,
            reason=access.reason,
            previous_attempts=attempts,
            access_refreshed_at=access_refreshed_at,
        )
        intents.append(intent)
        if intent.runnable:
            remaining -= 1
    return intents


def mark_bootstrap_access_refresh(
    records: dict[str, VmBootstrapRecord],
    intent: VmBootstrapIntent,
) -> dict[str, VmBootstrapRecord]:
    refreshed_at = intent.access_refreshed_at
    if refreshed_at is None:
        return records
    existing = records.get(intent.job_id)
    updated = dict(records)
    updated[intent.job_id] = VmBootstrapRecord(
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status=existing.status if existing is not None else "",
        attempts=existing.attempts
        if existing is not None
        else intent.previous_attempts,
        last_attempt_at=existing.last_attempt_at if existing is not None else None,
        last_success_at=existing.last_success_at if existing is not None else None,
        last_access_refresh_at=refreshed_at,
        last_error=existing.last_error if existing is not None else "",
        retry_delay_seconds=(
            existing.retry_delay_seconds if existing is not None else None
        ),
    )
    return updated


def mark_bootstrap_attempt(
    records: dict[str, VmBootstrapRecord],
    intent: VmBootstrapIntent,
    *,
    now: datetime | None = None,
) -> dict[str, VmBootstrapRecord]:
    if now is None:
        now = utc_now()
    existing = records.get(intent.job_id)
    attempts = (existing.attempts if existing is not None else 0) + 1
    updated = dict(records)
    updated[intent.job_id] = VmBootstrapRecord(
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status="attempting",
        attempts=attempts,
        last_attempt_at=now,
        last_success_at=existing.last_success_at if existing is not None else None,
        last_access_refresh_at=(
            existing.last_access_refresh_at if existing is not None else None
        ),
        last_error="",
        retry_delay_seconds=None,
    )
    return updated


def mark_bootstrap_success(
    records: dict[str, VmBootstrapRecord],
    intent: VmBootstrapIntent,
    *,
    now: datetime | None = None,
) -> dict[str, VmBootstrapRecord]:
    if now is None:
        now = utc_now()
    existing = records.get(intent.job_id)
    updated = dict(records)
    updated[intent.job_id] = VmBootstrapRecord(
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status="succeeded",
        attempts=existing.attempts
        if existing is not None
        else intent.previous_attempts,
        last_attempt_at=existing.last_attempt_at if existing is not None else now,
        last_success_at=now,
        last_access_refresh_at=(
            existing.last_access_refresh_at if existing is not None else None
        ),
        last_error="",
        retry_delay_seconds=None,
    )
    return updated


def mark_bootstrap_failure(
    records: dict[str, VmBootstrapRecord],
    intent: VmBootstrapIntent,
    error: str,
    *,
    retry_delay_seconds: int | None = None,
    now: datetime | None = None,
) -> dict[str, VmBootstrapRecord]:
    if now is None:
        now = utc_now()
    existing = records.get(intent.job_id)
    updated = dict(records)
    updated[intent.job_id] = VmBootstrapRecord(
        job_id=intent.job_id,
        node_id=intent.node_id,
        role=intent.role,
        status="failed",
        attempts=existing.attempts
        if existing is not None
        else intent.previous_attempts,
        last_attempt_at=existing.last_attempt_at if existing is not None else now,
        last_success_at=existing.last_success_at if existing is not None else None,
        last_access_refresh_at=(
            existing.last_access_refresh_at if existing is not None else None
        ),
        last_error=error,
        retry_delay_seconds=(
            max(0, retry_delay_seconds) if retry_delay_seconds is not None else None
        ),
    )
    return updated


def prune_bootstrap_records(
    records: dict[str, VmBootstrapRecord],
    active_job_ids: set[str],
) -> dict[str, VmBootstrapRecord]:
    return {
        job_id: record for job_id, record in records.items() if job_id in active_job_ids
    }


def _is_builder(node: SandboxNode) -> bool:
    return node.job.labels.get(
        "ucloud-sandboxes/builder"
    ) == "true" or node.job.name.startswith("ucloud-sandbox-builder")


def _string(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"bootstrap record {field} must be a string.")
    return value


def _required_string(value: object, *, field: str) -> str:
    parsed = _string(value, field=field)
    if not parsed.strip():
        raise ValueError(f"bootstrap record {field} must not be empty.")
    return parsed


def _nonnegative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"bootstrap record {field} must be a non-negative integer.")
    return value


def _parse_iso(value: object, *, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"bootstrap record {field} must be an ISO-8601 timestamp or null."
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"bootstrap record {field} must be an ISO-8601 timestamp or null."
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"bootstrap record {field} must include a timezone.")
    return parsed


def _format_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError("bootstrap record timestamps must be datetime values.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("bootstrap record timestamps must include a timezone.")
    return value.isoformat()
