from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .models import SandboxNode, utc_now
from .vm_init import VmInitOptions, VmInitPlan


@dataclass(frozen=True)
class VmBootstrapRecord:
    job_id: str
    node_id: str = ""
    role: str = ""
    status: str = ""
    attempts: int = 0
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str = ""
    retry_delay_seconds: int | None = None

    @classmethod
    def from_dict(cls, raw: object) -> "VmBootstrapRecord":
        if not isinstance(raw, dict):
            raise ValueError("bootstrap record must be a JSON object.")
        return cls(
            job_id=str(raw.get("jobId") or ""),
            node_id=str(raw.get("nodeId") or ""),
            role=str(raw.get("role") or ""),
            status=str(raw.get("status") or ""),
            attempts=int(raw.get("attempts") or 0),
            last_attempt_at=_parse_iso(raw.get("lastAttemptAt")),
            last_success_at=_parse_iso(raw.get("lastSuccessAt")),
            last_error=str(raw.get("lastError") or ""),
            retry_delay_seconds=(
                max(0, int(raw["retryDelaySeconds"]))
                if raw.get("retryDelaySeconds") is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "nodeId": self.node_id,
            "role": self.role,
            "status": self.status,
            "attempts": self.attempts,
            "lastAttemptAt": _format_iso(self.last_attempt_at),
            "lastSuccessAt": _format_iso(self.last_success_at),
            "lastError": self.last_error,
            "retryDelaySeconds": self.retry_delay_seconds,
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


class VmBootstrapStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, VmBootstrapRecord]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        records = raw.get("jobs") if isinstance(raw, dict) else None
        if not isinstance(records, dict):
            return {}
        result: dict[str, VmBootstrapRecord] = {}
        for job_id, record in records.items():
            parsed = VmBootstrapRecord.from_dict(record)
            if parsed.job_id:
                result[str(job_id)] = parsed
        return result

    def save(self, records: dict[str, VmBootstrapRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "jobs": {
                job_id: record.to_dict()
                for job_id, record in sorted(records.items())
            }
        }
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.path)


@dataclass(frozen=True)
class VmBootstrapIntent:
    job_id: str
    node_id: str
    role: str
    plan: VmInitPlan
    options: VmInitOptions
    runnable: bool
    reason: str
    previous_attempts: int = 0


def build_vm_bootstrap_intents(
    nodes: list[SandboxNode],
    records: dict[str, VmBootstrapRecord],
    *,
    retry_seconds: int,
    max_per_cycle: int,
    options_for_node: Any,
    plan_for_payload: Any,
    now: datetime | None = None,
) -> list[VmBootstrapIntent]:
    if now is None:
        now = utc_now()
    remaining = max(0, max_per_cycle)
    intents: list[VmBootstrapIntent] = []
    for node in nodes:
        if remaining <= 0:
            break
        if node.is_ready or node.job.state != "RUNNING":
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
            plan = plan_for_payload(node.job.raw)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    plan=plan,
                    options=options,
                    runnable=False,
                    reason="stale node still owns gateway-managed work",
                    previous_attempts=attempts,
                )
            )
            continue
        if record is not None and record.status == "succeeded":
            plan = plan_for_payload(node.job.raw)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    plan=plan,
                    options=options,
                    runnable=False,
                    reason="VM init previously succeeded; waiting for heartbeat",
                    previous_attempts=attempts,
                )
            )
            continue
        if record is not None and not record.retry_due(now=now, retry_seconds=retry_seconds):
            plan = plan_for_payload(node.job.raw)
            intents.append(
                VmBootstrapIntent(
                    job_id=node.job_id,
                    node_id=options.normalized_node_id(),
                    role=role,
                    plan=plan,
                    options=options,
                    runnable=False,
                    reason="waiting for VM init retry backoff",
                    previous_attempts=attempts,
                )
            )
            continue
        plan = plan_for_payload(node.job.raw)
        intent = VmBootstrapIntent(
            job_id=node.job_id,
            node_id=options.normalized_node_id(),
            role=role,
            plan=plan,
            options=options,
            runnable=plan.runnable,
            reason=plan.reason,
            previous_attempts=attempts,
        )
        intents.append(intent)
        if intent.runnable:
            remaining -= 1
    return intents


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
        attempts=existing.attempts if existing is not None else intent.previous_attempts,
        last_attempt_at=existing.last_attempt_at if existing is not None else now,
        last_success_at=now,
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
        attempts=existing.attempts if existing is not None else intent.previous_attempts,
        last_attempt_at=existing.last_attempt_at if existing is not None else now,
        last_success_at=existing.last_success_at if existing is not None else None,
        last_error=error,
        retry_delay_seconds=(
            max(0, retry_delay_seconds)
            if retry_delay_seconds is not None
            else None
        ),
    )
    return updated


def prune_bootstrap_records(
    records: dict[str, VmBootstrapRecord],
    active_job_ids: set[str],
) -> dict[str, VmBootstrapRecord]:
    return {
        job_id: record
        for job_id, record in records.items()
        if job_id in active_job_ids
    }


def _is_builder(node: SandboxNode) -> bool:
    return (
        node.job.labels.get("ucloud-sandboxes/builder") == "true"
        or node.job.name.startswith("ucloud-sandbox-builder")
    )


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
