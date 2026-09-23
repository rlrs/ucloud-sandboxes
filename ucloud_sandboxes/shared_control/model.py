from __future__ import annotations

from dataclasses import dataclass
import math
from uuid import UUID


class StateConflict(ValueError):
    """Identity, lease or incarnation no longer matches the submitted intent."""


class DatabaseAdmissionUnavailable(RuntimeError):
    """No connection acquired; this transaction has not executed BEGIN."""


def positive_seconds(value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError("duration must be finite and positive")
    return value


@dataclass(frozen=True)
class WakeOperation:
    operation_id: UUID
    sandbox_id: str
    generation: int
    create_operation_id: str
    spec_hash: str
    node_id: str
    node_epoch: str
    lifecycle_sequence: int
    restore_mb: int
    claim_token: UUID


@dataclass(frozen=True)
class WakeProof:
    operation_id: UUID
    generation: int
    node_epoch: str
    lifecycle_sequence: int
    activity_epoch: int


@dataclass(frozen=True)
class AcceptedResult:
    request_id: str
    operation_id: UUID | None
    response_hash: str
    duplicate: bool


@dataclass(frozen=True)
class StoredResponse:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass(frozen=True)
class TransactionSample:
    operation: str
    pool_wait_seconds: float
    transaction_seconds: float
    commit_seconds: float
    succeeded: bool
    lock_query_seconds: float = 0
