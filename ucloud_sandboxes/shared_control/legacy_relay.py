"""Bounded offline decoding for the final v3 SQLite relay migration boundary.

No live relay store or writer belongs here. The importer takes an exclusive
source transaction and permanently fences the source before activating PG.
"""

from __future__ import annotations
import asyncio
import json
from ..model_relay import (
    JsonObject,
    RelayRequest,
    RelayWorkerResponse,
    _encoded_body,
    _decoded_body,
    _encoded_body_bytes,
    _string_mapping,
)

SQLITE_RELAY_VERSION = 3
MAX_IMPORT_ROWS = 100_000
MAX_IMPORT_PAYLOAD_BYTES = 512 * 1024**2


def read_rows(source):
    """Bound importer memory independently of any retired runtime limits."""
    rows, used_bytes, count = {}, 0, 0
    for table in ("relay_rollouts", "relay_requests"):
        table_count, table_bytes = source.execute(
            f"SELECT count(*), coalesce(sum(length(CAST(payload AS BLOB))),0) FROM {table}"
        ).fetchone()
        count += table_count
        used_bytes += table_bytes
    if count > MAX_IMPORT_ROWS or used_bytes > MAX_IMPORT_PAYLOAD_BYTES:
        raise ValueError(
            "legacy relay exceeds bounded offline import size; retain the source and contact an operator"
        )
    for table, identity in (
        ("relay_rollouts", "rollout_id"),
        ("relay_requests", "request_id"),
    ):
        values = []
        for (payload,) in source.execute(
            f"SELECT payload FROM {table} ORDER BY {identity}"
        ):
            value = json.loads(payload)
            if not isinstance(value, dict):
                raise ValueError("invalid legacy relay row")
            values.append(value)
        rows[table] = values
    return rows["relay_rollouts"], rows["relay_requests"]


def encode_request(request: RelayRequest) -> JsonObject:
    response = request.completed_response
    completed = request.state == "completed"
    return {
        "request_id": request.request_id,
        "rollout_id": request.rollout_id,
        "registration_token": request.registration_token,
        "endpoint": request.endpoint,
        "method": request.method,
        "body": None if completed else request.body,
        "headers": {} if completed else request.headers,
        "created_at": request.created_at,
        "expires_at": request.expires_at,
        "payload_bytes": 0 if completed else request.payload_bytes,
        "delivered_at": request.delivered_at,
        "first_delivered_at": request.first_delivered_at,
        "lease_id": request.lease_id,
        "lease_expires_at": request.lease_expires_at,
        "leased_by": request.leased_by,
        "delivery_count": request.delivery_count,
        "state": request.state,
        "idempotency_key": request.idempotency_key,
        "request_digest": request.request_digest,
        "sandbox_id": request.sandbox_id,
        "sandbox_generation": request.sandbox_generation,
        "completed_at": request.completed_at,
        "completed_bytes": request.completed_bytes,
        "completed_response": (
            None
            if response is None
            else {
                "status": response.status,
                "body": _encoded_body(response.body),
                "headers": response.headers,
            }
        ),
        "wake_notified_at": request.wake_notified_at,
        "accepted_notified_at": request.accepted_notified_at,
        "parked_transport_epoch": request.parked_transport_epoch,
        "reattachable": request.reattachable,
        "delivery_pending": request.delivery_pending,
    }


def decode_request(
    payload: JsonObject,
    *,
    loop: asyncio.AbstractEventLoop,
) -> RelayRequest:
    raw_response = payload["completed_response"]
    raw_delivery_pending = payload["delivery_pending"]
    if not isinstance(raw_delivery_pending, bool):
        raise ValueError("persisted relay delivery_pending flag is invalid")
    raw_reattachable = payload["reattachable"]
    if not isinstance(raw_reattachable, bool):
        raise ValueError("persisted relay reattachable flag is invalid")
    response = None
    if raw_response is not None:
        if not isinstance(raw_response, dict):
            raise ValueError("persisted relay response is invalid")
        response = RelayWorkerResponse(
            status=int(raw_response["status"]),
            body=_decoded_body(raw_response["body"]),
            headers=_string_mapping(raw_response["headers"]),
        )
    state = payload["state"]
    if state not in {"pending", "leased", "completed"}:
        raise ValueError("persisted relay request state is invalid")
    raw_body = payload["body"]
    if state == "completed":
        if raw_body is not None:
            raise ValueError("completed relay request retained its body")
    elif not isinstance(raw_body, dict):
        raise ValueError("persisted relay request body is invalid")
    else:
        _encoded_body_bytes(raw_body)
    return RelayRequest(
        request_id=str(payload["request_id"]),
        rollout_id=str(payload["rollout_id"]),
        registration_token=str(payload["registration_token"]),
        endpoint=str(payload["endpoint"]),
        method=str(payload["method"]),
        body=raw_body,
        headers=_string_mapping(payload["headers"]),
        created_at=float(payload["created_at"]),
        future=loop.create_future(),
        expires_at=_optional_float(payload["expires_at"]),
        payload_bytes=int(payload["payload_bytes"]),
        delivered_at=_optional_float(payload["delivered_at"]),
        first_delivered_at=_optional_float(payload["first_delivered_at"]),
        lease_id=_optional_string(payload["lease_id"]),
        lease_expires_at=_optional_float(payload["lease_expires_at"]),
        leased_by=_optional_string(payload["leased_by"]),
        delivery_count=int(payload["delivery_count"]),
        state=state,
        idempotency_key=_optional_string(payload["idempotency_key"]),
        request_digest=str(payload["request_digest"]),
        sandbox_id=_optional_string(payload["sandbox_id"]),
        sandbox_generation=(
            int(payload["sandbox_generation"])
            if payload["sandbox_generation"] is not None
            else None
        ),
        completed_at=_optional_float(payload["completed_at"]),
        completed_response=response,
        completed_bytes=int(payload["completed_bytes"]),
        wake_notified_at=_optional_float(payload["wake_notified_at"]),
        accepted_notified_at=_optional_float(payload["accepted_notified_at"]),
        parked_transport_epoch=_optional_string(payload["parked_transport_epoch"]),
        reattachable=raw_reattachable,
        delivery_pending=raw_delivery_pending,
    )


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    result = str(value)
    return result or None
