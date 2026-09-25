"""Canonical worker record and lifecycle request codecs, without HTTP handlers."""
from __future__ import annotations

from dataclasses import replace
import json
from typing import Any

from .lifecycle_commit import route_with_snapshot_payload as _route_with_snapshot_payload
from .models import SandboxInventoryEntry
from .routing import SandboxRoute
from .sandbox import SandboxSpec, sandbox_spec_fingerprint, sandbox_specs_match

def _sandbox_inventory_from_record(record: dict[str, Any]) -> SandboxInventoryEntry:
    spec = record.get("spec")
    if not isinstance(spec, dict):
        raise ValueError("sandbox record is missing its spec")
    parsed_spec = SandboxSpec.from_dict(spec)
    parsed_spec.validate()
    generation = _record_generation(record)
    operation_id = record.get("operation_id")
    spec_hash = record.get("spec_hash")
    state = record.get("state")
    if generation is None:
        raise ValueError("sandbox record generation must be positive")
    if spec_hash != sandbox_spec_fingerprint(parsed_spec):
        raise ValueError("sandbox record spec_hash does not match its spec")
    if not isinstance(state, str) or not state.strip():
        raise ValueError("sandbox record state is required")
    return SandboxInventoryEntry(
        sandbox_id=parsed_spec.id,
        generation=generation,
        operation_id=operation_id,
        spec_hash=spec_hash,
        state=state.strip(),
        resources=parsed_spec.requested_resources(),
    )


def _route_with_sandbox_record(
    route: SandboxRoute,
    record: dict[str, Any],
) -> SandboxRoute:
    observation = _sandbox_inventory_from_record(record)
    if observation.sandbox_id != route.sandbox_id:
        raise ValueError("sandbox record id does not match its route")
    route_state = observation.route_state
    if route_state is None:
        raise ValueError(f"sandbox record state is not routable: {observation.state!r}")
    node_epoch = route.node_epoch
    activity_epoch = route.activity_epoch
    if "node_epoch" in record or "activity_epoch" in record:
        confirmed_epoch = record.get("node_epoch")
        confirmed_activity = record.get("activity_epoch")
        if not isinstance(confirmed_epoch, str) or not confirmed_epoch.strip():
            raise ValueError("sandbox confirmation requires a node epoch")
        if node_epoch and confirmed_epoch != node_epoch:
            raise ValueError("sandbox confirmation belongs to another node boot")
        if type(confirmed_activity) is not int or confirmed_activity < 0:
            raise ValueError("sandbox confirmation requires a non-negative activity epoch")
        node_epoch, activity_epoch = confirmed_epoch, confirmed_activity
    storage_schema = str(record.get("storage_schema") or "")
    storage_snapshot: dict[str, Any] = {}
    snapshot_manifest_digest = ""
    snapshot_repository = ""
    snapshot_tag = ""
    if storage_schema:
        validated = _route_with_snapshot_payload(
            route,
            record,
            observation=observation,
        )
        storage_schema = validated.storage_schema
        storage_snapshot = dict(validated.storage_snapshot)
        snapshot_manifest_digest = validated.snapshot_manifest_digest
        snapshot_repository = validated.snapshot_repository
        snapshot_tag = validated.snapshot_tag
    elif route_state == "parked":
        storage_schema = route.storage_schema
        storage_snapshot = dict(route.storage_snapshot)
        snapshot_manifest_digest = route.snapshot_manifest_digest
        snapshot_repository = route.snapshot_repository
        snapshot_tag = route.snapshot_tag
    return replace(
        route,
        resources=observation.resources,
        spec=dict(record["spec"]),
        state=route_state,
        generation=observation.generation,
        create_operation_id=observation.operation_id,
        spec_hash=observation.spec_hash,
        delete_operation_id=route.delete_operation_id,
        node_epoch=node_epoch,
        activity_epoch=activity_epoch,
        storage_schema=storage_schema,
        snapshot_manifest_digest=snapshot_manifest_digest,
        snapshot_repository=snapshot_repository,
        snapshot_tag=snapshot_tag,
        storage_snapshot=storage_snapshot,
    )


def _sandbox_record_matches_spec(
    record: dict[str, Any], requested: SandboxSpec
) -> bool:
    raw_spec = record.get("spec")
    if not isinstance(raw_spec, dict):
        return False
    try:
        existing = SandboxSpec.from_dict(raw_spec)
    except (TypeError, ValueError):
        return False
    return sandbox_specs_match(existing, requested)


def _record_generation(record: object) -> int | None:
    if not isinstance(record, dict):
        return None
    try:
        generation = int(record.get("generation"))
    except (TypeError, ValueError, OverflowError):
        return None
    return generation if generation > 0 else None


def _sandbox_record_matches_route(
    record: dict[str, Any],
    route: SandboxRoute,
    requested: SandboxSpec,
) -> bool:
    if not _sandbox_record_matches_spec(record, requested):
        return False
    try:
        confirmed = _route_with_sandbox_record(route, record)
    except (TypeError, ValueError):
        return False
    return (
        confirmed.generation == route.generation
        and confirmed.create_operation_id == route.create_operation_id
        and confirmed.spec_hash == route.spec_hash
        and route.spec_hash == sandbox_spec_fingerprint(requested)
    )


def _sandbox_create_request_body(spec: SandboxSpec, route: SandboxRoute) -> bytes:
    payload = spec.to_dict()
    payload["_ucloud_operation"] = {
        "operation_id": route.create_operation_id,
        "generation": route.generation,
        "kind": "create",
        "spec_hash": route.spec_hash,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

