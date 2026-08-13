from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Iterable

from .config import DeploymentConfig
from .routing import RoutingStore
from .storage_native_migration import StorageNativeMigration
from .storage_native_registry import StorageSnapshotPublication
from .storage_native_s3 import Boto3S3ObjectClient, S3ObjectClient, S3ObjectStat


@dataclass(frozen=True)
class S3SnapshotGcPlan:
    protected: tuple[str, ...]
    candidates: tuple[str, ...]
    candidate_bytes: int
    inventory_objects: int
    markers_to_create: tuple[str, ...] = ()
    markers_to_clear: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "protectedObjects": len(self.protected),
            "candidateObjects": len(self.candidates),
            "candidateBytes": self.candidate_bytes,
            "inventoryObjects": self.inventory_objects,
            "newlyUnreferencedObjects": len(self.markers_to_create),
            "clearedUnreferencedMarkers": len(self.markers_to_clear),
            "candidates": list(self.candidates),
        }


def plan_s3_snapshot_gc(
    client: S3ObjectClient,
    *,
    prefix: str,
    publications: Iterable[StorageSnapshotPublication],
    now: float,
    grace_seconds: float,
    incomplete_upload_grace_seconds: float = 24 * 60 * 60,
) -> S3SnapshotGcPlan:
    if grace_seconds < 0 or incomplete_upload_grace_seconds < 0:
        raise ValueError("S3 snapshot GC grace periods cannot be negative")
    normalized_prefix = prefix.strip("/")
    protected: set[str] = set()
    for publication in publications:
        if publication.backend != "s3":
            continue
        manifest_key = f"{normalized_prefix}/manifests/{publication.manifest_digest}.json"
        manifest = client.get_bytes(manifest_key, max_bytes=1024 * 1024)
        try:
            raw = json.loads(manifest.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("cannot GC with an invalid protected S3 manifest") from exc
        config = raw.get("config") if isinstance(raw, dict) else None
        if not isinstance(config, dict):
            raise ValueError("cannot GC with a malformed protected S3 manifest")
        config_digest = str(config.get("digest") or "")
        if not config_digest.startswith("sha256:"):
            raise ValueError("cannot GC with an invalid protected config digest")
        protected.add(manifest_key)
        protected.add(f"{normalized_prefix}/metadata/{config_digest}.json")
        protected.update(
            f"{normalized_prefix}/managed-layers/{layer.digest}"
            for layer in publication.layers
        )

    inventory = dict(client.list_objects(f"{normalized_prefix}/"))
    candidates: list[str] = []
    candidate_bytes = 0
    upload_cutoff = now - incomplete_upload_grace_seconds
    managed_roots = (
        f"{normalized_prefix}/manifests/",
        f"{normalized_prefix}/metadata/",
        f"{normalized_prefix}/managed-layers/",
    )
    upload_root = f"{normalized_prefix}/.uploads/"
    marker_root = _unreferenced_marker_root(normalized_prefix)
    marker_stats: dict[str, tuple[str, S3ObjectStat]] = {}
    markers_to_clear: set[str] = set()
    for marker_key, marker_stat in inventory.items():
        if not marker_key.startswith(marker_root):
            continue
        target_key = _target_from_unreferenced_marker(
            normalized_prefix,
            marker_key,
        )
        if target_key is None or not target_key.startswith(managed_roots):
            markers_to_clear.add(marker_key)
            continue
        marker_stats[target_key] = (marker_key, marker_stat)

    markers_to_create: set[str] = set()
    for key, stat in inventory.items():
        if key.startswith(upload_root):
            if stat.modified_at <= upload_cutoff:
                candidates.append(key)
                candidate_bytes += stat.size
            continue
        if not key.startswith(managed_roots):
            continue
        marker = marker_stats.get(key)
        if key in protected:
            if marker is not None:
                markers_to_clear.add(marker[0])
            continue
        if marker is None:
            markers_to_create.add(key)
            continue
        marker_key, marker_stat = marker
        # A write after the object was marked unreferenced restarts the grace
        # period. Reads are represented by durable routes and clear the marker
        # through the protected-object branch above.
        if stat.modified_at > marker_stat.modified_at:
            markers_to_clear.add(marker_key)
            markers_to_create.add(key)
            continue
        if marker_stat.modified_at <= now - grace_seconds:
            candidates.append(key)
            candidate_bytes += stat.size

    for target_key, (marker_key, _marker_stat) in marker_stats.items():
        if target_key not in inventory:
            markers_to_clear.add(marker_key)

    return S3SnapshotGcPlan(
        protected=tuple(sorted(protected)),
        candidates=tuple(sorted(candidates)),
        candidate_bytes=candidate_bytes,
        inventory_objects=len(inventory),
        markers_to_create=tuple(sorted(markers_to_create)),
        markers_to_clear=tuple(sorted(markers_to_clear)),
    )


def execute_s3_snapshot_gc(
    client: S3ObjectClient,
    plan: S3SnapshotGcPlan,
    *,
    max_delete_objects: int,
) -> int:
    if max_delete_objects < 1:
        raise ValueError("S3 snapshot GC delete bound must be positive")
    if len(plan.candidates) > max_delete_objects:
        raise ValueError(
            "S3 snapshot GC candidate count exceeds --max-delete-objects"
        )
    candidate_markers: set[str] = set()
    for key in plan.candidates:
        client.delete(key)
        marker_key = _unreferenced_marker_for_target(key)
        if marker_key is not None:
            client.delete(marker_key)
            candidate_markers.add(marker_key)
    for marker_key in plan.markers_to_clear:
        if marker_key not in candidate_markers:
            client.delete(marker_key)
    for target_key in plan.markers_to_create:
        marker_key = _unreferenced_marker_for_target(target_key)
        if marker_key is None:
            raise ValueError("S3 snapshot GC cannot mark an unmanaged object")
        payload = _unreferenced_marker_payload(target_key)
        client.put_bytes(
            marker_key,
            payload,
            sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
        )
    return len(plan.candidates)


def _unreferenced_marker_root(prefix: str) -> str:
    return f"{prefix.strip('/')}/.gc/unreferenced/"


def _unreferenced_marker_for_target(target_key: str) -> str | None:
    prefix = ""
    for managed_root in ("/manifests/", "/metadata/", "/managed-layers/"):
        if managed_root in target_key:
            prefix = target_key.split(managed_root, 1)[0]
            break
    if not prefix:
        return None
    encoded = base64.urlsafe_b64encode(target_key.encode("utf-8")).decode("ascii")
    return _unreferenced_marker_root(prefix) + encoded.rstrip("=") + ".json"


def _target_from_unreferenced_marker(prefix: str, marker_key: str) -> str | None:
    root = _unreferenced_marker_root(prefix)
    if not marker_key.startswith(root) or not marker_key.endswith(".json"):
        return None
    encoded = marker_key[len(root) : -len(".json")]
    if not encoded:
        return None
    try:
        padding = "=" * (-len(encoded) % 4)
        target = base64.b64decode(
            encoded + padding,
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return None
    return target if _unreferenced_marker_for_target(target) == marker_key else None


def _unreferenced_marker_payload(target_key: str) -> bytes:
    return (
        json.dumps(
            {"schema": 1, "target": target_key},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("ascii")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or execute reference-based S3 sandbox snapshot GC"
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--grace-days", default=7.0, type=float)
    parser.add_argument("--max-delete-objects", default=10_000, type=int)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = DeploymentConfig.from_file(args.config.resolve())
    store = config.snapshot_store
    if store.kind != "s3":
        raise ValueError("deployment snapshot store is not S3")
    access_key_id = os.environ.get(store.access_key_id_env, "").strip()
    secret_access_key = os.environ.get(store.secret_access_key_env, "").strip()
    security_token = os.environ.get(store.security_token_env, "").strip()
    if not access_key_id or not secret_access_key:
        raise ValueError("configured S3 snapshot credentials are unavailable")
    client = Boto3S3ObjectClient(
        endpoint=store.endpoint,
        bucket=store.bucket,
        region=store.region,
        credentials={
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
            "security_token": security_token,
        },
    )
    expected_repository = f"{store.bucket}/{store.prefix}"
    publications: list[StorageSnapshotPublication] = []
    for route in RoutingStore(config.routing_file()).sandbox_routes_readonly():
        if not route.storage_snapshot:
            continue
        publication = StorageNativeMigration.from_dict(
            route.storage_snapshot
        ).publication
        if publication.backend == "s3" and publication.repository == expected_repository:
            publications.append(publication)
    plan = plan_s3_snapshot_gc(
        client,
        prefix=store.prefix,
        publications=publications,
        now=time.time(),
        grace_seconds=args.grace_days * 24 * 60 * 60,
    )
    result = plan.to_dict()
    result["executed"] = bool(args.execute)
    result["deletedObjects"] = (
        execute_s3_snapshot_gc(
            client, plan, max_delete_objects=args.max_delete_objects
        )
        if args.execute
        else 0
    )
    print(json.dumps(result, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
