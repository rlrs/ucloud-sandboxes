"""Resolve client image names to protected, worker-pullable references."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import Event, RLock, Thread
import time
from typing import Any
from urllib.parse import urlparse

from ..image_inventory_cache import ImageInventoryCache, ImageInventorySnapshot
from ..images import ImageManager, ImageRecord
from ..managed_registry import (
    RegistryClient, RegistryManifestLayers, RegistryRequestError, canonical_image_digest_ref,
    image_ref_with_manifest_digest, manifest_digest_from_image_ref, normalize_manifest_digest,
    registry_host_from_image_ref, registry_repository_tag_from_image_ref, registry_summary,
)
from ..registry_disk import RegistryDiskMonitor, RegistryDiskUsage
from .exchange import Exchange
from .fleet import FleetView, _node_metadata
from .node_rpc import _header_value
from .registry_refs import _managed_registry_image_coordinates, _managed_registry_worker_reference

IMAGE_REFERENCE_KIND_HEADER = "X-UCloud-Image-Reference-Kind"
MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE = (
    "managed_registry_digest_protection_unavailable"
)
TRANSIENT_IMAGE_RESOLUTION_ERROR_CODES = frozenset(
    {
        "image_inventory_incomplete",
        MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE,
    }
)
REGISTRY_METRICS_TIMEOUT_SECONDS = 1.5
REGISTRY_STATUS_CACHE_TTL_SECONDS = 30.0
REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS = 2.0
REGISTRY_LAYER_METADATA_CACHE_MAX_ENTRIES = 4096
REGISTRY_MANIFEST_CACHE_MAX_ENTRIES = 4096
REGISTRY_IMMUTABLE_MANIFEST_CACHE_TTL_SECONDS = 5 * 60.0
REGISTRY_MUTABLE_MANIFEST_CACHE_TTL_SECONDS = 5.0
IMAGE_EVICTED_ERROR_CODE = "image_evicted"
IMAGE_INVENTORY_CACHE_TTL_SECONDS = 5.0


class RegistryLayerMetadataCache:
    """Bounded immutable-manifest cache used by placement scoring."""

    def __init__(
        self,
        registry_url: str,
        *,
        registry_worker_url: str | None = None,
        max_entries: int = 4096,
    ) -> None:
        self.registry_url = registry_url.rstrip("/")
        self.registry_worker_url = (registry_worker_url or "").rstrip("/")
        self.max_entries = max(1, int(max_entries))
        self._lock = RLock()
        self._records: OrderedDict[str, RegistryManifestLayers] = OrderedDict()
        self._loading: dict[str, Event] = {}

    def get(
        self,
        image_ref: str,
        *,
        load: bool = False,
    ) -> RegistryManifestLayers | None:
        coordinates = self._coordinates(image_ref)
        if coordinates is None:
            return None
        key, repository, digest = coordinates
        waiter: Event | None = None
        with self._lock:
            if key in self._records:
                record = self._records.pop(key)
                self._records[key] = record
                return record
            if key in self._loading:
                if load:
                    waiter = self._loading[key]
                else:
                    return None
            elif not load:
                return None
            else:
                self._loading[key] = Event()
        if waiter is not None:
            waiter.wait(REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS)
            with self._lock:
                return self._records.get(key)
        return self._load_one(key, repository, digest)

    def hydrate_async(self, image_refs: tuple[str, ...]) -> None:
        pending: list[tuple[str, str, str]] = []
        with self._lock:
            for image_ref in image_refs:
                coordinates = self._coordinates(image_ref)
                if coordinates is None:
                    continue
                key, repository, digest = coordinates
                if key in self._records or key in self._loading:
                    continue
                self._loading[key] = Event()
                pending.append((key, repository, digest))
        if not pending:
            return
        Thread(
            target=self._hydrate,
            args=(tuple(pending),),
            daemon=True,
            name="registry-layer-metadata",
        ).start()

    def _hydrate(self, pending: tuple[tuple[str, str, str], ...]) -> None:
        for key, repository, digest in pending:
            self._load_one(key, repository, digest)

    def _load_one(
        self,
        key: str,
        repository: str,
        digest: str,
    ) -> RegistryManifestLayers | None:
        record: RegistryManifestLayers | None = None
        try:
            record = RegistryClient(
                self.registry_url,
                timeout_seconds=REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS,
            ).manifest_layers(repository, digest)
        except (OSError, RegistryRequestError, ValueError):
            record = None
        finally:
            waiter: Event | None = None
            with self._lock:
                waiter = self._loading.pop(key, None)
                if record is not None:
                    self._records[key] = record
                    while len(self._records) > self.max_entries:
                        self._records.popitem(last=False)
            if waiter is not None:
                waiter.set()
        return record

    def _coordinates(self, image_ref: str) -> tuple[str, str, str] | None:
        coordinates = _managed_registry_image_coordinates(
            image_ref,
            self.registry_url,
            self.registry_worker_url,
        )
        digest = manifest_digest_from_image_ref(image_ref)
        if coordinates is None or not digest:
            return None
        repository, _tag = coordinates
        key = canonical_image_digest_ref(image_ref)
        if not key:
            return None
        return key, repository, digest


@dataclass(frozen=True)
class RegistryManifestResolution:
    digest: str
    expires_at: float


class RegistryManifestResolutionCache:
    """Bound repeated verification/protection work for managed manifests."""

    def __init__(self, *, max_entries: int = 4096) -> None:
        self.max_entries = max(1, int(max_entries))
        self._lock = RLock()
        self._records: OrderedDict[tuple[str, str], RegistryManifestResolution] = (
            OrderedDict()
        )

    def get(self, repository: str, reference: str) -> str:
        key = (repository, reference)
        now = time.monotonic()
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return ""
            if record.expires_at <= now:
                self._records.pop(key, None)
                return ""
            self._records.move_to_end(key)
            return record.digest

    def clear(self) -> None:
        with self._lock:
            self._records.clear()

    def put(self, repository: str, reference: str, digest: str) -> None:
        normalized = normalize_manifest_digest(digest)
        if not normalized:
            return
        immutable = bool(normalize_manifest_digest(reference))
        ttl_seconds = (
            REGISTRY_IMMUTABLE_MANIFEST_CACHE_TTL_SECONDS
            if immutable
            else REGISTRY_MUTABLE_MANIFEST_CACHE_TTL_SECONDS
        )
        key = (repository, reference)
        with self._lock:
            self._records[key] = RegistryManifestResolution(
                digest=normalized,
                expires_at=time.monotonic() + ttl_seconds,
            )
            self._records.move_to_end(key)
            while len(self._records) > self.max_entries:
                self._records.popitem(last=False)


class ImageResolution:
    """Turn image ids and tags into digest-pinned references workers can pull.

    Owns the fleet image inventory cache, the Registry status cache, the
    managed-manifest cache and the eviction epoch that flushes it, so one
    instance per server is shared by every request thread. Node RPCs go
    through the caller's Exchange, passed per call and never stored.
    """

    def __init__(
        self, *, image_manager: ImageManager, registry_url: str | None,
        registry_worker_url: str | None, disk_monitor: RegistryDiskMonitor | None,
        fleet: FleetView,
    ) -> None:
        self.image_manager = image_manager
        self.registry_url = registry_url
        self.registry_worker_url = registry_worker_url
        self.disk_monitor = disk_monitor
        self.fleet = fleet
        self.inventory_cache = ImageInventoryCache(ttl_seconds=IMAGE_INVENTORY_CACHE_TTL_SECONDS)
        self.manifest_cache = RegistryManifestResolutionCache(
            max_entries=REGISTRY_MANIFEST_CACHE_MAX_ENTRIES,
        ) if registry_url else None
        # The disk monitor's last eviction the manifest cache was flushed for.
        self.eviction_epoch = ""
        self._status_lock = RLock()
        self._status: tuple[float, dict[str, Any]] | None = None

    def is_managed(self, image_ref: str) -> bool:
        """Whether the reference targets this deployment's managed Registry."""
        return bool(self.registry_url and _managed_registry_image_coordinates(
            image_ref, self.registry_url, self.registry_worker_url or "") is not None)

    def inventory(self, ex: Exchange) -> dict[str, Any]:
        snapshot = self.cached_raw_inventory(ex)
        return {"images": self.enrich_records(snapshot.records), "complete": snapshot.complete}

    def cached_raw_inventory(self, ex: Exchange) -> ImageInventorySnapshot:
        return self.inventory_cache.get_or_load(lambda: self.load_raw_inventory(ex))

    def invalidate_inventory(self) -> None:
        self.inventory_cache.invalidate()

    def load_raw_inventory(self, ex: Exchange) -> ImageInventorySnapshot:
        images: list[dict[str, Any]] = []
        for record in sorted(self.image_manager.list(), key=lambda item: item.id):
            raw = record.to_dict()
            raw["location"] = "control-plane"
            images.append(raw)
        complete = True
        unobserved_references: set[str] = set()
        for heartbeat in self.fleet.ready_heartbeats():
            response = ex._proxy_request(heartbeat.node_url or "", "/v1/images", method="GET")
            if response.status >= 400:
                complete = False
                if heartbeat.cached_images_known:
                    unobserved_references.update(heartbeat.cached_images)
                continue
            payload = response.json()
            raw_images = payload.get("images")
            if not isinstance(raw_images, list):
                complete = False
                if heartbeat.cached_images_known:
                    unobserved_references.update(heartbeat.cached_images)
                continue
            for record in raw_images:
                if not isinstance(record, dict):
                    complete = False
                    if heartbeat.cached_images_known:
                        unobserved_references.update(heartbeat.cached_images)
                    continue
                raw = dict(record)
                raw["node"] = _node_metadata(heartbeat)
                images.append(raw)
        return ImageInventorySnapshot.from_records(
            images, complete=complete, unobserved_references=unobserved_references,
        )

    def enrich_records(
        self, records: tuple[dict[str, Any], ...], *, image_id: str | None = None,
    ) -> list[dict[str, Any]]:
        images: list[dict[str, Any]] = []
        for raw in records:
            if image_id is not None and raw.get("id") != image_id:
                continue
            record = dict(raw)
            if self.record_missing_manifest(record):
                if record.get("location") == "control-plane":
                    tag = str(record.get("tag") or "")
                    if tag:
                        self.image_manager.store.delete_by_tags([tag])
                continue
            images.append(self.record_with_digest(record))
        return images

    def registry_status(self) -> dict[str, Any]:
        result = self.registry_catalog_status()
        monitor = self.disk_monitor
        result["disk"] = monitor.status() if monitor is not None else None
        return result

    def registry_catalog_status(self) -> dict[str, Any]:
        empty = {
            "configured": bool(self.registry_url), "ok": False, "url": self.registry_url or "",
            "repository_count": 0, "scanned_repository_count": 0, "scanned_tag_count": 0,
            "visible_tag_count": 0, "catalog_truncated": False, "repositories": [],
        }
        if not self.registry_url:
            return empty
        client = RegistryClient(self.registry_url, timeout_seconds=REGISTRY_METRICS_TIMEOUT_SECONDS)
        try:
            return registry_summary(client)
        except Exception as exc:
            return {**empty, "error": str(exc)}

    def registry_status_cached(self, *, force_refresh: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        with self._status_lock:
            if (
                not force_refresh
                and self._status is not None
                and now - self._status[0] <= REGISTRY_STATUS_CACHE_TTL_SECONDS
            ):
                return {**self._status[1], "cached": True}
            result = self.registry_status()
            result["cached"] = False
            self._status = (now, dict(result))
            return result

    def managed_manifest_digest(self, image_ref: str) -> str:
        try:
            digest = self.resolve_and_protect_manifest(image_ref)
        except (OSError, ValueError, RegistryRequestError):
            return ""
        existing = manifest_digest_from_image_ref(image_ref)
        if existing and digest != existing:
            return ""
        return digest

    def resolve_and_protect_manifest(self, image_ref: str) -> str:
        existing = manifest_digest_from_image_ref(image_ref)
        if not self.registry_url:
            return existing
        coordinates = _managed_registry_image_coordinates(
            image_ref, self.registry_url, self.registry_worker_url or "",
        )
        if coordinates is None:
            return existing
        repository, image_tag = coordinates
        reference = existing or image_tag
        cache = self.manifest_cache_current()
        if cache is not None:
            cached = cache.get(repository, reference)
            if cached:
                return cached
        client = RegistryClient(self.registry_url)
        digest = normalize_manifest_digest(client.manifest_digest(repository, reference))
        if not digest or (existing and digest != existing):
            return ""
        client.ensure_digest_protection_tag(repository, digest)
        if cache is not None:
            cache.put(repository, reference, digest)
            cache.put(repository, digest, digest)
        return digest

    def record_with_digest(self, record: dict[str, Any]) -> dict[str, Any]:
        updated = dict(record)
        existing = normalize_manifest_digest(str(record.get("manifest_digest") or ""))
        tag = str(record.get("tag") or "")
        digest = self.managed_manifest_digest(
            image_ref_with_manifest_digest(tag, existing) if existing else tag
        )
        managed_record = self.is_managed(tag)
        if not digest and not managed_record:
            digest = existing
        if digest:
            updated["manifest_digest"] = digest
        elif managed_record:
            # Never advertise an unprotected managed digest retained in a
            # builder/node response from before protection was established.
            updated["manifest_digest"] = ""
        return updated

    def manifest_cache_current(self) -> RegistryManifestResolutionCache | None:
        """The manifest cache, emptied after each disk-pressure eviction.

        Eviction runs in the root maintenance unit; a cached resolution would
        otherwise keep pinning creates to a deleted manifest for minutes.
        """

        cache = self.manifest_cache
        monitor = self.disk_monitor
        if cache is None or monitor is None:
            return cache
        epoch = str(monitor.maintenance_state().get("last_eviction_at") or "")
        if epoch != self.eviction_epoch:
            cache.clear()
            self.eviction_epoch = epoch
        return cache

    def evicted_image_error(self, image: str) -> dict[str, Any] | None:
        monitor = self.disk_monitor
        if monitor is None or not _looks_like_image_id_reference(image):
            return None
        record = monitor.evicted_image(image)
        if record is None:
            return None
        return {
            "error": (
                f"image {image} was evicted from the registry under disk "
                f"pressure at {record.get('evicted_at')}; build it again"
            ),
            "error_code": IMAGE_EVICTED_ERROR_CODE,
            "retryable": False,
            "rebuild_required": True,
            "image_id": image,
        }

    def disk_refusal(self) -> RegistryDiskUsage | None:
        monitor = self.disk_monitor
        return monitor.refusal() if monitor is not None else None

    def resolve(
        self, ex: Exchange, image: str, *, reference_kind: str = "auto",
    ) -> tuple[str, dict[str, Any] | None]:
        if reference_kind not in {"auto", "name", "registry"}:
            raise ValueError(f"unsupported image reference kind: {reference_kind!r}")
        existing_digest = manifest_digest_from_image_ref(image)
        if existing_digest:
            protected_digest = self.managed_manifest_digest(image)
            if self.is_managed(image) and protected_digest != existing_digest:
                return image, _digest_protection_unavailable(image=image)
            return self.worker_reference(image), None
        if reference_kind != "name":
            direct_digest = self.managed_manifest_digest(image)
            if direct_digest:
                return self.worker_reference(
                    image_ref_with_manifest_digest(image, direct_digest)
                ), None
            if reference_kind == "registry":
                return image, None
            if not _looks_like_image_id_reference(image):
                return image, None
        # The gateway's published record already wins inventory selection.
        # Read that exact row before discovering copies across the fleet.
        local = self.image_manager.get_image(image)
        local_matches = []
        if local is not None and _image_record_available_to_sandboxes(local.to_dict()):
            local_matches = self.enrich_records(
                ({**local.to_dict(), "location": "control-plane"},), image_id=image,
            )
        inventory = (
            ImageInventorySnapshot.from_records(local_matches, complete=True)
            if local_matches else self.cached_raw_inventory(ex)
        )
        matches = local_matches or self.enrich_records(inventory.records, image_id=image)
        if not matches:
            evicted = self.evicted_image_error(image)
            if evicted is not None:
                return image, evicted
            if reference_kind == "name":
                if not inventory.complete:
                    return image, _incomplete_image_inventory_error(image)
                return image, {
                    "error": f"gateway image id was not found: {image}",
                    "error_code": "image_id_not_found",
                    "retryable": False,
                    "image_id": image,
                }
            if (
                not inventory.complete
                and image in inventory.unobserved_references
                and self.is_known_successful_build(image)
            ):
                return image, _incomplete_image_inventory_error(image)
            return image, None
        available = [
            record
            for record in matches
            if _image_record_available_to_sandboxes(record)
            and isinstance(record.get("tag"), str)
            and record.get("tag")
        ]
        if available:
            selected = sorted(
                available,
                key=lambda record: (
                    0 if record.get("location") == "control-plane" else 1,
                    str(record.get("tag") or ""),
                ),
            )[0]
            selected_tag = str(selected["tag"])
            digest = normalize_manifest_digest(str(selected.get("manifest_digest") or ""))
            if not digest and self.is_managed(selected_tag):
                return image, _digest_protection_unavailable(image_id=image)
            if digest and selected.get("location") == "control-plane":
                try:
                    self.image_manager.store.upsert(ImageRecord.from_dict(selected))
                except ValueError:
                    pass
            return self.worker_reference(
                image_ref_with_manifest_digest(selected_tag, digest)
            ), None
        return image, {
            "error": (
                "image id exists, but it is not available to sandbox nodes; "
                "resubmit the gateway-managed build, then create the sandbox "
                "with that image id"
            ),
            "image_id": image,
            "matches": [_image_record_summary(record) for record in matches],
        }

    def is_known_successful_build(self, image: str) -> bool:
        get_build = getattr(self.image_manager, "get_build", None)
        if not callable(get_build):
            return False
        try:
            build = get_build(image)
        except (OSError, TypeError, ValueError):
            return False
        return bool(
            build is not None
            and getattr(build, "image_id", None) == image
            and getattr(build, "status", None) == "succeeded"
        )

    def worker_reference(self, image_ref: str) -> str:
        if not self.registry_url:
            return image_ref
        return _managed_registry_worker_reference(
            image_ref, self.registry_url, self.registry_worker_url or "",
        )

    def record_missing_manifest(self, record: dict[str, Any]) -> bool:
        tag = str(record.get("tag") or "")
        if not self.registry_url or not _image_record_requires_registry_manifest(
            record, self.registry_url, self.registry_worker_url or "",
        ):
            return False
        parsed = registry_repository_tag_from_image_ref(tag)
        if parsed is None:
            return False
        try:
            recorded_digest = normalize_manifest_digest(str(record.get("manifest_digest") or ""))
            resolved_digest = self.resolve_and_protect_manifest(
                image_ref_with_manifest_digest(tag, recorded_digest) if recorded_digest else tag
            )
            if recorded_digest:
                return normalize_manifest_digest(resolved_digest) != recorded_digest
            normalized_digest = normalize_manifest_digest(resolved_digest)
            if normalized_digest:
                record["manifest_digest"] = normalized_digest
                return False
            return True
        except RegistryRequestError as exc:
            return exc.status_code == 404
        except (OSError, ValueError):
            return False


def _digest_protection_unavailable(**identity: str) -> dict[str, Any]:
    return {
        "error": "managed registry digest protection is unavailable",
        "error_code": MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE,
        "retryable": True,
        **identity,
    }


def _image_reference_kind_from_headers(headers: Any) -> str:
    raw = _header_value(headers, IMAGE_REFERENCE_KIND_HEADER).strip().lower()
    if not raw:
        return "auto"
    if raw not in {"auto", "name", "registry"}:
        raise ValueError(
            f"{IMAGE_REFERENCE_KIND_HEADER} must be 'auto', 'name', or 'registry'"
        )
    return raw


def _incomplete_image_inventory_error(image: str) -> dict[str, Any]:
    return {
        "error": (
            "image inventory is temporarily incomplete; image id could not be resolved"
        ),
        "error_code": "image_inventory_incomplete",
        "retryable": True,
        "image_id": image,
    }


def _looks_like_image_id_reference(image: str) -> bool:
    return (
        bool(image.strip())
        and "/" not in image
        and ":" not in image
        and "@" not in image
    )


def _image_record_available_to_sandboxes(record: dict[str, Any]) -> bool:
    return bool(
        record.get("available_to_sandboxes")
        or record.get("pushed")
        or record.get("source") == "registry"
    )


def _image_record_requires_registry_manifest(
    record: dict[str, Any],
    registry_url: str,
    registry_worker_url: str = "",
) -> bool:
    if not _image_record_available_to_sandboxes(record):
        return False
    source = str(record.get("source") or "")
    if not source.startswith("build:"):
        return False
    host = registry_host_from_image_ref(str(record.get("tag") or ""))
    if not host:
        return False
    allowed: set[str] = set()
    for configured_url in (registry_url, registry_worker_url):
        configured = urlparse(configured_url).netloc
        if configured:
            allowed.add(configured)
    return host in allowed


def _image_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "id": record.get("id"),
        "tag": record.get("tag"),
        "source": record.get("source"),
        "pushed": bool(record.get("pushed")),
        "available_to_sandboxes": _image_record_available_to_sandboxes(record),
    }
    if record.get("manifest_digest"):
        summary["manifest_digest"] = record.get("manifest_digest")
    node = record.get("node")
    if isinstance(node, dict):
        summary["node"] = {
            "node_id": node.get("node_id"),
            "job_id": node.get("job_id"),
        }
    if record.get("location"):
        summary["location"] = record.get("location")
    return summary
