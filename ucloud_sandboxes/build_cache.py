"""Disposable, shared BuildKit registry caches with conservative retention.

The byte budget counts unique compressed blob descriptors reachable from retained
cache manifests. It is not a filesystem quota: uploads and registry garbage
collection can temporarily leave more data on disk. Only the reserved cache
repository and this module's immutable tag namespace are eligible for pruning.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
import time
from typing import Any, Callable
from urllib.parse import quote, urlparse
from uuid import uuid4

from .managed_registry import (
    MAX_REGISTRY_JSON_RESPONSE_BYTES,
    RegistryClient,
    RegistryRequestError,
    _CaseInsensitiveHeaders,
    _next_link_path,
    _read_response_bytes,
    normalize_manifest_digest,
)


DEFAULT_CACHE_MAX_BYTES = 32 * 1024**3
DEFAULT_CACHE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
DEFAULT_CACHE_MAX_ENTRIES = 64
DEFAULT_CACHE_IMPORT_LIMIT = 8
MAX_CACHE_INVENTORY_TAGS = 4096
MAX_CACHE_INVENTORY_PAGES = 16
MAX_CACHE_LAYERS = 10_000
CACHE_CONFIG_MEDIA_TYPE = "application/vnd.buildkit.cacheconfig.v0"
CACHE_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
_OWNED_TAG = re.compile(r"^bc1-([0-9a-f]{16})-([0-9]{10})-([0-9a-f]{32})$")
_TAG = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$")
_REPOSITORY_PART = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$")
_INVENTORY_TIMEOUT_SECONDS = 3.0


@dataclass(frozen=True)
class BuildCachePlan:
    imports: tuple[str, ...]
    export_ref: str


@dataclass(frozen=True)
class _CacheManifest:
    digest: str
    tags: tuple[str, ...]
    created_at: int
    blobs: dict[str, int]
    protected: bool


class _CacheRegistryClient(RegistryClient):
    def _json_request(self, path: str, *, headers: dict[str, str] | None = None):
        # Chunked reads prevent a trickling response from holding up optional
        # cache preparation indefinitely. A blocking read may overshoot the
        # deadline by one socket timeout, as with other registry bounded reads.
        deadline = time.monotonic() + self.timeout_seconds
        response = self._request(path, headers=headers)
        try:
            body = _read_response_bytes(response, MAX_REGISTRY_JSON_RESPONSE_BYTES + 1, deadline=deadline)
            response_headers = _CaseInsensitiveHeaders(response.headers.items())
        finally:
            response.close()
        if len(body) > MAX_REGISTRY_JSON_RESPONSE_BYTES:
            raise ValueError("BuildKit cache registry response exceeds the byte limit")
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("BuildKit cache registry response must be an object")
        return payload, response_headers


class RegistryBuildCache:
    def __init__(
        self,
        ref: str,
        *,
        max_bytes: int = DEFAULT_CACHE_MAX_BYTES,
        max_age_seconds: int = DEFAULT_CACHE_MAX_AGE_SECONDS,
        max_entries: int = DEFAULT_CACHE_MAX_ENTRIES,
        import_limit: int = DEFAULT_CACHE_IMPORT_LIMIT,
        registry_url: str | None = None,
        client: RegistryClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        authority, repository = _cache_repository(ref)
        for name, value in (
            ("max_bytes", max_bytes),
            ("max_age_seconds", max_age_seconds),
            ("max_entries", max_entries),
            ("import_limit", import_limit),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"BuildKit cache {name} must be a positive integer")
        if import_limit > DEFAULT_CACHE_IMPORT_LIMIT:
            raise ValueError("BuildKit cache import_limit must not exceed 8")
        base_url = registry_url or f"https://{authority}"
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("BuildKit cache registry URL must use HTTP or HTTPS")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("BuildKit cache registry URL must not contain credentials or query")
        self.repository = repository
        self.repository_ref = f"{authority}/{repository}"
        self.max_bytes = max_bytes
        self.max_age_seconds = max_age_seconds
        self.max_entries = max_entries
        self.import_limit = import_limit
        self.client = client or _CacheRegistryClient(base_url, timeout_seconds=_INVENTORY_TIMEOUT_SECONDS)
        self._clock = clock

    def prepare(self, recipe_key: str) -> BuildCachePlan:
        """Choose recent caches and allocate an independent tag for this writer."""
        now = int(self._clock())
        recipe = hashlib.sha256(recipe_key.encode("utf-8")).hexdigest()[:16]
        eligible: list[tuple[int, str, str]] = []
        for tag in self._tags(deadline=time.monotonic() + _INVENTORY_TIMEOUT_SECONDS):
            match = _OWNED_TAG.fullmatch(tag)
            if match is None:
                continue
            timestamp = int(match[2])
            if now - self.max_age_seconds <= timestamp <= now:
                eligible.append((timestamp, tag, match[1]))
        eligible.sort(reverse=True)
        matching = next((entry for entry in eligible if entry[2] == recipe), None)
        selected = ([matching] if matching else []) + [
            entry for entry in eligible if entry != matching
        ]
        return BuildCachePlan(
            imports=tuple(
                f"{self.repository_ref}:{entry[1]}"
                for entry in selected[: self.import_limit]
            ),
            export_ref=f"{self.repository_ref}:bc1-{recipe}-{now:010d}-{uuid4().hex}",
        )

    def prune(self, *, execute: bool = False) -> dict[str, Any]:
        """Plan or remove cache manifests; reject incomplete or changed inventories.

        Deleting a manifest removes all its aliases. Unknown tags always protect
        their digest, including aliases of a tag that otherwise looks owned.
        Concurrent publication can still race a registry DELETE, since the API
        has no conditional delete; caches are disposable and imports tolerate a
        miss. A digest not present in the original inventory is never deleted.
        """
        now = int(self._clock())
        inventory = self._inventory()
        manifests = self._cache_manifests(inventory, now)
        retained: list[_CacheManifest] = []
        candidates: list[_CacheManifest] = []
        blobs: dict[str, int] = {}
        retained_entries = 0
        for manifest in sorted(manifests, key=lambda entry: (not entry.protected, -entry.created_at, entry.digest)):
            additions = {
                digest: size for digest, size in manifest.blobs.items() if digest not in blobs
            }
            expired = manifest.created_at < now - self.max_age_seconds
            fits = (
                retained_entries + len(manifest.tags) <= self.max_entries
                and sum(blobs.values()) + sum(additions.values()) <= self.max_bytes
            )
            if manifest.protected or (not expired and fits):
                retained.append(manifest)
                blobs.update(additions)
                retained_entries += len(manifest.tags)
            else:
                candidates.append(manifest)
        summary: dict[str, Any] = {
            "repository": self.repository,
            "execute": execute,
            "inventoried_tags": len(inventory),
            "retained_entries": retained_entries,
            "retained_bytes": sum(blobs.values()),
            "protected_entries": sum(len(entry.tags) for entry in retained if entry.protected),
            "candidate_tags": sorted(tag for entry in candidates for tag in entry.tags),
            "candidate_digests": [entry.digest for entry in candidates],
            "deleted_digests": [],
            "deleted_manifests": 0,
            "max_bytes": self.max_bytes,
            "max_entries": self.max_entries,
            "max_age_seconds": self.max_age_seconds,
            "physical_reclamation_requires_registry_gc": True,
        }
        if not execute or not candidates:
            return summary
        # Fully validate before the first mutation: partial/paged scans, renamed
        # aliases and concurrent publishers must never widen the deletion set.
        if self._inventory() != inventory:
            raise ValueError("BuildKit cache inventory changed; pruning deferred")
        for entry in candidates:
            try:
                for tag in entry.tags:
                    if self._digest(tag) != entry.digest:
                        raise ValueError("BuildKit cache tag changed; pruning deferred")
                self.client.delete_manifest(self.repository, entry.digest)
            except (OSError, ValueError, RuntimeError) as exc:
                if not summary["deleted_manifests"]:
                    raise
                # Report confirmed deletions even when a later request fails,
                # so the maintenance caller still schedules blob reclamation.
                summary["error"] = type(exc).__name__
                summary["incomplete"] = True
                return summary
            summary["deleted_digests"].append(entry.digest)
            summary["deleted_manifests"] += 1
        return summary

    def _tags(self, *, deadline: float | None = None) -> list[str]:
        # RegistryClient.tags intentionally tolerates malformed entries for
        # read-only image listings. A deleting cache policy needs strict pages.
        path_prefix = f"/v2/{quote(self.repository, safe='/')}/tags/list"
        path = f"{path_prefix}?n=1000"
        found: set[str] = set()
        visited: set[str] = set()
        while path:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("BuildKit cache preparation exceeded its inventory deadline")
            if path in visited or len(visited) >= MAX_CACHE_INVENTORY_PAGES:
                raise ValueError("BuildKit cache inventory pagination is incomplete")
            if urlparse(path).path != path_prefix:
                raise ValueError("BuildKit cache pagination left the cache repository")
            visited.add(path)
            try:
                payload, headers = self.client._json_request(path)
            except RegistryRequestError as exc:
                if exc.status_code == 404 and len(visited) == 1:
                    return []
                raise
            if payload.get("name") != self.repository or "tags" not in payload:
                raise ValueError("BuildKit cache inventory is malformed")
            tags = payload["tags"]
            if tags is None:
                tags = []
            if not isinstance(tags, list) or any(
                not isinstance(tag, str) or not _TAG.fullmatch(tag) for tag in tags
            ):
                raise ValueError("BuildKit cache inventory contains invalid tags")
            if len(set(tags)) != len(tags) or found.intersection(tags):
                raise ValueError("BuildKit cache inventory contains repeated tags")
            found.update(tags)
            if len(found) > MAX_CACHE_INVENTORY_TAGS:
                raise ValueError("BuildKit cache inventory exceeds the tag limit")
            link = headers.get("Link", "")
            path = _next_link_path(link, current_path=path, base_url=self.client.base_url)
            if link and not path:
                raise ValueError("BuildKit cache inventory has an invalid pagination link")
        return sorted(found)

    def _digest(self, tag: str) -> str:
        digest = normalize_manifest_digest(self.client.manifest_digest(self.repository, tag))
        if not digest:
            raise ValueError("BuildKit cache inventory is missing a manifest digest")
        return digest

    def _inventory(self) -> dict[str, str]:
        return {tag: self._digest(tag) for tag in self._tags()}

    def _cache_manifests(self, inventory: dict[str, str], now: int) -> list[_CacheManifest]:
        aliases: dict[str, list[str]] = {}
        for tag, digest in inventory.items():
            aliases.setdefault(digest, []).append(tag)
        result: list[_CacheManifest] = []
        blob_sizes: dict[str, int] = {}
        for digest, tags in aliases.items():
            owned = [(tag, _OWNED_TAG.fullmatch(tag)) for tag in tags]
            owned = [(tag, match) for tag, match in owned if match is not None]
            if not owned:
                continue
            created_at = max(int(match[2]) for _tag, match in owned)
            if created_at > now + 300:
                raise ValueError("BuildKit cache tag has an invalid future timestamp")
            manifest, headers = self.client.manifest_document(self.repository, digest)
            returned_digest = normalize_manifest_digest(str(headers.get("Docker-Content-Digest", "")))
            if returned_digest != digest:
                raise ValueError("BuildKit cache manifest digest does not match the inventory")
            if manifest.get("schemaVersion") != 2 or manifest.get("mediaType") != CACHE_MANIFEST_MEDIA_TYPE:
                raise ValueError("BuildKit cache manifest must be a flat OCI image manifest")
            config = manifest.get("config")
            if not isinstance(config, dict) or config.get("mediaType") != CACHE_CONFIG_MEDIA_TYPE:
                raise ValueError("BuildKit cache tag does not contain a BuildKit cache manifest")
            layers = manifest.get("layers")
            if not isinstance(layers, list) or len(layers) > MAX_CACHE_LAYERS:
                raise ValueError("BuildKit cache manifest has an invalid layer inventory")
            blobs: dict[str, int] = {}
            for descriptor in [config, *layers]:
                if not isinstance(descriptor, dict):
                    raise ValueError("BuildKit cache manifest has an invalid blob descriptor")
                blob_digest = normalize_manifest_digest(str(descriptor.get("digest", "")))
                size = descriptor.get("size")
                if not blob_digest or type(size) is not int or size < 0:
                    raise ValueError("BuildKit cache manifest has an invalid blob size or digest")
                if blob_digest in blob_sizes and blob_sizes[blob_digest] != size:
                    raise ValueError("BuildKit cache manifests disagree about a blob size")
                blobs[blob_digest] = size
                blob_sizes[blob_digest] = size
            result.append(_CacheManifest(
                digest=digest,
                tags=tuple(tag for tag, _match in owned),
                created_at=created_at,
                blobs=blobs,
                protected=len(owned) != len(tags),
            ))
        return result


def _cache_repository(ref: str) -> tuple[str, str]:
    if not isinstance(ref, str) or not ref or ref != ref.strip() or "://" in ref or "@" in ref:
        raise ValueError("BuildKit cache reference must be a registry image reference")
    authority, separator, path = ref.partition("/")
    if not separator or not authority or not path:
        raise ValueError("BuildKit cache reference must include the registry host")
    parsed = urlparse(f"https://{authority}")
    try:
        valid_authority = parsed.hostname and parsed.port != 0
    except ValueError:
        valid_authority = False
    if not valid_authority or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("BuildKit cache reference has an invalid registry host")
    repository, colon, tag = path.rpartition(":")
    if not colon:
        repository = path
    elif not _TAG.fullmatch(tag):
        raise ValueError("BuildKit cache reference has an invalid tag")
    parts = repository.split("/")
    if not all(_REPOSITORY_PART.fullmatch(part) for part in parts):
        raise ValueError("BuildKit cache reference has an invalid repository")
    if parts[-1] != "ucloud-build-cache" and not parts[-1].startswith("ucloud-build-cache-"):
        raise ValueError("BuildKit caches require a dedicated ucloud-build-cache repository")
    return authority, repository
