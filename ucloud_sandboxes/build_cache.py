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
    MANIFEST_ACCEPT,
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
_OWNED_TAG_V1 = re.compile(r"^bc1-([0-9a-f]{16})-([0-9]{10})-([0-9a-f]{32})$")
_OWNED_TAG_V2 = re.compile(r"^bc2-([0-9a-f]{16})-([0-9a-f]{32})-([0-9]{10})-([0-9a-f]{32})$")
_AFFINITY_KEY = re.compile(r"^[0-9a-f]{64}$")
_TAG = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$")
_REPOSITORY_PART = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$")
_INVENTORY_TIMEOUT_SECONDS = 3.0
_MOUNT_TIMEOUT_SECONDS = 3.0
_MAX_MOUNT_LAYERS = 64
_MAX_MOUNT_MANIFEST_BYTES = 256 * 1024
_MOUNT_LAYER_TYPES = frozenset({
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.oci.image.layer.v1.tar+zstd",
    "application/vnd.docker.image.rootfs.diff.tar",
    "application/vnd.docker.image.rootfs.diff.tar.gzip",
})


@dataclass(frozen=True)
class BuildCachePlan:
    imports: tuple[str, ...]
    export_ref: str
    matching_ref: str = ""
    affinity_match: bool = False


@dataclass(frozen=True)
class _CacheManifest:
    digest: str
    tags: tuple[str, ...]
    created_at: int
    blobs: dict[str, int]
    protected: bool


@dataclass(frozen=True)
class _OwnedCacheTag:
    recipe: str
    affinity: str
    created_at: int


def _parse_owned_tag(tag: str) -> _OwnedCacheTag | None:
    """One ownership definition for cache import, blob mounts and pruning."""
    if match := _OWNED_TAG_V2.fullmatch(tag):
        return _OwnedCacheTag(match[1], match[2], int(match[3]))
    if match := _OWNED_TAG_V1.fullmatch(tag):
        return _OwnedCacheTag(match[1], "", int(match[2]))
    return None


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
        self.authority = authority
        self.repository_ref = f"{authority}/{repository}"
        self.max_bytes = max_bytes
        self.max_age_seconds = max_age_seconds
        self.max_entries = max_entries
        self.import_limit = import_limit
        self.client = client or _CacheRegistryClient(base_url, timeout_seconds=_INVENTORY_TIMEOUT_SECONDS)
        self._clock = clock

    def prepare(self, recipe_key: str, *, affinity_key: str = "") -> BuildCachePlan:
        """Choose recent caches and allocate an independent tag for this writer."""
        if not isinstance(affinity_key, str) or (affinity_key and not _AFFINITY_KEY.fullmatch(affinity_key)):
            raise ValueError("BuildKit cache affinity_key must be a lowercase SHA256 hex digest")
        # This truncated digest is a cache-selection hint, never authority to
        # reuse an image. BuildKit still validates the complete build graph.
        affinity = affinity_key[:32]
        now = int(self._clock())
        recipe = hashlib.sha256(recipe_key.encode("utf-8")).hexdigest()[:16]
        eligible: list[tuple[int, str, _OwnedCacheTag]] = []
        for tag in self._tags(deadline=time.monotonic() + _INVENTORY_TIMEOUT_SECONDS):
            owned = _parse_owned_tag(tag)
            if owned is None:
                continue
            timestamp = owned.created_at
            if now - self.max_age_seconds <= timestamp <= now:
                eligible.append((timestamp, tag, owned))
        eligible.sort(reverse=True)
        matching = next((entry for entry in eligible if entry[2].recipe == recipe), None)
        exact = next((entry for entry in eligible if affinity and entry[2].recipe == recipe
                      and entry[2].affinity == affinity), None)
        # Competing imports can contain the same graph keys with only partial
        # results and prevent BuildKit from using the complete exact cache.
        # Offer that cache alone; BuildKit still validates every build input.
        selection_limit = 1 if exact is not None else self.import_limit
        selected = [exact] if exact else []
        if matching is not None and matching != exact and len(selected) < selection_limit:
            selected.append(matching)
        # Keep useful cross-recipe prefixes without letting a burst from one
        # unrelated recipe occupy every fallback slot. Then fill by recency.
        seen_recipes = {recipe}
        for entry in eligible:
            if len(selected) >= selection_limit:
                break
            if entry[2].recipe not in seen_recipes:
                selected.append(entry)
                seen_recipes.add(entry[2].recipe)
        selected_tags = {entry[1] for entry in selected}
        for entry in eligible:
            if len(selected) >= selection_limit:
                break
            if entry[1] not in selected_tags:
                selected.append(entry)
                selected_tags.add(entry[1])
        preferred = exact or matching
        identity = f"bc2-{recipe}-{affinity}" if affinity else f"bc1-{recipe}"
        return BuildCachePlan(
            imports=tuple(
                f"{self.repository_ref}:{entry[1]}"
                for entry in selected[:selection_limit]
            ),
            export_ref=f"{self.repository_ref}:{identity}-{now:010d}-{uuid4().hex}",
            matching_ref=f"{self.repository_ref}:{preferred[1]}" if preferred else "",
            affinity_match=exact is not None,
        )

    def pre_mount(self, target_ref: str, matching_ref: str) -> dict[str, Any]:
        """Offer existing cache blobs to one managed destination before pushing.

        A fresh BuildKit store may know a layer's public origin but not its
        existing private-registry location. Creating repository links avoids
        uploading those identical bytes again. These links never select build
        results: BuildKit still validates its inputs and pushes its own result.
        Every failure leaves ordinary image push and cache imports available.
        """
        started = time.monotonic()
        deadline = started + _MOUNT_TIMEOUT_SECONDS
        result: dict[str, Any] = {
            "attempted": 0, "mounted": 0, "mounted_descriptor_bytes": 0,
        }
        try:
            target = self._mount_target(target_ref)
            prefix = self.repository_ref + ":"
            if not target or not matching_ref.startswith(prefix):
                result["skipped"] = True
                return result
            tag = matching_ref[len(prefix):]
            if _parse_owned_tag(tag) is None:
                result["skipped"] = True
                return result
            # A single bounded GET captures a complete manifest snapshot. Bind
            # its raw bytes to the registry digest; reserializing JSON would
            # change the digest. A concurrent prune can only make mounts miss.
            response = self.client._request(
                f"/v2/{quote(self.repository, safe='/')}/manifests/{quote(tag, safe='')}",
                headers={"Accept": MANIFEST_ACCEPT},
                timeout_seconds=self._mount_remaining(deadline),
            )
            try:
                payload = _read_response_bytes(response, _MAX_MOUNT_MANIFEST_BYTES + 1, deadline=deadline)
                returned_digest = normalize_manifest_digest(str(response.headers.get("Docker-Content-Digest", "")))
            finally:
                response.close()
            if len(payload) > _MAX_MOUNT_MANIFEST_BYTES:
                raise ValueError("BuildKit mount manifest exceeds the byte limit")
            if returned_digest != "sha256:" + hashlib.sha256(payload).hexdigest():
                raise ValueError("BuildKit mount manifest digest does not match its bytes")
            layers = self._mount_layers(json.loads(payload))
            # Reach the largest repeated uploads first if the budget expires.
            for blob_digest, size in sorted(layers.items(), key=lambda item: (-item[1], item[0])):
                remaining = self._mount_remaining(deadline)
                result["attempted"] += 1
                if self.client.mount_blob(target, self.repository, blob_digest, timeout_seconds=remaining):
                    result["mounted"] += 1
                    # Descriptor accounting, not measured avoided disk writes.
                    result["mounted_descriptor_bytes"] += size
        except (OSError, ValueError, RuntimeError) as exc:
            result["error"] = type(exc).__name__
        finally:
            result["elapsed_ms"] = round(max(0.0, time.monotonic() - started) * 1000, 3)
        return result

    def _mount_target(self, target_ref: str) -> str:
        if not isinstance(target_ref, str) or "://" in target_ref or "@" in target_ref:
            return ""
        authority, separator, path = target_ref.partition("/")
        endpoint = urlparse(self.client.base_url)
        if (not separator or authority != self.authority or endpoint.netloc != self.authority
                or endpoint.path not in {"", "/"} or endpoint.username or endpoint.password
                or endpoint.query or endpoint.fragment):
            return ""
        repository, colon, tag = path.rpartition(":")
        if not colon:
            repository = path
        elif not _TAG.fullmatch(tag):
            return ""
        parts = repository.split("/")
        if len(parts) < 2 or parts[0] != "ucloud-managed" or not all(
            _REPOSITORY_PART.fullmatch(part) for part in parts
        ):
            return ""
        return repository

    @staticmethod
    def _mount_remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("BuildKit cache mounts exceeded the preparation deadline")
        return remaining

    @staticmethod
    def _mount_layers(manifest: Any) -> dict[str, int]:
        if (not isinstance(manifest, dict) or manifest.get("schemaVersion") != 2
                or manifest.get("mediaType") != CACHE_MANIFEST_MEDIA_TYPE):
            raise ValueError("BuildKit mount source must be a flat cache manifest")
        config = manifest.get("config")
        if not isinstance(config, dict) or config.get("mediaType") != CACHE_CONFIG_MEDIA_TYPE:
            raise ValueError("BuildKit mount source has an invalid cache config")
        layers = manifest.get("layers")
        if not isinstance(layers, list) or len(layers) > _MAX_MOUNT_LAYERS:
            raise ValueError("BuildKit mount source exceeds the layer limit")
        blobs: dict[str, int] = {}
        # Validate every descriptor before the first mount, including config.
        for descriptor in [config, *layers]:
            if not isinstance(descriptor, dict):
                raise ValueError("BuildKit mount source has an invalid descriptor")
            blob_digest = normalize_manifest_digest(str(descriptor.get("digest", "")))
            size = descriptor.get("size")
            if not blob_digest or type(size) is not int or size < 0:
                raise ValueError("BuildKit mount source has an invalid digest or size")
            if blob_digest in blobs and blobs[blob_digest] != size:
                raise ValueError("BuildKit mount source has conflicting blob sizes")
            blobs[blob_digest] = size
        for descriptor in layers:
            if descriptor.get("mediaType") not in _MOUNT_LAYER_TYPES:
                raise ValueError("BuildKit mount source has an unsupported layer type")
        return {normalize_manifest_digest(descriptor["digest"]): descriptor["size"] for descriptor in layers}

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
            owned = [(tag, _parse_owned_tag(tag)) for tag in tags]
            owned = [(tag, match) for tag, match in owned if match is not None]
            if not owned:
                continue
            created_at = max(parsed.created_at for _tag, parsed in owned)
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
