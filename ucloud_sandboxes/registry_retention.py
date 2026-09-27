"""Reference-based retention for registry repositories with durable owners.

Age rules cannot bound the snapshot and environment repositories: every park
publishes a new snapshot manifest into one repository, and environments are
shared by images of any age. Their manifests are instead live while something
references them:

- a snapshot manifest while a sandbox route, a worker-reported storage
  dependency, or an active migration names its tag or digest;
- an environment root while a tagged managed image carries its
  ``org.ucloud.immutable-environment.v1`` annotation, and each component while
  a live root lists it.

Everything else becomes deletable once its newest known registry time is
older than a grace period. Registry leases still fence every deletion.

Under disk pressure, managed images are additionally evicted least recently
used first (``select_lru_evictions``); see docs/managed-registry.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .environment_artifact import ENVIRONMENT_ANNOTATION
from .managed_registry import (
    RegistryClient,
    RegistryImageUsage,
    RegistryRequestError,
    RegistryTag,
    RegistryUsageStore,
    _registry_repository_name_unknown,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
    registry_repository_tag_from_image_ref,
)
from .models import parse_iso_datetime


SNAPSHOT_REASON = "unreferenced_snapshot"
ENVIRONMENT_REASON = "unreferenced_environment"
# Each batch holds the usage-store writer lock through its registry DELETEs.
REFERENCE_PRUNE_BATCH = 64
# The snapshot repository holds tens of thousands of tags; keep output bounded.
DECISION_SAMPLE_SIZE = 20
_MAX_ENVIRONMENT_ROOT_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ReferenceRetentionDecision:
    reason: str
    repository: str
    delete: tuple[RegistryTag, ...] = ()
    kept: Mapping[str, int] = field(default_factory=dict)
    skipped: str = ""
    # Layer bytes referenced by the selected manifests, when cheap to read.
    # An upper bound on what GC reclaims: live manifests may share blobs.
    delete_layer_bytes: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "repository": self.repository,
            "skipped": self.skipped,
            "delete_manifests": len({record.digest for record in self.delete}),
            "delete_tags": len(self.delete),
            "delete_layer_bytes": self.delete_layer_bytes,
            "kept_manifests": dict(sorted(self.kept.items())),
            "delete_sample": [
                record.to_dict() for record in self.delete[:DECISION_SAMPLE_SIZE]
            ],
        }


def manifest_layer_bytes(
    client: RegistryClient,
    repository: str,
    digests: Iterable[str],
) -> int:
    total = 0
    for digest in dict.fromkeys(digests):
        try:
            document, _headers = client.manifest_document(repository, digest)
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                continue
            raise
        for layer in document.get("layers") or ():
            if isinstance(layer, dict) and type(layer.get("size")) is int:
                total += max(0, layer["size"])
    return total


def referenced_strings(value: object) -> set[str]:
    """Every string inside a JSON-like value.

    Snapshot descriptors nest their registry identities (split checkpoints,
    memory artifacts, compacted layers) in several schema versions. Treating
    every contained string as a potential tag or digest over-retains a little
    and never misses an identity a newer schema adds.
    """

    found: set[str] = set()
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            found.add(item)
        elif isinstance(item, Mapping):
            pending.extend(item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend(item)
    return found


def snapshot_live_identities(routing_store: Any) -> set[str]:
    """Tags and digests of every snapshot a route, sandbox, or migration needs.

    Raises when a sandbox has not reported its storage dependencies: a woken
    sandbox may still read lower layers of the snapshot it was restored from.
    """

    live: set[str] = set()
    for snapshot in routing_store.storage_snapshot_dependencies_readonly(
        require_complete=True
    ):
        live |= referenced_strings(snapshot)
    for route in routing_store.sandbox_routes_readonly():
        live |= {route.snapshot_tag, route.snapshot_manifest_digest}
        live |= referenced_strings(route.storage_snapshot)
    for migration in routing_store.sandbox_migrations(active_only=True):
        live |= referenced_strings(migration.storage_snapshot)
    live.discard("")
    return live


def list_repository_tags(client: RegistryClient, repository: str) -> list[RegistryTag]:
    """Tag to digest for one repository, one HEAD per tag and no config reads."""

    try:
        tags = client.tags(repository)
    except RegistryRequestError as exc:
        if _registry_repository_name_unknown(exc):
            return []
        raise
    records: list[RegistryTag] = []
    for tag in tags:
        try:
            digest = client.manifest_digest(repository, tag)
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                continue
            raise
        if digest:
            records.append(RegistryTag(repository=repository, tag=tag, digest=digest))
    return records


class RegistryTagClock:
    """Newest known write or use time of a registry tag.

    Docker Distribution's filesystem driver rewrites a tag's ``current/link``
    whenever the tag is pushed, so its mtime is the registry's own tag time.
    Image config ``created`` and gateway usage records are fallbacks; a tag
    with no known time is never old enough to delete.
    """

    def __init__(
        self,
        registry_data_dir: Path | None,
        usage_records: Mapping[tuple[str, str], RegistryImageUsage] | None = None,
    ) -> None:
        self.repositories_dir = (
            registry_data_dir / "docker" / "registry" / "v2" / "repositories"
            if registry_data_dir is not None
            else None
        )
        self.usage_records = usage_records or {}

    def __call__(self, record: RegistryTag) -> datetime | None:
        candidates: list[datetime] = []
        if self.repositories_dir is not None:
            link = (
                self.repositories_dir.joinpath(*record.repository.split("/"))
                / "_manifests" / "tags" / record.tag / "current" / "link"
            )
            try:
                candidates.append(
                    datetime.fromtimestamp(link.stat().st_mtime, tz=timezone.utc)
                )
            except (OSError, ValueError):
                pass
        usage = self.usage_records.get((record.repository, record.tag))
        for raw in (record.created_at, usage.last_used_at if usage else ""):
            parsed = parse_iso_datetime(raw) if raw else None
            if parsed is not None:
                candidates.append(
                    parsed.replace(tzinfo=timezone.utc)
                    if parsed.tzinfo is None
                    else parsed.astimezone(timezone.utc)
                )
        return max(candidates) if candidates else None


def select_unreferenced(
    records: Iterable[RegistryTag],
    *,
    reason: str,
    repository: str,
    live: set[str],
    grace_seconds: float,
    tag_time: Callable[[RegistryTag], datetime | None],
    leased_digests: set[tuple[str, str]] = frozenset(),
    now: datetime | None = None,
) -> ReferenceRetentionDecision:
    """Select whole digests: deleting one manifest removes every tag alias."""

    reference = now or datetime.now(timezone.utc)
    cutoff = reference.astimezone(timezone.utc) - timedelta(seconds=grace_seconds)
    aliases: dict[str, list[RegistryTag]] = {}
    for record in records:
        if record.repository == repository:
            aliases.setdefault(record.digest, []).append(record)
    kept = {"live": 0, "grace": 0, "leased": 0, "age_unknown": 0}
    delete: list[RegistryTag] = []
    for digest, digest_aliases in aliases.items():
        if digest in live or any(alias.tag in live for alias in digest_aliases):
            kept["live"] += 1
            continue
        if (repository, digest) in leased_digests:
            kept["leased"] += 1
            continue
        times = [tag_time(alias) for alias in digest_aliases]
        if any(item is None for item in times):
            kept["age_unknown"] += 1
            continue
        if any(item >= cutoff for item in times if item is not None):
            kept["grace"] += 1
            continue
        delete.extend(digest_aliases)
    return ReferenceRetentionDecision(
        reason=reason,
        repository=repository,
        delete=tuple(sorted(delete, key=lambda item: (item.digest, item.tag))),
        kept=kept,
    )


class ImageEnvironmentIndex:
    """Environment root annotated on each managed image manifest, memoized.

    Manifests are immutable per digest, so a second scan only reads new ones.
    """

    def __init__(self, client: RegistryClient) -> None:
        self.client = client
        self._roots: dict[tuple[str, str], str] = {}

    def roots(self, records: Iterable[RegistryTag]) -> set[str]:
        found = {self.root(record.repository, record.digest) for record in records}
        found.discard("")
        return found

    def root(self, repository: str, digest: str) -> str:
        key = (repository, digest)
        if key not in self._roots:
            self._roots[key] = self._root(*key)
        return self._roots[key]

    def _root(self, repository: str, digest: str) -> str:
        try:
            document, _headers = self.client.manifest_document(repository, digest)
        except RegistryRequestError as exc:
            if exc.status_code == 404:
                return ""
            raise
        annotations = document.get("annotations")
        if not isinstance(annotations, dict):
            return ""
        root = annotations.get(ENVIRONMENT_ANNOTATION)
        return normalize_manifest_digest(root) if isinstance(root, str) else ""




def environment_components(
    client: RegistryClient,
    repository: str,
    root: str,
) -> tuple[str, ...] | None:
    """Base, workspace, and toolkit digests of one root; None when it is gone.

    Liveness needs no producer trust: a forged root can only retain more.
    Malformed roots raise so that nothing is deleted on a guess.
    """

    try:
        document, _headers = client.manifest_document(repository, root)
    except RegistryRequestError as exc:
        if exc.status_code == 404:
            return None
        raise
    config = document.get("config")
    if not isinstance(config, dict):
        raise ValueError(f"environment root {root} has no config")
    size = config.get("size")
    digest = config.get("digest")
    if type(size) is not int or not 0 < size <= _MAX_ENVIRONMENT_ROOT_BYTES:
        raise ValueError(f"environment root {root} has an invalid config size")
    payload = json.loads(client.blob_bytes(repository, str(digest), max_bytes=size))
    environment = payload.get("environment") if isinstance(payload, dict) else None
    if not isinstance(environment, dict):
        raise ValueError(f"environment root {root} has no environment manifest")
    toolkits = environment.get("toolkits") or []
    if not isinstance(toolkits, list):
        raise ValueError(f"environment root {root} has invalid toolkits")
    components: list[str] = []
    for component in (environment.get("base"), environment.get("workspace"), *toolkits):
        if component is None:
            continue
        normalized = normalize_manifest_digest(component) if isinstance(component, str) else ""
        if not normalized:
            raise ValueError(f"environment root {root} has an invalid component")
        components.append(normalized)
    return tuple(components)


def environment_live_identities(
    client: RegistryClient,
    repository: str,
    roots: Iterable[str],
) -> set[str]:
    """Live roots plus their component digests.

    Per-layer images share components: a base's ``layer-*`` components are
    live while any live root lists them, however old their index tag.
    """

    live: set[str] = set()
    for root in roots:
        live.add(root)
        live.update(environment_components(client, repository, root) or ())
    return live


def still_unreferenced_environment(
    fresh_live: set[str],
    tag_time: Callable[[RegistryTag], datetime | None],
    cutoff: datetime,
) -> Callable[[RegistryTag], bool]:
    """Delete-time recheck: no fresh root lists it and nobody re-tagged it.

    A builder reusing a ``layer-*`` component re-puts its tag before it
    publishes the root that will list it; a tag written since planning
    therefore keeps the component through this run.
    """

    def check(record: RegistryTag) -> bool:
        if record.digest in fresh_live:
            return False
        written = tag_time(record)
        return written is not None and written < cutoff

    return check


# Least-recently-used eviction of managed images under disk pressure.
#
# Workloads that build one image per task push each image's Docker layers
# plus its EROFS environment copy; age rules never free them during a run.
# Above the cleanup threshold the pressure unit evicts managed images oldest
# use first until the projected usage reaches the target. An image is never
# evicted while leased, while a route, prepared capacity, or warmup names it,
# or within the grace period of its last push or use.

MANAGED_IMAGE_REPOSITORY_PREFIX = "ucloud-managed/"
LRU_EVICTION_REASON = "lru_evicted_image"


@dataclass(frozen=True)
class ManagedImage:
    repository: str
    digest: str
    tags: tuple[RegistryTag, ...]
    last_used: datetime | None
    # Blob digest to size: Docker layers and config plus the whole-image blobs
    # of its environment components, which GC frees with the last owner.
    blobs: Mapping[str, int] = field(default_factory=dict)
    environment_root: str = ""


@dataclass(frozen=True)
class LruEvictionPlan:
    evict: tuple[ManagedImage, ...]
    projected_freed_bytes: int
    target_used_bytes: int
    used_bytes: int
    kept: Mapping[str, int]

    @property
    def records(self) -> tuple[RegistryTag, ...]:
        return tuple(tag for image in self.evict for tag in image.tags)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": LRU_EVICTION_REASON,
            "used_bytes": self.used_bytes,
            "target_used_bytes": self.target_used_bytes,
            "projected_freed_bytes": self.projected_freed_bytes,
            "evict_images": len(self.evict),
            "kept_images": dict(sorted(self.kept.items())),
            "evict_sample": [
                {
                    "repository": image.repository,
                    "digest": image.digest,
                    "tags": [tag.tag for tag in image.tags],
                    "last_used": image.last_used.isoformat() if image.last_used else "",
                }
                for image in self.evict[:DECISION_SAMPLE_SIZE]
            ],
        }


def image_reference_identities(image_refs: Iterable[str]) -> set[str]:
    """``repository@digest`` and ``repository:tag`` keys of image references."""

    live: set[str] = set()
    for image_ref in image_refs:
        if not isinstance(image_ref, str) or not image_ref.strip():
            continue
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        if coordinates is None:
            continue
        repository, tag = coordinates
        live.add(f"{repository}:{tag}")
        digest = manifest_digest_from_image_ref(image_ref)
        if digest:
            live.add(f"{repository}@{digest}")
    return live


def routing_image_identities(routing_store: Any) -> set[str]:
    """Images that routes, prepared capacity, and warmups still name."""

    state = routing_store.load()
    refs: list[str] = []
    for route in state.sandboxes.values():
        spec = route.spec if isinstance(route.spec, Mapping) else {}
        refs.append(str(spec.get("image") or ""))
    refs.extend(str(getattr(item, "image", "") or "") for item in state.prepared.values())
    refs.extend(str(getattr(item, "image", "") or "") for item in state.image_warmups.values())
    return image_reference_identities(refs)


def image_is_referenced(image: ManagedImage, live: set[str]) -> bool:
    return f"{image.repository}@{image.digest}" in live or any(
        f"{image.repository}:{tag.tag}" in live for tag in image.tags
    )


def managed_images(
    records: Iterable[RegistryTag],
    *,
    tag_time: Callable[[RegistryTag], datetime | None],
    blobs: Callable[[str, str], Mapping[str, int]] | None = None,
    environment_root: Callable[[RegistryTag], str] | None = None,
) -> list[ManagedImage]:
    grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in records:
        if record.repository.startswith(MANAGED_IMAGE_REPOSITORY_PREFIX):
            grouped.setdefault((record.repository, record.digest), []).append(record)
    images: list[ManagedImage] = []
    for (repository, digest), tags in grouped.items():
        times = [tag_time(tag) for tag in tags]
        images.append(ManagedImage(
            repository=repository,
            digest=digest,
            tags=tuple(sorted(tags, key=lambda item: item.tag)),
            # One unknown alias time makes the whole image's age unknown.
            last_used=None if any(item is None for item in times) else max(times),
            blobs=dict(blobs(repository, digest)) if blobs is not None else {},
            environment_root=environment_root(tags[0]) if environment_root else "",
        ))
    return images


def select_lru_evictions(
    images: Iterable[ManagedImage],
    *,
    used_bytes: int,
    target_used_bytes: int,
    grace_seconds: float,
    live: set[str] = frozenset(),
    leased_digests: set[tuple[str, str]] = frozenset(),
    now: datetime | None = None,
) -> LruEvictionPlan:
    """Evict oldest-used first until projected usage reaches the target.

    A blob counts as freed only when its last retained owner is evicted, so
    shared base layers and environment components are not double counted.
    """

    reference = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = reference - timedelta(seconds=grace_seconds)
    images = list(images)
    owners: dict[str, int] = {}
    sizes: dict[str, int] = {}
    for image in images:
        for blob, size in image.blobs.items():
            owners[blob] = owners.get(blob, 0) + 1
            sizes[blob] = max(sizes.get(blob, 0), int(size))
    kept = {"live": 0, "leased": 0, "grace": 0, "age_unknown": 0, "target_reached": 0}
    candidates: list[ManagedImage] = []
    for image in images:
        if image_is_referenced(image, live):
            kept["live"] += 1
        elif (image.repository, image.digest) in leased_digests:
            kept["leased"] += 1
        elif image.last_used is None:
            kept["age_unknown"] += 1
        elif image.last_used >= cutoff:
            kept["grace"] += 1
        else:
            candidates.append(image)
    candidates.sort(key=lambda image: (image.last_used, image.repository, image.digest))
    freed = 0
    evict: list[ManagedImage] = []
    for image in candidates:
        if used_bytes - freed <= target_used_bytes:
            kept["target_reached"] += 1
            continue
        evict.append(image)
        for blob in image.blobs:
            owners[blob] -= 1
            if owners[blob] == 0:
                freed += sizes[blob]
    return LruEvictionPlan(
        evict=tuple(evict),
        projected_freed_bytes=freed,
        target_used_bytes=target_used_bytes,
        used_bytes=used_bytes,
        kept=kept,
    )


def manifest_blob_sizes(
    client: RegistryClient,
    repository: str,
    digest: str,
) -> dict[str, int]:
    """Layer and config blobs of one manifest, following a platform index."""

    try:
        document, _headers = client.manifest_document(repository, digest)
    except RegistryRequestError as exc:
        if exc.status_code == 404:
            return {}
        raise
    blobs: dict[str, int] = {}
    manifests = document.get("manifests")
    if isinstance(manifests, list):
        # An index owns every child: buildx attestations are GC'd with it.
        for child in manifests:
            if isinstance(child, dict) and isinstance(child.get("digest"), str):
                blobs.update(manifest_blob_sizes(client, repository, child["digest"]))
        return blobs
    for descriptor in (document.get("config"), *(document.get("layers") or ())):
        if (
            isinstance(descriptor, dict)
            and isinstance(descriptor.get("digest"), str)
            and type(descriptor.get("size")) is int
        ):
            blobs[descriptor["digest"]] = max(0, descriptor["size"])
    return blobs


class EnvironmentBlobIndex:
    """Whole-image blob sizes of each environment root's components."""

    def __init__(self, client: RegistryClient, repository: str) -> None:
        self.client = client
        self.repository = repository
        self._roots: dict[str, dict[str, int]] = {}
        self._components: dict[str, dict[str, int]] = {}

    def blobs(self, root: str) -> dict[str, int]:
        if not root:
            return {}
        if root not in self._roots:
            blobs: dict[str, int] = {}
            for component in environment_components(self.client, self.repository, root) or ():
                if component not in self._components:
                    self._components[component] = manifest_blob_sizes(
                        self.client, self.repository, component
                    )
                blobs.update(self._components[component])
            self._roots[root] = blobs
        return self._roots[root]


def execute_reference_prune(
    client: RegistryClient,
    records: ReferenceRetentionDecision | Iterable[RegistryTag],
    *,
    usage_store: RegistryUsageStore | None,
    still_unreferenced: Callable[[RegistryTag], bool] = lambda _record: True,
    unused_since: datetime | None = None,
    batch_size: int = REFERENCE_PRUNE_BATCH,
    now: datetime | None = None,
) -> list[RegistryTag]:
    """Delete selected digests, fenced by active leases in bounded batches.

    Unlike age pruning there is no usage-generation precondition: every image
    touch advances the generation, and thousands of snapshot deletions would
    never finish. Liveness is revalidated by the caller's fresh reference set;
    each digest's leases, and with ``unused_since`` its aliases' last use, are
    checked inside the writer transaction that also covers the DELETE.
    """

    if isinstance(records, ReferenceRetentionDecision):
        records = records.delete
    grouped: dict[tuple[str, str], list[RegistryTag]] = {}
    for record in records:
        grouped.setdefault((record.repository, record.digest), []).append(record)
    keys = list(grouped)
    step = max(1, batch_size)
    deleted: list[RegistryTag] = []
    for start in range(0, len(keys), step):
        batch = keys[start:start + step]
        if usage_store is None:
            deleted.extend(_delete_batch(
                client, batch, grouped, leased=set(), usage={},
                unused_since=None, still_unreferenced=still_unreferenced,
            ))
            continue
        with usage_store.lease_fence(now=now) as snapshot:
            deleted.extend(_delete_batch(
                client, batch, grouped,
                leased=snapshot.active_lease_digests(now=now),
                usage=snapshot.records,
                unused_since=unused_since,
                still_unreferenced=still_unreferenced,
            ))
    return deleted


def _delete_batch(
    client: RegistryClient,
    keys: list[tuple[str, str]],
    grouped: Mapping[tuple[str, str], list[RegistryTag]],
    *,
    leased: set[tuple[str, str]],
    usage: Mapping[tuple[str, str], RegistryImageUsage],
    unused_since: datetime | None,
    still_unreferenced: Callable[[RegistryTag], bool],
) -> list[RegistryTag]:
    deleted: list[RegistryTag] = []
    for repository, digest in keys:
        aliases = grouped[(repository, digest)]
        if (repository, digest) in leased or not all(
            still_unreferenced(alias) for alias in aliases
        ):
            continue
        if unused_since is not None and any(
            _used_since(usage.get((alias.repository, alias.tag)), unused_since)
            for alias in aliases
        ):
            # A create touched this image after planning.
            continue
        try:
            client.delete_manifest(repository, digest)
        except RegistryRequestError as exc:
            if exc.status_code != 404:
                raise
            # Already gone: another prune or an operator deleted it.
            continue
        deleted.extend(aliases)
    return deleted


def _used_since(usage: RegistryImageUsage | None, cutoff: datetime) -> bool:
    if usage is None:
        return False
    used = parse_iso_datetime(usage.last_used_at)
    if used is None:
        return True
    if used.tzinfo is None:
        used = used.replace(tzinfo=timezone.utc)
    return used >= cutoff
