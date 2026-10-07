"""Durable Registry owners that keep route, snapshot and pull images alive."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse

from ..host_locks import HOST_LOCKS
from ..images import ImageBuildSpec
from ..managed_registry import (
    RegistryUsageStateError, RegistryUsageStore, digest_protection_tag,
    image_ref_with_manifest_digest, manifest_digest_from_image_ref,
    registry_host_from_image_ref, registry_repository_tag_from_image_ref,
)
from ..models import parse_iso_datetime, utc_now
from ..routing import SandboxRoute, is_portable_parked_route
from ..sandbox import sandbox_spec_fingerprint
from ..storage_native_migration import StorageNativeMigration


REGISTRY_IMAGE_LEASE_TTL_SECONDS = 60 * 60


class RegistryImageReferenceUnavailable(RuntimeError):
    pass


class RegistryReferences:
    """Acquire and release this deployment's Registry owners.

    Holds configuration and the usage store only, so every request thread
    shares one instance. Each ensure_* raises RegistryImageReferenceUnavailable
    when protection cannot be persisted; callers must not publish the route.
    """

    def __init__(
        self, *, registry_url: str | None, registry_worker_url: str | None,
        usage_store: RegistryUsageStore | None, deployment_id: str, dependency_resolver: Any,
    ) -> None:
        self.registry_url = registry_url
        self.registry_worker_url = registry_worker_url
        self.usage_store = usage_store
        self.deployment_id = deployment_id
        self.dependency_resolver = dependency_resolver

    def usage_health_error(self) -> str:
        store = self.usage_store
        if store is None:
            return ""
        try:
            store.check_readable()
        except (OSError, sqlite3.DatabaseError, RegistryUsageStateError, ValueError):
            return "state file is unavailable"
        return ""

    def managed_coordinates(self, image_ref: str) -> tuple[str, str] | None:
        return _managed_registry_image_coordinates(
            image_ref, self.registry_url or "", self.registry_worker_url or "")

    def requires_digest_identity(self, image: str) -> bool:
        """A managed tag can move under a heartbeat, so only a digest is a hit."""
        return bool(self.registry_url and _managed_registry_image_coordinates(
            image, self.registry_url, self.registry_worker_url or "") is not None)

    def record_image_used(self, image_ref: str) -> None:
        if self.usage_store is None:
            return
        if self.managed_coordinates(image_ref) is None:
            return
        try:
            self.usage_store.touch_image(image_ref)
        except (OSError, ValueError):
            return

    def ensure_image_lease(self, image_ref: str, owner: str, *, touch: bool) -> None:
        store = self.usage_store
        if store is None:
            return
        if self.managed_coordinates(image_ref) is None:
            return
        try:
            _persist_registry_image_protection(
                store, image_ref, owner, touch=touch, persistent=False,
                dependency_resolver=self.dependency_resolver,
            )
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-use state could not be persisted"
            ) from exc

    def ensure_image_reference(self, image_ref: str, owner: str) -> None:
        """A durable reference, released only by its owner: a pinned image recipe."""
        store = self.usage_store
        if store is None or self.managed_coordinates(image_ref) is None:
            return
        try:
            _persist_registry_image_protection(
                store, image_ref, owner, touch=True, persistent=True,
                dependency_resolver=self.dependency_resolver,
            )
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-use state could not be persisted"
            ) from exc

    def ensure_route_reference(self, route: SandboxRoute, *, touch: bool) -> None:
        image_ref = str(route.spec.get("image") or "")
        store = self.usage_store
        if store is None:
            return
        if image_ref and self.managed_coordinates(image_ref) is not None:
            try:
                _persist_registry_image_protection(
                    store, image_ref,
                    _registry_route_reference_owner(
                        route, deployment_id=self.deployment_id, route_generation=route.generation),
                    touch=touch, persistent=True, dependency_resolver=self.dependency_resolver,
                )
            except (OSError, TypeError, ValueError) as exc:
                raise RegistryImageReferenceUnavailable(
                    "registry route image reference could not be persisted"
                ) from exc
        if route.snapshot_repository and route.snapshot_tag and route.snapshot_manifest_digest:
            self.ensure_snapshot_reference(
                route, repository=route.snapshot_repository, tag=route.snapshot_tag,
                digest=route.snapshot_manifest_digest,
            )

    def protect_build_target(self, spec: ImageBuildSpec, *, push: bool) -> None:
        if (
            not push
            or self.usage_store is None
            or not self.registry_url
            or _managed_registry_image_coordinates(
                spec.tag, self.registry_url, self.registry_worker_url or "") is None
        ):
            return
        try:
            touched = self.usage_store.touch_image(spec.tag)
            if touched is None:
                raise ValueError("registry image-build target could not be recorded")
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-build target could not be protected"
            ) from exc

    def release_route_reference(
        self, route: SandboxRoute, *, keep_route: SandboxRoute | None = None,
    ) -> None:
        store = self.usage_store
        if store is not None:
            release_registry_route_references(
                store, route, deployment_id=self.deployment_id, keep_route=keep_route)

    def ensure_snapshot_reference(
        self, route: SandboxRoute, *, repository: str, tag: str, digest: str,
    ) -> None:
        store = self.usage_store
        if store is None:
            return
        if not repository or not tag or not digest:
            raise RegistryImageReferenceUnavailable(
                "snapshot registry identity is incomplete"
            )
        try:
            owner = _registry_snapshot_reference_owner(route, deployment_id=self.deployment_id)
            references = [(repository, tag, digest)]
            if route.storage_snapshot:
                snapshot = StorageNativeMigration.from_dict(route.storage_snapshot)
                if (snapshot.reference.repository, snapshot.reference.tag,
                    snapshot.reference.manifest_digest) != (repository, tag, digest):
                    raise ValueError("checkpoint lease identity does not match its descriptor")
                references = [(ref.repository, ref.tag, ref.manifest_digest)
                              for ref in snapshot.references]
            # Partial acquisition leaks protection conservatively; never release
            # uncertain dependencies before a complete route transition commits.
            with _registry_lease_coordination(owner):
                for ref_repository, ref_tag, ref_digest in references:
                    store.acquire_reference(ref_repository, ref_tag, owner, digest=ref_digest)
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "snapshot registry reference could not be persisted"
            ) from exc

    def release_snapshot_reference(
        self, route: SandboxRoute, *, keep_route: SandboxRoute | None = None,
    ) -> None:
        store = self.usage_store
        if store is not None:
            release_registry_snapshot_reference(
                store, route, deployment_id=self.deployment_id, keep_route=keep_route)


def _portable_snapshot_for_route(route: SandboxRoute) -> StorageNativeMigration:
    if not is_portable_parked_route(route):
        raise ValueError("sandbox route is not a fully published park")
    snapshot = StorageNativeMigration.from_dict(route.storage_snapshot)
    manifest = snapshot.manifest
    publication = snapshot.reference
    if (
        manifest.sandbox_id != route.sandbox_id
        or manifest.sandbox_generation != route.generation
        or manifest.create_operation_id != route.create_operation_id
        or sandbox_spec_fingerprint(manifest.spec) != route.spec_hash
        or publication.manifest_digest != route.snapshot_manifest_digest
        or publication.repository != route.snapshot_repository
        or publication.tag != route.snapshot_tag
    ):
        raise ValueError("published snapshot does not match its sandbox route")
    return snapshot


def _registry_lease_coordination(owner: str):
    """One owner's check-then-push-then-lease sequences, across gateway processes.

    Owners are disjoint, so creates never wait for each other's registry I/O.
    Prune and OCI release fence through the usage store's ``lease_fence``.
    """
    return HOST_LOCKS.hold("registry-leases", owner)


def _private_registry_image_coordinates(
    image_ref: str,
) -> tuple[str, str] | None:
    # A host-qualified reference is the strongest signal currently available
    # that this request depends on a registry rather than a public shorthand
    # such as ``ubuntu:latest``. Repositories not present in the managed
    # registry are harmless: their leases never match a prune candidate.
    if not registry_host_from_image_ref(image_ref):
        return None
    return registry_repository_tag_from_image_ref(image_ref)


def _managed_registry_image_coordinates(
    image_ref: str,
    registry_url: str,
    registry_worker_url: str = "",
) -> tuple[str, str] | None:
    """Return coordinates only when the tag targets this managed registry."""

    image_host = registry_host_from_image_ref(image_ref).lower()
    if not image_host:
        return None
    allowed_hosts: set[str] = set()
    for configured_url in (registry_url, registry_worker_url):
        configured_host = urlparse(configured_url).netloc.lower()
        if configured_host:
            allowed_hosts.add(configured_host)
    if image_host not in allowed_hosts:
        return None
    return registry_repository_tag_from_image_ref(image_ref)


def _managed_registry_build_tag(image_id: str, registry_worker_url: str) -> str:
    """Allocate a stable internal tag without exposing registry naming to clients."""

    host = urlparse(registry_worker_url).netloc
    if not host:
        raise ValueError("gateway-managed image builds require a worker registry URL")
    component = "".join(
        character.lower() if character.isalnum() else "-"
        for character in image_id.strip()
    ).strip("-")
    component = component[:40].rstrip("-") or "image"
    suffix = hashlib.sha256(image_id.encode("utf-8")).hexdigest()[:12]
    return f"{host}/ucloud-managed/{component}-{suffix}:latest"


def _managed_registry_worker_reference(
    image_ref: str,
    registry_url: str,
    registry_worker_url: str,
) -> str:
    """Rewrite a managed image reference for worker transport."""

    if not registry_worker_url:
        return image_ref
    coordinates = _managed_registry_image_coordinates(
        image_ref,
        registry_url,
        registry_worker_url,
    )
    worker_host = urlparse(registry_worker_url).netloc
    if coordinates is None or not worker_host:
        return image_ref
    repository, tag = coordinates
    rewritten = f"{worker_host}/{repository}:{tag}"
    digest = manifest_digest_from_image_ref(image_ref)
    return image_ref_with_manifest_digest(rewritten, digest) if digest else rewritten


def _persist_registry_image_protection(
    store: RegistryUsageStore,
    image_ref: str,
    owner: str,
    *,
    touch: bool,
    persistent: bool,
    now: Any | None = None,
    ttl_seconds: float = REGISTRY_IMAGE_LEASE_TTL_SECONDS,
    dependency_resolver: Any = None,
    lease_key: str = "",
) -> bool:
    """Persist either a durable reference or a finite transient lease."""

    coordinates = _private_registry_image_coordinates(image_ref)
    if coordinates is None:
        return False
    repository, tag = coordinates
    digest = manifest_digest_from_image_ref(image_ref)
    lease_key = lease_key or owner  # The primary's lock covers its dependency owner.
    with _registry_lease_coordination(lease_key):
        # Acquire artifact closure before publishing its primary image owner.
        # Exact persisted dependency-owner rows survive tag/annotation changes;
        # release never has to ask a mutable source what used to be retained.
        if dependency_resolver is not None:
            dependencies = dependency_resolver(image_ref)
            for dependency_repository, dependency_tag, dependency_digest in dependencies:
                dependency_owner = owner + ":environment"
                if store.get_lease(dependency_repository, dependency_tag, dependency_owner, now=now) is None:
                    dependency_resolver.ensure_reference(dependency_repository, dependency_tag, dependency_digest)
                _persist_registry_image_protection(
                    store, f"{registry_host_from_image_ref(image_ref)}/{dependency_repository}:{dependency_tag}@{dependency_digest}",
                    dependency_owner, touch=touch, persistent=persistent, now=now,
                    ttl_seconds=ttl_seconds, lease_key=lease_key,
                )
        if touch:
            usage_refs = [image_ref]
            if digest:
                usage_refs.append(f"{repository}:{digest_protection_tag(digest)}")
            touched = store.touch_images(usage_refs, when=now)
            if len(touched) != len(usage_refs):
                raise ValueError("private-registry image could not be recorded")
        timestamp = now or utc_now()
        existing = store.get_lease(repository, tag, owner, now=timestamp)
        digest_matches = not digest or (
            existing is not None and existing.digest == digest
        )
        if existing is not None and not existing.expires_at and digest_matches:
            return True
        if persistent:
            store.acquire_reference(
                repository,
                tag,
                owner,
                digest=digest,
                now=timestamp,
            )
            return True
        ttl_seconds = float(ttl_seconds)
        if existing is not None:
            existing_expiry = parse_iso_datetime(existing.expires_at)
            if existing_expiry is not None:
                remaining = max(
                    0.0,
                    (existing_expiry - timestamp).total_seconds(),
                )
                # Heartbeats arrive far more frequently than the lease TTL.
                # Renew only after half the lifetime has elapsed to avoid an
                # fsync/generation bump on every node report.
                if remaining >= ttl_seconds / 2 and digest_matches:
                    return True
                # Never replace an existing lease with an earlier deadline,
                # including leases created with a longer TTL.
                ttl_seconds = max(ttl_seconds, remaining)
        store.acquire_lease(
            repository,
            tag,
            owner,
            ttl_seconds=ttl_seconds,
            digest=digest,
            now=timestamp,
        )
    return True


def _registry_route_reference_owner(
    route: SandboxRoute,
    *,
    deployment_id: str,
    route_generation: int | str | None = None,
) -> str:
    """Return a restart-stable, generation-specific route incarnation owner."""

    effective_generation = (
        route.generation if route_generation is None else route_generation
    )

    identity = {
        "kind": "sandbox-route",
        "version": 1,
        "deployment_id": deployment_id,
        "sandbox_id": route.sandbox_id,
        "node_id": route.node_id,
        "job_id": route.job_id,
        "route_generation": (
            str(effective_generation) if effective_generation is not None else ""
        ),
        "route_created_at": route.created_at,
        "image": str(route.spec.get("image") or ""),
    }
    return _registry_operation_lease_owner("sandbox-route", identity)


def _registry_snapshot_reference_owner(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> str:
    return _registry_operation_lease_owner(
        "sandbox-snapshot",
        {
            "version": 1,
            "deployment_id": deployment_id,
            "sandbox_id": route.sandbox_id,
            "generation": route.generation,
            "create_operation_id": route.create_operation_id,
            "node_id": route.node_id,
            "job_id": route.job_id,
        },
    )


def _registry_route_image_reference_key(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> tuple[str, str, str] | None:
    coordinates = _private_registry_image_coordinates(
        str(route.spec.get("image") or "")
    )
    if coordinates is None:
        return None
    repository, tag = coordinates
    return (
        repository,
        tag,
        _registry_route_reference_owner(
            route,
            deployment_id=deployment_id,
            route_generation=route.generation,
        ),
    )


def _registry_snapshot_reference_key(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> tuple[str, str, str] | None:
    if not route.snapshot_repository or not route.snapshot_tag:
        return None
    return (
        route.snapshot_repository,
        route.snapshot_tag,
        _registry_snapshot_reference_owner(route, deployment_id=deployment_id),
    )


def _registry_snapshot_reference_keys(
    route: SandboxRoute, *, deployment_id: str,
) -> tuple[tuple[str, str, str], ...]:
    reference = _registry_snapshot_reference_key(route, deployment_id=deployment_id)
    if reference is None:
        return ()
    if not route.storage_snapshot:
        return (reference,)
    try:
        snapshot = StorageNativeMigration.from_dict(route.storage_snapshot)
        if (snapshot.reference.repository, snapshot.reference.tag) != reference[:2]:
            return (reference,)
    except (ValueError, TypeError):
        return (reference,)
    return tuple((ref.repository, ref.tag, reference[2]) for ref in snapshot.references)


def _registry_route_reference_keys(
    route: SandboxRoute, *, deployment_id: str,
) -> tuple[tuple[str, str, str], ...]:
    image = _registry_route_image_reference_key(route, deployment_id=deployment_id)
    return ((image,) if image is not None else ()) + _registry_snapshot_reference_keys(
        route, deployment_id=deployment_id)


def _release_registry_reference_keys(
    store: RegistryUsageStore,
    references: set[tuple[str, str, str]],
    *,
    image_owners: frozenset[str] = frozenset(),
) -> None:
    for repository, tag, owner in sorted(references):
        try:
            with _registry_lease_coordination(owner):
                store.release_lease(repository, tag, owner)
                if owner in image_owners:
                    store.release_owner(owner + ":environment")
        except (AttributeError, OSError, TypeError, ValueError):
            # A leaked durable reference is conservative. Explicit
            # reconciliation may remove it after proving the owner terminal.
            continue


def release_registry_snapshot_reference(
    store: RegistryUsageStore,
    route: SandboxRoute,
    *,
    deployment_id: str,
    keep_route: SandboxRoute | None = None,
) -> None:
    """Release one route's durable snapshot owner, if present."""

    references = set(_registry_snapshot_reference_keys(route, deployment_id=deployment_id))
    if keep_route is not None:
        references.difference_update(_registry_snapshot_reference_keys(
            keep_route, deployment_id=deployment_id))
    _release_registry_reference_keys(store, references)


def release_registry_route_references(
    store: RegistryUsageStore,
    route: SandboxRoute,
    *,
    deployment_id: str,
    keep_route: SandboxRoute | None = None,
) -> None:
    """Release route owners that are not shared by a successor route.

    ``keep_route`` makes migration transition and rollback safe even when a
    detached sandbox is re-adopted by the same node and therefore retains one
    or both deterministic Registry owner keys.
    """

    references = set(_registry_route_reference_keys(route, deployment_id=deployment_id))
    if keep_route is not None:
        references.difference_update(
            _registry_route_reference_keys(
                keep_route,
                deployment_id=deployment_id,
            )
        )
    image = _registry_route_image_reference_key(route, deployment_id=deployment_id)
    image_owners = frozenset({image[2]}) if image in references else frozenset()
    _release_registry_reference_keys(store, references, image_owners=image_owners)


def _registry_operation_lease_owner(kind: str, identity: object) -> str:
    encoded = json.dumps(
        {"kind": kind, "identity": identity},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{kind}:v1:{digest}"
