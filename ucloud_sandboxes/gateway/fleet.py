"""Freshness-filtered reads of the worker heartbeat store."""

from __future__ import annotations

from typing import Any

from ..capabilities import REQUEST_BODY_KEEPALIVE_CAPABILITY
from ..control_state import ControlStateStore
from ..images import image_id_from_tag
from ..managed_registry import canonical_image_digest_ref, registry_host_from_image_ref
from ..models import NodeHeartbeat, utc_now
from .registry_refs import RegistryReferences


class FleetView:
    """Which workers may take new work, and which already cache an image.

    Reads only the shared heartbeat store, so every request thread shares one
    instance. Per-request transport state derived from a lookup (the pooled
    body origin) stays on the handler.
    """

    def __init__(
        self, store: ControlStateStore, heartbeat_ttl_seconds: int, *,
        registry_refs: RegistryReferences,
    ) -> None:
        self.store = store
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self.registry_refs = registry_refs

    def ready_heartbeats(self, *, shared: bool = False) -> list[NodeHeartbeat]:
        now = utc_now()
        return [
            heartbeat
            for heartbeat in (
                self.store.load_heartbeats(shared=True) if shared
                else self.store.load_heartbeats()
            ).values()
            if heartbeat.node_url
            and not heartbeat.draining
            and heartbeat.admission_open
            and heartbeat.is_fresh(now, self.heartbeat_ttl_seconds)
        ]

    def ready_sandbox_heartbeats(self, *, shared: bool = False) -> list[NodeHeartbeat]:
        return [
            heartbeat
            for heartbeat in (
                self.ready_heartbeats(shared=True) if shared
                else self.ready_heartbeats()
            )
            if "sandbox" in heartbeat.capabilities
        ]

    def heartbeat_for_route(
        self, *, job_id: str, include_inventory: bool = True,
    ) -> NodeHeartbeat | None:
        # Every persisted sandbox and exec route has a non-empty immutable job
        # binding. An exact miss means that worker heartbeat is unavailable;
        # scanning unrelated node inventories cannot make the route current.
        return (
            self.store.get_heartbeat(job_id) if include_inventory
            else self.store.get_heartbeat(job_id, include_inventory=False)
        )

    def body_keepalive_origin(self, heartbeat: NodeHeartbeat | None) -> str | None:
        """The origin that may reuse a connection after a fully framed body."""
        return (
            heartbeat.node_url.rstrip("/")
            if heartbeat is not None and heartbeat.node_url
            and heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
            and REQUEST_BODY_KEEPALIVE_CAPABILITY in heartbeat.capabilities
            else None
        )

    def nodes_with_cached_image(
        self, image: str, heartbeats: list[NodeHeartbeat], *, image_id: str = "",
    ) -> set[str]:
        """Nodes whose heartbeat cache proves the image; unknown caches are absent."""
        if not image.strip() and not image_id.strip():
            return set()
        image_keys = _requested_image_cache_keys(
            image, image_id, require_digest=self.registry_refs.requires_digest_identity(image),
        )
        return {
            heartbeat.node_id
            for heartbeat in heartbeats
            if heartbeat.cached_images_known
            and image_keys.intersection(heartbeat.cached_images)
        }


def _node_metadata(heartbeat: NodeHeartbeat) -> dict[str, Any]:
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
        "active_sandboxes": heartbeat.active_sandboxes,
    }


def _heartbeat_has_image(
    heartbeat: NodeHeartbeat,
    image: str,
    image_id: str = "",
    *,
    require_digest: bool = False,
) -> bool:
    if not heartbeat.cached_images_known:
        return False
    image_keys = _requested_image_cache_keys(
        image,
        image_id,
        require_digest=require_digest,
    )
    return bool(image_keys.intersection(heartbeat.cached_images))


def _requested_image_cache_keys(
    image: str,
    image_id: str = "",
    *,
    require_digest: bool = False,
) -> set[str]:
    """Return only cache identities that prove the requested image is present."""

    digest_ref = canonical_image_digest_ref(image)
    if digest_ref:
        return {image.strip(), digest_ref}
    # A mutable host-qualified tag can move independently of a node heartbeat.
    # It must be resolved to a digest (or pulled again) before it is a cache hit.
    if require_digest and registry_host_from_image_ref(image):
        return set()
    return {item for item in (image, image_id, image_id_from_tag(image)) if item}
