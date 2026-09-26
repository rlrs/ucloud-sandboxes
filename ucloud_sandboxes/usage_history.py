"""Remembered sandbox usage, so cold demand is forecast from what earlier runs used.

Requested resources are maximums, not reservations. Running sandboxes are
already charged their measured memory; announced (prepared) and pending ones
were charged their full request, which bought workers that stayed idle. Runs
usually start cold, so the only usable evidence is what sandboxes of the same
image and shape used in earlier runs. This history keeps each one's observed
peak and is persisted by the single autoscaler process.

The full request remains the forecast when nothing has been observed. Worker
admission still owns execution safety; a low forecast only delays scale-up.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import math
import os
from pathlib import Path
import tempfile
import time
from typing import Iterable

from .managed_registry import canonical_image_digest_ref
from .models import ResourceQuantity, SandboxDemand, SandboxPlacementRequest

# A forecast adds this margin to the highest usage observed for its key.
FORECAST_MARGIN = 1.25
# Forget a key that no running sandbox has refreshed for this long.
RETENTION_SECONDS = 30 * 24 * 3600
# Persist at most this often; the history is advisory, a lost update is harmless.
SAVE_INTERVAL_SECONDS = 30.0
_SCHEMA = 1


def image_identity(image: str) -> str:
    return canonical_image_digest_ref(image) or image.strip()


def shape_key(resources: ResourceQuantity) -> str:
    return f"{resources.vcpu:g}:{resources.memory_mb}:{resources.disk_mb}"


@dataclass(frozen=True)
class UsageObservation:
    memory_mb: int
    updated_at: float


class UsageHistory:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self.entries: dict[tuple[str, str], UsageObservation] = {}
        self._dirty = False
        self._saved_at = 0.0

    @classmethod
    def load(cls, path: Path) -> "UsageHistory":
        history = cls(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return history
        except (OSError, ValueError):
            return history  # Advisory state: start over rather than block scaling.
        if not isinstance(raw, dict) or raw.get("schema") != _SCHEMA:
            return history
        for item in raw.get("entries", ()):
            try:
                key = (str(item["image"]), str(item["shape"]))
                history.entries[key] = UsageObservation(
                    max(0, int(item["memory_mb"])), float(item["updated_at"]),
                )
            except (KeyError, TypeError, ValueError):
                continue
        return history

    def observe(
        self, image: str, resources: ResourceQuantity, memory_mb: int, now: float,
    ) -> None:
        key = (image_identity(image), shape_key(resources))
        if not key[0] or memory_mb <= 0:
            return
        previous = self.entries.get(key)
        if previous is not None and now - previous.updated_at < RETENTION_SECONDS:
            memory_mb = max(memory_mb, previous.memory_mb)
            if memory_mb == previous.memory_mb and now - previous.updated_at < 3600:
                return  # Refresh the timestamp at most hourly.
        self.entries[key] = UsageObservation(memory_mb, now)
        self._dirty = True

    def forecast_memory_mb(self, image: str, resources: ResourceQuantity, now: float) -> int | None:
        """Observed peak (plus margin) for this image and shape, or None if unknown."""

        identity = image_identity(image)
        shape = shape_key(resources)
        live = {
            key: item for key, item in self.entries.items()
            if now - item.updated_at < RETENTION_SECONDS
        }
        # Same image and shape, then the same image at any shape (usage follows
        # the workload more than its limit), then the same shape across images.
        for matches in (
            [item for key, item in live.items() if key == (identity, shape)],
            [item for key, item in live.items() if identity and key[0] == identity],
            [item for key, item in live.items() if key[1] == shape],
        ):
            if matches:
                peak = max(item.memory_mb for item in matches)
                return min(resources.memory_mb, math.ceil(peak * FORECAST_MARGIN))
        return None

    def prune(self, now: float) -> None:
        stale = [key for key, item in self.entries.items() if now - item.updated_at >= RETENTION_SECONDS]
        for key in stale:
            del self.entries[key]
        self._dirty = self._dirty or bool(stale)

    def save_if_due(self, now: float) -> None:
        if self.path is None or not self._dirty or now - self._saved_at < SAVE_INTERVAL_SECONDS:
            return
        payload = {
            "schema": _SCHEMA,
            "entries": [
                {"image": image, "shape": shape, "memory_mb": item.memory_mb,
                 "updated_at": item.updated_at}
                for (image, shape), item in sorted(self.entries.items())
            ],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".usage-history.", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
        self._dirty = False
        self._saved_at = now


def record_inventory_usage(
    history: UsageHistory,
    heartbeats: Iterable,
    images_by_sandbox: dict[tuple[str, int], str],
    now: float,
) -> None:
    """Fold running sandboxes' observed memory into the history."""

    for heartbeat in heartbeats:
        for entry in heartbeat.inventory:
            observation = entry.memory_observation
            if entry.state != "running" or observation is None:
                continue
            image = images_by_sandbox.get((entry.sandbox_id, entry.generation))
            if image:
                history.observe(
                    image, entry.resources,
                    math.ceil(observation.memory_bytes / 1024**2), now,
                )


def demand_with_usage_forecast(
    demand: SandboxDemand,
    history: UsageHistory,
    *,
    initial_disk_claim_mb: int,
    now: float | None = None,
) -> SandboxDemand:
    """Size cold (pending and prepared) demand from remembered usage.

    Memory uses the observed peak for the image and shape when one exists.
    Disk uses the claim a worker actually makes at create (the initial
    workspace grant plus the idle memory claim); growth is reserved later as
    the sandbox writes.
    """

    now = time.time() if now is None else now

    def forecast(request: SandboxPlacementRequest) -> SandboxPlacementRequest:
        resources = request.resources
        memory = history.forecast_memory_mb(request.image, resources, now)
        disk = (
            min(resources.disk_mb, initial_disk_claim_mb)
            if initial_disk_claim_mb > 0 else resources.disk_mb
        )
        forecasted = replace(
            resources,
            memory_mb=resources.memory_mb if memory is None else memory,
            disk_mb=disk,
        )
        return request if forecasted == resources else replace(request, resources=forecasted)

    return replace(
        demand,
        placement_requests=tuple(forecast(item) for item in demand.placement_requests),
        prepared_placement_requests=tuple(
            forecast(item) for item in demand.prepared_placement_requests
        ),
    )
