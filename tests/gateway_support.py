"""Gateway use cases for a handler a test builds without build_server."""

from __future__ import annotations

from typing import Any

from ucloud_sandboxes.gateway.services import GatewayServices, build_services
from ucloud_sandboxes.models import ScalePolicy
from ucloud_sandboxes.telemetry import Telemetry


def gateway_services(
    *,
    store: Any = None,
    routing_store: Any = None,
    metrics_store: Any = None,
    telemetry: Telemetry | None = None,
    heartbeat_ttl_seconds: int = 120,
    registry_url: str | None = None,
    registry_worker_url: str | None = None,
    usage_store: Any = None,
    registry_disk_monitor: Any = None,
    image_manager: Any = None,
    deployment_id: str = "test-deployment",
    dependency_resolver: Any = None,
    create_target_concurrency_per_node: int = ScalePolicy().create_target_concurrency_per_node,
) -> GatewayServices:
    return build_services(
        store=store,
        routing_store=routing_store,
        metrics_store=metrics_store,
        telemetry=telemetry or Telemetry.disabled(),
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        registry_url=registry_url,
        registry_worker_url=registry_worker_url,
        registry_usage_store=usage_store,
        registry_disk_monitor=registry_disk_monitor,
        image_manager=image_manager,
        deployment_id=deployment_id,
        dependency_resolver=dependency_resolver,
        create_target_concurrency_per_node=create_target_concurrency_per_node,
        delete_on_worker=None,
    )
