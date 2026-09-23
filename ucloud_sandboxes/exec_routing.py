"""Gateway-owned exec route resolution, independent of HTTP transport."""

from dataclasses import dataclass
from http import HTTPStatus

from .control_state import QUARANTINE_REASON
from .models import NodeHeartbeat, parse_iso_datetime, utc_now
from .routing import ExecRoute


@dataclass
class ExecRouteUnavailable(Exception):
    status: int
    payload: dict
    headers: dict | None = None


def heartbeat_proves_route_absent(
    heartbeat: NodeHeartbeat | None,
    *,
    sandbox_id: str | None = None,
    route_created_at: str,
    route_updated_at: str,
    heartbeat_ttl_seconds: int,
) -> bool:
    if heartbeat is None or heartbeat.labels.get(QUARANTINE_REASON):
        return False
    if not heartbeat.is_fresh(utc_now(), heartbeat_ttl_seconds):
        return False
    if heartbeat.active_sandboxes != 0:
        return False
    if (
        sandbox_id is not None
        and heartbeat.inventory_complete
        and any(item.sandbox_id == sandbox_id for item in heartbeat.inventory)
    ):
        return False
    reference = parse_iso_datetime(route_updated_at) or parse_iso_datetime(
        route_created_at
    )
    return reference is None or heartbeat.freshness_at >= reference


class ExecRoutingService:
    def __init__(self, control_store, routing_store, heartbeat_ttl_seconds):
        self.control_store = control_store
        self.routing_store = routing_store
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds

    def heartbeat(self, route: ExecRoute) -> NodeHeartbeat | None:
        heartbeat = self.control_store.get_heartbeat(
            route.job_id, include_inventory=False
        )
        if heartbeat is not None and heartbeat.active_sandboxes == 0:
            heartbeat = self.control_store.get_heartbeat(route.job_id)
        return heartbeat

    def resolve(self, session_id: str) -> tuple[ExecRoute, NodeHeartbeat]:
        route = self.routing_store.get_exec(session_id)
        if route is None:
            loss = self.routing_store.get_exec_loss(session_id)
            if loss is not None:
                raise ExecRouteUnavailable(
                    HTTPStatus.GONE,
                    {
                        "error": "exec worker was lost; the accepted command cannot resume",
                        "error_code": "exec_worker_lost",
                        "retryable": False,
                        "session_id": session_id,
                        "sandbox_id": loss["sandbox_id"],
                        "sandbox_generation": loss["generation"],
                        "lost_at": loss["lost_at"],
                    },
                )
            raise ExecRouteUnavailable(
                HTTPStatus.NOT_FOUND,
                {
                    "error": "exec route not found",
                    "retryable": False,
                },
            )
        heartbeat = self.heartbeat(route)
        if heartbeat_proves_route_absent(
            heartbeat,
            sandbox_id=route.sandbox_id,
            route_created_at=route.created_at,
            route_updated_at=route.updated_at,
            heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
        ):
            self.routing_store.delete_exec(route.session_id)
            raise ExecRouteUnavailable(
                HTTPStatus.NOT_FOUND,
                {
                    "error": "exec route is stale",
                    "sandbox_id": route.sandbox_id,
                    "retryable": False,
                },
            )
        if not (
            heartbeat is not None
            and heartbeat.node_url
            and heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
        ):
            raise ExecRouteUnavailable(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "error": "sandbox worker heartbeat is stale or unavailable",
                    "error_code": "sandbox_worker_unreachable",
                    "retryable": True,
                    "node_id": route.node_id,
                    "job_id": route.job_id,
                },
                {"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"},
            )
        return route, heartbeat
