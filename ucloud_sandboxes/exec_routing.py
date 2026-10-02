"""Gateway-owned exec route resolution, independent of HTTP transport."""

from dataclasses import dataclass
from http import HTTPStatus
from typing import Callable, Optional

from .control_state import QUARANTINE_REASON
from .exec_session_routes import ExecSessionRoutes, SignedExecRoute
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


# Pulls one stale worker heartbeat; None once the stale boot was replaced.
Refresh = Optional[Callable[[NodeHeartbeat], Optional[NodeHeartbeat]]]


def _worker_unreachable(route: ExecRoute | SignedExecRoute) -> ExecRouteUnavailable:
    return ExecRouteUnavailable(
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


def _worker_lost(
    session_id: str, sandbox_id: str, generation: int, lost_at: str
) -> ExecRouteUnavailable:
    return ExecRouteUnavailable(
        HTTPStatus.GONE,
        {
            "error": "exec worker was lost; the accepted command cannot resume",
            "error_code": "exec_worker_lost",
            "retryable": False,
            "session_id": session_id,
            "sandbox_id": sandbox_id,
            "sandbox_generation": generation,
            "lost_at": lost_at,
        },
    )


class ExecRoutingService:
    def __init__(
        self,
        control_store,
        routing_store,
        heartbeat_ttl_seconds,
        session_routes: ExecSessionRoutes | None = None,
    ):
        self.control_store = control_store
        self.routing_store = routing_store
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self.session_routes = session_routes

    def signed_prefix(self, route) -> str | None:
        """Prefix for a new session on this exact sandbox incarnation."""

        if self.session_routes is None:
            return None
        try:
            return self.session_routes.prefix(
                SignedExecRoute(
                    sandbox_id=route.sandbox_id,
                    sandbox_generation=route.generation,
                    node_id=route.node_id,
                    job_id=route.job_id,
                    issued_at=max(1, int(utc_now().timestamp())),
                )
            )
        except ValueError:
            # An identity too long to sign keeps the durable exec route.
            return None

    def is_signed_for(self, session_id: str, route) -> bool:
        """Whether a worker-named session already carries this route."""

        signed = (
            self.session_routes.decode(session_id)
            if self.session_routes is not None
            else None
        )
        return bool(
            signed is not None
            and signed.sandbox_id == route.sandbox_id
            and signed.sandbox_generation == route.generation
            and signed.job_id == route.job_id
        )

    def fresh(self, heartbeat: NodeHeartbeat | None) -> bool:
        return bool(
            heartbeat is not None
            and heartbeat.node_url
            and heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
        )

    def heartbeat(self, route: ExecRoute | SignedExecRoute, refresh: Refresh = None) -> NodeHeartbeat | None:
        heartbeat = self.control_store.get_heartbeat(
            route.job_id, include_inventory=False
        )
        if refresh is not None and heartbeat is not None and not self.fresh(heartbeat):
            # Silence is never loss: ask the worker once before a 503.
            heartbeat = refresh(heartbeat)
        if heartbeat is not None and heartbeat.active_sandboxes == 0:
            heartbeat = self.control_store.get_heartbeat(route.job_id)
        return heartbeat

    def resolve(self, session_id: str, *, refresh: Refresh = None) -> tuple[ExecRoute, NodeHeartbeat]:
        """Route one session; ``refresh`` pulls a stale worker's heartbeat."""
        signed = (
            self.session_routes.decode(session_id)
            if self.session_routes is not None
            else None
        )
        if signed is not None:
            return self._resolve_signed(session_id, signed, refresh)
        route = self.routing_store.get_exec(session_id)
        if route is None:
            raise self._missing(session_id)
        heartbeat = self.heartbeat(route, refresh)
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
        if not self.fresh(heartbeat):
            if refresh is not None and self.routing_store.get_exec(session_id) is None:
                # The pull proved a new boot, whose ingest retired this route.
                raise self._missing(session_id)
            raise _worker_unreachable(route)
        return route, heartbeat

    def _missing(self, session_id: str) -> ExecRouteUnavailable:
        loss = self.routing_store.get_exec_loss(session_id)
        if loss is None:
            return ExecRouteUnavailable(
                HTTPStatus.NOT_FOUND, {"error": "exec route not found", "retryable": False},
            )
        return _worker_lost(session_id, loss["sandbox_id"], loss["generation"], loss["lost_at"])

    def _resolve_signed(
        self, session_id: str, signed: SignedExecRoute, refresh: Refresh
    ) -> tuple[ExecRoute, NodeHeartbeat]:
        """Route by the signed worker identity; read routing only on failure.

        A fresh worker is the authority on its own sessions and answers an
        unknown one with 404. No heartbeat inventory can prove absence here:
        a heartbeat captured before the sandbox existed may be received after
        the prefix was minted.
        """

        heartbeat = self.heartbeat(signed, refresh)
        if self.fresh(heartbeat):
            return (
                ExecRoute(
                    session_id=session_id,
                    sandbox_id=signed.sandbox_id,
                    node_id=signed.node_id,
                    job_id=signed.job_id,
                    node_url=heartbeat.node_url,
                    created_at=signed.issued_at_iso,
                    updated_at=signed.issued_at_iso,
                ),
                heartbeat,
            )
        # Keep the routed-session distinction between a lost owner and a
        # temporarily silent one. Sessions are never redirected to another owner.
        current = self.routing_store.get_sandbox_readonly(signed.sandbox_id)
        if current is None or current.generation != signed.sandbox_generation:
            loss = (
                self.routing_store.get_sandbox_loss(signed.sandbox_id)
                if current is None else None
            )
            if loss is not None and int(loss["generation"]) == signed.sandbox_generation:
                raise _worker_lost(
                    session_id,
                    signed.sandbox_id,
                    signed.sandbox_generation,
                    loss["lost_at"],
                )
            # A deleted or replaced incarnation retains no routed sessions.
            raise ExecRouteUnavailable(
                HTTPStatus.NOT_FOUND,
                {"error": "exec route not found", "retryable": False},
            )
        if current.job_id != signed.job_id or current.worker_state == "detached":
            raise _worker_lost(
                session_id,
                signed.sandbox_id,
                signed.sandbox_generation,
                current.updated_at,
            )
        raise _worker_unreachable(signed)
