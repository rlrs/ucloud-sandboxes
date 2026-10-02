from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from ucloud_sandboxes.exec_routing import ExecRoutingService, ExecRouteUnavailable
from ucloud_sandboxes.exec_session_routes import (
    ExecSessionRoutes,
    SignedExecRoute,
    valid_session_prefix,
)

ROUTE = SignedExecRoute(
    sandbox_id="rollout-7f3c",
    sandbox_generation=3,
    node_id="node-a",
    job_id="job-1",
    issued_at=1_790_000_000,
)
NODE_SUFFIX = "0123456789abcdef0123456789abcdef"


class ExecSessionRoutesTests(unittest.TestCase):
    def test_round_trip_binds_incarnation_and_worker(self) -> None:
        routes = ExecSessionRoutes("gateway-secret")
        prefix = routes.prefix(ROUTE)

        self.assertTrue(valid_session_prefix(prefix))
        self.assertEqual(routes.decode(f"{prefix}.{NODE_SUFFIX}"), ROUTE)
        self.assertEqual(
            routes.decode(f"{prefix}.{NODE_SUFFIX}").issued_at_iso,
            "2026-09-21T14:13:20+00:00",
        )

    def test_routed_and_malformed_names_are_not_signed_routes(self) -> None:
        routes = ExecSessionRoutes("gateway-secret")
        prefix = routes.prefix(ROUTE)

        for session_id in (
            f"exec-{NODE_SUFFIX}",
            prefix,
            f"{prefix}.{NODE_SUFFIX.upper()}",
            f"{prefix}.{NODE_SUFFIX}0",
            f"{prefix}.{NODE_SUFFIX}/../x",
            "",
        ):
            with self.subTest(session_id=session_id):
                self.assertIsNone(routes.decode(session_id))

    def test_signature_rejects_forgery_and_rotated_secret(self) -> None:
        routes = ExecSessionRoutes("gateway-secret")
        version, payload, mac = routes.prefix(ROUTE).split(".")
        forged = ExecSessionRoutes("other").prefix(
            SignedExecRoute("rollout-7f3c", 3, "node-b", "job-2", ROUTE.issued_at)
        )
        _, forged_payload, _ = forged.split(".")

        self.assertIsNone(
            routes.decode(f"{version}.{forged_payload}.{mac}.{NODE_SUFFIX}")
        )
        self.assertIsNone(ExecSessionRoutes("rotated").decode(
            f"{version}.{payload}.{mac}.{NODE_SUFFIX}"
        ))

    def test_rejects_identity_that_cannot_fit_a_prefix(self) -> None:
        routes = ExecSessionRoutes("gateway-secret")
        with self.assertRaises(ValueError):
            routes.prefix(
                SignedExecRoute("s" * 600, 1, "node-a", "job-1", ROUTE.issued_at)
            )

    def test_worker_shape_check(self) -> None:
        self.assertFalse(valid_session_prefix("exec-abc"))
        self.assertFalse(valid_session_prefix("xr1.payload.short"))
        self.assertFalse(valid_session_prefix("xr1.a b.AAAAAAAAAAAAAAAAAAAAAA"))
        self.assertTrue(valid_session_prefix("xr1.YQ.AAAAAAAAAAAAAAAAAAAAAA"))

    def test_requires_secret(self) -> None:
        with self.assertRaises(ValueError):
            ExecSessionRoutes(" ")



@dataclass
class _Heartbeat:
    node_url: str = "http://node-a:8090"
    fresh: bool = True
    active_sandboxes: int = 1
    inventory_complete: bool = True
    inventory: tuple = ()
    labels: dict = field(default_factory=dict)
    freshness_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def is_fresh(self, now, ttl_seconds) -> bool:
        return self.fresh


class _Routing:
    def __init__(self, route=None, loss=None) -> None:
        self.route, self.loss, self.reads = route, loss, 0

    def get_sandbox_readonly(self, sandbox_id):
        self.reads += 1
        return self.route

    def get_sandbox_loss(self, sandbox_id):
        self.reads += 1
        return self.loss

    def get_exec(self, session_id):  # pragma: no cover - must not be called
        raise AssertionError("signed sessions must not read exec routes")


class SignedExecRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.routes = ExecSessionRoutes("gateway-secret")
        issued = int((datetime.now(timezone.utc) - timedelta(seconds=5)).timestamp())
        self.signed = SignedExecRoute("one", 3, "node-a", "job-1", issued)
        self.session_id = f"{self.routes.prefix(self.signed)}.{NODE_SUFFIX}"

    def service(self, heartbeat, routing) -> ExecRoutingService:
        control = SimpleNamespace(
            get_heartbeat=lambda job_id, include_inventory=True: heartbeat
        )
        return ExecRoutingService(control, routing, 120, session_routes=self.routes)

    def unavailable(self, heartbeat, routing) -> ExecRouteUnavailable:
        with self.assertRaises(ExecRouteUnavailable) as raised:
            self.service(heartbeat, routing).resolve(self.session_id)
        return raised.exception

    def test_fresh_worker_routes_without_routing_reads(self) -> None:
        routing = _Routing()
        route, _ = self.service(_Heartbeat(), routing).resolve(self.session_id)
        self.assertEqual(
            (route.sandbox_id, route.job_id, route.node_url),
            ("one", "job-1", "http://node-a:8090"),
        )
        self.assertEqual(routing.reads, 0)

    def test_fresh_worker_decides_even_when_its_inventory_looks_empty(self) -> None:
        # The inventory may predate the sandbox; only the worker knows.
        routing = _Routing()
        route, _ = self.service(_Heartbeat(active_sandboxes=0), routing).resolve(
            self.session_id
        )
        self.assertEqual(route.job_id, "job-1")
        self.assertEqual(routing.reads, 0)

    def test_silent_worker_still_owning_the_incarnation_is_retryable(self) -> None:
        route = SimpleNamespace(
            generation=3, job_id="job-1", worker_state="attached", updated_at="t"
        )
        error = self.unavailable(_Heartbeat(fresh=False), _Routing(route))
        self.assertEqual(error.status, 503)
        self.assertEqual(error.payload["error_code"], "sandbox_worker_unreachable")

    def test_moved_or_detached_owner_reports_the_session_lost(self) -> None:
        for job_id, state in (("job-2", "attached"), ("job-1", "detached")):
            with self.subTest(job_id=job_id, state=state):
                route = SimpleNamespace(
                    generation=3, job_id=job_id, worker_state=state, updated_at="t"
                )
                error = self.unavailable(None, _Routing(route))
                self.assertEqual(error.status, 410)
                self.assertEqual(error.payload["error_code"], "exec_worker_lost")
                self.assertEqual(error.payload["sandbox_generation"], 3)

    def test_recorded_loss_of_the_incarnation_reports_lost(self) -> None:
        loss = {"generation": 3, "lost_at": "2026-10-01T00:00:00+00:00"}
        error = self.unavailable(None, _Routing(loss=loss))
        self.assertEqual(error.status, 410)
        self.assertEqual(error.payload["lost_at"], loss["lost_at"])

    def test_deleted_or_replaced_incarnation_is_not_found(self) -> None:
        replaced = SimpleNamespace(
            generation=4, job_id="job-1", worker_state="attached", updated_at="t"
        )
        for routing in (_Routing(), _Routing(replaced)):
            with self.subTest(route=routing.route):
                self.assertEqual(self.unavailable(None, routing).status, 404)

    def test_unsignable_identity_falls_back_to_the_durable_route(self) -> None:
        service = self.service(_Heartbeat(), _Routing())
        route = SimpleNamespace(
            sandbox_id="one", generation=3, node_id="n" * 600, job_id="job-1"
        )
        self.assertIsNone(service.signed_prefix(route))
        self.assertTrue(
            service.signed_prefix(
                SimpleNamespace(sandbox_id="one", generation=3, node_id="a", job_id="j")
            ).startswith("xr1.")
        )

    def test_prefix_is_only_trusted_for_its_own_incarnation(self) -> None:
        service = self.service(_Heartbeat(), _Routing())
        current = SimpleNamespace(sandbox_id="one", generation=3, job_id="job-1")
        self.assertTrue(service.is_signed_for(self.session_id, current))
        for other in (
            SimpleNamespace(sandbox_id="one", generation=4, job_id="job-1"),
            SimpleNamespace(sandbox_id="one", generation=3, job_id="job-2"),
            SimpleNamespace(sandbox_id="two", generation=3, job_id="job-1"),
        ):
            self.assertFalse(service.is_signed_for(self.session_id, other))
        self.assertFalse(service.is_signed_for(f"exec-{NODE_SUFFIX}", current))


if __name__ == "__main__":
    unittest.main()
