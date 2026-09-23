import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock

from tests import test_control_plane as fixtures
from tests import test_resident_wait_ranking as ranking_fixtures
from ucloud_sandboxes.capabilities import (
    RELAY_WAKE_FENCE_CAPABILITY,
    RESOURCE_PHASE_CAPABILITY,
)
from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.warm_park import WarmParkDeferred


class ResourcePhaseForwardingTests(TestCase):
    def test_gateway_forwards_hint_only_to_capable_owner_on_bound_park(self):
        handler = object.__new__(ControlPlaneHandler)
        handler._write_json = Mock()
        route = fixtures._sandbox_route(
            sandbox_id="agent", node_id="node", job_id="node", node_url="http://node"
        )
        payload = dict(
            operation_id="park:r",
            durable_lifecycle=True,
            request_id="r",
            resource_phase=ranking_fixtures.ResidentWaitRankingTests.hint(),
        )
        for enabled in (False, True):
            caps = (
                (RELAY_WAKE_FENCE_CAPABILITY, RESOURCE_PHASE_CAPABILITY)
                if enabled
                else (RELAY_WAKE_FENCE_CAPABILITY,)
            )
            handler._heartbeat_for_route = lambda **_: SimpleNamespace(
                capabilities=caps
            )
            result = json.loads(handler._lifecycle_proxy_body(route, "park", payload))
            self.assertEqual("resource_phase" in result, enabled)
            self.assertEqual(result["generation"], route.generation)
            self.assertEqual(result["relay_request_id"], "r")
        self.assertNotIn(
            "resource_phase",
            json.loads(handler._lifecycle_proxy_body(route, "wake", payload)),
        )

    def test_node_validates_advice_before_forwarding_it_under_existing_identity(self):
        handler = object.__new__(NodeAgentHandler)
        handler._write_json = Mock()
        handler._write_exception = Mock()
        park = Mock(side_effect=WarmParkDeferred(1))
        handler.manager = SimpleNamespace(park_with_activity_revision=park)
        payload = dict(
            operation_id="park:r",
            relay_request_id="r",
            generation=1,
            resource_phase=ranking_fixtures.ResidentWaitRankingTests.hint(),
        )
        handler._read_json_body = lambda: payload
        handler._park_sandbox("/v1/sandboxes/agent/park")
        self.assertEqual(
            park.call_args.kwargs["resource_phase"], payload["resource_phase"]
        )
        self.assertEqual(park.call_args.kwargs["generation"], 1)
        self.assertEqual(park.call_args.kwargs["relay_request_id"], "r")
        for mutation in (
            {"generation": 2.5},
            {"relay_request_id": None},
            {
                "resource_phase": {
                    **ranking_fixtures.ResidentWaitRankingTests.hint(),
                    "extra": 1,
                }
            },
        ):
            park.reset_mock()
            handler._read_json_body = lambda mutation=mutation: {**payload, **mutation}
            handler._park_sandbox("/v1/sandboxes/agent/park")
            park.assert_not_called()
        handler._read_json_body = lambda: {
            "operation_id": "park:r",
            "resource_phase": payload["resource_phase"],
        }
        handler._park_sandbox("/v1/sandboxes/agent/park")
        park.assert_not_called()

    def test_gateway_rejects_unbound_or_wake_phase_hint(self):
        handler = object.__new__(ControlPlaneHandler)
        handler._write_json = Mock()
        route = fixtures._sandbox_route(
            sandbox_id="agent", node_id="node", job_id="node", node_url="http://node"
        )
        for action, extra in [
            ("park", {}),
            ("wake", {"durable_lifecycle": True, "request_id": "r"}),
        ]:
            payload = dict(
                operation_id="op:r",
                resource_phase=ranking_fixtures.ResidentWaitRankingTests.hint(),
                **extra,
            )
            self.assertIsNone(
                handler._parse_lifecycle_request(
                    route, action, json.dumps(payload).encode()
                )
            )
