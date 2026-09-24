from dataclasses import asdict, replace
from datetime import timedelta
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests.test_control_plane import _sandbox_route, build_heartbeat
from ucloud_sandboxes.control_plane import _route_only_sandbox_record
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer
from ucloud_sandboxes.models import utc_now
from ucloud_sandboxes.routing import _sandbox_route_from_row


def rows(routes):
    result = []
    for route in routes:
        row = asdict(route)
        for key in ("resources", "spec", "storage_snapshot"):
            row[key + "_json"] = json.dumps(row.pop(key))
        result.append(row)
    return result


class FleetRenderCacheTests(unittest.TestCase):
    def setUp(self):
        self.now = utc_now()
        self.route = _sandbox_route(
            sandbox_id="agent",
            node_id="node",
            job_id="job",
            node_url="http://node:8090",
            spec={"id": "agent", "image": "example"},
            state="running",
        )
        self.heartbeat = build_heartbeat(
            node_id=self.route.node_id,
            job_id=self.route.job_id,
            node_url=self.route.node_url,
            active_sandboxes=1,
        )

        self.now = self.heartbeat.freshness_at

    def render(self, renderer, routes=None, heartbeat=None):
        routes = [self.route] if routes is None else routes
        heartbeat = self.heartbeat if heartbeat is None else heartbeat
        nodes = {heartbeat.node_id: heartbeat}
        with (
            patch("ucloud_sandboxes.models.utc_now", return_value=self.now),
            patch("ucloud_sandboxes.control_plane.utc_now", return_value=self.now),
            patch("ucloud_sandboxes.exec_routing.utc_now", return_value=self.now),
        ):
            result = renderer.render(rows(routes), nodes, 120)
            expected = [
                _route_only_sandbox_record(
                    r, nodes.get(r.node_id), heartbeat_ttl_seconds=120
                )
                for r in routes
            ]
        self.assertEqual(json.loads(result)["sandboxes"], expected)
        return result

    def test_unchanged_routes_encode_once_and_changed_spec_is_visible(self):
        renderer = FleetResponseRenderer()
        self.render(renderer)
        with patch(
            "ucloud_sandboxes.routing._sandbox_route_from_row",
            wraps=_sandbox_route_from_row,
        ) as encode:
            self.render(renderer)
            encode.assert_not_called()
            changed = replace(
                self.route, spec={**self.route.spec, "labels": {"changed": "yes"}}
            )
            self.render(renderer, [changed])
            encode.assert_called_once()

    def test_node_changes_and_clock_expiry_invalidate(self):
        renderer = FleetResponseRenderer()
        self.render(renderer)
        self.render(renderer, heartbeat=replace(self.heartbeat, active_sandboxes=2))
        self.now += timedelta(seconds=121)
        self.render(renderer)
        self.assertFalse(
            json.loads(self.render(renderer))["sandboxes"][0]["node"]["fresh"]
        )

    def test_removed_route_and_node_do_not_survive(self):
        renderer = FleetResponseRenderer()
        self.render(renderer)
        self.render(renderer, [])
        self.assertEqual(renderer._routes, {})
        result = json.loads(renderer.render(rows([self.route]), {}, 120))
        self.assertFalse(result["sandboxes"][0]["node"]["fresh"])
        self.render(renderer)

    def test_changed_invalid_row_is_not_served_from_cache(self):
        renderer = FleetResponseRenderer()
        self.render(renderer)
        changed = rows([self.route])
        changed[0]["spec_hash"] = "invalid"
        with self.assertRaises(sqlite3.DatabaseError):
            renderer.render(changed, {self.heartbeat.node_id: self.heartbeat}, 120)

    def test_budget_only_limits_retention_not_results(self):
        renderer = FleetResponseRenderer(max_cached_bytes=1)
        self.render(renderer)
        self.assertEqual(renderer._routes, {})
        self.render(renderer)

    def test_changed_generation_state_and_inventory_proof_match_uncached(self):
        renderer = FleetResponseRenderer()
        self.render(renderer)
        changed = replace(
            self.route, generation=self.route.generation + 1, state="parked"
        )
        self.render(renderer, [changed])
        self.render(renderer, [changed], replace(self.heartbeat, active_sandboxes=0))
