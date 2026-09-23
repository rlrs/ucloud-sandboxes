"""Startup must exercise the same retained worker records as fleet requests."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import sqlite3
from urllib.request import urlopen
import unittest
from unittest.mock import patch

from tests.test_control_plane import _gateway_server, _running_server, build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore, _json
from ucloud_sandboxes.models import NodeRuntimeMetrics, utc_now
from ucloud_sandboxes.resource_evidence import ResourceEvidence


class GatewayStartupStateTests(unittest.TestCase):
    def persist_legacy_worker(self, root):
        path = root / "control-state.sqlite"
        store = ControlStateStore(path)
        store.receive_heartbeat(
            replace(
                build_heartbeat(job_id="legacy-worker"),
                received_at=utc_now(),
                runtime_metrics=NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    resource_evidence=ResourceEvidence(
                        collected_at=utc_now().isoformat()
                    ),
                ),
            )
        )
        with sqlite3.connect(path) as connection:
            raw = json.loads(
                connection.execute("SELECT payload FROM control_records").fetchone()[0]
            )
            raw["runtime_metrics"]["resource_evidence"].pop("host_cpu_usage_usec")
            raw["runtime_metrics"]["resource_evidence"].pop("host_cpu_steal_usec")
            connection.execute("UPDATE control_records SET payload=?", (_json(raw),))
        return path, raw

    def test_valid_legacy_worker_is_ready_for_health_and_fleet_after_startup(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.persist_legacy_worker(root)
            server = _gateway_server(root)
            self.addCleanup(server.server_close)
            self.assertIn(
                "legacy-worker", server.RequestHandlerClass.store._heartbeat_cache
            )
            with _running_server(server) as address:
                with urlopen(address + "/healthz", timeout=2) as response:
                    self.assertTrue(json.load(response)["ok"])
                with urlopen(address + "/v1/nodes", timeout=2) as response:
                    nodes = json.load(response)["nodes"]
                self.assertEqual([row["job_id"] for row in nodes], ["legacy-worker"])
                self.assertIsNone(
                    nodes[0]["runtime_metrics"]["resource_evidence"][
                        "host_cpu_usage_usec"
                    ]
                )

    def test_malformed_retained_worker_fails_before_listener_or_background_stores(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            path, raw = self.persist_legacy_worker(root)
            raw["runtime_metrics"]["resource_evidence"]["unknown"] = None
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE control_records SET payload=?", (_json(raw),)
                )
            with patch(
                "socketserver.TCPServer.server_bind",
                side_effect=AssertionError("listener bound before state validation"),
            ):
                with self.assertRaisesRegex(
                    ValueError, "invalid heartbeat control-state record"
                ):
                    _gateway_server(root)
            self.assertFalse((root / "control-state-routes.sqlite").exists())
            self.assertFalse((root / "control-state-metrics.sqlite").exists())
