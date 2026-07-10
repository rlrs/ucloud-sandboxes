import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread

from ucloud_sandboxes.agent import build_heartbeat, detect_job_id, fetch_node_agent_heartbeat
from ucloud_sandboxes.models import (
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
)
from ucloud_sandboxes.registry import heartbeat_to_dict


class AgentTests(unittest.TestCase):
    def test_detects_job_id_from_environment_mapping(self) -> None:
        self.assertEqual(detect_job_id({"UCLOUD_JOB_ID": "123"}), "123")
        self.assertEqual(detect_job_id({"JOB_ID": "456"}), "456")
        self.assertIsNone(detect_job_id({}))

    def test_builds_heartbeat(self) -> None:
        heartbeat = build_heartbeat(
            job_id="123",
            node_id="node-123",
            active_sandboxes=1,
            draining=True,
            node_url="http://sandbox-node-123:8090",
            agent_version="0.1.0-test",
            deployment_id="prod-a",
            init_version="init-1",
            capabilities=("sandbox", "image-build"),
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
            used_resources=ResourceQuantity(vcpu=1.5, memory_mb=1024, disk_mb=4096),
            cpu_overcommit=2.0,
            labels={"pool": "default"},
            node_epoch="boot-1",
            activity_epoch=4,
            inventory=(SandboxInventoryEntry(sandbox_id="sandbox-1", generation=2),),
            inventory_complete=True,
            reserved_resources=ResourceQuantity(vcpu=0.5, memory_mb=512, disk_mb=256),
            physical_disk_total_mb=200_000,
            physical_disk_free_mb=150_000,
            drain_token="drain-1",
            drain_activity_epoch=4,
            admission_open=False,
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=12.5,
                cpu_vcpu=0.5,
                cpu_count=4,
                memory_total_mb=8192,
                memory_used_mb=2048,
                memory_available_mb=6144,
                memory_percent=25.0,
            ),
        )

        self.assertEqual(heartbeat.job_id, "123")
        self.assertEqual(heartbeat.node_id, "node-123")
        self.assertEqual(heartbeat.active_sandboxes, 1)
        self.assertTrue(heartbeat.draining)
        self.assertEqual(heartbeat.node_url, "http://sandbox-node-123:8090")
        self.assertEqual(heartbeat.agent_version, "0.1.0-test")
        self.assertEqual(heartbeat.deployment_id, "prod-a")
        self.assertEqual(heartbeat.init_version, "init-1")
        self.assertEqual(heartbeat.capabilities, ("sandbox", "image-build"))
        self.assertEqual(heartbeat.effective_resources.vcpu, 8)
        self.assertEqual(heartbeat.free_resources.vcpu, 6)
        self.assertEqual(heartbeat.free_resources.memory_mb, 6656)
        self.assertEqual(heartbeat.free_resources.disk_mb, 95_648)
        self.assertEqual(heartbeat.active_workloads, 2)
        self.assertEqual(heartbeat.labels, {"pool": "default"})
        self.assertEqual(heartbeat.node_epoch, "boot-1")
        self.assertEqual(heartbeat.activity_epoch, 4)
        self.assertEqual(heartbeat.inventory[0].generation, 2)
        self.assertTrue(heartbeat.inventory_complete)
        self.assertEqual(heartbeat.reserved_resources.memory_mb, 512)
        self.assertEqual(heartbeat.physical_disk_free_mb, 150_000)
        self.assertEqual(heartbeat.drain_token, "drain-1")
        self.assertEqual(heartbeat.drain_activity_epoch, 4)
        self.assertFalse(heartbeat.admission_open)
        self.assertEqual(heartbeat.reported_at, heartbeat.updated_at)
        self.assertIsNotNone(heartbeat.runtime_metrics)
        assert heartbeat.runtime_metrics is not None
        self.assertEqual(heartbeat.runtime_metrics.cpu_percent, 12.5)

    def test_rejects_missing_job_id(self) -> None:
        with self.assertRaises(ValueError):
            build_heartbeat(job_id="")

    def test_rejects_invalid_resources_and_overcommit(self) -> None:
        with self.assertRaises(ValueError):
            build_heartbeat(
                job_id="123",
                used_resources=ResourceQuantity(memory_mb=-1),
            )
        with self.assertRaises(ValueError):
            build_heartbeat(job_id="123", cpu_overcommit=float("nan"))

    def test_fetches_node_agent_heartbeat(self) -> None:
        heartbeat = build_heartbeat(
            job_id="123",
            node_id="node-123",
            active_sandboxes=2,
            node_url="http://node-123:8090",
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
            used_resources=ResourceQuantity(vcpu=1, memory_mb=2048, disk_mb=4096),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=25.0,
                cpu_vcpu=1.0,
                cpu_count=4,
            ),
        )

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.headers.get("Authorization") != "Bearer node-secret":
                    self.send_response(401)
                    self.end_headers()
                    return
                body = json.dumps({"heartbeat": heartbeat_to_dict(heartbeat)}).encode(
                    "utf-8"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address
            fetched = fetch_node_agent_heartbeat(
                f"http://{host}:{port}",
                bearer_token="node-secret",
            )
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(fetched.job_id, "123")
        self.assertEqual(fetched.active_sandboxes, 2)
        self.assertEqual(fetched.used_resources.memory_mb, 2048)
        self.assertIsNotNone(fetched.runtime_metrics)
        assert fetched.runtime_metrics is not None
        self.assertEqual(fetched.runtime_metrics.cpu_vcpu, 1.0)


if __name__ == "__main__":
    unittest.main()
