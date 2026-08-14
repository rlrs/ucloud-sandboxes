import importlib
import json
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import unittest

from ucloud_sandboxes.control_plane import build_server
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    utc_now,
)


SDK_SOURCE = Path(__file__).parents[1] / "ucloud-sandboxes-sdk" / "src"
SDK_AVAILABLE = SDK_SOURCE.is_dir()
if SDK_AVAILABLE:
    sys.path.insert(0, str(SDK_SOURCE))
    sdk = importlib.import_module("ucloud_sandboxes_sdk")
else:
    sdk = None

IMAGE = "registry.example/contract@sha256:" + "a" * 64


@contextmanager
def running(server: ThreadingHTTPServer):
    thread = Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=1)
        server.server_close()


class RuntimeBoundaryHandler(BaseHTTPRequestHandler):
    """Controlled node boundary; the gateway and SDK remain real HTTP peers."""

    requests: list[dict[str, object]] = []

    def do_POST(self) -> None:
        payload = self._read_json()
        operation = payload.pop("_ucloud_operation")
        assert isinstance(operation, dict)
        sandbox_id = str(payload["id"])
        record = {
            "spec": payload,
            "state": "running",
            "generation": operation["generation"],
            "operation_id": operation["operation_id"],
            "spec_hash": operation["spec_hash"],
        }
        type(self).requests.append(
            {
                "method": "POST",
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "sandbox_id": sandbox_id,
                "generation": operation["generation"],
                "operation_id": operation["operation_id"],
                "spec_hash": operation["spec_hash"],
            }
        )
        self._write_json({"sandbox": record}, status=201)

    def do_DELETE(self) -> None:
        sandbox_id = self.path.rsplit("/", 1)[-1]
        generation = int(self.headers["X-UCloud-Sandbox-Generation"])
        type(self).requests.append(
            {
                "method": "DELETE",
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "sandbox_id": sandbox_id,
                "generation": generation,
                "operation_id": self.headers.get("X-UCloud-Sandbox-Operation-Id"),
            }
        )
        self._write_json(
            {"ok": True, "deleted": {"id": sandbox_id, "generation": generation}}
        )

    def _read_json(self) -> dict[str, object]:
        size = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(size))
        assert isinstance(payload, dict)
        return payload

    def _write_json(self, payload: dict[str, object], *, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


@unittest.skipUnless(SDK_AVAILABLE, "sibling ucloud-sandboxes-sdk is unavailable")
class SdkGatewayContractTests(unittest.TestCase):
    def test_create_get_list_delete_lifecycle_preserves_identity(self) -> None:
        assert sdk is not None
        RuntimeBoundaryHandler.requests = []
        with TemporaryDirectory() as raw:
            root = Path(raw)
            node = ThreadingHTTPServer(("127.0.0.1", 0), RuntimeBoundaryHandler)
            with running(node):
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                now = utc_now()
                ControlStateStore(root / "control.sqlite").upsert_heartbeat(
                    NodeHeartbeat(
                        node_id="node-contract",
                        job_id="job-contract",
                        node_url=node_url,
                        updated_at=now,
                        received_at=now,
                        active_sandboxes=0,
                        agent_version=package_version(),
                        deployment_id="contract-deployment",
                        capabilities=("sandbox", "disk-quota"),
                        total_resources=ResourceQuantity(
                            vcpu=8,
                            memory_mb=16_384,
                            disk_mb=32_768,
                        ),
                        resources_known=True,
                        runtime_metrics=NodeRuntimeMetrics(
                            collected_at=now,
                            cpu_percent=0.0,
                            cpu_count=8,
                            load_average_1m=0.0,
                            memory_total_mb=16_384,
                            memory_available_mb=16_384,
                        ),
                        cached_images=(IMAGE,),
                        cached_images_known=True,
                        node_epoch="node-epoch-1",
                        activity_epoch=1,
                        inventory_complete=True,
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    root / "control.sqlite",
                    routing_file=root / "routes.sqlite",
                    image_file=root / "images.json",
                    metrics_file=root / "metrics.sqlite",
                    gateway_bearer_token="gateway-secret",
                    sandbox_api_token="sdk-secret",
                    heartbeat_bearer_token="heartbeat-secret",
                    node_control_bearer_token="node-secret",
                    deployment_id="contract-deployment",
                )
                with running(gateway):
                    host, port = gateway.server_address
                    client = sdk.SandboxClient(
                        f"http://{host}:{port}", api_token="sdk-secret"
                    )
                    handle = client.create_sandbox(
                        sdk.SandboxSpec(
                            id="contract-one",
                            image=sdk.Image.from_registry(IMAGE),
                            command=("sh", "-c", "echo contract"),
                            env={"CONTRACT_ID": "request-one"},
                            memory_mb=512,
                            cpus=1.5,
                            disk_mb=1024,
                            labels={"request": "request-one"},
                        ),
                        request_timeout_seconds=5,
                    )
                    fetched = client.get_sandbox("contract-one")
                    listed = client.list_sandboxes()
                    deleted = handle.delete()
                    after_delete = client.get_sandbox("contract-one")

        self.assertEqual(handle.id, "contract-one")
        self.assertEqual(handle.record["spec"]["id"], handle.id)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["spec"]["id"], handle.id)
        self.assertEqual(fetched["node"]["node_id"], "node-contract")
        self.assertEqual([item["spec"]["id"] for item in listed], [handle.id])
        self.assertEqual(deleted["deleted"]["id"], handle.id)
        self.assertEqual(deleted["deleted"]["generation"], 1)
        self.assertIsNone(after_delete)

        created, removed = RuntimeBoundaryHandler.requests
        self.assertEqual(created["path"], "/v1/sandboxes")
        self.assertEqual(removed["path"], f"/v1/sandboxes/{handle.id}")
        self.assertEqual(created["sandbox_id"], handle.id)
        self.assertEqual(removed["sandbox_id"], handle.id)
        self.assertEqual(created["generation"], removed["generation"])
        self.assertEqual(created["authorization"], "Bearer node-secret")
        self.assertEqual(removed["authorization"], "Bearer node-secret")
        self.assertTrue(str(created["operation_id"]).startswith("create-"))
        self.assertTrue(str(removed["operation_id"]).startswith("delete-"))
        self.assertEqual(len(str(created["spec_hash"])), 64)


if __name__ == "__main__":
    unittest.main()
