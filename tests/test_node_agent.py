from pathlib import Path
from dataclasses import replace
from tempfile import TemporaryDirectory
from threading import Event, Thread
import asyncio
import http.client
import json
from urllib import request
from urllib.parse import quote
import unittest

from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.gateway import NodeGatewayClient
from ucloud_sandboxes.http_server import DEFAULT_HTTP_REQUEST_QUEUE_SIZE
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_agent import (
    SANDBOX_GENERATION_HEADER,
    SANDBOX_OPERATION_ID_HEADER,
    build_node_agent_server,
)
from ucloud_sandboxes.sandbox import (
    CommandResult,
    DockerGvisorRuntime,
    RecordingExecutor,
    SandboxSpec,
    sandbox_fork_target,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class NodeAgentTests(unittest.TestCase):
    def test_fork_capability_requires_enabled_runtime(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            disabled = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=root / "disabled-sandboxes.json",
                image_file=root / "disabled-images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                extra_capabilities=("fork-local-v1",),
            )
            enabled = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=root / "enabled-sandboxes.json",
                image_file=root / "enabled-images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    fork_enabled=True,
                    checkpoint_root=root / "checkpoints",
                ),
                extra_capabilities=("fork-local-v1",),
            )
            try:
                self.assertNotIn(
                    "fork-local-v1", disabled.RequestHandlerClass.capabilities
                )
                self.assertIn(
                    "fork-local-v1", enabled.RequestHandlerClass.capabilities
                )
            finally:
                disabled.server_close()
                enabled.server_close()

    def test_live_fork_endpoint_requires_and_replays_exact_envelopes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=Path(raw_dir) / "checkpoints",
            )
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
            )
            Thread(target=server.serve_forever, daemon=True).start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "fork-parent",
                        "image": "busybox",
                        "command": ["sleep", "infinity"],
                        "memory_mb": 64,
                        "disk_mb": 64,
                        "forkable": True,
                        "fork_protocol": {
                            "version": "agent-v1",
                            "prepare_command": ["/ucloud/fork-agent", "prepare"],
                            "ready_command": ["/ucloud/fork-agent", "ready"],
                        },
                    },
                )["sandbox"]
                manager = server.RequestHandlerClass.manager
                source_record = manager.get("fork-parent")
                self.assertIsNotNone(source_record)
                manager.store.upsert(replace(source_record, state="running"))
                source_spec = SandboxSpec.from_dict(created["spec"])
                target = sandbox_fork_target(
                    source_spec,
                    {"id": "fork-child", "env": {"AGENT_BRANCH": "child"}},
                )
                payload = {
                    "sandbox": target.to_dict(),
                    "_ucloud_operation": {
                        "operation_id": "fork-child-operation",
                        "generation": 1,
                        "kind": "create",
                        "spec_hash": sandbox_spec_fingerprint(target),
                    },
                    "_ucloud_source": {
                        "generation": created["generation"],
                        "spec_hash": created["spec_hash"],
                    },
                }

                forked = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=payload,
                )
                replayed = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=payload,
                )
                stale = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        **payload,
                        "sandbox": {**target.to_dict(), "id": "stale-child"},
                        "_ucloud_operation": {
                            **payload["_ucloud_operation"],
                            "operation_id": "stale-child-operation",
                            "spec_hash": sandbox_spec_fingerprint(
                                replace(target, id="stale-child")
                            ),
                        },
                        "_ucloud_source": {
                            **payload["_ucloud_source"],
                            "spec_hash": "0" * 64,
                        },
                    },
                    allow_error=True,
                )
                mixed = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        **payload,
                        "sandboxes": [target.to_dict()],
                        "_ucloud_operations": [payload["_ucloud_operation"]],
                    },
                    allow_error=True,
                )
                original_get = manager.get

                def unavailable_store(_sandbox_id: str):
                    raise ValueError("corrupt sandbox store")

                manager.get = unavailable_store  # type: ignore[method-assign]
                try:
                    store_unavailable = self._json_request(
                        f"{base}/v1/sandboxes/fork-parent/forks",
                        method="POST",
                        payload=payload,
                        allow_error=True,
                    )
                finally:
                    manager.get = original_get  # type: ignore[method-assign]
                failed_target = sandbox_fork_target(
                    source_spec,
                    {"id": "failed-after-intent"},
                )
                failed_payload = {
                    "sandbox": failed_target.to_dict(),
                    "_ucloud_operation": {
                        "operation_id": "failed-after-intent-operation",
                        "generation": 1,
                        "kind": "create",
                        "spec_hash": sandbox_spec_fingerprint(failed_target),
                    },
                    "_ucloud_source": payload["_ucloud_source"],
                }
                runtime.dry_run = False
                runtime.executor = RecordingExecutor(
                    exit_code=1,
                    stderr="forced restore failure",
                )
                failed_after_intent = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=failed_payload,
                    allow_error=True,
                )
                manager.store.delete("fork-parent")
                source_missing_replay = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=payload,
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(forked["sandbox"]["creation_kind"], "restore")
        self.assertEqual(forked["sandbox"]["source_sandbox_id"], "fork-parent")
        self.assertEqual(forked["fork"]["commands"], [])
        self.assertIs(forked["intent_persisted"], True)
        self.assertTrue(replayed["timings"]["manager"]["idempotent"])
        self.assertEqual(stale["status"], 409)
        self.assertIs(stale["intent_persisted"], False)
        self.assertEqual(mixed["status"], 400)
        self.assertIn("both sandbox and sandboxes", mixed["error"])
        self.assertEqual(store_unavailable["status"], 503)
        self.assertNotIn("intent_persisted", store_unavailable)
        self.assertEqual(failed_after_intent["status"], 503)
        self.assertIs(failed_after_intent["intent_persisted"], True)
        self.assertEqual(source_missing_replay["status"], 404)
        self.assertIs(source_missing_replay["intent_persisted"], True)

    def test_live_fork_endpoint_restores_batch_from_one_checkpoint(self) -> None:
        with TemporaryDirectory() as raw_dir:
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=Path(raw_dir) / "checkpoints",
            )
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
            )
            Thread(target=server.serve_forever, daemon=True).start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "fork-parent",
                        "image": "busybox",
                        "command": ["sleep", "infinity"],
                        "memory_mb": 64,
                        "disk_mb": 64,
                        "forkable": True,
                        "fork_protocol": {
                            "version": "agent-v1",
                            "prepare_command": ["/ucloud/fork-agent", "prepare"],
                            "ready_command": ["/ucloud/fork-agent", "ready"],
                        },
                    },
                )["sandbox"]
                manager = server.RequestHandlerClass.manager
                source_record = manager.get("fork-parent")
                self.assertIsNotNone(source_record)
                manager.store.upsert(replace(source_record, state="running"))
                source_spec = SandboxSpec.from_dict(created["spec"])
                targets = tuple(
                    sandbox_fork_target(source_spec, {"id": child_id})
                    for child_id in ("fork-child-a", "fork-child-b")
                )
                payload = {
                    "sandboxes": [target.to_dict() for target in targets],
                    "_ucloud_operations": [
                        {
                            "operation_id": f"fork-batch-{index}",
                            "generation": 1,
                            "kind": "create",
                            "spec_hash": sandbox_spec_fingerprint(target),
                        }
                        for index, target in enumerate(targets)
                    ],
                    "_ucloud_source": {
                        "generation": created["generation"],
                        "spec_hash": created["spec_hash"],
                    },
                }
                forked = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=payload,
                )
                replayed = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload=payload,
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(
            [record["id"] for record in forked["sandboxes"]],
            ["fork-child-a", "fork-child-b"],
        )
        self.assertEqual(
            {record["checkpoint_id"] for record in forked["sandboxes"]},
            {forked["forks"][0]["checkpoint_id"]},
        )
        self.assertEqual(
            [item["sandbox_id"] for item in forked["forks"]],
            ["fork-child-a", "fork-child-b"],
        )
        self.assertIs(forked["intent_persisted"], True)
        self.assertTrue(replayed["timings"]["manager"]["idempotent"])

    def test_node_control_auth_protects_every_non_health_route(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                node_control_bearer_token="node-secret",
            )
            Thread(target=server.serve_forever, daemon=True).start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                health = self._json_request(f"{base}/healthz")
                unauthorized = self._json_request(
                    f"{base}/v1/heartbeat",
                    allow_error=True,
                )
                wrong_header = self._json_request(
                    f"{base}/v1/heartbeat",
                    headers={"X-UCloud-Sandbox-Token": "node-secret"},
                    allow_error=True,
                )
                heartbeat = self._json_request(
                    f"{base}/v1/heartbeat",
                    headers={"Authorization": "Bearer node-secret"},
                )
                drain = self._json_request(
                    f"{base}/v1/drain",
                    method="POST",
                    payload={"token": "drain-auth", "draining": True},
                    headers={"Authorization": "Bearer node-secret"},
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertTrue(health["ok"])
        self.assertEqual(unauthorized["status"], 401)
        self.assertEqual(wrong_header["status"], 401)
        self.assertEqual(heartbeat["heartbeat"]["node_id"], "node-1")
        self.assertEqual(drain["drain"]["token"], "drain-auth")

    def test_node_control_auth_rejects_empty_configured_token(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            NodeGatewayClient(
                "http://node.invalid",
                node_control_bearer_token="",
            )
        with TemporaryDirectory() as raw_dir:
            with self.assertRaisesRegex(ValueError, "cannot be empty"):
                build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    job_id="job-1",
                    node_id="node-1",
                    node_control_bearer_token="",
                )

    def test_create_capacity_uses_effective_overcommitted_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(memory_mb=64),
                memory_overcommit=2.0,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                payload = {"id": "fills-node", "image": "busybox", "memory_mb": 128}
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=payload,
                )
                replayed = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=payload,
                )
                rejected = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "one-too-many", "image": "busybox", "memory_mb": 1},
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(created["sandbox"]["id"], "fills-node")
            self.assertTrue(replayed["timings"]["manager"]["idempotent"])
            self.assertEqual(rejected["status"], 503)
            self.assertIn("exhausted memory_mb", rejected["error"])

    def test_drain_endpoint_persists_blocks_admission_and_undrains(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"

            def start_server():
                server = build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=sandbox_file,
                    image_file=image_file,
                    job_id="job-1",
                    node_id="node-1",
                    image_builds_enabled=True,
                )
                thread = Thread(target=server.serve_forever, daemon=True)
                thread.start()
                host, port = server.server_address
                return server, f"http://{host}:{port}"

            server, base = start_server()
            try:
                drained = self._json_request(
                    f"{base}/v1/drain",
                    method="POST",
                    payload={"token": "drain-http", "draining": True},
                )["drain"]
                replay = self._json_request(
                    f"{base}/v1/drain",
                    method="POST",
                    payload={"token": "drain-http", "draining": True},
                )["drain"]
                mismatch = self._json_request(
                    f"{base}/v1/drain",
                    method="POST",
                    payload={"token": "other-drain", "draining": True},
                    allow_error=True,
                )
                blocked_create = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "blocked", "image": "busybox", "memory_mb": 64},
                    allow_error=True,
                )
                blocked_build = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "blocked-image",
                        "tag": "local/blocked:latest",
                        "context_path": "/tmp/context",
                        "wait": False,
                    },
                    allow_error=True,
                )
                heartbeat = self._json_request(f"{base}/v1/heartbeat")["heartbeat"]
            finally:
                server.shutdown()
                server.server_close()

            self.assertTrue(drained["ready"])
            self.assertFalse(drained["admission_open"])
            self.assertEqual(replay["activity_epoch"], drained["activity_epoch"])
            self.assertEqual(mismatch["status"], 409)
            self.assertEqual(blocked_create["status"], 503)
            self.assertEqual(blocked_build["status"], 503)
            self.assertTrue(heartbeat["draining"])
            self.assertEqual(heartbeat["drain_token"], "drain-http")
            self.assertFalse(heartbeat["admission_open"])
            self.assertEqual(
                heartbeat["drain_activity_epoch"],
                heartbeat["activity_epoch"],
            )

            restarted, restarted_base = start_server()
            try:
                restarted_heartbeat = self._json_request(
                    f"{restarted_base}/v1/heartbeat"
                )["heartbeat"]
                opened = self._json_request(
                    f"{restarted_base}/v1/drain",
                    method="POST",
                    payload={"token": "drain-http", "draining": False},
                )["drain"]
                opened_replay = self._json_request(
                    f"{restarted_base}/v1/drain",
                    method="POST",
                    payload={"token": "drain-http", "draining": False},
                )["drain"]
                accepted = self._json_request(
                    f"{restarted_base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "accepted", "image": "busybox", "memory_mb": 64},
                )
                open_heartbeat = self._json_request(
                    f"{restarted_base}/v1/heartbeat"
                )["heartbeat"]
            finally:
                restarted.shutdown()
                restarted.server_close()

            self.assertTrue(restarted_heartbeat["draining"])
            self.assertEqual(restarted_heartbeat["drain_token"], "drain-http")
            self.assertFalse(opened["draining"])
            self.assertTrue(opened["admission_open"])
            self.assertEqual(
                opened_replay["activity_epoch"],
                opened["activity_epoch"],
            )
            self.assertEqual(accepted["sandbox"]["id"], "accepted")
            self.assertFalse(open_heartbeat["draining"])
            self.assertEqual(open_heartbeat["drain_token"], "")
            self.assertTrue(open_heartbeat["admission_open"])

    def test_generation_envelope_delete_headers_and_inventory(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                spec_payload = {
                    "id": "versioned",
                    "image": "busybox",
                    "memory_mb": 128,
                }
                spec_hash = sandbox_spec_fingerprint(
                    SandboxSpec.from_dict(spec_payload)
                )
                create_payload = {
                    **spec_payload,
                    "_ucloud_operation": {
                        "operation_id": "create-1",
                        "generation": 1,
                        "kind": "create",
                        "spec_hash": spec_hash,
                    },
                }
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                )
                replay = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                )
                heartbeat = self._json_request(f"{base}/v1/heartbeat")["heartbeat"]
                legacy_delete = self._json_request(
                    f"{base}/v1/sandboxes/versioned",
                    method="DELETE",
                    allow_error=True,
                )
                missing_header = self._json_request(
                    f"{base}/v1/sandboxes/versioned",
                    method="DELETE",
                    headers={SANDBOX_GENERATION_HEADER: "1"},
                    allow_error=True,
                )
                delete_headers = {
                    SANDBOX_GENERATION_HEADER: "1",
                    SANDBOX_OPERATION_ID_HEADER: "delete-1",
                }
                deleted = self._json_request(
                    f"{base}/v1/sandboxes/versioned",
                    method="DELETE",
                    headers=delete_headers,
                )
                delete_replay = self._json_request(
                    f"{base}/v1/sandboxes/versioned",
                    method="DELETE",
                    headers=delete_headers,
                )
                stale_create = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(created["sandbox"]["generation"], 1)
            self.assertEqual(created["sandbox"]["operation_id"], "create-1")
            self.assertEqual(replay["sandbox"]["generation"], 1)
            self.assertEqual(legacy_delete["status"], 409)
            self.assertEqual(missing_header["status"], 400)
            self.assertEqual(deleted["deleted"]["generation"], 1)
            self.assertIsNone(delete_replay["deleted"])
            self.assertEqual(stale_create["status"], 409)
            self.assertEqual(heartbeat["inventory"][0]["generation"], 1)
            self.assertEqual(
                heartbeat["inventory"][0]["operation_id"],
                "create-1",
            )
            self.assertEqual(heartbeat["inventory"][0]["spec_hash"], spec_hash)

    def test_heartbeat_inventory_is_coherent_during_concurrent_creates(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
            )
            server_thread = Thread(target=server.serve_forever, daemon=True)
            server_thread.start()
            errors: list[BaseException] = []
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"

                def create(index: int) -> None:
                    try:
                        self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": f"sandbox-{index}",
                                "image": "busybox",
                                "memory_mb": 64,
                            },
                        )
                    except BaseException as exc:
                        errors.append(exc)

                creators = [Thread(target=create, args=(index,)) for index in range(6)]
                for creator in creators:
                    creator.start()
                samples = [
                    self._json_request(f"{base}/v1/heartbeat")["heartbeat"]
                    for _index in range(6)
                ]
                for creator in creators:
                    creator.join(timeout=5)
                samples.append(self._json_request(f"{base}/v1/heartbeat")["heartbeat"])
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(errors, [])
            for heartbeat in samples:
                inventory = heartbeat["inventory"]
                reserved_memory = sum(
                    item["resources"]["memory_mb"] for item in inventory
                )
                self.assertTrue(heartbeat["inventory_complete"])
                self.assertEqual(heartbeat["activity_epoch"], len(inventory))
                self.assertEqual(
                    heartbeat["used_resources"]["memory_mb"],
                    0,
                )
                self.assertEqual(
                    heartbeat["reserved_resources"]["memory_mb"],
                    reserved_memory,
                )

    def test_rejects_oversized_and_negative_length_requests(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                max_json_body_bytes=32,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                oversized = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes",
                    method="POST",
                    payload={"id": "large", "image": "busybox", "memory_mb": 128},
                    allow_error=True,
                )
                connection = http.client.HTTPConnection(host, port, timeout=5)
                connection.putrequest("POST", "/v1/sandboxes")
                connection.putheader("Content-Length", "-1")
                connection.endheaders()
                negative_response = connection.getresponse()
                negative_status = negative_response.status
                negative_response.read()
                connection.close()
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(oversized["status"], 413)
            self.assertEqual(negative_status, 400)

    def test_runtime_delete_failure_is_503_and_validation_is_400(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=DeleteFailureRuntime(),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                failed_delete = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1",
                    method="DELETE",
                    allow_error=True,
                )
                invalid = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "bad/id", "image": "busybox", "memory_mb": 128},
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(failed_delete["status"], 503)
            self.assertEqual(invalid["status"], 400)

    def test_node_agent_server_uses_high_listen_backlog(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
            )
            try:
                self.assertGreaterEqual(
                    server.request_queue_size,
                    DEFAULT_HTTP_REQUEST_QUEUE_SIZE,
                )
            finally:
                server.server_close()

    def test_creates_lists_deletes_sandbox_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = DockerGvisorRuntime(dry_run=True)
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                node_url="http://node-1:8090",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
                cpu_overcommit=2.0,
                runtime=runtime,
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=25.0,
                    cpu_vcpu=1.0,
                    cpu_count=4,
                    memory_total_mb=8192,
                    memory_used_mb=2048,
                    memory_available_mb=6144,
                    memory_percent=25.0,
                ),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                create_payload = {
                    "id": "sbx-1",
                    "image": "busybox",
                    "command": ["true"],
                    "memory_mb": 128,
                }
                create = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                )
                retry = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                )
                conflict = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "sbx-1",
                        "image": "python:3.12-slim",
                        "command": ["true"],
                        "memory_mb": 128,
                    },
                    allow_error=True,
                )
                listed = self._json_request(f"{base}/v1/sandboxes")
                healthz = self._json_request(f"{base}/healthz")
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                second_heartbeat = self._json_request(f"{base}/v1/heartbeat")
                deleted = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1",
                    method="DELETE",
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(create["sandbox"]["spec"]["id"], "sbx-1")
            self.assertFalse(create["timings"]["manager"]["idempotent"])
            self.assertEqual(retry["sandbox"]["id"], "sbx-1")
            self.assertTrue(retry["timings"]["manager"]["idempotent"])
            self.assertEqual(conflict["status"], 409)
            self.assertEqual(create["sandbox"]["state"], "planned")
            self.assertEqual(listed["sandboxes"][0]["spec"]["id"], "sbx-1")
            self.assertEqual(listed["sandboxes"][0]["id"], "sbx-1")
            self.assertEqual(listed["sandboxes"][0]["sandbox_id"], "sbx-1")
            self.assertEqual(listed["sandboxes"][0]["image"], "busybox")
            self.assertEqual(
                healthz,
                {
                    "ok": True,
                    "service": "node-agent",
                    "version": package_version(),
                },
            )
            self.assertEqual(heartbeat["heartbeat"]["node_url"], "http://node-1:8090")
            self.assertEqual(heartbeat["heartbeat"]["active_sandboxes"], 0)
            self.assertEqual(heartbeat["heartbeat"]["effective_resources"]["vcpu"], 8.0)
            self.assertEqual(heartbeat["heartbeat"]["runtime_metrics"]["cpu_percent"], 25.0)
            self.assertEqual(heartbeat["heartbeat"]["runtime_metrics"]["memory_used_mb"], 2048)
            self.assertTrue(heartbeat["heartbeat"]["node_epoch"])
            self.assertEqual(
                second_heartbeat["heartbeat"]["node_epoch"],
                heartbeat["heartbeat"]["node_epoch"],
            )
            self.assertEqual(heartbeat["heartbeat"]["activity_epoch"], 1)
            self.assertTrue(heartbeat["heartbeat"]["inventory_complete"])
            self.assertEqual(
                heartbeat["heartbeat"]["used_resources"]["memory_mb"],
                0,
            )
            self.assertEqual(
                heartbeat["heartbeat"]["reserved_resources"]["memory_mb"],
                128,
            )
            self.assertGreater(heartbeat["heartbeat"]["physical_disk_total_mb"], 0)
            self.assertGreater(heartbeat["heartbeat"]["physical_disk_free_mb"], 0)
            inventory = heartbeat["heartbeat"]["inventory"]
            self.assertEqual(len(inventory), 1)
            self.assertEqual(inventory[0]["sandbox_id"], "sbx-1")
            self.assertEqual(inventory[0]["state"], "planned")
            self.assertEqual(inventory[0]["resources"]["memory_mb"], 128)
            self.assertEqual(
                inventory[0]["spec_hash"],
                sandbox_spec_fingerprint(SandboxSpec.from_dict(create_payload)),
            )
            self.assertEqual(deleted["deleted"]["spec"]["id"], "sbx-1")

    def test_builds_images_and_snapshots_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = DockerGvisorRuntime(dry_run=True)
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
                image_builds_enabled=True,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                built = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "python-base",
                        "tag": "local/python-base:latest",
                        "context_path": "/tmp/context",
                        "dockerfile": "Dockerfile",
                    },
                )
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                snapshot = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/snapshot",
                    method="POST",
                    payload={
                        "id": "snap-1",
                        "image": "local/snap-1:latest",
                    },
                )
                images = self._json_request(f"{base}/v1/images")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(built["image"]["id"], "python-base")
            self.assertIn("build", built["command"])
            self.assertIn("timings", built)
            self.assertIn("wait_for_build_ms", built["timings"]["phases"])
            self.assertIn("docker_build_ms", built["build"]["timings"]["phases"])
            self.assertIn("timings", created)
            self.assertIn("manager_create_ms", created["timings"]["phases"])
            self.assertIn("docker_create_ms", created["timings"]["manager"]["phases"])
            self.assertEqual(
                heartbeat["heartbeat"]["capabilities"],
                ["image-cache", "image-build", "snapshot"],
            )
            self.assertEqual(snapshot["image"]["id"], "snap-1")
            self.assertIn("commit", snapshot["command"])
            self.assertEqual(len(images["images"]), 2)

    def test_image_builds_are_tracked_and_deduplicated(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            executor = BlockingExecutor()
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                image_runtime=DockerImageRuntime(executor=executor),
                image_builds_enabled=True,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                payload = {
                    "id": "python-base",
                    "tag": "local/python-base:latest",
                    "context_path": "/tmp/context",
                    "wait": False,
                }
                first = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=payload,
                )
                self.assertTrue(executor.started.wait(2))
                duplicate = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=payload,
                )
                active = self._json_request(f"{base}/v1/images/builds/python-base")
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                executor.release.set()
                finished = self._wait_for_build(base, "python-base")
                images = self._json_request(f"{base}/v1/images")
            finally:
                server.shutdown()
                server.server_close()

            self.assertTrue(first["started"])
            self.assertFalse(duplicate["started"])
            self.assertEqual(first["build"]["build_id"], duplicate["build"]["build_id"])
            self.assertEqual(active["build"]["status"], "running")
            self.assertEqual(heartbeat["heartbeat"]["active_image_builds"], 1)
            self.assertEqual(finished["status"], "succeeded")
            self.assertIn("building layer", finished["log_tail"])
            self.assertEqual(executor.commands, [("docker", "build", "-f", "/tmp/context/Dockerfile", "-t", "local/python-base:latest", "--label", "ucloud-sandboxes.image=true", "--label", "ucloud-sandboxes.image-id=python-base", "/tmp/context")])
            self.assertEqual(images["images"][0]["id"], "python-base")

    def test_regular_node_rejects_image_builds(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                result = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "base",
                        "tag": "local/base:latest",
                        "context_path": "/tmp/context",
                    },
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(heartbeat["heartbeat"]["capabilities"], ["sandbox", "image-cache"])
            self.assertEqual(result["status"], 403)

    def test_node_heartbeat_includes_extra_security_capabilities(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                extra_capabilities=("disk-quota",),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                heartbeat = self._json_request(f"http://{host}:{port}/v1/heartbeat")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(
                heartbeat["heartbeat"]["capabilities"],
                ["sandbox", "image-cache", "disk-quota"],
            )

    def test_node_heartbeat_includes_cached_images(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                image_runtime=DockerImageRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/images/pull",
                    method="POST",
                    payload={"image": "busybox:latest", "id": "busybox"},
                )
                digest = "sha256:" + "c" * 64
                self._json_request(
                    f"{base}/v1/images/pull",
                    method="POST",
                    payload={
                        "image": f"registry.test/team/image:v1@{digest}",
                        "id": "pinned",
                    },
                )
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
            finally:
                server.shutdown()
                server.server_close()

            self.assertTrue(heartbeat["heartbeat"]["cached_images_known"])
            self.assertIn("busybox", heartbeat["heartbeat"]["cached_images"])
            self.assertIn("busybox:latest", heartbeat["heartbeat"]["cached_images"])
            self.assertIn(
                f"registry.test/team/image@{digest}",
                heartbeat["heartbeat"]["cached_images"],
            )

    def test_rejects_disk_request_without_validated_quota_runtime(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                result = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "disk-denied",
                        "image": "busybox",
                        "disk_mb": 16,
                    },
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(result["status"], 400)
            self.assertIn("validated Docker storage quota", result["error"])

    def test_exec_session_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                started = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/exec",
                    method="POST",
                    payload={"command": ["echo", "ok"]},
                )
                session_id = started["session"]["id"]
                events = self._json_request(f"{base}/v1/exec/{session_id}/events")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(started["session"]["status"], "exited")
            self.assertEqual(events["session"]["exit_code"], 0)
            self.assertEqual(
                [event["stream"] for event in events["events"]],
                ["status", "status", "exit"],
            )

    def test_sandbox_ssh_target_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                ssh_port_range=(23000, 23001),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "ssh-one",
                        "image": "sandbox-ssh:latest",
                        "memory_mb": 128,
                        "network": "bridge",
                        "ssh": {"enabled": True, "user": "sandbox"},
                    },
                )
                target = self._json_request(f"{base}/v1/sandboxes/ssh-one/ssh")
                manager = server.RequestHandlerClass.manager
                record = manager.get("ssh-one")
                self.assertIsNotNone(record)
                manager.store.upsert(replace(record, state="restoring"))
                manager.runtime.dry_run = False
                quarantined = self._json_request(
                    f"{base}/v1/sandboxes/ssh-one/ssh",
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(target["ssh"]["port"], 23000)
            self.assertEqual(target["ssh"]["command"], "ssh -p 23000 sandbox@127.0.0.1")
            self.assertEqual(quarantined["status"], 409)

    def test_file_upload_and_download_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = FileRuntime()
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                uploaded = self._bytes_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/a.txt')}",
                    method="PUT",
                    body=b"hello file\n",
                )
                downloaded = self._bytes_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/a.txt')}",
                )
                bad_path = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/')}",
                    method="PUT",
                    payload={},
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(uploaded["json"]["size"], 11)
        self.assertEqual(downloaded["body"], b"hello file\n")
        self.assertEqual(downloaded["headers"]["X-Sandbox-Path"], "/workspace/a.txt")
        self.assertEqual(bad_path["status"], 400)
        self.assertIn("must identify a file", bad_path["error"])

    def test_file_download_enforces_exact_configured_body_limit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            runtime = FileRuntime()
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "sandboxes.json",
                image_file=Path(raw_dir) / "images.json",
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
                max_file_body_bytes=8,
            )
            Thread(target=server.serve_forever, daemon=True).start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                exact = b"\x00\xffbinary"
                runtime.files[("sbx-1", "/workspace/exact.bin")] = exact
                runtime.files[("sbx-1", "/workspace/large.bin")] = exact + b"!"
                downloaded = self._bytes_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/exact.bin')}"
                )
                oversized = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/large.bin')}",
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(downloaded["body"], exact)
        self.assertEqual(oversized["status"], 413)
        self.assertIn("8 byte download limit", oversized["error"])

    def test_async_gateway_exec_handle_reads_events(self) -> None:
        async def scenario(base: str) -> list[str]:
            client = NodeGatewayClient(
                base,
                node_control_bearer_token="node-secret",
            )
            handle = await client.start_exec(
                "sbx-1",
                SandboxExecSpec(sandbox_id="sbx-1", command=("echo", "ok")),
            )
            events = []
            async for event in handle.events(wait_seconds=0.0):
                events.append(event["stream"])
            return events

        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                node_control_bearer_token="node-secret",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                    headers={"Authorization": "Bearer node-secret"},
                )
                streams = asyncio.run(scenario(base))
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(streams, ["status", "status", "exit"])

    def _json_request(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
        allow_error: bool = False,
    ) -> dict:
        body = None
        request_headers = dict(headers or {})
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, method=method, headers=request_headers)
        try:
            with request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            if not allow_error or not hasattr(exc, "code"):
                raise
            body = json.loads(exc.read().decode("utf-8"))
            body["status"] = exc.code
            return body

    def _bytes_request(
        self,
        url: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
    ) -> dict:
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        req = request.Request(url, data=body, method=method, headers=headers)
        with request.urlopen(req, timeout=5) as response:
            raw = response.read()
            content_type = response.headers.get("Content-Type", "")
            if content_type.startswith("application/json"):
                return {
                    "json": json.loads(raw.decode("utf-8")),
                    "headers": response.headers,
                }
            return {"body": raw, "headers": response.headers}

    def _wait_for_build(self, base: str, image_id: str) -> dict:
        for _ in range(40):
            payload = self._json_request(f"{base}/v1/images/builds/{image_id}")
            build = payload["build"]
            if build["status"] in {"succeeded", "failed"}:
                return build
            asyncio.run(asyncio.sleep(0.05))
        raise AssertionError("image build did not finish")


class FileRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self.files: dict[tuple[str, str], bytes] = {}

    def write_file_to_container(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
        *,
        owner: str | None = None,
    ):
        result = super().write_file_to_container(
            sandbox_id,
            container_path,
            content,
            owner=owner,
        )
        self.files[(sandbox_id, container_path)] = content
        return result

    def read_file_from_container(
        self,
        sandbox_id: str,
        container_path: str,
        *,
        max_bytes: int | None = None,
    ):
        _, result = super().read_file_from_container(
            sandbox_id,
            container_path,
            max_bytes=max_bytes,
        )
        return self.files[(sandbox_id, container_path)], result


class DeleteFailureRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    def delete(self, sandbox_id: str) -> CommandResult:
        del sandbox_id
        raise RuntimeError("docker daemon temporarily unavailable")


class BlockingExecutor:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.commands: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        self.commands.append(argv)
        self.started.set()
        self.release.wait(5)
        return CommandResult(
            argv=argv,
            exit_code=0,
            stdout="building layer\n",
        )


if __name__ == "__main__":
    unittest.main()
