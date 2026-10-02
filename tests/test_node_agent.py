from __future__ import annotations

import hashlib
import json
import tarfile
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from types import SimpleNamespace
from unittest.mock import Mock
from urllib import error, request

from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.build_admission import BUILD_ADMISSION_CAPACITY_LABEL
from ucloud_sandboxes.memory_backing import MemoryBackingBusyError
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.node_agent import (
    NodeAgentHandler,
    _host_boot_epoch,
    build_builder_node_agent_server,
)
from ucloud_sandboxes.sandbox import (
    SandboxDeleteBusyError,
    CommandResult,
    SandboxOperation,
    SandboxRestoreBusyError,
    SandboxSpec,
    sandbox_spec_fingerprint,
)

TEST_TIER = "contract"

TOKEN = "node-control-secret"


def _tar_gz_context(files: dict[str, bytes]) -> bytes:
    output = BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, payload in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            info.mtime = 0
            archive.addfile(info, BytesIO(payload))
    return output.getvalue()


class BuilderNodeAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        self.server = build_builder_node_agent_server(
            "127.0.0.1",
            0,
            state_file=root / "builder.json",
            image_file=root / "images.json",
            job_id="builder-job",
            node_id="builder-node",
            deployment_id="deployment-a",
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=1024),
            image_runtime=DockerImageRuntime(dry_run=True),
            node_control_bearer_token=TOKEN,
            node_epoch="builder-boot-1",
        )
        self.thread = Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary.cleanup()

    def _json(self, path: str, *, method: str = "GET", payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        req = request.Request(
            self.base_url + path,
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                **({"Content-Type": "application/json"} if body is not None else {}),
            },
        )
        with request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)

    def _upload_context(self, files: dict[str, bytes]) -> tuple[bytes, str]:
        archive = _tar_gz_context(files)
        digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
        upload = request.Request(
            f"{self.base_url}/v1/image-contexts/{digest}",
            data=archive,
            method="PUT",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/gzip",
            },
        )
        with request.urlopen(upload, timeout=5) as response:
            self.assertEqual(response.status, 201)
        return archive, digest

    def test_heartbeat_has_exact_builder_surface(self) -> None:
        status, payload = self._json("/v1/heartbeat")
        heartbeat = payload["heartbeat"]
        self.assertEqual(status, 200)
        self.assertEqual(heartbeat["capabilities"], ["image-cache", "image-build", "request-body-keepalive-v1"])
        self.assertEqual(heartbeat["inventory"], [])
        self.assertEqual(heartbeat["deployment_id"], "deployment-a")
        self.assertEqual(heartbeat["node_epoch"], "builder-boot-1")
        self.assertEqual(heartbeat["labels"][BUILD_ADMISSION_CAPACITY_LABEL], "4")
        with self.assertRaises(error.HTTPError) as rejected:
            self._json("/v1/sandboxes")
        self.assertEqual(rejected.exception.code, 404)

    def test_live_heartbeat_reports_build_count_and_capacity_from_one_snapshot(self):
        manager = self.server.RequestHandlerClass.image_manager
        manager.build_admission_snapshot = Mock(return_value={
            "active_builds": 5, "admission_capacity": 6,
        })
        _, payload = self._json("/v1/heartbeat")
        self.assertEqual(payload["heartbeat"]["active_image_builds"], 5)
        self.assertEqual(payload["heartbeat"]["labels"][BUILD_ADMISSION_CAPACITY_LABEL], "6")
        manager.build_admission_snapshot.assert_called_once_with()

    def test_host_boot_epoch_is_stable_and_canonical(self) -> None:
        root = Path(self.temporary.name)
        boot_id_path = root / "boot_id"
        boot_id_path.write_text(
            "4F44F5A7-2504-4FC3-8C26-A14D8D47E81C\n",
            encoding="utf-8",
        )

        first = _host_boot_epoch(boot_id_path)
        second = _host_boot_epoch(boot_id_path)

        self.assertEqual(first, "4f44f5a725044fc38c26a14d8d47e81c")
        self.assertEqual(second, first)

    def test_drain_fences_image_build_admission(self) -> None:
        archive, digest = self._upload_context({"Dockerfile": b"FROM scratch\n"})
        status, payload = self._json(
            "/v1/drain",
            method="POST",
            payload={"draining": True, "token": "drain-1"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(payload["drain"]["ready"])
        with self.assertRaises(error.HTTPError) as rejected:
            self._json(
                "/v1/images/build",
                method="POST",
                payload={
                    "id": "image",
                    "tag": "example/image:latest",
                    "context_path": ".",
                    "context_archive_digest": digest,
                    "context_archive_format": "tar.gz",
                    "context_archive_size": len(archive),
                },
            )
        self.assertEqual(rejected.exception.code, 503)
        with rejected.exception as response:
            body = json.load(response)
            self.assertEqual(body["error_code"], "node_admission_closed")
            self.assertTrue(body["retryable"])
            self.assertEqual(response.headers["Retry-After"], "1")

    def test_drain_rejects_image_pull_before_work_with_admission_fence(self) -> None:
        self._json("/v1/drain", method="POST", payload={"draining": True, "token": "drain-pull"})
        with self.assertRaises(error.HTTPError) as rejected:
            self._json("/v1/images/pull", method="POST", payload={"image": "busybox"})
        with rejected.exception as response:
            body = json.load(response)
            self.assertEqual(response.code, 503)
            self.assertEqual(body["error_code"], "node_admission_closed")
            self.assertTrue(body["retryable"])
        self.assertEqual(self._json("/v1/images")[1]["images"], [])

    def test_image_build_requires_uploaded_content_addressed_context(self) -> None:
        with self.assertRaises(error.HTTPError) as rejected:
            self._json(
                "/v1/images/build",
                method="POST",
                payload={"id": "image", "tag": "example/image:latest"},
            )

        self.assertEqual(rejected.exception.code, 400)
        payload = json.loads(rejected.exception.read())
        self.assertIn("context_archive_digest is required", payload["error"])

    def test_image_build_materializes_uploaded_archive_and_cleans_it(self) -> None:
        archive, digest = self._upload_context({"Dockerfile": b"FROM scratch\n"})

        status, payload = self._json(
            "/v1/images/build",
            method="POST",
            payload={
                "id": "image",
                "tag": "example/image:latest",
                "context_path": ".",
                "context_archive_digest": digest,
                "context_archive_format": "tar.gz",
                "context_archive_size": len(archive),
            },
        )

        self.assertEqual(status, 201)
        self.assertEqual(payload["build"]["status"], "succeeded")
        self.assertFalse(Path(payload["build"]["context_path"]).exists())

    def test_full_builder_rejects_new_work_but_preserves_retry_conflict_and_drain(self):
        archive, digest = self._upload_context({"Dockerfile": b"FROM scratch\n"})
        manager = self.server.RequestHandlerClass.image_manager
        release, full, lock = Event(), Event(), Lock()
        active = 0

        class Executor:
            def run(self, argv):
                nonlocal active
                with lock:
                    active += 1
                    if active == 4:
                        full.set()
                if not release.wait(5):
                    raise TimeoutError("test did not release builds")
                return CommandResult(argv=argv, exit_code=0)

        manager.runtime = DockerImageRuntime(executor=Executor())
        payload = {"context_path": ".", "context_archive_digest": digest,
                   "context_archive_format": "tar.gz", "context_archive_size": len(archive), "wait": False}
        builds = []
        try:
            self.assertEqual(manager.max_active_builds, 4)
            self.assertEqual(manager.max_queued_builds, 0)
            for index in range(4):
                spec = {**payload, "id": f"slot-{index}", "tag": f"local/slot-{index}:latest"}
                status, result = self._json("/v1/images/build", method="POST", payload=spec)
                self.assertEqual(status, 202)
                builds.append(result["build"])
            self.assertTrue(full.wait(2))
            with self.assertRaises(error.HTTPError) as rejected:
                self._json("/v1/images/build", method="POST",
                           payload={**payload, "id": "pending", "tag": "local/pending:latest"})
            with rejected.exception as response:
                self.assertEqual(response.status, 503)
                self.assertEqual(json.load(response)["error_code"], "builder_busy")
            self.assertIsNone(manager.get_build("pending"))
            self.assertEqual(len(manager._queued_builds), 0)
            # Rejection does not consume the reusable uploaded context.
            self.assertEqual(self._json(f"/v1/image-contexts/{digest}")[0], 200)
            _, duplicate = self._json("/v1/images/build", method="POST", payload=spec)
            self.assertFalse(duplicate["started"])
            self.assertEqual(duplicate["build"]["build_id"], builds[-1]["build_id"])
            with self.assertRaises(error.HTTPError) as conflict:
                self._json("/v1/images/build", method="POST", payload={**spec, "build_args": {"X": "changed"}})
            with conflict.exception as response:
                self.assertEqual(response.status, 409)
            _, draining = self._json("/v1/drain", method="POST", payload={"draining": True, "token": "active-builds"})
            self.assertFalse(draining["drain"]["ready"])
        finally:
            release.set()
            for build in builds:
                self.assertEqual(manager.wait_for_build(build["build_id"], timeout_seconds=5).status, "succeeded")
        _, drained = self._json("/v1/drain", method="POST", payload={"draining": True, "token": "active-builds"})
        self.assertTrue(drained["drain"]["ready"])


class RetryableErrorMappingTests(unittest.TestCase):
    def write(self, exc):
        handler = SimpleNamespace(_write_json=Mock())
        NodeAgentHandler._write_exception(handler, exc)
        return handler._write_json.call_args

    def test_restore_busy_keeps_its_retry_contract(self) -> None:
        call = self.write(SandboxRestoreBusyError("busy"))
        self.assertEqual(call.args[0]["error_code"], "node_restore_busy")

    def test_delete_behind_publication_reader_is_retryable(self) -> None:
        for exc in (
            SandboxDeleteBusyError("sandbox memory publication is still draining"),
            MemoryBackingBusyError("memory allocation still has publication readers"),
        ):
            with self.subTest(exc=type(exc).__name__):
                call = self.write(exc)
                self.assertEqual(call.kwargs["status"], 503)
                self.assertEqual(
                    call.kwargs["headers"],
                    {"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"},
                )
                self.assertEqual(
                    call.args[0],
                    {
                        "error": str(exc),
                        "error_code": "memory_publication_draining",
                        "retryable": True,
                    },
                )


class SandboxWireContractTests(unittest.TestCase):
    def test_operation_generation_is_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive"):
            SandboxOperation.from_dict(
                {
                    "generation": 0,
                    "kind": "create",
                    "operation_id": "create-1",
                    "spec_hash": "a" * 64,
                }
            )
        with self.assertRaisesRegex(ValueError, "integer"):
            SandboxOperation.from_dict(
                {
                    "generation": True,
                    "kind": "create",
                    "operation_id": "create-1",
                    "spec_hash": "a" * 64,
                }
            )

    def test_operation_rejects_unknown_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid schema"):
            SandboxOperation.from_dict(
                {
                    "generation": 1,
                    "kind": "create",
                    "operation_id": "create-1",
                    "spec_hash": "a" * 64,
                    "extra": 0,
                }
            )

    def test_spec_rejects_noncanonical_and_permissive_shapes(self) -> None:
        canonical = {
            "id": "sandbox",
            "image": "example/image:latest",
            "memory_mb": 512,
        }
        spec = SandboxSpec.from_dict(canonical)
        spec.validate()
        self.assertEqual(len(sandbox_spec_fingerprint(spec)), 64)
        for invalid in (
            {**canonical, "forkable": False},
            {**canonical, "runtime_profile": "container"},
            {**canonical, "command": "true"},
            {**canonical, "command": ["echo", 1]},
            {**canonical, "parkable": 1},
            {**canonical, "env": {"COUNT": 1}},
            {**canonical, "labels": {"priority": 1}},
            {**canonical, "security": {"init": 1}},
            {**canonical, "filesystem": {"unknown": True}},
            {**canonical, "linux_host": {"enabled": 1}},
            {**canonical, "ssh": {"enabled": False, "extra": True}},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    SandboxSpec.from_dict(invalid)


if __name__ == "__main__":
    unittest.main()
