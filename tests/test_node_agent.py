from __future__ import annotations

import hashlib
import json
import tarfile
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib import error, request

from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.node_agent import (
    _host_boot_epoch,
    build_builder_node_agent_server,
)
from ucloud_sandboxes.sandbox import (
    SandboxOperation,
    SandboxSpec,
    sandbox_spec_fingerprint,
)

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
        self.assertEqual(heartbeat["capabilities"], ["image-cache", "image-build"])
        self.assertEqual(heartbeat["inventory"], [])
        self.assertEqual(heartbeat["deployment_id"], "deployment-a")
        self.assertEqual(heartbeat["node_epoch"], "builder-boot-1")
        with self.assertRaises(error.HTTPError) as rejected:
            self._json("/v1/sandboxes")
        self.assertEqual(rejected.exception.code, 404)

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
