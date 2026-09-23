"""Assemble the actual runtime against the actual storage Unix protocol.

Only privileged filesystem checks and runtime attestation are substituted;
none of the runtime, storage-client, provisioner, or HTTP constructors are mocks.
Native filesystem/cgroup behavior is covered by the release qualification.
"""

from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from tests import test_storage_native_daemon as storage_fixtures
from tests.test_image_rootfs import Overlay2Runner
from ucloud_sandboxes.direct_runtime import build_direct_runtime_service
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import SandboxSpec
from ucloud_sandboxes.node_agent import build_direct_node_agent_server
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeServer,
    StorageVolumeOwner,
)


class DirectRuntimeAssemblyTests(unittest.TestCase):
    def test_split_and_ram_runtime_start_over_real_storage_wire(self):
        fingerprints = {}
        for ram, reflink in ((False, False), (True, False), (False, True), (True, True)):
            with (
                self.subTest(ram=ram, reflink=reflink),
                TemporaryDirectory(prefix="assembly-", dir="/tmp") as raw,
                ExitStack() as stack,
            ):
                root = Path(raw).resolve()
                capacity = 8 << 30
                native, _, _ = (
                    storage_fixtures.StorageNativeNodeServiceTests()._service(
                        root, capacity=capacity
                    )
                )
                socket = root / "service" / "storage.sock"
                storage_server = StorageNativeNodeServer(
                    socket, native, require_root_peer=False
                )
                storage_thread = Thread(
                    target=storage_server.serve_forever, daemon=True
                )
                storage_thread.start()
                stack.callback(storage_thread.join, 2)
                stack.callback(storage_server.shutdown)
                runsc = root / "runsc"
                runsc.write_bytes(b"test-runtime")
                active = root / "active" if ram else None
                if active:
                    active.mkdir(mode=0o700)
                stack.enter_context(
                    patch(
                        "ucloud_sandboxes.direct_runtime.require_capture_barrier_runtime"
                    )
                )
                stack.enter_context(
                    patch("ucloud_sandboxes.direct_runtime.require_ram_backing_runtime")
                )
                reflink_gate = stack.enter_context(
                    patch("ucloud_sandboxes.direct_runtime.require_reflink_restore_runtime")
                )
                stack.enter_context(
                    patch(
                        "ucloud_sandboxes.direct_runtime.installed_sidecar_fingerprints",
                        return_value={},
                    )
                )
                stack.enter_context(
                    patch(
                        "ucloud_sandboxes.direct_runtime._cpu_features_sha256",
                        return_value="1" * 64,
                    )
                )
                stack.enter_context(
                    patch(
                        "ucloud_sandboxes.memory_backing.XfsMemoryQuota.validate_root"
                    )
                )
                if ram:
                    original_run = subprocess.run

                    def filesystem_probe(args, *a, **kw):
                        if args[0] == "findmnt" and args[-1] == str(active):
                            return subprocess.CompletedProcess(
                                args, 0, stdout="tmpfs rw,noswap\n"
                            )
                        return original_run(args, *a, **kw)

                    stack.enter_context(
                        patch(
                            "ucloud_sandboxes.memory_backing.subprocess.run",
                            side_effect=filesystem_probe,
                        )
                    )
                service = build_direct_runtime_service(
                    state_root=root / "state",
                    volume_mount_root=root / "mounts",
                    runsc=runsc,
                    runsc_commit="a" * 40,
                    init_binary=root / "init",
                    storage_native_socket=socket,
                    split_memory_backing=True,
                    memory_backing_hard_capacity_bytes=capacity,
                    application_memory_root=active,
                    reflink_memory_restore=reflink,
                    checkpoint_registry_url="http://127.0.0.1:5000",
                    checkpoint_registry_repository="qualification",
                )
                stack.callback(service.stop)
                self.assertEqual(reflink_gate.call_count, int(reflink))
                self.assertEqual(service.warden.config.reflink_memory_restore, reflink)
                fingerprints[ram, reflink] = service.warden.config.runtime_fingerprint.node_compatibility_sha256
                service.provisioner.overlays.image_store.runner = Overlay2Runner(
                    root / "docker"
                )
                (root / "docker").chmod(0o700)
                (root / "docker" / "overlay2").chmod(0o700)
                service.provisioner.overlays.image_store._configured_docker_root = (
                    root / "docker"
                )
                self.assertEqual(
                    service.warden.storage.get_metrics()["hard_capacity_bytes"],
                    capacity,
                )
                self.assertEqual(service.warden.memory_backing.active_root, active)
                server = build_direct_node_agent_server(
                    "127.0.0.1",
                    0,
                    service=service,
                    image_file=root / "images.json",
                    job_id="job",
                    node_id="node",
                    deployment_id="test",
                    image_runtime=None,
                    node_control_bearer_token="test-token",
                    runtime_metrics_provider=lambda: None,
                )
                stack.callback(server.server_close)
                self.assertEqual(
                    "sandbox-memory-reflink-restore-v1" in server.RequestHandlerClass.capabilities,
                    reflink,
                )
                stack.callback(server.RequestHandlerClass.manager.stop)
                thread = Thread(target=server.serve_forever, daemon=True)
                thread.start()
                stack.callback(thread.join, 2)
                stack.callback(server.shutdown)
                origin = "http://127.0.0.1:" + str(server.server_address[1])
                with urlopen(origin + "/healthz", timeout=2) as response:
                    self.assertEqual(response.status, 200)
                registry = service.provisioner.registry
                registration = registry.plan(
                    spec=SandboxSpec(
                        id="sandbox",
                        image="image",
                        memory_mb=1,
                        disk_mb=1,
                        parkable=True,
                    ),
                    sandbox_generation=1,
                    operation_id="test-create",
                    runtime_compatibility_sha256=service.provisioner.runtime_compatibility_sha256,
                    split_memory_backing=True,
                )
                workspace = registration.workspace_directory
                volume = service.warden.storage.prepare_volume(
                    StorageVolumeOwner(workspace, "sandbox", 1),
                    operation_id="test-volume",
                    virtual_size=8 << 20,
                )
                registration = registry.commit_quota(
                    "sandbox",
                    expected_revision=registration.revision,
                    project_id=volume.accounting_id,
                    total_mb=8,
                    quota_path=root / "mounts" / workspace,
                )
                registry.commit_rootfs(
                    "sandbox",
                    expected_revision=registration.revision,
                    image_id="sha256:" + "b" * 64,
                    sandbox=DirectSandbox(
                        sandbox_id="sandbox",
                        sandbox_generation=1,
                        container_id="c" * 64,
                        bundle=root / "bundle",
                        spec_sha256=registration.spec_sha256,
                        rootfs_sha256="b" * 64,
                        memory_directory=registration.memory_allocation_id,
                        workspace_directory=workspace,
                        memory=registration.memory_reference,
                    ),
                )
                with urlopen(
                    Request(
                        origin + "/v1/heartbeat",
                        headers={"Authorization": "Bearer test-token"},
                    ),
                    timeout=2,
                ) as response:
                    heartbeat = json.load(response)
                self.assertEqual(heartbeat["heartbeat"]["job_id"], "job")
                self.assertIsNotNone(service.provisioner.checkpoint_store)
        self.assertEqual(fingerprints[False, True], fingerprints[True, True])
        self.assertNotEqual(fingerprints[False, False], fingerprints[True, False])
        self.assertNotEqual(fingerprints[False, True], fingerprints[False, False])
        self.assertNotEqual(fingerprints[True, True], fingerprints[True, False])


if __name__ == "__main__":
    unittest.main()
