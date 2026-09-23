"""Heartbeat storage identities and charges using real registry/volume records."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from tests import test_direct_warden as warden_fixtures
from ucloud_sandboxes.direct_registry import (
    DirectSandboxRegistry,
    DirectSandboxRegistration,
)
from ucloud_sandboxes.direct_service import (
    DirectSandboxInventoryItem,
    DirectServiceInventorySnapshot,
    DirectServiceActivitySnapshot,
)
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import SandboxRecord, SandboxSpec
from ucloud_sandboxes.storage_native_daemon import StorageVolumeState


class _SnapshotService:
    def __init__(self, registry, registration, warden, state):
        self.provisioner = SimpleNamespace(registry=registry)
        self.registration = registration
        self.warden = warden
        self.state = state

    def open_admission(self):
        pass

    def activity_snapshot(self):
        return DirectServiceActivitySnapshot({}, 0, 0, 1)

    def inventory_snapshot(self):
        r = self.registration
        record = SandboxRecord(
            r.spec,
            r.container_id,
            self.state,
            utc_now(),
            utc_now(),
            r.sandbox_generation,
            r.operation_id,
            r.spec_sha256,
        )
        return DirectServiceInventorySnapshot(
            (DirectSandboxInventoryItem(r, record),), 1
        )


class SplitStorageIdentityTests(unittest.TestCase):
    setUp = warden_fixtures.DirectRunscWardenTests.setUp
    tearDown = warden_fixtures.DirectRunscWardenTests.tearDown

    def registration(self, *, split):
        spec = SandboxSpec(
            id="sandbox-1", image="image", memory_mb=128, disk_mb=512, parkable=True
        )
        incarnation = "sandbox-1.sandbox-1"
        registration = DirectSandboxRegistration(
            spec=spec,
            sandbox_generation=1,
            operation_id="create",
            phase="owned",
            runtime_compatibility_sha256="a" * 64,
            revision=3,
            created_ns=1,
            updated_ns=1,
            quota_project_id=42,
            quota_total_mb=spec.requested_resources().disk_mb,
            quota_path=str(
                self.config.memory_root
                / (("workspace-" if split else "") + incarnation)
            ),
            image_id="sha256:" + "b" * 64,
            rootfs_sha256="b" * 64,
            container_id="c" * 64,
            bundle=str(self.bundle),
            memory_directory=incarnation,
            workspace_directory="workspace-" + incarnation if split else "",
            memory_allocation_id=incarnation if split else "",
            version=4 if split else 3,
        )
        self.warden.storage = warden_fixtures.FakeStorage(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=1,
            volume_id=registration.workspace_volume_id,
            mount_path=Path(registration.quota_path),
        )
        return registration

    def test_split_workspace_publication_retains_memory_claim_without_metrics(self):
        for split in (False, True):
            for storage_state in (
                StorageVolumeState.MOUNTED,
                StorageVolumeState.PUBLISHED,
            ):
                for lifecycle_state in ("running", "parked", "waking"):
                    with self.subTest(
                        split=split, storage=storage_state, lifecycle=lifecycle_state
                    ):
                        r = self.registration(split=split)
                        self.warden.storage.record["state"] = storage_state.value
                        with TemporaryDirectory() as raw:
                            registry = DirectSandboxRegistry(
                                Path(raw) / "registry.sqlite"
                            )
                            service = _SnapshotService(
                                registry, r, self.warden, lifecycle_state
                            )
                            runtime = DirectNodeRuntime(service)
                            observed = runtime._heartbeat_snapshot_locked(
                                active_build_count=lambda: 0
                            ).activity
                        expected = r.quota_total_mb
                        if storage_state == StorageVolumeState.PUBLISHED:
                            expected = (
                                r.memory_reference.quota_bytes // 1024**2
                                if split
                                else 0
                            )
                        self.assertEqual(
                            observed.used_resources.disk_mb
                            + observed.reserved_resources.disk_mb,
                            expected,
                        )
                        node = NodeHeartbeat(
                            job_id="job",
                            node_id="node",
                            deployment_id="test",
                            node_url="http://node",
                            updated_at=utc_now(),
                            active_sandboxes=observed.active_sandboxes,
                            total_resources=ResourceQuantity(disk_mb=10_000),
                            used_resources=observed.used_resources,
                            reserved_resources=observed.reserved_resources,
                        )
                        self.assertEqual(node.free_resources.disk_mb, 10_000 - expected)

    def test_real_typed_owner_uses_workspace_and_rejects_memory_volume_alias(self):
        r = self.registration(split=True)
        sandbox = r.to_direct_sandbox()
        self.assertNotEqual(sandbox.workspace_volume_id, sandbox.memory_directory)
        self.assertEqual(
            DirectSandboxProvisioner._storage_owner(r),
            self.warden._storage_owner(sandbox),
        )
        self.assertEqual(
            tuple(self.warden.storage_records_snapshot((sandbox,))),
            (r.workspace_volume_id,),
        )
        self.warden.storage.record["volume_id"] = sandbox.memory_directory
        with self.assertRaises(DirectWardenError):
            self.warden.storage_records_snapshot((sandbox,))

    def test_property_preserves_legacy_wire_and_planned_identity(self):
        for split in (False, True):
            r = self.registration(split=split)
            wire = r.to_dict()
            self.assertNotIn("workspace_volume_id", wire)
            self.assertEqual(DirectSandboxRegistration.from_dict(wire).to_dict(), wire)
            planned = replace(
                r,
                phase="planned",
                quota_project_id=None,
                quota_total_mb=None,
                quota_path="",
                image_id="",
                rootfs_sha256="",
                container_id="",
                bundle="",
                memory_directory="",
            )
            self.assertEqual(planned.workspace_volume_id, r.workspace_volume_id)
