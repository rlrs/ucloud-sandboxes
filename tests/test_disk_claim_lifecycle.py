"""Lifecycle of demonstrated disk claims (docs/disk-density.md).

A RAM-backed sandbox's memory claim is idle while it runs, is raised to its
measured memory at park admission, settles to the checkpoint's allocated
bytes, and returns to idle after a failed capture. Workspace growth is
admitted against physical capacity before the filesystem grows.
"""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_direct_warden as fixtures
from tests.test_split_memory_lifecycle import FakeQuota, SplitStorage
from ucloud_sandboxes import disk_claims
from ucloud_sandboxes.direct_registry import (
    DirectRegistryCapacityUnavailable,
    DirectSandboxRegistry,
    DiskClaim,
)
from ucloud_sandboxes.disk_claims import DiskClaimPolicy, capture_claim_mb, next_grant
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.memory_backing import MemoryBackingStore
from ucloud_sandboxes.sandbox import SandboxSpec
from ucloud_sandboxes.storage_native_daemon import StorageVolumeRecord, StorageVolumeState

MIB = 1024**2
GIB = 1024**3


def _spec(parkable=True):
    return SandboxSpec(id="sandbox-1", image="registry/image@sha256:" + "a" * 64,
                       memory_mb=1024, disk_mb=4096, parkable=parkable)


class PolicyTests(unittest.TestCase):
    def test_initial_claim_is_grant_plus_idle_memory(self):
        policy = DiskClaimPolicy(workspace_grant_mb=1024, demonstrated_memory=True)
        self.assertEqual(policy.initial_claim(_spec()), DiskClaim(1024, 64))
        # File-backed memory keeps the formula; small disks are never grown.
        formula = _spec().requested_resources().disk_mb - 4096
        self.assertEqual(DiskClaimPolicy(workspace_grant_mb=1024).initial_claim(_spec()),
                         DiskClaim(1024, formula))
        small = replace(_spec(), disk_mb=512)
        self.assertEqual(policy.initial_claim(small).workspace_mb, 512)
        self.assertIsNone(policy.initial_claim(_spec(parkable=False)))
        self.assertIsNone(DiskClaimPolicy().initial_claim(_spec()))
        with self.assertRaises(ValueError):
            DiskClaimPolicy(workspace_grant_mb=256)

    def test_capture_reservation_bounds_memory_and_filestore_without_a_formula_cap(self):
        self.assertEqual(capture_claim_mb(base_bytes=300 * MIB, filestore_bytes=0), 364)
        # A guest that wrote 3 GiB to its rootfs captures it as private pages.
        self.assertEqual(capture_claim_mb(base_bytes=3136 * MIB, filestore_bytes=3 * GIB),
                         3136 + 3072 + 64)

    def test_growth_waits_for_low_free_space_and_steps_toward_the_ceiling(self):
        self.assertIsNone(next_grant(granted=GIB, free=300 * MIB, ceiling=4 * GIB))
        self.assertEqual(next_grant(granted=512 * MIB, free=200 * MIB, ceiling=4 * GIB), GIB)
        self.assertEqual(next_grant(granted=GIB, free=200 * MIB, ceiling=4 * GIB), 1536 * MIB)
        self.assertEqual(next_grant(granted=3 * GIB, free=100 * MIB, ceiling=4 * GIB), 4 * GIB)
        self.assertIsNone(next_grant(granted=4 * GIB, free=0, ceiling=4 * GIB))
        # Large filesystems grow by half and keep a quarter free.
        self.assertEqual(next_grant(granted=8 * GIB, free=GIB, ceiling=64 * GIB), 12 * GIB)

    def test_cgroup_demand_includes_swap(self):
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "proc/7").mkdir(parents=True)
            (root / "proc/7/cgroup").write_text("0::/pod/sandbox\n")
            cgroup = root / "cg/pod/sandbox"
            cgroup.mkdir(parents=True)
            (cgroup / "memory.current").write_text("1000\n")
            self.assertEqual(disk_claims.cgroup_memory_demand(
                7, proc_root=root / "proc", cgroup_root=root / "cg"), 1000)
            (cgroup / "memory.swap.current").write_text("24\n")
            self.assertEqual(disk_claims.cgroup_memory_demand(
                7, proc_root=root / "proc", cgroup_root=root / "cg"), 1024)
            self.assertIsNone(disk_claims.cgroup_memory_demand(
                8, proc_root=root / "proc", cgroup_root=root / "cg"))


class DemonstratedMemoryClaimTests(unittest.TestCase):
    setUp = fixtures.DirectRunscWardenTests.setUp
    tearDown = fixtures.DirectRunscWardenTests.tearDown

    def ram_split(self, capacity_mb=100_000):
        ram = (self.root / "ram").resolve()
        ram.mkdir(mode=0o700)
        self.config = replace(self.config, application_memory_root=ram)
        self.warden.config = self.config
        self.registry = DirectSandboxRegistry(self.root / "registry.sqlite",
                                              hard_disk_capacity_mb=capacity_mb)
        registration = self.registry.plan(
            spec=_spec(), sandbox_generation=1, operation_id="create:1",
            runtime_compatibility_sha256="b" * 64, split_memory_backing=True,
            initial_claim=DiskClaim(1024, 64),
        )
        memory = registration.memory_reference
        self.sandbox = replace(self.sandbox, workspace_directory="workspace-" + self.memory_directory,
                               memory=memory)
        self.storage = SplitStorage(
            sandbox_id=self.sandbox.sandbox_id, sandbox_generation=1,
            volume_id=self.sandbox.workspace_directory,
            mount_path=self.config.memory_root / self.sandbox.workspace_directory,
        )
        self.storage.mount_path.mkdir(parents=True)
        self.warden.storage = self.storage
        self.rootfs.events = self.storage.events
        with patch("ucloud_sandboxes.memory_backing.subprocess.run",
                   return_value=SimpleNamespace(stdout="tmpfs rw,noswap\n")):
            self.memory_backing = MemoryBackingStore(
                self.config.memory_root, self.root / "allocations.sqlite",
                hard_capacity_bytes=capacity_mb * MIB, quota=FakeQuota(), active_root=ram,
            )
        self.memory_backing.prepare(memory, sandbox_id="sandbox-1", sandbox_generation=1,
                                    limit_bytes=64 * MIB)
        self.warden.memory_backing = self.memory_backing
        self.warden.disk_capacity = self.registry
        self.warden.create(self.sandbox, operation_id="create")
        self.memory_backing.trim = lambda: None  # no loop device under test

    def claim(self):
        return self.registry.disk_claim("sandbox-1", 1)

    def limit_mb(self):
        return self.memory_backing.limit_bytes(self.sandbox.memory) // MIB

    def park(self, operation_id="park", demand=3 * MIB):
        with patch("ucloud_sandboxes.disk_claims.cgroup_memory_demand", return_value=demand):
            return self.warden.park(self.sandbox, operation_id=operation_id)

    def test_park_reserves_measured_memory_then_settles_to_the_checkpoint(self):
        self.ram_split()
        self.assertEqual(self.claim().memory_mb, 64)
        reserved = []
        publish = self.warden.artifacts.publish_complete

        def observe(manifest):
            reserved.append((self.claim().memory_mb, self.limit_mb()))
            return publish(manifest)

        with patch.object(self.warden.artifacts, "publish_complete", side_effect=observe):
            parked = self.park()
        self.assertEqual(parked.state, HibernationState.PARKED)
        # 3 MiB resident + 64 MiB capture overhead, held during the capture.
        self.assertEqual(reserved, [(67, 67)])
        allocated = self.memory_backing.allocated_bytes(self.sandbox.memory)
        settled = disk_claims.settled_claim_mb(allocated)
        self.assertLess(settled, 67)
        self.assertEqual((self.claim().memory_mb, self.limit_mb()), (settled, settled))
        self.warden.resume(self.sandbox, operation_id="wake")
        # Nothing to lower: a running RAM owner writes nothing to its directory.
        self.assertEqual(self.warden.settle_idle_memory_claims(), 0)

    def test_capture_estimate_adds_the_ram_memory_file_to_resident_private_pages(self):
        self.ram_split()
        active = self.config.application_memory_root / self.sandbox.memory.allocation_id
        (active / "application_memory.active").write_bytes(b"x" * (8 * MIB))
        allocated = (active / "application_memory.active").stat().st_blocks * 512
        # Resident memory above the 1 GiB limit is capped at the limit.
        with patch("ucloud_sandboxes.disk_claims.cgroup_memory_demand", return_value=5 * GIB):
            demand = self.warden._capture_demand_bytes(self.sandbox, 1)
        self.assertEqual(demand, allocated + GIB)
        with patch("ucloud_sandboxes.disk_claims.cgroup_memory_demand", return_value=None):
            self.assertIsNone(self.warden._capture_demand_bytes(self.sandbox, 1))

    def test_rootfs_filestore_is_reserved_even_beyond_the_formula(self):
        self.ram_split()
        rootfs = self.sandbox.bundle / "rootfs"
        rootfs.mkdir(exist_ok=True)
        filestore = rootfs / f".gvisor.filestore.{self.sandbox.container_id}"
        filestore.write_bytes(b"f" * (16 * MIB))
        allocated = filestore.stat().st_blocks * 512
        observed = []
        publish = self.warden.artifacts.publish_complete
        with patch.object(self.warden.artifacts, "publish_complete",
                          side_effect=lambda manifest: observed.append(self.limit_mb()) or publish(manifest)):
            self.park()
        self.assertEqual(observed, [-(-(3 * MIB + allocated) // MIB) + 64])

    def test_file_backed_dynamic_claims_add_the_filestore_and_keep_the_formula(self):
        self.ram_split()
        self.warden.config = replace(self.config, application_memory_root=None)
        rootfs = self.sandbox.bundle / "rootfs"
        rootfs.mkdir(exist_ok=True)
        (rootfs / f".gvisor.filestore.{self.sandbox.container_id}").write_bytes(b"f" * (4 * MIB))
        self.assertFalse(self.warden._demonstrated_memory(self.sandbox))
        floor = self.warden._reserve_capture_space(self.sandbox, 1)
        formula = self.sandbox.memory.quota_bytes // MIB
        self.assertEqual(floor, formula)
        self.assertGreaterEqual(self.claim().memory_mb, formula + 4 + 64)
        self.warden._settle_capture_space(self.sandbox, floor)
        self.assertEqual(self.claim().memory_mb, formula)

    def test_refused_capture_space_keeps_the_sandbox_running_and_unchanged(self):
        self.ram_split()
        self.registry.hard_disk_capacity_mb = self.claim().total_mb + 2
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.park()
        self.assertEqual(self.warden.inspect(self.sandbox).state, HibernationState.RUNNING)
        self.assertEqual((self.claim().memory_mb, self.limit_mb()), (64, 64))
        # Space appears; idle parking retries are refused cheaply for a moment.
        self.registry.hard_disk_capacity_mb = 100_000
        with patch.object(self.registry, "update_disk_claim",
                          side_effect=AssertionError("backoff should skip the registry")):
            with self.assertRaises(DirectRegistryCapacityUnavailable):
                self.park()
        self.warden._capture_refused_until.clear()  # the backoff elapsed
        self.assertEqual(self.park().state, HibernationState.PARKED)
        self.assertLess(self.claim().memory_mb, 67)

    def test_failed_capture_returns_to_idle_and_next_park_reserves_the_ceiling(self):
        self.ram_split()
        with patch.object(self.warden.artifacts, "publish_complete",
                          side_effect=OSError("quota exceeded")):
            with self.assertRaises(OSError):
                self.park()
        self.assertEqual(self.warden.inspect(self.sandbox).state, HibernationState.RUNNING)
        self.assertEqual(self.claim().memory_mb, 67)
        self.assertEqual(self.warden.settle_idle_memory_claims(), 1)
        self.assertEqual((self.claim().memory_mb, self.limit_mb()), (64, 64))
        observed = []
        publish = self.warden.artifacts.publish_complete
        with patch.object(self.warden.artifacts, "publish_complete",
                          side_effect=lambda manifest: observed.append(self.limit_mb()) or publish(manifest)):
            self.park(operation_id="retry")
        ceiling = self.sandbox.memory.quota_bytes // MIB
        self.assertEqual(observed, [ceiling + disk_claims.CAPTURE_OVERHEAD_MB])

    def test_fixed_split_claims_are_adopted_before_capture(self):
        self.ram_split()
        self.registry = DirectSandboxRegistry(self.root / "fixed.sqlite", hard_disk_capacity_mb=100_000)
        self.registry.plan(spec=_spec(), sandbox_generation=1, operation_id="create:1",
                           runtime_compatibility_sha256="b" * 64, split_memory_backing=True)
        self.warden.disk_capacity = self.registry
        self.assertIsNone(self.claim())
        with self.registry._transaction(write=False) as connection:
            before = self.registry._reserved_disk_bytes(connection)
        # Adoption alone moves nothing: ceiling workspace + formula memory.
        self.assertEqual(self.registry.update_disk_claim("sandbox-1", 1, adopt=True),
                         DiskClaim(4096, _spec().requested_resources().disk_mb - 4096))
        with self.registry._transaction(write=False) as connection:
            self.assertEqual(self.registry._reserved_disk_bytes(connection), before)
        self.park()
        self.assertLess(self.claim().memory_mb, 67)


class _GrowthStorage:
    def __init__(self, mount_path, volume_id):
        self.granted = GIB
        self.state = "mounted"
        self.local = 5 * MIB
        self.mount_path, self.volume_id = mount_path, volume_id
        self.grown = []

    def get_volume(self, volume_id):
        return self.record()

    def record(self):
        return StorageVolumeRecord(
            volume_id=self.volume_id, sandbox_id="sandbox-1", sandbox_generation=1, revision=3,
            state=StorageVolumeState(self.state), operation_id="op", virtual_size=4 * GIB,
            runtime_dir="/runtime", mount_path=str(self.mount_path), source_image_config="/source.json",
            device_owner_id="", accounting_id=1, granted_size=self.granted,
            local_layer_bytes=self.local,
        )

    def grow_volume(self, owner, *, granted_size):
        self.grown.append(granted_size)
        self.granted = granted_size
        return self.record()


class WorkspaceGrowthTests(DemonstratedMemoryClaimTests):
    def growth(self, capacity_mb=100_000):
        self.ram_split(capacity_mb)
        self.growth_storage = _GrowthStorage(
            self.config.memory_root / self.sandbox.workspace_directory, self.sandbox.workspace_directory)
        self.warden.storage = self.growth_storage
        self.warden._sync_workspace_claim(self.sandbox, self.growth_storage.record())

    def test_growth_is_charged_before_the_filesystem_grows(self):
        self.growth()
        self.assertEqual(self.claim().workspace_mb, 1024 + 5)
        (key, mount), = self.warden.workspace_mounts()
        self.assertEqual((mount.granted_size, mount.virtual_size), (GIB, 4 * GIB))
        self.warden.grow_workspace(self.sandbox, 2 * GIB)
        self.assertEqual(self.growth_storage.grown, [2 * GIB])
        self.assertEqual(self.claim().workspace_mb, 2048 + 5)
        self.assertEqual(self.warden.workspace_mounts()[0][1].granted_size, 2 * GIB)

    def test_growth_without_headroom_is_refused_before_the_daemon(self):
        self.growth(capacity_mb=1024 + 5 + 64 + 100)
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.warden.grow_workspace(self.sandbox, 2 * GIB)
        self.assertEqual(self.growth_storage.grown, [])
        self.assertEqual(self.claim().workspace_mb, 1024 + 5)

    def test_busy_owner_is_skipped_and_unmounted_volumes_leave_the_monitor(self):
        self.growth()
        with self.warden._locked(self.sandbox):
            self.assertIsNone(self.warden.grow_workspace(self.sandbox, 2 * GIB))
        self.growth_storage.state = "released"
        self.warden.grow_workspace(self.sandbox, 2 * GIB)
        self.assertEqual(self.growth_storage.grown, [])
        self.assertEqual(self.warden.workspace_mounts(), ())


if __name__ == "__main__":
    unittest.main()
