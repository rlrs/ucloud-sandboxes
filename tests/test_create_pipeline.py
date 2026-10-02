"""Node create pipeline: three durable registry commits, and a crash at each step.

The create writes ``planned``, then ``rootfs_ready`` with its quota, then
``owned``. A crash after any step must restart to one owned sandbox with one
storage volume, one runsc create and one network pair, or delete cleanly.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
import shutil
import unittest

from tests.test_direct_network_pool import FakeKernel, wait_for
from tests.test_direct_provisioner import FakeImageStore, FakeOverlays, FakeStorage, FakeWarden
from ucloud_sandboxes.direct_network import DirectNetworkManager
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


class Crash(BaseException):
    """Process death: no handler below the test may observe it."""


class Overlays(FakeOverlays):
    def discard_unregistered(self, *, sandbox_id, sandbox_generation, workspace_directory=""):
        # As the real manager: the quota-owned writable keeps only its root.
        super().discard_unregistered(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            workspace_directory=workspace_directory,
        )
        writable = self.writable_root / (workspace_directory or f"{sandbox_id}.sandbox-{sandbox_generation}")
        for name in ("upper", "work"):
            if (writable / name).exists():
                shutil.rmtree(writable / name)


class Storage(FakeStorage):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.guard = Lock()
        self.prepares = 0

    def prepare_volume(self, owner, **kwargs):
        with self.guard:
            self.prepares += 1
            return super().prepare_volume(owner, **kwargs)


class Warden(FakeWarden):
    def __init__(self, root: Path, storage: FakeStorage) -> None:
        super().__init__(root, storage)
        self.config.network = "sandbox"
        self.creates = 0

    def create(self, sandbox, *, operation_id):
        self.creates += 1
        return super().create(sandbox, operation_id=operation_id)


class SimpleNode:
    def __init__(self, provisioner, registry, network) -> None:
        self.provisioner, self.registry, self.network = provisioner, registry, network


BOUNDARIES = (
    # (name, object attribute, method): crash just after the method returns.
    ("plan", "registry", "plan"),
    ("storage_prepare", "storage", "prepare_volume"),
    ("network_ensure", "network", "ensure"),
    ("rootfs_prepare", "overlays", "prepare"),
    ("rootfs_commit", "registry", "commit_rootfs"),
    ("runtime_create", "warden", "create"),
    ("owned_commit", "registry", "commit_owned"),
)


class CreatePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.root = Path(self._directory.name).resolve()
        self.kernel = FakeKernel(self.root / "netns")
        images = FakeImageStore(self.root)
        self.overlays = Overlays(images, self.root)
        self.storage = Storage(self.overlays.writable_root)
        self.warden = Warden(self.root, self.storage)
        self.warden.rootfs_lifecycle = self.overlays
        self.networks: list[DirectNetworkManager] = []

    def tearDown(self) -> None:
        for network in self.networks:
            network.stop_pool()
        self._directory.cleanup()

    def node(self, *, pool_size: int = 0) -> SimpleNode:
        """One node-agent process over the durable state in ``self.root``."""
        network = self.kernel.install(DirectNetworkManager(
            self.root / "network-slots.json",
            namespace_root=self.root / "netns",
            pool_size=pool_size,
        ))
        self.networks.append(network)
        registry = DirectSandboxRegistry(self.root / "registry.sqlite")
        provisioner = DirectSandboxProvisioner(
            registry=registry,
            overlays=self.overlays,
            oci=DirectOciConfigBuilder(network_mode="sandbox"),
            warden=self.warden,
            network_manager=network,
        )
        return SimpleNode(provisioner, registry, network)

    @staticmethod
    def spec(name: str = "sandbox") -> SandboxSpec:
        return SandboxSpec(
            id=name, image="image", memory_mb=1024, disk_mb=2048,
            network="bridge", security=SandboxSecuritySpec(init=False),
        )

    def create(self, node, name: str = "sandbox"):
        return node.provisioner.create(
            spec=self.spec(name), sandbox_generation=7, operation_id=f"create:{name}:7"
        )

    def crash_at(self, node, boundary: tuple[str, str, str]) -> None:
        _name, owner, method = boundary
        target = {"registry": node.registry, "storage": self.storage, "network": node.network,
                  "overlays": self.overlays, "warden": self.warden}[owner]
        original = getattr(target, method)

        def then_crash(*args, **kwargs):
            original(*args, **kwargs)
            raise Crash(method)

        setattr(target, method, then_crash)
        try:
            with self.assertRaises(Crash):
                self.create(node)
        finally:
            delattr(target, method)

    def assert_one_owned_sandbox(self, node, registration) -> None:
        self.assertEqual(registration.phase, "owned")
        self.assertEqual(len(self.storage.active_records), 1)
        self.assertEqual(registration.quota_project_id, 200_000)
        self.assertEqual(self.storage.next_project_id, 200_001)
        self.assertEqual(self.warden.creates, 1)
        lease = node.network.lease("sandbox", 7)
        self.assertEqual(self.kernel.links[lease.host_interface], self.kernel.namespace(lease.namespace))
        self.assertNotIn(f"ucloud-pool-{lease.slot}", self.kernel.names())
        state = node.network._load()
        self.assertFalse(set(state.get("pool", ())) & set(state["leases"].values()))

    def test_create_writes_three_commits(self) -> None:
        node = self.node()
        phases: list[str] = []
        write = node.registry._write

        def recorded_write(connection, record, **kwargs):
            phases.append(record.phase)
            write(connection, record, **kwargs)

        node.registry._write = recorded_write
        before = node.registry.activity_revision()

        registration = self.create(node)

        self.assertEqual(phases, ["planned", "rootfs_ready", "owned"])
        self.assertEqual(node.registry.activity_revision() - before, 3)
        self.assertEqual(registration.quota_path, str(self.overlays.writable_root / "sandbox.sandbox-7"))
        self.assertEqual(self.storage.prepares, 1)
        self.assert_one_owned_sandbox(node, registration)

    def test_restart_after_a_crash_at_every_boundary(self) -> None:
        for boundary in BOUNDARIES:
            with self.subTest(boundary=boundary[0]):
                self.tearDown()
                self.setUp()
                crashed = self.node(pool_size=2)
                crashed.provisioner.start()
                wait_for(lambda: len(crashed.network._pool_ready) == 2)
                self.crash_at(crashed, boundary)  # After a pooled hand-off.
                crashed.network.stop_pool()  # Process death frees the pool's flock.
                restarted = self.node(pool_size=2)
                results = restarted.provisioner.start()
                self.assertEqual(len(results), 1)
                self.assert_one_owned_sandbox(restarted, results[0])
                # Pooled slot 1, unless the crash preceded the network lease.
                self.assertEqual(restarted.network.lease("sandbox", 7).slot,
                                 3 if boundary[0] in {"plan", "storage_prepare"} else 1)
                # Storage prepare replays only while the quota is unrecorded.
                replayed = boundary[0] in {"storage_prepare", "network_ensure", "rootfs_prepare"}
                self.assertEqual(self.storage.prepares, 2 if replayed else 1)
                wait_for(lambda: len(restarted.network._pool_ready) == 2)

    def test_delete_after_a_crash_at_every_boundary(self) -> None:
        for boundary in BOUNDARIES:
            with self.subTest(boundary=boundary[0]):
                self.tearDown()
                self.setUp()
                self.crash_at(self.node(), boundary)
                restarted = self.node()
                restarted.provisioner.delete("sandbox")
                self.assertIsNone(restarted.registry.get("sandbox"))
                self.assertEqual(self.storage.active_records, {})
                self.assertEqual(restarted.network._load()["leases"], {})
                self.assertEqual(self.kernel.links, {})
                self.assertEqual(self.kernel.names(), set())
                self.assertEqual(self.warden.records, {})
                self.assertEqual(list(self.overlays.bundle_root.iterdir()), [])

    def test_restart_advances_a_quota_ready_record_from_an_earlier_release(self) -> None:
        node = self.node()
        planned = node.registry.plan(
            spec=self.spec(), sandbox_generation=7, operation_id="create:sandbox:7",
            runtime_compatibility_sha256=node.provisioner.runtime_compatibility_sha256,
        )
        project_id, total_mb, path = node.provisioner._prepare_quota(planned)
        node.registry.commit_quota("sandbox", expected_revision=planned.revision,
                                   project_id=project_id, total_mb=total_mb, quota_path=path)

        results = self.node().provisioner.start()

        self.assertEqual(self.storage.prepares, 1)
        self.assert_one_owned_sandbox(node, results[0])

    def test_concurrent_creates_with_a_partly_empty_pool(self) -> None:
        node = self.node(pool_size=8)
        node.provisioner.start()
        wait_for(lambda: len(node.network._pool_ready) == 8)
        before = node.registry.activity_revision()
        names = [f"s{index:02d}" for index in range(32)]
        with ThreadPoolExecutor(max_workers=32) as pool:
            created = list(pool.map(lambda name: self.create(node, name), names))

        self.assertEqual({item.phase for item in created}, {"owned"})
        self.assertEqual(node.registry.activity_revision() - before, 3 * len(names))
        leases = [node.network.lease(name, 7) for name in names]
        self.assertEqual(len({lease.slot for lease in leases}), len(names))
        for lease in leases:
            self.assertEqual(self.kernel.links[lease.host_interface], self.kernel.namespace(lease.namespace))
        self.assertEqual(len({item.quota_project_id for item in created}), len(names))
        wait_for(lambda: len(node.network._pool_ready) == 8)


if __name__ == "__main__":
    unittest.main()
