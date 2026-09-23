"""Real allocation/journal placement changes precede admission cost quotes."""
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from tests import test_split_memory_lifecycle as fixtures
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.memory_backing import MemoryBackingStore
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity
from ucloud_sandboxes.sandbox import SandboxSpec


class HybridMemoryAdmissionTests(unittest.TestCase):
    def setUp(self):
        f = self.fixture = fixtures.SplitLifecycleTests()
        f.setUp()
        self.addCleanup(f.tearDown)
        registry = self.registry = DirectSandboxRegistry(f.root / "registry.sqlite")
        spec = SandboxSpec(id=f.sandbox.sandbox_id, image="image", memory_mb=4096,
                           disk_mb=1024, parkable=True, managed_process=True)
        registration = registry.plan(spec=spec, sandbox_generation=1,
            operation_id="create", runtime_compatibility_sha256="a" * 64,
            split_memory_backing=True)
        f.sandbox = replace(f.sandbox, spec_sha256=registration.spec_sha256,
                            workspace_directory=registration.workspace_directory,
                            memory=registration.memory_reference)
        active_root = f.root / "ram"
        active_root.mkdir(mode=0o700)
        f.warden.config = replace(f.warden.config, application_memory_root=active_root)
        f.runner.memory_root = active_root
        f.storage = fixtures.SplitStorage(sandbox_id=spec.id, sandbox_generation=1,
            volume_id=f.sandbox.workspace_directory,
            mount_path=f.config.memory_root / f.sandbox.workspace_directory)
        f.storage.mount_path.mkdir()
        f.warden.storage = f.storage
        f.rootfs.events = f.storage.events
        with patch("ucloud_sandboxes.memory_backing.subprocess.run",
                   return_value=SimpleNamespace(stdout="tmpfs rw,noswap\n")):
            f.warden.memory_backing = MemoryBackingStore(f.config.memory_root,
                f.root / "allocations.sqlite", hard_capacity_bytes=64 << 30,
                quota=fixtures.FakeQuota(), active_root=active_root)
        f.warden.memory_backing.prepare(registration.memory_reference,
                                       sandbox_id=spec.id, sandbox_generation=1)
        f.warden.memory_capacity = registry
        registration = registry.commit_quota(spec.id, expected_revision=registration.revision,
            project_id=1, total_mb=spec.requested_resources().disk_mb,
            quota_path=f.storage.mount_path)
        registration = registry.commit_rootfs(spec.id, expected_revision=registration.revision,
            image_id="sha256:" + "b" * 64, sandbox=f.sandbox)
        registry.commit_owned(spec.id, expected_revision=registration.revision)
        f.warden.create(f.sandbox, operation_id="create")
        self.capture_initial_checkpoint()
        self.assertEqual(f.warden.application_memory_mode(spec.id, 1), "ram")
        f.warden.config = replace(f.warden.config, reflink_memory_restore=True)
        self.service = DirectSandboxService(SimpleNamespace(warden=f.warden, registry=registry))
        self.service.admission_wait_seconds = .05
        self.service.configure_active_capacity(ResourceQuantity(vcpu=8, memory_mb=16 << 10),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(collected_at=utc_now(),
                cpu_count=8, cpu_percent=0, memory_total_mb=16 << 10,
                memory_available_mb=12 << 10,
                memory_backing=MemoryBackingCapacity(total_bytes=8 << 30,
                    available_bytes=0, identity="ram-mount")))

    def capture_initial_checkpoint(self):
        self.fixture.warden.park(self.fixture.sandbox, operation_id="park-before-upgrade")

    def test_recovered_ram_checkpoint_selects_file_before_restore_admission(self):
        service, f = self.service, self.fixture
        key = (f.sandbox.sandbox_id, 1)
        with service._lock(*key), service._restore_admission(
                *key, ResourceQuantity(memory_mb=4096)):
            self.assertEqual(f.warden.application_memory_mode(*key), "file")
            costs = service.resident_demand_snapshot()
            self.assertEqual(costs["admitted_demand_bytes"], 4 << 30)
            self.assertEqual(costs["admitted_ram_backing_bytes"], 0)
        self.assertEqual(service.resident_demand_snapshot()["admitted_demand_bytes"], 0)

    def test_parked_continuation_selects_file_before_growth_admission(self):
        service, f = self.service, self.fixture
        key = (f.sandbox.sandbox_id, 1)
        self.registry.growth_intent(*key, action="wait", request_id="model-request")
        service.admit_managed_continuation(*key, "model-request")
        self.assertEqual(f.warden.application_memory_mode(*key), "file")
        self.assertTrue(self.registry.relay_wake_fence(*key, "model-request"))
        self.assertEqual(service.warm_park_demand().ram_backing_bytes, 0)
        self.assertEqual(service._restore_cost(*key, ResourceQuantity(memory_mb=4096)).memory_bytes,
                         4 << 30)

    def test_recovered_completed_capture_unblocks_already_queued_continuation(self):
        item = HybridMemoryAdmissionTests()
        self.addCleanup(item.doCleanups)

        def interrupted_capture():
            f = item.fixture
            f.fencer.fail_next_terminate = True
            with self.assertRaisesRegex(RuntimeError, "terminate"):
                f.warden.park(f.sandbox, operation_id="interrupted-capture")
            self.assertEqual(f.warden.inspect(f.sandbox).state, HibernationState.HIBERNATING)

        with patch.object(item, "capture_initial_checkpoint", interrupted_capture):
            item.setUp()
        service, f = item.service, item.fixture
        service.admission_wait_seconds = 3
        key = (f.sandbox.sandbox_id, 1)
        item.registry.growth_intent(*key, action="wait", request_id="queued-model")
        with ThreadPoolExecutor(max_workers=1) as pool:
            with service._lock(*key):
                pending = pool.submit(service.admit_managed_continuation, *key, "queued-model")
                deadline = time.monotonic() + 1
                while not service._transitions.foreground_waiting and time.monotonic() < deadline:
                    time.sleep(.002)
                self.assertTrue(service._transitions.foreground_waiting)
                self.assertFalse(pending.done())
                self.assertEqual(f.warden.application_memory_mode(*key), "ram")
                recovered = f.warden.reconcile(f.sandbox)
                self.assertEqual(recovered.state, HibernationState.PARKED)
                self.assertEqual(f.warden.application_memory_mode(*key), "file")
            # Neither a new wake request nor an admission timeout is required.
            pending.result(1)
        self.assertTrue(item.registry.relay_wake_fence(*key, "queued-model"))

    def test_already_parked_reconcile_selects_file_without_new_wake(self):
        f = self.fixture
        record = f.warden.reconcile(f.sandbox)
        self.assertEqual(record.state, HibernationState.PARKED)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), "file")

    def test_import_adoption_selects_file_before_exposing_parked_owner(self):
        f = self.fixture
        manifest = f.warden.artifacts.load_complete(sandbox_id=f.sandbox.sandbox_id,
            sandbox_generation=1, hibernation_generation=1)
        record = f.warden.adopt_parked(f.sandbox, manifest)
        self.assertEqual(record.state, HibernationState.PARKED)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), "file")

    def test_recovered_checkpoint_keeps_ram_when_feature_disabled(self):
        f = self.fixture
        f.warden.config = replace(f.warden.config, reflink_memory_restore=False)
        self.assertEqual(f.warden.reconcile(f.sandbox).state, HibernationState.PARKED)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), "ram")

    def test_absent_candidate_restore_rollback_selects_file_only_after_parked(self):
        f = self.fixture
        journal = f.warden._journal(f.sandbox)
        parked = journal.load()
        journal.begin_restore(operation_id="interrupted-restore", expected_revision=parked.revision)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), "ram")
        self.assertEqual(f.warden.reconcile(f.sandbox).state, HibernationState.PARKED)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), "file")
