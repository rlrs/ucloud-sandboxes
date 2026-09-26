from dataclasses import replace
import json
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.direct_registry import (
    DirectSandboxRegistry,
    DirectRegistryConflictError,
)
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.hibernation import HibernationManifest, HibernationState
from ucloud_sandboxes.memory_backing import MemoryBackingStore, MemoryBackingError
from ucloud_sandboxes.sandbox import SandboxSpec
from tests import test_direct_warden as fixtures
from tests import test_storage_native_daemon as storage_fixtures
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeClient, StorageNativeNodeServer, StorageVolumeState,
    storage_operation_id,
)


class FakeQuota:
    def __init__(self):
        self.projects = {}
        self.limits = {}
        self.fail = False

    def validate_root(self, root):
        pass

    def provision(self, root, path, project_id, quota_bytes):
        if self.fail:
            raise OSError("quota failed")
        self.projects[path] = (project_id, quota_bytes)
        self.limits[project_id] = quota_bytes

    def set_limit(self, project_id, quota_bytes):
        if self.fail:
            raise OSError("quota failed")
        self.limits[project_id] = quota_bytes

    def validate_project(self, path, project_id):
        if self.projects.get(path, (None,))[0] != project_id:
            raise MemoryBackingError("project changed")

    def release(self, root, project_id):
        self.projects = {p: v for p, v in self.projects.items() if v[0] != project_id}

    def release_many(self, root, project_ids):
        for project_id in project_ids:
            self.release(root, project_id)

    def retain_file(self, path, project_id, quota_bytes):
        if self.fail:
            raise OSError("quota failed")
        self.projects[path] = (project_id, quota_bytes)


class SplitStorage(fixtures.FakeStorage):
    def _typed(self):
        return replace(super()._typed(), capture_id=self.record.get("capture_id", ""))

    def prepare_capture(self, owner, *, operation_id, expected_revision):
        self._require_owner(owner)
        assert expected_revision == self.record["revision"]
        self.events.append("prepare-capture")
        self.record.update(
            state="capture_prepared",
            revision=expected_revision + 1,
            capture_id="a" * 64,
        )
        return self._typed()

    def commit_capture(self, owner, *, operation_id, expected_revision):
        self._require_owner(owner)
        assert self.record["state"] == "capture_prepared"
        assert expected_revision == self.record["revision"]
        self.events.append("commit-capture")
        self.record.update(state="sealed", revision=expected_revision + 1)
        return self._typed()

    def abort_capture(self, owner, *, operation_id, expected_revision):
        self._require_owner(owner)
        assert self.record["state"] == "capture_prepared"
        assert expected_revision == self.record["revision"]
        self.events.append("abort-capture")
        self.record.update(state="mounted", revision=expected_revision + 1)
        return self._typed()


class SplitLifecycleTests(unittest.TestCase):
    setUp = fixtures.DirectRunscWardenTests.setUp
    tearDown = fixtures.DirectRunscWardenTests.tearDown

    def split(self, memory=None):
        memory = memory or MemoryBackingRef(self.memory_directory, 8 * 1024 * 1024)
        self.sandbox = replace(
            self.sandbox,
            workspace_directory="workspace-" + self.memory_directory,
            memory=memory,
        )
        self.storage = SplitStorage(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=1,
            volume_id=self.sandbox.workspace_directory,
            mount_path=self.config.memory_root / self.sandbox.workspace_directory,
        )
        self.storage.mount_path.mkdir()
        self.warden.storage = self.storage
        self.rootfs.events = self.storage.events
        self.warden.memory_backing = MemoryBackingStore(
            self.config.memory_root,
            self.root / "allocations.sqlite",
            hard_capacity_bytes=max(16 * 1024 * 1024, memory.quota_bytes),
            quota=FakeQuota(),
        )
        self.warden.memory_backing.prepare(
            memory, sandbox_id=self.sandbox.sandbox_id, sandbox_generation=1
        )
        self.warden.create(self.sandbox, operation_id="create")

    def test_shared_storage_journal_scopes_capture_operations_and_recovery(self):
        self.split()
        storage_root = self.root / "real-storage"
        storage_root.mkdir()
        service, backend, _ = storage_fixtures.StorageNativeNodeServiceTests()._service(storage_root)
        service.config = replace(service.config, mount_root=self.config.memory_root)
        socket_path = self.root / "s.sock"
        server = StorageNativeNodeServer(socket_path, service, require_root_peer=False)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: thread.join(timeout=2))
        self.addCleanup(server.shutdown)
        client = StorageNativeNodeClient(socket_path, timeout_seconds=2)
        client.wait_ready(timeout_seconds=2)
        subjects = [self]
        other = SplitLifecycleTests()
        other.setUp()
        self.addCleanup(other.tearDown)
        other.memory_directory = "sandbox-2.sandbox-1"
        other.config = replace(other.config, memory_root=self.config.memory_root)
        other.warden.config = other.config
        other.sandbox = replace(
            other.sandbox, sandbox_id="sandbox-2", memory_directory=other.memory_directory,
        )
        other.runner.memory_root = self.config.memory_root
        other.runner.memory_directory = other.memory_directory
        other.runner.identity_config = other.config
        config = json.loads((other.bundle / "config.json").read_text())
        config["annotations"]["dev.gvisor.internal.application-memory-directory"] = other.memory_directory
        (other.bundle / "config.json").write_text(json.dumps(config))
        other.split()
        subjects.append(other)
        for subject in subjects:
            subject.warden.storage = client
            (self.config.memory_root / subject.sandbox.workspace_volume_id).rmdir()
            client.prepare_volume(
                subject.warden._storage_owner(subject.sandbox),
                operation_id="same-create", virtual_size=1 << 30,
            )
        # Real Unix protocol and one SQLite storage journal: identical caller
        # park IDs must be independent across sandbox owners, on both decisions.
        for subject in subjects:
            with patch.object(subject.warden.artifacts, "publish_complete", side_effect=OSError("injected publication failure")):
                with self.assertRaisesRegex(OSError, "publication failure"):
                    subject.warden.park(subject.sandbox, operation_id="same-abort")
            self.assertEqual(subject.warden.inspect(subject.sandbox).state, HibernationState.RUNNING)
            subject.warden._abort_workspace_capture(subject.sandbox, operation_seed="same-abort")
        for subject in subjects:
            parked = subject.warden.park(subject.sandbox, operation_id="same-park")
            self.assertEqual(parked.state, HibernationState.PARKED)
            self.assertEqual(subject.warden.reconcile(subject.sandbox), parked)
        self.assertEqual(backend.restack_calls, 4)
        # A pre-upgrade capture already prepared under a raw ID must be
        # aborted by recovery without repeating its physical snapshot.
        for subject in subjects:
            subject.warden.resume(subject.sandbox, operation_id="same-wake")
        owner = self.warden._storage_owner(self.sandbox)
        live = client.get_volume(owner.volume_id)
        prepared = client.prepare_capture(owner, operation_id="old:workspace-prepare", expected_revision=live.revision)
        before = backend.restack_calls
        self.warden._abort_workspace_capture(self.sandbox, operation_seed="old")
        self.warden._abort_workspace_capture(self.sandbox, operation_seed="old")
        self.assertEqual(backend.restack_calls, before)
        self.assertEqual(client.get_volume(owner.volume_id).state, StorageVolumeState.MOUNTED)
        aborted = client.abort_capture(
            owner, operation_id=storage_operation_id(owner, "old", "workspace-abort"),
            expected_revision=prepared.revision,
        )
        self.assertEqual(aborted.state, StorageVolumeState.MOUNTED)
        # An old prepared capture with a COMPLETE artifact instead commits;
        # reconciliation uses its existing capture identity, with no recapture.
        other.fencer.fail_next_terminate = True
        def legacy_prepare_id(owner, operation_id, step):
            if step == "workspace-prepare":
                return f"{operation_id}:workspace-prepare"
            return storage_operation_id(owner, operation_id, step)
        with patch("ucloud_sandboxes.direct_warden.storage_operation_id", side_effect=legacy_prepare_id):
            with self.assertRaisesRegex(DirectWardenError, "terminate"):
                other.warden.park(other.sandbox, operation_id="old-complete")
        before = backend.restack_calls
        recovered = other.warden.reconcile(other.sandbox)
        self.assertEqual(recovered.state, HibernationState.PARKED)
        self.assertEqual(other.warden.reconcile(other.sandbox), recovered)
        self.assertEqual(backend.restack_calls, before)
        self.assertNotEqual(
            storage_operation_id(owner, "old", "workspace-prepare"),
            storage_operation_id(replace(owner, sandbox_generation=2), "old", "workspace-prepare"),
        )

    def test_capture_commit_order_and_round_trip(self):
        self.split()
        original = self.fencer.open

        def fenced(pid, ticks):
            handle = original(pid, ticks)
            terminate = handle.terminate

            def stop(**kw):
                self.storage.events.append("reap")
                terminate(**kw)

            handle.terminate = stop
            return handle

        self.fencer.open = fenced
        parked = self.warden.park(self.sandbox, operation_id="park")
        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(
            self.storage.events,
            ["prepare-capture", "reap", "rootfs-park", "commit-capture", "release"],
        )
        manifest = self.warden.artifacts.load_complete(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=1,
            hibernation_generation=1,
        )
        self.assertEqual(manifest.version, 3)
        self.assertEqual(manifest.memory, self.sandbox.memory)
        self.assertEqual(HibernationManifest.from_dict(manifest.to_dict()), manifest)
        malformed = manifest.to_dict()
        malformed["workspace"]["capture_id"] = "b" * 64
        with self.assertRaises(ValueError):
            HibernationManifest.from_dict(malformed)
        running = self.warden.resume(self.sandbox, operation_id="wake")
        self.assertEqual(running.state, HibernationState.RUNNING)

    def test_publication_failure_aborts_workspace_before_runtime_thaw(self):
        self.split()
        self.runner.before_resume = lambda: self.assertEqual(
            self.storage.events[-1], "abort-capture"
        )
        with patch.object(
            self.warden.artifacts, "publish_complete", side_effect=OSError("disk")
        ):
            with self.assertRaisesRegex(OSError, "disk"):
                self.warden.park(self.sandbox, operation_id="park")
        self.assertEqual(self.runner.status, "running")
        self.assertEqual(
            self.warden._journal(self.sandbox).load().state, HibernationState.RUNNING
        )

    def test_uncertain_capture_never_thaws_original(self):
        self.split()

        def fail(*args, **kwargs):
            self.storage.record["state"] = "error"
            raise OSError("capture failed")

        with patch.object(self.storage, "prepare_capture", side_effect=fail):
            with self.assertRaisesRegex(DirectWardenError, "safe abort"):
                self.warden.park(self.sandbox, operation_id="park")
        self.assertEqual(self.runner.status, "paused")
        self.assertEqual(
            self.warden._journal(self.sandbox).load().state,
            HibernationState.HIBERNATING,
        )

    def test_complete_capture_reconcile_never_thaws_and_preserves_identity(self):
        self.split()
        self.fencer.fail_next_terminate = True
        with self.assertRaisesRegex(DirectWardenError, "terminate"):
            self.warden.park(self.sandbox, operation_id="park")
        self.assertEqual(self.storage.record["state"], "capture_prepared")
        parked = self.warden.reconcile(self.sandbox)
        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(self.storage.record["state"], "released")
        self.assertNotIn("rootfs-resume", self.storage.events)

    def test_mixed_workspace_is_rejected_before_restore(self):
        self.split()
        self.warden.park(self.sandbox, operation_id="park")
        self.storage.record["capture_id"] = "b" * 64
        with self.assertRaisesRegex(DirectWardenError, "component ownership"):
            self.warden.resume(self.sandbox, operation_id="wake")
        self.assertFalse(any("restore" in command for command in self.runner.commands))


class AllocationTests(unittest.TestCase):
    def test_quota_failure_cleanup_retains_then_releases_claim(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            quota = FakeQuota()
            store = MemoryBackingStore(
                root / "memory",
                root / "state.sqlite",
                hard_capacity_bytes=100,
                quota=quota,
            )
            ref = MemoryBackingRef("s.sandbox-1", 80)
            quota.fail = True
            with self.assertRaises(OSError):
                store.prepare(ref, sandbox_id="s", sandbox_generation=1)
            self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 80)
            store.delete(ref, sandbox_id="s", sandbox_generation=1)
            self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 0)
            self.assertFalse((root / "memory" / ref.allocation_id).exists())

    def test_publication_reader_retains_claim_and_reimport_gets_new_project(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            store = MemoryBackingStore(
                root / "memory",
                root / "state.sqlite",
                hard_capacity_bytes=100,
                quota=FakeQuota(),
            )
            ref = MemoryBackingRef("s.sandbox-1", 80)
            old = store.prepare(ref, sandbox_id="s", sandbox_generation=1)
            with store.read_lease(ref, sandbox_id="s", sandbox_generation=1):
                with self.assertRaisesRegex(MemoryBackingError, "publication readers"):
                    store.delete(ref, sandbox_id="s", sandbox_generation=1)
                self.assertEqual(
                    store.metrics()["memory_backing_hard_reserved_bytes"], 80
                )
                self.assertTrue(old.path.exists())
                with self.assertRaises(MemoryBackingError):
                    with store.read_lease(ref, sandbox_id="s", sandbox_generation=1):
                        pass
            store.delete(ref, sandbox_id="s", sandbox_generation=1)
            new = store.prepare(ref, sandbox_id="s", sandbox_generation=1)
            self.assertNotEqual(old.project_id, new.project_id)

    def test_quota_identity_mismatch_cannot_delete_allocation(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            store = MemoryBackingStore(
                root / "memory",
                root / "state.sqlite",
                hard_capacity_bytes=100,
                quota=FakeQuota(),
            )
            ref = MemoryBackingRef("s.sandbox-1", 80)
            lease = store.prepare(ref, sandbox_id="s", sandbox_generation=1)
            data = json.loads((lease.path / store.MARKER).read_text())
            data["project_id"] += 1
            (lease.path / store.MARKER).write_text(json.dumps(data))
            with self.assertRaises(MemoryBackingError):
                store.delete(ref, sandbox_id="s", sandbox_generation=1)
            self.assertTrue(lease.path.exists())

    def test_shared_registry_budget_blocks_combined_overclaim_before_allocation(self):
        with TemporaryDirectory() as directory:
            spec = SandboxSpec(
                id="a", image="image", memory_mb=1024, disk_mb=4096, parkable=True
            )
            required = spec.requested_resources().disk_mb
            registry = DirectSandboxRegistry(
                Path(directory) / "registry.sqlite", hard_disk_capacity_mb=required
            )
            first = registry.plan(
                spec=spec,
                sandbox_generation=1,
                operation_id="a",
                runtime_compatibility_sha256="a" * 64,
                split_memory_backing=True,
            )
            self.assertEqual(
                first.memory_reference.quota_bytes + spec.disk_mb * 1024**2,
                required * 1024**2,
            )
            with self.assertRaisesRegex(DirectRegistryConflictError, "combined"):
                registry.plan_import(
                    spec=replace(spec, id="b"),
                    sandbox_generation=1,
                    operation_id="b",
                    runtime_compatibility_sha256="a" * 64,
                    migration_id="m",
                    migration_sha256="b" * 64,
                    split_memory_backing=True,
                )
