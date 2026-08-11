import json
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.direct_warden import (
    CommandResult,
    DirectRunscWarden,
    DirectRunscWardenConfig,
    DirectSandbox,
    DirectWardenError,
)
from ucloud_sandboxes.hibernation import (
    HibernationAuthority,
    HibernationGenerationInventory,
    HibernationValidationError,
    HibernationFileRole,
    HibernationManifest,
    HibernationRecoveryAction,
    HibernationRuntimeFingerprint,
    HibernationState,
    LocalHibernationArtifactFile,
    classify_hibernation_recovery,
)
from ucloud_sandboxes.storage_native_daemon import (
    StorageVolumeOwner,
    StorageVolumeRecord,
    StorageVolumeState,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
CONTAINER_ID = "d" * 64


def write_process(proc_root: Path, pid: int, ticks: int) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True, exist_ok=True)
    suffix = ["S", *(["0"] * 18), str(ticks), "0"]
    (process / "stat").write_text(
        f"{pid} (runsc worker) " + " ".join(suffix) + "\n",
        encoding="ascii",
    )


class FakeHandle:
    def __init__(self, proc_root: Path, pid: int, ticks: int) -> None:
        self.proc_root = proc_root
        self.pid = pid
        self.start_time_ticks = ticks
        self.fail_terminate = False
        self.closed = False

    def alive(self) -> bool:
        return (self.proc_root / str(self.pid) / "stat").exists()

    def terminate(self, *, timeout: float) -> None:
        del timeout
        if self.fail_terminate:
            raise DirectWardenError("injected terminate failure")
        try:
            (self.proc_root / str(self.pid) / "stat").unlink()
            (self.proc_root / str(self.pid)).rmdir()
        except FileNotFoundError:
            pass

    def close(self) -> None:
        self.closed = True


class FakeFencer:
    def __init__(self, proc_root: Path) -> None:
        self.proc_root = proc_root
        self.handles: list[FakeHandle] = []
        self.fail_next_terminate = False

    def open(self, pid: int, start_time_ticks: int) -> FakeHandle:
        handle = FakeHandle(self.proc_root, pid, start_time_ticks)
        handle.fail_terminate = self.fail_next_terminate
        self.fail_next_terminate = False
        self.handles.append(handle)
        return handle


class FakeRunsc:
    def __init__(self, proc_root: Path, memory_root: Path, memory_directory: str):
        self.proc_root = proc_root
        self.memory_root = memory_root
        self.memory_directory = memory_directory
        self.status = "absent"
        self.pid = 100
        self.ticks = 1000
        self.checkpoint: Path | None = None
        self.fail_readiness = False
        self.fail_restore_after_start = False
        self.fail_state_inspection = False
        self.before_resume = None
        self.commands: list[tuple[str, ...]] = []

    def run(self, argv, *, timeout):
        del timeout
        command = tuple(str(item) for item in argv)
        self.commands.append(command)
        verb = next(
            item
            for item in command
            if item
            in {
                "create",
                "start",
                "state",
                "list",
                "checkpoint",
                "resume",
                "restore",
                "exec",
                "delete",
            }
        )
        if verb == "create":
            self.status = "created"
        elif verb == "start":
            self._start_process()
        elif verb == "state":
            if self.fail_state_inspection:
                return CommandResult(command, 1, stderr="injected state failure")
            if self.status == "absent":
                return CommandResult(command, 1, stderr="container not found")
            return CommandResult(
                command,
                0,
                json.dumps({"pid": self.pid, "status": self.status}),
            )
        elif verb == "list":
            containers = (
                []
                if self.status == "absent"
                else [{"id": CONTAINER_ID, "status": self.status}]
            )
            return CommandResult(command, 0, json.dumps(containers))
        elif verb == "checkpoint":
            image = Path(
                next(
                    item.split("=", 1)[1]
                    for item in command
                    if item.startswith("--image-path=")
                )
            )
            self.checkpoint = image
            active = (
                self.memory_root / self.memory_directory / "application_memory.active"
            )
            if not active.exists():
                active.write_bytes(b"memory")
                active.chmod(0o600)
            active.replace(image / "application_memory.img")
            (image / "checkpoint.img").write_bytes(b"kernel")
            (image / "pages_meta.img").write_bytes(b"metadata")
            (image / "pages.img").write_bytes(b"private")
            self.status = "paused"
        elif verb == "resume":
            if self.before_resume is not None:
                self.before_resume()
            assert self.checkpoint is not None
            captured = self.checkpoint / "application_memory.img"
            active = (
                self.memory_root / self.memory_directory / "application_memory.active"
            )
            if captured.exists():
                captured.replace(active)
            self.status = "running"
        elif verb == "restore":
            image = Path(
                next(
                    item.split("=", 1)[1]
                    for item in command
                    if item.startswith("--image-path=")
                )
            )
            self.checkpoint = image
            captured = image / "application_memory.img"
            active = (
                self.memory_root / self.memory_directory / "application_memory.active"
            )
            captured.replace(active)
            self._start_process()
            if "--start-paused" in command:
                self.status = "paused"
            if self.fail_restore_after_start:
                return CommandResult(
                    command,
                    1,
                    stderr="injected restore parent failure",
                )
        elif verb == "exec" and self.fail_readiness:
            return CommandResult(command, 1, stderr="injected readiness failure")
        elif verb == "delete":
            active = (
                self.memory_root / self.memory_directory / "application_memory.active"
            )
            try:
                active.unlink()
            except FileNotFoundError:
                pass
            self.status = "absent"
        return CommandResult(command, 0)

    def _start_process(self) -> None:
        self.pid += 1
        self.ticks += 1
        write_process(self.proc_root, self.pid, self.ticks)
        self.status = "running"


class FakeStorage:
    def __init__(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        mount_path: Path,
    ) -> None:
        self.events: list[str] = []
        self.fail_next_mount = False
        self.fail_next_release = False
        self.fail_next_seal = False
        self.list_calls = 0
        self.mount_path = mount_path
        self.snapshot_path = mount_path.parent / f".{mount_path.name}.snapshot"
        self.record = {
            "mount_path": str(mount_path),
            "revision": 1,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "state": "mounted",
            "volume_id": volume_id,
        }

    def _typed(self) -> StorageVolumeRecord:
        return StorageVolumeRecord(
            volume_id=self.record["volume_id"],
            sandbox_id=self.record["sandbox_id"],
            sandbox_generation=self.record["sandbox_generation"],
            revision=self.record["revision"],
            state=StorageVolumeState(self.record["state"]),
            operation_id="fake:1",
            virtual_size=1,
            runtime_dir=str(self.mount_path.parent / "runtime"),
            mount_path=str(self.mount_path),
            source_image_config=str(self.mount_path.parent / "source.json"),
            device_owner_id="",
            accounting_id=1,
        )

    def _require_owner(self, owner: StorageVolumeOwner) -> None:
        if owner != self._typed().owner:
            raise AssertionError("wrong volume owner")

    def get_volume(self, volume_id: str):
        if volume_id != self.record["volume_id"]:
            raise AssertionError("wrong volume")
        return self._typed()

    def list_volumes(self):
        self.list_calls += 1
        return (self._typed(),)

    def _seal(self):
        if self.fail_next_seal:
            self.fail_next_seal = False
            raise OSError("injected storage seal failure")
        if self.record["state"] != "mounted":
            raise AssertionError("seal requires mounted storage")
        self.events.append("seal")
        self.record["revision"] += 1
        self.record["state"] = "sealed"

    def _release(self):
        if self.fail_next_release:
            self.fail_next_release = False
            raise OSError("injected storage release failure")
        if self.record["state"] != "sealed":
            raise AssertionError("release requires sealed storage")
        self.events.append("release")
        self.record["revision"] += 1
        self.record["state"] = "released"
        if self.snapshot_path.exists():
            shutil.rmtree(self.snapshot_path)
        shutil.copytree(self.mount_path, self.snapshot_path, copy_function=os.link)

    def _mount(self):
        if self.fail_next_mount:
            self.fail_next_mount = False
            raise OSError("injected storage mount failure")
        if self.record["state"] != "released":
            raise AssertionError("mount requires released storage")
        self.events.append("mount")
        self.record["revision"] += 1
        self.record["state"] = "mounted"

    def _discard(self):
        if self.record["state"] != "mounted":
            raise AssertionError("discard requires mounted storage")
        self.events.append("discard")
        self.record["revision"] += 1
        self.record["state"] = "released"
        for child in tuple(self.mount_path.iterdir()):
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        for child in self.snapshot_path.iterdir():
            target = self.mount_path / child.name
            if child.is_dir():
                shutil.copytree(child, target, copy_function=os.link)
            else:
                os.link(child, target)

    def ensure_mounted(self, owner, *, operation_id):
        del operation_id
        self._require_owner(owner)
        if self.record["state"] == "sealed":
            self._release()
        if self.record["state"] == "released":
            self._mount()
        return self._typed()

    def ensure_released(self, owner, *, operation_id):
        del operation_id
        self._require_owner(owner)
        if self.record["state"] == "mounted":
            self._seal()
        if self.record["state"] == "sealed":
            self._release()
        return self._typed()

    def discard_resume(self, owner, *, operation_id):
        del operation_id
        self._require_owner(owner)
        if self.record["state"] == "mounted":
            self._discard()
        return self._typed()


class FakeRootfsLifecycle:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.fail_next_park = False
        self.fail_next_resume = False

    def park_sandbox(self, _sandbox) -> None:
        if self.fail_next_park:
            self.fail_next_park = False
            raise OSError("injected rootfs park failure")
        self.events.append("rootfs-park")

    def resume_sandbox(self, _sandbox) -> None:
        if self.fail_next_resume:
            self.fail_next_resume = False
            raise OSError("injected rootfs resume failure")
        self.events.append("rootfs-resume")


class DirectRunscWardenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.proc_root = self.root / "proc"
        self.proc_root.mkdir()
        self.bundle_root = self.root / "bundles"
        self.bundle = self.bundle_root / "sandbox"
        self.bundle.mkdir(parents=True)
        self.bundle_root.chmod(0o700)
        self.bundle.chmod(0o700)
        self.memory_directory = "sandbox-1.sandbox-1"
        (self.bundle / "config.json").write_text(
            json.dumps(
                {
                    "annotations": {
                        "dev.gvisor.internal.application-memory-directory": self.memory_directory
                    }
                }
            ),
            encoding="utf-8",
        )
        self.runtime = HibernationRuntimeFingerprint(
            runsc_sha256=DIGEST_A,
            runsc_commit="e" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256=DIGEST_B,
            boot_config_sha256=DIGEST_C,
            rootfs_sha256=DIGEST_A,
        )
        self.config = DirectRunscWardenConfig(
            runsc=(self.root / "runsc").resolve(),
            runtime_root=(self.root / "run").resolve(),
            memory_root=(self.root / "memory").resolve(),
            bundle_root=self.bundle_root.resolve(),
            journal_root=(self.root / "journals").resolve(),
            runtime_fingerprint=self.runtime,
            proc_root=self.proc_root.resolve(),
        )
        self.sandbox = DirectSandbox(
            sandbox_id="sandbox-1",
            sandbox_generation=1,
            container_id=CONTAINER_ID,
            spec_sha256=DIGEST_B,
            rootfs_sha256=DIGEST_A,
            bundle=self.bundle.resolve(),
            memory_directory=self.memory_directory,
        )
        self.runner = FakeRunsc(
            self.proc_root,
            self.config.memory_root,
            self.memory_directory,
        )
        self.fencer = FakeFencer(self.proc_root)
        self.storage = FakeStorage(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            volume_id=self.memory_directory,
            mount_path=self.config.memory_root / self.memory_directory,
        )
        self.rootfs = FakeRootfsLifecycle(self.storage.events)
        self.warden = DirectRunscWarden(
            self.config,
            runner=self.runner,
            fencer=self.fencer,
            storage=self.storage,
            rootfs_lifecycle=self.rootfs,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _use_storage_native(
        self,
    ) -> tuple[FakeStorage, FakeRootfsLifecycle, Path]:
        incarnation = self.config.memory_root / self.memory_directory
        return self.storage, self.rootfs, incarnation

    def _artifact_inventory(self) -> tuple[HibernationGenerationInventory, ...]:
        return self.warden.artifacts.inventory_incarnation(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            ignored_entries=(
                "upper",
                "work",
                "application_memory.img",
                "application_memory.active",
            ),
        )

    def test_two_phase_park_and_new_backend_resume(self) -> None:
        running = self.warden.create(self.sandbox, operation_id="create:1")
        self.assertEqual(running.state, HibernationState.RUNNING)

        parked = self.warden.park(self.sandbox, operation_id="park:1")
        self.assertEqual(parked.state, HibernationState.PARKED)
        generation = self.warden.artifacts.generation_path(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=1,
        )
        self.assertTrue((generation / "COMPLETE").is_file())
        self.assertFalse((self.proc_root / str(running.sentry_pid)).exists())
        verbs = [
            command[-2] if command[-1] == CONTAINER_ID else command[-1]
            for command in self.runner.commands
        ]
        self.assertIn(
            "checkpoint", " ".join(" ".join(item) for item in self.runner.commands)
        )
        self.assertNotIn("resume", verbs)

        restored = self.warden.resume(self.sandbox, operation_id="wake:1")
        self.assertEqual(restored.state, HibernationState.RUNNING)
        self.assertNotEqual(restored.sentry_pid, running.sentry_pid)
        self.assertEqual(self._artifact_inventory(), ())
        self.assertFalse(
            (
                self.config.memory_root
                / f"{self.sandbox.sandbox_id}.sandbox-1"
                / "hibernate-1"
            ).exists()
        )
        self.assertTrue(
            (
                self.config.memory_root
                / self.memory_directory
                / "application_memory.active"
            ).is_file()
        )

    def test_managed_process_ledger_is_verified_before_restore(self) -> None:
        managed_directory = self.bundle / "rootfs" / ".ucloud-managed"
        managed_directory.mkdir(parents=True)
        ledger = managed_directory / "state.json"
        ledger.write_text(
            json.dumps(
                {
                    "version": 1,
                    "job_id": "rollout-1",
                    "spec_sha256": DIGEST_A,
                    "state": "running",
                    "sequence": 2,
                }
            ),
            encoding="utf-8",
        )
        (self.bundle / "config.json").write_text(
            json.dumps(
                {
                    "annotations": {
                        "dev.gvisor.internal.application-memory-directory": (
                            self.memory_directory
                        ),
                        "dev.ucloud-sandboxes.managed-process": "v1",
                    }
                }
            ),
            encoding="utf-8",
        )
        self.warden.create(self.sandbox, operation_id="create:managed")
        self.warden.park(self.sandbox, operation_id="park:managed")
        restore_count = sum("restore" in command for command in self.runner.commands)

        ledger.write_text(
            ledger.read_text(encoding="utf-8").replace(
                '"sequence": 2', '"sequence": 3'
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(DirectWardenError, "ledger does not match"):
            self.warden.resume(self.sandbox, operation_id="wake:managed")
        self.assertEqual(
            sum("restore" in command for command in self.runner.commands),
            restore_count,
        )
        self.assertEqual(
            self.warden.inspect(self.sandbox).state,
            HibernationState.PARKED,
        )

    def test_storage_native_park_releases_and_resume_mounts_new_cow(self) -> None:
        storage, _rootfs, incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")

        parked = self.warden.park(self.sandbox, operation_id="park:1")

        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(
            storage.events,
            ["rootfs-park", "seal", "release"],
        )
        self.assertEqual(storage.record["state"], "released")
        portable = self.warden.load_parked_manifest(self.sandbox)
        self.assertEqual(portable.metadata_sha256, parked.manifest_sha256)

        running = self.warden.resume(self.sandbox, operation_id="wake:1")

        self.assertEqual(running.state, HibernationState.RUNNING)
        self.assertEqual(
            storage.events,
            [
                "rootfs-park",
                "seal",
                "release",
                "mount",
                "rootfs-resume",
            ],
        )
        self.assertEqual(storage.record["state"], "mounted")
        self.assertTrue(incarnation.is_dir())

    def test_storage_inventory_snapshot_uses_one_bulk_rpc_and_validates_owner(
        self,
    ) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()

        snapshot = self.warden.storage_records_snapshot((self.sandbox,))

        self.assertEqual(storage.list_calls, 1)
        self.assertEqual(snapshot[self.sandbox.memory_directory], storage._typed())

        storage.list_volumes = lambda: (storage._typed(), storage._typed())
        with self.assertRaisesRegex(DirectWardenError, "duplicate volume"):
            self.warden.storage_records_snapshot((self.sandbox,))

    def test_storage_native_delete_does_not_remount_or_traverse_volume(self) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        events_before_delete = tuple(storage.events)

        self.warden.delete(self.sandbox)

        self.assertEqual(tuple(storage.events), events_before_delete)
        self.assertEqual(storage.record["state"], "released")
        self.assertIsNone(self.warden._journal(self.sandbox).load())

    def test_storage_native_reconcile_finishes_failed_seal(self) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        storage.fail_next_seal = True

        with self.assertRaisesRegex(OSError, "seal failure"):
            self.warden.park(self.sandbox, operation_id="park:1")

        interrupted = self.warden.inspect(self.sandbox)
        self.assertIsNotNone(interrupted)
        self.assertEqual(interrupted.state, HibernationState.HIBERNATING)
        self.assertEqual(interrupted.authority, HibernationAuthority.PENDING)
        self.assertEqual(storage.record["state"], "mounted")
        self.assertEqual(self.runner.status, "absent")

        parked = self.warden.reconcile(self.sandbox)

        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(storage.record["state"], "released")
        self.assertEqual(
            storage.events,
            [
                "rootfs-park",
                "rootfs-resume",
                "rootfs-park",
                "seal",
                "release",
            ],
        )

    def test_storage_native_reconcile_finishes_failed_rootfs_detach(self) -> None:
        storage, rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        rootfs.fail_next_park = True

        with self.assertRaisesRegex(OSError, "rootfs park failure"):
            self.warden.park(self.sandbox, operation_id="park:1")

        parked = self.warden.reconcile(self.sandbox)

        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(storage.record["state"], "released")
        self.assertEqual(
            storage.events,
            ["rootfs-resume", "rootfs-park", "seal", "release"],
        )

    def test_storage_native_reconcile_finishes_failed_release(self) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        storage.fail_next_release = True

        with self.assertRaisesRegex(OSError, "release failure"):
            self.warden.park(self.sandbox, operation_id="park:1")

        self.assertEqual(storage.record["state"], "sealed")
        parked = self.warden.reconcile(self.sandbox)

        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(storage.record["state"], "released")
        self.assertEqual(
            storage.events,
            [
                "rootfs-park",
                "seal",
                "release",
                "mount",
                "rootfs-resume",
                "rootfs-park",
                "seal",
                "release",
            ],
        )

    def test_storage_native_resume_retries_failed_acquire(self) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        storage.fail_next_mount = True

        with self.assertRaisesRegex(OSError, "mount failure"):
            self.warden.resume(self.sandbox, operation_id="wake:1")

        parked = self.warden.inspect(self.sandbox)
        self.assertIsNotNone(parked)
        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(storage.record["state"], "released")
        running = self.warden.resume(self.sandbox, operation_id="wake:2")
        self.assertEqual(running.state, HibernationState.RUNNING)

    def test_storage_native_resume_retries_failed_rootfs_remount(self) -> None:
        storage, rootfs, _incarnation = self._use_storage_native()
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        rootfs.fail_next_resume = True

        with self.assertRaisesRegex(OSError, "rootfs resume failure"):
            self.warden.resume(self.sandbox, operation_id="wake:1")

        parked = self.warden.inspect(self.sandbox)
        self.assertIsNotNone(parked)
        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertEqual(storage.record["state"], "released")
        self.assertEqual(storage.events[-2:], ["rootfs-park", "discard"])
        running = self.warden.resume(self.sandbox, operation_id="wake:2")
        self.assertEqual(running.state, HibernationState.RUNNING)

    def test_storage_native_validation_failure_discards_mounted_restore_cow(
        self,
    ) -> None:
        storage, _rootfs, _incarnation = self._use_storage_native()
        self.assertFalse(self.warden.artifacts.require_stable_device)
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")

        with patch.object(
            self.warden.artifacts,
            "load_complete",
            side_effect=HibernationValidationError("injected identity failure"),
        ):
            with self.assertRaisesRegex(
                HibernationValidationError,
                "identity failure",
            ):
                self.warden.resume(self.sandbox, operation_id="wake:1")

        self.assertEqual(
            self.warden.inspect(self.sandbox).state, HibernationState.PARKED
        )
        self.assertEqual(storage.record["state"], "released")
        self.assertEqual(
            storage.events[-4:],
            ["mount", "rootfs-resume", "rootfs-park", "discard"],
        )

    def test_restore_uses_canonical_paused_background_handoff(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.warden.resume(self.sandbox, operation_id="wake:1")

        restore = next(
            command for command in self.runner.commands if "restore" in command
        )
        for flag in ("--background", "--cpu-startup-burst", "--start-paused"):
            self.assertIn(flag, restore)

    def test_connected_sockets_are_preserved_across_checkpoint_restore(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.warden.resume(self.sandbox, operation_id="wake:1")

        lifecycle_commands = [
            command
            for command in self.runner.commands
            if any(verb in command for verb in ("create", "checkpoint", "restore"))
        ]
        self.assertTrue(lifecycle_commands)
        for command in lifecycle_commands:
            self.assertIn("--allow-connected-on-save=true", command)

    def test_adopts_migrated_parked_generation_without_create(self) -> None:
        generation = self.warden.artifacts.prepare_generation(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=4,
        )
        payloads = {
            HibernationFileRole.MAIN_MEMORY: "application_memory.img",
            HibernationFileRole.KERNEL_STATE: "checkpoint.img",
            HibernationFileRole.ALLOCATOR_METADATA: "pages_meta.img",
        }
        for role, name in payloads.items():
            (generation / name).write_bytes(role.value.encode("ascii"))
        manifest = HibernationManifest(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=4,
            operation_id="park:migrated",
            spec_sha256=self.sandbox.spec_sha256,
            container_id=self.sandbox.container_id,
            created_ns=1,
            runtime=self.runtime,
            files=tuple(
                LocalHibernationArtifactFile.from_path(
                    generation / name,
                    role=role,
                )
                for role, name in payloads.items()
            ),
            managed_process_sha256=self.warden._managed_process_ledger_digest(
                self.sandbox
            ),
        )
        manifest = self.warden.artifacts.publish_complete(manifest)

        parked = self.warden.adopt_parked(self.sandbox, manifest)

        self.assertEqual(parked.state, HibernationState.PARKED)
        self.assertFalse(any("create" in command for command in self.runner.commands))
        active_root = self.config.memory_root / self.memory_directory
        active_root.mkdir(parents=True, exist_ok=True)
        restored = self.warden.resume(self.sandbox, operation_id="wake:migrated")
        self.assertEqual(restored.state, HibernationState.RUNNING)

    def test_streaming_exec_lease_builds_flags_under_lifecycle_fence(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")

        with self.warden.exec_lease(
            self.sandbox,
            ("/bin/sh", "-lc", "pwd"),
            env={"TERM": "xterm"},
            working_dir="/workspace",
            user="1000:1001",
        ) as command:
            self.assertEqual(
                command[-7:],
                (
                    "--cwd=/workspace",
                    "--user=1000:1001",
                    "--env=TERM=xterm",
                    CONTAINER_ID,
                    "/bin/sh",
                    "-lc",
                    "pwd",
                ),
            )

    def test_cleanup_failure_does_not_turn_live_restore_into_failed_wake(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        with patch.object(
            self.warden,
            "_finalize_restore_artifacts",
            side_effect=OSError("injected cleanup failure"),
        ):
            restored = self.warden.resume(self.sandbox, operation_id="wake:1")

        self.assertEqual(restored.state, HibernationState.RUNNING)
        self.assertEqual(self.runner.status, "running")
        self.assertEqual(len(self._artifact_inventory()), 1)

        reconciled = self.warden.reconcile(self.sandbox)

        self.assertEqual(reconciled.state, HibernationState.RUNNING)
        self.assertEqual(self._artifact_inventory(), ())

    def test_failed_publication_resumes_live_backend(self) -> None:
        running = self.warden.create(self.sandbox, operation_id="create:1")
        with patch.object(
            self.warden.artifacts,
            "publish_complete",
            side_effect=OSError("injected fsync failure"),
        ):
            with self.assertRaisesRegex(OSError, "injected"):
                self.warden.park(self.sandbox, operation_id="park:1")
        record = self.warden._journal(self.sandbox).load()
        self.assertIsNotNone(record)
        self.assertEqual(record.state, HibernationState.RUNNING)
        self.assertEqual(record.sentry_pid, running.sentry_pid)
        self.assertEqual(self.runner.status, "running")
        self.assertEqual(self._artifact_inventory(), ())

    def test_complete_capture_is_never_resumed_after_stop_failure(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.fencer.fail_next_terminate = True
        with self.assertRaisesRegex(DirectWardenError, "terminate failure"):
            self.warden.park(self.sandbox, operation_id="park:1")
        record = self.warden._journal(self.sandbox).load()
        self.assertIsNotNone(record)
        self.assertEqual(record.state, HibernationState.HIBERNATING)
        self.assertEqual(
            classify_hibernation_recovery(
                record,
                sentry_alive=True,
                candidate_alive=False,
                complete_manifest=True,
            ),
            HibernationRecoveryAction.FINISH_PUBLISHED_GENERATION,
        )
        self.assertFalse(any("resume" in command for command in self.runner.commands))

        reconciled = self.warden.reconcile(self.sandbox)
        self.assertEqual(reconciled.state, HibernationState.PARKED)
        self.assertFalse((self.proc_root / str(record.sentry_pid) / "stat").exists())

    def test_reconcile_resumes_unpublished_live_capture(self) -> None:
        running = self.warden.create(self.sandbox, operation_id="create:1")
        journal = self.warden._journal(self.sandbox)
        hibernating = journal.begin_hibernate(
            operation_id="park:1",
            expected_revision=running.revision,
        )
        generation = self.warden.artifacts.prepare_generation(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=hibernating.hibernation_generation,
        )
        self.warden._checked(
            *self.warden._common(),
            "checkpoint",
            "--hibernate",
            f"--image-path={generation}",
            self.sandbox.container_id,
        )

        reconciled = self.warden.reconcile(self.sandbox)

        self.assertEqual(reconciled.state, HibernationState.RUNNING)
        self.assertEqual(self.runner.status, "running")
        self.assertFalse(generation.exists())

    def test_single_owner_restore_stays_paused_until_candidate_is_fenced(
        self,
    ) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        parked = self.warden.park(self.sandbox, operation_id="park:1")
        generation = self.warden.artifacts.generation_path(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=parked.hibernation_generation,
        )

        def assert_paused_single_owner_handoff() -> None:
            record = self.warden._journal(self.sandbox).load()
            self.assertIsNotNone(record)
            self.assertEqual(record.state, HibernationState.RESTORING)
            self.assertEqual(record.authority.value, "candidate")
            self.assertEqual(self.runner.status, "paused")
            self.assertTrue((generation / "COMPLETE").is_file())
            self.assertFalse((generation / "application_memory.img").exists())
            self.assertTrue(
                (
                    self.config.memory_root
                    / self.memory_directory
                    / "application_memory.active"
                ).is_file()
            )

        self.runner.before_resume = assert_paused_single_owner_handoff

        self.warden.resume(self.sandbox, operation_id="wake:1")

        restore = next(
            command for command in self.runner.commands if "restore" in command
        )
        self.assertIn("--start-paused", restore)
        self.assertFalse(generation.exists())
        self.assertEqual(self.runner.status, "running")

    def test_single_owner_readiness_failure_returns_memory_to_checkpoint(
        self,
    ) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        parked = self.warden.park(self.sandbox, operation_id="park:1")
        self.runner.fail_readiness = True

        with self.assertRaisesRegex(DirectWardenError, "readiness failure"):
            self.warden.resume(self.sandbox, operation_id="wake:1")

        record = self.warden._journal(self.sandbox).load()
        self.assertIsNotNone(record)
        self.assertEqual(record.state, HibernationState.PARKED)
        generation = self.warden.artifacts.generation_path(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=parked.hibernation_generation,
        )
        self.assertTrue((generation / "application_memory.img").is_file())
        self.assertTrue((generation / "COMPLETE").is_file())
        self.assertEqual(self.runner.status, "absent")
        self.assertEqual(self.storage.record["state"], "released")
        self.assertEqual(self.storage.events[-2:], ["rootfs-park", "discard"])

    def test_reconcile_discards_cow_before_settling_dead_restore(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        parked = self.warden.park(self.sandbox, operation_id="park:1")
        generation = self.warden.artifacts.generation_path(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=parked.hibernation_generation,
        )

        for attempt, journal_candidate in enumerate((False, True), start=1):
            with self.subTest(journal_candidate=journal_candidate):
                self.warden._mount_storage(
                    self.sandbox,
                    operation_id=f"wake:{attempt}:storage-mount",
                )
                self.rootfs.resume_sandbox(self.sandbox)
                journal = self.warden._journal(self.sandbox)
                restoring = journal.begin_restore(
                    operation_id=f"wake:{attempt}",
                    expected_revision=journal.load().revision,
                )
                self.warden._checked(
                    *self.warden._common(),
                    "restore",
                    "--detach",
                    "--background",
                    "--cpu-startup-burst",
                    "--start-paused",
                    f"--image-path={generation}",
                    f"--bundle={self.sandbox.bundle}",
                    self.sandbox.container_id,
                )
                pid, ticks = self.warden._state_identity(self.sandbox)
                if journal_candidate:
                    restoring = journal.mark_candidate_started(
                        operation_id=restoring.operation_id,
                        expected_revision=restoring.revision,
                        candidate_pid=pid,
                        candidate_start_time_ticks=ticks,
                    )
                process = self.proc_root / str(pid)
                (process / "stat").unlink()
                process.rmdir()
                self.runner.status = "absent"

                original_rollback = self.warden._rollback_parked_storage_mount

                def assert_discard_before_journal(*args, **kwargs):
                    current = self.warden._journal(self.sandbox).load()
                    self.assertEqual(current.state, HibernationState.RESTORING)
                    return original_rollback(*args, **kwargs)

                with patch.object(
                    self.warden,
                    "_rollback_parked_storage_mount",
                    side_effect=assert_discard_before_journal,
                ):
                    reconciled = self.warden.reconcile(self.sandbox)

                self.assertEqual(reconciled.state, HibernationState.PARKED)
                self.assertEqual(self.storage.record["state"], "released")
                self.assertEqual(self.storage.events[-2:], ["rootfs-park", "discard"])
                self.assertTrue((generation / "application_memory.img").is_file())
                self.assertTrue(
                    any("list" in command for command in self.runner.commands)
                )

    def test_restore_command_failure_fences_daemonized_candidate(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.runner.fail_restore_after_start = True

        with self.assertRaisesRegex(DirectWardenError, "restore parent failure"):
            self.warden.resume(self.sandbox, operation_id="wake:1")

        self.assertEqual(
            self.warden.inspect(self.sandbox).state,
            HibernationState.PARKED,
        )
        self.assertEqual(self.runner.status, "absent")
        self.assertEqual(self.storage.record["state"], "released")
        self.assertEqual(self.storage.events[-2:], ["rootfs-park", "discard"])

    def test_state_inspection_failure_does_not_authorize_cow_discard(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.runner.fail_restore_after_start = True
        self.runner.fail_state_inspection = True

        with self.assertRaisesRegex(DirectWardenError, "candidate is fenced"):
            self.warden.resume(self.sandbox, operation_id="wake:1")

        self.runner.fail_state_inspection = False
        record = self.warden.inspect(self.sandbox)
        self.assertEqual(record.state, HibernationState.RESTORING)
        self.assertEqual(self.storage.record["state"], "mounted")
        self.assertNotIn("discard", self.storage.events)
        self.assertEqual(self.runner.status, "paused")
        self.assertTrue(any("list" in command for command in self.runner.commands))

    def test_delete_reconciles_interrupted_hibernation(self) -> None:
        running = self.warden.create(self.sandbox, operation_id="create:1")
        journal = self.warden._journal(self.sandbox)
        hibernating = journal.begin_hibernate(
            operation_id="park:1",
            expected_revision=running.revision,
        )
        self.warden.artifacts.prepare_generation(
            sandbox_id=self.sandbox.sandbox_id,
            sandbox_generation=self.sandbox.sandbox_generation,
            hibernation_generation=hibernating.hibernation_generation,
        )

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())
        self.assertEqual(self.runner.status, "absent")

    def test_delete_replay_recovers_a_previously_reaped_sentry(self) -> None:
        created = self.warden.create(self.sandbox, operation_id="create:1")
        assert created.sentry_pid is not None
        process = self.proc_root / str(created.sentry_pid)
        (process / "stat").unlink()
        process.rmdir()
        self.runner.status = "absent"

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())
        control_copy = self.warden._parked_manifest_path(self.sandbox)
        control_copy.parent.mkdir(parents=True, exist_ok=True)
        control_copy.write_bytes(b"stale")

        self.warden.delete(self.sandbox)

        self.assertFalse(control_copy.exists())


if __name__ == "__main__":
    unittest.main()
