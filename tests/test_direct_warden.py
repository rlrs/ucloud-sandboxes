import json
from dataclasses import replace
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
    HibernationRecoveryAction,
    HibernationRuntimeFingerprint,
    HibernationState,
    classify_hibernation_recovery,
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
            return CommandResult(
                command,
                0,
                json.dumps({"pid": self.pid, "status": self.status}),
            )
        elif verb == "checkpoint":
            image = Path(
                next(
                    item.split("=", 1)[1]
                    for item in command
                    if item.startswith("--image-path=")
                )
            )
            self.checkpoint = image
            active = self.memory_root / self.memory_directory / "application_memory.img"
            active.write_bytes(b"memory")
            active.replace(image / active.name)
            (image / "checkpoint.img").write_bytes(b"kernel")
            (image / "pages_meta.img").write_bytes(b"metadata")
            (image / "pages.img").write_bytes(b"private")
            self.status = "paused"
        elif verb == "resume":
            assert self.checkpoint is not None
            captured = self.checkpoint / "application_memory.img"
            active = self.memory_root / self.memory_directory / captured.name
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
            active = self.memory_root / self.memory_directory / captured.name
            captured.replace(active)
            self._start_process()
        elif verb == "exec" and self.fail_readiness:
            return CommandResult(command, 1, stderr="injected readiness failure")
        elif verb == "delete":
            active = self.memory_root / self.memory_directory / "application_memory.img"
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


class DirectRunscWardenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.proc_root = self.root / "proc"
        self.proc_root.mkdir()
        self.bundle_root = self.root / "bundles"
        self.bundle = self.bundle_root / "sandbox"
        self.bundle.mkdir(parents=True)
        self.memory_directory = "sandbox-memory"
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
            artifact_root=(self.root / "artifacts").resolve(),
            runtime_fingerprint=self.runtime,
            proc_root=self.proc_root.resolve(),
        )
        self.runner = FakeRunsc(
            self.proc_root,
            self.config.memory_root,
            self.memory_directory,
        )
        self.fencer = FakeFencer(self.proc_root)
        self.warden = DirectRunscWarden(
            self.config,
            runner=self.runner,
            fencer=self.fencer,
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

    def tearDown(self) -> None:
        self.temporary.cleanup()

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
        self.assertEqual(self.warden.artifacts.inventory(), ())
        self.assertFalse(
            (
                self.config.artifact_root
                / f"{self.sandbox.sandbox_id}.sandbox-1"
            ).exists()
        )
        self.assertTrue(
            (
                self.config.memory_root
                / self.memory_directory
                / "application_memory.img"
            ).is_file()
        )

    def test_restore_cpu_startup_burst_is_explicit(self) -> None:
        self.warden = DirectRunscWarden(
            replace(self.config, restore_cpu_startup_burst=True),
            runner=self.runner,
            fencer=self.fencer,
        )
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.warden.resume(self.sandbox, operation_id="wake:1")

        restore = next(
            command for command in self.runner.commands if "restore" in command
        )
        self.assertIn("--cpu-startup-burst", restore)

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
            self.warden.artifacts,
            "delete_published",
            side_effect=OSError("injected cleanup failure"),
        ):
            restored = self.warden.resume(self.sandbox, operation_id="wake:1")

        self.assertEqual(restored.state, HibernationState.RUNNING)
        self.assertEqual(self.runner.status, "running")
        self.warden.park(self.sandbox, operation_id="park:2")
        self.warden.delete(self.sandbox)
        self.assertEqual(self.warden.artifacts.inventory(), ())

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
        self.assertEqual(self.warden.artifacts.inventory(), ())

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

    def test_failed_restore_reaps_candidate_and_returns_memory_to_generation(
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

    def test_delete_removes_running_backend_artifacts_and_journal(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")
        self.warden.resume(self.sandbox, operation_id="wake:1")

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())
        self.assertEqual(self.warden.artifacts.inventory(), ())
        self.assertFalse((self.config.memory_root / self.memory_directory).exists())

    def test_delete_replay_recovers_a_previously_reaped_sentry(self) -> None:
        created = self.warden.create(self.sandbox, operation_id="create:1")
        assert created.sentry_pid is not None
        process = self.proc_root / str(created.sentry_pid)
        (process / "stat").unlink()
        process.rmdir()
        self.runner.status = "absent"

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())

    def test_delete_removes_parked_backend_artifacts_and_journal(self) -> None:
        self.warden.create(self.sandbox, operation_id="create:1")
        self.warden.park(self.sandbox, operation_id="park:1")

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())
        self.assertEqual(self.warden.artifacts.inventory(), ())

    def test_unified_storage_delete_ignores_owned_runtime_layout(self) -> None:
        self.config = replace(
            self.config,
            memory_root=self.config.artifact_root,
            remove_memory_directory_on_delete=False,
        )
        incarnation = (
            self.config.memory_root
            / f"{self.sandbox.sandbox_id}.sandbox-{self.sandbox.sandbox_generation}"
        )
        incarnation.mkdir(parents=True)
        (incarnation / "upper").mkdir(mode=0o700)
        (incarnation / "work").mkdir(mode=0o700)
        active_memory = incarnation / "application_memory.active"
        active_memory.write_bytes(b"active")
        active_memory.chmod(0o600)
        quota_lock = self.config.artifact_root / ".quota.lock"
        quota_lock.write_bytes(b"")
        quota_lock.chmod(0o600)
        self.sandbox = replace(
            self.sandbox,
            memory_directory=incarnation.name,
        )
        (self.bundle / "config.json").write_text(
            json.dumps(
                {
                    "annotations": {
                        "dev.gvisor.internal.application-memory-directory": (
                            self.sandbox.memory_directory
                        )
                    }
                }
            ),
            encoding="utf-8",
        )
        self.runner = FakeRunsc(
            self.proc_root,
            self.config.memory_root,
            self.sandbox.memory_directory,
        )
        self.warden = DirectRunscWarden(
            self.config,
            runner=self.runner,
            fencer=self.fencer,
        )
        self.warden.create(self.sandbox, operation_id="create:1")

        self.warden.delete(self.sandbox)

        self.assertIsNone(self.warden._journal(self.sandbox).load())


if __name__ == "__main__":
    unittest.main()
