from pathlib import Path
from dataclasses import replace
from datetime import timedelta
from tempfile import TemporaryDirectory
from threading import Barrier, Event, Lock, Thread
import hashlib
import json
import multiprocessing
import sys
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.sandbox import (
    CommandResult,
    DEFAULT_FORK_RESTORE_PARALLELISM,
    DockerGvisorRuntime,
    FORK_CHILD_SETUP_ALLOWANCE_SECONDS,
    FORK_REQUEST_TIMEOUT_SECONDS,
    MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS,
    MAX_FORK_FANOUT,
    MAX_FORK_PROTOCOL_TIMEOUT_SECONDS,
    MAX_FORK_RESTORE_PARALLELISM,
    MAX_FORK_RESTORE_TIMEOUT_SECONDS,
    RecordingExecutor,
    SandboxFileTooLargeError,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxAdmissionClosedError,
    SandboxFilesystemSpec,
    SandboxForkProtocolSpec,
    SandboxForkRuntimeResult,
    SandboxBusyError,
    SandboxManager,
    SandboxOperation,
    SandboxSecuritySpec,
    SandboxSshSpec,
    SandboxSpec,
    SandboxStore,
    SandboxStaleOperationError,
    SandboxForkCommandTimeoutError,
    SANDBOX_GENERATION_LABEL,
    SANDBOX_OPERATION_ID_LABEL,
    SANDBOX_SPEC_HASH_LABEL,
    application_checkpoint_id,
    sandbox_spec_fingerprint,
    sandbox_fork_target,
)

FORK_PROTOCOL = SandboxForkProtocolSpec(
    version="agent-v1",
    prepare_command=("/usr/local/bin/ucloud-fork-agent", "prepare"),
    ready_command=("/usr/local/bin/ucloud-fork-agent", "ready"),
)
FORK_NONCE = "a" * 64


class ForkCaptureFailureExecutor:
    def __init__(
        self,
        source: SandboxSpec,
        source_hash: str,
        *,
        timeout: bool,
        ambiguous_exit_code: int = 124,
    ) -> None:
        self.source = source
        self.source_hash = source_hash
        self.timeout = timeout
        self.ambiguous_exit_code = ambiguous_exit_code
        self.commands: list[tuple[str, ...]] = []

    def _result(self, argv: tuple[str, ...]) -> CommandResult:
        if argv[:2] == ("docker", "inspect"):
            template = argv[3]
            if template == "{{json .Config.Labels}}":
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=json.dumps(
                        {
                            "ucloud-sandboxes.managed": "true",
                            "ucloud-sandboxes.sandbox-id": self.source.id,
                            SANDBOX_GENERATION_LABEL: "3",
                            SANDBOX_OPERATION_ID_LABEL: "source-create-3",
                            SANDBOX_SPEC_HASH_LABEL: self.source_hash,
                        }
                    ),
                )
            if template == "{{.State.Running}}":
                return CommandResult(argv=argv, exit_code=0, stdout="true")
            if template == "{{.Id}} {{.Image}}":
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=("1" * 64 + " sha256:" + "2" * 64),
                )
        if "/usr/local/libexec/ucloud-sandbox-checkpoint" in argv:
            helper_index = argv.index("/usr/local/libexec/ucloud-sandbox-checkpoint")
            if argv[helper_index + 1] == "status":
                return CommandResult(argv=argv, exit_code=4)
            return CommandResult(argv=argv, exit_code=0)
        if argv[:2] == ("docker", "exec"):
            role = argv[-1]
            output = (
                f"UCLOUD_FORK_PREPARED={FORK_NONCE}\n"
                if role == "prepare"
                else f"UCLOUD_FORK_READY={FORK_NONCE}:{role}\n"
            )
            return CommandResult(argv=argv, exit_code=0, stdout=output)
        if argv[:3] == ("docker", "checkpoint", "create"):
            return CommandResult(argv=argv, exit_code=1, stderr="save rejected")
        return CommandResult(argv=argv, exit_code=0)

    def run(
        self,
        argv: tuple[str, ...],
        *,
        input: bytes | None = None,
    ) -> CommandResult:
        del input
        self.commands.append(argv)
        return self._result(argv)

    def run_with_timeout(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        input: bytes | None = None,
    ) -> CommandResult:
        del timeout_seconds, input
        self.commands.append(argv)
        if self.timeout and argv[:3] == ("docker", "checkpoint", "create"):
            return CommandResult(
                argv=argv,
                exit_code=self.ambiguous_exit_code,
                stderr="client did not complete",
            )
        return self._result(argv)


class SandboxRuntimeTests(unittest.TestCase):
    def test_forkable_create_and_delete_manage_private_application_path(self) -> None:
        class LifecycleExecutor:
            def __init__(self) -> None:
                self.commands: list[tuple[str, ...]] = []

            def run(self, argv, *, input=None):
                del input
                self.commands.append(argv)
                if argv[:2] == ("docker", "inspect"):
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr="No such container",
                    )
                return CommandResult(argv=argv, exit_code=0)

        with TemporaryDirectory() as raw_dir:
            executor = LifecycleExecutor()
            checkpoint_root = Path(raw_dir) / "ucloud-checkpoints"
            runtime = DockerGvisorRuntime(
                executor=executor,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=checkpoint_root,
                checkpoint_helper="/checkpoint-helper",
                checkpoint_helper_sudo=False,
            )
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                runtime,
            )
            spec = SandboxSpec(
                id="forkable",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )

            record, _result = manager.create(spec)
            manager.delete(spec.id)

        application_id = application_checkpoint_id(
            spec.id,
            record.generation,
            record.spec_hash,
        )
        app_prepare = ("/checkpoint-helper", "app-prepare", application_id)
        app_drop = ("/checkpoint-helper", "app-drop", application_id)
        self.assertIn(app_prepare, executor.commands)
        self.assertIn(app_drop, executor.commands)
        create = next(
            command for command in executor.commands if command[:2] == ("docker", "run")
        )
        self.assertIn(
            "dev.gvisor.internal.checkpoint.path="
            f"{checkpoint_root}/application/{application_id}",
            create,
        )
        self.assertLess(
            executor.commands.index(app_prepare), executor.commands.index(create)
        )
        remove = next(
            command for command in executor.commands if command[:2] == ("docker", "rm")
        )
        self.assertLess(
            executor.commands.index(remove), executor.commands.index(app_drop)
        )

    def test_fork_ready_hook_must_acknowledge_exact_nonce_and_role(self) -> None:
        spec = SandboxSpec(
            id="child",
            image="busybox",
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        expected = f"UCLOUD_FORK_READY={FORK_NONCE}:restore\n"
        runtime = DockerGvisorRuntime(
            executor=RecordingExecutor(stdout=expected),
        )

        result = runtime.wait_fork_ready(
            spec,
            checkpoint_id="fork-artifact-1",
            fork_nonce=FORK_NONCE,
        )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.argv[-3:], ("fork-artifact-1", FORK_NONCE, "restore"))

        rejected = DockerGvisorRuntime(
            executor=RecordingExecutor(stdout="UCLOUD_FORK_READY=stale:restore\n"),
        )
        with self.assertRaisesRegex(RuntimeError, "no nonce acknowledgment"):
            rejected.wait_fork_ready(
                spec,
                checkpoint_id="fork-artifact-1",
                fork_nonce=FORK_NONCE,
            )

        overflowed = DockerGvisorRuntime(
            executor=RecordingExecutor(exit_code=125),
        )
        with self.assertRaises(SandboxForkCommandTimeoutError):
            overflowed.wait_fork_ready(
                spec,
                checkpoint_id="fork-artifact-1",
                fork_nonce=FORK_NONCE,
            )

    def test_live_fork_builds_checkpoint_reflink_restore_sequence(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox:latest",
            command=("sh", "-c", "while :; do sleep 1; done"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(
            source,
            {"id": "child", "env": {"AGENT_BRANCH": "child"}},
        )
        operation = _create_operation(target, 4, "fork-child-4")
        runtime = DockerGvisorRuntime(
            dry_run=True,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        result = runtime.fork(
            source,
            target,
            operation,
            source_generation=3,
            source_spec_hash=sandbox_spec_fingerprint(source),
            checkpoint_id="fork-artifact-1",
            fork_nonce=FORK_NONCE,
        )

        commands = result.commands
        self.assertTrue(
            any(command[1:3] == ("checkpoint", "create") for command in commands)
        )
        create = next(
            command for command in commands if command[:2] == ("docker", "create")
        )
        self.assertIn("sha256:" + "0" * 64, create)
        self.assertIn("UCLOUD_SANDBOX_FORK_PARENT=parent", create)
        self.assertIn("UCLOUD_SANDBOX_ID=child", create)
        self.assertIn(f"UCLOUD_SANDBOX_FORK_NONCE={FORK_NONCE}", create)
        self.assertEqual(create[create.index("--runtime") + 1], "runsc-restore")
        self.assertIn(
            "dev.ucloud.sandboxes.restore.checkpoint=state",
            create,
        )
        helper_prepare = next(
            command
            for command in commands
            if "/usr/local/libexec/ucloud-sandbox-checkpoint" in command
            and "prepare" in command
        )
        self.assertEqual(helper_prepare[-4:], ("128", "1024", "64", "16"))
        self.assertTrue(any("stage" in command for command in commands))
        self.assertTrue(any(command[:2] == ("docker", "start") for command in commands))
        prepare_index = next(
            index
            for index, command in enumerate(commands)
            if "/usr/local/bin/ucloud-fork-agent" in command
            and command[-1] == "prepare"
        )
        storage_prepare_index = commands.index(helper_prepare)
        checkpoint_index = next(
            index
            for index, command in enumerate(commands)
            if command[1:3] == ("checkpoint", "create")
        )
        resume_index = next(
            index
            for index, command in enumerate(commands)
            if "/usr/local/bin/ucloud-fork-agent" in command and command[-1] == "resume"
        )
        start_index = next(
            index
            for index, command in enumerate(commands)
            if command[:2] == ("docker", "start")
        )
        restore_index = next(
            index
            for index, command in enumerate(commands)
            if "/usr/local/bin/ucloud-fork-agent" in command
            and command[-1] == "restore"
        )
        self.assertLess(storage_prepare_index, prepare_index)
        self.assertLess(prepare_index, checkpoint_index)
        self.assertLess(checkpoint_index, resume_index)
        self.assertLess(resume_index, start_index)
        self.assertLess(start_index, restore_index)

    def test_live_fork_fanout_captures_source_once_for_all_children(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox:latest",
            command=("sh", "-c", "while :; do sleep 1; done"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        targets = (
            sandbox_fork_target(source, {"id": "child-a"}),
            sandbox_fork_target(source, {"id": "child-b"}),
            sandbox_fork_target(source, {"id": "child-c"}),
        )
        operations = tuple(
            _create_operation(target, 4, f"fork-{target.id}-4") for target in targets
        )
        runtime = DockerGvisorRuntime(
            dry_run=True,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        results = runtime.fork_many(
            source,
            tuple(zip(targets, operations, strict=True)),
            source_generation=3,
            source_spec_hash=sandbox_spec_fingerprint(source),
            checkpoint_id="fork-set-artifact-1",
            fork_nonce=FORK_NONCE,
        )

        commands = [command for result in results for command in result.commands]
        self.assertEqual(
            sum(command[1:3] == ("checkpoint", "create") for command in commands),
            1,
        )
        self.assertEqual(
            sum(command[:2] == ("docker", "start") for command in commands),
            3,
        )
        self.assertEqual(sum("stage" in command for command in commands), 3)
        self.assertEqual(
            {result.checkpoint_id for result in results},
            {"fork-set-artifact-1"},
        )
        checkpoint_index = next(
            index
            for index, command in enumerate(commands)
            if command[1:3] == ("checkpoint", "create")
        )
        complete_index = next(
            index for index, command in enumerate(commands) if "complete" in command
        )
        seal_index = next(
            index for index, command in enumerate(commands) if "seal" in command
        )
        self.assertLess(checkpoint_index, complete_index)
        self.assertLess(complete_index, seal_index)

    def test_live_fork_fanout_restores_in_bounded_parallel_request_order(self) -> None:
        class ParallelForkRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=Path("/checkpoints"),
                    fork_restore_parallelism=8,
                )
                self.guard = Lock()
                self.first_wave = Barrier(8)
                self.capture_count = 0
                self.capture_complete = False
                self.active = 0
                self.max_active = 0

            def fork(  # type: ignore[override]
                self,
                source: SandboxSpec,
                target: SandboxSpec,
                operation: SandboxOperation,
                **kwargs: object,
            ) -> SandboxForkRuntimeResult:
                del source, operation
                if kwargs.get("_capture_only"):
                    with self.guard:
                        self.capture_count += 1
                        self.capture_complete = True
                    return SandboxForkRuntimeResult(
                        checkpoint_id=str(kwargs["checkpoint_id"]),
                        commands=(("capture",),),
                    )
                with self.guard:
                    self.assert_capture_complete()
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                try:
                    index = int(target.id.removeprefix("child-"))
                    if index < 8:
                        self.first_wave.wait(timeout=1)
                    time.sleep(0.005 * (1 + index % 3))
                finally:
                    with self.guard:
                        self.active -= 1
                return SandboxForkRuntimeResult(
                    checkpoint_id=str(kwargs["checkpoint_id"]),
                    commands=(("restore", target.id),),
                )

            def assert_capture_complete(self) -> None:
                if not self.capture_complete:
                    raise AssertionError("restore started before checkpoint capture")

        source = SandboxSpec(
            id="parent",
            image="busybox",
            memory_mb=128,
            disk_mb=128,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        targets = tuple(
            sandbox_fork_target(source, {"id": f"child-{index}"}) for index in range(10)
        )
        operations = tuple(
            _create_operation(target, 4, f"fork-{target.id}-4") for target in targets
        )
        runtime = ParallelForkRuntime()

        results = runtime.fork_many(
            source,
            tuple(zip(targets, operations, strict=True)),
            source_generation=3,
            source_spec_hash=sandbox_spec_fingerprint(source),
            checkpoint_id="fork-set-parallel",
            fork_nonce=FORK_NONCE,
        )

        self.assertEqual(runtime.capture_count, 1)
        self.assertEqual(runtime.max_active, 8)
        self.assertEqual(
            [result.commands[-1][-1] for result in results],
            [target.id for target in targets],
        )
        self.assertEqual(results[0].commands[0], ("capture",))

    def test_live_fork_fanout_stops_scheduling_after_restore_failure(self) -> None:
        class FailingParallelRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=Path("/checkpoints"),
                    fork_restore_parallelism=8,
                )
                self.guard = Lock()
                self.started: list[str] = []
                self.first_started = Event()

            def fork(  # type: ignore[override]
                self,
                source: SandboxSpec,
                target: SandboxSpec,
                operation: SandboxOperation,
                **kwargs: object,
            ) -> SandboxForkRuntimeResult:
                del source, operation
                if kwargs.get("_capture_only"):
                    return SandboxForkRuntimeResult(
                        checkpoint_id=str(kwargs["checkpoint_id"]),
                        commands=(("capture",),),
                    )
                with self.guard:
                    self.started.append(target.id)
                if target.id == "child-0":
                    self.first_started.set()
                    time.sleep(0.05)
                elif target.id == "child-1":
                    self.first_started.wait(timeout=1)
                    raise RuntimeError("restore rejected")
                else:
                    time.sleep(0.05)
                return SandboxForkRuntimeResult(
                    checkpoint_id=str(kwargs["checkpoint_id"]),
                    commands=(("restore", target.id),),
                )

        source = SandboxSpec(
            id="parent",
            image="busybox",
            memory_mb=128,
            disk_mb=128,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        targets = tuple(
            sandbox_fork_target(source, {"id": f"child-{index}"}) for index in range(10)
        )
        operations = tuple(
            _create_operation(target, 4, f"fork-{target.id}-4") for target in targets
        )
        runtime = FailingParallelRuntime()

        with self.assertRaisesRegex(RuntimeError, "restore rejected"):
            runtime.fork_many(
                source,
                tuple(zip(targets, operations, strict=True)),
                source_generation=3,
                source_spec_hash=sandbox_spec_fingerprint(source),
                checkpoint_id="fork-set-failure",
                fork_nonce=FORK_NONCE,
            )

        self.assertNotIn("child-8", runtime.started)
        self.assertNotIn("child-9", runtime.started)

    def test_live_fork_deadlines_and_parallelism_are_bounded(self) -> None:
        class DeadlineExecutor(RecordingExecutor):
            def __init__(self) -> None:
                super().__init__()
                self.timeouts: list[float] = []

            def run_with_timeout(
                self,
                argv: tuple[str, ...],
                *,
                timeout_seconds: float,
                input: bytes | None = None,
            ) -> CommandResult:
                self.timeouts.append(timeout_seconds)
                return self.run(argv, input=input)

        self.assertEqual(DEFAULT_FORK_RESTORE_PARALLELISM, 8)
        coupled_child_seconds = max(
            (
                (recovered + DEFAULT_FORK_RESTORE_PARALLELISM - 1)
                // DEFAULT_FORK_RESTORE_PARALLELISM
            )
            * MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
            + (
                (MAX_FORK_FANOUT - recovered + DEFAULT_FORK_RESTORE_PARALLELISM - 1)
                // DEFAULT_FORK_RESTORE_PARALLELISM
            )
            * (
                FORK_CHILD_SETUP_ALLOWANCE_SECONDS
                + MAX_FORK_RESTORE_TIMEOUT_SECONDS
                + MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
            )
            for recovered in range(MAX_FORK_FANOUT + 1)
        )
        self.assertEqual(coupled_child_seconds, 37 * 60)
        self.assertEqual(FORK_REQUEST_TIMEOUT_SECONDS, 55 * 60)
        executor = DeadlineExecutor()
        runtime = DockerGvisorRuntime(
            executor=executor,
            fork_command_timeout_seconds=MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS,
            fork_restore_timeout_seconds=MAX_FORK_RESTORE_TIMEOUT_SECONDS,
            fork_restore_parallelism=MAX_FORK_RESTORE_PARALLELISM,
        )
        self.assertEqual(
            runtime.fork_command_timeout_seconds,
            MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            runtime.fork_restore_timeout_seconds,
            MAX_FORK_RESTORE_TIMEOUT_SECONDS,
        )
        runtime._run_fork_command(("docker", "checkpoint"), phase="checkpoint")
        runtime._run_fork_command(("docker", "start"), phase="restore")
        self.assertEqual(
            executor.timeouts,
            [
                float(MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS),
                float(MAX_FORK_RESTORE_TIMEOUT_SECONDS),
            ],
        )
        with self.assertRaisesRegex(ValueError, "checkpoint timeout"):
            DockerGvisorRuntime(
                fork_command_timeout_seconds=MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS + 1
            )
        with self.assertRaisesRegex(ValueError, "restore timeout"):
            DockerGvisorRuntime(
                fork_restore_timeout_seconds=MAX_FORK_RESTORE_TIMEOUT_SECONDS + 1
            )
        with self.assertRaisesRegex(ValueError, "parallelism"):
            DockerGvisorRuntime(
                fork_restore_parallelism=MAX_FORK_RESTORE_PARALLELISM + 1
            )
        with self.assertRaisesRegex(ValueError, "parallelism"):
            DockerGvisorRuntime(
                fork_restore_parallelism=DEFAULT_FORK_RESTORE_PARALLELISM - 1
            )
        with self.assertRaisesRegex(ValueError, r"\[1, 60\]"):
            replace(FORK_PROTOCOL, timeout_seconds=61).validate(required=True)

    def test_live_fork_setup_timeout_cleans_stage_but_preserves_ambiguous_create(
        self,
    ) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            memory_mb=128,
            disk_mb=128,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)
        operation = _create_operation(target, 4, "fork-child-4")

        class SetupTimeoutExecutor:
            def __init__(self, timeout_phase: str) -> None:
                self.timeout_phase = timeout_phase
                self.commands: list[tuple[str, ...]] = []
                self.timeouts: list[float] = []

            def _result(self, argv: tuple[str, ...]) -> CommandResult:
                if argv[:2] == ("docker", "inspect"):
                    template = argv[3]
                    sandbox_name = argv[-1]
                    if template == "{{json .Config.Labels}}":
                        if sandbox_name.endswith("parent"):
                            return CommandResult(
                                argv=argv,
                                exit_code=0,
                                stdout=json.dumps(
                                    {
                                        "ucloud-sandboxes.managed": "true",
                                        "ucloud-sandboxes.sandbox-id": source.id,
                                        SANDBOX_GENERATION_LABEL: "3",
                                        SANDBOX_OPERATION_ID_LABEL: "source-create-3",
                                        SANDBOX_SPEC_HASH_LABEL: source_hash,
                                    }
                                ),
                            )
                        return CommandResult(
                            argv=argv,
                            exit_code=1,
                            stderr="No such container",
                        )
                    if template == "{{.State.Running}}":
                        return CommandResult(argv=argv, exit_code=0, stdout="true")
                    if template == "{{.Id}} {{.Image}}":
                        container_id = (
                            "1" * 64 if sandbox_name.endswith("parent") else "3" * 64
                        )
                        return CommandResult(
                            argv=argv,
                            exit_code=0,
                            stdout=f"{container_id} sha256:{'2' * 64}",
                        )
                helper_path = "/usr/local/libexec/ucloud-sandbox-checkpoint"
                if helper_path in argv:
                    action = argv[argv.index(helper_path) + 1]
                    if action == "status":
                        return CommandResult(
                            argv=argv,
                            exit_code=0,
                            stdout=json.dumps(
                                {
                                    "artifact_id": "fork-setup-timeout",
                                    "checkpoint_id": "state",
                                    "source_container_id": "1" * 64,
                                    "source_image_id": "sha256:" + "2" * 64,
                                    "source_spec_hash": source_hash,
                                }
                            ),
                        )
                    return CommandResult(argv=argv, exit_code=0)
                if argv[:2] == ("docker", "exec"):
                    role = argv[-1]
                    return CommandResult(
                        argv=argv,
                        exit_code=0,
                        stdout=f"UCLOUD_FORK_READY={FORK_NONCE}:{role}\n",
                    )
                if argv[:2] == ("docker", "create"):
                    return CommandResult(argv=argv, exit_code=0, stdout="3" * 64)
                return CommandResult(argv=argv, exit_code=0)

            def run(
                self,
                argv: tuple[str, ...],
                *,
                input: bytes | None = None,
            ) -> CommandResult:
                del input
                self.commands.append(argv)
                return self._result(argv)

            def run_with_timeout(
                self,
                argv: tuple[str, ...],
                *,
                timeout_seconds: float,
                input: bytes | None = None,
            ) -> CommandResult:
                del input
                self.commands.append(argv)
                self.timeouts.append(timeout_seconds)
                helper_path = "/usr/local/libexec/ucloud-sandbox-checkpoint"
                helper_action = (
                    argv[argv.index(helper_path) + 1] if helper_path in argv else ""
                )
                if (
                    self.timeout_phase == "create" and argv[:2] == ("docker", "create")
                ) or (self.timeout_phase == "stage" and helper_action == "stage"):
                    return CommandResult(argv=argv, exit_code=124, stderr="timed out")
                return self._result(argv)

        for timeout_phase in ("create", "stage"):
            with self.subTest(timeout_phase=timeout_phase):
                executor = SetupTimeoutExecutor(timeout_phase)
                runtime = DockerGvisorRuntime(
                    executor=executor,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
                )

                with self.assertRaises(SandboxForkCommandTimeoutError):
                    runtime.fork(
                        source,
                        target,
                        operation,
                        source_generation=3,
                        source_operation_id="source-create-3",
                        source_spec_hash=source_hash,
                        checkpoint_id="fork-setup-timeout",
                        fork_nonce=FORK_NONCE,
                    )

                helper_actions = [
                    command[
                        command.index("/usr/local/libexec/ucloud-sandbox-checkpoint")
                        + 1
                    ]
                    for command in executor.commands
                    if "/usr/local/libexec/ucloud-sandbox-checkpoint" in command
                ]
                removed = any(
                    command[:3] == ("docker", "rm", "-f")
                    for command in executor.commands
                )
                if timeout_phase == "stage":
                    self.assertIn("unstage", helper_actions)
                    self.assertTrue(removed)
                else:
                    self.assertNotIn("unstage", helper_actions)
                    self.assertFalse(removed)
                self.assertFalse(
                    any(
                        command[:2] == ("docker", "start")
                        for command in executor.commands
                    )
                )

    def test_live_fork_rejects_changed_source_runtime_identity(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)
        executor = RecordingExecutor(
            stdout=json.dumps(
                {
                    "ucloud-sandboxes.managed": "true",
                    "ucloud-sandboxes.sandbox-id": source.id,
                    SANDBOX_GENERATION_LABEL: "99",
                    SANDBOX_OPERATION_ID_LABEL: "replacement",
                    SANDBOX_SPEC_HASH_LABEL: source_hash,
                }
            )
        )
        runtime = DockerGvisorRuntime(
            executor=executor,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        with self.assertRaisesRegex(
            SandboxStaleOperationError, "source runtime identity changed"
        ):
            runtime.fork(
                source,
                target,
                _create_operation(target, 4, "fork-child-4"),
                source_generation=3,
                source_operation_id="source-create-3",
                source_spec_hash=source_hash,
                checkpoint_id="fork-artifact-identity",
                fork_nonce=FORK_NONCE,
            )

        self.assertFalse(
            any(
                command[1:3] == ("checkpoint", "create")
                for command in executor.commands
            )
        )

    def test_known_checkpoint_failure_cancels_quiesce_before_drop(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)
        executor = ForkCaptureFailureExecutor(source, source_hash, timeout=False)
        runtime = DockerGvisorRuntime(
            executor=executor,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        with self.assertRaisesRegex(RuntimeError, "checkpoint failed"):
            runtime.fork(
                source,
                target,
                _create_operation(target, 4, "fork-child-4"),
                source_generation=3,
                source_operation_id="source-create-3",
                source_spec_hash=source_hash,
                checkpoint_id="fork-artifact-failure",
                fork_nonce=FORK_NONCE,
            )

        commands = executor.commands
        helper_path = "/usr/local/libexec/ucloud-sandbox-checkpoint"
        helper_prepare_index = next(
            index
            for index, command in enumerate(commands)
            if helper_path in command
            and command[command.index(helper_path) + 1] == "prepare"
        )
        protocol_prepare_index = next(
            index
            for index, command in enumerate(commands)
            if command[:2] == ("docker", "exec") and command[-1] == "prepare"
        )
        checkpoint_index = next(
            index
            for index, command in enumerate(commands)
            if command[:3] == ("docker", "checkpoint", "create")
        )
        cancel_index = next(
            index
            for index, command in enumerate(commands)
            if command[:2] == ("docker", "exec") and command[-1] == "cancel"
        )
        final_drop_index = max(
            index
            for index, command in enumerate(commands)
            if helper_path in command
            and command[command.index(helper_path) + 1] == "drop"
        )
        self.assertEqual(
            commands[helper_prepare_index][-4:],
            ("128", "1024", "64", "16"),
        )
        self.assertLess(helper_prepare_index, protocol_prepare_index)
        self.assertLess(protocol_prepare_index, checkpoint_index)
        self.assertLess(checkpoint_index, cancel_index)
        self.assertLess(cancel_index, final_drop_index)

    def test_checkpoint_timeout_keeps_quiesce_and_pending_artifact(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)
        executor = ForkCaptureFailureExecutor(source, source_hash, timeout=True)
        runtime = DockerGvisorRuntime(
            executor=executor,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        with self.assertRaises(SandboxForkCommandTimeoutError):
            runtime.fork(
                source,
                target,
                _create_operation(target, 4, "fork-child-4"),
                source_generation=3,
                source_operation_id="source-create-3",
                source_spec_hash=source_hash,
                checkpoint_id="fork-artifact-timeout",
                fork_nonce=FORK_NONCE,
            )

        helper_path = "/usr/local/libexec/ucloud-sandbox-checkpoint"
        helper_actions = [
            command[command.index(helper_path) + 1]
            for command in executor.commands
            if helper_path in command
        ]
        self.assertEqual(helper_actions, ["status", "drop", "prepare"])
        self.assertFalse(
            any(
                command[:2] == ("docker", "exec") and command[-1] == "cancel"
                for command in executor.commands
            )
        )

    def test_checkpoint_output_overflow_is_also_ambiguous(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)
        executor = ForkCaptureFailureExecutor(
            source,
            source_hash,
            timeout=True,
            ambiguous_exit_code=125,
        )
        runtime = DockerGvisorRuntime(
            executor=executor,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        with self.assertRaises(SandboxForkCommandTimeoutError):
            runtime.fork(
                source,
                target,
                _create_operation(target, 4, "fork-child-overflow-4"),
                source_generation=3,
                source_operation_id="source-create-3",
                source_spec_hash=source_hash,
                checkpoint_id="fork-artifact-output-overflow",
                fork_nonce=FORK_NONCE,
            )

        self.assertFalse(
            any(
                command[:2] == ("docker", "exec") and command[-1] == "cancel"
                for command in executor.commands
            )
        )
        helper_path = "/usr/local/libexec/ucloud-sandbox-checkpoint"
        self.assertEqual(
            [
                command[command.index(helper_path) + 1]
                for command in executor.commands
                if helper_path in command
            ],
            ["status", "drop", "prepare"],
        )

    def test_live_fork_never_recaptures_ambiguous_pending_checkpoint(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        target = sandbox_fork_target(source, {"id": "child"})
        source_hash = sandbox_spec_fingerprint(source)

        class PendingCheckpointExecutor:
            def __init__(self) -> None:
                self.commands: list[tuple[str, ...]] = []

            def run(
                self,
                argv: tuple[str, ...],
                *,
                input: bytes | None = None,
            ) -> CommandResult:
                del input
                self.commands.append(argv)
                if argv[:2] == ("docker", "inspect"):
                    template = argv[3]
                    if template == "{{json .Config.Labels}}":
                        return CommandResult(
                            argv=argv,
                            exit_code=0,
                            stdout=json.dumps(
                                {
                                    "ucloud-sandboxes.managed": "true",
                                    "ucloud-sandboxes.sandbox-id": source.id,
                                    SANDBOX_GENERATION_LABEL: "3",
                                    SANDBOX_OPERATION_ID_LABEL: "source-create-3",
                                    SANDBOX_SPEC_HASH_LABEL: source_hash,
                                }
                            ),
                        )
                    if template == "{{.State.Running}}":
                        return CommandResult(argv=argv, exit_code=0, stdout="true")
                    if template == "{{.Id}} {{.Image}}":
                        return CommandResult(
                            argv=argv,
                            exit_code=0,
                            stdout=("1" * 64 + " sha256:" + "2" * 64),
                        )
                if "status" in argv:
                    return CommandResult(argv=argv, exit_code=3)
                if "seal" in argv:
                    return CommandResult(
                        argv=argv,
                        exit_code=2,
                        stderr="completion marker is missing",
                    )
                return CommandResult(argv=argv, exit_code=0)

        executor = PendingCheckpointExecutor()
        runtime = DockerGvisorRuntime(
            executor=executor,
            allow_storage_opt_quota=True,
            fork_enabled=True,
            checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
        )

        with self.assertRaisesRegex(RuntimeError, "refusing to recapture"):
            runtime.fork(
                source,
                target,
                _create_operation(target, 4, "fork-child-4"),
                source_generation=3,
                source_operation_id="source-create-3",
                source_spec_hash=source_hash,
                checkpoint_id="fork-artifact-pending",
                fork_nonce=FORK_NONCE,
            )

        self.assertFalse(any("drop" in command for command in executor.commands))
        self.assertFalse(
            any(
                command[1:3] == ("checkpoint", "create")
                for command in executor.commands
            )
        )

    def test_manager_fanout_persists_all_intents_with_one_checkpoint(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=Path(raw_dir) / "checkpoints",
            )
            manager = SandboxManager(store, runtime)
            source = SandboxSpec(
                id="parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            targets = (
                sandbox_fork_target(source, {"id": "child-a"}),
                sandbox_fork_target(source, {"id": "child-b"}),
            )

            records, results = manager.fork_many(source.id, targets)

            checkpoint_ids = {record.checkpoint_id for record in records}
            fork_nonces = {record.fork_nonce for record in records}
            self.assertEqual(len(checkpoint_ids), 1)
            self.assertEqual(len(fork_nonces), 1)
            self.assertRegex(next(iter(fork_nonces)), r"^[0-9a-f]{64}$")
            self.assertTrue(next(iter(checkpoint_ids)).startswith("fork-set-"))
            self.assertEqual({record.state for record in records}, {"running"})
            persisted = store.load_state().records
            self.assertEqual(
                {persisted[target.id].checkpoint_id for target in targets},
                checkpoint_ids,
            )
            self.assertEqual(
                {persisted[target.id].fork_nonce for target in targets},
                fork_nonces,
            )
            commands = [command for result in results for command in result.commands]
            self.assertEqual(
                sum(command[1:3] == ("checkpoint", "create") for command in commands),
                1,
            )

            replayed, replay_result = manager.fork(source.id, targets[0])
            self.assertEqual(replayed.checkpoint_id, next(iter(checkpoint_ids)))
            self.assertEqual(replay_result.checkpoint_id, replayed.checkpoint_id)

    def test_manager_recovery_is_bounded_parallel_ordered_and_fail_safe(self) -> None:
        class RecoveryRuntime(DockerGvisorRuntime):
            def __init__(self, checkpoint_root: Path) -> None:
                super().__init__(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=checkpoint_root,
                    fork_restore_parallelism=8,
                )
                self.guard = Lock()
                self.mode = "create"
                self.identities: dict[str, tuple[int, str, str]] = {}
                self.inspect_barrier = Barrier(8)
                self.ready_barrier = Barrier(8)
                self.failure_started = Event()
                self.inspect_active = 0
                self.ready_active = 0
                self.max_inspect_active = 0
                self.max_ready_active = 0
                self.inspect_budgets: list[float] = []
                self.ready_started: list[str] = []

            def managed_container_identity(  # type: ignore[override]
                self,
                sandbox_id: str,
                **kwargs: object,
            ) -> tuple[int, str, str] | None:
                if self.mode == "create":
                    return None
                budget = kwargs.get("_fork_setup_budget")
                with self.guard:
                    self.inspect_active += 1
                    self.max_inspect_active = max(
                        self.max_inspect_active,
                        self.inspect_active,
                    )
                    self.inspect_budgets.append(float(getattr(budget, "limit_seconds")))
                try:
                    index = int(sandbox_id.removeprefix("child-"))
                    if index < 8:
                        self.inspect_barrier.wait(timeout=1)
                    time.sleep(0.002 * (index % 3))
                    return self.identities[sandbox_id]
                finally:
                    with self.guard:
                        self.inspect_active -= 1

            def managed_container_running(  # type: ignore[override]
                self,
                sandbox_id: str,
                **_kwargs: object,
            ) -> bool:
                return self.mode != "create" and sandbox_id in self.identities

            def wait_fork_ready(  # type: ignore[override]
                self,
                spec: SandboxSpec,
                *,
                checkpoint_id: str,
                fork_nonce: str,
            ) -> CommandResult:
                if self.mode == "create":
                    return super().wait_fork_ready(
                        spec,
                        checkpoint_id=checkpoint_id,
                        fork_nonce=fork_nonce,
                    )
                with self.guard:
                    self.ready_started.append(spec.id)
                    self.ready_active += 1
                    self.max_ready_active = max(
                        self.max_ready_active,
                        self.ready_active,
                    )
                try:
                    if self.mode == "failure":
                        if spec.id == "child-1":
                            self.failure_started.set()
                            raise RuntimeError("recovery readiness rejected")
                        self.failure_started.wait(timeout=1)
                        time.sleep(0.05)
                    else:
                        index = int(spec.id.removeprefix("child-"))
                        if index < 8:
                            self.ready_barrier.wait(timeout=1)
                        time.sleep(0.002 * (2 - index % 3))
                    return CommandResult(
                        argv=("ready", spec.id),
                        exit_code=0,
                    )
                finally:
                    with self.guard:
                        self.ready_active -= 1

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = RecoveryRuntime(Path(raw_dir) / "checkpoints")
            manager = SandboxManager(store, runtime)
            source = SandboxSpec(
                id="parent",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            targets = tuple(
                sandbox_fork_target(source, {"id": f"child-{index}"})
                for index in range(10)
            )
            created, _results = manager.fork_many(source.id, targets)
            runtime.identities = {
                record.spec.id: (
                    record.generation,
                    record.operation_id,
                    record.spec_hash,
                )
                for record in created
            }

            state = store.load_state()
            for record in created:
                state.records[record.spec.id] = replace(record, state="restoring")
            store.save_state(state.records, state.tombstones)
            runtime.mode = "success"

            recovered, results, timings = manager.fork_many_with_timings(
                source.id,
                targets,
            )

            self.assertEqual(
                [record.spec.id for record in recovered],
                [target.id for target in targets],
            )
            self.assertTrue(all(result.commands == () for result in results))
            self.assertEqual(runtime.max_inspect_active, 8)
            self.assertEqual(runtime.max_ready_active, 8)
            self.assertTrue(runtime.inspect_budgets)
            self.assertTrue(all(0 < budget <= 30 for budget in runtime.inspect_budgets))
            self.assertIn("recover_inspect_ms", timings["phases"])
            self.assertIn("recover_ready_ms", timings["phases"])

            state = store.load_state()
            for record in recovered:
                state.records[record.spec.id] = replace(record, state="restoring")
            store.save_state(state.records, state.tombstones)
            runtime.mode = "failure"
            runtime.inspect_barrier = Barrier(8)
            runtime.failure_started = Event()
            runtime.ready_started.clear()

            with self.assertRaisesRegex(RuntimeError, "readiness rejected"):
                manager.fork_many(source.id, targets)

            self.assertNotIn("child-8", runtime.ready_started)
            self.assertNotIn("child-9", runtime.ready_started)
            persisted = store.load_state().records
            self.assertEqual(
                {persisted[target.id].state for target in targets},
                {"restoring"},
            )

    def test_manager_post_commit_cleanup_has_one_wall_clock_budget(self) -> None:
        class SlowCleanupRuntime(DockerGvisorRuntime):
            def __init__(self, checkpoint_root: Path) -> None:
                super().__init__(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=checkpoint_root,
                    fork_restore_parallelism=8,
                )
                self.guard = Lock()
                self.cleanup_calls = 0
                self.cleanup_active = 0
                self.max_cleanup_active = 0
                self.cleanup_budgets: list[float] = []
                self.release_budgets: list[float] = []

            def cleanup_restored_checkpoint(  # type: ignore[override]
                self,
                sandbox_id: str,
                **kwargs: object,
            ) -> CommandResult:
                budget = kwargs["_fork_setup_budget"]
                remaining = float(getattr(budget, "remaining_seconds"))
                with self.guard:
                    self.cleanup_calls += 1
                    self.cleanup_active += 1
                    self.max_cleanup_active = max(
                        self.max_cleanup_active,
                        self.cleanup_active,
                    )
                    self.cleanup_budgets.append(remaining)
                try:
                    time.sleep(remaining)
                finally:
                    with self.guard:
                        self.cleanup_active -= 1
                return CommandResult(argv=("cleanup", sandbox_id), exit_code=0)

            def release_checkpoint(  # type: ignore[override]
                self,
                checkpoint_id: str,
                **kwargs: object,
            ) -> CommandResult:
                budget = kwargs["_fork_setup_budget"]
                remaining = float(getattr(budget, "remaining_seconds"))
                self.release_budgets.append(remaining)
                time.sleep(remaining)
                return CommandResult(argv=("release", checkpoint_id), exit_code=0)

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = SlowCleanupRuntime(Path(raw_dir) / "checkpoints")
            manager = SandboxManager(store, runtime)
            source = SandboxSpec(
                id="parent",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            targets = tuple(
                sandbox_fork_target(source, {"id": f"child-{index}"})
                for index in range(16)
            )

            started = time.monotonic()
            with patch(
                "ucloud_sandboxes.sandbox.FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS",
                0.05,
            ):
                records, _results = manager.fork_many(source.id, targets)
            elapsed = time.monotonic() - started

            self.assertLess(elapsed, 0.3)
            self.assertLess(runtime.cleanup_calls, len(targets))
            self.assertEqual(runtime.max_cleanup_active, 8)
            self.assertTrue(
                all(0 < budget <= 0.05 for budget in runtime.cleanup_budgets)
            )
            self.assertTrue(
                all(0 < budget <= 0.05 for budget in runtime.release_budgets)
            )
            self.assertEqual({record.state for record in records}, {"running"})

    def test_shared_checkpoint_is_released_after_last_restore_intent(self) -> None:
        class TrackingRuntime(DockerGvisorRuntime):
            def __init__(self, checkpoint_root: Path) -> None:
                super().__init__(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=checkpoint_root,
                )
                self.released: list[str] = []

            def release_checkpoint(
                self,
                checkpoint_id: str,
                **_kwargs: object,
            ) -> CommandResult:
                self.released.append(checkpoint_id)
                return CommandResult(argv=("release", checkpoint_id), exit_code=0)

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = TrackingRuntime(Path(raw_dir) / "checkpoints")
            manager = SandboxManager(store, runtime)
            source = SandboxSpec(
                id="parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            targets = (
                sandbox_fork_target(source, {"id": "child-a"}),
                sandbox_fork_target(source, {"id": "child-b"}),
            )
            records, _results = manager.fork_many(source.id, targets)
            checkpoint_id = records[0].checkpoint_id
            self.assertEqual(runtime.released, [checkpoint_id])

            manager.delete("child-a")
            self.assertEqual(runtime.released, [checkpoint_id, checkpoint_id])

            manager.delete("child-b")
            self.assertEqual(
                runtime.released,
                [checkpoint_id, checkpoint_id, checkpoint_id],
            )

    def test_manager_fanout_capacity_failure_persists_no_partial_intent(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=Path(raw_dir) / "checkpoints",
            )
            manager = SandboxManager(
                store,
                runtime,
                effective_capacity=ResourceQuantity(memory_mb=300),
            )
            source = SandboxSpec(
                id="parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            targets = (
                sandbox_fork_target(source, {"id": "child-a", "memory_mb": 96}),
                sandbox_fork_target(source, {"id": "child-b", "memory_mb": 96}),
            )

            with self.assertRaises(SandboxCapacityUnavailableError):
                manager.fork_many(source.id, targets)

            self.assertEqual(set(store.load_state().records), {source.id})

    def test_manager_rejects_oversized_fanout_before_persisting_intents(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(
                store,
                DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                    fork_enabled=True,
                    checkpoint_root=Path(raw_dir) / "checkpoints",
                ),
            )
            targets = tuple(
                SandboxSpec(id=f"child-{index}", image="busybox") for index in range(65)
            )

            with self.assertRaisesRegex(ValueError, "cannot exceed 64"):
                manager.fork_many("parent", targets)

            self.assertEqual(store.load_state().records, {})

    def test_fork_target_rejects_restore_incompatible_changes(self) -> None:
        source = SandboxSpec(
            id="parent",
            image="busybox",
            command=("sleep", "infinity"),
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )

        with self.assertRaisesRegex(ValueError, "restore-incompatible"):
            sandbox_fork_target(
                source,
                {
                    "sandbox": {
                        **source.to_dict(),
                        "id": "child",
                        "command": ["echo", "fresh-process"],
                    }
                },
            )

    def test_fork_persists_restore_intent_and_exec_lease_blocks_it(self) -> None:
        from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=Path(raw_dir) / "checkpoints",
            )
            manager = SandboxManager(store, runtime)
            source = SandboxSpec(
                id="parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_record, _result = manager.create(source)
            store.upsert(replace(source_record, state="running"))
            target = sandbox_fork_target(source, {"id": "child"})
            sessions = ExecSessionManager(manager)
            session = sessions.start(
                SandboxExecSpec(
                    sandbox_id=source.id,
                    command=("sh",),
                    stdin=True,
                )
            )

            with self.assertRaises(SandboxBusyError):
                manager.fork(source.id, target)

            sessions.close_stdin(session.id)
            record, fork_result = manager.fork(source.id, target)

            self.assertEqual(record.state, "running")
            self.assertEqual(record.creation_kind, "restore")
            self.assertEqual(record.source_sandbox_id, source.id)
            self.assertEqual(record.source_generation, source_record.generation)
            self.assertEqual(record.checkpoint_id, fork_result.checkpoint_id)

            self.assertEqual(
                manager.require_activity_sandbox(source.id).state,
                "running",
            )
            runtime.checkpoint_artifact_state = lambda _checkpoint: "pending"  # type: ignore[method-assign]
            deleted, _delete_result = manager.delete(target.id)
            self.assertIsNotNone(deleted)

    def test_drain_persists_replays_and_requires_matching_undrain_token(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )

            drained = manager.configure_drain(
                "drain-1",
                True,
                active_build_count=lambda: 0,
            )
            replay = manager.configure_drain(
                "drain-1",
                True,
                active_build_count=lambda: 0,
            )

            self.assertTrue(drained.ready)
            self.assertFalse(drained.drain.admission_open)
            self.assertEqual(
                replay.activity.activity_revision, drained.activity.activity_revision
            )
            persisted = SandboxStore(path).load_state().drain
            self.assertTrue(persisted.draining)
            self.assertEqual(persisted.token, "drain-1")
            with self.assertRaises(SandboxConflictError):
                manager.configure_drain(
                    "other-drain",
                    True,
                    active_build_count=lambda: 0,
                )
            with self.assertRaises(SandboxAdmissionClosedError):
                manager.create(
                    SandboxSpec(id="blocked", image="busybox", memory_mb=128)
                )
            with self.assertRaises(SandboxConflictError):
                manager.configure_drain(
                    "other-drain",
                    False,
                    active_build_count=lambda: 0,
                )

            opened = manager.configure_drain(
                "drain-1",
                False,
                active_build_count=lambda: 0,
            )
            opened_replay = manager.configure_drain(
                "drain-1",
                False,
                active_build_count=lambda: 0,
            )
            self.assertTrue(opened.drain.admission_open)
            self.assertFalse(opened.drain.draining)
            self.assertEqual(
                opened_replay.activity.activity_revision,
                opened.activity.activity_revision,
            )
            record, _result = manager.create(
                SandboxSpec(id="accepted", image="busybox", memory_mb=128)
            )
            self.assertEqual(record.spec.id, "accepted")

    def test_drain_waits_for_existing_work_then_reacknowledges_revision(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            spec = SandboxSpec(id="existing", image="busybox", memory_mb=128)
            existing, _result = manager.create(spec)

            draining = manager.configure_drain(
                "drain-existing",
                True,
                active_build_count=lambda: 0,
            )

            self.assertFalse(draining.ready)
            self.assertEqual(draining.drain.drain_activity_epoch, 0)
            replay, _result = manager.create(spec)
            self.assertEqual(replay, existing)
            with self.assertRaises(SandboxAdmissionClosedError):
                manager.create(SandboxSpec(id="new", image="busybox", memory_mb=128))
            manager.delete(spec.id)
            ready = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            self.assertTrue(ready.ready)
            self.assertEqual(
                ready.drain.drain_activity_epoch,
                ready.activity.activity_revision,
            )

    def test_multiprocess_create_cannot_enter_after_drain_ack(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )
            drained = manager.configure_drain(
                "drain-process",
                True,
                active_build_count=lambda: 0,
            )
            self.assertTrue(drained.ready)
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_create_after_drain,
                    args=(str(path), index, results),
                )
                for index in range(4)
            ]
            for process in processes:
                process.start()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes, ["closed"] * 4)
            self.assertEqual(SandboxStore(path).load(), {})

    def test_runtime_command_carries_operation_identity_labels(self) -> None:
        spec = SandboxSpec(id="versioned", image="busybox", memory_mb=128)
        operation = _create_operation(spec, generation=7, operation_id="create-7")

        argv = DockerGvisorRuntime(dry_run=True).create_command(
            spec,
            operation=operation,
        )

        self.assertIn(f"{SANDBOX_GENERATION_LABEL}=7", argv)
        self.assertIn(f"{SANDBOX_OPERATION_ID_LABEL}=create-7", argv)
        self.assertIn(f"{SANDBOX_SPEC_HASH_LABEL}={operation.spec_hash}", argv)

    def test_generation_replay_conflict_and_tombstone_fencing(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="versioned", image="busybox", memory_mb=128)
            create_one = _create_operation(spec, 1, "create-1")

            first, _result = manager.create(spec, operation=create_one)
            replay, _result = manager.create(spec, operation=create_one)

            self.assertEqual(first.operation_id, "create-1")
            self.assertEqual(first.generation, 1)
            self.assertEqual(replay, first)
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 1, "other-create"),
                )
            changed_spec = replace(spec, image="alpine")
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    changed_spec,
                    operation=_create_operation(changed_spec, 1, "create-1"),
                )
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 0, "stale-create"),
                )
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 2, "create-2"),
                )

            deleted, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )
            replay_delete, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )

            self.assertIsNotNone(deleted)
            assert deleted is not None
            self.assertEqual(deleted.spec, first.spec)
            self.assertEqual(deleted.generation, first.generation)
            self.assertEqual(deleted.operation_id, first.operation_id)
            self.assertEqual(deleted.state, "deleting")
            self.assertEqual(deleted.delete_operation_id, "delete-1")
            self.assertIsNone(replay_delete)
            state = store.load_state()
            self.assertEqual(state.records, {})
            self.assertEqual(state.tombstones[spec.id].generation, 1)
            self.assertEqual(state.tombstones[spec.id].operation_id, "delete-1")
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec, operation=create_one)
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=1,
                    operation_id="different-delete",
                )
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec)
            with self.assertRaises(SandboxStaleOperationError):
                manager.delete(spec.id)

            create_two = _create_operation(spec, 2, "create-2")
            second, _result = manager.create(spec, operation=create_two)
            self.assertEqual(second.generation, 2)
            delayed_delete, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )
            self.assertIsNone(delayed_delete)
            self.assertEqual(store.load()[spec.id].generation, 2)
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=3,
                    operation_id="delete-3",
                )
            self.assertEqual(store.load()[spec.id].generation, 2)

    def test_delete_of_absent_generation_persists_fence(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="canceled", image="busybox", memory_mb=128)

            deleted, result = manager.delete(
                spec.id,
                generation=4,
                operation_id="cancel-4",
            )

            self.assertIsNone(deleted)
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(store.load_state().tombstones[spec.id].generation, 4)
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 4, "create-4"),
                )

    def test_ttl_tombstone_accepts_later_explicit_delete_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(
                id="expired",
                image="busybox",
                memory_mb=128,
                ttl_seconds=1,
            )
            record, _result = manager.create(
                spec,
                operation=_create_operation(spec, 5, "create-5"),
            )
            manager.cleanup_expired(now=record.created_at + timedelta(seconds=2))

            ttl_tombstone = store.load_state().tombstones[spec.id]
            self.assertEqual(ttl_tombstone.operation_id, "ttl:create-5")
            absent, _result = manager.delete(
                spec.id,
                generation=5,
                operation_id="delete-5",
            )
            replay, _result = manager.delete(
                spec.id,
                generation=5,
                operation_id="delete-5",
            )

            self.assertIsNone(absent)
            self.assertIsNone(replay)
            self.assertEqual(
                store.load_state().tombstones[spec.id].operation_id,
                "delete-5",
            )
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=5,
                    operation_id="other-delete-5",
                )

    def test_legacy_delete_is_replayable_but_fences_id_reuse(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="legacy", image="busybox", memory_mb=128)
            manager.create(spec)

            deleted, _result = manager.delete(spec.id)
            replay, _result = manager.delete(spec.id)

            self.assertIsNotNone(deleted)
            self.assertIsNone(replay)
            self.assertEqual(store.load_state().tombstones[spec.id].generation, 0)
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec)

    def test_create_retry_recovers_runtime_side_effect_from_labels(self) -> None:
        with TemporaryDirectory() as raw_dir:
            spec = SandboxSpec(id="recovered", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 3, "create-3")
            executor = CrashRecoveryExecutor(spec.id, operation)
            runtime = DockerGvisorRuntime(executor=executor)
            runtime.create(spec, operation=operation)
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                runtime,
            )

            record, result = manager.create(spec, operation=operation)

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(record.generation, 3)
            self.assertEqual(record.operation_id, "create-3")
            self.assertEqual(record.spec_hash, operation.spec_hash)
            self.assertEqual(manager.store.load()[spec.id], record)

    def test_create_persists_intent_before_ambiguous_runtime_failure(self) -> None:
        class AmbiguousCreateExecutor:
            def __init__(self, spec: SandboxSpec, operation: SandboxOperation) -> None:
                self.spec = spec
                self.operation = operation
                self.created = False
                self.create_calls = 0

            def run(self, argv, *, input=None):
                del input
                if len(argv) > 1 and argv[1] == "run":
                    self.create_calls += 1
                    self.created = True
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr="docker response was lost after create",
                    )
                if len(argv) > 1 and argv[1] == "inspect" and self.created:
                    return CommandResult(
                        argv=argv,
                        exit_code=0,
                        stdout=json.dumps(
                            {
                                "ucloud-sandboxes.managed": "true",
                                "ucloud-sandboxes.sandbox-id": self.spec.id,
                                SANDBOX_GENERATION_LABEL: str(
                                    self.operation.generation
                                ),
                                SANDBOX_OPERATION_ID_LABEL: self.operation.operation_id,
                                SANDBOX_SPEC_HASH_LABEL: self.operation.spec_hash,
                            }
                        ),
                    )
                return CommandResult(
                    argv=argv,
                    exit_code=1,
                    stderr="No such container",
                )

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            spec = SandboxSpec(id="ambiguous", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 7, "create-7")
            executor = AmbiguousCreateExecutor(spec, operation)
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )

            with self.assertRaisesRegex(RuntimeError, "failed with exit code 1"):
                manager.create(spec, operation=operation)

            intent = SandboxStore(path).load()[spec.id]
            self.assertEqual(intent.state, "planned")
            self.assertEqual(intent.operation_id, operation.operation_id)
            restarted = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            recovered, result, timings = restarted.create_with_timings(
                spec,
                operation=operation,
            )

            self.assertEqual(recovered.state, "running")
            self.assertEqual(result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "container")
            self.assertEqual(executor.create_calls, 1)

    def test_create_replay_resumes_planned_intent_when_runtime_is_absent(self) -> None:
        class FailBeforeCreateExecutor:
            def __init__(self) -> None:
                self.create_calls = 0

            def run(self, argv, *, input=None):
                del input
                if len(argv) > 1 and argv[1] == "inspect":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr="No such container",
                    )
                if len(argv) > 1 and argv[1] == "run":
                    self.create_calls += 1
                    if self.create_calls == 1:
                        return CommandResult(
                            argv=argv,
                            exit_code=1,
                            stderr="daemon unavailable before create",
                        )
                    return CommandResult(argv=argv, exit_code=0)
                return CommandResult(argv=argv, exit_code=0)

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            spec = SandboxSpec(id="resumed", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 8, "create-8")
            executor = FailBeforeCreateExecutor()
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            with self.assertRaisesRegex(RuntimeError, "failed with exit code 1"):
                manager.create(spec, operation=operation)
            self.assertEqual(SandboxStore(path).load()[spec.id].state, "planned")

            restarted = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            recovered, _result, timings = restarted.create_with_timings(
                spec,
                operation=operation,
            )

            self.assertEqual(recovered.state, "running")
            self.assertTrue(timings["idempotent"])
            self.assertEqual(executor.create_calls, 2)

    def test_delete_replay_completes_durable_intent_after_runtime_crash_window(
        self,
    ) -> None:
        class FailSecondSaveStore(SandboxStore):
            def __init__(self, path: Path) -> None:
                super().__init__(path)
                self.calls = 0

            def save_state(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("crash after runtime delete")
                return super().save_state(*args, **kwargs)

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor)
            spec = SandboxSpec(id="delete-crash", image="busybox", memory_mb=128)
            create_operation = _create_operation(spec, 4, "create-4")
            SandboxManager(SandboxStore(path), runtime).create(
                spec,
                operation=create_operation,
            )

            crashing = SandboxManager(FailSecondSaveStore(path), runtime)
            with self.assertRaisesRegex(RuntimeError, "crash after runtime delete"):
                crashing.delete(
                    spec.id,
                    generation=4,
                    operation_id="delete-4",
                )

            deleting = SandboxStore(path).load()[spec.id]
            self.assertEqual(deleting.state, "deleting")
            self.assertEqual(deleting.delete_operation_id, "delete-4")
            restarted = SandboxManager(SandboxStore(path), runtime)
            with self.assertRaisesRegex(SandboxConflictError, "being deleted"):
                restarted.create(spec, operation=create_operation)

            deleted, _result = restarted.delete(
                spec.id,
                generation=4,
                operation_id="delete-4",
            )
            final_state = SandboxStore(path).load_state()

            self.assertIsNotNone(deleted)
            self.assertNotIn(spec.id, final_state.records)
            self.assertEqual(final_state.tombstones[spec.id].generation, 4)
            self.assertEqual(
                final_state.tombstones[spec.id].operation_id,
                "delete-4",
            )

    def test_multiprocess_delayed_create_cannot_cross_tombstone(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )
            spec = SandboxSpec(id="raced", image="busybox", memory_mb=128)
            manager.create(spec, operation=_create_operation(spec, 1, "create-1"))
            manager.delete(spec.id, generation=1, operation_id="delete-1")
            context = multiprocessing.get_context("spawn")
            start = context.Event()
            results = context.Queue()
            generations = [1, 1, 1, 2]
            processes = [
                context.Process(
                    target=_multiprocess_versioned_create,
                    args=(
                        str(path),
                        generation,
                        f"create-{generation}-{index}",
                        start,
                        results,
                    ),
                )
                for index, generation in enumerate(generations)
            ]
            for process in processes:
                process.start()
            start.set()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes.count("created:2"), 1)
            self.assertEqual(outcomes.count("stale:1"), 3)
            record = SandboxStore(path).load()[spec.id]
            self.assertEqual(record.generation, 2)

    def test_delete_treats_missing_container_as_idempotent_success(self) -> None:
        executor = RecordingExecutor(
            exit_code=1,
            stderr="Error response from daemon: No such container: missing",
        )
        runtime = DockerGvisorRuntime(executor=executor)

        result = runtime.delete("missing")

        self.assertEqual(result.exit_code, 0)

    def test_delete_surfaces_transient_runtime_failure(self) -> None:
        executor = RecordingExecutor(exit_code=1, stderr="daemon unavailable")
        runtime = DockerGvisorRuntime(executor=executor)

        with self.assertRaisesRegex(RuntimeError, "daemon unavailable"):
            runtime.delete("sandbox")

    def test_delete_preempts_active_exec_and_file_activity(self) -> None:
        from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec

        class BlockingFileRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(dry_run=True)
                self.file_started = Event()
                self.release_file = Event()

            def write_file_to_container(
                self,
                sandbox_id: str,
                container_path: str,
                content: bytes,
                *,
                owner: str | None = None,
            ) -> CommandResult:
                self.file_started.set()
                self.release_file.wait(2)
                return super().write_file_to_container(
                    sandbox_id,
                    container_path,
                    content,
                    owner=owner,
                )

        with TemporaryDirectory() as raw_dir:
            runtime = BlockingFileRuntime()
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                runtime,
            )
            manager.create(
                SandboxSpec(id="terminate-me", image="busybox", memory_mb=128)
            )
            sessions = ExecSessionManager(manager)
            session = sessions.start(
                SandboxExecSpec(
                    sandbox_id="terminate-me",
                    command=("cat",),
                    stdin=True,
                )
            )
            file_errors: list[BaseException] = []

            def upload() -> None:
                try:
                    manager.upload_file(
                        "terminate-me",
                        "/workspace/active",
                        b"data",
                    )
                except BaseException as exc:
                    file_errors.append(exc)

            file_thread = Thread(target=upload)
            file_thread.start()
            self.assertTrue(runtime.file_started.wait(1))

            deleted, result = manager.delete("terminate-me")

            self.assertIsNotNone(deleted)
            self.assertEqual(result.exit_code, 0)
            self.assertNotIn("terminate-me", manager.store.load())
            sessions.close_stdin(session.id)
            runtime.release_file.set()
            file_thread.join(timeout=1)

        self.assertFalse(file_thread.is_alive())
        self.assertEqual(file_errors, [])

    def test_delete_closes_new_activity_admission_while_runtime_removes(self) -> None:
        class BlockingDeleteRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(dry_run=True)
                self.delete_started = Event()
                self.release_delete = Event()

            def delete(self, sandbox_id: str) -> CommandResult:
                self.delete_started.set()
                self.release_delete.wait(2)
                return super().delete(sandbox_id)

        with TemporaryDirectory() as raw_dir:
            runtime = BlockingDeleteRuntime()
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                runtime,
            )
            manager.create(
                SandboxSpec(id="terminate-me", image="busybox", memory_mb=128)
            )
            manager.lifecycle.acquire_shared("terminate-me")
            errors: list[BaseException] = []

            def delete() -> None:
                try:
                    manager.delete("terminate-me")
                except BaseException as exc:
                    errors.append(exc)

            delete_thread = Thread(target=delete)
            delete_thread.start()
            self.assertTrue(runtime.delete_started.wait(1))
            with self.assertRaises(SandboxBusyError):
                manager.lifecycle.acquire_shared("terminate-me")
            runtime.release_delete.set()
            delete_thread.join(timeout=1)
            manager.lifecycle.release_shared("terminate-me")

        self.assertFalse(delete_thread.is_alive())
        self.assertEqual(errors, [])

    def test_activity_snapshot_uses_one_store_load(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = CountingSandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            manager.create(SandboxSpec(id="one", image="busybox", memory_mb=128))
            store.load_count = 0

            snapshot = manager.activity_snapshot()

            self.assertEqual(store.load_count, 1)
            self.assertEqual(snapshot.used_resources.memory_mb, 0)
            self.assertEqual(snapshot.reserved_resources.memory_mb, 128)
            self.assertEqual([record.spec.id for record in snapshot.records], ["one"])
            self.assertEqual(snapshot.activity_revision, 1)

    def test_capacity_admission_counts_planned_records_and_preserves_replay(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(
                store,
                DockerGvisorRuntime(dry_run=True),
                effective_capacity=ResourceQuantity(memory_mb=128),
            )
            spec = SandboxSpec(id="fills-node", image="busybox", memory_mb=128)

            created, _result = manager.create(spec)
            replayed, _result = manager.create(spec)
            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "exhausted memory_mb",
            ):
                manager.create(
                    SandboxSpec(id="over-capacity", image="busybox", memory_mb=1)
                )

            self.assertEqual(replayed, created)
            records, revision = store.load_with_revision()
            self.assertEqual(list(records), ["fills-node"])
            self.assertEqual(revision, 1)

    def test_zero_capacity_dimensions_remain_unbounded(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                effective_capacity=ResourceQuantity(),
            )

            manager.create(
                SandboxSpec(
                    id="legacy-unbounded",
                    image="busybox",
                    cpus=32.0,
                    memory_mb=1_000_000,
                    disk_mb=1_000_000,
                )
            )

    def test_store_revision_is_persisted_and_monotonic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            store = SandboxStore(path)
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))

            self.assertEqual(store.load_with_revision(), ({}, 0))
            manager.create(SandboxSpec(id="one", image="busybox", memory_mb=128))
            _records, first_revision = store.load_with_revision()
            store.delete("missing")
            records, second_revision = store.load_with_revision()

            self.assertEqual(list(records), ["one"])
            self.assertEqual(first_revision, 1)
            self.assertEqual(second_revision, 2)
            self.assertEqual(json.loads(path.read_text())["revision"], 2)

    def test_multiprocess_creates_do_not_lose_updates_or_ssh_ports(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            context = multiprocessing.get_context("spawn")
            process_count = 3
            records_per_process = 5
            processes = [
                context.Process(
                    target=_multiprocess_sandbox_writer,
                    args=(str(path), worker, records_per_process),
                )
                for worker in range(process_count)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=15)

            self.assertEqual([process.exitcode for process in processes], [0, 0, 0])
            records, revision = SandboxStore(path).load_with_revision()
            ports = [record.spec.ssh.host_port for record in records.values()]
            expected = process_count * records_per_process
            self.assertEqual(len(records), expected)
            self.assertEqual(revision, expected)
            self.assertEqual(len(set(ports)), expected)
            self.assertEqual(
                list(path.parent.glob(f".{path.name}.*.tmp")),
                [],
            )

    def test_multiprocess_capacity_admission_allows_only_one_winner(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            context = multiprocessing.get_context("spawn")
            start = context.Event()
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_capacity_create,
                    args=(str(path), index, start, results),
                )
                for index in range(2)
            ]
            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(timeout=15)

            self.assertEqual([process.exitcode for process in processes], [0, 0])
            outcomes = sorted(results.get(timeout=2) for _process in processes)
            self.assertEqual(outcomes, ["capacity", "created"])
            records, revision = SandboxStore(path).load_with_revision()
            self.assertEqual(len(records), 1)
            self.assertEqual(revision, 1)

    def test_concurrent_ssh_creates_allocate_distinct_ports(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                SlowCreateRuntime(),
                ssh_port_range=(23000, 23001),
            )
            barrier = Barrier(2)
            results: list[int] = []
            errors: list[BaseException] = []
            result_lock = Lock()

            def create(sandbox_id: str) -> None:
                try:
                    barrier.wait()
                    record, _result = manager.create(
                        SandboxSpec.from_dict(
                            {
                                "id": sandbox_id,
                                "image": "sandbox-ssh:latest",
                                "memory_mb": 128,
                                "network": "bridge",
                                "ssh": True,
                            }
                        )
                    )
                    assert record.spec.ssh.host_port is not None
                    with result_lock:
                        results.append(record.spec.ssh.host_port)
                except BaseException as exc:
                    with result_lock:
                        errors.append(exc)

            threads = [
                Thread(target=create, args=(f"ssh-{index}",)) for index in range(2)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

            self.assertEqual(errors, [])
            self.assertEqual(sorted(results), [23000, 23001])

    def test_builds_docker_gvisor_run_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
        spec = SandboxSpec(
            id="abc-123",
            image="python:3.12-slim",
            command=("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            memory_mb=512,
            cpus=1.5,
            disk_mb=2048,
            labels={"purpose": "test"},
        )

        argv = runtime.create_command(spec)

        self.assertEqual(
            argv[:7],
            (
                "docker",
                "run",
                "-d",
                "--name",
                "ucloud-sandbox-abc-123",
                "--runtime",
                "runsc",
            ),
        )
        self.assertIn("--network", argv)
        self.assertIn("none", argv)
        self.assertIn("--memory", argv)
        self.assertIn("512m", argv)
        self.assertIn("--cpus", argv)
        self.assertIn("1.5", argv)
        self.assertIn("--storage-opt", argv)
        self.assertIn("size=2048m", argv)
        self.assertIn("--init", argv)
        self.assertIn("--user", argv)
        self.assertIn("1000:1000", argv)
        self.assertIn("--security-opt", argv)
        self.assertIn("no-new-privileges", argv)
        self.assertIn("--cap-drop", argv)
        self.assertIn("ALL", argv)
        self.assertIn("--pids-limit", argv)
        self.assertIn("256", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("-e", argv)
        self.assertIn("A=1", argv)
        self.assertIn("B=2", argv)
        self.assertEqual(argv[-4:], ("python:3.12-slim", "python", "-c", "print('ok')"))

    def test_disk_request_requires_validated_storage_quota_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="disk",
            image="busybox",
            disk_mb=2048,
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_tmpfs_workspace_requires_validated_runtime_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_can_request_tmpfs_workspace_on_validated_runtime(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_tmpfs_workspace=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--storage-opt", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/workspace:rw,nosuid,nodev,size=2048m", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("--workdir", argv)
        self.assertIn("/workspace", argv)

    def test_compatibility_security_profile_can_opt_out_of_hardening(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="compat",
            image="busybox",
            memory_mb=128,
            security=SandboxSecuritySpec(
                user=None,
                cap_drop=(),
                no_new_privileges=False,
                pids_limit=None,
                init=False,
            ),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--user", argv)
        self.assertNotIn("--security-opt", argv)
        self.assertNotIn("--cap-drop", argv)
        self.assertNotIn("--pids-limit", argv)
        self.assertNotIn("--init", argv)

    def test_linux_host_profile_uses_vm_like_entrypoint_and_defaults(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
        spec = SandboxSpec.from_dict(
            {
                "id": "linux-host",
                "image": "ubuntu:24.04",
                "memory_mb": 512,
                "disk_mb": 2048,
                "profile": "linux_host",
                "network": "bridge",
                "command": ["sleep", "infinity"],
                "ssh": {
                    "enabled": True,
                    "host_port": 23000,
                    "authorized_keys": ["ssh-ed25519 AAAA test"],
                },
                "linux_host": {"enable_cron": True},
            }
        )

        argv = runtime.create_command(spec)

        self.assertIsNone(spec.security.user)
        self.assertEqual(spec.security.cap_drop, ())
        self.assertFalse(spec.security.no_new_privileges)
        self.assertIsNone(spec.security.pids_limit)
        self.assertIn("--init", argv)
        self.assertNotIn("--user", argv)
        self.assertNotIn("--cap-drop", argv)
        self.assertNotIn("--security-opt", argv)
        self.assertNotIn("--pids-limit", argv)
        self.assertIn("UCLOUD_SANDBOX_PROFILE=linux_host", argv)
        self.assertIn("UCLOUD_SANDBOX_ENABLE_CRON=1", argv)
        self.assertIn("UCLOUD_SANDBOX_ENABLE_SSHD=1", argv)
        self.assertIn("UCLOUD_SANDBOX_SSH_PORT=22", argv)
        paths_env = next(
            item for item in argv if item.startswith("UCLOUD_SANDBOX_LINUX_HOST_PATHS=")
        )
        self.assertIn("/var/spool/cron", paths_env)
        self.assertIn("--entrypoint", argv)
        self.assertIn("/bin/sh", argv)
        image_index = argv.index("ubuntu:24.04")
        self.assertEqual(argv[image_index + 1], "-lc")
        script = argv[image_index + 2]
        self.assertIn("/usr/local/bin/service", script)
        self.assertIn("ssh-keygen -A", script)
        self.assertEqual(argv[-2:], ("sleep", "infinity"))

    def test_linux_host_profile_round_trips_from_dict(self) -> None:
        spec = SandboxSpec.from_dict(
            {
                "id": "linux-host",
                "image": "ubuntu:24.04",
                "memory_mb": 512,
                "profile": "linux_host",
                "linux_host": {
                    "enable_cron": True,
                    "enable_sshd": True,
                    "keep_alive": False,
                    "writable_paths": ["/tests", "/logs/verifier"],
                },
            }
        )

        raw = spec.to_dict()
        round_tripped = SandboxSpec.from_dict(raw)

        self.assertEqual(raw["profile"], "linux_host")
        self.assertEqual(
            raw["linux_host"]["writable_paths"], ["/tests", "/logs/verifier"]
        )
        self.assertTrue(round_tripped.linux_host.enable_cron)
        self.assertTrue(round_tripped.linux_host.enable_sshd)
        self.assertFalse(round_tripped.linux_host.keep_alive)
        self.assertEqual(
            round_tripped.linux_host.writable_paths,
            ("/tests", "/logs/verifier"),
        )

    def test_rejects_unknown_sandbox_profile(self) -> None:
        spec = SandboxSpec(
            id="bad-profile",
            image="busybox",
            profile="vm",
            memory_mb=128,
        )

        with self.assertRaisesRegex(ValueError, "profile must be one of"):
            spec.validate()

    def test_rejects_invalid_sandbox_id(self) -> None:
        with self.assertRaises(ValueError):
            SandboxSpec(id="../bad", image="busybox").validate()

    def test_forkable_sandbox_requires_bounded_memory_disk_and_agent_protocol(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "memory_mb"):
            SandboxSpec(
                id="unbounded-fork",
                image="busybox",
                cpus=1,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            ).validate()

        with self.assertRaisesRegex(ValueError, "disk_mb"):
            SandboxSpec(
                id="unbounded-fork-storage",
                image="busybox",
                memory_mb=128,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            ).validate()

        with self.assertRaisesRegex(ValueError, "fork_protocol"):
            SandboxSpec(
                id="unsafe-fork",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
            ).validate()

        SandboxSpec(
            id="safe-fork",
            image="busybox",
            memory_mb=128,
            disk_mb=1024,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        ).validate()

        with self.assertRaisesRegex(ValueError, "cannot expose SSH"):
            SandboxSpec(
                id="fork-with-ssh",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                network="bridge",
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
                ssh=SandboxSshSpec(enabled=True, host_port=22022),
            ).validate()

    def test_rejects_user_labels_reserved_for_runtime_identity(self) -> None:
        spec = SandboxSpec(
            id="forged-label",
            image="busybox",
            memory_mb=128,
            labels={"UCLOUD-SANDBOXES.managed": "false"},
        )

        with self.assertRaisesRegex(ValueError, "reserved.*ucloud-sandboxes"):
            spec.validate()

    def test_rejects_missing_resource_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "resources are required"):
            SandboxSpec(id="no-resources", image="busybox").validate()

    def test_manager_records_planned_sandbox_in_dry_run_mode(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            spec = SandboxSpec(
                id="one",
                image="busybox",
                command=("true",),
                memory_mb=128,
            )

            record, result = manager.create(spec)

            self.assertEqual(record.state, "planned")
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(executor.commands, [])
            self.assertEqual(len(manager.list()), 1)

    def test_manager_create_is_idempotent_for_same_spec(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(
                executor=executor, allow_storage_opt_quota=True
            )
            manager = SandboxManager(store, runtime)
            spec = SandboxSpec(
                id="same",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
                labels={"sample": "one"},
            )

            first, _first_result = manager.create(spec)
            second, second_result, timings = manager.create_with_timings(spec)

            self.assertEqual(first.spec.id, second.spec.id)
            self.assertEqual(second_result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "store")
            self.assertEqual(len(executor.commands), 1)

    def test_manager_create_conflicts_for_same_id_different_spec(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="same",
                    image="busybox",
                    cpus=1.0,
                    memory_mb=128,
                    disk_mb=512,
                )
            )

            with self.assertRaises(SandboxConflictError):
                manager.create(
                    SandboxSpec(
                        id="same",
                        image="python:3.12-slim",
                        cpus=1.0,
                        memory_mb=128,
                        disk_mb=512,
                    )
                )

    def test_manager_recovers_managed_container_after_conflict_without_store_record(
        self,
    ) -> None:
        class ConflictExecutor:
            def __init__(self, spec: SandboxSpec) -> None:
                self.spec = spec
                self.commands = []

            def run(self, argv, *, input=None):
                self.commands.append(argv)
                if len(argv) > 1 and argv[1] == "run":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr=(
                            "Conflict. The container name "
                            '"/ucloud-sandbox-recovered" is already in use'
                        ),
                    )
                labels = {
                    "ucloud-sandboxes.managed": "true",
                    "ucloud-sandboxes.sandbox-id": self.spec.id,
                    "ucloud-sandboxes.spec-sha256": sandbox_spec_fingerprint(self.spec),
                }
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=__import__("json").dumps(labels),
                )

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            spec = SandboxSpec(
                id="recovered",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
            )
            executor = ConflictExecutor(spec)
            runtime = DockerGvisorRuntime(
                executor=executor, allow_storage_opt_quota=True
            )
            manager = SandboxManager(store, runtime)

            record, result, timings = manager.create_with_timings(spec)

            self.assertEqual(record.spec.id, "recovered")
            self.assertEqual(result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "container")
            self.assertEqual(store.load()["recovered"].spec.id, "recovered")

    def test_manager_recovers_container_with_legacy_default_profile_fingerprint(
        self,
    ) -> None:
        class LegacyFingerprintConflictExecutor:
            def __init__(self, spec: SandboxSpec) -> None:
                raw = spec.to_dict()
                raw.pop("profile", None)
                raw.pop("linux_host", None)
                self.legacy_fingerprint = hashlib.sha256(
                    json.dumps(raw, sort_keys=True, separators=(",", ":")).encode(
                        "utf-8"
                    )
                ).hexdigest()
                self.commands = []

            def run(self, argv, *, input=None):
                self.commands.append(argv)
                if len(argv) > 1 and argv[1] == "run":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr=(
                            "Conflict. The container name "
                            '"/ucloud-sandbox-legacy" is already in use'
                        ),
                    )
                labels = {
                    "ucloud-sandboxes.managed": "true",
                    "ucloud-sandboxes.sandbox-id": "legacy",
                    "ucloud-sandboxes.spec-sha256": self.legacy_fingerprint,
                }
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=json.dumps(labels),
                )

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            spec = SandboxSpec(
                id="legacy",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
            )
            executor = LegacyFingerprintConflictExecutor(spec)
            runtime = DockerGvisorRuntime(
                executor=executor, allow_storage_opt_quota=True
            )
            manager = SandboxManager(store, runtime)

            record, _result, timings = manager.create_with_timings(spec)

        self.assertEqual(record.spec.id, "legacy")
        self.assertTrue(timings["idempotent"])
        self.assertEqual(timings["recovered"], "container")

    def test_manager_sums_requested_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="one",
                    image="busybox",
                    cpus=0.5,
                    memory_mb=256,
                    disk_mb=1024,
                )
            )
            manager.create(
                SandboxSpec(
                    id="two",
                    image="busybox",
                    cpus=1.0,
                    memory_mb=512,
                    disk_mb=2048,
                )
            )

            resources = manager.requested_resources()

            self.assertEqual(resources.vcpu, 1.5)
            self.assertEqual(resources.memory_mb, 768)
            self.assertEqual(resources.disk_mb, 3072)

    def test_manager_cleans_up_expired_sandboxes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="short",
                    image="busybox",
                    ttl_seconds=1,
                    memory_mb=128,
                )
            )

            expired = manager.cleanup_expired()

            self.assertEqual(expired, [])
            records = store.load()
            record = records["short"]
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0)
            )
            self.assertEqual(expired, [])
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0) + timedelta(seconds=2)
            )

            self.assertEqual([record.spec.id for record in expired], ["short"])
            self.assertEqual(store.load(), {})

    def test_expiration_preempts_active_exec(self) -> None:
        from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="expired-active",
                    image="busybox",
                    ttl_seconds=1,
                    memory_mb=128,
                )
            )
            sessions = ExecSessionManager(manager)
            session = sessions.start(
                SandboxExecSpec(
                    sandbox_id="expired-active",
                    command=("cat",),
                    stdin=True,
                )
            )
            record = store.load()["expired-active"]

            expired = manager.cleanup_expired(
                now=record.created_at + timedelta(seconds=2)
            )

            self.assertEqual(
                [expired_record.spec.id for expired_record in expired],
                ["expired-active"],
            )
            self.assertEqual(store.load(), {})
            sessions.close_stdin(session.id)

    def test_ssh_enabled_sandbox_gets_port_and_publish_flag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True)
            manager = SandboxManager(
                store,
                runtime,
                ssh_port_range=(23000, 23001),
            )
            spec = SandboxSpec.from_dict(
                {
                    "id": "ssh-one",
                    "image": "sandbox-ssh:latest",
                    "memory_mb": 128,
                    "network": "bridge",
                    "ssh": {
                        "enabled": True,
                        "user": "sandbox",
                        "authorized_keys": ["ssh-ed25519 AAAA test"],
                    },
                }
            )

            record, result = manager.create(spec)

            self.assertEqual(record.spec.ssh.host_port, 23000)
            self.assertIn("-p", result.argv)
            self.assertIn("127.0.0.1:23000:22", result.argv)
            self.assertEqual(
                record.to_dict()["ssh"]["command"],
                "ssh -p 23000 sandbox@127.0.0.1",
            )

    def test_ssh_requires_bridge_network(self) -> None:
        spec = SandboxSpec.from_dict(
            {
                "id": "bad-ssh",
                "image": "sandbox-ssh:latest",
                "memory_mb": 128,
                "ssh": {"enabled": True, "host_port": 23000},
            }
        )

        with self.assertRaises(ValueError):
            spec.validate()

    def test_builds_docker_exec_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        argv = runtime.exec_command(
            "abc-123",
            ("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            working_dir="/workspace",
            interactive=True,
        )

        self.assertEqual(
            argv,
            (
                "docker",
                "exec",
                "-i",
                "-w",
                "/workspace",
                "-e",
                "A=1",
                "-e",
                "B=2",
                "ucloud-sandbox-abc-123",
                "python",
                "-c",
                "print('ok')",
            ),
        )

    def test_builds_docker_file_copy_commands(self) -> None:
        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            target = Path(raw_dir) / "download.txt"
            source.write_bytes(b"hello")
            runtime = DockerGvisorRuntime(dry_run=True)

            upload = runtime.copy_to_container(
                "abc-123", source, "/workspace/payload.txt"
            )
            download = runtime.copy_from_container(
                "abc-123",
                "/workspace/payload.txt",
                target,
            )

        self.assertEqual(
            upload.argv,
            (
                "docker",
                "cp",
                str(source),
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
            ),
        )
        self.assertEqual(
            download.argv,
            (
                "docker",
                "cp",
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
                str(target),
            ),
        )

    def test_streams_file_upload_and_download_through_exec(self) -> None:
        executor = RecordingExecutor(stdout_bytes=b"downloaded bytes\n")
        runtime = DockerGvisorRuntime(executor=executor)

        upload = runtime.write_file_to_container(
            "abc-123",
            "/workspace/payload.txt",
            b"uploaded bytes\n",
            owner="1000:1000",
        )
        content, download = runtime.read_file_from_container(
            "abc-123",
            "/workspace/payload.txt",
        )

        self.assertEqual(executor.inputs[0], b"uploaded bytes\n")
        self.assertIsNone(executor.inputs[1])
        self.assertEqual(content, b"downloaded bytes\n")
        self.assertEqual(
            upload.argv[:9],
            (
                "docker",
                "exec",
                "-i",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-e",
                "UCLOUD_SANDBOX_OWNER=1000:1000",
                "-u",
                "0",
            ),
        )
        self.assertEqual(
            download.argv[:6],
            (
                "docker",
                "exec",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-u",
                "0",
            ),
        )

    def test_file_download_preserves_exact_limit_and_rejects_limit_plus_one(
        self,
    ) -> None:
        exact = b"\x00\xffbinary"
        runtime = DockerGvisorRuntime(executor=RecordingExecutor(stdout_bytes=exact))

        content, _ = runtime.read_file_from_container(
            "abc-123",
            "/workspace/payload.bin",
            max_bytes=len(exact),
        )

        self.assertEqual(content, exact)
        oversized = DockerGvisorRuntime(
            executor=RecordingExecutor(stdout_bytes=exact + b"!")
        )
        with self.assertRaisesRegex(SandboxFileTooLargeError, "download limit"):
            oversized.read_file_from_container(
                "abc-123",
                "/workspace/payload.bin",
                max_bytes=len(exact),
            )

    def test_bounded_command_output_does_not_retain_unbounded_diagnostics(self) -> None:
        from ucloud_sandboxes.sandbox import SubprocessExecutor

        result = SubprocessExecutor().run_bounded_stdout(
            (
                sys.executable,
                "-c",
                "import os; os.write(2, b'e' * 100000); os.write(1, b'x' * 100000)",
            ),
            max_stdout_bytes=8,
            max_stderr_bytes=32,
        )

        self.assertEqual(result.stdout_bytes, b"x" * 9)
        self.assertEqual(result.stderr_bytes, b"e" * 32)

    def test_container_file_copy_rejects_directory_paths(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            source.write_bytes(b"hello")
            with self.assertRaises(ValueError):
                runtime.copy_to_container("abc-123", source, "/workspace/")

    def test_startup_reconciles_only_proven_unreferenced_checkpoint_state(self) -> None:
        class InventoryRuntime(DockerGvisorRuntime):
            def __init__(self, inventory: dict[str, object]) -> None:
                super().__init__(
                    checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
                    fork_enabled=True,
                    allow_storage_opt_quota=True,
                    dry_run=False,
                )
                self.inventory = inventory
                self.removed_artifacts: list[str] = []
                self.removed_staged: list[tuple[str, str]] = []
                self.removed_applications: list[str] = []

            def checkpoint_helper_inventory(self) -> dict[str, object]:
                return self.inventory

            def cleanup_staged_checkpoint(
                self, target_container_id: str, checkpoint_id: str
            ) -> CommandResult:
                self.removed_staged.append((target_container_id, checkpoint_id))
                return CommandResult(argv=(), exit_code=0)

            def release_checkpoint(self, checkpoint_id: str) -> CommandResult:
                self.removed_artifacts.append(checkpoint_id)
                return CommandResult(argv=(), exit_code=0)

            def drop_application_checkpoint_id(
                self, application_id: str
            ) -> CommandResult:
                self.removed_applications.append(application_id)
                return CommandResult(argv=(), exit_code=0)

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            setup_runtime = DockerGvisorRuntime(
                dry_run=True,
                fork_enabled=True,
                allow_storage_opt_quota=True,
                checkpoint_root=Path("/var/lib/docker/ucloud-checkpoints"),
            )
            setup = SandboxManager(store, setup_runtime)
            spec = SandboxSpec(
                id="restoring-child",
                image="busybox",
                memory_mb=128,
                disk_mb=1024,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            operation = _create_operation(spec, 3, "restore-child-3")
            record, _result = setup.create(spec, operation=operation)
            store.upsert(
                replace(
                    record,
                    state="restoring",
                    creation_kind="restore",
                    source_sandbox_id="source",
                    source_generation=2,
                    checkpoint_id="active-checkpoint",
                    fork_nonce=FORK_NONCE,
                )
            )
            active_application = application_checkpoint_id(
                spec.id, operation.generation, operation.spec_hash
            )
            target_container_id = "b" * 64
            inventory: dict[str, object] = {
                "version": 1,
                "artifacts": [
                    {"artifact_id": "active-checkpoint", "state": "pending"},
                    {"artifact_id": "completed-checkpoint", "state": "sealed"},
                ],
                "applications": [active_application, "orphan-application"],
                "staged": [
                    {
                        "artifact_id": "completed-checkpoint",
                        "target_container_id": target_container_id,
                        "checkpoint_id": "state",
                    }
                ],
            }
            runtime = InventoryRuntime(inventory)
            manager = SandboxManager(store, runtime)

            counters = manager.reconcile_checkpoint_storage()

            self.assertEqual(runtime.removed_artifacts, ["completed-checkpoint"])
            self.assertEqual(runtime.removed_staged, [(target_container_id, "state")])
            self.assertEqual(runtime.removed_applications, ["orphan-application"])
            self.assertEqual(counters["pending_retained"], 0)

            runtime.inventory = {
                "version": 1,
                "artifacts": [
                    {"artifact_id": "orphan-pending", "state": "pending"},
                    {"artifact_id": "also-sealed", "state": "sealed"},
                ],
                "applications": [active_application, "second-orphan-app"],
                "staged": [
                    {
                        "artifact_id": "also-sealed",
                        "target_container_id": "c" * 64,
                        "checkpoint_id": "state",
                    }
                ],
            }
            cleanup_before = (
                list(runtime.removed_artifacts),
                list(runtime.removed_staged),
                list(runtime.removed_applications),
            )
            with self.assertRaisesRegex(RuntimeError, "operator reconciliation"):
                manager.reconcile_checkpoint_storage()
            self.assertEqual(
                (
                    runtime.removed_artifacts,
                    runtime.removed_staged,
                    runtime.removed_applications,
                ),
                cleanup_before,
            )


class CountingSandboxStore(SandboxStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.load_count = 0

    def load_state(self):
        self.load_count += 1
        return super().load_state()


class SlowCreateRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    def create(
        self,
        spec: SandboxSpec,
        operation=None,
    ) -> CommandResult:
        time.sleep(0.05)
        return super().create(spec, operation=operation)


class CrashRecoveryExecutor:
    def __init__(self, sandbox_id: str, operation: SandboxOperation) -> None:
        self.sandbox_id = sandbox_id
        self.operation = operation
        self.create_calls = 0

    def run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult:
        del input
        if len(argv) > 1 and argv[1] == "run":
            self.create_calls += 1
            if self.create_calls == 1:
                return CommandResult(argv=argv, exit_code=0)
            return CommandResult(
                argv=argv,
                exit_code=1,
                stderr=(
                    "Conflict. The container name is already in use by container name"
                ),
            )
        if len(argv) > 1 and argv[1] == "inspect":
            return CommandResult(
                argv=argv,
                exit_code=0,
                stdout=json.dumps(
                    {
                        "ucloud-sandboxes.managed": "true",
                        "ucloud-sandboxes.sandbox-id": self.sandbox_id,
                        SANDBOX_GENERATION_LABEL: str(self.operation.generation),
                        SANDBOX_OPERATION_ID_LABEL: self.operation.operation_id,
                        SANDBOX_SPEC_HASH_LABEL: self.operation.spec_hash,
                    }
                ),
            )
        return CommandResult(argv=argv, exit_code=0)


def _create_operation(
    spec: SandboxSpec,
    generation: int,
    operation_id: str,
) -> SandboxOperation:
    return SandboxOperation(
        operation_id=operation_id,
        generation=generation,
        kind="create",
        spec_hash=sandbox_spec_fingerprint(spec),
    )


def _multiprocess_sandbox_writer(path: str, worker: int, count: int) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
        ssh_port_range=(24000, 24999),
    )
    for index in range(count):
        manager.create(
            SandboxSpec.from_dict(
                {
                    "id": f"worker-{worker}-{index}",
                    "image": "busybox",
                    "memory_mb": 64,
                    "network": "bridge",
                    "ssh": True,
                }
            )
        )


def _multiprocess_versioned_create(
    path: str,
    generation: int,
    operation_id: str,
    start,
    results,
) -> None:
    spec = SandboxSpec(id="raced", image="busybox", memory_mb=128)
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
    )
    start.wait(10)
    try:
        record, _result = manager.create(
            spec,
            operation=_create_operation(spec, generation, operation_id),
        )
    except SandboxStaleOperationError:
        results.put(f"stale:{generation}")
    except SandboxConflictError:
        results.put(f"conflict:{generation}")
    else:
        results.put(f"created:{record.generation}")


def _multiprocess_create_after_drain(path: str, index: int, results) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
    )
    try:
        manager.create(
            SandboxSpec(
                id=f"blocked-{index}",
                image="busybox",
                memory_mb=64,
            )
        )
    except SandboxAdmissionClosedError:
        results.put("closed")
    else:
        results.put("created")


def _multiprocess_capacity_create(path: str, index: int, start, results) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
        effective_capacity=ResourceQuantity(memory_mb=128),
    )
    start.wait(10)
    try:
        manager.create(
            SandboxSpec(
                id=f"capacity-{index}",
                image="busybox",
                memory_mb=128,
            )
        )
    except SandboxCapacityUnavailableError:
        results.put("capacity")
    else:
        results.put("created")


if __name__ == "__main__":
    unittest.main()
