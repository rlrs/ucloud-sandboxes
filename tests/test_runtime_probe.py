from threading import Lock
import json
import time
import unittest

from ucloud_sandboxes.capabilities import GVISOR_LIVE_FORK_PROBE
from ucloud_sandboxes.runtime_probe import (
    DockerRuntimeProbe,
    _LIVE_FORK_PROCESS_LOCK,
)
from ucloud_sandboxes.sandbox import CommandResult


class FakeExecutor:
    def __init__(
        self,
        *,
        storage_failure: str = "no_space",
        tmpfs_failure: str = "no_space",
    ) -> None:
        self.storage_failure = storage_failure
        self.tmpfs_failure = tmpfs_failure

    def run(self, argv: tuple[str, ...]) -> CommandResult:
        joined = " ".join(argv)
        if "uname -a" in joined:
            return CommandResult(argv, 0, "Linux test 4.19.0-gvisor\n", "")
        if "wget -T 2" in joined:
            return CommandResult(argv, 1, "", "Network is unreachable")
        if "cat /proc/meminfo" in joined:
            return CommandResult(argv, 0, "MemTotal:         131072 kB\n", "")
        if "mount -t tmpfs" in joined:
            return CommandResult(argv, 1, "", "permission denied")
        if joined.endswith(" id"):
            return CommandResult(argv, 0, "uid=1000 gid=1000 groups=1000\n", "")
        if "--storage-opt size=16m" in joined:
            if self.storage_failure == "permission":
                return CommandResult(
                    argv,
                    1,
                    "",
                    "dd: can't open '/ucloud-storage-probe': Permission denied",
                )
            return CommandResult(
                argv,
                1,
                "No space left on device\n",
                "dd: error writing '/ucloud-storage-probe': No space left on device",
            )
        if "tmpfs /tmp:rw,nosuid,nodev,size=16m" in joined:
            if self.tmpfs_failure == "permission":
                return CommandResult(
                    argv,
                    1,
                    "",
                    "dd: can't open '/tmp/ucloud-tmpfs-probe': Permission denied",
                )
            return CommandResult(
                argv,
                1,
                "No space left on device\n",
                "dd: error writing '/tmp/ucloud-tmpfs-probe': No space left on device",
            )
        return CommandResult(argv, 1, "", "unexpected command")


class LiveForkExecutor(FakeExecutor):
    source_id = "1" * 64
    child_id = "2" * 64
    image_id = "3" * 64
    sentinel = "01234567-89ab-cdef-0123-456789abcdef"

    def __init__(
        self,
        *,
        stage_failure: bool = False,
        safe_socket_migration: bool = True,
        source_socket_connected: bool = True,
        restored_socket_disconnected: bool = True,
        restored_spec_identity: bool = True,
        stale_staged_child: bool = False,
        stale_source_remove_failure: bool = False,
        cleanup_source_remove_failure: bool = False,
        distinct_bridge_identity: bool = True,
        restored_exec_origin_absent: bool = True,
        restored_in_child_bridge_identity: bool = True,
    ) -> None:
        super().__init__()
        self.stage_failure = stage_failure
        self.safe_socket_migration = safe_socket_migration
        self.source_socket_connected = source_socket_connected
        self.restored_socket_disconnected = restored_socket_disconnected
        self.restored_spec_identity = restored_spec_identity
        self.stale_staged_child = stale_staged_child
        self.stale_source_remove_failure = stale_source_remove_failure
        self.cleanup_source_remove_failure = cleanup_source_remove_failure
        self.distinct_bridge_identity = distinct_bridge_identity
        self.restored_exec_origin_absent = restored_exec_origin_absent
        self.restored_in_child_bridge_identity = restored_in_child_bridge_identity
        self.source_name = ""
        self.child_name = ""
        self.commands: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...]) -> CommandResult:
        self.commands.append(argv)
        if argv[:2] == ("docker", "info"):
            runtime_args = (
                [
                    "--allow-live-tcp-migration=false",
                    "--net-disconnect-ok=true",
                    "--allow-connected-on-save=false",
                ]
                if self.safe_socket_migration
                else []
            )
            return CommandResult(
                argv,
                0,
                json.dumps(
                    {
                        "runsc": {
                            "path": "/usr/bin/runsc",
                            "runtimeArgs": runtime_args,
                        },
                        "runsc-restore": {
                            "path": "/test/runsc-restore",
                            "runtimeArgs": runtime_args,
                        },
                    }
                ),
                "",
            )
        if argv[:2] == ("docker", "version"):
            return CommandResult(argv, 0, "29.6.1\n", "")
        if argv[:2] == ("/usr/bin/runsc", "--version"):
            return CommandResult(argv, 0, "runsc version release-test\n", "")
        if argv == ("/test/runsc-restore", "--ucloud-wrapper-version"):
            return CommandResult(argv, 0, "ucloud-runsc-restore 1\n", "")
        if argv[:2] == ("docker", "inspect"):
            if "peer" in argv[-1]:
                address = "172.17.0.4"
            elif "child" in argv[-1] and self.distinct_bridge_identity:
                address = "172.17.0.3"
            else:
                address = "172.17.0.2"
            return CommandResult(argv, 0, f"{address}\n", "")
        if argv[:2] == ("docker", "image"):
            return CommandResult(argv, 0, f"sha256:{self.image_id}\n", "")
        if argv[:2] == ("docker", "run") and "--detach" in argv:
            name = argv[argv.index("--name") + 1]
            if "peer" in name:
                return CommandResult(argv, 0, f"{'4' * 64}\n", "")
            self.source_name = name
            return CommandResult(argv, 0, f"{self.source_id}\n", "")
        if argv[:2] == ("docker", "create"):
            self.child_name = argv[argv.index("--name") + 1]
            return CommandResult(argv, 0, f"{self.child_id}\n", "")
        if argv[:2] == ("docker", "logs"):
            if argv[-1] == self.source_name:
                socket_state = "connected" if self.source_socket_connected else "failed"
                output = (
                    f"UCLOUD_FORK_SOCKET_READY={socket_state}\n"
                    "UCLOUD_FORK_CHECKPOINT_ARMED_1=true\n"
                    f"UCLOUD_FORK_READY={self.sentinel}\n"
                    "UCLOUD_FORK_CHECKPOINT_1=resume\n"
                    "UCLOUD_FORK_CHECKPOINT_ARMED_2=true\n"
                    "UCLOUD_FORK_CHECKPOINT_2=resume\n"
                )
            else:
                spec_id = (
                    self.child_name if self.restored_spec_identity else self.source_name
                )
                socket_state = (
                    "disconnected" if self.restored_socket_disconnected else "connected"
                )
                output = (
                    f"UCLOUD_FORK_INHERITED_ID={self.source_name}\n"
                    f"UCLOUD_FORK_SPEC_ID={spec_id}\n"
                    f"UCLOUD_FORK_SOCKET_RESTORED={socket_state}\n"
                    "UCLOUD_FORK_CHECKPOINT_1=restore\n"
                    f"UCLOUD_FORK_ROOTFS_RESTORED={self.sentinel}\n"
                    f"UCLOUD_FORK_TMPFS_RESTORED={self.sentinel}\n"
                    f"UCLOUD_FORK_RUN_TMPFS_RESTORED={self.sentinel}\n"
                    f"UCLOUD_FORK_RESTORED={self.sentinel}\n"
                )
            return CommandResult(argv, 0, output, "")
        if argv[:2] == ("docker", "exec") and any(
            item.startswith("UCLOUD_EXEC_TAG=") for item in argv
        ):
            if "UCLOUD_EXEC_ORIGIN=" in argv[-1]:
                container_name = argv[4]
                state = (
                    "absent"
                    if "child" in container_name and self.restored_exec_origin_absent
                    else "present"
                )
                return CommandResult(
                    argv,
                    0,
                    f"UCLOUD_EXEC_ORIGIN={state}\n",
                    "",
                )
            return CommandResult(argv, 0, "", "")
        if argv[:2] == ("docker", "exec") and argv[-2:] == ("hostname", "-i"):
            address = (
                "172.17.0.3" if self.restored_in_child_bridge_identity else "172.17.0.2"
            )
            return CommandResult(argv, 0, f"{address}\n", "")
        if argv[:2] == ("docker", "exec"):
            return CommandResult(argv, 0, "Linux test 4.19.0-gvisor\n", "")
        if argv[:2] == ("docker", "checkpoint"):
            return CommandResult(argv, 0, "", "")
        if argv[:2] in {
            ("docker", "start"),
            ("docker", "kill"),
            ("docker", "rm"),
        }:
            if (
                argv[:2] == ("docker", "rm")
                and argv[-1] == "ucloud-fork-source-gvisor-live-fork-v1"
                and (
                    self.stale_source_remove_failure
                    or (self.cleanup_source_remove_failure and self.source_name)
                )
            ):
                return CommandResult(argv, 1, "", "daemon still owns checkpoint")
            return CommandResult(argv, 0, "", "")
        if argv and argv[0] == "/test/checkpoint-helper":
            if argv[1] == "list":
                staged = (
                    [
                        {
                            "artifact_id": "runtime-conformance-gvisor-live-fork-v1",
                            "checkpoint_id": "runtime-conformance-v1",
                            "target_container_id": self.child_id,
                        }
                    ]
                    if self.stale_staged_child
                    else []
                )
                return CommandResult(argv, 0, json.dumps({"staged": staged}), "")
            if argv[1] == "stage" and self.stage_failure:
                return CommandResult(argv, 1, "", "stage failed")
            return CommandResult(argv, 0, "", "")
        return super().run(argv)


class RuntimeProbeTests(unittest.TestCase):
    def test_independent_runtime_probes_run_concurrently_after_first_pull(self) -> None:
        class SlowExecutor(FakeExecutor):
            def __init__(self) -> None:
                super().__init__()
                self._lock = Lock()
                self.active = 0
                self.max_active = 0

            def run(self, argv: tuple[str, ...]) -> CommandResult:
                if "uname -a" in " ".join(argv):
                    return super().run(argv)
                with self._lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                try:
                    time.sleep(0.02)
                    return super().run(argv)
                finally:
                    with self._lock:
                        self.active -= 1

        executor = SlowExecutor()

        report = DockerRuntimeProbe(executor=executor, execute=True).run()

        self.assertTrue(report.ok)
        self.assertGreater(executor.max_active, 1)

    def test_probe_reports_workspace_quota_success(self) -> None:
        report = DockerRuntimeProbe(executor=FakeExecutor(), execute=True).run()

        self.assertTrue(report.ok)
        results = {result.name: result for result in report.results}
        self.assertTrue(results["gvisor-kernel"].ok)
        self.assertTrue(results["network-none-blocks-outbound"].ok)
        self.assertTrue(results["memory-limit-visible"].ok)
        self.assertTrue(results["mount-blocked"].ok)
        self.assertTrue(results["non-root-supported"].ok)
        self.assertTrue(results["storage-opt-quota-enforced"].ok)
        self.assertTrue(results["tmpfs-quota-enforced"].ok)

    def test_storage_probe_rejects_unrelated_write_failures(self) -> None:
        report = DockerRuntimeProbe(
            executor=FakeExecutor(storage_failure="permission"),
            execute=True,
        ).run()

        results = {result.name: result for result in report.results}
        self.assertFalse(results["storage-opt-quota-enforced"].ok)
        self.assertFalse(report.ok)

    def test_tmpfs_probe_rejects_unrelated_write_failures(self) -> None:
        report = DockerRuntimeProbe(
            executor=FakeExecutor(tmpfs_failure="permission"),
            execute=True,
        ).run()

        results = {result.name: result for result in report.results}
        self.assertFalse(results["tmpfs-quota-enforced"].ok)
        self.assertFalse(report.ok)

    def test_probe_dry_run_marks_commands_skipped(self) -> None:
        report = DockerRuntimeProbe(executor=FakeExecutor(), execute=False).run()

        self.assertTrue(report.ok)
        self.assertTrue(all(result.skipped for result in report.results))

    def test_probe_can_prefix_docker_commands_with_sudo(self) -> None:
        report = DockerRuntimeProbe(
            executor=FakeExecutor(),
            execute=False,
            use_sudo=True,
        ).run()

        self.assertTrue(report.to_dict()["use_sudo"])
        self.assertEqual(report.results[0].command[:2], ("sudo", "docker"))

    def test_live_fork_probe_restores_memory_into_distinct_gvisor_child(self) -> None:
        executor = LiveForkExecutor()

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        results = {result.name: result for result in report.results}
        live_fork = results[GVISOR_LIVE_FORK_PROBE]
        self.assertTrue(report.ok)
        self.assertTrue(live_fork.ok)
        self.assertFalse(live_fork.required)
        live_container_commands = [
            command
            for command in executor.commands
            if command[:2] in {("docker", "run"), ("docker", "create")}
            and "--name" in command
            and "peer" not in command[command.index("--name") + 1]
        ]
        self.assertEqual(len(live_container_commands), 2)
        self.assertEqual(
            {
                command[command.index("--name") + 1]
                for command in live_container_commands
            },
            {
                "ucloud-fork-source-gvisor-live-fork-v1",
                "ucloud-fork-child-gvisor-live-fork-v1",
            },
        )
        self.assertTrue(
            all(
                f"sha256:{executor.image_id}" in command
                for command in live_container_commands
            )
        )
        for command in live_container_commands:
            self.assertIn("--annotation", command)
            self.assertEqual(command[command.index("--network") + 1], "bridge")
            self.assertIn("--init", command)
            self.assertEqual(command[command.index("--user") + 1], "1000:1000")
            self.assertIn("no-new-privileges", command)
            self.assertEqual(command[command.index("--cap-drop") + 1], "ALL")
            self.assertEqual(command[command.index("--pids-limit") + 1], "256")
            app_annotation = next(
                argument
                for argument in command
                if argument.startswith("dev.gvisor.internal.checkpoint.path=")
            )
            self.assertIn(
                app_annotation,
                {
                    "dev.gvisor.internal.checkpoint.path=/test/checkpoints/"
                    "application/probe-source-gvisor-live-fork-v1",
                    "dev.gvisor.internal.checkpoint.path=/test/checkpoints/"
                    "application/probe-child-gvisor-live-fork-v1",
                },
            )
            self.assertIn("dev.gvisor.internal.checkpoint.resume=true", command)
            name = command[command.index("--name") + 1]
            self.assertIn(f"UCLOUD_SANDBOX_ID={name}", command)
            self.assertIn("UCLOUD_FORK_PEER_IP=172.17.0.4", command)
            process = command[-1]
            self.assertIn('nc "$UCLOUD_FORK_PEER_IP" 45678', process)
            self.assertIn(":B26E 01 ", process)
            self.assertNotIn(":B26E .* 01 ", process)
            self.assertIn("/proc/gvisor/spec_environ", process)
        child_create = next(
            command
            for command in live_container_commands
            if command[:2] == ("docker", "create")
        )
        self.assertEqual(
            child_create[child_create.index("--runtime") + 1],
            "runsc-restore",
        )
        self.assertIn(
            "dev.ucloud.sandboxes.restore.checkpoint=runtime-conformance-v1",
            child_create,
        )
        prepare = next(
            command
            for command in executor.commands
            if command[:2] == ("/test/checkpoint-helper", "prepare")
        )
        self.assertIn(f"sha256:{executor.image_id}", prepare)
        self.assertEqual(prepare[-4:], ("128", "128", "64", "16"))
        self.assertTrue(
            all(
                command[command.index("--memory") + 1] == "128m"
                for command in live_container_commands
            )
        )
        checkpoints = [
            command
            for command in executor.commands
            if command[:3] == ("docker", "checkpoint", "create")
        ]
        self.assertEqual(len(checkpoints), 2)
        checkpoint = checkpoints[0]
        self.assertIn("--leave-running", checkpoint)
        checkpoint_dir = checkpoint[checkpoint.index("--checkpoint-dir") + 1]
        self.assertEqual(
            checkpoint_dir,
            "/test/checkpoints/runtime-conformance-gvisor-live-fork-v1/pending",
        )
        start = next(
            command
            for command in executor.commands
            if command[:2] == ("docker", "start")
        )
        self.assertEqual(
            start,
            ("docker", "start", "ucloud-fork-child-gvisor-live-fork-v1"),
        )
        bridge_inspects = [
            command
            for command in executor.commands
            if command[:2] == ("docker", "inspect")
            and ".NetworkSettings.Networks" in command[3]
        ]
        self.assertEqual(len(bridge_inspects), 3)
        peer_run = next(
            command
            for command in executor.commands
            if command[:2] == ("docker", "run")
            and "--name" in command
            and "peer" in command[command.index("--name") + 1]
        )
        self.assertNotIn("--runtime", peer_run)
        self.assertIn("nc -l -p 45678", peer_run[-1])
        self.assertIn(
            (
                "docker",
                "exec",
                "ucloud-fork-child-gvisor-live-fork-v1",
                "hostname",
                "-i",
            ),
            executor.commands,
        )
        exec_origin_commands = [
            command
            for command in executor.commands
            if command[:2] == ("docker", "exec")
            and any(item.startswith("UCLOUD_EXEC_TAG=") for item in command)
        ]
        self.assertEqual(len(exec_origin_commands), 4)
        self.assertEqual(
            sum(
                "UCLOUD_EXEC_ORIGIN=" in command[-1] for command in exec_origin_commands
            ),
            3,
        )
        tmpfs_mutation = next(
            command
            for command in executor.commands
            if command[:2] == ("docker", "exec")
            and "source-only >/tmp/ucloud-fork-tmpfs" in command[-1]
        )
        self.assertEqual(
            tmpfs_mutation[tmpfs_mutation.index("--user") + 1],
            "1000:1000",
        )
        helper_actions = [
            command[1]
            for command in executor.commands
            if command[0] == "/test/checkpoint-helper"
        ]
        self.assertEqual(
            helper_actions,
            [
                "list",
                "app-drop",
                "app-drop",
                "drop",
                "drop",
                "app-prepare",
                "prepare",
                "complete",
                "seal",
                "app-prepare",
                "stage",
                "prepare",
                "complete",
                "seal",
                "unstage",
                "app-drop",
                "app-drop",
                "drop",
                "drop",
            ],
        )
        self.assertRegex(live_fork.runtime_fingerprint, r"^[0-9a-f]{64}$")

    def test_live_fork_probe_precleans_interrupted_state_before_artifacts(
        self,
    ) -> None:
        executor = LiveForkExecutor(stale_staged_child=True)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertTrue(live_fork.ok)
        preclean = [
            (
                "docker",
                "rm",
                "--force",
                "--volumes",
                "ucloud-fork-child-gvisor-live-fork-v1",
            ),
            (
                "docker",
                "rm",
                "--force",
                "--volumes",
                "ucloud-fork-source-gvisor-live-fork-v1",
            ),
            (
                "docker",
                "rm",
                "--force",
                "--volumes",
                "ucloud-fork-peer-gvisor-live-fork-v1",
            ),
            ("/test/checkpoint-helper", "list"),
            (
                "/test/checkpoint-helper",
                "unstage",
                executor.child_id,
                "runtime-conformance-v1",
            ),
            (
                "/test/checkpoint-helper",
                "app-drop",
                "probe-child-gvisor-live-fork-v1",
            ),
            (
                "/test/checkpoint-helper",
                "app-drop",
                "probe-source-gvisor-live-fork-v1",
            ),
            (
                "/test/checkpoint-helper",
                "drop",
                "runtime-conformance-gvisor-live-fork-v1",
            ),
            (
                "/test/checkpoint-helper",
                "drop",
                "runtime-conformance-gvisor-live-fork-v1-repeat",
            ),
        ]
        preclean_start = executor.commands.index(preclean[0])
        self.assertEqual(
            executor.commands[preclean_start : preclean_start + len(preclean)],
            preclean,
        )
        first_create = next(
            index
            for index, command in enumerate(executor.commands)
            if command[:2] == ("docker", "run") and "--name" in command
        )
        self.assertLess(preclean_start + len(preclean) - 1, first_create)

    def test_concurrent_live_fork_probe_fails_closed_before_resetting_artifacts(
        self,
    ) -> None:
        executor = LiveForkExecutor()
        self.assertTrue(_LIVE_FORK_PROCESS_LOCK.acquire(blocking=False))
        try:
            report = DockerRuntimeProbe(
                executor=executor,
                execute=True,
                max_parallel_probes=1,
                probe_live_fork=True,
                checkpoint_helper="/test/checkpoint-helper",
                checkpoint_root="/test/checkpoints",
                live_fork_wait_seconds=0,
            ).run()
        finally:
            _LIVE_FORK_PROCESS_LOCK.release()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("already running", live_fork.stderr)
        self.assertFalse(
            any(
                command[:2] == ("/test/checkpoint-helper", "drop")
                for command in executor.commands
            )
        )
        self.assertFalse(
            any(
                "ucloud-fork-source-gvisor-live-fork-v1" in command
                or "ucloud-fork-child-gvisor-live-fork-v1" in command
                for command in executor.commands
            )
        )

    def test_live_fork_probe_does_not_drop_artifacts_until_stale_writer_stops(
        self,
    ) -> None:
        executor = LiveForkExecutor(stale_source_remove_failure=True)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("stale live fork source", live_fork.detail)
        helper_actions = [
            command[1]
            for command in executor.commands
            if command[0] == "/test/checkpoint-helper"
        ]
        self.assertEqual(helper_actions, [])

    def test_live_fork_probe_retains_current_artifacts_when_source_cleanup_fails(
        self,
    ) -> None:
        executor = LiveForkExecutor(cleanup_source_remove_failure=True)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("remove live fork source", live_fork.detail)
        helper_actions = [
            command[1]
            for command in executor.commands
            if command[0] == "/test/checkpoint-helper"
        ]
        final_unstage = len(helper_actions) - 1 - helper_actions[::-1].index("unstage")
        self.assertEqual(helper_actions[final_unstage + 1 :], [])

    def test_live_fork_probe_requires_distinct_bridge_identity(self) -> None:
        executor = LiveForkExecutor(distinct_bridge_identity=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("distinct Docker bridge address", live_fork.detail)

    def test_live_fork_probe_rejects_restored_exec_origin_descendant(self) -> None:
        executor = LiveForkExecutor(restored_exec_origin_absent=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("OriginExec descendant", live_fork.detail)

    def test_live_fork_probe_rejects_stale_in_child_bridge_identity(self) -> None:
        executor = LiveForkExecutor(restored_in_child_bridge_identity=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("netstack did not adopt", live_fork.detail)

    def test_live_fork_failure_is_reported_without_failing_base_conformance(
        self,
    ) -> None:
        executor = LiveForkExecutor(stage_failure=True)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertTrue(report.ok)
        self.assertFalse(live_fork.ok)
        self.assertFalse(live_fork.required)
        self.assertIn("stage", live_fork.detail)
        self.assertIn(
            "unstage",
            [
                command[1]
                for command in executor.commands
                if command[0] == "/test/checkpoint-helper"
            ],
        )

    def test_live_fork_probe_rejects_socket_migration_configuration(self) -> None:
        executor = LiveForkExecutor(safe_socket_migration=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("disconnect live sockets", live_fork.detail)
        self.assertFalse(
            any(
                command[:2] == ("docker", "run") and "--detach" in command
                for command in executor.commands
            )
        )

    def test_live_fork_probe_requires_an_established_socket_before_save(self) -> None:
        executor = LiveForkExecutor(source_socket_connected=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("establish the TCP session", live_fork.detail)
        self.assertFalse(
            any(
                command[:2] == ("docker", "checkpoint") for command in executor.commands
            )
        )

    def test_live_fork_probe_rejects_a_restored_live_socket(self) -> None:
        executor = LiveForkExecutor(restored_socket_disconnected=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("retained the source's external TCP session", live_fork.detail)

    def test_live_fork_probe_requires_restore_time_spec_identity(self) -> None:
        executor = LiveForkExecutor(restored_spec_identity=False)

        report = DockerRuntimeProbe(
            executor=executor,
            execute=True,
            max_parallel_probes=1,
            probe_live_fork=True,
            checkpoint_helper="/test/checkpoint-helper",
            checkpoint_root="/test/checkpoints",
            live_fork_wait_seconds=0,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertFalse(live_fork.ok)
        self.assertIn("/proc/gvisor/spec_environ", live_fork.detail)

    def test_live_fork_dry_run_is_optional_and_skipped(self) -> None:
        report = DockerRuntimeProbe(
            executor=FakeExecutor(),
            execute=False,
            probe_live_fork=True,
        ).run()

        live_fork = next(
            result for result in report.results if result.name == GVISOR_LIVE_FORK_PROBE
        )
        self.assertTrue(report.ok)
        self.assertTrue(live_fork.skipped)
        self.assertFalse(live_fork.required)


if __name__ == "__main__":
    unittest.main()
