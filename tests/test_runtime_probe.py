from threading import Lock
import time
import unittest

from ucloud_sandboxes.runtime_probe import DockerRuntimeProbe
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


if __name__ == "__main__":
    unittest.main()
