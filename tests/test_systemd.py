from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.systemd import require_registry_mount, run_registry_gc


class SystemdHelperTests(unittest.TestCase):
    def test_registry_mount_fails_closed(self) -> None:
        config = DeploymentConfig.default()
        with patch.object(Path, "is_mount", return_value=False), self.assertRaisesRegex(
            RuntimeError,
            "registry storage is not mounted",
        ):
            require_registry_mount(config)

    def test_registry_gc_is_a_noop_for_fresh_empty_registry(self) -> None:
        calls: list[list[str]] = []

        def runner(
            command: list[str],
            *,
            check: bool,
            text: bool,
        ) -> subprocess.CompletedProcess[str]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "", "")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            run_registry_gc(
                data_dir=root / "registry",
                registry_image="registry:2",
                lock_file=root / "maintenance",
                runner=runner,
            )

        self.assertEqual(calls, [])

    def test_registry_gc_restarts_registry_after_gc_failure(self) -> None:
        calls: list[list[str]] = []

        def runner(
            command: list[str],
            *,
            check: bool,
            text: bool,
        ) -> subprocess.CompletedProcess[str]:
            self.assertTrue(check)
            self.assertTrue(text)
            calls.append(command)
            if command[0] == "docker":
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, "", "")

        with TemporaryDirectory() as raw_dir, self.assertRaises(
            subprocess.CalledProcessError
        ):
            data_dir = Path(raw_dir) / "registry"
            (data_dir / "docker" / "registry" / "v2" / "repositories").mkdir(
                parents=True
            )
            run_registry_gc(
                data_dir=data_dir,
                registry_image="registry:2",
                lock_file=Path(raw_dir) / "maintenance",
                runner=runner,
            )

        self.assertEqual(
            calls[0],
            ["systemctl", "stop", "ucloud-sandbox-registry.service"],
        )
        self.assertEqual(calls[1][0], "docker")
        self.assertEqual(
            calls[2],
            ["systemctl", "start", "ucloud-sandbox-registry.service"],
        )


if __name__ == "__main__":
    unittest.main()
