from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.systemd import run_registry_gc


class SystemdHelperTests(unittest.TestCase):
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
            run_registry_gc(
                data_dir=Path("/work/data/registry"),
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
