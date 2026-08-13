from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.systemd import (
    REGISTRY_CONFIG_PATH,
    REGISTRY_IMAGE,
    registry_gc_command,
    registry_process_environment,
    registry_run_command,
    require_registry_mount,
    run_registry_gc,
)


class SystemdHelperTests(unittest.TestCase):
    @staticmethod
    def _filesystem_config(root: Path) -> DeploymentConfig:
        raw = DeploymentConfig.default("project").to_dict()
        store = raw["registry_store"]
        assert isinstance(store, dict)
        store["mount_point"] = str(root)
        store["data_root"] = str(root / "registry")
        return DeploymentConfig.from_dict(raw)

    @staticmethod
    def _s3_config() -> DeploymentConfig:
        raw = DeploymentConfig.default("project").to_dict()
        raw["registry_store"] = {
            "kind": "s3",
            "mount_point": "",
            "data_root": "",
            "endpoint": "https://hel1.your-objectstorage.com",
            "bucket": "sandboxes",
            "region": "hel1",
            "prefix": "production/oci",
            "access_key_id_env": "REGISTRY_ACCESS_KEY",
            "secret_access_key_env": "REGISTRY_SECRET_KEY",
            "force_path_style": False,
        }
        return DeploymentConfig.from_dict(raw)

    def test_registry_uses_host_network_without_docker_port_publishing(self) -> None:
        config = DeploymentConfig.default("project")

        command = registry_run_command(config)

        self.assertIn("--network", command)
        self.assertIn("host", command)
        self.assertIn("REGISTRY_HTTP_ADDR", command)
        self.assertEqual(
            registry_process_environment(config)["REGISTRY_HTTP_ADDR"],
            f"0.0.0.0:{config.registry_port}",
        )
        self.assertNotIn("-p", command)
        self.assertEqual(command[-1], REGISTRY_IMAGE)

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
            env: dict[str, str] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "", "")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            run_registry_gc(
                config=self._filesystem_config(root),
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
            env: dict[str, str] | None = None,
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
            config = self._filesystem_config(Path(raw_dir))
            data_dir = config.registry_data_dir()
            (data_dir / "docker" / "registry" / "v2" / "repositories").mkdir(
                parents=True
            )
            run_registry_gc(
                config=config,
                lock_file=Path(raw_dir) / "maintenance",
                runner=runner,
            )

        self.assertEqual(
            calls[0],
            ["systemctl", "stop", "ucloud-sandbox-registry.service"],
        )
        self.assertEqual(calls[1][0], "docker")
        self.assertEqual(calls[1][-1], REGISTRY_CONFIG_PATH)
        self.assertEqual(
            calls[2],
            ["systemctl", "start", "ucloud-sandbox-registry.service"],
        )

    def test_s3_registry_forwards_secrets_without_process_arguments(self) -> None:
        config = self._s3_config()
        source = {
            "REGISTRY_ACCESS_KEY": "access-value",
            "REGISTRY_SECRET_KEY": "secret-value",
        }

        command = registry_run_command(config)
        environment = registry_process_environment(config, environ=source)

        self.assertNotIn("access-value", command)
        self.assertNotIn("secret-value", command)
        self.assertNotIn("-v", command)
        self.assertEqual(environment["REGISTRY_STORAGE"], "s3")
        self.assertEqual(environment["REGISTRY_STORAGE_S3_ACCESSKEY"], "access-value")
        self.assertEqual(environment["REGISTRY_STORAGE_S3_SECRETKEY"], "secret-value")
        self.assertEqual(
            environment["REGISTRY_STORAGE_S3_ROOTDIRECTORY"],
            "production/oci",
        )
        self.assertEqual(environment["REGISTRY_STORAGE_S3_FORCEPATHSTYLE"], "false")

    def test_s3_registry_gc_scans_remote_store_without_mount(self) -> None:
        config = self._s3_config()
        calls: list[tuple[list[str], dict[str, str] | None]] = []

        def runner(
            command: list[str],
            *,
            check: bool,
            text: bool,
            env: dict[str, str] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            calls.append((command, env))
            return subprocess.CompletedProcess(command, 0, "", "")

        with TemporaryDirectory() as raw_dir:
            run_registry_gc(
                config=config,
                lock_file=Path(raw_dir) / "maintenance",
                runner=runner,
                environ={
                    "REGISTRY_ACCESS_KEY": "access-value",
                    "REGISTRY_SECRET_KEY": "secret-value",
                },
            )

        self.assertEqual(calls[0][0][0], "systemctl")
        self.assertEqual(calls[1][0], registry_gc_command(config))
        self.assertEqual(
            calls[1][1]["REGISTRY_STORAGE_S3_SECRETKEY"],
            "secret-value",
        )
        self.assertEqual(calls[2][0][0], "systemctl")


if __name__ == "__main__":
    unittest.main()
