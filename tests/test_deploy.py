import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.cli import build_parser
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.deploy import (
    AllInOneDeployPlan,
    BUNDLED_SYSTEMD_RUNTIME_PACKAGES,
    packaged_systemd_units,
    render_remote_deploy_script,
    run_remote_script_over_ssh,
)
from ucloud_sandboxes.vm_init import RUNTIME_KERNEL_MODULES


class DeployTests(unittest.TestCase):
    @staticmethod
    def _config(**overrides: object) -> DeploymentConfig:
        raw = DeploymentConfig.default(scope_id="project-1").to_dict()
        provider = raw["provider"]
        sandbox = raw["sandbox"]
        assert isinstance(provider, dict)
        assert isinstance(sandbox, dict)
        provider["private_network_id"] = "net-1"
        raw["deployment_id"] = "prod-a"
        raw["gateway_private_host"] = "sandbox-gateway-prod"
        raw["registry_private_ip"] = "10.0.0.5"
        sandbox["direct_runsc_commit"] = "9f653e577965df2ddd13875b5530cd2588661f1c"
        raw.update(overrides)
        return DeploymentConfig.from_dict(raw)

    @staticmethod
    def _plan(
        root: Path,
        *,
        config: DeploymentConfig | None = None,
        local_wheel: Path | None = None,
    ) -> AllInOneDeployPlan:
        wheel = local_wheel or root / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
        wheel.parent.mkdir(parents=True, exist_ok=True)
        wheel.write_bytes(b"wheel")
        runsc = root / "runsc"
        runsc.write_bytes(b"patched-runsc")
        runsc.chmod(0o755)
        managed_init = root / "ucloud-sandbox-init"
        managed_init.write_bytes(b"managed-process-init")
        managed_init.chmod(0o755)
        backend_bytes = b"pinned-storage-native-backend"
        backend_digest = hashlib.sha256(backend_bytes).hexdigest()
        backend = root / f"uvm-ublk-daemon-{backend_digest}"
        backend.write_bytes(backend_bytes)
        backend.chmod(0o755)
        (root / f"{backend.name}.LICENSE").write_text("MIT\n", encoding="utf-8")
        manifest = root / f"{backend.name}.manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "agentenv_commit": "db1492b7915a408b37f863c9e3a34b2ccb2fb1b0",
                    "artifact": backend.name,
                    "artifact_sha256": backend_digest,
                    "cargo_package": "uvm-ublk-daemon",
                    "host_architecture": "x86_64",
                    "license": "MIT",
                    "patches": [
                        {
                            "name": "agentenv-streaming-dense-export.patch",
                            "sha256": "a" * 64,
                        },
                        {
                            "name": "agentenv-pooled-delete.patch",
                            "sha256": "b" * 64,
                        },
                        {
                            "name": "agentenv-owner-identity.patch",
                            "sha256": "c" * 64,
                        },
                    ],
                    "schema": 3,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return AllInOneDeployPlan(
            job_id="job-1",
            config=config or DeployTests._config(),
            local_wheel=wheel,
            local_direct_runsc=runsc,
            local_managed_init=managed_init,
            local_storage_native_manifest=manifest,
        )

    def test_remote_deploy_failure_retains_bounded_diagnostics(self) -> None:
        with patch(
            "ucloud_sandboxes.deploy.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=("ssh",),
                returncode=1,
                stdout="x" * 5000,
                stderr="durable mount is unavailable\n",
            ),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "durable mount is unavailable",
            ) as raised:
                run_remote_script_over_ssh("ssh ucloud@example.org", "set -eu\n")
        self.assertLess(len(str(raised.exception)), 8200)

    def test_renderer_installs_one_manifest_and_no_env_projection(self) -> None:
        with TemporaryDirectory() as raw_dir:
            plan = self._plan(Path(raw_dir))
            script = render_remote_deploy_script(plan)

        self.assertIn("/etc/ucloud-sandboxes/deployment.json", script)
        self.assertIn('"schema": 2', script)
        self.assertIn('"deployment_id": "prod-a"', script)
        self.assertNotIn("/etc/ucloud-sandboxes/gateway.env", script)
        self.assertNotIn("/etc/ucloud-sandboxes/autoscaler.env", script)
        self.assertNotIn("UCLOUD_PROJECT_ID", script)
        self.assertIn("runtime/direct/runsc", script)
        self.assertIn("runtime/direct/ucloud-sandbox-init", script)
        self.assertIn("agentenv-owner-identity.patch", script)
        self.assertIn("SANDBOX_NODE_PACKAGE_BUNDLE=", script)
        self.assertIn("BUILDER_NODE_PACKAGE_BUNDLE=", script)
        self.assertIn("package-bundle.json", script)
        for package in BUNDLED_SYSTEMD_RUNTIME_PACKAGES:
            self.assertIn(package, script)
        self.assertIn("gzip.GzipFile", script)
        self.assertIn("RequiresMountsFor=/work/data", script)
        self.assertIn(
            "create_secret /work/data/ucloud-sandboxes/state/gateway-token",
            script,
        )
        self.assertIn(
            "REGISTRY_MOUNT_POINT=/work/data",
            script,
        )
        self.assertIn(
            "REGISTRY_DATA_ROOT=/work/data/ucloud-sandbox-registry/docker-registry",
            script,
        )
        self.assertEqual(
            plan.to_dict()["deployment"],
            plan.config.to_dict(),
        )
        syntax = subprocess.run(
            ["bash", "-n"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_registry_units_fence_the_configured_registry_mount(self) -> None:
        with TemporaryDirectory() as raw_dir:
            config = self._config(
                registry_mount_point="/mnt/registry",
                registry_data_root="/mnt/registry/docker-registry",
            )
            plan = self._plan(Path(raw_dir), config=config)
            script = render_remote_deploy_script(plan)

        self.assertIn("REGISTRY_MOUNT_POINT=/mnt/registry", script)
        self.assertIn("RequiresMountsFor=/mnt/registry", script)
        self.assertIn("ExecStartPre=/usr/bin/mountpoint -q /mnt/registry", script)
        syntax = subprocess.run(
            ["bash", "-n"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_offline_bundle_builder_python_compiles(self) -> None:
        with TemporaryDirectory() as raw_dir:
            script = render_remote_deploy_script(self._plan(Path(raw_dir)))
        start = script.index("import hashlib\nimport gzip")
        end = script.index('\nPY\ndone\nrm -rf "$NODE_PACKAGE_WORK"', start)
        compile(script[start:end], "<offline-bundle-builder>", "exec")

    def test_offline_bundle_builder_is_deterministic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            runtime_dir = root / "runtime"
            package_dir = runtime_dir / "debs"
            package_dir.mkdir(parents=True)
            (package_dir / "docker-ce_1.0_amd64.deb").write_bytes(b"docker")
            agent_archive = root / "node-agent-runtime.tar"
            agent_archive.write_bytes(b"preassembled-agent")
            kernel_dir = root / "kernel-modules"
            kernel_dir.mkdir()
            (kernel_dir / "xfs.ko.zst").write_bytes(b"xfs-module")
            (kernel_dir / "overlay.ko.zst").write_bytes(b"overlay-module")
            script = render_remote_deploy_script(self._plan(root))
            start = script.index("import hashlib\nimport gzip")
            end = script.index('\nPY\ndone\nrm -rf "$NODE_PACKAGE_WORK"', start)
            code = compile(script[start:end], "<offline-bundle-builder>", "exec")
            targets = (root / "first.tar.gz", root / "second.tar.gz")
            original_argv = sys.argv
            try:
                for target in targets:
                    sys.argv = [
                        "builder",
                        str(target),
                        str(runtime_dir),
                        "ubuntu",
                        "24.04",
                        "noble",
                        "amd64",
                        "builder",
                        "xfsprogs docker-ce docker-ce-cli containerd.io docker-buildx-plugin",
                        str(agent_archive),
                        "6.8.0-test-generic",
                        str(kernel_dir),
                        " ".join(RUNTIME_KERNEL_MODULES),
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                    ]
                    exec(code, {"__name__": "__main__"})
            finally:
                sys.argv = original_argv
            self.assertEqual(targets[0].read_bytes(), targets[1].read_bytes())
            with tarfile.open(targets[0], mode="r:gz") as archive:
                manifest_file = archive.extractfile("package-bundle.json")
                assert manifest_file is not None
                manifest = json.loads(manifest_file.read())
        self.assertEqual(manifest["runtime"]["role"], "builder")
        self.assertEqual(
            manifest["runtime"]["platform"]["architecture"],
            "amd64",
        )
        self.assertEqual(
            [item["name"] for item in manifest["runtime"]["kernel"]["files"]],
            ["overlay.ko.zst", "xfs.ko.zst"],
        )

    def test_packaged_services_accept_only_manifest_and_operations(self) -> None:
        units = packaged_systemd_units()
        for name in (
            "ucloud-sandbox-gateway.service",
            "ucloud-sandbox-relay.service",
            "ucloud-sandbox-autoscaler.service",
            "ucloud-sandbox-registry-prune.service",
        ):
            self.assertIn("--config /etc/ucloud-sandboxes/deployment.json", units[name])
            self.assertNotIn("Environment=UCLOUD_", units[name])
            self.assertNotIn("EnvironmentFile=", units[name])

        autoscaler_exec = next(
            line.removeprefix("ExecStart=")
            for line in units["ucloud-sandbox-autoscaler.service"].splitlines()
            if line.startswith("ExecStart=")
        )
        args = build_parser().parse_args(shlex.split(autoscaler_exec)[1:])
        self.assertEqual(args.command, "autoscaler")
        self.assertTrue(args.execute)
        self.assertEqual(
            args.config,
            Path("/etc/ucloud-sandboxes/deployment.json"),
        )


if __name__ == "__main__":
    unittest.main()
