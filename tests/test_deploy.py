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
import zipfile

from ucloud_sandboxes.cli import build_parser
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.deploy import (
    AllInOneDeployPlan,
    packaged_systemd_units,
    render_remote_deploy_script,
    run_remote_script_over_ssh,
    stage_file_over_ssh,
    wheel_package_version,
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
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr(
                "ucloud_sandboxes-0.2.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: ucloud-sandboxes\nVersion: 0.2.0\n",
            )
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

    def test_release_version_comes_from_the_wheel_metadata(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            wheel = root / "misleading-filename-99.0.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr(
                    "ucloud_sandboxes-7.4.1.dist-info/METADATA",
                    "Metadata-Version: 2.1\nName: ucloud-sandboxes\nVersion: 7.4.1\n",
                )

            self.assertEqual(wheel_package_version(wheel), "7.4.1")

    def test_release_rejects_an_unrelated_wheel(self) -> None:
        with TemporaryDirectory() as raw_dir:
            wheel = Path(raw_dir) / "other.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr(
                    "other-1.0.dist-info/METADATA",
                    "Metadata-Version: 2.1\nName: other\nVersion: 1.0\n",
                )

            with self.assertRaisesRegex(ValueError, "ucloud-sandboxes"):
                wheel_package_version(wheel)

    def test_rendered_deploy_script_is_valid_for_each_registry_backend(self) -> None:
        filesystem = {
            **DeploymentConfig.default().to_dict()["registry_store"],
            "mount_point": "/mnt/registry",
            "data_root": "/mnt/registry/docker-registry",
        }
        s3 = {
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
        for name, config, marker in (
            ("default", self._config(), "RequiresMountsFor=/work/data"),
            (
                "filesystem",
                self._config(registry_store=filesystem),
                "RequiresMountsFor=/mnt/registry",
            ),
            ("s3", self._config(registry_store=s3), "REGISTRY_STORE_KIND=s3"),
        ):
            with self.subTest(name=name), TemporaryDirectory() as raw_dir:
                plan = self._plan(Path(raw_dir), config=config)
                script = render_remote_deploy_script(plan)
                syntax = subprocess.run(
                    ["bash", "-n"],
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(syntax.returncode, 0, syntax.stderr)
                self.assertIn("/etc/ucloud-sandboxes/deployment.json", script)
                self.assertIn(marker, script)
                self.assertEqual(plan.to_dict()["deployment"], config.to_dict())
                self.assertNotIn("REGISTRY_ACCESS_KEY=", script)
                self.assertNotIn("REGISTRY_SECRET_KEY=", script)
                self.assertIn(
                    "download_runtime_packages runtime xfsprogs "
                    "docker-ce docker-ce-cli containerd.io apparmor "
                    "bsdextrautils eject fdisk kmod libfdisk1 ",
                    script,
                )
                self.assertIn("util-linux util-linux-extra uuid-runtime", script)
                self.assertIn(
                    "OPTIONAL_SYSTEMD_RUNTIME_PACKAGES=systemd-cryptsetup",
                    script,
                )
                self.assertIn(
                    "Skipping unavailable optional runtime package: $package",
                    script,
                )
                self.assertIn("os.replace(marker_temporary, marker)", script)

    def test_gateway_service_convergence_is_shared_with_hetzner(self) -> None:
        s3_snapshot_store = {
            "kind": "s3",
            "endpoint": "https://hel1.your-objectstorage.com",
            "bucket": "sandbox-snapshots",
            "region": "hel1",
            "prefix": "production",
            "access_key_id_env": "SNAPSHOT_ACCESS_KEY",
            "secret_access_key_env": "SNAPSHOT_SECRET_KEY",
            "security_token_env": "SNAPSHOT_SECURITY_TOKEN",
        }
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            registry_script = render_remote_deploy_script(self._plan(root))
            s3_script = render_remote_deploy_script(
                self._plan(
                    root / "s3",
                    config=self._config(snapshot_store=s3_snapshot_store),
                )
            )

        for script in (registry_script, s3_script):
            self.assertIn(
                "ucloud_sandboxes.systemd gateway-reconcile",
                script,
            )
            self.assertNotIn("SNAPSHOT_STORE_KIND=", script)

        hetzner_installer = (
            Path(__file__).parents[1] / "scripts" / "install_hetzner_gateway.sh"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "gateway-reconcile --config",
            hetzner_installer,
        )

    def test_stage_file_atomically_replaces_root_owned_release_artifact(self) -> None:
        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "release.whl"
            source.write_bytes(b"new wheel")
            completed = subprocess.CompletedProcess(
                args=(),
                returncode=0,
                stdout=b"",
                stderr=b"",
            )
            with patch("ucloud_sandboxes.deploy.subprocess.run", return_value=completed) as run:
                stage_file_over_ssh(
                    "ssh gateway",
                    source,
                    "/work/release/release.whl",
                )

        command = run.call_args.args[0]
        remote_command = command[-1]
        self.assertIn("mktemp /work/release/.ucloud-stage.XXXXXX", remote_command)
        self.assertIn("[ ! -w /work/release/release.whl ]", remote_command)
        self.assertIn(
            'sudo install -m 0644 "$temporary" /work/release/release.whl',
            remote_command,
        )
        self.assertIn('mv -f "$temporary" /work/release/release.whl', remote_command)
        self.assertEqual(run.call_args.kwargs["input"], b"new wheel")

    def test_sandbox_bundle_contains_complete_new_gvisor_distribution(self):
        from ucloud_sandboxes.gvisor_distribution import GVISOR_COMMIT, GVISOR_SIDECARS

        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = self._config().to_dict()
            raw["sandbox"]["direct_runsc_commit"] = GVISOR_COMMIT
            plan = self._plan(root, config=DeploymentConfig.from_dict(raw))
            runtime = root / "runtime"
            (runtime / "debs").mkdir(parents=True)
            (runtime / "debs/runtime.deb").write_bytes(b"package")
            agent = root / "agent.tar"
            agent.write_bytes(b"agent")
            kernel = root / "kernel"
            kernel.mkdir()
            (kernel / "xfs.ko").write_bytes(b"module")
            files = {}
            names = ["runsc", *("gvisor-bin/" + name for name in GVISOR_SIDECARS)]
            for name in names:
                binary = root / name
                binary.parent.mkdir(exist_ok=True)
                binary.write_bytes(name.encode())
                binary.chmod(0o755)
                files[name] = {
                    "size": binary.stat().st_size,
                    "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                }
            (root / "build-manifest.json").write_text(
                json.dumps(
                    {"schema": 2, "gvisor_commit": GVISOR_COMMIT, "files": files}
                )
            )
            script = render_remote_deploy_script(plan)
            start = script.index("import hashlib\nimport gzip")
            code = compile(
                script[
                    start : script.index(
                        '\nPY\ndone\nrm -rf "$NODE_PACKAGE_WORK"', start
                    )
                ],
                "<builder>",
                "exec",
            )
            backend_manifest = plan.local_storage_native_manifest
            backend = root / json.loads(backend_manifest.read_text())["artifact"]
            target = root / "sandbox.tar.gz"
            argv = [
                "builder",
                str(target),
                str(runtime),
                "ubuntu",
                "26.04",
                "resolute",
                "amd64",
                "sandbox",
                "xfsprogs",
                str(agent),
                "7.0.0",
                str(kernel),
                "xfs",
                str(root / "runsc"),
                GVISOR_COMMIT,
                str(backend),
                str(backend_manifest),
                str(root / (backend.name + ".LICENSE")),
                str(plan.local_managed_init),
            ]
            with patch.object(sys, "argv", argv):
                exec(code, {"__name__": "__main__"})
            with tarfile.open(target) as archive:
                manifest = json.load(archive.extractfile("package-bundle.json"))
                sidecars = manifest["runtime"]["direct_runsc"]["sidecars"]
                self.assertEqual(len(sidecars), 4)
                for item in sidecars:
                    self.assertEqual(
                        hashlib.sha256(
                            archive.extractfile(item["file"]).read()
                        ).hexdigest(),
                        item["sha256"],
                    )
            (root / "gvisor-bin/gvisor_sentry").write_bytes(b"tampered")
            with (
                patch.object(sys, "argv", argv),
                self.assertRaisesRegex(SystemExit, "executable mismatch"),
            ):
                exec(code, {"__name__": "__main__"})

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
