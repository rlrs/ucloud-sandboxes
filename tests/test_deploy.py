import hashlib
import json
from pathlib import Path
import re
import shlex
import sys
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.cli import build_parser
from ucloud_sandboxes.deploy import (
    AllInOneDeployPlan,
    autoscaler_env,
    gateway_env,
    packaged_systemd_units,
    relay_env,
    registry_env,
    render_env_file,
    render_remote_deploy_script,
    run_remote_script_over_ssh,
)
from ucloud_sandboxes.vm_init import RUNTIME_KERNEL_MODULES


class DeployTests(unittest.TestCase):
    @staticmethod
    def _plan(root: Path, **overrides: object) -> AllInOneDeployPlan:
        wheel = root / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
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
        storage_manifest = root / f"{backend.name}.manifest.json"
        storage_manifest.write_text(
            json.dumps(
                {
                    "agentenv_commit": "f41abb21324f6b0520abf34b7720aa260ddd10eb",
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
        values: dict[str, object] = {
            "job_id": "job-1",
            "project_id": "project-1",
            "deployment_id": "prod-a",
            "local_wheel": wheel,
            "local_direct_runsc": runsc,
            "local_managed_init": managed_init,
            "direct_runsc_commit": "9f653e577965df2ddd13875b5530cd2588661f1c",
            "local_storage_native_manifest": storage_manifest,
            "gateway_private_host": "sandbox-gateway-prod",
            "registry_private_ip": "10.0.0.5",
            "private_network_id": "net-1",
        }
        values.update(overrides)
        return AllInOneDeployPlan(**values)

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
                run_remote_script_over_ssh(
                    "ssh ucloud@example.org",
                    "set -eu\n",
                )

        self.assertLess(len(str(raised.exception)), 8200)

    def test_direct_deploy_requires_and_bundles_pinned_runsc(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            wheel = root / "ucloud_sandboxes-0.3.52-py3-none-any.whl"
            runsc = root / "runsc"
            managed_init = root / "ucloud-sandbox-init"
            wheel.write_bytes(b"wheel")
            runsc.write_bytes(b"patched-runsc")
            runsc.chmod(0o755)
            managed_init.write_bytes(b"managed-process-init")
            managed_init.chmod(0o755)
            backend_bytes = b"pinned-storage-native-backend"
            backend_digest = hashlib.sha256(backend_bytes).hexdigest()
            backend = root / f"uvm-ublk-daemon-{backend_digest}"
            backend.write_bytes(backend_bytes)
            backend.chmod(0o755)
            (root / f"{backend.name}.LICENSE").write_text(
                "MIT\n",
                encoding="utf-8",
            )
            storage_manifest = root / f"{backend.name}.manifest.json"
            storage_manifest.write_text(
                json.dumps(
                    {
                        "agentenv_commit": ("f41abb21324f6b0520abf34b7720aa260ddd10eb"),
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
            plan = AllInOneDeployPlan(
                job_id="job-1",
                project_id="project-1",
                deployment_id="prod-direct",
                local_wheel=wheel,
                local_direct_runsc=runsc,
                local_managed_init=managed_init,
                direct_runsc_commit="9f653e577965df2ddd13875b5530cd2588661f1c",
                local_storage_native_manifest=storage_manifest,
                gateway_private_host="sandbox-gateway-prod",
                registry_private_ip="10.0.0.5",
                private_network_id="net-1",
                max_concurrent_image_pulls=7,
            )

            env = autoscaler_env(plan)
            script = render_remote_deploy_script(plan)

        self.assertNotIn("UCLOUD_INIT_NODE_RUNTIME", env)
        self.assertEqual(env["UCLOUD_INIT_MAX_CONCURRENT_IMAGE_PULLS"], "7")
        self.assertEqual(
            env["UCLOUD_INIT_STORAGE_NATIVE_POOL_LOW_WATERMARK"],
            "2",
        )
        self.assertEqual(
            env["UCLOUD_INIT_STORAGE_NATIVE_POOL_HIGH_WATERMARK"],
            "16",
        )
        self.assertEqual(
            env["UCLOUD_INIT_DIRECT_RUNSC_COMMIT"],
            "9f653e577965df2ddd13875b5530cd2588661f1c",
        )
        self.assertIn("runtime/direct/runsc", script)
        self.assertIn("runtime/direct/ucloud-sandbox-init", script)
        self.assertIn("managed_init", script)
        self.assertIn("DIRECT_RUNSC_COMMIT=", script)
        self.assertIn("agentenv-owner-identity.patch", script)

    def test_env_rendering_quotes_only_when_needed(self) -> None:
        text = render_env_file({"A": "plain-value", "B": "two words"})

        self.assertIn("A=plain-value\n", text)
        self.assertIn('B="two words"\n', text)

    def test_all_in_one_plan_renders_env_and_script(self) -> None:
        with TemporaryDirectory() as raw_dir:
            plan = self._plan(Path(raw_dir))

            gateway = gateway_env(plan)
            relay = relay_env(plan)
            autoscaler = autoscaler_env(plan)
            registry = registry_env(plan)
            script = render_remote_deploy_script(plan)

        self.assertEqual(gateway["UCLOUD_DEPLOYMENT_ID"], "prod-a")
        self.assertEqual(
            gateway["UCLOUD_HEARTBEAT_TOKEN_FILE"],
            "/work/data/ucloud-sandboxes/state/heartbeat-token",
        )
        self.assertEqual(
            gateway["UCLOUD_NODE_CONTROL_TOKEN_FILE"],
            "/work/data/ucloud-sandboxes/state/node-control-token",
        )
        self.assertEqual(gateway["UCLOUD_REGISTRY_URL"], "http://127.0.0.1:5000")
        self.assertEqual(
            gateway["UCLOUD_REGISTRY_WORKER_URL"],
            "http://ucloud-sandbox-registry:5000",
        )
        self.assertEqual(
            relay["UCLOUD_RELAY_SANDBOX_TOKEN_FILE"],
            "/work/data/ucloud-sandboxes/state/relay-sandbox-token",
        )
        self.assertEqual(
            relay["UCLOUD_RELAY_WORKER_TOKEN_FILE"],
            "/work/data/ucloud-sandboxes/state/relay-worker-token",
        )
        self.assertIn(
            "create_secret /work/data/ucloud-sandboxes/state/relay-sandbox-token",
            script,
        )
        self.assertEqual(registry["UCLOUD_REGISTRY_RETENTION_DAYS"], "30")
        self.assertEqual(registry["UCLOUD_REGISTRY_KEEP_PER_REPOSITORY"], "0")
        self.assertEqual(
            registry["UCLOUD_REGISTRY_USAGE_FILE"],
            "/work/data/ucloud-sandboxes/state/registry-usage.json",
        )
        self.assertEqual(
            registry["UCLOUD_IMAGE_FILE"],
            "/work/data/ucloud-sandboxes/state/images.json",
        )
        self.assertIn(
            "REGISTRY_USAGE_FILE=/work/data/ucloud-sandboxes/state/registry-usage.json",
            script,
        )
        self.assertIn(
            'for path in "$REGISTRY_USAGE_FILE" "$REGISTRY_USAGE_FILE.lock"; do',
            script,
        )
        self.assertIn(
            'sudo chown "$SERVICE_USER:$SERVICE_GROUP" "$path"',
            script,
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_HEARTBEAT_URL"],
            "http://sandbox-gateway-prod:8090/v1/nodes/heartbeat",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_HEARTBEAT_TOKEN_SOURCE_FILE"],
            "/work/data/ucloud-sandboxes/state/heartbeat-token",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_NODE_CONTROL_TOKEN_SOURCE_FILE"],
            "/work/data/ucloud-sandboxes/state/node-control-token",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_PACKAGE_SPEC"],
            "/work/ucloud-sandboxes/release/"
            "ucloud_sandboxes-0.2.0-py3-none-any-sandbox-node-package.tar.gz",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_BUILDER_PACKAGE_SPEC"],
            "/work/ucloud-sandboxes/release/"
            "ucloud_sandboxes-0.2.0-py3-none-any-builder-node-package.tar.gz",
        )
        self.assertEqual(autoscaler["UCLOUD_MAX_INIT_PER_CYCLE"], "4")
        self.assertEqual(
            autoscaler["UCLOUD_SANDBOX_PRODUCT_ID"],
            "cpu-amd-zen5-32-vcpu",
        )
        self.assertEqual(
            autoscaler["UCLOUD_BUILDER_PRODUCT_ID"],
            "cpu-amd-zen5-16-vcpu",
        )
        self.assertEqual(autoscaler["UCLOUD_INIT_CPU_OVERCOMMIT"], "3")
        self.assertEqual(autoscaler["UCLOUD_INIT_MEMORY_OVERCOMMIT"], "2")
        self.assertEqual(autoscaler["UCLOUD_SANDBOX_DISK_GB"], "2000")
        self.assertEqual(autoscaler["UCLOUD_INIT_DOCKER_QUOTA_IMAGE_GB"], "440")
        self.assertEqual(autoscaler["UCLOUD_INIT_BUILDER_DOCKER_QUOTA_IMAGE_GB"], "200")
        self.assertEqual(autoscaler["UCLOUD_INIT_SWAP_GB"], "96")
        self.assertEqual(
            autoscaler["UCLOUD_DOCKER_HOST_ALIAS"],
            "ucloud-sandbox-registry=10.0.0.5",
        )
        self.assertIn("/etc/ucloud-sandboxes/gateway.env", script)
        self.assertIn("SANDBOX_NODE_PACKAGE_BUNDLE=", script)
        self.assertIn("BUILDER_NODE_PACKAGE_BUNDLE=", script)
        self.assertIn("ca-certificates curl docker.io", script)
        self.assertIn("package-bundle.json", script)
        self.assertIn("gzip.GzipFile", script)
        self.assertIn("compresslevel=1", script)
        bundle_complete = script.index("trap - EXIT")
        gateway_install = script.index(
            '"$VENV_DIR/bin/pip" install --force-reinstall "$REMOTE_WHEEL"'
        )
        self.assertGreater(gateway_install, bundle_complete)
        self.assertLess(gateway_install, script.index("create_secret()"))
        self.assertIn('Dir::State::status="$status_file"', script)
        self.assertIn('Dir::Cache::archives="$archive_dir"', script)
        self.assertIn("download_runtime_packages runtime xfsprogs", script)
        self.assertIn("NODE_AGENT_RUNTIME_ARCHIVE", script)
        self.assertNotIn("prune_runsc_package", script)
        self.assertNotIn("gvisor/releases", script)
        self.assertNotIn("python3-pip", script)
        self.assertNotIn("docker-compose-plugin", script)
        self.assertIn("'architecture': sys.argv[6]", script)
        self.assertIn("'sha256': sha256_file(path)", script)
        self.assertIn("mode='w|'", script)
        self.assertNotIn("repository fallback", script)
        self.assertNotIn("runtime-conformance", script)
        self.assertIn("ucloud-sandbox-autoscaler.service", script)
        self.assertIn("ucloud-sandbox-registry-prune.timer", script)
        self.assertIn(
            "systemctl enable --now ucloud-sandbox-registry-prune.timer", script
        )
        self.assertIn("wait_for_http gateway http://127.0.0.1:8090/healthz", script)
        self.assertIn('while [ "$attempt" -le 30 ]; do', script)
        self.assertNotIn("sleep 2\ncurl -fsS", script)
        self.assertIn(
            "create_secret /work/data/ucloud-sandboxes/state/gateway-token",
            script,
        )
        self.assertIn(
            "create_secret /work/data/ucloud-sandboxes/state/heartbeat-token",
            script,
        )
        self.assertIn(
            "create_secret /work/data/ucloud-sandboxes/state/node-control-token",
            script,
        )
        self.assertIn("PROJECT_MOUNT_DIR=/work/data", script)
        self.assertIn('mountpoint -q "$PROJECT_MOUNT_DIR"', script)
        self.assertNotIn("LEGACY_STATE", script)
        self.assertNotIn("PERSISTENT_STATE_MARKER", script)
        self.assertIn('sudo systemctl stop "$unit"', script)
        self.assertNotIn('systemctl stop "$unit" 2>/dev/null || true', script)
        self.assertIn("RequiresMountsFor=/work/data", script)
        self.assertIn("ExecStartPre=/usr/bin/mountpoint -q /work/data", script)
        self.assertEqual(
            len(
                {
                    plan.gateway_token_file,
                    plan.heartbeat_token_file,
                    plan.node_control_token_file,
                }
            ),
            3,
        )
        self.assertNotIn(
            "gateway-token /work/data/ucloud-sandboxes/state/heartbeat-token",
            script,
        )

        syntax = subprocess.run(
            ["bash", "-n"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_all_in_one_plan_uses_project_drive_for_durable_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            plan = self._plan(
                Path(raw_dir),
                install_root="/srv/ucloud-sandboxes",
                project_mount_dir="/mnt/project-data",
            )
            script = render_remote_deploy_script(plan)

        self.assertEqual(plan.state_dir, "/mnt/project-data/ucloud-sandboxes/state")
        self.assertEqual(
            plan.staged_session_file,
            "/srv/ucloud-sandboxes/release/.deploy-ucloud-session.json",
        )
        self.assertEqual(
            plan.remote_session_file,
            "/mnt/project-data/ucloud-sandboxes/state/ucloud-session.json",
        )
        self.assertIn("WorkingDirectory=/srv/ucloud-sandboxes", script)
        self.assertIn(
            "ExecStart=/srv/ucloud-sandboxes/gateway-venv/bin/ucloud-sandboxes",
            script,
        )
        self.assertNotIn("WorkingDirectory=/work/ucloud-sandboxes", script)

    def test_offline_bundle_builder_python_compiles(self) -> None:
        with TemporaryDirectory() as raw_dir:
            script = render_remote_deploy_script(self._plan(Path(raw_dir)))

        start = script.index("import hashlib\nimport gzip")
        end = script.index('\nPY\ndone\nrm -rf "$NODE_PACKAGE_WORK"', start)
        compile(script[start:end], "<offline-bundle-builder>", "exec")

    def test_offline_bundle_builder_records_platform_and_is_deterministic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            wheel_dir = root / "wheels"
            wheel_dir.mkdir()
            wheel = wheel_dir / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            runtime_dir = root / "runtime"
            package_dir = runtime_dir / "debs"
            package_dir.mkdir(parents=True)
            (package_dir / "docker-ce_1.0_amd64.deb").write_bytes(b"docker")
            agent_runtime_archive = root / "node-agent-runtime.tar"
            agent_runtime_archive.write_bytes(b"preassembled-agent")
            kernel_module_dir = root / "kernel-modules"
            kernel_module_dir.mkdir()
            xfs_module = kernel_module_dir / "xfs.ko.zst"
            xfs_module.write_bytes(b"xfs-module")
            overlay_module = kernel_module_dir / "overlay.ko.zst"
            overlay_module.write_bytes(b"overlay-module")
            plan = self._plan(root, local_wheel=wheel)
            script = render_remote_deploy_script(plan)
            start = script.index("import hashlib\nimport gzip")
            end = script.index('\nPY\ndone\nrm -rf "$NODE_PACKAGE_WORK"', start)
            code = compile(
                script[start:end],
                "<offline-bundle-builder>",
                "exec",
            )
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
                        str(agent_runtime_archive),
                        "6.8.0-test-generic",
                        str(kernel_module_dir),
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

        self.assertEqual(
            manifest["runtime"]["platform"],
            {
                "os_id": "ubuntu",
                "version_id": "24.04",
                "codename": "noble",
                "architecture": "amd64",
            },
        )
        self.assertIn("docker-ce", manifest["runtime"]["packages"])
        self.assertNotIn("runsc", manifest["runtime"]["packages"])
        self.assertEqual(manifest["runtime"]["role"], "builder")
        self.assertEqual(
            manifest["runtime"]["kernel"]["release"],
            "6.8.0-test-generic",
        )
        self.assertEqual(
            manifest["runtime"]["kernel"]["load"],
            list(RUNTIME_KERNEL_MODULES),
        )
        self.assertEqual(
            [item["name"] for item in manifest["runtime"]["kernel"]["files"]],
            ["overlay.ko.zst", "xfs.ko.zst"],
        )
        self.assertEqual(
            [item["name"] for item in manifest["runtime"]["files"]],
            ["docker-ce_1.0_amd64.deb"],
        )
        self.assertNotIn("probe_image", manifest["runtime"])

    def test_all_in_one_plan_uses_restart_stable_private_dns(self) -> None:
        with TemporaryDirectory() as raw_dir:
            plan = self._plan(
                Path(raw_dir),
                gateway_private_host="sandbox-gateway-prod",
                registry_private_ip="",
            )

            autoscaler = autoscaler_env(plan)
            script = render_remote_deploy_script(plan)

        self.assertEqual(autoscaler["UCLOUD_DOCKER_HOST_ALIAS"], "")
        self.assertEqual(
            autoscaler["UCLOUD_DOCKER_INSECURE_REGISTRY"],
            "sandbox-gateway-prod:5000",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_STORAGE_NATIVE_REGISTRY_URL"],
            "http://sandbox-gateway-prod:5000",
        )
        self.assertEqual(
            gateway_env(plan)["UCLOUD_REGISTRY_WORKER_URL"],
            "http://sandbox-gateway-prod:5000",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_DIRECT_NETWORK_ALLOW_TCP"],
            "sandbox-gateway-prod:8092",
        )
        self.assertNotIn("detect_registry_private_ip() {", script)
        self.assertIn(
            "UCLOUD_DOCKER_HOST_ALIAS=",
            script,
        )
        self.assertIn(
            "UCLOUD_INIT_DIRECT_NETWORK_ALLOW_TCP=" "sandbox-gateway-prod:8092",
            script,
        )
        self.assertNotIn("__UCLOUD_REGISTRY_PRIVATE_IP__", script)
        self.assertIn("--init-host-alias=", script)
        self.assertNotIn("--init-host-alias-optional", script)

    def test_packaged_systemd_units_are_available(self) -> None:
        units = packaged_systemd_units()

        self.assertIn("ucloud-sandbox-gateway.service", units)
        self.assertIn("ucloud-sandbox-autoscaler.service", units)
        self.assertIn("ucloud-sandbox-registry-prune.service", units)
        self.assertIn("ucloud-sandbox-registry-prune.timer", units)
        self.assertIn("--max-age-days", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn("--usage-file", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn("--image-file", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn(
            "--prune-stale-image-records",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertIn(
            "flock --exclusive --nonblock",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertNotIn(
            "ExecStartPost",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertIn(
            "/work/data/ucloud-sandbox-registry/docker-registry",
            units["ucloud-sandbox-registry-gc.service"],
        )
        self.assertIn(
            "-m ucloud_sandboxes.systemd registry-gc",
            units["ucloud-sandbox-registry-gc.service"],
        )
        self.assertIn(
            "--init-heartbeat-bearer-token-source-file ${UCLOUD_INIT_HEARTBEAT_TOKEN_SOURCE_FILE}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--heartbeat-bearer-token-file ${UCLOUD_HEARTBEAT_TOKEN_FILE}",
            units["ucloud-sandbox-gateway.service"],
        )
        self.assertIn(
            "--node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_TOKEN_FILE}",
            units["ucloud-sandbox-gateway.service"],
        )
        self.assertIn(
            "--init-node-control-bearer-token-source-file ${UCLOUD_INIT_NODE_CONTROL_TOKEN_SOURCE_FILE}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--init-buildx-direct-push",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--create-pressure-enabled ${UCLOUD_CREATE_PRESSURE_ENABLED}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--create-target-concurrency-per-node "
            "${UCLOUD_CREATE_TARGET_CONCURRENCY_PER_NODE}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "EnvironmentFile=/etc/ucloud-sandboxes/gateway.env",
            units["ucloud-sandbox-gateway.service"],
        )

    def test_packaged_autoscaler_unit_matches_cli_parser(self) -> None:
        unit = packaged_systemd_units()["ucloud-sandbox-autoscaler.service"]
        exec_start = next(
            line.removeprefix("ExecStart=")
            for line in unit.splitlines()
            if line.startswith("ExecStart=")
        )
        rendered = exec_start.replace(
            "${UCLOUD_INIT_DIRECT_NETWORK}",
            "sandbox",
        )
        rendered = re.sub(r"\$\{[^}]+\}", "1", rendered)
        argv = shlex.split(rendered)

        args = build_parser().parse_args(argv[1:])

        self.assertEqual(args.command, "autoscaler-loop")
        self.assertFalse(hasattr(args, "execute_resumes"))
        self.assertEqual(args.max_storage_native_migrations_per_cycle, 1)
        self.assertEqual(args.init_max_concurrent_image_pulls, 1)
        self.assertEqual(args.gateway_control_bearer_token_file, Path("1"))
        self.assertEqual(args.init_builder_docker_quota_image_gb, 1)
        self.assertEqual(args.init_swap_gb, 1)
        self.assertEqual(args.init_direct_network, "sandbox")


if __name__ == "__main__":
    unittest.main()
