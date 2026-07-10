import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import ucloud_sandboxes.vm_init as vm_init
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.vm_init import (
    RUNTIME_KERNEL_MODULES,
    VmInitOptions,
    extract_ssh_command,
    extract_ssh_command_from_text,
    parse_vm_init_phases,
    plan_vm_init,
    render_vm_init_script,
    stage_vm_init_package_over_ssh,
    ssh_init_command,
)


class VmInitTests(unittest.TestCase):
    def test_parses_machine_readable_init_phase_timings(self) -> None:
        phases, total = parse_vm_init_phases(
            "noise\n"
            "UCLOUD_INIT_PHASE name=offline-runtime duration_ms=17321 total_ms=19002\n"
            "UCLOUD_INIT_PHASE name=docker-daemon duration_ms=823 total_ms=24100\n"
        )

        self.assertEqual(
            phases,
            {"offline-runtime": 17321, "docker-daemon": 823},
        )
        self.assertEqual(total, 24100)

    def test_extracts_ssh_access_update(self) -> None:
        payload = {
            "updates": [
                {"status": "Starting"},
                {"status": "SSH Access: ssh ucloud@ssh.cloud.sdu.dk -p 41231"},
            ]
        }

        self.assertEqual(
            extract_ssh_command(payload),
            "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
        )

    def test_extracts_available_at_update(self) -> None:
        self.assertEqual(
            extract_ssh_command_from_text(
                "SSH: ready\nSSH: Available at: ssh ucloud@ssh.cloud.sdu.dk -p 41231"
            ),
            "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
        )

    def test_plans_runnable_vm_init(self) -> None:
        payload = {
            "id": "123",
            "specification": {
                "application": {"name": "vm-ubuntu", "version": "24.04"},
                "product": {"id": "cpu-amd-zen5-2-vcpu", "category": "cpu-amd-zen5"},
            },
            "status": {
                "state": "RUNNING",
                "jobParametersJson": {"request": {"sshEnabled": False}},
            },
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }

        plan = plan_vm_init(payload)

        self.assertTrue(plan.runnable)
        self.assertEqual(plan.ssh_command, "ssh ucloud@example -p 22")

    def test_plan_requires_announced_ssh_command(self) -> None:
        payload = {
            "id": "123",
            "status": {
                "state": "RUNNING",
                "jobParametersJson": {"request": {"sshEnabled": False}},
            },
            "updates": [{"status": "Your job is now running."}],
        }

        plan = plan_vm_init(payload)

        self.assertFalse(plan.runnable)
        self.assertIn("No SSH access command", plan.reason)

    def test_plan_waits_until_vm_is_running(self) -> None:
        payload = {
            "id": "123",
            "status": {
                "state": "IN_QUEUE",
                "jobParametersJson": {"request": {"sshEnabled": True}},
            },
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }

        plan = plan_vm_init(payload)

        self.assertFalse(plan.runnable)
        self.assertIn("not running", plan.reason)

    def test_renders_idempotent_init_script(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                heartbeat_bearer_token_file="/work/ucloud-sandboxes/state/heartbeat-token",
                heartbeat_bearer_token="SECRET",
                node_control_bearer_token_file="/work/ucloud-sandboxes/state/node-control-token",
                node_control_bearer_token="NODE-SECRET",
                init_authorized_keys=("ssh-ed25519 AAAA gateway",),
                node_id="node-123",
                node_url="http://node-123:8090",
                package_spec="git+https://example.invalid/ucloud-sandboxes.git",
                total_resources=ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=500000),
                cpu_overcommit=4.0,
                docker_insecure_registries=("gateway:5000",),
                host_aliases=("ucloud-sandbox-registry=10.36.125.67",),
                enable_image_builds=True,
                buildx_direct_push=True,
                buildx_cache_ref="gateway:5000/cache/buildkit",
                labels={"pool": "builder"},
            )
        )

        self.assertIn("CONTAINER_PACKAGES+=(docker-ce docker-ce-cli", script)
        self.assertIn(
            'PACKAGES_TO_INSTALL=("${MISSING_BASE_PACKAGES[@]}" "${CONTAINER_PACKAGES[@]}")',
            script,
        )
        self.assertLess(
            script.index("Preparing Docker Engine repository"),
            script.index("Installing base and container packages"),
        )
        self.assertIn("xfsprogs", script)
        self.assertIn("https://storage.googleapis.com/gvisor/releases", script)
        self.assertIn("Init phase complete:", script)
        self.assertIn("UCLOUD_SERVICE_USER=ucloud", script)
        self.assertIn("UCLOUD_HEARTBEAT_BEARER_TOKEN=SECRET", script)
        self.assertIn("Installing heartbeat bearer token", script)
        self.assertIn("Installing node-control bearer token", script)
        self.assertIn("UCLOUD_NODE_CONTROL_BEARER_TOKEN=NODE-SECRET", script)
        self.assertIn("ssh-ed25519 AAAA gateway", script)
        self.assertIn("$UCLOUD_SERVICE_HOME/.ssh/authorized_keys", script)
        self.assertIn("$SUDO usermod -aG docker \"$UCLOUD_SERVICE_USER\"", script)
        self.assertIn("runuser -u \"$UCLOUD_SERVICE_USER\" -- \"$@\"", script)
        self.assertIn("run_as_service_user python3 -m venv \"$UCLOUD_VENV_DIR\"", script)
        self.assertNotIn("$SUDO python3 -m venv \"$UCLOUD_VENV_DIR\"", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE_GB=200", script)
        self.assertIn("UCLOUD_DOCKER_MTU=0", script)
        self.assertIn("UCLOUD_DOCKER_DATA_ROOT=/var/lib/ucloud-sandboxes/docker", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE=/var/lib/ucloud-sandboxes/docker-xfs.img", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_ROOT=/var/lib/ucloud-sandboxes/docker-xfs", script)
        self.assertNotIn("UCLOUD_DOCKER_QUOTA_IMAGE=/work/ucloud-sandboxes/docker-xfs.img", script)
        self.assertIn(
            "UCLOUD_HOST_ALIASES_JSON='[\"ucloud-sandbox-registry=10.36.125.67\"]'",
            script,
        )
        self.assertIn("Installing host aliases", script)
        self.assertIn("# ucloud-sandboxes host-alias ", script)
        self.assertIn(
            "UCLOUD_RUNTIME_CONFORMANCE_FILE=/work/ucloud-sandboxes/state/runtime-conformance.json",
            script,
        )
        self.assertIn("truncate -s \"${UCLOUD_DOCKER_QUOTA_IMAGE_GB}G\"", script)
        self.assertIn("mkfs.xfs -f -m reflink=1", script)
        self.assertIn("mount -o loop,pquota", script)
        self.assertIn('"data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"]', script)
        self.assertIn("cmp -s \"$DOCKER_DAEMON_JSON\" /etc/docker/daemon.json", script)
        self.assertIn("UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON='[\"gateway:5000\"]'", script)
        self.assertIn(
            "export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB UCLOUD_DOCKER_MTU UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON",
            script,
        )
        self.assertIn("detect_default_route_mtu()", script)
        self.assertIn("ip -o route get 1.1.1.1", script)
        self.assertIn("Configuring Docker daemon with bridge MTU $UCLOUD_DOCKER_MTU", script)
        self.assertIn('docker_mtu = int(os.environ.get("UCLOUD_DOCKER_MTU") or "0")', script)
        self.assertIn('config["mtu"] = docker_mtu', script)
        self.assertIn('ip link set docker0 mtu "$UCLOUD_DOCKER_MTU"', script)
        self.assertIn("UCLOUD_DOCKER_MTU=$UCLOUD_DOCKER_MTU", script)
        self.assertIn('config["insecure-registries"] = insecure_registries', script)
        self.assertIn('config["storage-driver"] = "overlay2"', script)
        self.assertIn('"containerd-snapshotter": False', script)
        self.assertIn('"max-concurrent-downloads": 8', script)
        self.assertIn('"max-concurrent-uploads": 8', script)
        self.assertIn("runtime-conformance --sudo --execute --output json", script)
        self.assertIn(
            '"$UCLOUD_AGENT_BIN" runtime-conformance',
            script,
        )
        self.assertNotIn(
            '"$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" runtime-conformance',
            script,
        )
        self.assertIn("--disable-pip-version-check --upgrade", script)
        self.assertIn("package-bundle.json", script)
        self.assertIn(
            'tar -tzf "$UCLOUD_PACKAGE_SPEC" package-bundle.json',
            script,
        )
        self.assertNotIn("| grep -qx 'package-bundle.json'", script)
        self.assertIn("Using offline node package bundle", script)
        self.assertIn("--no-index --find-links", script)
        self.assertIn("offline runtime platform does not match this VM", script)
        self.assertIn("offline runtime file checksum mismatch", script)
        self.assertIn("Verified offline busybox conformance image", script)
        self.assertIn('docker load --input "$UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE"', script)
        self.assertIn("range .RepoDigests", script)
        self.assertIn("probe_image_identity_matches", script)
        self.assertIn(
            "Installing base packages, Docker Engine, and gVisor from verified offline packages",
            script,
        )
        self.assertIn(
            "apt-get install --no-download --no-install-recommends -y",
            script,
        )
        self.assertIn("Activating preassembled ucloud-sandboxes runtime", script)
        self.assertIn("Dpkg::Use-Pty=0", script)
        self.assertIn("dpkg-deb --fsys-tarfile", script)
        self.assertIn("systemctl daemon-reload", script)
        self.assertIn(
            'required_packages_installed "${OFFLINE_REQUIRED_PACKAGES[@]}"',
            script,
        )
        self.assertEqual(script.count("$SUDO apt-get update"), 3)
        self.assertIn("using package repository fallback", script)
        self.assertIn("APT_REPOSITORY_PACKAGES=()", script)
        self.assertIn('if [ "$NEED_DOCKER_REPOSITORY" -eq 1 ]', script)
        self.assertIn('if [ "$NEED_GVISOR_REPOSITORY" -eq 1 ]', script)
        self.assertIn('"$UCLOUD_PACKAGE_INSTALL_SPEC"', script)
        self.assertIn("--runtime-conformance-file ${UCLOUD_RUNTIME_CONFORMANCE_FILE}", script)
        self.assertIn("ucloud-sandbox-node.service", script)
        self.assertIn("User=$UCLOUD_SERVICE_USER", script)
        self.assertIn("Group=$UCLOUD_SERVICE_GROUP", script)
        self.assertIn("SupplementaryGroups=docker", script)
        self.assertIn("ucloud-sandbox-heartbeat.timer", script)
        self.assertIn("systemctl restart ucloud-sandbox-node.service", script)
        self.assertIn("UCLOUD_NODE_AGENT_HOST=0.0.0.0", script)
        self.assertIn("UCLOUD_NODE_URL=http://node-123:8090", script)
        self.assertIn("--host ${UCLOUD_NODE_AGENT_HOST}", script)
        self.assertIn("--node-url ${UCLOUD_NODE_URL}", script)
        self.assertIn(
            "--enable-image-builds --buildx-direct-push "
            "--buildx-cache-ref gateway:5000/cache/buildkit --execute-runtime",
            script,
        )
        self.assertIn(
            "agent-heartbeat --from-node-agent-url http://127.0.0.1:${UCLOUD_NODE_AGENT_PORT}",
            script,
        )
        self.assertIn(
            "--node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE}",
            script,
        )
        self.assertIn(
            "--bearer-token-file ${UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE}",
            script,
        )
        self.assertNotIn("--active 0", script)
        self.assertIn("--label pool=builder", script)

    def test_offline_runtime_validator_python_compiles(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
            )
        )

        start = script.index("import hashlib\nimport json")
        end = script.index("\nPY\n    then", start)
        compile(script[start:end], "<offline-runtime-validator>", "exec")

        start = script.index("import json\nfrom pathlib import Path\nimport sys\n\nruntime =")
        end = script.index("\nPY\n)\"", start)
        compile(script[start:end], "<offline-probe-image-validator>", "exec")

    def test_offline_runtime_validator_checks_files_and_exact_platform(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
            )
        )
        start = script.index("import hashlib\nimport json")
        end = script.index("\nPY\n    then", start)
        code = compile(script[start:end], "<offline-runtime-validator>", "exec")
        packages = [
            "xfsprogs",
            "docker-ce",
            "docker-ce-cli",
            "containerd.io",
            "runsc",
        ]

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            package_dir = root / "runtime" / "debs"
            package_dir.mkdir(parents=True)
            agent_dir = root / "runtime" / "agent"
            agent_dir.mkdir(parents=True)
            agent_archive = agent_dir / "node-agent-runtime.tar"
            agent_archive.write_bytes(b"agent-runtime")
            kernel_release = os.uname().release
            kernel_dir = root / "runtime" / "kernel" / kernel_release
            kernel_dir.mkdir(parents=True)
            xfs_module = kernel_dir / "xfs.ko.zst"
            xfs_module.write_bytes(b"xfs-module")
            overlay_module = kernel_dir / "overlay.ko.zst"
            overlay_module.write_bytes(b"overlay-module")
            files = []
            for name in ("docker-ce", "runsc"):
                package = package_dir / f"{name}_1.0_amd64.deb"
                package.write_bytes(name.encode("utf-8"))
                files.append(
                    {
                        "name": package.name,
                        "size": package.stat().st_size,
                        "sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
                    }
                )
            manifest = root / "package-bundle.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "package_file": "service.whl",
                        "runtime": {
                            "role": "sandbox",
                            "platform": {
                                "os_id": "ubuntu",
                                "version_id": "24.04",
                                "codename": "noble",
                                "architecture": "amd64",
                            },
                            "packages": packages,
                            "files": files,
                            "agent": {
                                "file": "runtime/agent/node-agent-runtime.tar",
                                "python": f"{sys.version_info.major}.{sys.version_info.minor}",
                                "size": agent_archive.stat().st_size,
                                "sha256": hashlib.sha256(
                                    agent_archive.read_bytes()
                                ).hexdigest(),
                            },
                            "kernel": {
                                "release": kernel_release,
                                "load": list(RUNTIME_KERNEL_MODULES),
                                "files": [
                                    {
                                        "name": "xfs.ko.zst",
                                        "size": xfs_module.stat().st_size,
                                        "sha256": hashlib.sha256(
                                            xfs_module.read_bytes()
                                        ).hexdigest(),
                                    },
                                    {
                                        "name": "overlay.ko.zst",
                                        "size": overlay_module.stat().st_size,
                                        "sha256": hashlib.sha256(
                                            overlay_module.read_bytes()
                                        ).hexdigest(),
                                    },
                                ],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            original_argv = sys.argv
            try:
                sys.argv = [
                    "validator",
                    str(manifest),
                    str(root),
                    "ubuntu",
                    "24.04",
                    "noble",
                    "amd64",
                    "",
                ]
                exec(code, {"__name__": "__main__"})
                sys.argv[-2] = "arm64"
                with self.assertRaisesRegex(SystemExit, "platform does not match"):
                    exec(code, {"__name__": "__main__"})
            finally:
                sys.argv = original_argv

    def test_rendered_host_alias_python_compiles(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                host_aliases=("ucloud-sandbox-registry=10.36.125.67",),
            )
        )
        start = script.index("import json\nimport os\nimport sys\n\nhosts_path")
        end = script.index("PY\n  $SUDO install -m 0644 \"$HOSTS_TMP\" /etc/hosts", start)
        compile(script[start:end], "<host-alias-heredoc>", "exec")

    def test_can_disable_quota_backed_docker_storage(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                docker_quota_image_gb=0,
            )
        )

        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE_GB=0", script)
        self.assertIn('if int(os.environ["UCLOUD_DOCKER_QUOTA_IMAGE_GB"]) > 0:', script)

    def test_can_override_docker_mtu(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                docker_mtu=1400,
            )
        )

        self.assertIn("UCLOUD_DOCKER_MTU=1400", script)
        self.assertIn('if [ "$UCLOUD_DOCKER_MTU" -eq 0 ]; then', script)

    def test_runtime_dry_run_omits_execute_runtime(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                runtime_dry_run=True,
            )
        )

        self.assertNotIn("--execute-runtime", script)

    def test_stages_local_package_spec_over_ssh(self) -> None:
        calls: list[dict] = []

        class Completed:
            def __init__(self, returncode: int) -> None:
                self.returncode = returncode

        def fake_run(
            command,
            *,
            stdin=None,
            check=None,
            timeout=None,
        ):
            calls.append(
                {
                    "command": command,
                    "input": stdin.read() if stdin is not None else None,
                    "check": check,
                    "timeout": timeout,
                }
            )
            return Completed(1 if stdin is None else 0)

        original = vm_init.subprocess.run
        vm_init.subprocess.run = fake_run
        try:
            with TemporaryDirectory() as raw_dir:
                package = Path(raw_dir) / "ucloud_sandboxes-0.1.0-py3-none-any.whl"
                package.write_bytes(b"wheel-bytes")

                result = stage_vm_init_package_over_ssh(
                    "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
                    VmInitOptions(
                        job_id="job-1",
                        heartbeat_url="https://control.example/v1/nodes/heartbeat",
                        package_spec=str(package),
                    ),
                    timeout_seconds=10,
                    private_key_file="/work/ucloud-sandboxes/state/ssh/gateway-init",
                )
        finally:
            vm_init.subprocess.run = original

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result.remote_path,
            "/tmp/ucloud-sandboxes-init-packages/job-1/ucloud_sandboxes-0.1.0-py3-none-any.whl",
        )
        self.assertEqual(len(calls), 2)
        self.assertIsNone(calls[0]["input"])
        self.assertEqual(calls[1]["input"], b"wheel-bytes")
        self.assertEqual(calls[1]["timeout"], 10)
        self.assertIn("-i", calls[1]["command"])
        self.assertIn("cat > /tmp/ucloud-sandboxes-init-packages/job-1/", calls[1]["command"][-1])
        self.assertEqual(
            result.package_sha256,
            hashlib.sha256(b"wheel-bytes").hexdigest(),
        )
        self.assertFalse(result.reused)

    def test_reuses_a_matching_staged_package(self) -> None:
        calls: list[tuple] = []

        class Completed:
            returncode = 0

        def fake_run(command, *, check=None, timeout=None):
            calls.append(tuple(command))
            return Completed()

        original = vm_init.subprocess.run
        vm_init.subprocess.run = fake_run
        try:
            with TemporaryDirectory() as raw_dir:
                package = Path(raw_dir) / "node-package.tar.gz"
                package.write_bytes(b"bundle")
                result = stage_vm_init_package_over_ssh(
                    "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
                    VmInitOptions(
                        job_id="job-1",
                        heartbeat_url="https://control.example/v1/nodes/heartbeat",
                        package_spec=str(package),
                    ),
                    timeout_seconds=10,
                )
        finally:
            vm_init.subprocess.run = original

        assert result is not None
        self.assertEqual(len(calls), 1)
        self.assertTrue(result.reused)
        self.assertEqual(result.returncode, 0)
        self.assertIn(".sha256", calls[0][-1])

    def test_builds_ssh_init_command(self) -> None:
        self.assertEqual(
            ssh_init_command(
                "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
                private_key_file="/work/ucloud-sandboxes/state/ssh/gateway-init",
            ),
            (
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-i",
                "/work/ucloud-sandboxes/state/ssh/gateway-init",
                "ucloud@ssh.cloud.sdu.dk",
                "-p",
                "41231",
                "bash",
                "-s",
            ),
        )

    def test_rejects_non_ssh_command(self) -> None:
        with self.assertRaises(ValueError):
            ssh_init_command("curl https://example.invalid")

    def test_rejects_unsafe_service_user(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    service_user="bad user",
                )
            )

    def test_rejects_multiline_init_authorized_key(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    init_authorized_keys=("ssh-ed25519 AAAA\nssh-ed25519 BBBB",),
                )
            )

    def test_rejects_invalid_host_alias(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    host_aliases=("ucloud-sandbox-registry",),
                )
            )

        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    host_aliases=("bad host=10.36.125.67",),
                )
            )

    def test_rejects_negative_docker_mtu(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    docker_mtu=-1,
                )
            )


if __name__ == "__main__":
    unittest.main()
