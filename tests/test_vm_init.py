import hashlib
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import ucloud_sandboxes.vm_init as vm_init
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.providers.ucloud.bootstrap import (
    bootstrap_access_from_payload,
    extract_ssh_command,
)
from ucloud_sandboxes.vm_init import (
    RUNTIME_KERNEL_MODULES,
    VmInitOptions,
    parse_vm_init_phases,
    render_vm_init_script,
    stage_vm_init_package_over_ssh,
)


class VmInitTests(unittest.TestCase):
    @staticmethod
    def _options(**overrides: object) -> VmInitOptions:
        values: dict[str, object] = {
            "job_id": "job-1",
            "heartbeat_url": "https://gateway.example/v1/nodes/heartbeat",
            "heartbeat_bearer_token_file": "/run/ucloud/heartbeat-token",
            "heartbeat_bearer_token": "heartbeat-secret",
            "node_control_bearer_token_file": "/run/ucloud/node-token",
            "node_control_bearer_token": "node-secret",
            "deployment_id": "test-deployment",
            "package_spec": "/tmp/node-package.tar.gz",
            "package_sha256": "a" * 64,
            "direct_runsc_commit": "9f653e577965df2ddd13875b5530cd2588661f1c",
            "storage_native_registry_url": "http://registry.internal:5000",
            "docker_quota_image_gb": 440,
            "total_resources": ResourceQuantity(
                vcpu=32,
                memory_mb=128 * 1024,
                disk_mb=2_000 * 1024,
            ),
        }
        values.update(overrides)
        return VmInitOptions(**values)

    def test_sandbox_boot_uses_only_verified_bundle(self) -> None:
        script = render_vm_init_script(self._options(direct_network="sandbox"))

        self.assertIn("serve-direct-node-agent", script)
        self.assertIn("A staged node package bundle is required", script)
        self.assertIn("Node package bundle checksum does not match", script)
        self.assertIn("Verified pinned Docker/gVisor bundle", script)
        self.assertIn("install_bundled_runtime", script)
        self.assertIn("bundle-verified patched runsc", script)
        self.assertIn("bundle-verified storage-native backend", script)
        self.assertIn("Activating bundled ucloud-sandboxes runtime", script)
        self.assertIn("--storage-native-socket", script)
        self.assertIn("--volume-mount-root", script)
        self.assertNotIn("apt-get update", script)
        self.assertNotIn("package repository", script.lower())
        self.assertNotIn("runtime-conformance", script)
        self.assertNotIn("Preassembled runtime unavailable", script)
        self.assertNotIn("installed-package.fingerprint", script)
        self.assertNotIn("serve-node-agent", script)
        self.assertNotIn("legacy", script.lower())

    def test_builder_keeps_image_build_runtime(self) -> None:
        script = render_vm_init_script(
            self._options(
                enable_image_builds=True,
                buildx_direct_push=True,
                buildx_cache_ref="registry.internal:5000/cache/buildkit",
                cpu_overcommit=4,
                memory_overcommit=2,
                docker_quota_image_gb=200,
            )
        )

        self.assertIn("serve-builder-agent", script)
        self.assertIn("--buildx-direct-push", script)
        self.assertIn(
            "--buildx-cache-ref registry.internal:5000/cache/buildkit", script
        )
        self.assertIn("SupplementaryGroups=docker", script)
        self.assertNotIn("ucloud-storage-native.service\nRestart=always", script)

    def test_bootstrap_auth_and_identity_are_mandatory(self) -> None:
        required = {
            "deployment_id": "deployment id",
            "heartbeat_bearer_token_file": "heartbeat bearer token file",
            "heartbeat_bearer_token": "heartbeat bearer token",
            "node_control_bearer_token_file": "node control bearer token file",
            "node_control_bearer_token": "node control bearer token",
            "package_sha256": "package sha256",
        }
        for field, message in required.items():
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                render_vm_init_script(self._options(**{field: ""}))

    def test_direct_runtime_requires_exact_capacity(self) -> None:
        with self.assertRaisesRegex(ValueError, "CPU and memory overcommit"):
            render_vm_init_script(self._options(cpu_overcommit=2))
        with self.assertRaisesRegex(ValueError, "bounded Docker image"):
            render_vm_init_script(self._options(docker_quota_image_gb=0))

    def test_embedded_runtime_validator_and_shell_compile(self) -> None:
        script = render_vm_init_script(self._options())
        start = script.index("import hashlib\nimport json\nimport os")
        end = script.index('\nPY\necho "Verified pinned', start)
        compile(script[start:end], "<bundle-validator>", "exec")

        syntax = subprocess.run(
            ["bash", "-n"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_runtime_module_closure_keeps_project_mounts_rebootable(self) -> None:
        self.assertIn("virtiofs", RUNTIME_KERNEL_MODULES)
        self.assertIn("ublk_drv", RUNTIME_KERNEL_MODULES)

    def test_plans_only_running_vm_with_ssh(self) -> None:
        payload = {
            "id": "123",
            "status": {"state": "RUNNING"},
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }
        plan = bootstrap_access_from_payload(payload)
        self.assertTrue(plan.runnable)
        self.assertEqual(plan.command, "ssh ucloud@example -p 22")
        self.assertEqual(extract_ssh_command(payload), plan.command)

        payload["status"]["state"] = "IN_QUEUE"
        self.assertFalse(bootstrap_access_from_payload(payload).runnable)

    def test_parses_machine_readable_phase_timings(self) -> None:
        phases, total = parse_vm_init_phases(
            "UCLOUD_INIT_PHASE name=runtime-bundle duration_ms=17321 total_ms=19002\n"
            "UCLOUD_INIT_PHASE name=docker-daemon duration_ms=823 total_ms=24100\n"
        )
        self.assertEqual(phases, {"runtime-bundle": 17321, "docker-daemon": 823})
        self.assertEqual(total, 24100)

    def test_stages_bundle_with_digest(self) -> None:
        calls: list[tuple[tuple[str, ...], bytes | None]] = []

        class Completed:
            def __init__(self, returncode: int) -> None:
                self.returncode = returncode

        def fake_run(command, *, stdin=None, check=None, timeout=None):
            del check, timeout
            body = stdin.read() if stdin is not None else None
            calls.append((tuple(command), body))
            return Completed(1 if stdin is None else 0)

        original = vm_init.subprocess.run
        vm_init.subprocess.run = fake_run
        try:
            with TemporaryDirectory() as raw_dir:
                package = Path(raw_dir) / "node-package.tar.gz"
                package.write_bytes(b"verified-bundle")
                result = stage_vm_init_package_over_ssh(
                    "ssh ucloud@example -p 22",
                    self._options(package_spec=str(package)),
                    timeout_seconds=10,
                )
        finally:
            vm_init.subprocess.run = original

        assert result is not None
        self.assertEqual(
            result.package_sha256,
            hashlib.sha256(b"verified-bundle").hexdigest(),
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][1], b"verified-bundle")


if __name__ == "__main__":
    unittest.main()
