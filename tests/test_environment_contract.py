from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.environment_contract import describe_environment
from ucloud_sandboxes.sandbox import SandboxSpec
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.guest_identity import resolve_groups


class EnvironmentContractTests(unittest.TestCase):
    def spec(self, **kw):
        return SandboxSpec.from_dict(
            {"id": "probe", "image": "test", "memory_mb": 512, "disk_mb": 2048, **kw}
        )

    def test_new_contracts_require_capable_nodes_during_placement(self):
        from ucloud_sandboxes.control_plane import _sandbox_required_capabilities
        from ucloud_sandboxes.capabilities import (
            ENVIRONMENT_CONTRACT_CAPABILITY,
            STATIC_FILE_MANAGEMENT_CAPABILITY,
        )

        self.assertEqual(_sandbox_required_capabilities(self.spec().to_dict()), ())
        for overrides in (
            {"dns_servers": ["9.9.9.9"]},
            {"profile": "linux_session"},
            {"security": {"supplementary_groups": ["42"]}},
        ):
            self.assertIn(
                ENVIRONMENT_CONTRACT_CAPABILITY,
                _sandbox_required_capabilities(self.spec(**overrides).to_dict()),
            )
        required = _sandbox_required_capabilities(
            self.spec(filesystem={"management_helper": "static"}).to_dict()
        )
        self.assertIn(STATIC_FILE_MANAGEMENT_CAPABILITY, required)
        self.assertIn(ENVIRONMENT_CONTRACT_CAPABILITY, required)

    def test_unknown_and_unqualified_requirements_fail_closed(self):
        for feature in (
            "posix-acl",
            "linux-kernel",
            "imaginary-feature",
            "framework-network-policy",
        ):
            spec = self.spec(required_features=[feature])
            self.assertFalse(describe_environment(spec)["requirements_satisfied"])
            with self.assertRaisesRegex(ValueError, feature):
                spec.validate()
        self.spec(required_features=["network-off"], network="none").validate()
        with self.assertRaises(ValueError):
            self.spec(required_features=["network-off"]).validate()

    def test_report_does_not_echo_environment_or_image_credentials(self):
        report = str(
            describe_environment(
                self.spec(env={"TOKEN": "secret-value"}, image="secret-image")
            )
        )
        self.assertNotIn("secret-value", report)
        self.assertNotIn("secret-image", report)
        self.assertIn("unresolved", report)

    def test_explicit_mounts_roundtrip_and_preserve_legacy(self):
        spec = self.spec(filesystem={"shm_mb": 1024, "workspace_storage": "image"})
        spec.validate()
        spec = SandboxSpec.from_dict(spec.to_dict())
        mounts = DirectOciConfigBuilder._mounts(spec)
        self.assertNotIn("/workspace", [m["destination"] for m in mounts])
        shm = next(m for m in mounts if m["destination"] == "/dev/shm")
        self.assertIn("size=1048576k", shm["options"])
        for fs in ({"workspace_storage": "tmpfs"}, {"enforce_disk_quota": True}):
            spec = self.spec(filesystem=fs)
            self.assertTrue(spec.filesystem.workspace_is_tmpfs)
            self.assertTrue(describe_environment(spec)["warnings"])
            self.assertIn(
                "/workspace",
                [m["destination"] for m in DirectOciConfigBuilder._mounts(spec)],
            )
        with self.assertRaises(ValueError):
            self.spec(
                filesystem={"enforce_disk_quota": True, "workspace_storage": "image"}
            ).validate()
        with self.assertRaises(ValueError):
            self.spec(filesystem={"shm_mb": 0}).validate()

    def test_explicit_groups_are_image_local_and_deduplicated(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "etc").mkdir()
            (root / "etc/group").write_text("build:x:789:coder\nother:x:456:coder\n")
            self.assertEqual(resolve_groups(root, ("build", "789", "42")), (789, 42))
            self.assertEqual(resolve_groups(root, ()), ())
            with self.assertRaises(ValueError):
                resolve_groups(root, ("missing",))
            with self.assertRaises(ValueError):
                resolve_groups(root, ("4294967295",))
            (root / "etc/group").unlink()
            (root / "etc/group").symlink_to("/etc/group")
            with self.assertRaises(ValueError):
                resolve_groups(root, ("build",))

    def test_dns_configuration_creates_missing_etc_without_following_symlinks(self):
        spec = self.spec(dns_servers=["9.9.9.9"])
        spec.validate()
        self.assertEqual(
            SandboxSpec.from_dict(spec.to_dict()).dns_servers, ("9.9.9.9",)
        )
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            builder = DirectOciConfigBuilder()
            builder.prepare_network_files(root, spec=spec)
            self.assertEqual(
                (root / "etc/resolv.conf").read_text(),
                "nameserver 9.9.9.9\noptions timeout:2 attempts:2\n",
            )
            (root / "etc/resolv.conf").unlink()
            (root / "etc").rmdir()
            (root / "etc").symlink_to("/etc")
            with self.assertRaises(ValueError):
                builder.prepare_network_files(root, spec=spec)
        for value in ("9.9.9.9\nnameserver 1.1.1.1", "example.com", "::1"):
            with self.assertRaises(ValueError):
                self.spec(dns_servers=[value]).validate()

    def test_static_helper_admission_rejects_old_artifacts(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        spec = self.spec(filesystem={"management_helper": "static"})
        with self.assertRaises(ValueError):
            DirectOciConfigBuilder().validate_management_helper(spec)
        builder = DirectOciConfigBuilder(
            managed_init_binary=Path("/trusted/node-helper")
        )
        with (
            patch.object(DirectOciConfigBuilder, "_validate_init_binary"),
            patch(
                "ucloud_sandboxes.direct_oci.subprocess.run",
                return_value=SimpleNamespace(returncode=1),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "does not support"):
                builder.validate_management_helper(spec)

    def test_static_file_dispatch_never_uses_image_shell_tools(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from ucloud_sandboxes.direct_service import DirectSandboxService

        service = object.__new__(DirectSandboxService)
        spec = self.spec(filesystem={"management_helper": "static"})
        service._require_registration = Mock(return_value=SimpleNamespace(spec=spec))
        service.exec = Mock(return_value=SimpleNamespace(exit_code=0, stdout=b"data"))
        self.assertEqual(service.read_file("probe", "/file ü", max_bytes=16), b"data")
        self.assertEqual(
            service.exec.call_args.args[1],
            ("/.ucloud-job-init", "files", "read", "/file ü", "16"),
        )
        service.write_file("probe", "/file ü", b"data")
        self.assertEqual(
            service.exec.call_args.args[1],
            ("/.ucloud-job-init", "files", "write", "/file ü", "4"),
        )

    def test_omitted_job_cwd_requires_resolved_environment(self):
        from ucloud_sandboxes.managed_process import ManagedProcessStart

        job = ManagedProcessStart.from_dict({"job_id": "test", "argv": ["true"]})
        with self.assertRaises(ValueError):
            job.control_payload(uid=1000, gid=1000)
        self.assertEqual(
            job.control_payload(uid=1000, gid=1000, default_cwd="/custom work")["cwd"],
            "/custom work",
        )
        explicit = ManagedProcessStart.from_dict(
            {"job_id": "test", "argv": ["true"], "cwd": "/"}
        )
        self.assertEqual(
            explicit.control_payload(uid=1000, gid=1000, default_cwd="/custom work")[
                "cwd"
            ],
            "/",
        )

    def test_managed_jobs_reject_unimplemented_group_contract(self):
        with self.assertRaisesRegex(ValueError, "supplementary groups"):
            self.spec(
                parkable=True,
                managed_process=True,
                security={"supplementary_groups": ["42"]},
            ).validate()


if __name__ == "__main__":
    unittest.main()
