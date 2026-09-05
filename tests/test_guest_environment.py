from dataclasses import replace
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.guest_identity import resolve_identity
from ucloud_sandboxes.guest_paths import validate_guest_path, validate_workspace_path
from ucloud_sandboxes.image_rootfs import DockerImageConfig, MaterializedRootfs
from ucloud_sandboxes.sandbox import SandboxSpec, linux_host_entrypoint_script


class GuestEnvironmentTests(unittest.TestCase):
    def spec(self, **overrides):
        return SandboxSpec.from_dict(
            {
                "id": "environment",
                "image": "test",
                "memory_mb": 128,
                "disk_mb": 128,
                "security": {"init": False},
                **overrides,
            }
        )

    def test_linux_paths_and_workspace_destinations_are_different_contracts(self):
        for path in (
            "/",
            "/eval.sh",
            "/testbed/project ü/file name:a,b",
            "/tmp/$literal",
        ):
            validate_guest_path("cwd", path)
        for path in ("relative", "/tmp/../etc", "/tmp/\0bad", "/tmp/\nbad"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_guest_path("cwd", path)
        for path in (
            "/",
            "/etc",
            "/proc",
            "/proc/work",
            "/run/app",
            "/.ucloud-managed/jobs",
            "//workspace",
            "/workspace/.",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_workspace_path(path)
        validate_workspace_path("/home/user/my project")

    def test_workspace_preserves_existing_permissions_and_rejects_symlinks(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            workspace = root / "workspace"
            workspace.mkdir(mode=0o750)
            builder = DirectOciConfigBuilder()
            builder.prepare_workspace(root, spec=self.spec())
            self.assertEqual(workspace.stat().st_mode & 0o7777, 0o750)
            workspace.rmdir()
            builder.prepare_workspace(root, spec=self.spec())
            self.assertEqual(workspace.stat().st_mode & 0o7777, 0o1777)
            workspace.rmdir()
            workspace.symlink_to("/tmp", target_is_directory=True)
            with self.assertRaises(ValueError):
                builder.prepare_workspace(root, spec=self.spec())

    def test_cwd_repeated_slashes_never_escape_descriptor_relative_traversal(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            original_open = os.open

            def confined_open(path, flags, *args, **kwargs):
                if "dir_fd" in kwargs:
                    self.assertFalse(str(path).startswith("/"))
                return original_open(path, flags, *args, **kwargs)

            with patch("os.open", side_effect=confined_open):
                builder = DirectOciConfigBuilder()
                for directory in ("//", "///", "/./", "//project//nested/."):
                    builder.prepare_working_directory(root, directory=directory)
            self.assertTrue((root / "project/nested").is_dir())

    def test_named_identity_uses_image_accounts_and_primary_group(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "etc").mkdir()
            (root / "etc/passwd").write_text("coder:x:123:456::/home/coder:/bin/bash\n")
            (root / "etc/group").write_text("build:x:789:\n")
            self.assertEqual(resolve_identity(root, "coder").gid, 456)
            self.assertEqual(resolve_identity(root, "123").gid, 456)
            self.assertEqual(resolve_identity(root, "coder:build").gid, 789)
            self.assertEqual(resolve_identity(root, "coder:42").gid, 42)
            self.assertEqual(resolve_identity(root, "999:1000").uid, 999)
            (root / "etc/passwd").unlink()
            (root / "etc/passwd").symlink_to("/etc/passwd")
            with self.assertRaises(ValueError):
                resolve_identity(root, "coder")

    def test_session_preserves_image_path_and_explicit_environment(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            image = MaterializedRootfs(
                image_ref="test",
                image_id="sha256:" + "a" * 64,
                rootfs_identity_sha256="b" * 64,
                rootfs=root,
                image_config=DockerImageConfig(
                    env=("PATH=/opt/conda/bin:/bin", "HOME=/root")
                ),
            )
            spec = self.spec(
                profile="linux_session", filesystem={"workspace_path": "/project"}
            )
            config = DirectOciConfigBuilder().build(spec, image)
            env = dict(item.split("=", 1) for item in config["process"]["env"])
            self.assertEqual(env["PATH"], "/opt/conda/bin:/bin")
            self.assertEqual(env["HOME"], "/project")
            self.assertEqual(config["process"]["cwd"], "/project")
            self.assertEqual(config["process"]["args"][:2], ["/bin/sh", "-c"])
            self.assertTrue(config["process"]["noNewPrivileges"])
            self.assertEqual(config["process"]["capabilities"]["effective"], [])
            explicit = DirectOciConfigBuilder().build(
                replace(spec, env={"HOME": "/custom", "PATH": "/custom/bin"}), image
            )
            self.assertIn("HOME=/custom", explicit["process"]["env"])
            self.assertIn("PATH=/custom/bin", explicit["process"]["env"])
            (root / "etc").mkdir()
            (root / "etc/passwd").write_text(
                "coder:x:1000:1000::relative-home:/bin/sh\n"
            )
            malformed_home = DirectOciConfigBuilder().build(spec, image)
            self.assertIn("HOME=/project", malformed_home["process"]["env"])

    def test_partial_profile_overrides_preserve_other_profile_fields(self):
        spec = self.spec(
            profile="linux_host",
            security={"read_only_rootfs": False},
            filesystem={"workspace_path": "/project"},
        )
        self.assertIsNone(spec.security.user)
        self.assertFalse(spec.security.no_new_privileges)
        self.assertEqual(spec.filesystem.tmpfs_mb, 256)
        session = self.spec(profile="linux_session")
        self.assertEqual(session.security.pids_limit, 256)
        self.assertEqual(session.linux_host.writable_paths, ())

    def test_direct_and_json_construction_resolve_the_same_presets(self):
        for profile in ("container", "linux_host", "linux_session"):
            raw = {
                "id": "defaults",
                "image": "test",
                "memory_mb": 128,
                "disk_mb": 128,
                "profile": profile,
            }
            self.assertEqual(SandboxSpec(**raw), SandboxSpec.from_dict(raw))

    def test_bootstrap_fails_when_required_service_is_missing(self):
        result = subprocess.run(
            ["/bin/sh", "-c", linux_host_entrypoint_script()],
            env={
                "PATH": "/nonexistent",
                "UCLOUD_SANDBOX_ENABLE_CRON": "1",
                "UCLOUD_SANDBOX_KEEP_ALIVE": "0",
            },
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cron was requested", result.stderr)

    def test_bootstrap_does_not_reset_image_path_or_execute_profile_files(self):
        result = subprocess.run(
            [
                "/bin/sh",
                "-c",
                linux_host_entrypoint_script(),
                "init",
                "/bin/sh",
                "-c",
                'printf "%s" "$PATH"',
            ],
            env={"PATH": "/custom/conda/bin:/bin"},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "/custom/conda/bin:/bin")

    def test_file_upload_handles_linux_filenames_and_directory_targets(self):
        service = Mock()

        def execute(sandbox_id, argv, *, input_bytes, **kwargs):
            completed = subprocess.run(argv, input=input_bytes, capture_output=True)
            return Mock(exit_code=completed.returncode)

        service.exec.side_effect = execute
        with TemporaryDirectory() as raw:
            target = Path(raw) / "a space ü ' $file"
            DirectSandboxService.write_file(
                service, "test", str(target), b"binary\0payload"
            )
            self.assertEqual(target.read_bytes(), b"binary\0payload")
            with self.assertRaises(Exception):
                DirectSandboxService.write_file(service, "test", raw, b"data")
            self.assertEqual(os.listdir(raw), [target.name])

    def test_root_level_upload_selects_root_parent(self):
        service = Mock()
        service.exec.return_value = Mock(exit_code=0)
        DirectSandboxService.write_file(service, "test", "/eval.sh", b"echo ok")
        script = service.exec.call_args.args[1][2]
        # Execute the actual parent-selection prefix without writing to host /.
        prefix = script.split('mkdir -p -- "$dir"')[0]
        completed = subprocess.run(
            [
                "/bin/sh",
                "-c",
                prefix + 'printf "%s" "$dir"',
                "test",
                "/ucloud-parent-probe",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "/")
