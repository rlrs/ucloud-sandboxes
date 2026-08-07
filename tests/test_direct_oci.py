from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder, DirectOciConfigError
from ucloud_sandboxes.image_rootfs import DockerImageConfig, MaterializedRootfs
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


class DirectOciConfigTests(unittest.TestCase):
    def image(self, root: Path) -> MaterializedRootfs:
        rootfs = root / "rootfs"
        rootfs.mkdir()
        return MaterializedRootfs(
            image_ref="example/image:latest",
            image_id="sha256:" + "a" * 64,
            rootfs_identity_sha256="b" * 64,
            rootfs=rootfs,
            image_config=DockerImageConfig(
                entrypoint=("/usr/bin/env",),
                command=("sh",),
                env=("PATH=/usr/bin", "IMAGE_ONLY=yes"),
                working_dir="/image-work",
                user="123:456",
            ),
        )

    def test_translates_image_and_product_contract_to_oci(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            spec = SandboxSpec(
                id="sandbox",
                image=image.image_ref,
                command=("python", "-V"),
                env={"IMAGE_ONLY": "overridden", "REQUEST": "yes"},
                working_dir="/workspace",
                memory_mb=2048,
                cpus=1.5,
                disk_mb=4096,
                security=SandboxSecuritySpec(
                    user="1000:1001",
                    cap_drop=("ALL",),
                    cap_add=("CHOWN",),
                    init=False,
                ),
            )

            config = DirectOciConfigBuilder().build(spec, image)

            self.assertEqual(
                config["process"]["args"],
                ["/usr/bin/env", "python", "-V"],
            )
            self.assertEqual(config["process"]["cwd"], "/workspace")
            self.assertEqual(config["process"]["user"], {"uid": 1000, "gid": 1001})
            self.assertEqual(
                config["process"]["capabilities"]["effective"],
                ["CAP_CHOWN"],
            )
            self.assertIn("IMAGE_ONLY=overridden", config["process"]["env"])
            self.assertIn("REQUEST=yes", config["process"]["env"])
            self.assertEqual(
                config["linux"]["resources"]["memory"]["limit"],
                2048 * 1024 * 1024,
            )
            self.assertEqual(
                config["linux"]["resources"]["cpu"],
                {"period": 100_000, "quota": 150_000},
            )

    def test_fails_closed_on_missing_resource_limits(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            for spec, message in (
                (
                    SandboxSpec(
                        id="implicit-disk",
                        image=image.image_ref,
                        memory_mb=1024,
                        security=SandboxSecuritySpec(init=False),
                    ),
                    "explicit memory_mb and disk_mb",
                ),
                (
                    SandboxSpec(
                        id="named-user",
                        image=image.image_ref,
                        memory_mb=1024,
                        disk_mb=1024,
                        security=SandboxSecuritySpec(user="sandbox", init=False),
                    ),
                    "numeric OCI user",
                ),
            ):
                with self.subTest(spec=spec.id):
                    with self.assertRaisesRegex(DirectOciConfigError, message):
                        DirectOciConfigBuilder().build(spec, image)

    def test_preserves_read_only_rootfs_for_overlay_bundle(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            config = DirectOciConfigBuilder().build(
                SandboxSpec(
                    id="readonly",
                    image=image.image_ref,
                    command=("true",),
                    memory_mb=512,
                    disk_mb=512,
                    security=SandboxSecuritySpec(
                        read_only_rootfs=True,
                        init=False,
                    ),
                ),
                image,
            )
            self.assertTrue(config["root"]["readonly"])

    def test_sandbox_network_uses_node_owned_network_namespace(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            config = DirectOciConfigBuilder(network_mode="sandbox").build(
                SandboxSpec(
                    id="sandbox-network",
                    image=image.image_ref,
                    command=("true",),
                    memory_mb=512,
                    disk_mb=512,
                    network="bridge",
                    security=SandboxSecuritySpec(init=False),
                ),
                image,
                network_namespace_path=Path("/run/netns/ucloud-test"),
            )

            network = next(
                item
                for item in config["linux"]["namespaces"]
                if item["type"] == "network"
            )
            self.assertEqual(network["path"], "/run/netns/ucloud-test")

    def test_installs_init_inside_rootfs_without_bind_mount(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            image = self.image(root)
            init = root / "docker-init"
            init.write_bytes(b"trusted-init")
            init.chmod(0o755)
            builder = DirectOciConfigBuilder(init_binary=init)
            spec = SandboxSpec(
                id="with-init",
                image=image.image_ref,
                command=("true",),
                memory_mb=512,
                disk_mb=512,
            )

            with (
                patch.object(DirectOciConfigBuilder, "_validate_init_binary"),
                patch.object(DirectOciConfigBuilder, "_validate_init_stat"),
            ):
                config = builder.build(spec, image)
                builder.install_init(image.rootfs, enabled=True)

            self.assertEqual(
                config["process"]["args"],
                ["/.ucloud-init", "--", "/usr/bin/env", "true"],
            )
            self.assertFalse(
                any(
                    mount["destination"] == "/.ucloud-init"
                    for mount in config["mounts"]
                )
            )
            installed = image.rootfs / ".ucloud-init"
            self.assertEqual(installed.read_bytes(), b"trusted-init")
            self.assertEqual(installed.stat().st_mode & 0o777, 0o755)

    def test_managed_process_uses_trusted_pid1_and_preserves_workload_identity(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            image = self.image(root)
            managed_init = root / "ucloud-sandbox-init"
            managed_init.write_bytes(b"static-managed-init")
            managed_init.chmod(0o755)
            builder = DirectOciConfigBuilder(managed_init_binary=managed_init)
            spec = SandboxSpec(
                id="managed",
                image=image.image_ref,
                memory_mb=512,
                disk_mb=512,
                parkable=True,
                managed_process=True,
                security=SandboxSecuritySpec(user=None),
            )

            with (
                patch.object(DirectOciConfigBuilder, "_validate_init_binary"),
                patch.object(DirectOciConfigBuilder, "_validate_init_stat"),
            ):
                config = builder.build(spec, image)
                builder.install_managed_init(image.rootfs, enabled=True)

            self.assertEqual(
                config["process"]["args"],
                [
                    "/.ucloud-job-init",
                    "supervise",
                    "--state-dir",
                    "/.ucloud-managed",
                ],
            )
            self.assertEqual(config["process"]["user"], {"uid": 0, "gid": 0})
            self.assertEqual(
                config["annotations"][
                    "dev.ucloud-sandboxes.managed-process.uid"
                ],
                "123",
            )
            self.assertEqual(
                config["annotations"][
                    "dev.ucloud-sandboxes.managed-process.gid"
                ],
                "456",
            )
            self.assertIn(
                "CAP_SETUID", config["process"]["capabilities"]["effective"]
            )
            self.assertEqual(
                (image.rootfs / ".ucloud-job-init").read_bytes(),
                b"static-managed-init",
            )

    def test_init_install_replaces_image_symlink(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            image = self.image(root)
            init = root / "docker-init"
            init.write_bytes(b"trusted-init")
            init.chmod(0o755)
            outside = root / "outside"
            outside.write_bytes(b"untouched")
            (image.rootfs / ".ucloud-init").symlink_to(outside)
            builder = DirectOciConfigBuilder(init_binary=init)

            with (
                patch.object(DirectOciConfigBuilder, "_validate_init_binary"),
                patch.object(DirectOciConfigBuilder, "_validate_init_stat"),
            ):
                builder.install_init(image.rootfs, enabled=True)

            self.assertEqual(outside.read_bytes(), b"untouched")
            self.assertFalse((image.rootfs / ".ucloud-init").is_symlink())
            self.assertEqual(
                (image.rootfs / ".ucloud-init").read_bytes(),
                b"trusted-init",
            )

    def test_prepares_sdk_workspace_without_following_image_symlinks(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            image = self.image(root)
            builder = DirectOciConfigBuilder()
            spec = SandboxSpec(
                id="workspace",
                image=image.image_ref,
                command=("true",),
                memory_mb=512,
                disk_mb=512,
                security=SandboxSecuritySpec(user="1000:1000", init=False),
            )

            builder.prepare_workspace(image.rootfs, spec=spec)

            workspace = image.rootfs / "workspace"
            self.assertTrue(workspace.is_dir())
            self.assertEqual(workspace.stat().st_mode & 0o777, 0o777)

            workspace.rmdir()
            outside = root / "outside"
            outside.mkdir(mode=0o700)
            workspace.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(
                DirectOciConfigError,
                "failed to prepare direct-runtime sandbox workspace",
            ):
                builder.prepare_workspace(image.rootfs, spec=spec)
            self.assertEqual(outside.stat().st_mode & 0o777, 0o700)

    def test_quota_workspace_tmpfs_is_writable_by_non_root_user(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            spec = SandboxSpec.from_dict(
                {
                    "id": "quota-workspace",
                    "image": image.image_ref,
                    "command": ["true"],
                    "memory_mb": 512,
                    "disk_mb": 512,
                    "filesystem": {"enforce_disk_quota": True},
                    "security": {"user": "1000:1000", "init": False},
                }
            )

            config = DirectOciConfigBuilder().build(spec, image)

            workspace = next(
                mount
                for mount in config["mounts"]
                if mount["destination"] == "/workspace"
            )
            self.assertIn("mode=1777", workspace["options"])


if __name__ == "__main__":
    unittest.main()
