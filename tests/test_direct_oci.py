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

    def test_fails_closed_on_deferred_or_unimplemented_contracts(self) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            for spec, message in (
                (
                    SandboxSpec(
                        id="fork",
                        image=image.image_ref,
                        memory_mb=1024,
                        disk_mb=1024,
                        forkable=True,
                    ),
                    "fork is deferred",
                ),
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


if __name__ == "__main__":
    unittest.main()
