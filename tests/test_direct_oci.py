import os
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder, DirectOciConfigError
from ucloud_sandboxes.image_rootfs import DockerImageConfig, MaterializedRootfs
from ucloud_sandboxes.sandbox import (
    SandboxFilesystemSpec,
    SandboxSecuritySpec,
    SandboxSpec,
)

PROPERTY_SETTINGS = settings(max_examples=50, deadline=None, derandomize=True)
ENV_KEYS = st.from_regex(r"[A-Z][A-Z0-9_]{0,11}", fullmatch=True)
SAFE_TEXT = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=126),
    max_size=20,
)
CAPABILITIES = (
    "AUDIT_WRITE",
    "CHOWN",
    "DAC_OVERRIDE",
    "KILL",
    "NET_ADMIN",
    "SETUID",
    "SYS_CHROOT",
)


def _stat_with_uid(info: os.stat_result, uid: int) -> os.stat_result:
    fields = list(info)
    fields[4] = uid
    return os.stat_result(fields)


@contextmanager
def reported_file_uid(path: Path, uid: int):
    """Report only a fixture's otherwise-uncontrollable owner deterministically."""
    real_path_stat = Path.stat
    real_fstat = os.fstat
    expected = real_path_stat(path)

    def stat_with_fixture_uid(candidate: Path, *args, **kwargs):
        info = real_path_stat(candidate, *args, **kwargs)
        if (info.st_dev, info.st_ino) == (expected.st_dev, expected.st_ino):
            return _stat_with_uid(info, uid)
        return info

    def fstat_with_fixture_uid(fd):
        info = real_fstat(fd)
        if (info.st_dev, info.st_ino) == (expected.st_dev, expected.st_ino):
            return _stat_with_uid(info, uid)
        return info

    with (
        patch.object(Path, "stat", stat_with_fixture_uid),
        patch("ucloud_sandboxes.direct_oci.os.fstat", fstat_with_fixture_uid),
    ):
        yield


class DirectOciConfigTests(unittest.TestCase):
    def image(
        self,
        root: Path,
        *,
        env: tuple[str, ...] = ("PATH=/usr/bin", "IMAGE_ONLY=yes"),
    ) -> MaterializedRootfs:
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
                env=env,
                working_dir="/image-work",
                user="123:456",
            ),
        )

    def property_spec(self, image: MaterializedRootfs, **changes) -> SandboxSpec:
        values = {
            "id": "property",
            "image": image.image_ref,
            "command": ("true",),
            "memory_mb": 512,
            "disk_mb": 512,
            "security": SandboxSecuritySpec(init=False),
        }
        values.update(changes)
        return SandboxSpec(**values)

    @PROPERTY_SETTINGS
    @given(
        env=st.dictionaries(ENV_KEYS, SAFE_TEXT, max_size=8),
        labels=st.dictionaries(ENV_KEYS, SAFE_TEXT, max_size=5),
        cap_add=st.sets(st.sampled_from(CAPABILITIES), max_size=5),
    )
    def test_output_is_deterministic_for_equivalent_inputs(
        self,
        env: dict[str, str],
        labels: dict[str, str],
        cap_add: set[str],
    ) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            forward = self.property_spec(
                image,
                env=dict(env.items()),
                labels=dict(labels.items()),
                security=SandboxSecuritySpec(
                    cap_drop=(), cap_add=tuple(cap_add), init=False
                ),
            )
            reverse = self.property_spec(
                image,
                env=dict(reversed(env.items())),
                labels=dict(reversed(labels.items())),
                security=SandboxSecuritySpec(
                    cap_drop=(), cap_add=tuple(reversed(tuple(cap_add))), init=False
                ),
            )

            builder = DirectOciConfigBuilder()
            self.assertEqual(
                builder.build(forward, image), builder.build(reverse, image)
            )
            self.assertEqual(
                builder.build(forward, image), builder.build(forward, image)
            )

    @PROPERTY_SETTINGS
    @given(key=ENV_KEYS, image_value=SAFE_TEXT, request_value=SAFE_TEXT)
    def test_request_environment_overrides_image_environment(
        self,
        key: str,
        image_value: str,
        request_value: str,
    ) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(
                Path(raw),
                env=("image_sentinel=image", f"{key}={image_value}"),
            )
            spec = self.property_spec(
                image,
                env={key: request_value},
            )

            config = DirectOciConfigBuilder().build(spec, image)
            environment = dict(item.split("=", 1) for item in config["process"]["env"])

            self.assertEqual(environment[key], request_value)
            self.assertEqual(environment["image_sentinel"], "image")
            self.assertEqual(len(environment), len(config["process"]["env"]))

    @PROPERTY_SETTINGS
    @given(
        cap_drop=st.sets(st.sampled_from(CAPABILITIES)),
        cap_add=st.sets(st.sampled_from(CAPABILITIES)),
    )
    def test_capability_add_drop_obeys_set_algebra(
        self,
        cap_drop: set[str],
        cap_add: set[str],
    ) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))

            def build_capabilities(
                *, drops: tuple[str, ...], adds: tuple[str, ...]
            ) -> dict[str, list[str]]:
                spec = self.property_spec(
                    image,
                    security=SandboxSecuritySpec(
                        cap_drop=drops, cap_add=adds, init=False
                    ),
                )
                return DirectOciConfigBuilder().build(spec, image)["process"][
                    "capabilities"
                ]

            baseline = set(build_capabilities(drops=(), adds=())["effective"])
            capabilities = build_capabilities(
                drops=tuple(cap_drop), adds=tuple(cap_add)
            )
            expected = (baseline - {f"CAP_{item}" for item in cap_drop}) | {
                f"CAP_{item}" for item in cap_add
            }

            for actual in capabilities.values():
                self.assertEqual(actual, sorted(expected))

    @PROPERTY_SETTINGS
    @given(
        network_mode=st.sampled_from(("none", "sandbox")),
        namespace=st.from_regex(r"/[a-z][a-z0-9/-]{0,20}", fullmatch=True),
    )
    def test_config_has_exactly_one_correct_network_namespace(
        self,
        network_mode: str,
        namespace: str,
    ) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            external = network_mode == "sandbox"
            namespace_path = Path(namespace)
            config = DirectOciConfigBuilder(network_mode=network_mode).build(
                self.property_spec(
                    image,
                    network="bridge" if external else "none",
                ),
                image,
                network_namespace_path=namespace_path if external else None,
            )

            networks = [
                item
                for item in config["linux"]["namespaces"]
                if item["type"] == "network"
            ]
            self.assertEqual(len(networks), 1)
            if external:
                self.assertEqual(
                    networks[0], {"type": "network", "path": str(namespace_path)}
                )
            else:
                self.assertEqual(networks[0], {"type": "network"})

    @PROPERTY_SETTINGS
    @given(
        uid=st.integers(min_value=0, max_value=2**32 - 2),
        gid=st.integers(min_value=0, max_value=2**32 - 2),
        memory_mb=st.integers(min_value=1, max_value=1_000_000),
        disk_mb=st.integers(min_value=1, max_value=1_000_000),
        cpu_millis=st.integers(min_value=1, max_value=128_000),
    )
    def test_uid_gid_and_resources_are_converted_to_oci_units(
        self,
        uid: int,
        gid: int,
        memory_mb: int,
        disk_mb: int,
        cpu_millis: int,
    ) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            config = DirectOciConfigBuilder().build(
                self.property_spec(
                    image,
                    memory_mb=memory_mb,
                    cpus=cpu_millis / 1000,
                    disk_mb=disk_mb,
                    security=SandboxSecuritySpec(user=f"{uid}:{gid}", init=False),
                    filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
                ),
                image,
            )

            resources = config["linux"]["resources"]
            self.assertEqual(
                config["process"]["user"],
                {"uid": uid, "gid": gid},
            )
            self.assertEqual(
                resources["memory"],
                {"limit": memory_mb * 1024**2, "swap": memory_mb * 1024**2},
            )
            self.assertEqual(
                resources["cpu"],
                {"period": 100_000, "quota": round(cpu_millis / 1000 * 100_000)},
            )
            workspace = next(
                mount
                for mount in config["mounts"]
                if mount["destination"] == "/workspace"
            )
            self.assertIn(f"size={disk_mb * 1024**2}", workspace["options"])

    @PROPERTY_SETTINGS
    @given(value=SAFE_TEXT)
    def test_nul_values_fail_closed(self, value: str) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            for spec in (
                self.property_spec(
                    image,
                    command=("true", value + "\0"),
                ),
                self.property_spec(
                    image,
                    env={"PAYLOAD": value + "\0"},
                ),
            ):
                with self.assertRaises(ValueError):
                    DirectOciConfigBuilder().build(spec, image)

            with self.assertRaises(ValueError):
                DirectOciConfigBuilder(network_mode="sandbox").build(
                    self.property_spec(image, network="bridge"),
                    image,
                    network_namespace_path=Path(f"/run/netns/{value}\0"),
                )

    @PROPERTY_SETTINGS
    @given(
        path=st.text(min_size=1, max_size=20).filter(
            lambda value: not value.startswith("/")
        )
    )
    def test_relative_runtime_paths_fail_closed(self, path: str) -> None:
        with TemporaryDirectory() as raw:
            image = self.image(Path(raw))
            with self.assertRaises(ValueError):
                DirectOciConfigBuilder().build(
                    self.property_spec(image, working_dir=path), image
                )

            with self.assertRaises(ValueError):
                DirectOciConfigBuilder(init_binary=Path(path))

            with self.assertRaises(ValueError):
                DirectOciConfigBuilder(network_mode="sandbox").build(
                    self.property_spec(image, network="bridge"),
                    image,
                    network_namespace_path=Path(path),
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
                with (
                    self.subTest(spec=spec.id),
                    self.assertRaisesRegex(DirectOciConfigError, message),
                ):
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

            with reported_file_uid(init, 0):
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

    def test_managed_process_uses_trusted_pid1_and_preserves_workload_identity(
        self,
    ) -> None:
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

            with reported_file_uid(managed_init, 0):
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
                config["annotations"]["dev.ucloud-sandboxes.managed-process.uid"],
                "123",
            )
            self.assertEqual(
                config["annotations"]["dev.ucloud-sandboxes.managed-process.gid"],
                "456",
            )
            self.assertIn("CAP_SETUID", config["process"]["capabilities"]["effective"])
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

            with reported_file_uid(init, 0):
                builder.install_init(image.rootfs, enabled=True)

            self.assertEqual(outside.read_bytes(), b"untouched")
            self.assertFalse((image.rootfs / ".ucloud-init").is_symlink())
            self.assertEqual(
                (image.rootfs / ".ucloud-init").read_bytes(),
                b"trusted-init",
            )

    def test_init_validation_rejects_untrusted_sources(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            image = self.image(root)
            spec = SandboxSpec(
                id="untrusted-init",
                image=image.image_ref,
                command=("true",),
                memory_mb=512,
                disk_mb=512,
            )
            non_executable = root / "non-executable"
            non_executable.write_bytes(b"init")
            non_executable.chmod(0o644)
            writable = root / "writable"
            writable.write_bytes(b"init")
            writable.chmod(0o775)
            target = root / "target"
            target.write_bytes(b"init")
            target.chmod(0o755)
            symlink = root / "symlink"
            symlink.symlink_to(target)

            for source in (non_executable, writable, symlink):
                with (
                    self.subTest(source=source.name),
                    reported_file_uid(source, 0),
                    self.assertRaisesRegex(
                        DirectOciConfigError,
                        "root-owned, executable, and immutable",
                    ),
                ):
                    DirectOciConfigBuilder(init_binary=source).build(spec, image)

            with (
                reported_file_uid(target, 1234),
                self.assertRaisesRegex(DirectOciConfigError, "root-owned"),
            ):
                DirectOciConfigBuilder(init_binary=target).build(spec, image)

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
