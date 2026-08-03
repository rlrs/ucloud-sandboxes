import io
import hashlib
import json
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
from threading import Lock, Thread
import time
import unittest

from ucloud_sandboxes.direct_warden import CommandResult, DirectWardenError
from ucloud_sandboxes.image_rootfs import (
    DockerOverlay2RootfsStore,
    DockerRootfsStore,
    GnuTarRootfsExtractor,
    OverlayRootfsManager,
)


IMAGE_DIGEST = "a" * 64
CONTAINER_ID = "b" * 64


class FakeRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.fail_export = False
        self.exporter_inventory = ""
        self.mounted: set[str] = set()
        self.not_mounted_returncode = 1

    def run(self, argv, *, timeout):
        del timeout
        command = tuple(str(item) for item in argv)
        self.commands.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return CommandResult(
                command,
                0,
                json.dumps(
                    [
                        {
                            "Id": f"sha256:{IMAGE_DIGEST}",
                            "Config": {
                                "Entrypoint": ["/usr/bin/env"],
                                "Cmd": ["true"],
                                "Env": ["PATH=/usr/bin"],
                                "WorkingDir": "/workspace",
                                "User": "1000:1000",
                            },
                        }
                    ]
                ),
            )
        if command[:2] == ("docker", "ps"):
            return CommandResult(command, 0, self.exporter_inventory)
        if command[:2] == ("docker", "create"):
            return CommandResult(command, 0, CONTAINER_ID + "\n")
        if command[:2] == ("docker", "export"):
            if self.fail_export:
                return CommandResult(command, 1, stderr="injected export failure")
            output = Path(
                next(
                    item.split("=", 1)[1]
                    for item in command
                    if item.startswith("--output=")
                )
            )
            output.write_bytes(b"fake tar")
        if command[0] == "mount":
            self.mounted.add(command[-1])
        elif command[0] == "mountpoint":
            return CommandResult(
                command,
                (
                    0
                    if command[-1] in self.mounted
                    else self.not_mounted_returncode
                ),
            )
        elif command[0] == "umount":
            self.mounted.discard(command[-1])
        return CommandResult(command, 0)


class FakeExtractor:
    def __init__(self) -> None:
        self.calls = 0

    def extract(self, archive: Path, destination: Path) -> None:
        self.calls += 1
        if not archive.is_file():
            raise AssertionError("docker export did not create an archive")
        (destination / "bin").mkdir()
        (destination / "bin" / "tool").write_bytes(b"tool")


class ConcurrentRunner(FakeRunner):
    def run(self, argv, *, timeout):
        command = tuple(str(item) for item in argv)
        if command[:3] == ("docker", "image", "inspect"):
            digest = hashlib.sha256(command[-1].encode("utf-8")).hexdigest()
            self.commands.append(command)
            return CommandResult(
                command,
                0,
                json.dumps([{"Id": f"sha256:{digest}", "Config": {}}]),
            )
        if command[:2] == ("docker", "create"):
            self.commands.append(command)
            return CommandResult(
                command,
                0,
                hashlib.sha256(command[-1].encode("utf-8")).hexdigest() + "\n",
            )
        return super().run(argv, timeout=timeout)


class ConcurrentExtractor(FakeExtractor):
    def __init__(self) -> None:
        super().__init__()
        self.guard = Lock()
        self.active = 0
        self.max_active = 0

    def extract(self, archive: Path, destination: Path) -> None:
        with self.guard:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.1)
            super().extract(archive, destination)
        finally:
            with self.guard:
                self.active -= 1


class Overlay2Runner(FakeRunner):
    def __init__(self, docker_root: Path) -> None:
        super().__init__()
        self.docker_root = docker_root.resolve()
        self.top = self.docker_root / "overlay2" / "top" / "diff"
        self.middle = self.docker_root / "overlay2" / "middle" / "diff"
        self.base = self.docker_root / "overlay2" / "base" / "diff"
        for path in (self.top, self.middle, self.base):
            path.mkdir(parents=True)

    def run(self, argv, *, timeout):
        command = tuple(str(item) for item in argv)
        if command[:3] == ("docker", "image", "inspect"):
            self.commands.append(command)
            return CommandResult(
                command,
                0,
                json.dumps(
                    [
                        {
                            "Id": f"sha256:{IMAGE_DIGEST}",
                            "Config": {"Cmd": ["true"]},
                            "GraphDriver": {
                                "Name": "overlay2",
                                "Data": {
                                    "UpperDir": str(self.top),
                                    "LowerDir": (
                                        f"{self.middle}:{self.base}"
                                    ),
                                },
                            },
                        }
                    ]
                ),
            )
        if command == ("docker", "info", "--format={{json .Driver}}"):
            self.commands.append(command)
            return CommandResult(command, 0, json.dumps("overlay2"))
        return super().run(command, timeout=timeout)


class ImageRootfsTests(unittest.TestCase):
    def test_overlay2_rootfs_identity_remains_migration_compatible(self) -> None:
        image_id = f"sha256:{IMAGE_DIGEST}"
        legacy_identity = hashlib.sha256(
            b"ucloud-docker-export-rootfs-v1\0" + image_id.encode("ascii")
        ).hexdigest()

        self.assertEqual(
            DockerOverlay2RootfsStore._rootfs_identity(image_id),
            legacy_identity,
        )

    def test_overlay2_store_mounts_shared_layers_without_export(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=docker_root.resolve(),
            )

            first = store.materialize("example/image:latest")
            second = store.materialize("example/image:latest")

            self.assertEqual(first, second)
            self.assertEqual(first.image_config.command, ("true",))
            mounts = [command for command in runner.commands if command[0] == "mount"]
            self.assertEqual(len(mounts), 1)
            options = mounts[0][mounts[0].index("-o") + 1]
            self.assertEqual(
                options,
                "ro,lowerdir="
                f"{runner.top}:{runner.middle}:{runner.base}",
            )
            self.assertIn(
                (
                    "docker",
                    "image",
                    "tag",
                    f"sha256:{IMAGE_DIGEST}",
                    f"ucloud-sandbox-rootfs-cache:{IMAGE_DIGEST}",
                ),
                runner.commands,
            )
            self.assertEqual(
                sum(command[:3] == ("docker", "image", "tag") for command in runner.commands),
                1,
            )
            self.assertFalse(
                any(command[:2] == ("docker", "create") for command in runner.commands)
            )
            self.assertFalse(
                any(command[:2] == ("docker", "export") for command in runner.commands)
            )

    def test_overlay2_store_remounts_completed_image_after_restart(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=docker_root.resolve(),
            )
            first = store.materialize("example/image:latest")
            runner.mounted.clear()

            restored = store.materialize(f"sha256:{IMAGE_DIGEST}")

            self.assertEqual(restored.rootfs, first.rootfs)
            self.assertEqual(
                len([command for command in runner.commands if command[0] == "mount"]),
                2,
            )

    def test_overlay2_startup_reconciles_durable_image_lease(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=docker_root.resolve(),
            )
            store.materialize("example/image:latest")
            runner.commands.clear()

            store.reconcile_export_containers()

            self.assertIn(
                (
                    "docker",
                    "image",
                    "tag",
                    f"sha256:{IMAGE_DIGEST}",
                    f"ucloud-sandbox-rootfs-cache:{IMAGE_DIGEST}",
                ),
                runner.commands,
            )

    def test_overlay2_store_rejects_layers_outside_docker_root(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            escaped = root / "escaped"
            escaped.mkdir()
            runner.top = escaped
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=docker_root.resolve(),
            )

            with self.assertRaisesRegex(DirectWardenError, "escaped"):
                store.materialize("example/image:latest")

    def test_distinct_cold_rootfs_exports_run_concurrently(self) -> None:
        with TemporaryDirectory() as raw:
            runner = ConcurrentRunner()
            extractor = ConcurrentExtractor()
            store = DockerRootfsStore(
                (Path(raw) / "cache").resolve(),
                runner=runner,
                extractor=extractor,
                max_concurrent_exports=4,
            )
            results: list[object] = []
            threads = [
                Thread(
                    target=lambda image=image: results.append(
                        store.materialize(image)
                    )
                )
                for image in ("image:a", "image:b", "image:c", "image:d")
            ]

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(len(results), 4)
            self.assertGreaterEqual(extractor.max_active, 2)
            self.assertEqual(
                sum(command[:2] == ("docker", "ps") for command in runner.commands),
                1,
            )

    def test_docker_only_materializes_one_content_addressed_rootfs(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            extractor = FakeExtractor()
            store = DockerRootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                extractor=extractor,
            )

            first = store.materialize("example/image:latest")
            second = store.materialize("example/image:latest")

            self.assertEqual(first.rootfs, second.rootfs)
            self.assertEqual(first.image_config.command, ("true",))
            self.assertEqual(first.image_config.working_dir, "/workspace")
            self.assertTrue((first.rootfs / "bin" / "tool").is_file())
            self.assertEqual(extractor.calls, 1)
            verbs = [command[1] for command in runner.commands]
            self.assertEqual(verbs.count("create"), 1)
            self.assertEqual(verbs.count("export"), 1)
            self.assertEqual(verbs.count("rm"), 1)
            self.assertNotIn("start", verbs)
            self.assertNotIn("run", verbs)

    def test_failed_export_removes_container_and_pending_tree(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            runner.fail_export = True
            store = DockerRootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                extractor=FakeExtractor(),
            )

            with self.assertRaisesRegex(DirectWardenError, "export failure"):
                store.materialize("example/image:latest")

            self.assertTrue(
                any(command[:2] == ("docker", "rm") for command in runner.commands)
            )
            self.assertEqual(
                [path.name for path in store.images.iterdir()],
                [],
            )

    def test_reconcile_removes_only_inactive_orphan_exporters(self) -> None:
        with TemporaryDirectory() as raw:
            runner = FakeRunner()
            runner.exporter_inventory = f"{CONTAINER_ID} created\n"
            store = DockerRootfsStore(
                (Path(raw) / "cache").resolve(),
                runner=runner,
                extractor=FakeExtractor(),
            )

            self.assertEqual(store.reconcile_export_containers(), (CONTAINER_ID,))
            self.assertIn(
                ("docker", "rm", "--force", "--volumes", CONTAINER_ID),
                runner.commands,
            )

            runner.exporter_inventory = f"{CONTAINER_ID} running\n"
            with self.assertRaisesRegex(DirectWardenError, "refusing to remove"):
                store.reconcile_export_containers()

    def test_overlay_bundle_uses_shared_lower_and_per_sandbox_upper(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            store = DockerRootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                extractor=FakeExtractor(),
            )
            manager = OverlayRootfsManager(
                store,
                writable_root=(root / "writable").resolve(),
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
            )

            lease = manager.prepare(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                image_ref="example/image:latest",
                config_template={"root": {"path": "unused", "readonly": True}},
            )

            config = json.loads(
                (lease.sandbox.bundle / "config.json").read_text(encoding="utf-8")
            )
            self.assertEqual(config["root"]["path"], "rootfs")
            self.assertTrue(config["root"]["readonly"])
            self.assertEqual(
                lease.sandbox.rootfs_sha256,
                lease.image.rootfs_identity_sha256,
            )
            self.assertTrue(lease.writable_owned_by_manager)
            mount = next(
                command for command in runner.commands if command[0] == "mount"
            )
            options = mount[mount.index("-o") + 1]
            self.assertIn(f"lowerdir={lease.image.rootfs}", options)
            self.assertIn(f"upperdir={lease.upper}", options)
            self.assertEqual(
                lease.upper.stat().st_mode & 0o7777,
                lease.image.rootfs.stat().st_mode & 0o7777,
            )

            manager.park_sandbox(lease.sandbox)
            self.assertNotIn(str(lease.merged), runner.mounted)
            runner.not_mounted_returncode = 32
            manager.resume_sandbox(lease.sandbox)
            self.assertIn(str(lease.merged), runner.mounted)
            mounts = [
                command for command in runner.commands if command[0] == "mount"
            ]
            self.assertEqual(len(mounts), 2)

            manager.release(lease)
            self.assertFalse(lease.sandbox.bundle.exists())
            self.assertFalse(lease.upper.parent.exists())

    def test_overlay_prepare_unmounts_and_removes_partial_state(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            manager = OverlayRootfsManager(
                DockerRootfsStore(
                    (root / "cache").resolve(),
                    runner=runner,
                    extractor=FakeExtractor(),
                ),
                writable_root=(root / "writable").resolve(),
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
            )
            recursive: dict[str, object] = {}
            recursive["recursive"] = recursive

            with self.assertRaises(ValueError):
                manager.prepare(
                    sandbox_id="sandbox-1",
                    sandbox_generation=8,
                    image_ref="example/image:latest",
                    config_template=recursive,
                )

            self.assertTrue(any(command[0] == "umount" for command in runner.commands))
            self.assertEqual(list(manager.bundle_root.iterdir()), [])
            self.assertEqual(list(manager.writable_root.iterdir()), [])

    def test_quota_owned_overlay_requires_and_preserves_precreated_root(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            writable_root = (root / "quota").resolve()
            writable_root.mkdir()
            incarnation = writable_root / "sandbox-1.sandbox-9"
            incarnation.mkdir(mode=0o700)
            manager = OverlayRootfsManager(
                DockerRootfsStore(
                    (root / "cache").resolve(),
                    runner=runner,
                    extractor=FakeExtractor(),
                ),
                writable_root=writable_root,
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
                require_precreated_writable=True,
            )

            lease = manager.prepare(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                image_ref="example/image:latest",
                config_template={"root": {}},
            )
            self.assertFalse(lease.writable_owned_by_manager)
            self.assertEqual(lease.writable, incarnation)
            self.assertEqual(lease.sandbox.memory_directory, incarnation.name)

            manager.release(lease)
            self.assertTrue(incarnation.is_dir())
            self.assertTrue((incarnation / "upper").is_dir())
            self.assertTrue((incarnation / "work").is_dir())

    def test_imported_overlay_preserves_upper_and_parked_generation(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = FakeRunner()
            writable_root = (root / "quota").resolve()
            incarnation = writable_root / "sandbox-1.sandbox-10"
            upper = incarnation / "upper"
            generation = incarnation / "hibernate-3"
            upper.mkdir(parents=True)
            generation.mkdir()
            (upper / "payload").write_bytes(b"migrated")
            (generation / "checkpoint.img").write_bytes(b"checkpoint")
            manager = OverlayRootfsManager(
                DockerRootfsStore(
                    (root / "cache").resolve(),
                    runner=runner,
                    extractor=FakeExtractor(),
                ),
                writable_root=writable_root,
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
                require_precreated_writable=True,
            )

            lease = manager.prepare(
                sandbox_id="sandbox-1",
                sandbox_generation=10,
                image_ref="example/image:latest",
                config_template={"root": {}},
                imported_parked=True,
            )

            self.assertEqual((lease.upper / "payload").read_bytes(), b"migrated")
            self.assertTrue((generation / "checkpoint.img").is_file())
            self.assertTrue(lease.work.is_dir())

    def test_tar_validator_rejects_parent_traversal(self) -> None:
        with TemporaryDirectory() as raw:
            archive = Path(raw) / "escape.tar"
            with tarfile.open(archive, "w") as target:
                member = tarfile.TarInfo("../escape")
                member.size = 1
                target.addfile(member, io.BytesIO(b"x"))
            with self.assertRaisesRegex(DirectWardenError, "unsafe path"):
                GnuTarRootfsExtractor._validate_archive(archive)


if __name__ == "__main__":
    unittest.main()
