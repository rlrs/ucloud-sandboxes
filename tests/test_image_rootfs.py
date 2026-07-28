import io
import json
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.direct_warden import CommandResult, DirectWardenError
from ucloud_sandboxes.image_rootfs import (
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


class ImageRootfsTests(unittest.TestCase):
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
