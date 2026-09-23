import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_images import _uploaded_context
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, allowlisted_build_view
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec, ImageManager, ImageStore
from ucloud_sandboxes.sandbox import CommandResult


class EnvironmentBuilderTests(unittest.TestCase):
    def test_failed_publication_releases_temporary_builder_mount_after_reader_lease(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            calls = []
            class Store(DockerOverlay2RootfsStore):
                def __init__(self):
                    pass
                @contextmanager
                def operation_lease(self, _):
                    calls.append("leased")
                    try:
                        yield SimpleNamespace(image_id="sha256:" + "1" * 64, rootfs=root)
                    finally:
                        calls.append("released")
                def collect_image(self, image_id, *, is_referenced):
                    self.assertion = is_referenced(image_id)
                    calls.append("collected")
            store = Store()
            builder = FreshEnvironmentBuilder(store, None, None, root / "scratch")
            with patch("ucloud_sandboxes.environment_builder.allowlisted_build_view"), \
                 patch("ucloud_sandboxes.environment_builder.subprocess.run", side_effect=OSError("mkfs failed")), \
                 self.assertRaisesRegex(OSError, "mkfs failed"):
                builder.build("image", allowlist=("bin",), tag="test")
            self.assertEqual(calls, ["leased", "released", "collected"])
            self.assertFalse(store.assertion)

    def test_fresh_view_preserves_links_and_literal_whiteout_names(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            (source / "etc").mkdir(parents=True)
            (source / "etc/file").write_text("keep")
            os.link(source / "etc/file", source / "etc/hardlink")
            (source / "etc/link").symlink_to("file")
            (source / "etc/.wh.literal").write_text("ordinary mounted image data")
            (source / "outside").symlink_to(root)
            target = root / "view"
            allowlisted_build_view(source, target, ["etc"])
            self.assertEqual((target / "etc/file").stat().st_ino, (target / "etc/hardlink").stat().st_ino)
            self.assertEqual((target / "etc/link").read_text(), "keep")
            self.assertEqual((target / "etc/.wh.literal").read_text(), "ordinary mounted image data")
            self.assertFalse((target / "outside").exists())
            for value in ("../outside", "/etc", 12, "outside/escape"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    allowlisted_build_view(source, root / "invalid", [value])
                if (root / "invalid").exists():
                    (root / "invalid").rmdir()

    def test_existing_builder_records_annotated_manifest_only_after_publish(self):
        with TemporaryDirectory() as temporary:
            calls = []
            class Executor:
                def run(self, argv):
                    calls.append(argv)
                    return CommandResult(argv=argv, exit_code=0)
            def publish(spec):
                self.assertEqual(spec.id, "fixture")
                self.assertTrue(any("push" in command for command in calls))
                return "sha256:" + "a" * 64
            manager = ImageManager(ImageStore(Path(temporary) / "images.sqlite"),
                DockerImageRuntime(executor=Executor()), environment_publisher=publish)
            identity, materialize = _uploaded_context(("Dockerfile", b"FROM scratch\n"))
            record, _ = manager.start_build(ImageBuildSpec(id="fixture", tag="registry.example/fixture:latest", context_path="."),
                push=True, context_identity=identity, materialize_context=materialize)
            done = manager.wait_for_build(record.build_id, timeout_seconds=5)
            self.assertEqual(done.status, "succeeded", done.error)
            self.assertEqual(manager.get_image("fixture").manifest_digest, "sha256:" + "a" * 64)
            self.assertIn("immutable_environment_ms", done.timings["phases"])


if __name__ == "__main__":
    unittest.main()
