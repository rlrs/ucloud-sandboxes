import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests.test_images import _uploaded_context
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, allowlisted_build_view, publication_metrics
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec, ImageManager, ImageStore
from ucloud_sandboxes.sandbox import CommandResult

TEST_TIER = "contract"


class EnvironmentBuilderTests(unittest.TestCase):
    def test_publication_releases_only_its_local_tag_after_success_or_failure(self):
        for failure in (False, True):
            with self.subTest(failure=failure):
                store = Mock(docker_binary="docker")
                builder = FreshEnvironmentBuilder(store, None, None, Path("/unused"),
                                                  release_published_tag=True)
                events = []
                def publish(*args, **kwargs):
                    events.append("publication_finished")
                    if failure:
                        raise ValueError("publication failed")
                    return "digest"
                store._checked.side_effect = lambda *args, **kwargs: events.append("removed")
                with patch.object(builder, "_publish_image", side_effect=publish):
                    if failure:
                        with self.assertRaisesRegex(ValueError, "publication failed"):
                            builder.publish_image("registry/owned:latest", allowlist=("*",))
                    else:
                        self.assertEqual(builder.publish_image("registry/owned:latest", allowlist=("*",)), "digest")
                self.assertEqual(events, ["publication_finished", "removed"])
                store._checked.assert_called_once_with("docker", "image", "rm", "registry/owned:latest", timeout=30)

    def test_cleanup_failure_does_not_lose_published_artifact(self):
        store = Mock(docker_binary="docker")
        store._checked.side_effect = OSError("busy")
        builder = FreshEnvironmentBuilder(store, None, None, Path("/unused"), release_published_tag=True)
        with patch.object(builder, "_publish_image", return_value="digest"):
            self.assertEqual(builder.publish_image("registry/owned:latest", allowlist=("*",)), "digest")

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

    def test_whole_image_publication_reads_the_overlay_directly(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "rootfs").mkdir()
            class Store(DockerOverlay2RootfsStore):
                def __init__(self):
                    pass
                @contextmanager
                def operation_lease(self, _):
                    yield SimpleNamespace(image_id="sha256:" + "1" * 64, rootfs=root / "rootfs")
                def collect_image(self, image_id, *, is_referenced):
                    pass
            builder = FreshEnvironmentBuilder(Store(), None, None, root / "scratch")
            for allowlist, direct in ((("*",), True), (("bin",), False)):
                with self.subTest(allowlist=allowlist), \
                     patch("ucloud_sandboxes.environment_builder.allowlisted_build_view") as view, \
                     patch("ucloud_sandboxes.environment_builder.subprocess.run",
                           side_effect=OSError("stop after mkfs")) as run, \
                     self.assertRaises(OSError):
                    builder.build("image", allowlist=allowlist, tag="test")
                command = run.call_args.args[0]
                self.assertIn("-zlz4", command)
                self.assertEqual(view.called, not direct)
                if direct:
                    self.assertEqual(command[-1], str(root / "rootfs"))
                    self.assertIn("--exclude-regex=^(dev|proc|run|sys)$", command)
                else:
                    self.assertTrue(command[-1].endswith("/view"))
                    self.assertFalse(any(part.startswith("--exclude") for part in command))

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

    def test_whole_image_entry_copies_everything_but_runtime_mounts(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            for name in ("etc", "testbed", "opt", "dev", "proc", "sys", "run"):
                (source / name).mkdir(parents=True)
            (source / "testbed/repo.py").write_text("task")
            (source / "bin").symlink_to("usr/bin")
            target = root / "view"
            allowlisted_build_view(source, target, ["*"])
            self.assertEqual(sorted(p.name for p in target.iterdir()), ["bin", "etc", "opt", "testbed"])
            self.assertEqual((target / "testbed/repo.py").read_text(), "task")
            self.assertTrue((target / "bin").is_symlink())

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
                with publication_metrics() as metrics:
                    metrics.update(docker_pull_skipped=1, groups_reused=3)
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
            self.assertEqual(done.timings["environment"], {"docker_pull_skipped": 1, "groups_reused": 3})


if __name__ == "__main__":
    unittest.main()
