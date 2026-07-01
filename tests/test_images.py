from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageStore,
    image_id_from_tag,
)
from ucloud_sandboxes.sandbox import RecordingExecutor


class ImageTests(unittest.TestCase):
    def test_image_id_from_tag_is_store_safe(self) -> None:
        self.assertEqual(
            image_id_from_tag("registry.example.org/ucloud/python-base:latest"),
            "registry.example.org-ucloud-python-base-latest",
        )

    def test_build_command_includes_tag_args_and_labels(self) -> None:
        runtime = DockerImageRuntime(dry_run=True)
        spec = ImageBuildSpec(
            id="base",
            tag="local/base:latest",
            context_path="/tmp/context",
            dockerfile="Containerfile",
            build_args={"B": "2", "A": "1"},
            labels={"role": "base"},
        )

        argv = runtime.build_command(spec)

        self.assertEqual(
            argv[:6],
            ("docker", "build", "-f", "/tmp/context/Containerfile", "-t", "local/base:latest"),
        )
        self.assertIn("--build-arg", argv)
        self.assertIn("A=1", argv)
        self.assertIn("B=2", argv)
        self.assertIn("role=base", argv)
        self.assertEqual(argv[-1], "/tmp/context")

    def test_build_command_preserves_absolute_dockerfile_path(self) -> None:
        runtime = DockerImageRuntime(dry_run=True)
        spec = ImageBuildSpec(
            id="base",
            tag="local/base:latest",
            context_path="/tmp/context",
            dockerfile="/tmp/Dockerfile",
        )

        argv = runtime.build_command(spec)

        self.assertEqual(argv[:6], ("docker", "build", "-f", "/tmp/Dockerfile", "-t", "local/base:latest"))

    def test_image_manager_records_planned_build(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ImageStore(Path(raw_dir) / "images.json")
            executor = RecordingExecutor()
            runtime = DockerImageRuntime(executor=executor, dry_run=True)
            manager = ImageManager(store, runtime)

            record, result = manager.build(
                ImageBuildSpec(
                    id="base",
                    tag="local/base:latest",
                    context_path="/tmp/context",
                )
            )

            self.assertEqual(record.state, "planned")
            self.assertFalse(record.pushed)
            self.assertFalse(record.available_to_sandboxes)
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(executor.commands, [])
            self.assertEqual(len(manager.list()), 1)

    def test_image_manager_marks_pushed_images_available_to_sandboxes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ImageStore(Path(raw_dir) / "images.json")
            runtime = DockerImageRuntime(dry_run=True)
            manager = ImageManager(store, runtime)

            manager.build(
                ImageBuildSpec(
                    id="base",
                    tag="registry.example.org/base:latest",
                    context_path="/tmp/context",
                )
            )
            record = manager.mark_pushed("base")
            reloaded = manager.list()[0]

            self.assertTrue(record.pushed)
            self.assertTrue(record.available_to_sandboxes)
            self.assertTrue(reloaded.pushed)
            self.assertTrue(reloaded.available_to_sandboxes)


if __name__ == "__main__":
    unittest.main()
