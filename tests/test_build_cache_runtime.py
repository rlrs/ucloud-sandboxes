from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from tests.test_images import _uploaded_context
from ucloud_sandboxes.images import (
    BUILD_LOG_TAIL_CHARS, DockerImageRuntime, ImageBuildSpec, ImageManager, ImageStore,
)
from ucloud_sandboxes.sandbox import CommandResult


class BuildCacheRuntimeTests(unittest.TestCase):
    def test_concurrent_builds_retain_cache_timings_after_log_truncation_and_failure(self):
        with TemporaryDirectory() as raw:
            Path(raw, "Dockerfile").write_text("FROM scratch\n")
            barrier = Barrier(2)

            class Runtime(DockerImageRuntime):
                def _run(self, argv, *, on_output=None):
                    barrier.wait(timeout=5)
                    on_output("stdout", "x" * (BUILD_LOG_TAIL_CHARS + 100))
                    if "registry/ucloud-managed/fail:latest" in argv:
                        raise RuntimeError("deliberate build failure")
                    return CommandResult(argv=argv, exit_code=0)

            runtime = Runtime(buildx_direct_push=True)
            runtime.build_cache = Mock()
            runtime.build_cache.prepare.return_value = SimpleNamespace(
                imports=(), export_ref="registry/ucloud-build-cache:unique",
                matching_ref="registry/ucloud-build-cache:one")
            runtime.build_cache.pre_mount.return_value = {"mounted": 1}
            manager = ImageManager(ImageStore(Path(raw) / "images.sqlite"), runtime,
                                   max_active_builds=2)
            identity, materialize = _uploaded_context(("Dockerfile", b"FROM scratch\n"))
            records = [manager.start_build(ImageBuildSpec(id=name,
                tag=f"registry/ucloud-managed/{name}:latest", context_path="."), push=True,
                context_identity=identity, materialize_context=materialize)[0]
                for name in ("ok", "fail")]
            completed = [manager.wait_for_build(record.build_id, timeout_seconds=10) for record in records]
            self.assertEqual([record.status for record in completed], ["succeeded", "failed"])
            for record in completed:
                self.assertNotIn("Shared build cache mounts:", record.log_tail)
                phases = record.timings["phases"]
                for key in ("cache_prepare_ms", "cache_mount_ms"):
                    self.assertIsInstance(phases[key], int)
                    self.assertGreaterEqual(phases[key], 0)
                    self.assertLessEqual(phases[key], phases["docker_build_and_push_ms"])
            # Terminal records are read back through the persisted build store.
            self.assertEqual(manager.build_store.get(records[1].build_id).timings,
                             completed[1].timings)

    def test_replacement_builder_imports_shared_cache_and_exports_own_ref(self):
        with TemporaryDirectory() as raw:
            Path(raw, "Dockerfile").write_text("FROM scratch\n")
            executor = Mock()
            executor.run.side_effect = lambda argv: CommandResult(argv=argv, exit_code=0)
            runtime = DockerImageRuntime(
                executor=executor, buildx_direct_push=True,
                buildx_builder="ucloud-shared-cache",
            )
            runtime.build_cache = Mock()
            runtime.build_cache.prepare.return_value = SimpleNamespace(
                imports=("registry/ucloud-build-cache:one", "registry/ucloud-build-cache:two"),
                export_ref="registry/ucloud-build-cache:unique",
                matching_ref="",
            )
            result = runtime.build(ImageBuildSpec(id="one", tag="registry/image:one", context_path=raw), push=True)
            args = result.argv
            self.assertEqual(args[args.index("--builder") + 1], "ucloud-shared-cache")
            self.assertIn("--provenance=false", args)
            self.assertEqual(args.count("--cache-from"), 2)
            self.assertEqual(args[args.index("--cache-to") + 1],
                "type=registry,ref=registry/ucloud-build-cache:unique,mode=min,oci-mediatypes=true,image-manifest=true,ignore-error=true")
            self.assertNotIn("mode=max", " ".join(args))
            self.assertEqual(runtime.build_cache.prepare.call_count, 1)

    def test_cache_failure_does_not_fail_build_or_enable_unscoped_export(self):
        with TemporaryDirectory() as raw:
            Path(raw, "Dockerfile").write_text("FROM scratch\n")
            executor = Mock()
            executor.run.side_effect = lambda argv: CommandResult(argv=argv, exit_code=0)
            runtime = DockerImageRuntime(executor=executor, buildx_direct_push=True)
            runtime.build_cache = Mock()
            runtime.build_cache.prepare.side_effect = OSError("cache unavailable")
            output = Mock()
            result = runtime.build(ImageBuildSpec(id="one", tag="registry/image:one", context_path=raw), push=True, on_output=output)
            self.assertEqual(result.exit_code, 0)
            self.assertNotIn("--cache-from", result.argv)
            self.assertNotIn("--cache-to", result.argv)
            self.assertTrue(any("building without it" in call.args[1] for call in output.call_args_list))

    def test_non_push_build_uses_existing_docker_path_without_cache_requests(self):
        runtime = DockerImageRuntime(dry_run=True, buildx_direct_push=True, buildx_builder="ucloud-shared-cache")
        runtime.build_cache = Mock()
        result = runtime.build(ImageBuildSpec(id="one", tag="local:one", context_path="."))
        self.assertEqual(result.argv[:2], ("docker", "build"))
        self.assertNotIn("--builder", result.argv)
        runtime.build_cache.prepare.assert_not_called()

    def test_optional_mount_failure_preserves_imports_export_and_original_build(self):
        with TemporaryDirectory() as raw:
            Path(raw, "Dockerfile").write_text("FROM scratch\n")
            executor = Mock()
            runtime = DockerImageRuntime(executor=executor, buildx_direct_push=True)
            runtime.build_cache = Mock()
            runtime.build_cache.prepare.return_value = SimpleNamespace(
                imports=("registry/ucloud-build-cache:one",),
                export_ref="registry/ucloud-build-cache:unique",
                matching_ref="registry/ucloud-build-cache:one",
            )
            runtime.build_cache.pre_mount.return_value = {"attempted": 1, "mounted": 0, "error": "TimeoutError"}
            def run(argv):
                runtime.build_cache.pre_mount.assert_called_once_with(
                    "registry/ucloud-managed/image:latest", "registry/ucloud-build-cache:one")
                return CommandResult(argv=argv, exit_code=0)
            executor.run.side_effect = run
            output = Mock()
            result = runtime.build(ImageBuildSpec(id="one", tag="registry/ucloud-managed/image:latest", context_path=raw), push=True, on_output=output)
            self.assertEqual(result.exit_code, 0)
            self.assertIn("type=registry,ref=registry/ucloud-build-cache:one", result.argv)
            self.assertIn("--cache-to", result.argv)
            self.assertTrue(any('"mounted": 0' in call.args[1] for call in output.call_args_list))

    def test_dry_run_push_never_prepares_or_mounts_cache(self):
        runtime = DockerImageRuntime(dry_run=True, buildx_direct_push=True)
        runtime.build_cache = Mock()
        runtime.build(ImageBuildSpec(id="one", tag="registry/ucloud-managed/image:latest", context_path="."), push=True)
        runtime.build_cache.prepare.assert_not_called()
        runtime.build_cache.pre_mount.assert_not_called()


if __name__ == "__main__":
    unittest.main()
