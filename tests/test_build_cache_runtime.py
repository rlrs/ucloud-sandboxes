from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec
from ucloud_sandboxes.sandbox import CommandResult


class BuildCacheRuntimeTests(unittest.TestCase):
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
