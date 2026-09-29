from dataclasses import replace
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, Event
import unittest
from unittest.mock import Mock, patch

from tests.test_images import _uploaded_context
from ucloud_sandboxes import images
from ucloud_sandboxes.build_cache import BuildCachePlan
from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec, ImageManager, ImageStore, build_cache_affinity
from ucloud_sandboxes.sandbox import CommandResult


class BuildCacheAffinityRuntimeTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.identity, self.materialize = _uploaded_context(("Dockerfile", b"FROM scratch\n"),
                                                           ("payload", b"verified payload"))

    def spec(self, name, **kwargs):
        return ImageBuildSpec(id=name, tag=f"registry/ucloud-managed/{name}:latest", context_path=".", **kwargs)

    def runtime(self, run):
        executor = Mock()
        executor.run.side_effect = run
        runtime = DockerImageRuntime(executor=executor, buildx_direct_push=True)
        runtime.build_cache = Mock()
        runtime.build_cache.prepare.return_value = BuildCachePlan((), "registry/ucloud-build-cache:writer")
        return runtime

    def test_canonical_affinity_ignores_destinations_labels_and_temporary_paths(self):
        original = self.spec("one", dockerfile="./Dockerfile", build_args={"B": "2", "A": "1"}, labels={"a": "one"})
        other = replace(original, id="two", tag="other/image:tag", context_path="/different/temporary/context",
                        dockerfile="Dockerfile", build_args={"A": "1", "B": "2"}, labels={"b": "two"})
        self.assertEqual(build_cache_affinity(original, self.identity), build_cache_affinity(other, self.identity))
        self.assertRegex(build_cache_affinity(original, self.identity), r"^[0-9a-f]{64}$")

    def test_context_build_argument_and_dockerfile_changes_have_distinct_affinities(self):
        spec = self.spec("one", build_args={"VALUE": "old"})
        changed_context, _ = _uploaded_context(("Dockerfile", b"FROM scratch\n"), ("payload", b"changed"))
        keys = {build_cache_affinity(spec, self.identity), build_cache_affinity(spec, changed_context),
                build_cache_affinity(replace(spec, build_args={"VALUE": "new"}), self.identity),
                build_cache_affinity(replace(spec, dockerfile="Otherfile"), self.identity)}
        self.assertEqual(len(keys), 4)
        for identity in ("", "local-path:/private", "archive:sha256:bad"):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                build_cache_affinity(spec, identity)

    def test_concurrent_workers_pass_their_verified_hint_and_reset_after_failure(self):
        barrier = Barrier(2)
        cleanups = []
        def run(argv):
            if "registry/ucloud-managed/fail:latest" in argv:
                raise RuntimeError("deliberate build failure")
            return CommandResult(argv=argv, exit_code=0)
        runtime = self.runtime(run)
        def prepare(recipe, *, affinity_key):
            barrier.wait(timeout=5)
            return BuildCachePlan((), "registry/ucloud-build-cache:writer", affinity_match=True)
        runtime.build_cache.prepare.side_effect = prepare
        manager = ImageManager(ImageStore(self.root / "images.sqlite"), runtime, max_active_builds=2)
        specs = [self.spec("ok", build_args={"VALUE": "first"}), self.spec("fail", build_args={"VALUE": "second"})]
        initial = [manager.start_build(spec, push=True, context_identity=self.identity,
                   materialize_context=self.materialize, cleanup=lambda: cleanups.append(images._BUILD_CACHE_AFFINITY.get()))[0]
                   for spec in specs]
        done = [manager.wait_for_build(record.build_id, timeout_seconds=10) for record in initial]
        self.assertEqual([record.status for record in done], ["succeeded", "failed"])
        expected = {build_cache_affinity(spec, self.identity) for spec in specs}
        self.assertEqual({call.kwargs["affinity_key"] for call in runtime.build_cache.prepare.call_args_list}, expected)
        self.assertEqual(cleanups, ["", ""])
        for record in done:
            line = next(line for line in record.log_tail.splitlines() if "Shared build cache selection:" in line)
            details = json.loads(line.split("Shared build cache selection: ", 1)[1])
            self.assertEqual(details, {"affinity_match": True, "imports": 0})
            self.assertFalse(any(key in line for key in expected))
        self.assertEqual(images._BUILD_CACHE_AFFINITY.get(), "")

    def test_queued_build_keeps_its_own_verified_hint_until_execution(self):
        entered, release = Event(), Event()
        def run(argv):
            if "registry/ucloud-managed/first:latest" in argv:
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("test did not release build")
            return CommandResult(argv=argv, exit_code=0)
        runtime = self.runtime(run)
        manager = ImageManager(ImageStore(self.root / "images.sqlite"), runtime,
                               max_active_builds=1, queue_builds=True)
        first_spec, second_spec = self.spec("first"), self.spec("second", build_args={"ARG": "different"})
        records = []
        try:
            records.append(manager.start_build(first_spec, push=True, context_identity=self.identity,
                           materialize_context=self.materialize)[0])
            self.assertTrue(entered.wait(2))
            records.append(manager.start_build(second_spec, push=True, context_identity=self.identity,
                           materialize_context=self.materialize)[0])
            self.assertEqual(runtime.build_cache.prepare.call_count, 1)
            self.assertEqual(manager.get_build(records[1].build_id).execution_started_at, "")
        finally:
            release.set()
            completed = [manager.wait_for_build(record.build_id, timeout_seconds=10) for record in records]
        self.assertTrue(all(record.status == "succeeded" for record in completed))
        self.assertEqual([call.kwargs["affinity_key"] for call in runtime.build_cache.prepare.call_args_list],
                         [build_cache_affinity(spec, self.identity) for spec in (first_spec, second_spec)])

    def test_affinity_preparation_failure_cleans_context_and_releases_reservation(self):
        runtime = self.runtime(lambda argv: CommandResult(argv=argv, exit_code=0))
        manager = ImageManager(ImageStore(self.root / "images.sqlite"), runtime)
        paths = []
        def materialize():
            result = self.materialize()
            paths.append(result.path)
            return result
        with patch.object(images, "build_cache_affinity", side_effect=ValueError("invalid affinity")), \
             self.assertRaisesRegex(ValueError, "invalid affinity"):
            manager.start_build(self.spec("failure"), push=True, context_identity=self.identity,
                                materialize_context=materialize)
        self.assertTrue(paths)
        self.assertTrue(all(not path.exists() for path in paths))
        self.assertEqual(manager.active_build_count(), 0)
        self.assertEqual(manager.list_builds()[0].status, "failed")
        runtime.build_cache.prepare.assert_not_called()

    def test_direct_runtime_caller_without_verified_context_retains_legacy_prepare_api(self):
        (self.root / "Dockerfile").write_bytes(b"FROM scratch\n")
        runtime = self.runtime(lambda argv: CommandResult(argv=argv, exit_code=0))
        result = runtime.build(replace(self.spec("direct"), context_path=str(self.root)), push=True)
        self.assertEqual(result.exit_code, 0)
        runtime.build_cache.prepare.assert_called_once_with(hashlib.sha256(b"FROM scratch\n").hexdigest())


if __name__ == "__main__":
    unittest.main()
