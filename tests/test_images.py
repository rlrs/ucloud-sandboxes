from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from io import BytesIO
import json
import multiprocessing
import os
import sys
import tarfile
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.images import (
    COMMAND_OUTPUT_TAIL_CHARS,
    COMMAND_OUTPUT_TRUNCATION_MARKER,
    DockerImageRuntime,
    ImageBuildCapacityError,
    ImageBuildConflictError,
    ImageBuildRecord,
    ImageBuildSpec,
    ImageBuildStore,
    ImageRecord,
    ImageManager,
    ImageStore,
    _extract_safe_tar_gz_file,
    image_build_context_digest,
    image_build_fingerprint,
    image_id_from_tag,
    snapshot_local_build_context,
)
from ucloud_sandboxes.models import utc_now
from ucloud_sandboxes.sandbox import (
    CommandResult,
    RecordingExecutor,
)


class ImageTests(unittest.TestCase):
    def test_cold_pull_slots_bound_concurrency_without_losing_drain_fence(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(dry_run=True),
                max_concurrent_pulls=2,
            )
            release = Event()
            full = Event()
            state_lock = Lock()
            active = 0
            peak = 0
            failures: list[BaseException] = []

            def worker() -> None:
                nonlocal active, peak
                try:
                    with manager.image_operation():
                        with manager.pull_slot():
                            with state_lock:
                                active += 1
                                peak = max(peak, active)
                                if active == 2:
                                    full.set()
                            release.wait(2)
                            with state_lock:
                                active -= 1
                except BaseException as exc:  # pragma: no cover - thread handoff
                    failures.append(exc)

            threads = [Thread(target=worker) for _ in range(6)]
            for thread in threads:
                thread.start()
            self.assertTrue(full.wait(1))
            deadline = time.monotonic() + 1
            snapshot = manager.pull_operation_snapshot()
            while snapshot["waiting_operations"] < 4 and time.monotonic() < deadline:
                time.sleep(0.01)
                snapshot = manager.pull_operation_snapshot()

            self.assertEqual(snapshot["active_operations"], 2)
            self.assertEqual(snapshot["waiting_operations"], 4)
            self.assertEqual(snapshot["max_concurrent_operations"], 2)
            self.assertEqual(manager.active_build_count(), 6)
            release.set()
            for thread in threads:
                thread.join(2)

            self.assertFalse(failures)
            self.assertEqual(peak, 2)
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(
                manager.pull_operation_snapshot()["active_operations"],
                0,
            )

    def test_image_record_manifest_digest_is_optional_and_persisted(self) -> None:
        digest = "sha256:" + "b" * 64
        now = utc_now()
        unpinned = ImageRecord.from_dict(
            {
                "id": "unpinned",
                "tag": "registry.test/image:v1",
                "source": "registry",
                "state": "available",
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
        )
        pinned = ImageRecord(
            id="pinned",
            tag="registry.test/image:v2",
            source="registry",
            state="available",
            created_at=now,
            updated_at=now,
            manifest_digest=digest,
        )

        self.assertEqual(unpinned.manifest_digest, "")
        self.assertEqual(ImageRecord.from_dict(pinned.to_dict()), pinned)
        self.assertEqual(
            pinned.digest_ref,
            f"registry.test/image@{digest}",
        )

    def test_build_records_use_only_canonical_fields(self) -> None:
        common = {
            "build_id": "build-1",
            "image_id": "image-1",
            "tag": "local/image-1:latest",
            "status": "succeeded",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:01+00:00",
        }
        canonical = ImageBuildRecord.from_dict(
            {
                **common,
                "exit_code": 0,
                "push_exit_code": 0,
            }
        )
        noncanonical = ImageBuildRecord.from_dict(
            {
                **common,
                "exitCode": 0,
                "pushExitCode": 0,
            }
        )

        self.assertIsNotNone(canonical)
        self.assertIsNone(noncanonical)
        assert canonical is not None
        self.assertEqual((canonical.exit_code, canonical.push_exit_code), (0, 0))
        self.assertEqual(ImageBuildRecord.from_dict(canonical.to_dict()), canonical)

    def test_image_state_fails_closed_on_malformed_or_duplicate_records(self) -> None:
        with TemporaryDirectory() as raw_dir:
            image_path = Path(raw_dir) / "images.json"
            build_path = Path(raw_dir) / "builds.json"
            duplicate_image = ImageRecord(
                id="image-1",
                tag="registry.test/image:latest",
                source="registry",
                state="available",
                created_at=utc_now(),
                updated_at=utc_now(),
            ).to_dict()
            image_path.write_text(
                json.dumps({"images": [duplicate_image, duplicate_image]}),
                encoding="utf-8",
            )
            build_path.write_text(
                json.dumps(
                    {
                        "builds": [
                            {
                                "build_id": "build-1",
                                "image_id": "image-1",
                                "status": "running",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "duplicate image id"):
                ImageStore(image_path).load()
            with self.assertRaisesRegex(ValueError, "invalid record"):
                ImageBuildStore(build_path).load()

    def test_streaming_runtime_retains_only_bounded_output_tail(self) -> None:
        runtime = DockerImageRuntime()
        delivered: list[str] = []
        output_size = COMMAND_OUTPUT_TAIL_CHARS * 3

        result = runtime._run_streaming(  # noqa: SLF001 - focused runtime regression
            (
                sys.executable,
                "-c",
                f"import sys; sys.stdout.write('a' * {output_size})",
            ),
            on_output=lambda _stream, chunk: delivered.append(chunk),
        )

        self.assertEqual("".join(delivered), "a" * output_size)
        self.assertEqual(len(result.stdout), COMMAND_OUTPUT_TAIL_CHARS)
        self.assertTrue(result.stdout.startswith(COMMAND_OUTPUT_TRUNCATION_MARKER))
        self.assertTrue(result.stdout.endswith("a" * 1024))

    def test_build_store_bounds_only_terminal_history(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ImageBuildStore(
                Path(raw_dir) / "builds.json",
                max_terminal_builds=2,
            )
            for index in range(3):
                store.upsert(
                    _build_record(
                        f"active-{index}",
                        status="running",
                        timestamp=f"2026-01-01T00:00:0{index}+00:00",
                    )
                )
            for index in range(6):
                store.upsert(
                    _build_record(
                        f"done-{index}",
                        status="succeeded",
                        timestamp=f"2026-01-02T00:00:0{index}+00:00",
                    )
                )

            records = store.load()

            self.assertEqual(
                {record.build_id for record in records.values() if not record.terminal},
                {"active-0", "active-1", "active-2"},
            )
            self.assertEqual(
                {record.build_id for record in records.values() if record.terminal},
                {"done-4", "done-5"},
            )

    def test_build_logs_are_batched_and_condition_history_is_released(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            build_store = CountingBuildStore(Path(raw_dir) / "builds.json")
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                ChattyBuildRuntime(),
                build_store=build_store,
            )

            build, started = manager.start_build(
                ImageBuildSpec(
                    id="chatty",
                    tag="local/chatty:latest",
                    context_path=str(context_path),
                )
            )
            self.assertTrue(started)
            result = manager.wait_for_build(build.build_id, timeout_seconds=2)
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.log_tail, "x" * 4_000)
            self.assertLess(build_store.upsert_calls, 20)

            deadline = time.monotonic() + 1
            while build.build_id in manager._build_conditions:  # noqa: SLF001
                if time.monotonic() >= deadline:
                    self.fail("completed build condition was not released")
                time.sleep(0.01)

    def test_multiprocess_image_and_build_writers_do_not_lose_updates(self) -> None:
        with TemporaryDirectory() as raw_dir:
            image_path = Path(raw_dir) / "images.json"
            build_path = Path(raw_dir) / "builds.json"
            context = multiprocessing.get_context("spawn")
            processes = []
            for worker in range(3):
                processes.append(
                    context.Process(
                        target=_multiprocess_image_writer,
                        args=(str(image_path), worker, 5),
                    )
                )
                processes.append(
                    context.Process(
                        target=_multiprocess_build_writer,
                        args=(str(build_path), worker, 5),
                    )
                )
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=15)

            self.assertEqual([process.exitcode for process in processes], [0] * 6)
            self.assertEqual(len(ImageStore(image_path).load()), 15)
            self.assertEqual(len(ImageBuildStore(build_path).load()), 15)
            self.assertEqual(
                list(image_path.parent.glob(f".{image_path.name}.*.tmp")),
                [],
            )
            self.assertEqual(
                list(build_path.parent.glob(f".{build_path.name}.*.tmp")),
                [],
            )

    def test_multiprocess_build_reservation_enforces_global_limit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            build_path = Path(raw_dir) / "builds.json"
            context = multiprocessing.get_context("spawn")
            start = context.Event()
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_build_reserver,
                    args=(str(build_path), worker, start, results),
                )
                for worker in range(4)
            ]
            for process in processes:
                process.start()
            start.set()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes.count("started"), 1)
            self.assertEqual(outcomes.count("capacity"), 3)
            self.assertEqual(len(ImageBuildStore(build_path).load()), 1)

    def test_manager_marks_interrupted_build_failed_on_startup(self) -> None:
        with TemporaryDirectory() as raw_dir:
            image_path = Path(raw_dir) / "images.json"
            build_store = ImageBuildStore(Path(raw_dir) / "builds.json")
            build_store.upsert(
                ImageBuildRecord(
                    build_id="build-1",
                    image_id="base",
                    tag="local/base:latest",
                    status="running",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                )
            )

            manager = ImageManager(
                ImageStore(image_path),
                DockerImageRuntime(dry_run=True),
                build_store=build_store,
            )

            build = manager.get_build("build-1")
            self.assertIsNotNone(build)
            assert build is not None
            self.assertEqual(build.status, "failed")
            self.assertIn("interrupted", build.error)
            self.assertTrue(build.finished_at)

    def test_second_manager_does_not_fail_build_owned_by_live_process(self) -> None:
        with TemporaryDirectory() as raw_dir:
            image_path = Path(raw_dir) / "images.json"
            build_store = ImageBuildStore(Path(raw_dir) / "builds.json")
            build_store.upsert(
                ImageBuildRecord(
                    build_id="build-live",
                    image_id="base",
                    tag="local/base:latest",
                    status="running",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    owner_pid=os.getpid(),
                )
            )

            manager = ImageManager(
                ImageStore(image_path),
                DockerImageRuntime(dry_run=True),
                build_store=build_store,
            )

            build = manager.get_build("build-live")
            assert build is not None
            self.assertEqual(build.status, "running")

    def test_manager_rejects_builds_above_concurrency_limit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            executor = BlockingBuildExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
                max_active_builds=1,
            )
            first, started = manager.start_build(
                ImageBuildSpec(
                    id="one",
                    tag="local/one:latest",
                    context_path=str(context_path),
                )
            )
            self.assertTrue(started)
            self.assertTrue(executor.started.wait(1))
            try:
                with self.assertRaisesRegex(ImageBuildCapacityError, "capacity"):
                    manager.start_build(
                        ImageBuildSpec(
                            id="two",
                            tag="local/two:latest",
                            context_path=str(context_path),
                        )
                    )
            finally:
                executor.release.set()
                manager.wait_for_build(first.build_id, timeout_seconds=2)

    def test_build_singleflight_requires_exact_immutable_fingerprint(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            dockerfile = context_path / "Dockerfile"
            dockerfile.write_text("FROM scratch\n", encoding="utf-8")
            executor = BlockingBuildExecutor()
            build_store = ImageBuildStore(Path(raw_dir) / "builds.json")
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
                build_store=build_store,
            )
            spec = ImageBuildSpec(
                id="singleflight",
                tag="local/singleflight:latest",
                context_path=str(context_path),
                build_args={"MODE": "one"},
            )

            first, started = manager.start_build(spec)
            self.assertTrue(started)
            self.assertTrue(executor.started.wait(1))
            snapshot_path = Path(first.context_path)
            self.assertNotEqual(snapshot_path, context_path)
            self.assertEqual(
                (snapshot_path / "Dockerfile").read_text(encoding="utf-8"),
                "FROM scratch\n",
            )
            duplicate, duplicate_started = manager.start_build(spec)
            self.assertFalse(duplicate_started)
            self.assertEqual(duplicate.build_id, first.build_id)
            self.assertEqual(
                build_store.load()[first.build_id].request_fingerprint,
                first.request_fingerprint,
            )

            cleanup_calls: list[str] = []
            differing = ImageBuildSpec(
                id=spec.id,
                tag=spec.tag,
                context_path=spec.context_path,
                build_args={"MODE": "two"},
            )
            with self.assertRaisesRegex(ImageBuildConflictError, "different"):
                manager.start_build(
                    differing,
                    cleanup=lambda: cleanup_calls.append("spec"),
                )
            old_fingerprint = image_build_fingerprint(spec)
            dockerfile.write_text("FROM scratch\n# changed\n", encoding="utf-8")
            self.assertEqual(
                (snapshot_path / "Dockerfile").read_text(encoding="utf-8"),
                "FROM scratch\n",
            )
            self.assertNotEqual(image_build_fingerprint(spec), old_fingerprint)
            with self.assertRaisesRegex(ImageBuildConflictError, "different"):
                manager.start_build(
                    spec,
                    cleanup=lambda: cleanup_calls.append("context"),
                )
            self.assertEqual(cleanup_calls, ["spec", "context"])

            executor.release.set()
            manager.wait_for_build(first.build_id, timeout_seconds=2)
            self.assertFalse(snapshot_path.exists())

    def test_active_build_output_key_conflicts_on_id_or_tag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            (context_path / "Dockerfile").write_text(
                "FROM scratch\n",
                encoding="utf-8",
            )
            executor = BlockingBuildExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
                max_active_builds=4,
            )
            first, started = manager.start_build(
                ImageBuildSpec(
                    id="shared-id",
                    tag="local/shared-tag:latest",
                    context_path=str(context_path),
                )
            )
            self.assertTrue(started)
            self.assertTrue(executor.started.wait(1))
            try:
                with self.assertRaisesRegex(ImageBuildConflictError, "id or tag"):
                    manager.start_build(
                        ImageBuildSpec(
                            id="shared-id",
                            tag="local/different-tag:latest",
                            context_path=str(context_path),
                        )
                    )
                with self.assertRaisesRegex(ImageBuildConflictError, "id or tag"):
                    manager.start_build(
                        ImageBuildSpec(
                            id="different-id",
                            tag="local/shared-tag:latest",
                            context_path=str(context_path),
                        )
                    )
            finally:
                executor.release.set()
                manager.wait_for_build(first.build_id, timeout_seconds=2)

    def test_reservation_precedes_context_materialization(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            (context_path / "Dockerfile").write_text(
                "FROM scratch\n",
                encoding="utf-8",
            )
            executor = BlockingBuildExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
            )
            identity = f"tree:{image_build_context_digest(context_path)}"
            materializations = 0

            def materialize():
                nonlocal materializations
                materializations += 1
                return snapshot_local_build_context(context_path)

            spec = ImageBuildSpec(
                id="lazy-context",
                tag="local/lazy-context:latest",
                context_path=str(context_path),
            )
            first, started = manager.start_build(
                spec,
                context_identity=identity,
                materialize_context=materialize,
            )
            self.assertTrue(started)
            self.assertTrue(executor.started.wait(1))
            duplicate, duplicate_started = manager.start_build(
                spec,
                context_identity=identity,
                materialize_context=materialize,
            )
            self.assertFalse(duplicate_started)
            self.assertEqual(duplicate.build_id, first.build_id)
            with self.assertRaises(ImageBuildConflictError):
                manager.start_build(
                    ImageBuildSpec(
                        id=spec.id,
                        tag=spec.tag,
                        context_path=spec.context_path,
                        build_args={"DIFFERENT": "1"},
                    ),
                    context_identity=identity,
                    materialize_context=materialize,
                )
            self.assertEqual(materializations, 1)
            executor.release.set()
            manager.wait_for_build(first.build_id, timeout_seconds=2)

    def test_local_context_preparation_has_a_hard_concurrency_cap(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            (context_path / "Dockerfile").write_text(
                "FROM scratch\n",
                encoding="utf-8",
            )
            executor = BlockingBuildExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
                max_active_builds=4,
                max_concurrent_context_preparations=1,
            )
            original_digest = image_build_context_digest
            first_digest_started = Event()
            release_digest = Event()
            digest_lock = Lock()
            digest_calls = 0
            active_digests = 0
            peak_digests = 0
            builds: list[ImageBuildRecord] = []
            failures: list[BaseException] = []

            def blocked_digest(path: Path) -> str:
                nonlocal digest_calls, active_digests, peak_digests
                with digest_lock:
                    digest_calls += 1
                    active_digests += 1
                    peak_digests = max(peak_digests, active_digests)
                    first_digest_started.set()
                try:
                    release_digest.wait(2)
                    return original_digest(path)
                finally:
                    with digest_lock:
                        active_digests -= 1

            def start(image_id: str) -> None:
                try:
                    record, _started = manager.start_build(
                        ImageBuildSpec(
                            id=image_id,
                            tag=f"local/{image_id}:latest",
                            context_path=str(context_path),
                        )
                    )
                    builds.append(record)
                except BaseException as exc:  # pragma: no cover - thread handoff
                    failures.append(exc)

            with patch(
                "ucloud_sandboxes.images.image_build_context_digest",
                side_effect=blocked_digest,
            ):
                first_thread = Thread(target=start, args=("prep-one",))
                second_thread = Thread(target=start, args=("prep-two",))
                first_thread.start()
                self.assertTrue(first_digest_started.wait(1))
                second_thread.start()
                deadline = time.monotonic() + 1
                snapshot = manager.context_preparation_snapshot()
                while (
                    snapshot["waiting_operations"] < 1
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                    snapshot = manager.context_preparation_snapshot()
                self.assertEqual(snapshot["active_operations"], 1)
                self.assertEqual(snapshot["waiting_operations"], 1)
                self.assertEqual(digest_calls, 1)
                release_digest.set()
                first_thread.join(2)
                second_thread.join(2)

            self.assertFalse(failures)
            self.assertEqual(len(builds), 2)
            self.assertEqual(peak_digests, 1)
            self.assertEqual(
                manager.context_preparation_snapshot()["active_operations"],
                0,
            )
            executor.release.set()
            for build in builds:
                manager.wait_for_build(build.build_id, timeout_seconds=2)

    def test_build_worker_thread_start_failure_is_terminal_and_cleans_snapshot(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            (context_path / "Dockerfile").write_text(
                "FROM scratch\n",
                encoding="utf-8",
            )
            build_store = ImageBuildStore(Path(raw_dir) / "builds.json")
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(dry_run=True),
                build_store=build_store,
            )
            cleanup_calls: list[str] = []

            with patch(
                "ucloud_sandboxes.images.Thread.start",
                side_effect=RuntimeError("cannot start build worker"),
            ), self.assertRaisesRegex(RuntimeError, "cannot start"):
                manager.start_build(
                    ImageBuildSpec(
                        id="thread-failure",
                        tag="local/thread-failure:latest",
                        context_path=str(context_path),
                    ),
                    cleanup=lambda: cleanup_calls.append("caller"),
                )

            records = list(build_store.load().values())
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].status, "failed")
            self.assertIn("cannot start build worker", records[0].error)
            self.assertFalse(Path(records[0].context_path).exists())
            self.assertEqual(cleanup_calls, ["caller"])
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(manager._active_threads, {})  # noqa: SLF001
            self.assertEqual(manager._build_conditions, {})  # noqa: SLF001

    def test_context_materialization_failure_marks_reserved_build_terminal(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            build_store = ImageBuildStore(Path(raw_dir) / "builds.json")
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(dry_run=True),
                build_store=build_store,
            )

            def fail_materialization():
                raise ValueError("cannot extract context")

            with self.assertRaisesRegex(ValueError, "cannot extract"):
                manager.start_build(
                    ImageBuildSpec(
                        id="materialization-failure",
                        tag="local/materialization-failure:latest",
                        context_path=".",
                    ),
                    context_identity="archive:sha256:deadbeef",
                    materialize_context=fail_materialization,
                )

            records = list(build_store.load().values())
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].status, "failed")
            self.assertIn("cannot extract context", records[0].error)
            self.assertEqual(manager._build_conditions, {})  # noqa: SLF001

    def test_context_archive_enforces_stream_and_member_limits(self) -> None:
        cases = (
            (
                "member",
                _tar_gz(("large", b"12345")),
                {"max_member_bytes": 4},
                "member exceeds",
            ),
            (
                "total",
                _tar_gz(("one", b"123"), ("two", b"456")),
                {"max_total_bytes": 5},
                "extracted-byte limit",
            ),
            (
                "count",
                _tar_gz(("one", b""), ("two", b"")),
                {"max_members": 1},
                "member limit",
            ),
            (
                "decompressed-pax",
                _tar_gz(("empty", b""), pax_value="x" * 4096),
                {"max_archive_bytes": 1024},
                "decompressed-byte limit",
            ),
        )
        for name, archive, limits, message in cases:
            with self.subTest(name=name), TemporaryDirectory() as raw_dir:
                with self.assertRaisesRegex(ValueError, message):
                    _extract_safe_tar_gz_file(
                        BytesIO(archive),
                        Path(raw_dir),
                        max_total_bytes=limits.get("max_total_bytes", 1024 * 1024),
                        max_member_bytes=limits.get(
                            "max_member_bytes", 1024 * 1024
                        ),
                        max_members=limits.get("max_members", 100),
                        max_archive_bytes=limits.get(
                            "max_archive_bytes", 1024 * 1024
                        ),
                    )

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
            (
                "docker",
                "build",
                "-f",
                "/tmp/context/Containerfile",
                "-t",
                "local/base:latest",
            ),
        )
        self.assertIn("--build-arg", argv)
        self.assertIn("A=1", argv)
        self.assertIn("B=2", argv)
        self.assertIn("role=base", argv)
        self.assertEqual(argv[-1], "/tmp/context")

    def test_dockerfile_must_be_normalized_context_relative_path(self) -> None:
        runtime = DockerImageRuntime(dry_run=True)
        for dockerfile in (
            "/tmp/Dockerfile",
            "../Dockerfile",
            "nested/../../Dockerfile",
            ".",
            "nested\\Dockerfile",
        ):
            with self.subTest(dockerfile=dockerfile), self.assertRaisesRegex(
                ValueError,
                "dockerfile",
            ):
                runtime.build_command(
                    ImageBuildSpec(
                        id="base",
                        tag="local/base:latest",
                        context_path="/tmp/context",
                        dockerfile=dockerfile,
                    )
                )

        normalized = ImageBuildSpec.from_dict(
            {
                "id": "base",
                "tag": "local/base:latest",
                "context_path": "/tmp/context",
                "dockerfile": "nested/./Containerfile",
            }
        )
        self.assertEqual(normalized.dockerfile, "nested/Containerfile")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            context = root / "context"
            context.mkdir()
            outside = root / "outside.Dockerfile"
            outside.write_text("FROM scratch\n", encoding="utf-8")
            (context / "Dockerfile").symlink_to(outside)
            manager = ImageManager(
                ImageStore(root / "images.json"),
                DockerImageRuntime(dry_run=True),
            )
            with self.assertRaisesRegex(ValueError, "immutable build context"):
                manager.start_build(
                    ImageBuildSpec(
                        id="escaped-dockerfile",
                        tag="local/escaped-dockerfile:latest",
                        context_path=str(context),
                    )
                )

    def test_buildx_direct_push_command_uses_registry_cache(self) -> None:
        runtime = DockerImageRuntime(
            dry_run=True,
            buildx_direct_push=True,
            buildx_cache_ref="registry.example.org/cache/base:buildcache",
        )
        spec = ImageBuildSpec(
            id="base",
            tag="registry.example.org/images/base:latest",
            context_path="/tmp/context",
        )

        argv = runtime.build_command(spec, push=True)

        self.assertEqual(argv[:3], ("docker", "buildx", "build"))
        self.assertIn("--push", argv)
        self.assertIn("--cache-from", argv)
        self.assertIn(
            "type=registry,ref=registry.example.org/cache/base:buildcache",
            argv,
        )
        self.assertIn("--cache-to", argv)
        self.assertIn(
            "type=registry,ref=registry.example.org/cache/base:buildcache,mode=max",
            argv,
        )

    def test_buildx_mode_is_opt_in_and_cache_requires_it(self) -> None:
        spec = ImageBuildSpec(
            id="base",
            tag="registry.example.org/images/base:latest",
            context_path="/tmp/context",
        )

        argv = DockerImageRuntime(dry_run=True).build_command(spec, push=True)

        self.assertEqual(argv[:2], ("docker", "build"))
        self.assertNotIn("--push", argv)
        with self.assertRaisesRegex(ValueError, "requires buildx_direct_push"):
            DockerImageRuntime(
                dry_run=True,
                buildx_cache_ref="registry.example.org/cache/base:buildcache",
            )

    def test_manager_records_integrated_buildx_push_as_one_command(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            executor = RecordingExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(
                    executor=executor,
                    buildx_direct_push=True,
                    buildx_cache_ref="registry.example.org/cache/base:buildcache",
                ),
            )
            spec = ImageBuildSpec(
                id="base",
                tag="registry.example.org/images/base:latest",
                context_path=str(context_path),
            )

            started_record, started = manager.start_build(spec, push=True)
            finished = manager.wait_for_build(
                started_record.build_id,
                timeout_seconds=2,
            )

            self.assertTrue(started)
            self.assertIsNotNone(finished)
            assert finished is not None
            self.assertEqual(finished.status, "succeeded")
            self.assertEqual(finished.command[:3], ("docker", "buildx", "build"))
            self.assertEqual(finished.push_command, ())
            self.assertIsNone(finished.push_exit_code)
            self.assertEqual(executor.commands, [finished.command])
            self.assertIn(
                "docker_build_and_push_ms",
                finished.timings["phases"],
            )
            self.assertNotIn("docker_push_ms", finished.timings["phases"])
            self.assertTrue(finished.image["pushed"])

    def test_manager_preserves_separate_push_by_default(self) -> None:
        with TemporaryDirectory() as raw_dir:
            context_path = Path(raw_dir) / "context"
            context_path.mkdir()
            executor = RecordingExecutor()
            manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
            )
            spec = ImageBuildSpec(
                id="base",
                tag="registry.example.org/images/base:latest",
                context_path=str(context_path),
            )

            started_record, _started = manager.start_build(spec, push=True)
            finished = manager.wait_for_build(
                started_record.build_id,
                timeout_seconds=2,
            )

            assert finished is not None
            self.assertEqual(finished.command[:2], ("docker", "build"))
            self.assertEqual(
                finished.push_command,
                ("docker", "push", "registry.example.org/images/base:latest"),
            )
            self.assertEqual(
                executor.commands,
                [finished.command, finished.push_command],
            )
            self.assertIn("docker_build_ms", finished.timings["phases"])
            self.assertIn("docker_push_ms", finished.timings["phases"])

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
            digest = "sha256:" + "f" * 64
            record = manager.mark_pushed("base", manifest_digest=digest)
            reloaded = manager.list()[0]

            self.assertTrue(record.pushed)
            self.assertTrue(record.available_to_sandboxes)
            self.assertTrue(reloaded.pushed)
            self.assertTrue(reloaded.available_to_sandboxes)
            self.assertEqual(reloaded.manifest_digest, digest)

    def test_image_store_deletes_records_by_tag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ImageStore(Path(raw_dir) / "images.json")
            runtime = DockerImageRuntime(dry_run=True)
            manager = ImageManager(store, runtime)
            manager.build(
                ImageBuildSpec(
                    id="keep",
                    tag="registry.example.org/keep:latest",
                    context_path="/tmp/context",
                )
            )
            manager.build(
                ImageBuildSpec(
                    id="delete",
                    tag="registry.example.org/delete:latest",
                    context_path="/tmp/context",
                )
            )

            removed = store.delete_by_tags(["registry.example.org/delete:latest"])

            self.assertEqual([record.id for record in removed], ["delete"])
            self.assertEqual(
                [(record.id, record.tag) for record in manager.list()],
                [("keep", "registry.example.org/keep:latest")],
            )


class BlockingBuildExecutor:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult:
        del input
        self.started.set()
        self.release.wait(2)
        return CommandResult(argv=argv, exit_code=0)


def _tar_gz(*entries: tuple[str, bytes], pax_value: str = "") -> bytes:
    payload = BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for name, content in entries:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            if pax_value:
                member.pax_headers = {"comment": pax_value}
            archive.addfile(member, BytesIO(content))
    return payload.getvalue()


class ChattyBuildRuntime(DockerImageRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    def build(self, spec, *, push=False, on_output=None):  # type: ignore[no-untyped-def]
        del push
        if on_output is not None:
            for _index in range(4_000):
                on_output("combined", "x")
        return CommandResult(argv=self.build_command(spec), exit_code=0)


class CountingBuildStore(ImageBuildStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.upsert_calls = 0

    def upsert(self, record: ImageBuildRecord) -> dict[str, ImageBuildRecord]:
        self.upsert_calls += 1
        return super().upsert(record)


def _build_record(
    build_id: str,
    *,
    status: str,
    timestamp: str,
) -> ImageBuildRecord:
    return ImageBuildRecord(
        build_id=build_id,
        image_id=f"image-{build_id}",
        tag=f"local/{build_id}:latest",
        status=status,
        created_at=timestamp,
        updated_at=timestamp,
        finished_at=timestamp if status in {"succeeded", "failed"} else "",
    )


def _multiprocess_image_writer(path: str, worker: int, count: int) -> None:
    store = ImageStore(Path(path))
    for index in range(count):
        now = utc_now()
        store.upsert(
            ImageRecord(
                id=f"image-{worker}-{index}",
                tag=f"local/image-{worker}-{index}:latest",
                source="test",
                state="available",
                created_at=now,
                updated_at=now,
            )
        )


def _multiprocess_build_writer(path: str, worker: int, count: int) -> None:
    store = ImageBuildStore(Path(path))
    for index in range(count):
        now = utc_now().isoformat()
        store.upsert(
            ImageBuildRecord(
                build_id=f"build-{worker}-{index}",
                image_id=f"image-{worker}-{index}",
                tag=f"local/image-{worker}-{index}:latest",
                status="succeeded",
                created_at=now,
                updated_at=now,
                finished_at=now,
            )
        )


def _multiprocess_build_reserver(path: str, worker: int, start, results) -> None:
    store = ImageBuildStore(Path(path))
    now = utc_now().isoformat()
    record = ImageBuildRecord(
        build_id=f"build-{worker}",
        image_id=f"image-{worker}",
        tag=f"local/image-{worker}:latest",
        status="running",
        created_at=now,
        updated_at=now,
        started_at=now,
    )
    start.wait(10)
    try:
        _record, started = store.reserve_build(record, max_active_builds=1)
    except ImageBuildCapacityError:
        results.put("capacity")
    else:
        results.put("started" if started else "duplicate")


if __name__ == "__main__":
    unittest.main()
