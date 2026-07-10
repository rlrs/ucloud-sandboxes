from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import json
import multiprocessing
import os
import sys
import time
import unittest

from ucloud_sandboxes.images import (
    COMMAND_OUTPUT_TAIL_CHARS,
    COMMAND_OUTPUT_TRUNCATION_MARKER,
    DockerImageRuntime,
    ImageBuildCapacityError,
    ImageBuildRecord,
    ImageBuildSpec,
    ImageBuildStore,
    ImageRecord,
    ImageManager,
    ImageStore,
    image_id_from_tag,
)
from ucloud_sandboxes.models import utc_now
from ucloud_sandboxes.sandbox import (
    CommandResult,
    DockerGvisorRuntime,
    RecordingExecutor,
    SandboxAdmissionClosedError,
    SandboxManager,
    SandboxStore,
)


class ImageTests(unittest.TestCase):
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
                    context_path="/tmp/context",
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

    def test_drain_blocks_new_build_but_allows_existing_build_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            sandbox_manager = SandboxManager(
                sandbox_store,
                DockerGvisorRuntime(dry_run=True),
            )
            executor = BlockingBuildExecutor()
            image_manager = ImageManager(
                ImageStore(Path(raw_dir) / "images.json"),
                DockerImageRuntime(executor=executor),
                admission_store=sandbox_store,
            )
            spec = ImageBuildSpec(
                id="existing",
                tag="local/existing:latest",
                context_path="/tmp/context",
            )
            build, started = image_manager.start_build(spec)
            self.assertTrue(started)
            self.assertTrue(executor.started.wait(1))
            draining = sandbox_manager.configure_drain(
                "drain-build",
                True,
                active_build_count=image_manager.active_build_count,
            )

            self.assertFalse(draining.ready)
            replay, replay_started = image_manager.start_build(spec)
            self.assertEqual(replay.build_id, build.build_id)
            self.assertFalse(replay_started)
            with self.assertRaises(SandboxAdmissionClosedError):
                image_manager.start_build(
                    ImageBuildSpec(
                        id="blocked",
                        tag="local/blocked:latest",
                        context_path="/tmp/context",
                    )
                )
            executor.release.set()
            finished = image_manager.wait_for_build(build.build_id, timeout_seconds=2)
            self.assertIsNotNone(finished)
            ready = sandbox_manager.heartbeat_snapshot(
                active_build_count=image_manager.active_build_count
            )
            self.assertTrue(ready.ready)

    def test_multiprocess_build_cannot_enter_after_drain_ack(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_path = Path(raw_dir) / "sandboxes.json"
            build_path = Path(raw_dir) / "builds.json"
            sandbox_manager = SandboxManager(
                SandboxStore(sandbox_path),
                DockerGvisorRuntime(dry_run=True),
            )
            drained = sandbox_manager.configure_drain(
                "drain-build-process",
                True,
                active_build_count=lambda: 0,
            )
            self.assertTrue(drained.ready)
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_build_after_drain,
                    args=(
                        str(sandbox_path),
                        str(build_path),
                        index,
                        results,
                    ),
                )
                for index in range(4)
            ]
            for process in processes:
                process.start()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes, ["closed"] * 4)
            self.assertEqual(ImageBuildStore(build_path).load(), {})
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
                    context_path="/tmp/context",
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
                            context_path="/tmp/context",
                        )
                    )
            finally:
                executor.release.set()
                manager.wait_for_build(first.build_id, timeout_seconds=2)

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

    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        del input
        self.started.set()
        self.release.wait(2)
        return CommandResult(argv=argv, exit_code=0)


class ChattyBuildRuntime(DockerImageRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    def build(self, spec, *, on_output=None):  # type: ignore[no-untyped-def]
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


def _multiprocess_build_after_drain(
    sandbox_path: str,
    build_path: str,
    index: int,
    results,
) -> None:
    sandbox_store = SandboxStore(Path(sandbox_path))
    manager = ImageManager(
        ImageStore(Path(build_path).with_name(f"images-{index}.json")),
        DockerImageRuntime(dry_run=True),
        build_store=ImageBuildStore(Path(build_path)),
        admission_store=sandbox_store,
    )
    try:
        manager.start_build(
            ImageBuildSpec(
                id=f"blocked-{index}",
                tag=f"local/blocked-{index}:latest",
                context_path="/tmp/context",
            )
        )
    except SandboxAdmissionClosedError:
        results.put("closed")
    else:
        results.put("started")


if __name__ == "__main__":
    unittest.main()
