import asyncio
from dataclasses import replace
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from threading import Event, Lock
import unittest
from unittest.mock import patch

from ucloud_sandboxes.async_exec import AsyncExecSessionManager
from ucloud_sandboxes.sandbox import DockerGvisorRuntime, SandboxManager, SandboxSpec, SandboxStore
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class AsyncExecTests(unittest.TestCase):
    def test_cancellation_during_threaded_lease_acquisition_releases_lease(self) -> None:
        async def scenario() -> int:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                lifecycle = BlockingLifecycle()
                manager.lifecycle = lifecycle
                exec_manager = AsyncExecSessionManager(manager)
                task = asyncio.create_task(
                    exec_manager.start(
                        SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                    )
                )
                await asyncio.to_thread(lifecycle.started.wait)
                task.cancel()
                lifecycle.proceed.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                return lifecycle.shared

        self.assertEqual(asyncio.run(scenario()), 0)

    def test_cancellation_during_spawn_removes_session_and_kills_process(self) -> None:
        async def scenario() -> tuple[int, int, bool, list[str]]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                record, _result = manager.create(
                    SandboxSpec(id="sbx-1", image="busybox", memory_mb=128)
                )
                manager.store.upsert(replace(record, state="running"))
                lifecycle = TrackingLifecycle()
                manager.lifecycle = lifecycle
                runtime = CancelledSpawnRuntime()
                manager.runtime = runtime  # type: ignore[assignment]
                exec_manager = AsyncExecSessionManager(manager)
                spawn_started = asyncio.Event()
                finish_spawn = asyncio.Event()
                process = FakeAsyncProcess()

                async def create_process(*_args, **_kwargs):
                    spawn_started.set()
                    await finish_spawn.wait()
                    return process

                with patch(
                    "ucloud_sandboxes.async_exec.asyncio.create_subprocess_exec",
                    side_effect=create_process,
                ):
                    task = asyncio.create_task(
                        exec_manager.start(
                            SandboxExecSpec(
                                sandbox_id="sbx-1",
                                command=("ignored",),
                            )
                        )
                    )
                    await spawn_started.wait()
                    task.cancel()
                    finish_spawn.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                return (
                    lifecycle.shared,
                    len(exec_manager._sessions),  # noqa: SLF001
                    process.killed,
                    runtime.failed,
                )

        self.assertEqual(asyncio.run(scenario()), (0, 0, True, ["sbx-1"]))

    def test_session_and_event_history_are_bounded_without_evicting_active(self) -> None:
        async def scenario() -> tuple[int, bool, bool, list[str]]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                exec_manager = AsyncExecSessionManager(
                    manager,
                    max_sessions=3,
                    max_events_per_session=2,
                )
                sessions = []
                for _index in range(20):
                    sessions.append(
                        await exec_manager.start(
                            SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                        )
                    )
                newest_events = await exec_manager.events_after(sessions[-1].id)

                active_sessions = []
                for _index in range(3):
                    active_sessions.append(
                        await exec_manager.start(
                            SandboxExecSpec(
                                sandbox_id="sbx-1",
                                command=("cat",),
                                stdin=True,
                            )
                        )
                    )
                try:
                    await exec_manager.start(
                        SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                    )
                except RuntimeError as exc:
                    capacity_error = "capacity" in str(exc)
                else:
                    capacity_error = False
                return (
                    len(exec_manager._sessions),  # noqa: SLF001
                    exec_manager.get(sessions[0].id) is None,
                    exec_manager.get(active_sessions[0].id) is not None
                    and capacity_error,
                    [event.stream for event in newest_events],
                )

        count, oldest_evicted, active_retained, newest_streams = asyncio.run(scenario())
        self.assertEqual(count, 3)
        self.assertTrue(oldest_evicted)
        self.assertTrue(active_retained)
        self.assertEqual(newest_streams, ["status", "exit"])

    def test_eviction_awaits_and_releases_completed_process_tasks(self) -> None:
        async def scenario() -> tuple[bool, bool]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                record, _result = manager.create(
                    SandboxSpec(id="sbx-1", image="busybox", memory_mb=128)
                )
                manager.store.upsert(replace(record, state="running"))
                manager.runtime = LocalExecRuntime()  # type: ignore[assignment]
                exec_manager = AsyncExecSessionManager(manager, max_sessions=1)
                first = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("ignored",))
                )
                while True:
                    event = await asyncio.wait_for(
                        exec_manager.next_output_event(first.id),
                        timeout=2,
                    )
                    if event.stream == "exit":
                        break
                second = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("ignored",))
                )
                while True:
                    event = await asyncio.wait_for(
                        exec_manager.next_output_event(second.id),
                        timeout=2,
                    )
                    if event.stream == "exit":
                        break
                await asyncio.gather(*second.tasks, return_exceptions=True)
                return (
                    exec_manager.get(first.id) is None,
                    not first.tasks and bool(second.tasks),
                )

        self.assertEqual(asyncio.run(scenario()), (True, True))

    def test_full_websocket_queue_does_not_block_completion(self) -> None:
        async def scenario() -> tuple[str, str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                exec_manager = AsyncExecSessionManager(manager, max_queue_events=1)

                session = await asyncio.wait_for(
                    exec_manager.start(
                        SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                    ),
                    timeout=1,
                )
                event = await asyncio.wait_for(
                    exec_manager.next_output_event(session.id),
                    timeout=1,
                )
                return session.status, event.stream

        self.assertEqual(asyncio.run(scenario()), ("exited", "exit"))

    def test_dry_run_async_exec_streams_status_events(self) -> None:
        async def scenario() -> list[str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                exec_manager = AsyncExecSessionManager(
                    manager,
                    max_queue_events=4,
                    stream_chunk_bytes=1024,
                )
                session = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                )
                streams = []
                while True:
                    event = await exec_manager.next_output_event(session.id)
                    streams.append(event.stream)
                    if event.stream == "exit":
                        return streams

        self.assertEqual(asyncio.run(scenario()), ["status", "status", "exit"])

    def test_dry_run_async_stdin_session_closes(self) -> None:
        async def scenario() -> list[str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                exec_manager = AsyncExecSessionManager(manager, max_queue_events=8)
                session = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("cat",), stdin=True)
                )
                await exec_manager.write_stdin(session.id, b"hello\n")
                await exec_manager.close_stdin(session.id)
                streams = []
                while True:
                    event = await exec_manager.next_output_event(session.id)
                    streams.append(event.stream)
                    if event.stream == "exit":
                        return streams

        self.assertEqual(
            asyncio.run(scenario()),
            ["status", "status", "stdin", "stdin_closed", "exit"],
        )

    def test_exit_event_is_after_process_output(self) -> None:
        async def scenario() -> list[str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                record, _result = manager.create(
                    SandboxSpec(id="sbx-1", image="busybox", memory_mb=128)
                )
                manager.store.upsert(replace(record, state="running"))
                manager.runtime = LocalExecRuntime()  # type: ignore[assignment]
                exec_manager = AsyncExecSessionManager(manager, max_queue_events=8)
                session = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("ignored",))
                )
                streams = []
                while True:
                    event = await asyncio.wait_for(
                        exec_manager.next_output_event(session.id),
                        timeout=2,
                    )
                    streams.append(event.stream)
                    if event.stream == "exit":
                        return streams

        streams = asyncio.run(scenario())
        self.assertEqual(streams[0], "status")
        self.assertEqual(streams[-1], "exit")
        self.assertCountEqual(streams[1:-1], ["stdout", "stderr"])

    def test_runtime_is_notified_after_async_exec_process_is_spawned(self) -> None:
        async def scenario() -> tuple[list[str], str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                record, _result = manager.create(
                    SandboxSpec(id="sbx-1", image="busybox", memory_mb=128)
                )
                manager.store.upsert(replace(record, state="running"))
                runtime = HookedLocalExecRuntime()
                manager.runtime = runtime  # type: ignore[assignment]
                exec_manager = AsyncExecSessionManager(manager)
                session = await exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("ignored",))
                )
                while session.status == "running":
                    await asyncio.sleep(0.01)
                return runtime.started, session.status

        self.assertEqual(asyncio.run(scenario()), (["sbx-1"], "exited"))


class LocalExecRuntime:
    dry_run = False

    def exec_command(self, *_args: object, **_kwargs: object) -> tuple[str, ...]:
        return (
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(b'out'); sys.stderr.buffer.write(b'err')",
        )


class HookedLocalExecRuntime(LocalExecRuntime):
    def __init__(self) -> None:
        self.started: list[str] = []

    def exec_started(self, sandbox_id: str) -> None:
        self.started.append(sandbox_id)


class BlockingLifecycle:
    def __init__(self) -> None:
        self.started = Event()
        self.proceed = Event()
        self.guard = Lock()
        self.shared = 0

    def acquire_shared(self, _sandbox_id: str) -> None:
        self.started.set()
        self.proceed.wait()
        with self.guard:
            self.shared += 1

    def release_shared(self, _sandbox_id: str) -> None:
        with self.guard:
            self.shared -= 1


class TrackingLifecycle(BlockingLifecycle):
    def __init__(self) -> None:
        super().__init__()
        self.proceed.set()


class CancelledSpawnRuntime(LocalExecRuntime):
    def __init__(self) -> None:
        self.failed: list[str] = []

    def exec_start_failed(self, sandbox_id: str) -> None:
        self.failed.append(sandbox_id)


class FakeAsyncProcess:
    def __init__(self) -> None:
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.returncode: int | None = None
        self.killed = False

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


if __name__ == "__main__":
    unittest.main()
