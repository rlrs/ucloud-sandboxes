import asyncio
from dataclasses import replace
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.async_exec import AsyncExecSessionManager
from ucloud_sandboxes.sandbox import DockerGvisorRuntime, SandboxManager, SandboxSpec, SandboxStore
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class AsyncExecTests(unittest.TestCase):
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


class LocalExecRuntime:
    dry_run = False

    def exec_command(self, *_args: object, **_kwargs: object) -> tuple[str, ...]:
        return (
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(b'out'); sys.stderr.buffer.write(b'err')",
        )


if __name__ == "__main__":
    unittest.main()
