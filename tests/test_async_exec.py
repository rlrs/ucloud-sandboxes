import asyncio
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.async_exec import AsyncExecSessionManager
from ucloud_sandboxes.sandbox import DockerGvisorRuntime, SandboxManager, SandboxSpec, SandboxStore
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class AsyncExecTests(unittest.TestCase):
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
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
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
