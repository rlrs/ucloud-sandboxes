import asyncio
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
import unittest

from ucloud_sandboxes.sandbox import DockerGvisorRuntime, SandboxManager, SandboxSpec, SandboxStore
from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec


class SandboxExecTests(unittest.TestCase):
    def test_event_history_and_session_count_are_bounded(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
            exec_manager = ExecSessionManager(
                manager,
                max_sessions=1,
                max_events_per_session=2,
            )
            active = exec_manager.start(
                SandboxExecSpec(
                    sandbox_id="sbx-1",
                    command=("cat",),
                    stdin=True,
                )
            )

            with self.assertRaisesRegex(RuntimeError, "capacity"):
                exec_manager.start(
                    SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
                )

            exec_manager.close_stdin(active.id)
            replacement = exec_manager.start(
                SandboxExecSpec(sandbox_id="sbx-1", command=("true",))
            )
            events = exec_manager.drain_events(replacement.id)

            self.assertEqual(len(events), 2)
            self.assertEqual(events[-1].stream, "exit")

    def test_dry_run_exec_records_events(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
            exec_manager = ExecSessionManager(manager)

            session = exec_manager.start(
                SandboxExecSpec(
                    sandbox_id="sbx-1",
                    command=("echo", "ok"),
                )
            )
            events = exec_manager.drain_events(session.id)

            self.assertEqual(session.status, "exited")
            self.assertEqual(session.exit_code, 0)
            self.assertEqual([event.stream for event in events], ["status", "status", "exit"])
            self.assertEqual(session.argv[-2:], ("echo", "ok"))

    def test_stdin_session_can_be_written_and_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
            exec_manager = ExecSessionManager(manager)

            session = exec_manager.start(
                SandboxExecSpec(
                    sandbox_id="sbx-1",
                    command=("cat",),
                    stdin=True,
                )
            )
            exec_manager.write_stdin(session.id, "hello\n")
            closed = exec_manager.close_stdin(session.id)
            events = exec_manager.drain_events(session.id)

            self.assertEqual(closed.status, "exited")
            self.assertEqual(
                [event.stream for event in events],
                ["status", "status", "stdin", "stdin_closed", "exit"],
            )

    def test_async_methods_wrap_exec_session_operations(self) -> None:
        async def scenario() -> list[str]:
            with TemporaryDirectory() as raw_dir:
                manager = SandboxManager(
                    SandboxStore(Path(raw_dir) / "sandboxes.json"),
                    DockerGvisorRuntime(dry_run=True),
                )
                manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
                exec_manager = ExecSessionManager(manager)
                session = await exec_manager.astart(
                    SandboxExecSpec(
                        sandbox_id="sbx-1",
                        command=("true",),
                    )
                )
                events = await exec_manager.adrain_events(session.id)
                return [event.stream for event in events]

        self.assertEqual(asyncio.run(scenario()), ["status", "status", "exit"])

    def test_exit_event_is_after_process_output(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            manager.create(SandboxSpec(id="sbx-1", image="busybox", memory_mb=128))
            manager.runtime = LocalExecRuntime()  # type: ignore[assignment]
            exec_manager = ExecSessionManager(manager)

            session = exec_manager.start(
                SandboxExecSpec(
                    sandbox_id="sbx-1",
                    command=("ignored",),
                )
            )
            deadline = time.monotonic() + 2
            while exec_manager.get(session.id).status == "running":  # type: ignore[union-attr]
                if time.monotonic() >= deadline:
                    self.fail("exec session did not exit")
                time.sleep(0.01)
            events = exec_manager.drain_events(session.id)

            streams = [event.stream for event in events]
            self.assertEqual(streams[0], "status")
            self.assertEqual(streams[-1], "exit")
            self.assertCountEqual(streams[1:-1], ["stdout", "stderr"])


class LocalExecRuntime:
    dry_run = False

    def exec_command(self, *_args: object, **_kwargs: object) -> tuple[str, ...]:
        return (
            sys.executable,
            "-c",
            "import sys; sys.stdout.write('out'); sys.stderr.write('err')",
        )


if __name__ == "__main__":
    unittest.main()
