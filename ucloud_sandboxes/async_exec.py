from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import subprocess
from typing import Any

from .models import utc_now
from .sandbox import SandboxManager
from .sandbox_exec import SandboxExecSpec, new_exec_session_id


STDOUT_STREAM_ID = 1
STDERR_STREAM_ID = 2
STREAM_IDS = {
    "stdout": STDOUT_STREAM_ID,
    "stderr": STDERR_STREAM_ID,
}
STREAM_NAMES = {value: key for key, value in STREAM_IDS.items()}


@dataclass(frozen=True)
class AsyncExecEvent:
    sequence: int
    stream: str
    data: bytes = b""
    exit_code: int | None = None
    created_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "stream": self.stream,
            "data": self.data.decode("utf-8", errors="replace"),
            "exit_code": self.exit_code,
            "created_at": self.created_at.isoformat(),
        }

    def binary_frame(self) -> bytes:
        stream_id = STREAM_IDS.get(self.stream)
        if stream_id is None:
            raise ValueError(f"stream is not binary: {self.stream}")
        return bytes([stream_id]) + self.data


@dataclass
class AsyncExecSession:
    id: str
    spec: SandboxExecSpec
    argv: tuple[str, ...]
    status: str
    created_at: datetime
    updated_at: datetime
    exit_code: int | None = None
    stdin_open: bool = False
    events: deque[AsyncExecEvent] = field(default_factory=deque)
    output_queue: asyncio.Queue[AsyncExecEvent] = field(default_factory=asyncio.Queue)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    next_sequence: int = 1
    process: asyncio.subprocess.Process | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    tasks: list[asyncio.Task[None]] = field(default_factory=list, repr=False, compare=False)
    activity_lease: bool = field(default=False, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "spec": self.spec.to_dict(),
            "argv": list(self.argv),
            "status": self.status,
            "exit_code": self.exit_code,
            "stdin_open": self.stdin_open,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class AsyncExecSessionManager:
    def __init__(
        self,
        sandbox_manager: SandboxManager,
        *,
        max_sessions: int = 128,
        max_events_per_session: int = 512,
        max_queue_events: int = 64,
        stream_chunk_bytes: int = 16 * 1024,
        route_node_id: str = "",
        route_job_id: str = "",
    ) -> None:
        self.sandbox_manager = sandbox_manager
        self.max_sessions = max(1, max_sessions)
        self.max_events_per_session = max(1, max_events_per_session)
        self.max_queue_events = max(1, max_queue_events)
        self.stream_chunk_bytes = max(1024, stream_chunk_bytes)
        self.route_node_id = route_node_id
        self.route_job_id = route_job_id
        self._sessions: dict[str, AsyncExecSession] = {}
        self._sessions_lock = asyncio.Lock()

    async def start(self, spec: SandboxExecSpec) -> AsyncExecSession:
        spec.validate()
        await asyncio.to_thread(
            self.sandbox_manager.lifecycle.acquire_shared,
            spec.sandbox_id,
        )
        runtime = self.sandbox_manager.runtime
        try:
            await asyncio.to_thread(
                self.sandbox_manager.require_activity_sandbox,
                spec.sandbox_id,
            )
            argv = runtime.exec_command(
                spec.sandbox_id,
                spec.command,
                env=spec.env,
                working_dir=spec.working_dir,
                interactive=spec.stdin,
                tty=spec.tty,
            )
        except Exception:
            await asyncio.to_thread(
                self.sandbox_manager.lifecycle.release_shared,
                spec.sandbox_id,
            )
            raise
        now = utc_now()
        session = AsyncExecSession(
            id=new_exec_session_id(
                spec.sandbox_id,
                node_id=self.route_node_id,
                job_id=self.route_job_id,
            ),
            spec=spec,
            argv=argv,
            status="running",
            created_at=now,
            updated_at=now,
            stdin_open=spec.stdin,
            output_queue=asyncio.Queue(maxsize=self.max_queue_events),
            events=deque(maxlen=self.max_events_per_session),
            activity_lease=True,
        )
        try:
            async with self._sessions_lock:
                await self._make_session_room_locked()
                self._sessions[session.id] = session
        except Exception:
            await asyncio.to_thread(
                self.sandbox_manager.lifecycle.release_shared,
                spec.sandbox_id,
            )
            session.activity_lease = False
            raise
        await self._append_event(session, "status", b"started")
        if runtime.dry_run:
            await self._append_event(session, "status", b"dry-run")
            if not session.stdin_open:
                await self._complete(session, 0)
            return session
        await self._start_process(session)
        return session

    def get(self, session_id: str) -> AsyncExecSession | None:
        return self._sessions.get(session_id)

    async def events_after(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
        wait_seconds: float = 0.0,
    ) -> list[AsyncExecEvent]:
        session = self._require_session(session_id)
        deadline = asyncio.get_running_loop().time() + max(0.0, wait_seconds)
        async with session.condition:
            while True:
                events = [event for event in session.events if event.sequence > after]
                if events or session.status in {"exited", "failed"} or wait_seconds <= 0:
                    return events[: max(0, limit)]
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return []
                try:
                    await asyncio.wait_for(session.condition.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return []

    async def next_output_event(self, session_id: str) -> AsyncExecEvent:
        session = self._require_session(session_id)
        return await session.output_queue.get()

    async def write_stdin(self, session_id: str, data: bytes) -> AsyncExecSession:
        session = self._require_session(session_id)
        if not session.stdin_open:
            raise ValueError("stdin is closed for this exec session.")
        if session.process is None:
            await self._append_event(session, "stdin", data)
            return session
        stdin = session.process.stdin
        if stdin is None:
            raise ValueError("stdin pipe is unavailable.")
        stdin.write(data)
        await stdin.drain()
        session.updated_at = utc_now()
        return session

    async def close_stdin(self, session_id: str) -> AsyncExecSession:
        session = self._require_session(session_id)
        if not session.stdin_open:
            return session
        session.stdin_open = False
        session.updated_at = utc_now()
        if session.process is None:
            await self._append_event(session, "stdin_closed", b"")
            await self._complete(session, 0)
            return session
        stdin = session.process.stdin
        if stdin is not None:
            stdin.close()
        return session

    async def _start_process(self, session: AsyncExecSession) -> None:
        try:
            process = await asyncio.create_subprocess_exec(
                *session.argv,
                stdin=subprocess.PIPE if session.spec.stdin else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            await self._append_event(session, "error", str(exc).encode("utf-8"))
            await self._complete(session, 1)
            return
        session.process = process
        stream_tasks = [
            asyncio.create_task(self._pump_stream(session, "stdout", process.stdout)),
            asyncio.create_task(self._pump_stream(session, "stderr", process.stderr)),
        ]
        session.tasks = [
            *stream_tasks,
            asyncio.create_task(self._wait_process(session, process, stream_tasks)),
        ]

    async def _pump_stream(
        self,
        session: AsyncExecSession,
        stream: str,
        reader: asyncio.StreamReader | None,
    ) -> None:
        if reader is None:
            return
        while True:
            chunk = await reader.read(self.stream_chunk_bytes)
            if not chunk:
                return
            await self._append_event(session, stream, chunk)

    async def _wait_process(
        self,
        session: AsyncExecSession,
        process: asyncio.subprocess.Process,
        stream_tasks: list[asyncio.Task[None]],
    ) -> None:
        exit_code = await process.wait()
        try:
            await asyncio.wait_for(
                asyncio.gather(*stream_tasks, return_exceptions=True),
                timeout=2.0,
            )
        except asyncio.TimeoutError:
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)
        session.stdin_open = False
        await self._complete(session, exit_code)

    async def _make_session_room_locked(self) -> None:
        if len(self._sessions) < self.max_sessions:
            return
        terminal = sorted(
            (
                session
                for session in self._sessions.values()
                if session.status in {"exited", "failed"}
            ),
            key=lambda session: (session.updated_at, session.id),
        )
        for session in terminal:
            self._sessions.pop(session.id, None)
            await self._cleanup_session_tasks(session)
            if len(self._sessions) < self.max_sessions:
                return
        raise RuntimeError("exec session capacity reached")

    @staticmethod
    async def _cleanup_session_tasks(session: AsyncExecSession) -> None:
        current = asyncio.current_task()
        tasks = [task for task in session.tasks if task is not current]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        session.tasks.clear()

    def _require_session(self, session_id: str) -> AsyncExecSession:
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"exec session not found: {session_id}")
        return session

    async def _append_event(
        self,
        session: AsyncExecSession,
        stream: str,
        data: bytes,
        *,
        exit_code: int | None = None,
    ) -> AsyncExecEvent:
        async with session.condition:
            event = AsyncExecEvent(
                sequence=session.next_sequence,
                stream=stream,
                data=data,
                exit_code=exit_code,
            )
            session.next_sequence += 1
            session.updated_at = utc_now()
            session.events.append(event)
            session.condition.notify_all()
        # WebSocket delivery is best effort; the bounded replay deque above is
        # the authoritative polling history.  Never let an absent or slow
        # WebSocket consumer block stdout/stderr pumps or the final exit event.
        if session.output_queue.full():
            try:
                session.output_queue.get_nowait()
                session.output_queue.task_done()
            except asyncio.QueueEmpty:
                pass
        try:
            session.output_queue.put_nowait(event)
        except asyncio.QueueFull:
            # A concurrent consumer/publisher can change queue state between
            # the checks.  The replay deque still retains this event.
            pass
        return event

    async def _complete(self, session: AsyncExecSession, exit_code: int) -> None:
        if session.status in {"exited", "failed"}:
            return
        session.exit_code = exit_code
        session.status = "exited" if exit_code == 0 else "failed"
        await self._append_event(session, "exit", b"", exit_code=exit_code)
        if session.activity_lease:
            session.activity_lease = False
            await asyncio.to_thread(
                self.sandbox_manager.lifecycle.release_shared,
                session.spec.sandbox_id,
            )
