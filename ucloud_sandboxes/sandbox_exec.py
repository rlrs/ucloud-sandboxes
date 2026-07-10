from __future__ import annotations

import asyncio
import base64
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import json
import subprocess
from threading import Condition, RLock, Thread
import time
from typing import Any
from uuid import uuid4

from .models import utc_now
from .sandbox import SANDBOX_ID_RE, SandboxManager


EXEC_SESSION_ID_PREFIX = "exec-"
ROUTABLE_EXEC_SESSION_ID_PREFIX = "exec-v1."
MAX_ROUTABLE_EXEC_SESSION_ID_LENGTH = 1024


@dataclass(frozen=True)
class ExecSessionRoute:
    sandbox_id: str
    node_id: str
    job_id: str


def new_exec_session_id(
    sandbox_id: str,
    *,
    node_id: str = "",
    job_id: str = "",
) -> str:
    if not node_id or not job_id:
        return EXEC_SESSION_ID_PREFIX + uuid4().hex
    encoded = _exec_session_route_payload(sandbox_id, node_id, job_id)
    return f"{ROUTABLE_EXEC_SESSION_ID_PREFIX}{encoded}.{uuid4().hex}"


def exec_session_sandbox_id(session_id: str) -> str | None:
    route = exec_session_route(session_id)
    return route.sandbox_id if route is not None else None


def exec_session_route(session_id: str) -> ExecSessionRoute | None:
    if len(session_id) > MAX_ROUTABLE_EXEC_SESSION_ID_LENGTH:
        return None
    if not session_id.startswith(ROUTABLE_EXEC_SESSION_ID_PREFIX):
        return None
    parts = session_id.split(".")
    if len(parts) != 3 or parts[0] != "exec-v1" or len(parts[2]) != 32:
        return None
    try:
        int(parts[2], 16)
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = base64.b64decode(
            padded,
            altchars=b"-_",
            validate=True,
        )
        raw = json.loads(decoded.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    sandbox_id = raw.get("s")
    node_id = raw.get("n")
    job_id = raw.get("j")
    if (
        not isinstance(sandbox_id, str)
        or SANDBOX_ID_RE.fullmatch(sandbox_id) is None
        or not isinstance(node_id, str)
        or not node_id
        or len(node_id) > 128
        or not isinstance(job_id, str)
        or not job_id
        or len(job_id) > 128
        or _exec_session_route_payload(sandbox_id, node_id, job_id) != parts[1]
    ):
        return None
    return ExecSessionRoute(sandbox_id, node_id, job_id)


def _exec_session_route_payload(sandbox_id: str, node_id: str, job_id: str) -> str:
    raw = json.dumps(
        {"j": job_id, "n": node_id, "s": sandbox_id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


@dataclass(frozen=True)
class SandboxExecSpec:
    sandbox_id: str
    command: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    working_dir: str | None = None
    stdin: bool = False
    tty: bool = False

    @classmethod
    def from_dict(
        cls,
        raw: dict[str, Any],
        *,
        sandbox_id: str | None = None,
    ) -> "SandboxExecSpec":
        command = raw.get("command", ())
        if isinstance(command, str):
            command_items = (command,)
        elif isinstance(command, list) and all(isinstance(item, str) for item in command):
            command_items = tuple(command)
        else:
            command_items = ()
        env = raw.get("env") or {}
        return cls(
            sandbox_id=str(sandbox_id or raw.get("sandbox_id") or ""),
            command=command_items,
            env={str(k): str(v) for k, v in dict(env).items()},
            working_dir=str(raw["working_dir"]) if raw.get("working_dir") else None,
            stdin=bool(raw.get("stdin", False)),
            tty=bool(raw.get("tty", False)),
        )

    def validate(self) -> None:
        if not self.sandbox_id:
            raise ValueError("sandbox id is required.")
        if not self.command:
            raise ValueError("exec command cannot be empty.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "sandbox_id": self.sandbox_id,
            "command": list(self.command),
            "env": dict(self.env),
            "working_dir": self.working_dir,
            "stdin": self.stdin,
            "tty": self.tty,
        }


@dataclass(frozen=True)
class ExecEvent:
    sequence: int
    stream: str
    data: str = ""
    exit_code: int | None = None
    created_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "stream": self.stream,
            "data": self.data,
            "exit_code": self.exit_code,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ExecSession:
    id: str
    spec: SandboxExecSpec
    argv: tuple[str, ...]
    status: str
    created_at: datetime
    updated_at: datetime
    condition: Condition = field(repr=False, compare=False)
    exit_code: int | None = None
    stdin_open: bool = False
    events: deque[ExecEvent] = field(default_factory=deque)
    next_sequence: int = 1
    process: subprocess.Popen[str] | None = field(default=None, repr=False, compare=False)

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


class ExecSessionManager:
    def __init__(
        self,
        sandbox_manager: SandboxManager,
        *,
        max_sessions: int = 128,
        max_events_per_session: int = 512,
        route_node_id: str = "",
        route_job_id: str = "",
    ) -> None:
        self.sandbox_manager = sandbox_manager
        self.max_sessions = max(1, max_sessions)
        self.max_events_per_session = max(1, max_events_per_session)
        self.route_node_id = route_node_id
        self.route_job_id = route_job_id
        self._sessions: dict[str, ExecSession] = {}
        self._lock = RLock()

    def start(self, spec: SandboxExecSpec) -> ExecSession:
        spec.validate()
        record = self.sandbox_manager.get(spec.sandbox_id)
        if record is None:
            raise ValueError(f"sandbox not found: {spec.sandbox_id}")
        runtime = self.sandbox_manager.runtime
        argv = runtime.exec_command(
            spec.sandbox_id,
            spec.command,
            env=spec.env,
            working_dir=spec.working_dir,
            interactive=spec.stdin,
            tty=spec.tty,
        )
        now = utc_now()
        session = ExecSession(
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
            condition=Condition(self._lock),
            stdin_open=spec.stdin,
            events=deque(maxlen=self.max_events_per_session),
        )
        with self._lock:
            self._make_session_room_locked()
            self._sessions[session.id] = session
            self._append_event_locked(session, "status", "started")
        if runtime.dry_run:
            self._finish_dry_run(session)
            return session
        self._start_process(session)
        return session

    async def astart(self, spec: SandboxExecSpec) -> ExecSession:
        return await asyncio.to_thread(self.start, spec)

    def get(self, session_id: str) -> ExecSession | None:
        with self._lock:
            return self._sessions.get(session_id)

    async def aget(self, session_id: str) -> ExecSession | None:
        return await asyncio.to_thread(self.get, session_id)

    def drain_events(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> list[ExecEvent]:
        with self._lock:
            session = self._require_session_locked(session_id)
            events = [event for event in session.events if event.sequence > after]
            return events[: max(0, limit)]

    def events_after(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
        wait_seconds: float = 0.0,
    ) -> list[ExecEvent]:
        deadline = time.monotonic() + max(0.0, wait_seconds)
        with self._lock:
            session = self._require_session_locked(session_id)
        with session.condition:
            while True:
                events = [event for event in session.events if event.sequence > after]
                if (
                    events
                    or session.status in {"exited", "failed"}
                    or wait_seconds <= 0
                ):
                    return events[: max(0, limit)]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                session.condition.wait(timeout=remaining)

    async def adrain_events(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> list[ExecEvent]:
        return await asyncio.to_thread(
            self.drain_events,
            session_id,
            after=after,
            limit=limit,
        )

    def write_stdin(self, session_id: str, data: str) -> ExecSession:
        with self._lock:
            session = self._require_session_locked(session_id)
            if not session.stdin_open:
                raise ValueError("stdin is closed for this exec session.")
            if session.process is None:
                self._append_event_locked(session, "stdin", data)
                return session
            stdin = session.process.stdin
            if stdin is None:
                raise ValueError("stdin pipe is unavailable.")
            stdin.write(data)
            stdin.flush()
            session.updated_at = utc_now()
            return session

    async def awrite_stdin(self, session_id: str, data: str) -> ExecSession:
        return await asyncio.to_thread(self.write_stdin, session_id, data)

    def close_stdin(self, session_id: str) -> ExecSession:
        with self._lock:
            session = self._require_session_locked(session_id)
            if not session.stdin_open:
                return session
            session.stdin_open = False
            session.updated_at = utc_now()
            if session.process is None:
                self._append_event_locked(session, "stdin_closed", "")
                self._complete_locked(session, 0)
                return session
            stdin = session.process.stdin
            if stdin is not None:
                stdin.close()
            return session

    async def aclose_stdin(self, session_id: str) -> ExecSession:
        return await asyncio.to_thread(self.close_stdin, session_id)

    def _finish_dry_run(self, session: ExecSession) -> None:
        with self._lock:
            self._append_event_locked(session, "status", "dry-run")
            if not session.stdin_open:
                self._complete_locked(session, 0)

    def _start_process(self, session: ExecSession) -> None:
        try:
            process = subprocess.Popen(
                list(session.argv),
                stdin=subprocess.PIPE if session.spec.stdin else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            with self._lock:
                self._append_event_locked(session, "error", str(exc))
                self._complete_locked(session, 1)
            return
        with self._lock:
            session.process = process
        stdout_thread = Thread(
            target=self._pump_stream,
            args=(session.id, "stdout", process.stdout),
            daemon=True,
        )
        stderr_thread = Thread(
            target=self._pump_stream,
            args=(session.id, "stderr", process.stderr),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        Thread(
            target=self._wait_process,
            args=(session.id, process, (stdout_thread, stderr_thread)),
            daemon=True,
        ).start()

    def _pump_stream(
        self,
        session_id: str,
        stream: str,
        pipe: Any,
    ) -> None:
        if pipe is None:
            return
        try:
            while True:
                chunk = pipe.read(4096)
                if chunk == "":
                    break
                with self._lock:
                    session = self._sessions.get(session_id)
                    if session is None:
                        return
                    self._append_event_locked(session, stream, chunk)
        finally:
            pipe.close()

    def _wait_process(
        self,
        session_id: str,
        process: subprocess.Popen[str],
        pump_threads: tuple[Thread, Thread],
    ) -> None:
        exit_code = process.wait()
        for thread in pump_threads:
            thread.join(timeout=2.0)
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return
            session.stdin_open = False
            self._complete_locked(session, exit_code)

    def _require_session_locked(self, session_id: str) -> ExecSession:
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"exec session not found: {session_id}")
        return session

    def _make_session_room_locked(self) -> None:
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
            if len(self._sessions) < self.max_sessions:
                return
        raise RuntimeError("exec session capacity reached")

    def _append_event_locked(
        self,
        session: ExecSession,
        stream: str,
        data: str,
        *,
        exit_code: int | None = None,
    ) -> None:
        session.events.append(
            ExecEvent(
                sequence=session.next_sequence,
                stream=stream,
                data=data,
                exit_code=exit_code,
            )
        )
        session.next_sequence += 1
        session.updated_at = utc_now()
        session.condition.notify_all()

    def _complete_locked(self, session: ExecSession, exit_code: int) -> None:
        if session.status in {"exited", "failed"}:
            return
        session.exit_code = exit_code
        session.status = "exited" if exit_code == 0 else "failed"
        self._append_event_locked(session, "exit", "", exit_code=exit_code)
