"""Warden side of the in-guest agent (plan C5.1).

The wire contract is the doc comment of runtime/managed_process/agent_linux.go;
docs/guest-agent-protocol.md explains it. A GuestAgentListener owns one host
socket per sandbox and one thread, which accepts the agent's connection and
reads it. Caller threads write; the thread only flushes what a full socket
buffer left behind. Ops never outlive their connection: when it ends, every op
without a terminal frame fails with GuestAgentDisconnected, and the agent kills
it. Pause and resume keep the connection, so ops simply stall.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import re
import selectors
import socket
import stat as stat_mode
import struct
import threading
import time
from typing import Any, BinaryIO, Iterator

PROTOCOL_VERSION = 1
DEFAULT_WINDOW = 1 << 20
MIN_WINDOW = 64 << 10
MAX_WINDOW = 64 << 20
MAX_HEADER_BYTES = 1 << 20
MAX_PAYLOAD_BYTES = 1 << 20
MAX_FILE_BYTES = 256 << 20
MAX_OPS = 1024
_MAX_ID = (1 << 53) - 1
_PREFIX = struct.Struct(">II")
_READ_BYTES = 128 << 10  # initial per-connection read buffer; grows for large frames
_ENV_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_STREAMS = ("stdout", "stderr")
# Exact header keys of every frame the node accepts.
_INBOUND_KEYS = {
    kind: frozenset({"type", *keys})
    for kind, keys in {
        "hello": ("version", "agent", "pid", "build", "abandoned"),
        "started": ("id", "pid"),
        "output": ("id", "stream"),
        "input_credit": ("id", "offset"),
        "exit": ("id", "exit_code", "signal", "stdout_bytes", "stderr_bytes", "output_complete"),
        "done": ("id", "stat"),
        "error": ("id", "code", "message"),
        "ping": (),
    }.items()
}
_STAT_INTS = ("size", "mode", "mtime_ns", "uid", "gid")
_STAT_TYPES = ("file", "directory", "other")  # a tuple: membership must not hash a peer's list
_ACCEPT_RETRY_SECONDS = 0.5
_LOG = logging.getLogger(__name__)


class GuestAgentError(RuntimeError):
    """An op failed; code is the agent's error code or a node-side one."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class GuestAgentUnavailable(GuestAgentError):
    """No agent connection was ready. Nothing was sent; retrying is safe."""


class GuestAgentDisconnected(GuestAgentError):
    """The connection ended first. The agent killed the op; effects are unknown."""


class GuestAgentTimeout(GuestAgentError):
    """The caller's deadline passed. The op was killed; effects are unknown."""


class _Violation(Exception):
    pass


@dataclass(frozen=True)
class AgentIdentity:
    agent: str
    pid: int
    build: str
    abandoned: int  # ops the agent killed when its previous connection ended


@dataclass(frozen=True)
class ExecOutput:
    stream: str
    data: bytes


@dataclass(frozen=True)
class ExecExit:
    exit_code: int | None  # None exactly when signal is set
    signal: int | None
    output_complete: bool  # False: a descendant held the output open

    @property
    def status(self) -> int:
        """The runsc exec convention: 128 + signal for a signalled command."""
        return self.exit_code if self.signal is None else 128 + self.signal


@dataclass(frozen=True)
class GuestFileStat:
    type: str  # file, directory or other; symlinks are followed
    size: int
    mode: int
    mtime_ns: int
    uid: int
    gid: int


def _int(header: dict, key: str, low: int = 0, high: int = _MAX_ID) -> int:
    value = header[key]
    if type(value) is not int or not low <= value <= high:
        raise _Violation(f"{key} is invalid")
    return value


def _str(header: dict, key: str) -> str:
    if type(header[key]) is not str:
        raise _Violation(f"{key} is not a string")
    return header[key]


def _text(value: object) -> bool:
    """A NUL-free string the agent decodes unchanged."""
    if type(value) is not str or "\0" in value:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _identity(uid: int, gid: int) -> dict[str, int]:
    if any(type(value) is not int or not 0 <= value < 1 << 32 for value in (uid, gid)):
        raise ValueError("uid and gid must be uint32 integers")
    return {"uid": uid, "gid": gid}


def _file_path(path: str) -> str:
    if not _text(path) or not path.startswith("/") or ".." in path.split("/") or any(
        ord(char) < 32 or ord(char) == 127 for char in path
    ):
        raise ValueError("guest file path must be absolute, without .. or control characters")
    return path


class _Connection:
    def __init__(self, sock: socket.socket, window: int) -> None:
        self.sock = sock
        self.window = window
        self.lock = threading.Lock()  # never held while taking an op's lock
        self.ops: dict[int, _Op] = {}
        self.next_id = 1
        self.out = bytearray()  # unsent bytes, bounded by credit and op count
        self.closed: GuestAgentError | None = None
        self.broken = False  # a caller's write failed; the listener thread drops it
        self.identity: AgentIdentity | None = None
        self.buffer = bytearray(_READ_BYTES)  # listener thread only
        self.start = self.end = 0

    def send(self, header: dict, payload: bytes | memoryview = b"", *, wake, op: _Op | None = None) -> None:
        with self.lock:
            if self.closed is not None or self.broken:
                if op is not None:
                    raise GuestAgentUnavailable("unavailable", "the agent connection closed")
                raise self.closed or GuestAgentDisconnected("disconnected", "writing to the agent failed")
            if op is not None:
                if len(self.ops) >= MAX_OPS:
                    raise GuestAgentError("too_many_ops", "the agent connection has too many live ops")
                # Issue and send under one lock: the agent requires increasing ids.
                header["id"] = self.next_id
            encoded = json.dumps(header, separators=(",", ":")).encode()
            if len(encoded) > MAX_HEADER_BYTES:  # the agent would drop the connection and every op
                raise ValueError("the request exceeds the 1 MiB frame header limit")
            if op is not None:
                op.id, self.next_id = self.next_id, self.next_id + 1
                self.ops[op.id] = op
            head = _PREFIX.pack(len(encoded), len(payload)) + encoded
            sent = 0
            if not self.out:
                try:
                    sent = self.sock.sendmsg([head, payload] if payload else [head])
                except (BlockingIOError, InterruptedError):
                    pass
                except OSError:
                    self.broken = True
                if not self.broken and sent == len(head) + len(payload):
                    return
            if not self.broken:
                if sent < len(head):
                    self.out += head[sent:]
                    self.out += payload
                else:
                    self.out += payload[sent - len(head):]
        wake()


class _Op:
    """One op's state. The listener thread applies frames under _cond."""

    def __init__(self, listener: GuestAgentListener, conn: _Connection, kind: str, *, takes_input: bool, limit: int = 0) -> None:
        self._listener = listener
        self._conn = conn
        self.kind = kind
        self.id = 0
        self.pid: int | None = None
        self._cond = threading.Condition(threading.Lock())
        self._input_lock = threading.Lock()
        self._takes_input = takes_input
        self._limit = limit  # read_file bytes; the peer is guest code, so the node enforces it
        forwarded = {"exec": _STREAMS, "read_file": ("stdout",)}.get(kind, ())
        self._received = dict.fromkeys(forwarded, 0)
        self._consumed = dict.fromkeys(forwarded, 0)
        self._credited = dict.fromkeys(forwarded, 0)
        self._events: deque[ExecOutput] = deque()
        self._data = bytearray()  # read_file content
        self._terminal: Any = None
        self._discard = kind != "exec"  # file ops consume on receipt
        self._input_sent = self._input_credit = 0
        self._input_closed = False

    def _send(self, header: dict, payload: bytes | memoryview = b"") -> None:
        self._conn.send(header, payload, wake=self._listener._wake)

    def _consume_locked(self, stream: str, size: int) -> int | None:
        self._consumed[stream] += size
        if self._consumed[stream] - self._credited[stream] < self._conn.window // 4:
            return None
        self._credited[stream] = self._consumed[stream]
        return self._credited[stream]

    def _credit(self, stream: str, offset: int | None) -> None:
        if offset is not None and self._terminal is None:
            try:
                self._send({"type": "credit", "id": self.id, "stream": stream, "offset": offset})
            except GuestAgentError:
                pass  # The connection ended; the terminal reports it.

    def _on_frame(self, kind: str, header: dict, payload: bytes) -> bool:
        """Apply one frame under _cond; True when it was terminal."""
        if kind == "started":
            if self.kind != "exec" or self.pid is not None:
                raise _Violation("unexpected started")
            self.pid = _int(header, "pid", 1, (1 << 31) - 1)
        elif kind == "output":
            stream = _str(header, "stream")
            if stream not in self._received or (self.kind == "exec" and self.pid is None):
                raise _Violation(f"unexpected {stream} output")
            self._received[stream] += len(payload)
            if self._received[stream] - self._credited[stream] > self._conn.window:
                raise _Violation("output exceeds its credit")
            if not self._discard:
                self._events.append(ExecOutput(stream, payload))
            else:
                if self.kind == "read_file":
                    if len(self._data) + len(payload) > self._limit:
                        raise _Violation("read_file returned more than max_bytes")
                    self._data += payload
                # Lock order is op, then connection: sending here is safe.
                self._credit(stream, self._consume_locked(stream, len(payload)))
        elif kind == "input_credit":
            offset = _int(header, "offset")
            if not self._takes_input or not self._input_credit <= offset <= self._input_sent:
                raise _Violation("input credit is out of range")
            self._input_credit = offset
        else:
            self._terminal = self._terminal_value(kind, header)
        self._cond.notify_all()
        return self._terminal is not None

    def _terminal_value(self, kind: str, header: dict) -> Any:
        if kind == "error":
            return GuestAgentError(_str(header, "code"), _str(header, "message"))
        if kind == "exit":
            if self.kind != "exec" or self.pid is None:
                raise _Violation("unexpected exit")
            if (header["exit_code"] is None) == (header["signal"] is None):
                raise _Violation("exit needs exactly one of exit_code and signal")
            code = None if header["exit_code"] is None else _int(header, "exit_code", 0, 255)
            number = None if header["signal"] is None else _int(header, "signal", 1, 64)
            if any(_int(header, f"{stream}_bytes") != self._received[stream] for stream in _STREAMS):
                raise _Violation("exit disagrees with the output received")
            if type(header["output_complete"]) is not bool:
                raise _Violation("output_complete is not a boolean")
            return ExecExit(code, number, header["output_complete"])
        stat = header["stat"]
        if self.kind == "exec" or (stat is None) != (self.kind != "stat"):
            raise _Violation("unexpected done")
        if stat is None:
            return True
        if type(stat) is not dict or stat.keys() != {"type", *_STAT_INTS} or stat["type"] not in _STAT_TYPES:
            raise _Violation("stat is malformed")
        return GuestFileStat(type=stat["type"], **{key: _int(stat, key, -(1 << 63), (1 << 63) - 1) for key in _STAT_INTS})

    def _lost(self, error: GuestAgentError) -> None:
        with self._cond:
            if self._terminal is None:
                self._terminal = error
                self._cond.notify_all()

    def _wait_terminal(self, deadline: float) -> Any:
        with self._cond:
            while self._terminal is None and (remaining := deadline - time.monotonic()) > 0:
                self._cond.wait(remaining)
            terminal = self._terminal
        if terminal is None:
            self._abandon()
            raise GuestAgentTimeout("timeout", f"guest {self.kind} did not finish in time")
        if isinstance(terminal, GuestAgentError):
            raise terminal
        return terminal

    def _abandon(self) -> None:
        """Kill a live op and drop its output; its terminal frame frees it.

        The agent applies frames in order, so the kill reaches an op whose
        started frame has not arrived yet.
        """
        with self._cond:
            if self._terminal is not None:
                return
            self._discard = True
            credits = [(event.stream, self._consume_locked(event.stream, len(event.data))) for event in self._events]
            self._events.clear()
        for stream, offset in credits:
            self._credit(stream, offset)
        try:
            self._send({"type": "signal", "id": self.id, "signal": 9})
        except GuestAgentError:
            pass

    def _send_input(self, data: bytes | bytearray | memoryview, deadline: float | None) -> None:
        """Send input as credit allows. The caller holds _input_lock."""
        view = memoryview(data).cast("B")
        offset = 0
        while offset < len(view):
            with self._cond:
                while (room := self._conn.window - (self._input_sent - self._input_credit)) <= 0 or (
                    self._terminal is not None or self._input_closed
                ):
                    if isinstance(self._terminal, GuestAgentError):
                        raise self._terminal
                    if self._terminal is not None or self._input_closed:
                        raise GuestAgentError("input_closed", "the op no longer takes input")
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise GuestAgentTimeout("timeout", "the agent did not credit input in time")
                    self._cond.wait(remaining)
                size = min(room, MAX_PAYLOAD_BYTES, len(view) - offset)
                self._input_sent += size
            self._send({"type": "input", "id": self.id}, view[offset:offset + size])
            offset += size

    def _close_input(self) -> None:
        with self._input_lock:
            with self._cond:
                if self._input_closed or self._terminal is not None:
                    return
                self._input_closed = True
            self._send({"type": "input_close", "id": self.id})


class GuestExecSession(_Op):
    """A started guest command. Output is credited as the caller consumes it."""

    def next_event(self, timeout: float | None = None) -> ExecOutput | ExecExit | None:
        """Return the next output chunk, then the exit; None on timeout.

        Raises the op's GuestAgentError once queued output is consumed.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while not self._events and self._terminal is None:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._cond.wait(remaining)
            if not self._events:
                if isinstance(self._terminal, GuestAgentError):
                    raise self._terminal
                return self._terminal
            event = self._events.popleft()
            offset = self._consume_locked(event.stream, len(event.data))
        self._credit(event.stream, offset)
        return event

    def __iter__(self) -> Iterator[ExecOutput | ExecExit]:
        while not isinstance(event := self.next_event(), ExecExit):
            yield event
        yield event

    def communicate(self, timeout: float | None = None) -> tuple[ExecExit, bytes, bytes]:
        deadline = None if timeout is None else time.monotonic() + timeout
        output = {stream: bytearray() for stream in _STREAMS}
        while True:
            event = self.next_event(None if deadline is None else max(0.0, deadline - time.monotonic()))
            if event is None:
                self._abandon()
                raise GuestAgentTimeout("timeout", "guest exec did not finish in time")
            if isinstance(event, ExecExit):
                return event, bytes(output["stdout"]), bytes(output["stderr"])
            output[event.stream] += event.data

    def write_stdin(self, data: bytes | bytearray | memoryview, timeout: float | None = None) -> None:
        """Send stdin, waiting for credit once a window is outstanding."""
        if not self._takes_input:
            raise GuestAgentError("input_closed", "the exec was started without stdin")
        with self._input_lock:
            self._send_input(data, None if timeout is None else time.monotonic() + timeout)

    def close_stdin(self) -> None:
        if self._takes_input:
            self._close_input()

    def signal(self, number: int) -> None:
        """Signal the command's process group; a no-op once it has ended."""
        if isinstance(number, bool) or not isinstance(number, int) or not 1 <= number <= 64:
            raise ValueError("signal must be an integer in [1, 64]")
        with self._cond:
            if self._terminal is not None:
                return
        self._send({"type": "signal", "id": self.id, "signal": int(number)})

    def close(self) -> None:
        """Kill the command if it is still running and drop its output."""
        self._abandon()

    def __enter__(self) -> GuestExecSession:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class GuestAgentListener:
    """The per-sandbox host socket the in-guest agent dials (--host-uds=open)."""

    def __init__(self, socket_path: str | os.PathLike[str], *, window: int = DEFAULT_WINDOW) -> None:
        if type(window) is not int or not MIN_WINDOW <= window <= MAX_WINDOW:
            raise ValueError(f"window must be {MIN_WINDOW}..{MAX_WINDOW} bytes")
        self.socket_path = Path(socket_path)
        self.window = window
        self._cond = threading.Condition()
        self._conn: _Connection | None = None
        self._closing = False
        self._listener: socket.socket | None = None
        self._inode: tuple[int, int] | None = None
        self._waker_r = self._waker_w = None
        self._thread = threading.Thread(target=self._run, name=f"guest-agent:{self.socket_path.name}", daemon=True)

    def start(self) -> GuestAgentListener:
        path = self.socket_path
        parent = os.lstat(path.parent)
        # The directory is bind-mounted into the guest. Only its owner may
        # create or replace names in it.
        if not path.is_absolute() or not stat_mode.S_ISDIR(parent.st_mode) or (
            parent.st_uid != os.geteuid() or parent.st_mode & 0o022
        ):
            raise ValueError("guest agent socket directory must be owned by this user and not shared-writable")
        temporary = path.parent / f".{os.urandom(6).hex()}.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(temporary))
            os.chmod(temporary, 0o600)
            listener.listen(8)
            # Rename publishes a listening, owner-only socket in one step and
            # replaces a stale one left by a previous owner.
            os.rename(temporary, path)
        except BaseException:
            listener.close()
            temporary.unlink(missing_ok=True)
            raise
        listener.setblocking(False)
        published = os.lstat(path)
        self._listener, self._inode = listener, (published.st_dev, published.st_ino)
        self._waker_r, self._waker_w = socket.socketpair()
        self._waker_r.setblocking(False)
        self._waker_w.setblocking(False)
        self._thread.start()
        return self

    def close(self) -> None:
        """Fail live ops, stop the thread and remove the socket if still ours."""
        with self._cond:
            self._closing = True
            self._cond.notify_all()  # callers waiting for a connection fail now
        self._wake()
        if self._thread.is_alive():
            self._thread.join()
        if self._listener is None:
            return
        self._listener.close()
        try:
            current = os.lstat(self.socket_path)
            if (current.st_dev, current.st_ino) == self._inode:
                self.socket_path.unlink()
        except FileNotFoundError:
            pass
        self._waker_r.close()
        self._waker_w.close()

    def __enter__(self) -> GuestAgentListener:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def identity(self) -> AgentIdentity | None:
        with self._cond:
            return None if self._conn is None else self._conn.identity

    def wait_connected(self, timeout: float) -> AgentIdentity:
        identity = self._ready(time.monotonic() + timeout).identity
        assert identity is not None
        return identity

    def start_exec(
        self,
        argv: list[str] | tuple[str, ...],
        *,
        env: dict[str, str],
        cwd: str,
        uid: int,
        gid: int,
        stdin: bool = False,
        timeout: float = 10.0,
    ) -> GuestExecSession:
        """Start a command; return once the agent reports its pid."""
        argv = list(argv)
        if not 1 <= len(argv) <= 4096 or not all(map(_text, argv)):
            raise ValueError("argv must hold 1..4096 NUL-free strings")
        if not all(type(key) is str and _ENV_KEY.fullmatch(key) and _text(value) for key, value in env.items()):
            raise ValueError("env must map shell identifiers to NUL-free strings")
        if not _text(cwd) or not cwd.startswith("/") or type(stdin) is not bool:
            raise ValueError("cwd must be absolute and stdin a boolean")
        header = {"type": "exec", "id": 0, "argv": argv, "env": dict(env), "cwd": cwd, **_identity(uid, gid), "stdin": stdin}
        deadline = time.monotonic() + timeout
        conn = self._ready(deadline)
        session = GuestExecSession(self, conn, "exec", takes_input=stdin)
        conn.send(header, wake=self._wake, op=session)
        with session._cond:
            while session.pid is None and session._terminal is None and (remaining := deadline - time.monotonic()) > 0:
                session._cond.wait(remaining)
            terminal, started = session._terminal, session.pid is not None
        if started:
            return session
        if isinstance(terminal, GuestAgentError):
            raise terminal
        session._abandon()
        raise GuestAgentTimeout("timeout", "the agent did not start the command in time")

    def read_file(self, path: str, *, max_bytes: int, uid: int, gid: int, timeout: float = 60.0) -> bytes:
        if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_FILE_BYTES:
            raise ValueError(f"max_bytes must be 1..{MAX_FILE_BYTES}")
        deadline = time.monotonic() + timeout
        fields = {"path": _file_path(path), "max_bytes": max_bytes, **_identity(uid, gid)}
        op = self._file_op("read_file", fields, deadline, limit=max_bytes)
        op._wait_terminal(deadline)
        return bytes(op._data)

    def write_file(
        self,
        path: str,
        source: bytes | bytearray | memoryview | BinaryIO,
        *,
        uid: int,
        gid: int,
        size: int | None = None,
        timeout: float = 60.0,
    ) -> None:
        """Atomically replace path with exactly size bytes of source."""
        buffered = isinstance(source, (bytes, bytearray, memoryview))
        if buffered:
            length = memoryview(source).nbytes
            if size not in (None, length):
                raise ValueError("size disagrees with the source")
            size = length
        if type(size) is not int or not 0 <= size <= MAX_FILE_BYTES:
            raise ValueError(f"size must be 0..{MAX_FILE_BYTES}")
        deadline = time.monotonic() + timeout
        header = {"path": _file_path(path), "max_bytes": max(1, size), **_identity(uid, gid)}
        op = self._file_op("write_file", header, deadline)
        try:
            with op._input_lock:
                if buffered:
                    op._send_input(source, deadline)
                else:
                    remaining = size
                    while remaining:
                        chunk = source.read(min(remaining, MAX_PAYLOAD_BYTES))
                        if not chunk:
                            raise ValueError("source ended before size bytes")
                        op._send_input(chunk, deadline)
                        remaining -= len(chunk)
            op._close_input()
        except BaseException:
            op._abandon()
            raise
        op._wait_terminal(deadline)

    def stat(self, path: str, *, uid: int, gid: int, timeout: float = 10.0) -> GuestFileStat:
        deadline = time.monotonic() + timeout
        return self._file_op("stat", {"path": _file_path(path), **_identity(uid, gid)}, deadline)._wait_terminal(deadline)

    def _file_op(self, kind: str, fields: dict, deadline: float, *, limit: int = 0) -> _Op:
        conn = self._ready(deadline)
        op = _Op(self, conn, kind, takes_input=kind == "write_file", limit=limit)
        conn.send({"type": kind, "id": 0, **fields}, wake=self._wake, op=op)
        return op

    def _ready(self, deadline: float) -> _Connection:
        with self._cond:
            while self._conn is None or self._conn.identity is None:
                remaining = deadline - time.monotonic()
                if self._closing or remaining <= 0:
                    raise GuestAgentUnavailable("unavailable", f"no guest agent is connected at {self.socket_path}")
                self._cond.wait(remaining)
            return self._conn

    def _wake(self) -> None:
        if self._waker_w is None:
            return  # Never started.
        try:
            self._waker_w.send(b"\0")
        except OSError:
            pass  # A wake is already pending, or the listener is closed.

    def _run(self) -> None:
        selector = selectors.DefaultSelector()
        selector.register(self._listener, selectors.EVENT_READ)
        selector.register(self._waker_r, selectors.EVENT_READ)
        retry = None  # when to accept again after a failed accept
        try:
            while True:
                for key, events in selector.select(None if retry is None else max(0.0, retry - time.monotonic())):
                    if key.fileobj is self._listener:
                        retry = self._accept(selector)
                    elif key.fileobj is self._waker_r:
                        try:
                            while self._waker_r.recv(4096):
                                pass
                        except (BlockingIOError, InterruptedError):
                            pass
                        with self._cond:
                            if self._closing:
                                return
                            conn = self._conn
                        if conn is not None:
                            self._flush(conn, selector)
                    else:
                        if events & selectors.EVENT_WRITE:
                            self._flush(key.data, selector)
                        if events & selectors.EVENT_READ and key.data.closed is None:
                            self._read(key.data, selector)
                if retry is not None and time.monotonic() >= retry:
                    retry = None
                    selector.register(self._listener, selectors.EVENT_READ)
        except Exception:
            _LOG.exception("guest agent listener %s failed", self.socket_path)
            raise
        finally:
            with self._cond:
                conn = self._conn
            if conn is not None:
                self._drop(conn, selector, "the listener closed")
            selector.close()

    def _accept(self, selector: selectors.BaseSelector) -> float | None:  # when to retry a failure
        try:
            sock, _ = self._listener.accept()  # type: ignore[union-attr]
        except (BlockingIOError, InterruptedError):
            return None
        except OSError:  # out of descriptors, say: pause accepting rather than spin or die
            _LOG.warning("guest agent listener %s cannot accept", self.socket_path, exc_info=True)
            selector.unregister(self._listener)
            return time.monotonic() + _ACCEPT_RETRY_SECONDS
        sock.setblocking(False)
        with self._cond:
            previous = self._conn
        if previous is not None:
            self._drop(previous, selector, "a new agent connection superseded this one")
        conn = _Connection(sock, self.window)
        selector.register(sock, selectors.EVENT_READ, conn)
        with self._cond:
            self._conn = conn
        conn.send({"type": "hello", "version": PROTOCOL_VERSION, "window": self.window}, wake=self._wake)

    def _flush(self, conn: _Connection, selector: selectors.BaseSelector) -> None:
        with conn.lock:
            if conn.out and not conn.broken:
                try:
                    del conn.out[: conn.sock.send(conn.out)]
                except (BlockingIOError, InterruptedError):
                    pass
                except OSError:
                    conn.broken = True
            broken, pending = conn.broken, bool(conn.out)
        if broken:
            self._drop(conn, selector, "writing to the agent failed")
        elif conn.closed is None:
            selector.modify(conn.sock, selectors.EVENT_READ | (selectors.EVENT_WRITE if pending else 0), conn)

    def _read(self, conn: _Connection, selector: selectors.BaseSelector) -> None:
        if conn.end == len(conn.buffer):  # full: compact, or grow for a large frame
            if conn.start:
                conn.buffer[: conn.end - conn.start] = conn.buffer[conn.start : conn.end]
                conn.end, conn.start = conn.end - conn.start, 0
            else:
                conn.buffer.extend(bytes(len(conn.buffer)))
        try:
            with memoryview(conn.buffer) as view:
                count = conn.sock.recv_into(view[conn.end :])
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            self._drop(conn, selector, f"the agent connection failed: {exc}")
            return
        if count == 0:
            self._drop(conn, selector, "the agent closed the connection")
            return
        conn.end += count
        try:
            with memoryview(conn.buffer) as view:
                while conn.end - conn.start >= _PREFIX.size:
                    header_size, payload_size = _PREFIX.unpack_from(view, conn.start)
                    if not 1 <= header_size <= MAX_HEADER_BYTES or payload_size > MAX_PAYLOAD_BYTES:
                        raise _Violation("frame lengths are out of bounds")
                    body = conn.start + _PREFIX.size
                    if conn.end - body < header_size + payload_size:
                        break
                    header = json.loads(bytes(view[body : body + header_size]), parse_constant=_reject_constant)
                    payload = bytes(view[body + header_size : body + header_size + payload_size])
                    conn.start = body + header_size + payload_size
                    self._dispatch(conn, header, payload)
                    if conn.closed is not None:
                        return
        except (_Violation, ValueError, RecursionError) as exc:  # the peer is guest code
            self._drop(conn, selector, f"agent protocol violation: {exc}")
            return
        if conn.start == conn.end:
            conn.start = conn.end = 0

    def _dispatch(self, conn: _Connection, header: Any, payload: bytes) -> None:
        kind = header.get("type") if type(header) is dict else None
        if type(kind) is not str or header.keys() != _INBOUND_KEYS.get(kind):
            raise _Violation(f"unexpected header {str(header)[:200]}")
        if (kind == "output") != bool(payload):
            raise _Violation(f"{kind} has the wrong payload")
        if conn.identity is None or kind == "hello":
            if kind != "hello" or conn.identity is not None:
                raise _Violation("expected exactly one hello, first")
            _int(header, "version", PROTOCOL_VERSION, PROTOCOL_VERSION)
            identity = AgentIdentity(_str(header, "agent"), _int(header, "pid", 1), _str(header, "build"), _int(header, "abandoned"))
            with self._cond:
                conn.identity = identity
                self._cond.notify_all()
            return
        if kind == "ping":
            # Bytes already queued reach the agent after its ping and answer
            # it; a pong per ping would let a peer that never reads grow them.
            with conn.lock:
                answered = bool(conn.out)
            if not answered:
                try:
                    conn.send({"type": "pong"}, wake=self._wake)
                except GuestAgentError:
                    pass  # A failed write already queued the drop.
            return
        op_id = _int(header, "id", 1)
        with conn.lock:
            if op_id >= conn.next_id:
                raise _Violation(f"{kind} for op {op_id}, which was never issued")
            op = conn.ops.get(op_id)
        if op is None:
            return  # It raced the op's end, like an input_credit after exit.
        with op._cond:
            terminal = op._on_frame(kind, header, payload)
        if terminal:
            with conn.lock:
                conn.ops.pop(op_id, None)

    def _drop(self, conn: _Connection, selector: selectors.BaseSelector, reason: str) -> None:
        error = GuestAgentDisconnected("disconnected", reason)
        with conn.lock:
            if conn.closed is not None:
                return
            conn.closed = error
            ops = list(conn.ops.values())
            conn.ops.clear()
            conn.out.clear()
        selector.unregister(conn.sock)
        conn.sock.close()
        for op in ops:
            op._lost(error)
        with self._cond:
            if self._conn is conn:
                self._conn = None
            self._cond.notify_all()


def _reject_constant(name: str) -> None:
    raise _Violation(f"{name} is not JSON")
