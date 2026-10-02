"""The Warden-side listener against the real Go guest agent, on the host.

No gVisor: the agent runs as an ordinary process and dials the listener's
socket, as it will through --host-uds=open. Pause is SIGSTOP of the agent and
the command's group. A restore is modelled by breaking the connection while
the agent and its commands survive, which is what the agent sees after one.
"""
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
from tempfile import TemporaryDirectory, mkdtemp
import threading
import time
import unittest
from unittest import mock

from ucloud_sandboxes import guest_agent
from ucloud_sandboxes.guest_agent import (
    ExecExit,
    ExecOutput,
    GuestAgentDisconnected,
    GuestAgentError,
    GuestAgentListener,
    GuestAgentTimeout,
    GuestAgentUnavailable,
    GuestFileStat,
)

TEST_TIER = "contract"
AGENT_SOURCE = Path(__file__).resolve().parents[1] / "runtime/managed_process"
UID, GID = os.getuid(), os.getgid()


def build_agent(directory: Path) -> Path:
    go = shutil.which("go")
    if go is None:
        raise unittest.SkipTest("Go toolchain is unavailable")
    binary = directory / "ucloud-sandbox-init"
    build_env = {key: value for key, value in os.environ.items() if key not in {"GOARCH", "GOFLAGS", "GOOS"}}
    build_env.update({"CGO_ENABLED": "0", "GOCACHE": str(directory / "go-build-cache"), "GOTOOLCHAIN": "local"})
    build = subprocess.run(
        [go, "build", "-trimpath", "-o", str(binary), "."],
        cwd=AGENT_SOURCE, env=build_env, capture_output=True, timeout=300, check=False,
    )
    if build.returncode != 0:
        raise AssertionError(build.stderr.decode(errors="replace"))
    return binary


def gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.01)
    return False


@unittest.skipUnless(sys.platform.startswith("linux"), "the guest agent is Linux-only")
class GuestAgentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._build_dir = TemporaryDirectory()
        cls.binary = build_agent(Path(cls._build_dir.name))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._build_dir.cleanup()

    def setUp(self) -> None:
        # Unix socket paths are limited to 108 bytes.
        self.dir = Path(mkdtemp(prefix="ga-", dir="/tmp"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.socket = self.dir / "agent.sock"

    def listener(self, **kwargs) -> GuestAgentListener:
        listener = GuestAgentListener(self.socket, **kwargs).start()
        self.addCleanup(listener.close)
        return listener

    def agent(self, *flags: str) -> subprocess.Popen:
        process = subprocess.Popen(
            [str(self.binary), "agent", "--connect", str(self.socket), "--backoff-min=5ms", "--backoff-max=50ms", *flags],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

        def stop() -> None:
            process.send_signal(signal.SIGCONT)
            process.terminate()
            process.wait(timeout=10)

        self.addCleanup(stop)
        return process

    def connected(self, *flags: str, **kwargs) -> tuple[GuestAgentListener, subprocess.Popen]:
        listener = self.listener(**kwargs)
        process = self.agent(*flags)
        listener.wait_connected(10)
        return listener, process

    def run_exec(self, listener, argv, **kwargs) -> tuple[ExecExit, bytes, bytes]:
        session = listener.start_exec(argv, env=kwargs.pop("env", {}), cwd=kwargs.pop("cwd", "/"), uid=UID, gid=GID, **kwargs)
        return session.communicate(timeout=30)

    def test_exec_is_binary_safe_and_reports_exit_status(self) -> None:
        listener, _ = self.connected()
        identity = listener.identity
        self.assertEqual((identity.build, identity.abandoned), ("guest-agent-v1", 0))
        every_byte = bytes(range(256)) * 64
        listener.write_file(str(self.dir / "bytes"), every_byte, uid=UID, gid=GID)
        exit_status, stdout, stderr = self.run_exec(
            listener,
            ["/bin/sh", "-c", 'cat bytes; printf "%s:%s" "$EXTRA" "$PWD" >&2; exit 3'],
            env={"EXTRA": "ü"}, cwd=str(self.dir),
        )
        self.assertEqual(exit_status, ExecExit(3, None, True))
        self.assertEqual(stdout, every_byte)
        self.assertEqual(stderr, f"ü:{self.dir}".encode())
        with self.assertRaises(GuestAgentError) as raised:
            listener.start_exec(["/nonexistent"], env={}, cwd="/", uid=UID, gid=GID)
        self.assertEqual(raised.exception.code, "spawn_failed")
        for argv, env, cwd in (
            (["x\0"], {}, "/"), (["true"], {"BAD-KEY": "x"}, "/"), (["true"], {}, "relative"),
            (["x" * (1 << 20)], {}, "/"),  # over the agent's header limit: refused before sending
        ):
            with self.assertRaises(ValueError):
                listener.start_exec(argv, env=env, cwd=cwd, uid=UID, gid=GID)
        self.assertFalse(listener._conn.ops)
        # The agent quotes argv[0] in the error; Go escapes '<' to six bytes.
        with self.assertRaises(GuestAgentError) as raised:
            listener.start_exec(["<" * 200_000], env={}, cwd="/", uid=UID, gid=GID)
        self.assertEqual(raised.exception.code, "spawn_failed")
        self.assertIs(listener.identity, identity)  # the reply was a valid frame

    def test_large_streams_meet_the_throughput_target(self) -> None:
        listener, _ = self.connected()
        data, copies = os.urandom(16 << 20), 8
        path = str(self.dir / "random")
        started = time.monotonic()
        listener.write_file(path, data, uid=UID, gid=GID)
        write_rate = len(data) / (time.monotonic() - started)
        session = listener.start_exec(["cat", *[path] * copies], env={}, cwd="/", uid=UID, gid=GID)
        chunks = []
        started = time.monotonic()
        for event in session:
            if isinstance(event, ExecOutput):
                chunks.append(event.data)
        stream_rate = copies * len(data) / (time.monotonic() - started)
        self.assertEqual(event, ExecExit(0, None, True))
        expected, received = hashlib.sha256(), hashlib.sha256()
        for _ in range(copies):
            expected.update(data)
        for chunk in chunks:
            received.update(chunk)
        self.assertEqual(received.hexdigest(), expected.hexdigest())
        self.assertEqual(listener.read_file(path, max_bytes=len(data), uid=UID, gid=GID), data)
        print(f"\nguest agent on the host: stdout {stream_rate / 1e6:.0f} MB/s, file write {write_rate / 1e6:.0f} MB/s")
        # The plan's per-stream gate is 100 MB/s; the host measures several
        # times that, so this floor tolerates a loaded CI runner.
        self.assertGreater(stream_rate, 100e6)

    def test_stdin_streams_with_credit_and_closes(self) -> None:
        listener, _ = self.connected(window=64 << 10)
        session = listener.start_exec(["cat"], env={}, cwd="/", uid=UID, gid=GID, stdin=True)
        payload = os.urandom(3 << 20)  # many windows: the writer must wait for credit

        def write() -> None:
            for offset in range(0, len(payload), 100_000):
                session.write_stdin(payload[offset : offset + 100_000], timeout=30)
            session.close_stdin()

        writer = threading.Thread(target=write)
        writer.start()
        exit_status, stdout, _ = session.communicate(timeout=30)
        writer.join()
        self.assertEqual((exit_status, stdout), (ExecExit(0, None, True), payload))
        quiet = listener.start_exec(["true"], env={}, cwd="/", uid=UID, gid=GID)
        with self.assertRaises(GuestAgentError):
            quiet.write_stdin(b"x")
        quiet.communicate(timeout=10)

    def test_signals_reach_the_process_group(self) -> None:
        listener, _ = self.connected()
        session = listener.start_exec(
            ["/bin/sh", "-c", 'trap "echo term; exit 7" TERM; echo ready; while :; do sleep 0.05; done'],
            env={}, cwd="/", uid=UID, gid=GID,
        )
        self.assertEqual(session.next_event(10), ExecOutput("stdout", b"ready\n"))
        session.signal(signal.SIGTERM)
        exit_status, stdout, _ = session.communicate(timeout=10)
        self.assertEqual((exit_status, stdout), (ExecExit(7, None, True), b"term\n"))
        sleeper = listener.start_exec(["sleep", "30"], env={}, cwd="/", uid=UID, gid=GID)
        sleeper.signal(signal.SIGKILL)
        exit_status = sleeper.communicate(timeout=10)[0]
        self.assertEqual((exit_status, exit_status.status), (ExecExit(None, 9, True), 137))
        sleeper.signal(signal.SIGTERM)  # after exit: a no-op
        with self.assertRaises(ValueError):
            sleeper.signal(65)

    def test_file_round_trips_and_errors(self) -> None:
        listener, _ = self.connected()
        target = str(self.dir / "nested" / "file")
        listener.write_file(target, io.BytesIO(b"from a stream\x00"), size=14, uid=UID, gid=GID)
        self.assertEqual(listener.read_file(target, max_bytes=14, uid=UID, gid=GID), b"from a stream\x00")
        stat = listener.stat(target, uid=UID, gid=GID)
        self.assertEqual((stat.type, stat.size, stat.mode, stat.uid), ("file", 14, 0o600, UID))
        self.assertEqual(listener.stat(str(self.dir), uid=UID, gid=GID).type, "directory")
        listener.write_file(target, b"", uid=UID, gid=GID)
        self.assertEqual(listener.read_file(target, max_bytes=1, uid=UID, gid=GID), b"")
        for call, code in (
            (lambda: listener.read_file(str(self.dir / "missing"), max_bytes=8, uid=UID, gid=GID), "not_found"),
            (lambda: listener.read_file(str(self.dir), max_bytes=8, uid=UID, gid=GID), "not_regular"),
            (lambda: listener.write_file(str(self.dir), b"x", uid=UID, gid=GID), "not_regular"),
            (lambda: listener.stat(str(self.dir / "missing"), uid=UID, gid=GID), "not_found"),
        ):
            with self.assertRaises(GuestAgentError) as raised:
                call()
            self.assertEqual(raised.exception.code, code)
        listener.write_file(target, b"0123456789", uid=UID, gid=GID)
        with self.assertRaises(GuestAgentError) as raised:
            listener.read_file(target, max_bytes=4, uid=UID, gid=GID)
        self.assertEqual(raised.exception.code, "too_large")
        with self.assertRaises(ValueError):
            listener.write_file(target, io.BytesIO(b"short"), size=10, uid=UID, gid=GID)
        self.assertEqual(listener.read_file(target, max_bytes=10, uid=UID, gid=GID), b"0123456789")
        for path in ("relative", "/a/../b", "/tab\there"):
            with self.assertRaises(ValueError):
                listener.read_file(path, max_bytes=1, uid=UID, gid=GID)
        if os.geteuid() != 0:
            with self.assertRaises(GuestAgentError) as raised:
                listener.stat(target, uid=UID + 1, gid=GID)
            self.assertEqual(raised.exception.code, "credentials")

    def test_a_slow_consumer_does_not_block_other_sessions(self) -> None:
        listener, _ = self.connected(window=64 << 10)
        flood = listener.start_exec(["head", "-c", str(64 << 20), "/dev/zero"], env={}, cwd="/", uid=UID, gid=GID)
        started = time.monotonic()
        self.assertEqual(self.run_exec(listener, ["echo", "hi"])[:2], (ExecExit(0, None, True), b"hi\n"))
        self.assertLess(time.monotonic() - started, 5)
        flood.close()  # kills it and credits whatever arrives until exit
        deadline = time.monotonic() + 10
        while flood.id in listener._conn.ops:  # its exit needs that credit
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertEqual(self.run_exec(listener, ["true"])[0], ExecExit(0, None, True))
        self.assertTrue(gone(flood.pid))

    def test_pause_and_resume_keep_sessions(self) -> None:
        listener, agent = self.connected("--ping-interval=100ms")
        identity = listener.identity
        session = listener.start_exec(
            ["/bin/sh", "-c", "echo before; sleep 0.3; echo after"], env={}, cwd="/", uid=UID, gid=GID,
        )
        self.assertEqual(session.next_event(10), ExecOutput("stdout", b"before\n"))
        # runsc pause freezes every guest task, the agent included.
        agent.send_signal(signal.SIGSTOP)
        os.killpg(session.pid, signal.SIGSTOP)
        time.sleep(0.6)  # longer than two liveness intervals
        self.assertIsNone(session.next_event(0))
        os.killpg(session.pid, signal.SIGCONT)
        agent.send_signal(signal.SIGCONT)
        exit_status, stdout, _ = session.communicate(timeout=10)
        self.assertEqual((exit_status, stdout), (ExecExit(0, None, True), b"after\n"))
        time.sleep(0.4)  # the restarted probe was answered
        self.assertIs(listener.identity, identity)

    def test_a_reconnect_fails_in_flight_ops_and_kills_them(self) -> None:
        listener, _ = self.connected()
        before = listener.identity
        session = listener.start_exec(["sleep", "30"], env={}, cwd="/", uid=UID, gid=GID)
        # After a restore the agent finds its connection dead and redials.
        listener._conn.sock.shutdown(socket.SHUT_RDWR)
        with self.assertRaises(GuestAgentDisconnected):
            session.communicate(timeout=10)
        self.assertTrue(gone(session.pid))
        deadline = time.monotonic() + 10
        while (after := listener.identity) is None or after is before:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertEqual((after.agent, after.abandoned), (before.agent, 1))
        self.assertEqual(self.run_exec(listener, ["true"])[0], ExecExit(0, None, True))

    def test_a_listener_restart_fails_ops_and_the_agent_redials(self) -> None:
        first = GuestAgentListener(self.socket).start()
        agent = self.agent()
        before = first.wait_connected(10)
        session = first.start_exec(["sleep", "30"], env={}, cwd="/", uid=UID, gid=GID)
        first.close()
        self.assertFalse(self.socket.exists())
        with self.assertRaises(GuestAgentDisconnected):
            session.next_event(10)
        self.assertTrue(gone(session.pid))
        with self.assertRaises(GuestAgentUnavailable):
            first.start_exec(["true"], env={}, cwd="/", uid=UID, gid=GID, timeout=0.1)
        second = self.listener()
        after = second.wait_connected(10)
        self.assertEqual((after.agent, after.pid, after.abandoned), (before.agent, agent.pid, 1))
        self.assertEqual(self.run_exec(second, ["echo", "back"])[1], b"back\n")

    def test_deadlines_kill_the_op(self) -> None:
        listener, _ = self.connected()
        session = listener.start_exec(["sleep", "30"], env={}, cwd="/", uid=UID, gid=GID)
        with self.assertRaises(GuestAgentTimeout):
            session.communicate(timeout=0.1)
        self.assertTrue(gone(session.pid))
        self.assertEqual(self.run_exec(listener, ["true"])[0], ExecExit(0, None, True))


@unittest.skipUnless(sys.platform.startswith("linux"), "Unix socket ownership checks are Linux-tested")
class GuestAgentListenerTests(unittest.TestCase):
    """The node side alone, against a scripted peer."""

    def setUp(self) -> None:
        self.dir = Path(mkdtemp(prefix="ga-", dir="/tmp"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.socket = self.dir / "agent.sock"

    def peer(self, listener: GuestAgentListener) -> socket.socket:
        peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        peer.settimeout(5)
        peer.connect(str(listener.socket_path))
        self.addCleanup(peer.close)
        self.assertEqual(self.recv(peer), {"type": "hello", "version": 1, "window": listener.window})
        return peer

    @staticmethod
    def send(peer: socket.socket, header: dict, payload: bytes = b"") -> None:
        encoded = json.dumps(header).encode()
        peer.sendall(struct.pack(">II", len(encoded), len(payload)) + encoded + payload)

    @staticmethod
    def recv(peer: socket.socket) -> dict | None:
        prefix = b""
        while len(prefix) < 8:
            chunk = peer.recv(8 - len(prefix))
            if not chunk:
                return None
            prefix += chunk
        header_size, payload_size = struct.unpack(">II", prefix)
        body = b""
        while len(body) < header_size + payload_size:
            body += peer.recv(header_size + payload_size - len(body))
        return json.loads(body[:header_size])

    def hello(self, peer: socket.socket, listener: GuestAgentListener) -> None:
        self.send(peer, {"type": "hello", "version": 1, "agent": "a" * 32, "pid": 1, "build": "test", "abandoned": 0})
        listener.wait_connected(5)

    def test_socket_is_owner_only_and_replaces_stale_ones(self) -> None:
        self.socket.write_text("stale")
        with GuestAgentListener(self.socket):
            self.assertEqual(self.socket.stat().st_mode & 0o777, 0o600)
            self.assertTrue(self.socket.is_socket())
            successor = self.dir / "successor.sock"
            with GuestAgentListener(successor):
                os.replace(successor, self.socket)  # a newer owner took the name
        self.assertTrue(self.socket.is_socket())
        self.assertEqual([path.name for path in self.dir.iterdir()], ["agent.sock"])
        shared = self.dir / "shared"
        shared.mkdir(mode=0o777)
        shared.chmod(0o777)
        with self.assertRaises(ValueError):
            GuestAgentListener(shared / "agent.sock").start()
        with self.assertRaises(ValueError):
            GuestAgentListener(self.socket, window=1)

    def test_violations_drop_the_connection_and_fail_its_ops(self) -> None:
        listener = GuestAgentListener(self.socket, window=64 << 10).start()
        self.addCleanup(listener.close)
        for frames in (
            [{"type": "hello", "version": 2, "agent": "a", "pid": 1, "build": "b", "abandoned": 0}],
            [{"type": "ping"}],
            [{"type": "hello", "version": 1, "agent": "a", "pid": True, "build": "b", "abandoned": 0}],
        ):
            peer = self.peer(listener)
            for frame in frames:
                self.send(peer, frame)
            self.assertIsNone(self.recv(peer))
        exit_frame = {"type": "exit", "id": 1, "exit_code": 0, "signal": None, "stdout_bytes": 0, "stderr_bytes": 0,
                      "output_complete": True}
        violations = (
            ({"type": "output", "id": 1, "stream": "stdout"}, b"", True),  # output without a payload
            ({"type": "started", "id": 2, "pid": 7}, b"", True),  # an op never issued
            ({"type": "started", "id": 1, "pid": 7, "extra": 1}, b"", True),
            ({**exit_frame, "signal": 9}, b"", True),
            ({**exit_frame, "stdout_bytes": 1}, b"", True),  # disagrees with the output received
            ({"type": "output", "id": 1, "stream": "stdout"}, b"x" * ((64 << 10) + 1), True),  # beyond credit
            ({"type": "output", "id": 1, "stream": "stdout"}, b"x", False),  # before started
        )
        for header, payload, started in violations:
            with self.subTest(header=header):
                peer = self.peer(listener)
                self.hello(peer, listener)
                result = {}
                thread = threading.Thread(target=lambda: result.update(error=self.start_and_fail(listener)))
                thread.start()
                self.assertEqual(self.recv(peer)["type"], "exec")
                if started:
                    self.send(peer, {"type": "started", "id": 1, "pid": 7})
                self.send(peer, header, payload)
                thread.join(10)
                self.assertIsInstance(result["error"], GuestAgentDisconnected)
                self.assertIsNone(self.recv(peer))

    def test_a_hostile_guest_peer_only_loses_its_connection(self) -> None:
        # Guest root can dial the socket too, so the listener trusts no peer.
        listener = GuestAgentListener(self.socket).start()
        self.addCleanup(listener.close)
        peer = self.peer(listener)
        nested = b"[" * 100_000 + b"]" * 100_000
        peer.sendall(struct.pack(">II", len(nested), 0) + nested)
        self.assertIsNone(self.recv(peer))
        peer = self.peer(listener)
        self.hello(peer, listener)
        result = {}

        def read() -> None:
            try:
                listener.read_file("/x", max_bytes=4, uid=0, gid=0, timeout=10)
            except GuestAgentError as exc:
                result["error"] = exc

        thread = threading.Thread(target=read)
        thread.start()
        self.assertEqual(self.recv(peer)["type"], "read_file")
        self.send(peer, {"type": "output", "id": 1, "stream": "stdout"}, b"12345")
        thread.join(10)
        self.assertIsInstance(result["error"], GuestAgentDisconnected)
        self.assertIsNone(self.recv(peer))
        # A list where a stat type belongs must not reach a set's hash.
        peer = self.peer(listener)
        self.hello(peer, listener)
        thread = threading.Thread(target=lambda: result.update(error=self.call(listener.stat, "/x", uid=0, gid=0)))
        thread.start()
        self.assertEqual(self.recv(peer)["type"], "stat")
        stat = {"type": [], "size": 0, "mode": 0, "mtime_ns": 0, "uid": 0, "gid": 0}
        self.send(peer, {"type": "done", "id": 1, "stat": stat})
        thread.join(10)
        self.assertIsInstance(result["error"], GuestAgentDisconnected)
        self.assertIsNone(self.recv(peer))
        self.hello(self.peer(listener), listener)  # the listener thread survived
        self.assertTrue(listener._thread.is_alive())

    def test_a_peer_that_never_reads_cannot_grow_queued_pongs(self) -> None:
        listener = GuestAgentListener(self.socket).start()
        self.addCleanup(listener.close)
        peer = self.peer(listener)
        self.hello(peer, listener)
        result = {}
        thread = threading.Thread(target=lambda: result.update(stat=self.call(listener.stat, "/x", uid=0, gid=0)))
        thread.start()
        self.assertEqual(self.recv(peer)["type"], "stat")
        ping = json.dumps({"type": "ping"}).encode()
        for _ in range(10):  # about 4 MB of pongs, far beyond the socket buffer
            peer.sendall((struct.pack(">II", len(ping), 0) + ping) * 20_000)
        stat = {"type": "file", "size": 0, "mode": 0, "mtime_ns": 0, "uid": 0, "gid": 0}
        self.send(peer, {"type": "done", "id": 1, "stat": stat})
        thread.join(10)  # frames apply in order: every ping was handled
        self.assertEqual(result["stat"], GuestFileStat(**stat))
        self.assertLess(len(listener._conn.out), 1024)

    def test_a_failed_accept_pauses_accepting_and_recovers(self) -> None:
        real_accept, failures = socket.socket.accept, [OSError(errno.EMFILE, "Too many open files")]

        def accept(sock: socket.socket):
            if failures:
                raise failures.pop()
            return real_accept(sock)

        with mock.patch.object(socket.socket, "accept", accept), mock.patch.object(guest_agent, "_ACCEPT_RETRY_SECONDS", 0.05):
            listener = GuestAgentListener(self.socket).start()
            self.addCleanup(listener.close)
            with self.assertLogs(guest_agent.__name__, "WARNING"):
                self.hello(self.peer(listener), listener)
        self.assertEqual(failures, [])

    @staticmethod
    def call(function, *args, **kwargs):
        try:
            return function(*args, **kwargs)
        except GuestAgentError as exc:
            return exc

    @staticmethod
    def start_and_fail(listener: GuestAgentListener) -> Exception | None:
        try:
            session = listener.start_exec(["true"], env={}, cwd="/", uid=0, gid=0)
            session.communicate(timeout=10)
        except GuestAgentError as exc:
            return exc
        return None

    def test_pings_are_answered_and_late_frames_ignored(self) -> None:
        listener = GuestAgentListener(self.socket, window=64 << 10).start()
        self.addCleanup(listener.close)
        peer = self.peer(listener)
        self.hello(peer, listener)
        self.send(peer, {"type": "ping"})
        self.assertEqual(self.recv(peer), {"type": "pong"})
        result = {}
        thread = threading.Thread(target=lambda: result.update(
            stat=listener.stat("/x", uid=0, gid=0, timeout=10)))
        thread.start()
        self.assertEqual(self.recv(peer), {"type": "stat", "id": 1, "path": "/x", "uid": 0, "gid": 0})
        stat = {"type": "file", "size": 1, "mode": 0o644, "mtime_ns": -1, "uid": 0, "gid": 0}
        self.send(peer, {"type": "done", "id": 1, "stat": stat})
        thread.join(10)
        self.assertEqual(result["stat"], GuestFileStat("file", 1, 0o644, -1, 0, 0))
        self.send(peer, {"type": "input_credit", "id": 1, "offset": 0})  # raced the op's end
        self.send(peer, {"type": "ping"})
        self.assertEqual(self.recv(peer), {"type": "pong"})

    def test_unavailable_without_an_agent(self) -> None:
        with GuestAgentListener(self.socket) as listener:
            started = time.monotonic()
            with self.assertRaises(GuestAgentUnavailable):
                listener.start_exec(["true"], env={}, cwd="/", uid=0, gid=0, timeout=0.2)
            self.assertLess(time.monotonic() - started, 2)
            with self.assertRaises(GuestAgentUnavailable):
                listener.wait_connected(0.05)
            result = {}
            waiter = threading.Thread(target=lambda: result.update(error=self.call(listener.wait_connected, 30)))
            waiter.start()
            time.sleep(0.1)
        waiter.join(5)  # close fails a caller waiting for a connection at once
        self.assertIsInstance(result["error"], GuestAgentUnavailable)


if __name__ == "__main__":
    unittest.main()
