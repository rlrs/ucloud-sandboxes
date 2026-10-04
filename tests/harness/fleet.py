"""A local fleet: real gateway and direct node agents over loopback HTTP.

Each node is assembled the way ``build_direct_runtime_service`` and
``cmd_serve_direct_node_agent`` assemble one, for ``network=none`` and the
legacy (non-split) storage layout. Production code runs everywhere except at
these boundaries:

- ``runsc`` is ``fake_runsc.py`` behind a per-node wrapper, and the Warden's
  ``proc_root`` is a harness directory exposing the sentry's real /proc
  stat/cmdline/exe plus a faked cgroup line.
- ``mount``/``umount``/``mountpoint`` for the sandbox overlay are
  ``fake_mount.py`` behind per-node wrappers.
- The storage-native service is the real service and unix-socket server with
  ``storage.FakeBlockBackend`` and ``storage.FakeStorageHost``.
- Images come from ``images.ImageCatalog`` through ``images.LocalRootfsStore``;
  the node's image "pull" is ``DockerImageRuntime(dry_run=True)``.
- Host metrics are a sample the test sets (``FleetNode.sample_metrics``),
  and heartbeats are relayed on demand by ``LocalFleet.heartbeat()``, as the
  production heartbeat timer would.

The gateway uses distinct real tokens and, when ``UCLOUD_TEST_POSTGRES_DSN``
is set and requested, PostgreSQL routing through the production descriptor.
Node agents run in the test process, or with ``node_processes=True`` each in
its own process (``node_process.py``) so a test can SIGKILL it mid-operation.
"""

from __future__ import annotations

import atexit
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import timedelta
import hashlib
from http.client import HTTPConnection
import inspect
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, Iterator
import unittest
from urllib.parse import quote, urlparse
import uuid
import weakref

import ucloud_sandboxes
from ucloud_sandboxes import routing
from ucloud_sandboxes.agent import fetch_node_agent_heartbeat, post_heartbeat_with_headers
from ucloud_sandboxes.control_plane import build_server
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.exec_session_routes import EXEC_SESSION_PREFIX_HEADER
from ucloud_sandboxes.models import NodeRuntimeMetrics
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeClient,
    StorageNativeNodeConfig,
    StorageNativeNodeServer,
    StorageNativeNodeService,
)

from . import faults
from .assembly import NodeAgentConfig, assemble_node_agent, fixed_metrics
from .fake_runsc import kill_exec_groups
from .images import ImageCatalog, LocalRootfsStore
from .pidfd import SyscallPidfdFencer, native_pidfd_available
from .storage import FakeBlockBackend, FakeStorageHost

HARNESS = Path(__file__).resolve().parent
PYTHON = Path(os.path.realpath(sys.executable))
DEFAULT_IMAGE = "harness/base:1"
POSTGRES_DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN", "")
# Scenarios send heartbeats explicitly; the periodic one never fires in a test.
HEARTBEAT_INTERVAL_SECONDS = 3600


def root_owned_init_binary() -> Path:
    """``DirectOciConfigBuilder`` requires a root-owned, immutable init.

    The fake runtime never runs it; any such host executable will do.
    """
    for candidate in (shutil.which("true"), "/usr/bin/true", "/bin/true"):
        if not candidate:
            continue
        path = Path(os.path.realpath(candidate))
        try:
            info = path.stat()
        except OSError:
            continue
        if (
            stat.S_ISREG(info.st_mode)
            and info.st_uid == 0
            and info.st_mode & 0o111
            and not info.st_mode & 0o022
        ):
            return path
    raise unittest.SkipTest("local fleet requires a root-owned init executable")


@dataclass(frozen=True)
class FleetTokens:
    gateway: str
    sandbox_api: str
    heartbeat: str
    node_control: str

    @classmethod
    def generate(cls) -> "FleetTokens":
        return cls(*(f"{label}-{secrets.token_hex(12)}" for label in (
            "gateway", "sandbox", "heartbeat", "node")))


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self):
        return json.loads(self.body) if self.body else None


@dataclass(frozen=True)
class ExecResult:
    session_id: str
    exit_code: int | None
    stdout: str
    stderr: str
    events: tuple[dict, ...] = field(repr=False)


def _scratch_root() -> str | None:
    """tmpfs when usable: every registry and journal commit fsyncs, and on a
    shared disk those fsyncs dominate a scenario's run time. The fakes are
    executables under the fleet root, so the mount must allow exec."""
    shm = Path("/dev/shm")
    try:
        usable = os.access(shm, os.W_OK | os.X_OK) and not os.statvfs(shm).f_flag & os.ST_NOEXEC
    except OSError:
        usable = False
    return str(shm) if usable else None


def _process_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    fields = raw[raw.rfind(")") + 2:].split()
    return None if fields[0] in {"Z", "X"} else int(fields[19])


def process_alive(pid: int) -> bool:
    """True while ``pid`` runs and is not a zombie."""
    return _process_ticks(pid) is not None


def _write_wrapper(path: Path, module: str, call: str) -> None:
    path.write_text(
        f"#!{PYTHON} -S\n"
        "import sys\n"
        f"sys.path.insert(0, {str(HARNESS)!r})\n"
        f"import {module}\n"
        f"raise SystemExit({call})\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _request(origin: str, method: str, path: str, credential: str, *, payload: object | None = None,
             body: bytes | None = None, headers: dict[str, str] | None = None,
             timeout: float = 30.0) -> Response:
    headers = {**(headers or {}), "Authorization": f"Bearer {credential}"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    parsed = urlparse(origin)
    connection = HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        return Response(response.status, dict(response.getheaders()), response.read())
    finally:
        connection.close()


_ZYGOTE: subprocess.Popen | None = None
_ZYGOTE_GUARD = threading.Lock()


@atexit.register
def _stop_zygote() -> None:
    if _ZYGOTE is not None:
        _ZYGOTE.stdin.close()  # It exits at end of input; its agents die with it.
        try:
            _ZYGOTE.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _ZYGOTE.kill()
            _ZYGOTE.wait()
        _ZYGOTE.stdout.close()


def _fork_node_agent(request: dict) -> int:
    """Fork a node-agent process from the shared zygote (``node_process.py``)."""
    global _ZYGOTE
    with _ZYGOTE_GUARD:
        if _ZYGOTE is None or _ZYGOTE.poll() is not None:
            environment = dict(os.environ)
            environment["PYTHONPATH"] = os.pathsep.join(
                item for item in (str(Path(ucloud_sandboxes.__file__).parent.parent),
                                  environment.get("PYTHONPATH", "")) if item)
            _ZYGOTE = subprocess.Popen(
                # The venv interpreter: PYTHON is its realpath, without site-packages.
                [sys.executable, str(HARNESS / "node_process.py")],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=environment, text=True,
            )
        _ZYGOTE.stdin.write(json.dumps(request) + "\n")
        _ZYGOTE.stdin.flush()
        answer = _ZYGOTE.stdout.readline()
    if not answer:
        raise AssertionError("the node-agent zygote exited")
    return int(answer)


def _serve(server) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    return thread


class FleetNode:
    """One worker VM: storage daemon, Warden state and a node-agent process."""

    def __init__(self, fleet: "LocalFleet", index: int) -> None:
        self.fleet = fleet
        self.index = index
        self.root = fleet.root / f"node-{index}"
        self.job_id = f"harness-job-{index}"
        self.node_id = f"harness-node-{index}"
        self.bin = self.root / "bin"
        self.proc_root = self.root / "proc"
        self.state_root = self.root / "state"
        self.volumes = self.root / "volumes"
        self.runtime_root = self.state_root / "runsc"
        self.faults = self.bin / "faults"
        self.boot_id = uuid.uuid4()
        self._sample_metrics: Callable[[], NodeRuntimeMetrics | None] = fixed_metrics
        self._requests: list[tuple[str, str]] = []
        self._honor_exec_session_prefix = True
        self.port = 0
        self.url: str | None = None
        self.server = None
        # (pid, start ticks) of a node-agent process.
        self.process: tuple[int, int | None] | None = None
        self.service: DirectSandboxService | None = None
        self._thread: threading.Thread | None = None
        self._storage_server: StorageNativeNodeServer | None = None
        self._storage_thread: threading.Thread | None = None
        self._socket_dir: Path | None = None
        self.storage_backend: FakeBlockBackend | None = None
        self.storage_host: FakeStorageHost | None = None
        # Closed connections drop out once the server releases them.
        self._connections: weakref.WeakSet[socket.socket] = weakref.WeakSet()

    # -- in-process observation and knobs ------------------------------------
    # A node-agent process has none of these; touching one there is an error,
    # never a silently vacuous assertion.

    def _in_process(self, knob: str) -> None:
        if self.fleet.node_processes:
            raise RuntimeError(f"node.{knob} needs an in-process node agent")

    @property
    def sample_metrics(self) -> Callable[[], NodeRuntimeMetrics | None]:
        """The host sample behind heartbeats and live admission, read uncached."""
        self._in_process("sample_metrics")
        return self._sample_metrics

    @sample_metrics.setter
    def sample_metrics(self, provider: Callable[[], NodeRuntimeMetrics | None]) -> None:
        self._in_process("sample_metrics")
        self._sample_metrics = provider

    @property
    def requests(self) -> list[tuple[str, str]]:
        """(method, path) of every request this node's agent parsed."""
        self._in_process("requests")
        return self._requests

    @property
    def honor_exec_session_prefix(self) -> bool:
        """False models a worker that predates signed exec session names."""
        self._in_process("honor_exec_session_prefix")
        return self._honor_exec_session_prefix

    @honor_exec_session_prefix.setter
    def honor_exec_session_prefix(self, honored: bool) -> None:
        self._in_process("honor_exec_session_prefix")
        self._honor_exec_session_prefix = honored

    # -- lifecycle -----------------------------------------------------------

    def provision(self) -> None:
        for path in (self.bin, self.faults, self.state_root, self.volumes, self.root / "mounts"):
            path.mkdir(mode=0o700, parents=True)
        (self.proc_root / "sys/kernel/random").mkdir(mode=0o700, parents=True)
        self.set_boot_id(self.boot_id)
        runsc_config = self.bin / "fake-runsc.json"
        runsc_config.write_text(json.dumps({
            "proc_root": str(self.proc_root),
            "python": str(PYTHON),
            "python_home": sys.base_prefix,
            "faults": str(self.faults),
        }), encoding="utf-8")
        _write_wrapper(self.bin / "runsc", "fake_runsc",
                       f"fake_runsc.main(sys.argv, {str(runsc_config)!r})")
        # The Warden trusts runsc or its packaged sentry as a sandbox's exe.
        # The fake sentry is the interpreter, so the companion names it.
        (self.bin / "gvisor-bin").mkdir(mode=0o700)
        os.symlink(PYTHON, self.bin / "gvisor-bin" / "gvisor_sentry")
        mount_config = self.bin / "fake-mount.json"
        mount_config.write_text(json.dumps({
            "table": str(self.root / "mounts"), "faults": str(self.faults),
        }), encoding="utf-8")
        for kind in ("mount", "umount", "mountpoint"):
            _write_wrapper(self.bin / kind, "fake_mount",
                           f"fake_mount.main({kind!r}, sys.argv, {str(mount_config)!r})")
        self._start_storage()

    def _socket_path(self) -> Path:
        path = self.root / "storage" / "storage.sock"
        if len(str(path)) <= 100:
            return path
        # AF_UNIX paths are bounded; keep long temporary roots usable.
        self._socket_dir = Path(tempfile.mkdtemp(prefix="ucloud-local-fleet-"))
        return self._socket_dir / "storage.sock"

    def _start_storage(self) -> None:
        storage_root = self.root / "storage"
        storage_root.mkdir(mode=0o700, exist_ok=True)
        global_config = storage_root / "global.json"
        global_config.write_text("{}\n", encoding="ascii")
        runtime_root = storage_root / "runtime"
        backend = self.storage_backend = FakeBlockBackend(runtime_root)
        service = StorageNativeNodeService(
            StorageNativeNodeConfig(
                journal_path=storage_root / "journal.sqlite",
                runtime_root=runtime_root,
                mount_root=self.volumes,
                hard_capacity_bytes=1 << 40,
            ),
            backend=backend,
            global_config_path=global_config,
            host=(host := FakeStorageHost(backend)),
        )
        self.storage_host = host
        self.storage_socket = self._socket_path()
        self._storage_server = StorageNativeNodeServer(
            self.storage_socket, service, require_root_peer=False
        )
        self._storage_thread = threading.Thread(
            target=self._storage_server.serve_forever, daemon=True
        )
        self._storage_thread.start()
        StorageNativeNodeClient(self.storage_socket, timeout_seconds=5).wait_ready(
            timeout_seconds=10
        )
        unix_server = self._storage_server._server
        report = unix_server.handle_error

        def handle_error(request, address) -> None:
            # A crashed node agent drops its connection mid-reply. The daemon
            # survives that by design; it is not worth a traceback.
            if not isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
                report(request, address)

        unix_server.handle_error = handle_error

    def _agent_config(self) -> NodeAgentConfig:
        return NodeAgentConfig(
            bin=str(self.bin), proc_root=str(self.proc_root), state_root=str(self.state_root),
            volumes=str(self.volumes), runtime_root=str(self.runtime_root),
            storage_socket=str(self.storage_socket), init_binary=str(self.fleet.init_binary),
            job_id=self.job_id, node_id=self.node_id, deployment_id=self.fleet.deployment_id,
            node_control_token=self.fleet.tokens.node_control, node_epoch=self.boot_id.hex,
            port=self.port, url=self.url, admission_wait_seconds=self.fleet.admission_wait_seconds,
            heartbeat_url=self.fleet.gateway_url + "/v1/nodes/heartbeat",
            heartbeat_token=self.fleet.tokens.heartbeat,
            heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS,
        )

    @property
    def running(self) -> bool:
        return self.server is not None or self.process is not None

    def start(self) -> None:
        """Start a node-agent process over this node's durable state."""
        if self.running:
            raise RuntimeError("node agent is already running")
        if self.fleet.node_processes:
            self._spawn()
            return
        server, self.service = assemble_node_agent(
            self._agent_config(),
            rootfs_store=LocalRootfsStore(self.state_root / "image-cache", self.fleet.catalog),
            fencer=None if native_pidfd_available() else SyscallPidfdFencer(proc_root=self.proc_root),
            sample_metrics=lambda: self._sample_metrics(),
        )
        self._bound(server.server_address[1])
        node = self

        class ObservedHandler(server.RequestHandlerClass):
            def parse_request(self) -> bool:
                if not super().parse_request():
                    return False
                node._requests.append((self.command, self.path))
                if not node._honor_exec_session_prefix:
                    del self.headers[EXEC_SESSION_PREFIX_HEADER]
                return True

        server.RequestHandlerClass = ObservedHandler
        accept = server.get_request

        def tracked_accept():
            connection, address = accept()
            self._connections.add(connection)
            return connection, address

        server.get_request = tracked_accept
        self.server = server
        self._thread = _serve(server)
        # The new agent's sender also sends at once; wait until the gateway
        # accepted one, so no send is in flight when a scenario continues.
        self.post_heartbeat()

    def _bound(self, port: int) -> None:
        if self.url is None:
            self.port = port
            self.url = f"http://127.0.0.1:{port}"

    def _spawn(self) -> None:
        ready = self.root / "agent.ready"
        ready.unlink(missing_ok=True)
        pid = _fork_node_agent({
            "config": asdict(self._agent_config()), "ready": str(ready),
            "catalog": str(self.fleet.catalog.root), "log": str(self.root / "agent.log"),
        })
        ticks = _process_ticks(pid)
        self.process = (pid, ticks)
        deadline = time.monotonic() + 30
        while not ready.exists():
            if ticks is None or _process_ticks(pid) != ticks or time.monotonic() > deadline:
                self._reap_process(signal.SIGKILL)
                log = (self.root / "agent.log").read_text(encoding="utf-8", errors="replace")
                raise AssertionError(f"node agent {self.node_id} did not start:\n{log[-4000:]}")
            time.sleep(0.005)
        self._bound(int(ready.read_text(encoding="ascii")))

    def _reap_process(self, sig: int) -> None:
        process, self.process = self.process, None
        if process is None:
            return
        pid, ticks = process
        if ticks is None or _process_ticks(pid) != ticks:
            return  # It already exited; the kernel reaped it.
        with suppress(ProcessLookupError):
            os.kill(pid, sig)
            deadline = time.monotonic() + 10
            while _process_ticks(pid) == ticks:
                if time.monotonic() > deadline:
                    os.kill(pid, signal.SIGKILL)
                time.sleep(0.005)

    def stop(self) -> None:
        """Stop the node agent; sentries and mounts survive, as on a VM.

        Process exit would also sever every accepted connection, including the
        gateway's pooled keep-alives. In-process handler threads would keep
        serving those with the old service, so they are shut down here.
        """
        self._reap_process(signal.SIGTERM)
        server, self.server = self.server, None
        if server is None:
            return
        server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
        for connection in list(self._connections):
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self._connections.clear()
        server.server_close()
        # Process exit would release registry ownership for the next agent.
        self.service.provisioner.registry.close()
        self.service = None

    def crash(self) -> None:
        """SIGKILL the node-agent process (``LocalFleet(node_processes=True)``).

        Nothing of the agent's own runs afterwards; runtime invocations it
        started keep running or blocking, as orphans would.
        """
        if self.process is None:
            raise RuntimeError("only a node-agent process can crash")
        self._reap_process(signal.SIGKILL)

    def restart(self) -> None:
        self.stop()
        self.start()

    def reboot(self) -> None:
        """Power-cycle the VM: guest processes die and the boot ID changes.

        Durable node state survives, as on the VM disk. Mount tables and
        storage devices are not reset (see README).
        """
        if self.process is not None:
            self.crash()
        else:
            self.stop()
        self.kill_hung()
        self.reap_containers()
        self.set_boot_id(uuid.uuid4())
        self.start()

    def set_boot_id(self, boot_id: uuid.UUID) -> None:
        self.boot_id = boot_id
        (self.proc_root / "sys/kernel/random/boot_id").write_text(f"{boot_id}\n", encoding="ascii")

    def close(self) -> None:
        try:
            self.kill_hung()
            if self.storage_host is not None:
                self.storage_host.release_holds()
            self.stop()
        finally:
            self.reap_containers()
            if self._storage_server is not None:
                self._stop_storage()
            if self._socket_dir is not None:
                shutil.rmtree(self._socket_dir, ignore_errors=True)

    def _stop_storage(self) -> None:
        """Stop the storage server without waiting out its 0.5 s accept poll.

        ``shutdown`` is only noticed between requests, so cheap feature reads
        wake the loop until it exits.
        """
        assert self._storage_server is not None
        stopper = threading.Thread(target=self._storage_server.shutdown, daemon=True)
        stopper.start()
        client = StorageNativeNodeClient(self.storage_socket, timeout_seconds=0.1)
        deadline = time.monotonic() + 5
        while stopper.is_alive() and time.monotonic() < deadline:
            try:
                client.get_features()
            except Exception:
                pass  # The server stopped and closed its socket.
            stopper.join(timeout=0.01)
        if self._storage_thread is not None:
            self._storage_thread.join(timeout=5)

    def reap_containers(self) -> None:
        """Kill every exec group and fake sentry this node ever started.

        Sentries come from the fake's spawn ledger, not from container state:
        ``delete`` drops the state of a sentry the Warden failed to reap.
        """
        if not self.runtime_root.is_dir():
            return
        for path in self.runtime_root.glob("*_sandbox:*.state"):
            try:
                fake = json.loads(path.read_text(encoding="utf-8")).get("fake", {})
            except (OSError, ValueError):
                continue
            kill_exec_groups(fake.get("execs", ()))
        for pid in self.live_sentries():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def live_sentries(self) -> list[int]:
        """PIDs of fake sentries started on this node that still run."""
        try:
            lines = (self.runtime_root / "fake-sentries").read_text(encoding="ascii").splitlines()
        except FileNotFoundError:
            return []
        live = []
        for line in lines:
            fields = line.split()
            if len(fields) == 2 and all(field.isdigit() for field in fields):
                pid, ticks = int(fields[0]), int(fields[1])
                if _process_ticks(pid) == ticks:
                    live.append(pid)
        return live

    # -- observation and fault injection --------------------------------------

    def post_heartbeat(self) -> dict:
        """Send this node's heartbeat now; return the gateway's answer.

        An in-process agent's real sender sends it. A node-agent process is
        relayed from its own heartbeat endpoint, which the sender also reads.
        """
        if self.server is not None:
            result = self.server.heartbeat_sender.send_now()
        else:
            assert self.process is not None and self.url is not None
            result = post_heartbeat_with_headers(
                self.fleet.gateway_url + "/v1/nodes/heartbeat",
                fetch_node_agent_heartbeat(self.url, bearer_token=self.fleet.tokens.node_control),
                {"Authorization": f"Bearer {self.fleet.tokens.heartbeat}"},
            )
        if result.status != 200:
            raise AssertionError(f"heartbeat rejected ({result.status}): {result.payload}")
        return result.payload

    def registration(self, sandbox_id: str):
        if self.service is not None:
            return self.service.provisioner.registry.get(sandbox_id)
        # A node-agent process, running or not: read its durable registry.
        return DirectSandboxRegistry(self.state_root / "direct-registry.sqlite").get(sandbox_id)

    def request(self, method: str, path: str, *, payload: object | None = None,
                headers: dict[str, str] | None = None) -> Response:
        """Call this node's control API directly, as the gateway or autoscaler does."""
        assert self.url is not None
        return _request(self.url, method, path, self.fleet.tokens.node_control,
                        payload=payload, headers=headers)

    def drain(self, token: str, *, draining: bool = True) -> Response:
        """Set node drain as the autoscaler does."""
        return self.request("POST", "/v1/drain", payload={"draining": draining, "token": token})

    def arm_fault(self, command: str, action: str, *, sandbox_id: str | None = None,
                  generation: int = 1) -> None:
        """Arm a one-shot fault (``faults.py``) on a fake runsc or mount command.

        With ``sandbox_id`` it fires only for that incarnation's container.
        """
        match = self.container_id(sandbox_id, generation) if sandbox_id is not None else ""
        faults.arm(self.faults, command, action, match)

    def wait_hung(self, command: str, *, timeout: float = 15.0) -> int:
        """Wait until a hang fault blocks ``command``; return the blocked PID."""
        marker = self.faults / f"{command}.hung"
        deadline = time.monotonic() + timeout
        while not marker.exists():
            if time.monotonic() > deadline:
                raise AssertionError(f"{command} did not reach its armed hang on {self.node_id}")
            time.sleep(0.005)
        return int(marker.read_text(encoding="ascii").split()[0])

    def release(self, command: str) -> None:
        """Let a blocked invocation continue as if it had only been slow."""
        (self.faults / f"{command}.hung").unlink()

    def kill_hung(self) -> None:
        """SIGKILL every blocked invocation: it died without finishing."""
        for marker in self.faults.glob("*.hung"):
            try:
                pid, ticks = (int(item) for item in marker.read_text(encoding="ascii").split())
            except (OSError, ValueError):
                continue
            if _process_ticks(pid) == ticks:
                with suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
                while _process_ticks(pid) == ticks:
                    time.sleep(0.005)
            marker.unlink(missing_ok=True)

    def container_id(self, sandbox_id: str, generation: int | None = None) -> str:
        """The incarnation's deterministic runsc ID (``OverlayRootfsManager``).

        Without ``generation`` the registered one is used, else 1, so a deleted
        sandbox's first incarnation stays inspectable.
        """
        if generation is None:
            registration = self.registration(sandbox_id)
            generation = registration.sandbox_generation if registration is not None else 1
        return hashlib.sha256(f"{sandbox_id}:{generation}".encode("utf-8")).hexdigest()

    def runsc_state(self, sandbox_id: str, generation: int | None = None) -> dict | None:
        cid = self.container_id(sandbox_id, generation)
        try:
            return json.loads(
                (self.runtime_root / f"{cid}_sandbox:{cid}.state").read_text(encoding="utf-8")
            )
        except FileNotFoundError:
            return None

    def sentry_pid(self, sandbox_id: str) -> int:
        state = self.runsc_state(sandbox_id)
        if state is None:
            raise AssertionError(f"{sandbox_id} has no runtime on {self.node_id}")
        return int(state["fake"]["sentryPid"])

    def kill_sentry(self, sandbox_id: str) -> int:
        """Crash the sandbox runtime out of band (OOM kill, sentry panic)."""
        pid = self.sentry_pid(sandbox_id)
        os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while _process_ticks(pid) is not None:
            if time.monotonic() > deadline:
                raise AssertionError(f"sentry {pid} did not exit")
            time.sleep(0.005)
        return pid


class LocalFleet:
    """Gateway plus ``nodes`` workers in one temporary directory.

    Use as a context manager. ``postgres=True`` routes through PostgreSQL when
    ``UCLOUD_TEST_POSTGRES_DSN`` is set and skips the test otherwise.
    ``node_processes=True`` runs each node agent in its own process, so
    ``FleetNode.crash`` can SIGKILL it; request observation, settable host
    samples and the exec-prefix knob then raise. ``admission_wait_seconds``
    bounds the gateway's and each node's admission waits (production: 30 s),
    and ``max_concurrent_sandbox_creates`` caps the gateway's in-flight creates.
    ``gateways`` servers share the routing and heartbeat state, as one host's
    gateway processes do (nodes heartbeat to the first); ``create_placement``
    is the deployment switch.
    """

    def __init__(self, *, nodes: int = 1, postgres: bool = False, heartbeat_ttl_seconds: int = 120,
                 node_processes: bool = False, admission_wait_seconds: float = 30.0,
                 max_concurrent_sandbox_creates: int | None = None, gateways: int = 1,
                 create_placement: str = "ranked") -> None:
        if nodes < 1:
            raise ValueError("a fleet needs at least one node")
        if postgres and not POSTGRES_DSN:
            raise unittest.SkipTest("requires real PostgreSQL (UCLOUD_TEST_POSTGRES_DSN)")
        self.node_count = nodes
        self.postgres = postgres
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self.node_processes = node_processes
        self.admission_wait_seconds = admission_wait_seconds
        self.gateway_options = (
            {} if max_concurrent_sandbox_creates is None
            else {"max_concurrent_sandbox_creates": max_concurrent_sandbox_creates}
        )
        self.gateway_options.update(create_placement=create_placement, process_count=gateways)
        self.gateway_count = gateways
        self.gateways: list = []
        self.tokens = FleetTokens.generate()
        self.deployment_id = "harness-" + secrets.token_hex(4)
        self.init_binary = root_owned_init_binary()
        self.nodes: list[FleetNode] = []
        self.gateway = None
        self.gateway_url = ""
        self._temporary: tempfile.TemporaryDirectory | None = None
        self._postgres_schema = ""

    def __enter__(self) -> "LocalFleet":
        try:
            self.start()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def start(self) -> None:
        self._temporary = tempfile.TemporaryDirectory(prefix="ucloud-local-fleet-", dir=_scratch_root())
        self.root = Path(self._temporary.name).resolve()
        self.catalog = ImageCatalog(self.root / "catalog")
        self.catalog.add(DEFAULT_IMAGE, {
            "etc/hostname": "harness\n",
            "etc/motd": "local fleet base image\n",
            "opt/greeting": "hello from the image\n",
        })
        gateway_root = self.root / "gateway"
        gateway_root.mkdir(mode=0o700)
        routing_file = self.routing_file = gateway_root / "routes.sqlite"
        if self.postgres:
            self._prepare_postgres_routing(routing_file)
        for _index in range(self.gateway_count):
            server = build_server(
                "127.0.0.1",
                0,
                gateway_root / "control-state.sqlite",
                gateway_bearer_token=self.tokens.gateway,
                sandbox_api_token=self.tokens.sandbox_api,
                heartbeat_bearer_token=self.tokens.heartbeat,
                node_control_bearer_token=self.tokens.node_control,
                deployment_id=self.deployment_id,
                routing_file=routing_file,
                image_file=gateway_root / "images.json",
                metrics_file=gateway_root / "metrics.sqlite",
                heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
                **self.gateway_options,
            )
            server.RequestHandlerClass.admission_wait_seconds = self.admission_wait_seconds
            self.gateways.append((server, _serve(server)))
        self.gateway = self.gateways[0][0]
        host, port = self.gateway.server_address
        self.gateway_url = f"http://{host}:{port}"
        for index in range(self.node_count):
            node = FleetNode(self, index)
            self.nodes.append(node)
            node.provision()
            node.start()
        self.heartbeat()

    def _prepare_postgres_routing(self, routing_file: Path) -> None:
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

        schema = "ucloud_routing_fleet_" + uuid.uuid4().hex
        store = PostgresRoutingStore(routing_file, dsn=POSTGRES_DSN, schema=schema)
        self._postgres_schema = schema
        try:
            store.migrate()
        finally:
            store.close()
        dsn_file = routing_file.with_suffix(".dsn")
        dsn_file.write_text(POSTGRES_DSN, encoding="utf-8")
        dsn_file.chmod(0o600)
        routing_file.write_text(json.dumps({
            "format": "ucloud-postgres-routing-v1",
            "schema": self._postgres_schema,
            "dsn_file": str(dsn_file),
        }), encoding="utf-8")

    def close(self) -> None:
        errors: list[BaseException] = []
        for server, thread in self.gateways:
            try:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()
            except BaseException as exc:
                errors.append(exc)
        self.gateways, self.gateway = [], None
        for node in self.nodes:
            try:
                node.close()
            except BaseException as exc:
                errors.append(exc)
        if self._postgres_schema:
            try:
                self._drop_postgres_routing()
            except BaseException as exc:
                errors.append(exc)
            self._postgres_schema = ""
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None
        if errors:
            raise errors[0]

    def _drop_postgres_routing(self) -> None:
        import psycopg
        from psycopg import sql

        cached = routing._POSTGRES_ROUTING_STORES.pop((os.getpid(), self.routing_file.resolve()), None)
        if cached is not None:
            cached.close()
        with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self._postgres_schema))
            )

    # -- public API ------------------------------------------------------------

    def heartbeat(self) -> None:
        """Make every running node agent send a heartbeat now."""
        for node in self.nodes:
            if node.running:
                node.post_heartbeat()

    def continuity_cycle(self, node: FleetNode, *, interrupted_at=None, state: str = "RUNNING"):
        """Run the autoscaler's per-cycle guest-continuity step for ``node``.

        A part of the S9 gap: the provider is a fake RUNNING (or ``state``)
        job, last interrupted at ``interrupted_at``, and the probe is real.
        Returns the resulting job, heartbeat and the routes it retired.
        """
        from ucloud_sandboxes import cli
        from ucloud_sandboxes.deployment import DEPLOYMENT_LABEL, NODE_LABEL
        from ucloud_sandboxes.models import InstancePhase, ProviderInstance, ScalePolicy

        handler = self.gateway.RequestHandlerClass
        store, routes = handler.services.fleet.store, handler.routing_store
        job = ProviderInstance(
            id=node.job_id, name=node.node_id, application_name="", application_version="",
            product_id="", product_category="", state=state, labels={
                DEPLOYMENT_LABEL: self.deployment_id, NODE_LABEL: "true"},
            phase=InstancePhase.RUNNING if state == "RUNNING" else InstancePhase.UNAVAILABLE,
            interrupted_at=interrupted_at,
        )
        owned = tuple(r for r in routes.sandbox_routes_readonly() if r.job_id == node.job_id)
        retired: list = []
        jobs, heartbeats = cli._quarantine_unverified_guests(
            [job], store.load_heartbeats(), control_state=store,
            policy=ScalePolicy(heartbeat_ttl_seconds=self.heartbeat_ttl_seconds),
            deployment_id=self.deployment_id, route_reservations={node.job_id: owned},
            execution_authorized=True, bearer_token=self.tokens.node_control,
            routing_store=routes, retired_routes=retired,
        )
        return jobs[0], heartbeats[node.job_id], retired

    def expire_heartbeat(self, node: FleetNode) -> None:
        """Age ``node``'s last heartbeat receipt past the TTL, as silence would.

        Only the gateway-controlled receipt time moves; the next relayed
        heartbeat is fresh again.
        """
        store = self.gateway.RequestHandlerClass.services.fleet.store
        current = store.get_heartbeat(node.job_id)
        if current is None:
            raise AssertionError(f"{node.node_id} has no heartbeat")
        aged = current.freshness_at - timedelta(seconds=self.heartbeat_ttl_seconds + 1)
        store.upsert_heartbeat(replace(current, received_at=aged, updated_at=aged))

    @contextmanager
    def routing_calls(self) -> Iterator[list[str]]:
        """Record the name of every routing-store method the gateway calls."""
        store = self.gateway.RequestHandlerClass.routing_store
        calls: list[str] = []
        names = [name for name, _ in inspect.getmembers(type(store), inspect.isfunction)
                 if not name.startswith("_")]
        for name in names:
            def record(*args, _name=name, _method=getattr(store, name), **kwargs):
                calls.append(_name)
                return _method(*args, **kwargs)
            setattr(store, name, record)
        try:
            yield calls
        finally:
            for name in names:
                delattr(store, name)

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: object | None = None,
        body: bytes | None = None,
        token: str = "gateway",
        timeout: float = 30.0,
        gateway: int = 0,
    ) -> Response:
        credential = {"gateway": self.tokens.gateway, "sandbox": self.tokens.sandbox_api}[token]
        host, port = self.gateways[gateway][0].server_address
        return _request(f"http://{host}:{port}", method, path, credential,
                        payload=payload, body=body, timeout=timeout)

    def create(self, sandbox_id: str, *, image: str = DEFAULT_IMAGE, **spec) -> dict:
        payload = {
            "id": sandbox_id, "image": image, "cpus": 1, "memory_mb": 256,
            "disk_mb": 1024, "network": "none", **spec,
        }
        response = self.request("POST", "/v1/sandboxes", payload=payload, token="sandbox")
        if response.status not in {200, 201}:
            raise AssertionError(f"create failed ({response.status}): {response.body!r}")
        return response.json()["sandbox"]

    def start_exec(self, sandbox_id: str, command: list[str], *, stdin: bool = False,
                   env: dict[str, str] | None = None, working_dir: str | None = None,
                   initial_wait_seconds: float | None = 0.05) -> Response:
        query = "" if initial_wait_seconds is None else f"?initial_wait_seconds={initial_wait_seconds}"
        return self.request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id)}/exec{query}",
            payload={"command": command, "env": env or {}, "working_dir": working_dir,
                     "stdin": stdin, "tty": False},
            token="sandbox",
        )

    def events(self, session_id: str, *, after: int, wait_seconds: float = 1.0) -> Response:
        return self.request(
            "GET", f"/v1/exec/{quote(session_id)}/events?after={after}&wait_seconds={wait_seconds}",
            token="sandbox",
        )

    def exec_stdin(self, session_id: str, data: str, *, eof: bool = False) -> Response:
        return self.request(
            "POST", f"/v1/exec/{quote(session_id)}/stdin",
            payload={"data": data, "eof": eof}, token="sandbox",
        )

    def exec_signal(self, session_id: str, signal_number: int) -> Response:
        return self.request(
            "POST", f"/v1/exec/{quote(session_id)}/signal",
            payload={"signal": signal_number}, token="sandbox",
        )

    def exec(self, sandbox_id: str, command: list[str], *, timeout: float = 15.0, **options) -> ExecResult:
        started = self.start_exec(sandbox_id, command, **options)
        if started.status != 201:
            raise AssertionError(f"exec start failed ({started.status}): {started.body!r}")
        payload = started.json()
        return self.wait_exec(payload["session"]["id"], payload.get("events", ()), timeout=timeout)

    def _poll_events(self, session_id: str, initial, done, *, timeout: float, waiting_for: str) -> list[dict]:
        """Poll the gateway's event route until ``done(events)``."""
        events = list(initial)
        deadline = time.monotonic() + timeout
        while not done(events):
            if time.monotonic() > deadline:
                raise AssertionError(f"exec {session_id} did not {waiting_for}: {events}")
            after = events[-1]["sequence"] if events else 0
            polled = self.events(session_id, after=after)
            if polled.status != 200:
                raise AssertionError(f"exec events failed ({polled.status}): {polled.body!r}")
            events.extend(polled.json()["events"])
        return events

    def wait_output(self, session_id: str, stdout: str, initial=(), *, timeout: float = 15.0) -> list[dict]:
        """Wait until the running session's stdout starts with ``stdout``."""
        def printed(events: list[dict]) -> str:
            return "".join(e["data"] for e in events if e["stream"] == "stdout")

        events = self._poll_events(
            session_id, initial,
            lambda events: printed(events).startswith(stdout)
            or any(event["stream"] == "exit" for event in events),
            timeout=timeout, waiting_for=f"print {stdout!r}",
        )
        if not printed(events).startswith(stdout):
            raise AssertionError(f"exec {session_id} exited before printing {stdout!r}: {events}")
        return events

    def wait_exec(self, session_id: str, initial=(), *, timeout: float = 15.0) -> ExecResult:
        """Poll the gateway's event route until the session's exit event."""
        events = self._poll_events(
            session_id, initial,
            lambda events: any(event["stream"] == "exit" for event in events),
            timeout=timeout, waiting_for="exit",
        )
        exit_code = next(event["exit_code"] for event in events if event["stream"] == "exit")
        return ExecResult(
            session_id=session_id,
            exit_code=exit_code,
            stdout="".join(e["data"] for e in events if e["stream"] == "stdout"),
            stderr="".join(e["data"] for e in events if e["stream"] == "stderr"),
            events=tuple(events),
        )

    def write_file(self, sandbox_id: str, path: str, data: bytes) -> Response:
        return self.request(
            "PUT", f"/v1/sandboxes/{quote(sandbox_id)}/files?path={quote(path)}",
            body=data, token="sandbox",
        )

    def read_file(self, sandbox_id: str, path: str) -> Response:
        return self.request(
            "GET", f"/v1/sandboxes/{quote(sandbox_id)}/files?path={quote(path)}", token="sandbox",
        )

    def park(self, sandbox_id: str) -> Response:
        return self.request(
            "POST", f"/v1/sandboxes/{quote(sandbox_id)}/park",
            payload={"operation_id": f"park-{uuid.uuid4().hex}"},
        )

    def wake(self, sandbox_id: str, *, generation: int) -> Response:
        return self.request(
            "POST", f"/v1/sandboxes/{quote(sandbox_id)}/wake",
            payload={"operation_id": f"wake-{uuid.uuid4().hex}", "generation": generation},
        )

    def status(self, sandbox_id: str) -> Response:
        return self.request("GET", f"/v1/sandboxes?view=status&id={quote(sandbox_id)}")

    def delete(self, sandbox_id: str) -> Response:
        return self.request("DELETE", f"/v1/sandboxes/{quote(sandbox_id)}", token="sandbox")

    def route(self, sandbox_id: str):
        return routing.open_routing_store(self.routing_file).get_sandbox(sandbox_id)

    def node_for(self, sandbox_id: str) -> FleetNode:
        route = self.route(sandbox_id)
        if route is None:
            raise AssertionError(f"{sandbox_id} has no route")
        return next(node for node in self.nodes if node.job_id == route.job_id)
