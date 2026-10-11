"""The agent's client for runtime/noded's internal socket (docs/admission-port-3c.md §3.3).

With ``--rust-admission`` noded serves ``<state_root>/noded/noded.sock``: mode
0600, the node control bearer token, and only the agent's uid (SO_PEERCRED).
It is the reverse of noded's client for this agent's socket
(runtime/noded/src/agent_rpc.rs), and works the same way: pooled HTTP/1
connections, JSON bodies, and session headers both ways. Every request names
this agent start (``X-UCloud-Agent-Session``, one uuid per process) and every
answer names noded's start (``X-UCloud-Noded-Session``), so each side notices
the other's restart.

Errors say what the caller may conclude:

- ``NodedUnavailable``: nothing reached noded, so a retry is safe;
- ``NodedLost``: the request was written and no answer came back, so noded
  may have acted on it;
- ``NodedRefused``: noded answered with an error (``error_code``, ``retryable``);
- ``NodedProtocolError``: an answer outside the contract.

``NodedLink`` says hello at agent start and again whenever a new noded
session appears; each hello reasserts the claims this agent holds (§3.5).
``admission_backend.NodedAdmission`` makes the admission calls over it.
"""

from __future__ import annotations

import http.client
import json
import logging
from pathlib import Path
import re
import socket
import threading
import time
from typing import Any, Callable
import uuid

from .create_handoff import NODED_SESSION_HEADER
from .pause_handoff import NODED_DIRECTORY

AGENT_SESSION_HEADER = "X-UCloud-Agent-Session"
NODED_SOCKET_FILE = "noded.sock"
DEFAULT_TIMEOUT_SECONDS = 30.0
# Pooled connections idle longer than this are closed, not reused: noded's
# server may close an idle connection, and a close that races a request
# would make that request Lost.
POOL_IDLE_SECONDS = 10.0
MAX_IDLE_CONNECTIONS = 16
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
# NodedLink's retry backoff while noded is unreachable.
HELLO_RETRY_SECONDS = (0.05, 2.0)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_LOG = logging.getLogger(__name__)


def noded_socket_path(service: Any) -> Path:
    """``<state_root>/noded/noded.sock``, beside pause_handoff's files."""
    return service.provisioner.registry.path.parent / NODED_DIRECTORY / NODED_SOCKET_FILE


class NodedError(RuntimeError):
    pass


class NodedUnavailable(NodedError):
    """Nothing reached noded: safe to retry."""


class NodedLost(NodedError):
    """The request was written but no answer came back: noded may have acted."""


class NodedProtocolError(NodedError):
    """An answer that is not the JSON the contract promises."""


class NodedRefused(NodedError):
    """noded answered with an error status and body."""

    def __init__(self, status: int, payload: dict[str, Any]) -> None:
        self.status = status
        self.payload = payload
        self.error_code = str(payload.get("error_code") or "")
        self.retryable = payload.get("retryable") is True
        super().__init__(f"noded refused ({status} {self.error_code}): {payload.get('error', '')}")


class _Connection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._path = path
        self.idle_since = 0.0

    def connect(self) -> None:
        stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            stream.settimeout(self.timeout)
            deadline = time.monotonic() + self.timeout
            delay = 0.005
            while True:
                try:
                    stream.connect(self._path)
                    break
                except BlockingIOError:
                    # A timeout makes the socket non-blocking; a full accept
                    # queue then fails at once instead of waiting.
                    if time.monotonic() + delay >= deadline:
                        raise
                    time.sleep(delay)
                    delay = min(delay * 2, 0.1)
        except BaseException:
            stream.close()
            raise
        self.sock = stream

    def reusable(self, now: float) -> bool:
        """Open, quiet and recently used: the server has not closed it, and
        no stray bytes wait on it (hyper's ``SendRequest::ready``)."""
        if self.sock is None or now - self.idle_since > POOL_IDLE_SECONDS:
            return False
        timeout = self.sock.gettimeout()
        # Non-blocking for the probe: with a timeout set, recv waits first.
        self.sock.setblocking(False)
        try:
            self.sock.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            return True
        except OSError:
            return False
        finally:
            self.sock.settimeout(timeout)
        return False  # Closed by noded (EOF), or bytes nobody asked for.


class NodedClient:
    """JSON calls to noded's internal socket over pooled connections.

    ``on_session`` is called with each new ``X-UCloud-Noded-Session`` an
    answer carries (not with the first one this client sees).
    """

    def __init__(self, socket_path: str | Path, token: str, *, agent_session: str | None = None,
                 timeout: float = DEFAULT_TIMEOUT_SECONDS,
                 on_session: Callable[[str], None] | None = None) -> None:
        token = token.strip()
        if not token:
            raise ValueError("noded client needs the node control bearer token")
        self.socket_path = str(socket_path)
        self.agent_session = agent_session or str(uuid.uuid4())
        self.timeout = float(timeout)
        self.on_session = on_session
        self._authorization = f"Bearer {token}"
        self._guard = threading.Lock()
        self._idle: list[_Connection] = []
        self._noded_session: str | None = None
        self.connections_opened = 0

    @property
    def noded_session(self) -> str | None:
        """The noded session of the last answer."""
        with self._guard:
            return self._noded_session

    def close(self) -> None:
        with self._guard:
            idle, self._idle = self._idle, []
        for connection in idle:
            connection.close()

    def _checkout(self, timeout: float) -> _Connection:
        now = time.monotonic()
        while True:
            with self._guard:
                connection = self._idle.pop() if self._idle else None
            if connection is None:
                break
            if connection.reusable(now):
                connection.sock.settimeout(timeout)
                return connection
            connection.close()
        connection = _Connection(self.socket_path, timeout)
        try:
            connection.connect()
        except OSError as exc:
            raise NodedUnavailable(f"noded is unavailable: {exc}") from exc
        with self._guard:
            self.connections_opened += 1
        return connection

    def _checkin(self, connection: _Connection) -> None:
        connection.idle_since = time.monotonic()
        with self._guard:
            if len(self._idle) < MAX_IDLE_CONNECTIONS:
                self._idle.append(connection)
                return
        connection.close()

    def _observe(self, session: str | None) -> None:
        if not session:
            return
        with self._guard:
            previous, self._noded_session = self._noded_session, session
        if previous is not None and previous != session and self.on_session is not None:
            self.on_session(session)

    def call(self, method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        """One request; the answer's JSON object, or a NodedError."""
        timeout = self.timeout if timeout is None else float(timeout)
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        headers = {"Authorization": self._authorization, AGENT_SESSION_HEADER: self.agent_session}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        connection = self._checkout(timeout)
        try:
            connection.request(method, path, body=payload, headers=headers)
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE_BYTES + 1)
        except (OSError, http.client.HTTPException) as exc:
            connection.close()
            raise NodedLost(f"noded did not answer {method} {path}: {exc!r}") from exc
        if len(raw) > MAX_RESPONSE_BYTES or response.will_close:
            connection.close()
        else:
            self._checkin(connection)
        self._observe(response.getheader(NODED_SESSION_HEADER))
        try:
            answer = json.loads(raw)
        except ValueError as exc:
            raise NodedProtocolError(f"noded answered {method} {path} with {response.status} and no JSON") from exc
        if not isinstance(answer, dict):
            raise NodedProtocolError(f"noded answered {method} {path} with a non-object")
        if response.status >= 400:
            raise NodedRefused(response.status, answer)
        if response.status != 200:
            raise NodedProtocolError(f"noded answered {method} {path} with {response.status}")
        return answer

    def hello(self, *, timeout: float | None = None, reassert: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """``{noded_session, admission_open, config_sha256}``. From step 3c.2
        noded then drops whatever an earlier agent session held, and holds
        the ``reassert`` claims ``[{owner, kind, cost, requested,
        agent_token}]`` under this session. ``admission_open`` and
        ``config_sha256`` are None until noded has loaded the node."""
        body: dict[str, Any] = {"agent_session": self.agent_session}
        if reassert:
            body["reassert"] = reassert
        answer = self.call("POST", "/internal/v1/admission/hello", body, timeout=timeout)
        session, admission_open, digest = (answer.get("noded_session"), answer.get("admission_open"),
                                           answer.get("config_sha256"))
        if (set(answer) != {"noded_session", "admission_open", "config_sha256"}
                or not isinstance(session, str) or not session
                or not (admission_open is None or isinstance(admission_open, bool))
                or not (digest is None or (isinstance(digest, str) and _DIGEST.match(digest)))):
            raise NodedProtocolError(f"noded's hello answer is outside its contract: {answer!r}")
        return answer

    def ping(self, *, timeout: float | None = None) -> dict[str, Any]:
        """``{noded_session, hello}``: whether this agent session said hello."""
        return self.call("GET", "/internal/v1/admission/ping", timeout=timeout)


class NodedLink:
    """Says hello at agent start and again whenever a new noded session appears.

    A background thread retries while noded is unreachable (it restarts on its
    own, and the agent must start without it). ``observe_session`` takes
    sessions seen elsewhere, such as on noded's requests to this agent.
    ``hello_now`` says hello at once, for a caller noded refused for want of
    one. Every hello reasserts ``reassert()``'s claims, and ``on_hello`` runs
    after each one that succeeds (admission_backend.NodedAdmission).
    """

    def __init__(self, client: NodedClient, *, retry_seconds: tuple[float, float] = HELLO_RETRY_SECONDS) -> None:
        self.client = client
        self.retry_seconds = retry_seconds
        client.on_session = self.observe_session
        self.reassert: Callable[[], list[dict[str, Any]]] = list
        self.on_hello: Callable[[dict[str, Any]], None] | None = None
        self._guard = threading.Condition()
        # One hello at a time, from the thread or from hello_now.
        self._saying = threading.Lock()
        self._hello: dict[str, Any] | None = None
        self._seen: str | None = None
        self._wanted = True
        self._stopped = False
        self._thread: threading.Thread | None = None

    @property
    def hello(self) -> dict[str, Any] | None:
        """The last hello's answer."""
        with self._guard:
            return self._hello

    def observe_session(self, session: str | None) -> None:
        """Say hello again if ``session`` is not the one the last hello met."""
        if not session:
            return
        with self._guard:
            if self._hello is not None and self._hello["noded_session"] == session:
                return
            self._seen, self._wanted = session, True
            self._guard.notify_all()

    def wait_hello(self, timeout: float, *, session: str | None = None) -> dict[str, Any] | None:
        """The hello answer once there is one (from ``session``, if given)."""
        deadline = time.monotonic() + timeout
        with self._guard:
            while self._hello is None or (session is not None and self._hello["noded_session"] != session):
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._stopped:
                    return None
                self._guard.wait(remaining)
            return self._hello

    def hello_now(self, *, timeout: float | None = None) -> dict[str, Any]:
        """Say hello now, reasserting the held claims; a NodedError if noded refuses or is down."""
        with self._saying:
            hello = self.client.hello(timeout=timeout, reassert=self.reassert())
            with self._guard:
                self._hello = hello
                if self._seen in (None, hello["noded_session"]):
                    self._wanted = False  # This hello already met the session that asked for it.
                self._seen = None
                self._guard.notify_all()
            if self.on_hello is not None:
                try:
                    self.on_hello(hello)
                except Exception:  # noqa: BLE001 - the hello itself succeeded.
                    _LOG.exception("work after the noded hello failed")
        return hello

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="ucloud-noded-hello", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        with self._guard:
            self._stopped = True
            self._guard.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self.client.close()

    def _loop(self) -> None:
        delay, logged = self.retry_seconds[0], float("-inf")
        while True:
            with self._guard:
                while not self._wanted and not self._stopped:
                    self._guard.wait()
                if self._stopped:
                    return
                self._wanted = False
            try:
                hello = self.hello_now()
            except NodedError as exc:
                now = time.monotonic()
                if now - logged >= 60.0:
                    logged = now
                    _LOG.warning("noded hello failed; retrying: %s", exc)
                with self._guard:
                    self._wanted = True
                    self._guard.wait_for(lambda: self._stopped, timeout=delay)
                delay = min(delay * 2, self.retry_seconds[1])
                continue
            delay = self.retry_seconds[0]
            _LOG.info("noded session %s: admission_open=%s config_sha256=%s", hello["noded_session"],
                      hello["admission_open"], hello["config_sha256"])
