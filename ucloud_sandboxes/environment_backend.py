"""Nodewide privileged artifact I/O, independent of node-agent process lifetime.

This owns kernel mounts/devices only. Image references, retention and scheduling
remain with the existing image store/registry. A restarted backend never adopts
an old live filesystem: lost kernel exports are an explicit admission fence.
"""
from dataclasses import dataclass
import errno
import fcntl
import json
import logging
import os
import re
from pathlib import Path
import socket
import socketserver
import stat
import subprocess
from threading import Lock, RLock
import time

from .environment_artifact import CHUNK_BYTES, canonical_bytes, require_digest
from .environment_cache import VerifiedEnvironmentCache
from .environment_nbd import EnvironmentReadWorkers, ReadOnlyEnvironmentDevice
from .environment_trace import LocalTraceStore, MAX_TRACE_CHUNKS, trace_order

_LOG = logging.getLogger(__name__)
MAX_RPC_BYTES = 64 * 1024
# Per socket op: a stalled backend costs a heartbeat half the gateway's 2 s wake read.
METRICS_TIMEOUT_SECONDS = 1.0
NO_BLOCK_DEVICE = "no available environment block device"


def block_devices():
    return tuple(sorted(path for path in Path("/dev").glob("nbd[0-9]*") if re.fullmatch(r"nbd[0-9]+", path.name)))


def block_device_count():
    return len(block_devices())


def _private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise ValueError("environment backend requires private owned directories")


def _mounted(path):
    return subprocess.run(["mountpoint", "-q", str(path)], check=False).returncode == 0


def mount_has_dependents(path, *, include_bind_mounts=True):
    # umount(2) does NOT return EBUSY for an OverlayFS lower reference: the
    # detached superblock survives, and disconnecting its NBD would cause EIO.
    # All canonical overlays share this service's host mount namespace; their
    # lowerdir mount options are the kernel's physical dependency evidence.
    target = str(path)
    def unescape(value):
        return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)
    device = path.stat().st_dev
    device_identity = f"{os.major(device)}:{os.minor(device)}"
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        fields, details = before.split(), after.split()
        if include_bind_mounts and fields[2] == device_identity and unescape(fields[4]) != target:
            return True  # Another bind mount retains this filesystem.

        if details[0] != "overlay":
            continue
        for option in details[2].split(","):
            if option.startswith(("lowerdir=", "lowerdir+=")):
                for lower in option.split("=", 1)[1].split(":"):
                    lower = unescape(lower)
                    if not lower.startswith("/"):
                        # The kernel shows lowers as given. Composed images
                        # name components relative to their shared directory
                        # (EnvironmentRootfsStore); resolving any relative
                        # lower there can only over-report a dependency.
                        lower = str(path.parent / lower)
                    if lower == target or lower.startswith(target + "/"):
                        return True
    return False


@dataclass(frozen=True)
class PrefetchPolicy:
    """Attach-time prefetch bounds (C2.2 metadata hints, C2.3 startup traces).

    Each budget is also capped at a quarter of the chunk cache, so a prefetch
    never evicts most of what other components use.
    """
    metadata_bytes: int = 32 * 1024 ** 2
    metadata_seconds: float = 30.0
    # ensure() waits at most this long for metadata before reporting ready.
    metadata_wait_seconds: float = 5.0
    trace_bytes: int = 256 * 1024 ** 2
    trace_seconds: float = 120.0
    trace_window_seconds: float = 30.0
    trace_window_chunks: int = MAX_TRACE_CHUNKS
    enabled: bool = True


class EnvironmentBackend:
    def __init__(self, root, registry, *, devices=None, device_factory=ReadOnlyEnvironmentDevice,
                 mount=None, unmount=None, mounted=_mounted, referenced=mount_has_dependents, cache_bytes=1024 ** 3,
                 prefetch=PrefetchPolicy(), traces=None, cache_options=None):
        self.root, self.registry = Path(root), registry
        if not self.root.is_absolute():
            raise ValueError("environment backend root must be absolute")
        _private_directory(self.root)
        self.mounts = self.root / "components"
        _private_directory(self.mounts)
        self._lock_fd = os.open(self.root / "owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Receipt precedes attach. Any remaining mount, including one whose
            # export died, must be drained explicitly; silently reusing it risks
            # mixing a failed filesystem with a newly bound block device.
            if any(mounted(path) for path in self.mounts.iterdir()):
                raise RuntimeError("environment backend lost with retained mounts; fence and drain affected sandboxes")
            self.cache = VerifiedEnvironmentCache(self.root / "cache", registry, max_bytes=cache_bytes,
                                                  **(cache_options or {}))
            # Node-local and disposable like the cache; replaceable for C2.7.
            self.traces = traces if traces is not None else LocalTraceStore(self.root / "traces")
            self.workers = EnvironmentReadWorkers()
        except BaseException:
            os.close(self._lock_fd)
            raise
        self._devices = tuple(devices) if devices is not None else block_devices()
        self._factory = device_factory
        self._mount = mount or (lambda dev, target: subprocess.run(
            ["mount", "-t", "erofs", "-o", "ro", str(dev), str(target)], check=True))
        self._unmount = unmount or (lambda target: subprocess.run(["umount", str(target)], check=True))
        self._mounted = mounted
        self._referenced = referenced
        self._guard = RLock()
        self._active = {}
        self._components = {}  # Attached digest -> authenticated component.
        # Attached digest -> (metadata job, ready deadline). Every caller of
        # ensure, not only the attaching one, waits until that deadline.
        self._warming = {}
        self._closed = False
        self.prefetch = prefetch
        # Not the attach guard: heartbeats read metrics while an attach holds
        # it across a registry load and mount.
        self._counter_guard = Lock()
        self._counters = {name: 0 for name in (
            "metadata_hint_present", "metadata_hint_absent", "metadata_hint_unsupported",
            "trace_hint_present", "trace_hint_absent", "trace_hint_invalid", "trace_recordings_started",
            "metadata_prefetch_wait_timeouts", "prefetch_start_failures")}

    def _count(self, name):
        with self._counter_guard:
            self._counters[name] += 1

    def metrics(self):
        """Exactly models.ENVIRONMENT_IO_METRICS, exported in node heartbeats."""
        with self._counter_guard:
            counters = dict(self._counters)
        # A single len() read of the attach-guarded map: at most one attach stale.
        return self.cache.metrics() | counters | {"active_components": len(self._active),
                                                  "prefetch_enabled": self.prefetch.enabled}

    def ensure(self, digest):
        """Attach and mount a component, warming its metadata before return.

        Waiting is bounded by the prefetch policy and never fails the attach.
        """
        target = self._attach(digest)
        with self._guard:
            job, ready_by = self._warming.get(digest, (None, 0.0))
        remaining = ready_by - time.monotonic()
        # Outside the guard: the threaded RPC server lets other components'
        # attach, liveness checks and drops proceed meanwhile.
        if job is not None and remaining > 0 and not job.wait(remaining):
            self._count("metadata_prefetch_wait_timeouts")
        return target

    def _start_prefetch(self, digest, component):
        """Schedule hint and trace prefetch; returns the metadata job, if any."""
        policy, budget = self.prefetch, self.cache.max_bytes // 4
        lookup = getattr(self.registry, "metadata_hint", None)
        status, hint = lookup(digest) if lookup is not None else ("absent", None)
        self._count(f"metadata_hint_{status}")
        metadata = None
        if hint is not None:
            max_bytes = min(policy.metadata_bytes, budget)
            metadata = self.cache.prefetch(
                component, hint.prefetch_order(max_bytes=max_bytes, max_chunks=max_bytes // CHUNK_BYTES),
                kind="metadata", max_bytes=max_bytes, deadline_seconds=policy.metadata_seconds)
        status, trace = self.traces.load(component)
        self._count(f"trace_hint_{status}")
        if trace is not None:
            # Lower priority than metadata on the same bounded miss pool.
            self.cache.prefetch(component, trace_order(trace), kind="trace",
                                max_bytes=min(policy.trace_bytes, budget), deadline_seconds=policy.trace_seconds)
        elif self.cache.record_startup(component, self.traces.save, window_seconds=policy.trace_window_seconds,
                                       max_chunks=policy.trace_window_chunks):
            self._count("trace_recordings_started")
        return metadata

    def _attach(self, digest):
        require_digest(digest)
        with self._guard:
            if self._closed:
                raise RuntimeError("environment backend is closed")
            if digest in self._active:
                if not getattr(self._active[digest], "healthy", True):
                    raise RuntimeError("environment block export failed; fence and drain affected sandboxes")
                return str(self.mounts / digest[7:])
            component = self.registry.load(digest)  # Signature before any privileged operation.
            target = self.mounts / digest[7:]
            _private_directory(target)
            # Durable physical ownership marker, deliberately no image reference
            # count. Registry leases and the rootfs manager govern retention.
            receipt = self.root / (digest[7:] + ".json")
            with receipt.open("wb") as stream:
                stream.write(canonical_bytes({"component": digest, "pid": os.getpid()}))
                stream.flush()
                os.fsync(stream.fileno())
            directory_fd = os.open(self.root, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            selected = None
            for device in self._devices:
                try:
                    selected = self._factory(device, component, self.cache, self.workers,
                                             trusted_keys=self.registry.trusted_keys)
                    break
                except OSError as exc:
                    if exc.errno != errno.EBUSY:
                        raise
            if selected is None:
                raise RuntimeError(NO_BLOCK_DEVICE)
            # Record ownership before invoking mount: an interrupted command
            # can have mounted successfully even if no acknowledgment arrived.
            self._active[digest] = selected
            self._components[digest] = component
            metadata = None
            if self.prefetch.enabled:
                # Concurrent with the mount, whose superblock read joins the
                # first bulk range. A hint is never a reason to fail attach.
                try:
                    metadata = self._start_prefetch(digest, component)
                except Exception:
                    self._count("prefetch_start_failures")
                    _LOG.warning("environment prefetch for %s did not start", digest, exc_info=True)
            try:
                self._mount(selected.path, target)
            except BaseException:
                # A failed attach is no startup trace: discard it before drop.
                self.cache.stop_recording(component)
                # Never disconnect a possibly mounted backing device. If
                # unmount/close cannot finish, retain it for a later cleanup.
                try:
                    self.drop(digest)
                except Exception:
                    pass
                raise
            if metadata is not None:
                self._warming[digest] = (metadata, time.monotonic() + self.prefetch.metadata_wait_seconds)
            return str(target)

    def drop(self, digest):
        require_digest(digest)
        with self._guard:
            target = self.mounts / digest[7:]
            device = self._active.get(digest)
            if device is None:
                return not self._mounted(target)
            if self._mounted(target):
                if self._referenced(target):
                    return False
                try:
                    self._unmount(target)
                except (OSError, subprocess.CalledProcessError):
                    return False
            # A failed close may be retried after unmount already succeeded;
            # do not inspect the now-unmounted directory's parent filesystem.
            component = self._components.get(digest)
            if component is not None:
                # Detached, often inside the trace window: save what was read.
                self.cache.cancel_prefetch(component)
                self.cache.stop_recording(component, save=True)
            device.close()
            del self._active[digest]
            self._components.pop(digest, None)
            self._warming.pop(digest, None)
            target.rmdir()
            (self.root / (digest[7:] + ".json")).unlink(missing_ok=True)
            return True

    def close(self):
        with self._guard:
            for digest in tuple(self._active):
                if not self.drop(digest):
                    raise RuntimeError("environment backend still has live filesystem users")
            self._closed = True
            self.workers.close()
            self.cache.close()
            if self._lock_fd is not None:
                os.close(self._lock_fd)
                self._lock_fd = None


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(60)
        # Filesystem mode600 limits the endpoint; peer credentials prevent
        # another uid reaching it through accidentally relaxed directory modes.
        if hasattr(socket, "SO_PEERCRED"):
            import struct
            _, uid, _ = struct.unpack("3i", self.connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid != os.geteuid():
                return
        try:
            raw = self.rfile.readline(MAX_RPC_BYTES + 1)
            if len(raw) > MAX_RPC_BYTES or not raw.endswith(b"\n"):
                raise ValueError("environment request exceeds its bound")
            request = json.loads(raw)
            method = request.get("method") if isinstance(request, dict) else None
            if method == "metrics" and set(request) == {"method"}:
                result = self.server.backend.metrics()
            elif method in ("ensure", "drop") and set(request) == {"method", "digest"}:
                result = getattr(self.server.backend, method)(request["digest"])
            else:
                raise ValueError("invalid environment backend request")
            response = {"result": result}
        except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as exc:
            response = {"error": str(exc)}
        self.wfile.write(canonical_bytes(response) + b"\n")


class EnvironmentBackendServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    """Lifecycle RPC; block reads use separate bounded worker pools.

    The backend guard serializes attach and drop. A thread per request only
    keeps one attach's bounded metadata wait from delaying every other
    ensure (each composition's liveness check) and drop on the node.
    """
    daemon_threads = True
    # A create burst opens one connection per composition at once. The
    # socketserver default of 5 refuses the 7th pending AF_UNIX connect with
    # EAGAIN; the kernel caps this at net.core.somaxconn.
    request_queue_size = 1024

    def __init__(self, path, backend):
        self.backend = backend
        path = Path(path)
        _private_directory(path.parent)
        if path.exists():
            if not stat.S_ISSOCK(path.lstat().st_mode):
                raise ValueError("environment endpoint is not a socket")
            path.unlink()  # Backend owner flock is already held.
        super().__init__(str(path), _Handler)
        path.chmod(0o600)


class EnvironmentBackendClient:
    """One connection per call. Image-store leases, not connections, own mounts."""

    def __init__(self, path, *, timeout=120):
        self.path, self.timeout = str(path), timeout

    def _call(self, request, timeout=None):
        timeout = self.timeout if timeout is None else timeout
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.settimeout(timeout)
            self._connect(stream, time.monotonic() + timeout)
            stream.sendall(canonical_bytes(request) + b"\n")
            with stream.makefile("rb") as reader:
                raw = reader.readline(MAX_RPC_BYTES + 1)
            if len(raw) > MAX_RPC_BYTES or not raw.endswith(b"\n"):
                raise ValueError("invalid environment backend response size")
            response = json.loads(raw)
            if "error" in response:
                raise RuntimeError(response["error"])
            return response["result"]

    def _connect(self, stream, deadline):
        # A timeout makes the socket non-blocking, and a non-blocking AF_UNIX
        # connect to a full accept queue fails with EAGAIN instead of waiting.
        # The queue drains as the server accepts, so retry within the deadline.
        delay = 0.005
        while True:
            try:
                return stream.connect(self.path)
            except BlockingIOError:
                if time.monotonic() + delay >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.1)

    def ensure(self, digest):
        return Path(self._call({"method": "ensure", "digest": require_digest(digest)}))

    def drop(self, digest):
        return self._call({"method": "drop", "digest": require_digest(digest)})

    def metrics(self):
        return self._call({"method": "metrics"}, METRICS_TIMEOUT_SECONDS)


def serve_backend(registry, *, root, socket_path, cache_bytes=1024 ** 3, prefetch=True):
    if os.geteuid() != 0 or registry is None:
        raise ValueError("the artifact I/O backend requires root and registry trust")
    backend = EnvironmentBackend(root, registry, cache_bytes=cache_bytes, prefetch=PrefetchPolicy(enabled=prefetch))
    try:
        with EnvironmentBackendServer(socket_path, backend) as server:
            server.serve_forever()
    finally:
        backend.close()

