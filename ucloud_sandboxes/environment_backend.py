"""Nodewide privileged artifact I/O, independent of node-agent process lifetime.

This owns kernel mounts/devices only. Image references, retention and scheduling
remain with the existing image store/registry. A restarted backend never adopts
an old live filesystem: lost kernel exports are an explicit admission fence.
"""
from contextlib import contextmanager
import errno
import fcntl
import json
import os
import re
from pathlib import Path
import socket
import socketserver
import stat
import subprocess
from threading import RLock

from .environment_artifact import canonical_bytes, require_digest
from .environment_cache import VerifiedEnvironmentCache
from .environment_nbd import EnvironmentReadWorkers, ReadOnlyEnvironmentDevice

MAX_RPC_BYTES = 64 * 1024


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
            if option.startswith("lowerdir="):
                for lower in option[len("lowerdir="):].split(":"):
                    lower = unescape(lower)
                    if lower == target or lower.startswith(target + "/"):
                        return True
    return False


class EnvironmentBackend:
    def __init__(self, root, registry, *, devices=None, device_factory=ReadOnlyEnvironmentDevice,
                 mount=None, unmount=None, mounted=_mounted, referenced=mount_has_dependents, cache_bytes=1024 ** 3):
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
            self.cache = VerifiedEnvironmentCache(self.root / "cache", registry, max_bytes=cache_bytes)
            self.workers = EnvironmentReadWorkers()
        except BaseException:
            os.close(self._lock_fd)
            raise
        self._devices = tuple(devices) if devices is not None else tuple(sorted(path for path in Path("/dev").glob("nbd[0-9]*") if re.fullmatch(r"nbd[0-9]+", path.name)))
        self._factory = device_factory
        self._mount = mount or (lambda dev, target: subprocess.run(
            ["mount", "-t", "erofs", "-o", "ro", str(dev), str(target)], check=True))
        self._unmount = unmount or (lambda target: subprocess.run(["umount", str(target)], check=True))
        self._mounted = mounted
        self._referenced = referenced
        self._guard = RLock()
        self._active = {}
        self._closed = False

    def ensure(self, digest):
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
                raise RuntimeError("no available environment block device")
            # Record ownership before invoking mount: an interrupted command
            # can have mounted successfully even if no acknowledgment arrived.
            self._active[digest] = selected
            try:
                self._mount(selected.path, target)
            except BaseException:
                # Never disconnect a possibly mounted backing device. If
                # unmount/close cannot finish, retain it for a later cleanup.
                try:
                    self.drop(digest)
                except Exception:
                    pass
                raise
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
            device.close()
            del self._active[digest]
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
            if not isinstance(request, dict) or set(request) != {"method", "digest"}:
                raise ValueError("invalid environment backend request")
            method = request["method"]
            if method not in {"ensure", "drop"}:
                raise ValueError("unsupported environment backend method")
            result = getattr(self.server.backend, method)(request["digest"])
            response = {"result": result}
        except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as exc:
            response = {"error": str(exc)}
        self.wfile.write(canonical_bytes(response) + b"\n")


class EnvironmentBackendServer(socketserver.UnixStreamServer):
    """Serialized lifecycle RPC; block reads use separate bounded worker pools."""
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
    def __init__(self, path, *, timeout=120):
        self.path, self.timeout = str(path), timeout

    def _call(self, method, digest):
        require_digest(digest)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.settimeout(self.timeout)
            stream.connect(self.path)
            stream.sendall(canonical_bytes({"method": method, "digest": digest}) + b"\n")
            with stream.makefile("rb") as reader:
                raw = reader.readline(MAX_RPC_BYTES + 1)
            if len(raw) > MAX_RPC_BYTES or not raw.endswith(b"\n"):
                raise ValueError("invalid environment backend response size")
            response = json.loads(raw)
            if "error" in response:
                raise RuntimeError(response["error"])
            return response["result"]

    def ensure(self, digest):
        return Path(self._call("ensure", digest))

    def drop(self, digest):
        return self._call("drop", digest)

    @contextmanager
    def mounted(self, digest):
        # Lifecycle ownership belongs to the image-store lease, not this short
        # frontend RPC connection. Closing the connection never unmounts bytes.
        yield self.ensure(digest)


def serve_backend(registry, *, root, socket_path, cache_bytes=1024 ** 3):
    if os.geteuid() != 0 or registry is None:
        raise ValueError("the artifact I/O backend requires root and registry trust")
    backend = EnvironmentBackend(root, registry, cache_bytes=cache_bytes)
    try:
        with EnvironmentBackendServer(socket_path, backend) as server:
            server.serve_forever()
    finally:
        backend.close()


def main(argv=None):
    import argparse
    from .environment_config import configured_environment_registry
    parser = argparse.ArgumentParser(description="Nodewide authenticated immutable environment I/O")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--trusted-keys", required=True, type=Path)
    parser.add_argument("--cache-bytes", type=int, default=1024 ** 3)
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        parser.error("the artifact I/O backend requires root")
    registry = configured_environment_registry(args.registry_url, args.repository, args.trusted_keys)
    serve_backend(registry, root=args.root, socket_path=args.socket, cache_bytes=args.cache_bytes)


if __name__ == "__main__":
    main()
