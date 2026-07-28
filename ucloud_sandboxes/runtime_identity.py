from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Iterator

from .hibernation import HibernationRuntimeFingerprint


DIRECT_RUNTIME_KIND = "direct-runsc-v1"
DIRECT_STATE_SCHEMA = "direct-node-v1"
DIRECT_ROOTFS_FORMAT = "docker-export-overlay-v2"
DIRECT_QUOTA_LAYOUT = "unified-xfs-project-v1"
RUNTIME_IDENTITY_VERSION = 1
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


class RuntimeIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class NodeRuntimeIdentity:
    runsc_sha256: str
    runsc_commit: str
    boot_config_sha256: str
    kind: str = DIRECT_RUNTIME_KIND
    state_schema: str = DIRECT_STATE_SCHEMA
    rootfs_format: str = DIRECT_ROOTFS_FORMAT
    quota_layout: str = DIRECT_QUOTA_LAYOUT
    version: int = RUNTIME_IDENTITY_VERSION

    def __post_init__(self) -> None:
        if self.version != RUNTIME_IDENTITY_VERSION:
            raise ValueError("unsupported node runtime identity version")
        if self.kind != DIRECT_RUNTIME_KIND:
            raise ValueError("node runtime kind must be direct-runsc-v1")
        if self.state_schema != DIRECT_STATE_SCHEMA:
            raise ValueError("node runtime state schema is invalid")
        if self.rootfs_format != DIRECT_ROOTFS_FORMAT:
            raise ValueError("node runtime rootfs format is invalid")
        if self.quota_layout != DIRECT_QUOTA_LAYOUT:
            raise ValueError("node runtime quota layout is invalid")
        if not _DIGEST.fullmatch(self.runsc_sha256):
            raise ValueError("node runtime runsc digest is invalid")
        if not _COMMIT.fullmatch(self.runsc_commit):
            raise ValueError("node runtime runsc commit is invalid")
        if not _DIGEST.fullmatch(self.boot_config_sha256):
            raise ValueError("node runtime boot configuration digest is invalid")

    @classmethod
    def from_fingerprint(
        cls,
        fingerprint: HibernationRuntimeFingerprint,
    ) -> NodeRuntimeIdentity:
        return cls(
            runsc_sha256=fingerprint.runsc_sha256,
            runsc_commit=fingerprint.runsc_commit,
            boot_config_sha256=fingerprint.boot_config_sha256,
        )

    @classmethod
    def from_dict(cls, raw: object) -> NodeRuntimeIdentity:
        if not isinstance(raw, dict):
            raise RuntimeIdentityError("node runtime identity must be an object")
        expected = {
            "boot_config_sha256",
            "kind",
            "quota_layout",
            "rootfs_format",
            "runsc_commit",
            "runsc_sha256",
            "state_schema",
            "version",
        }
        if set(raw) != expected:
            raise RuntimeIdentityError("node runtime identity schema is invalid")
        try:
            return cls(
                boot_config_sha256=str(raw["boot_config_sha256"]),
                kind=str(raw["kind"]),
                quota_layout=str(raw["quota_layout"]),
                rootfs_format=str(raw["rootfs_format"]),
                runsc_commit=str(raw["runsc_commit"]),
                runsc_sha256=str(raw["runsc_sha256"]),
                state_schema=str(raw["state_schema"]),
                version=int(raw["version"]),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeIdentityError("node runtime identity is invalid") from exc

    def to_dict(self) -> dict[str, object]:
        return {
            "boot_config_sha256": self.boot_config_sha256,
            "kind": self.kind,
            "quota_layout": self.quota_layout,
            "rootfs_format": self.rootfs_format,
            "runsc_commit": self.runsc_commit,
            "runsc_sha256": self.runsc_sha256,
            "state_schema": self.state_schema,
            "version": self.version,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()


class NodeRuntimeIdentityStore:
    """Bind a node state directory to exactly one direct runtime identity."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("node runtime identity path must be absolute")
        self.path = path
        self.lock_path = path.with_name(f".{path.name}.lock")

    def bind(self, expected: NodeRuntimeIdentity) -> NodeRuntimeIdentity:
        with self._locked():
            if self.path.exists():
                actual = self._load_unlocked()
                if actual != expected:
                    raise RuntimeIdentityError(
                        "node state belongs to another runtime identity"
                    )
                return actual
            if os.path.lexists(self.path):
                raise RuntimeIdentityError(
                    "node runtime identity path is not a regular file"
                )
            self._atomic_write(_canonical_json(expected.to_dict()) + b"\n")
            return expected

    def load(self) -> NodeRuntimeIdentity | None:
        with self._locked():
            if not os.path.lexists(self.path):
                return None
            return self._load_unlocked()

    def _load_unlocked(self) -> NodeRuntimeIdentity:
        try:
            info = self.path.lstat()
            if not self.path.is_file() or self.path.is_symlink():
                raise RuntimeIdentityError(
                    "node runtime identity must be a regular file"
                )
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise RuntimeIdentityError(
                    "node runtime identity must be private and owned"
                )
            raw = self.path.read_bytes()
            if len(raw) > 16 * 1024:
                raise RuntimeIdentityError("node runtime identity is too large")
            return NodeRuntimeIdentity.from_dict(json.loads(raw.decode("ascii")))
        except RuntimeIdentityError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeIdentityError("node runtime identity is unreadable") from exc

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if (
            not self.path.parent.is_dir()
            or self.path.parent.is_symlink()
            or parent.st_uid != os.geteuid()
            or parent.st_mode & 0o022
        ):
            raise RuntimeIdentityError(
                "node runtime identity directory must be private and owned"
            )
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise RuntimeIdentityError("node runtime identity lock is invalid")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _atomic_write(self, payload: bytes) -> None:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        temporary = Path(raw_path)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(self.path)
            directory = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
