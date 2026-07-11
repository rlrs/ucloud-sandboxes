from __future__ import annotations

from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, Callable, Sequence
import uuid


DEFAULT_CONFIG_PATH = "/etc/ucloud-sandboxes/checkpoint-helper.json"
HELPER_VERSION = 1
MANIFEST_VERSION = 1
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_TREE_ENTRIES = 100_000
MIB = 1024 * 1024
# A runsc save contains the sandbox's resident memory, writable rootfs state,
# bounded tmpfs mounts, and serialized kernel metadata. Reserve every enforced
# writable bound plus a second memory allowance and fixed metadata margin;
# ``seal`` rejects an artifact that exceeds this declared upper bound.
CHECKPOINT_MEMORY_MULTIPLIER = 2
CHECKPOINT_FIXED_OVERHEAD_BYTES = 64 * MIB
MAX_SOURCE_MEMORY_MB = 16 * 1024 * 1024
MAX_SOURCE_DISK_MB = 16 * 1024 * 1024
MAX_TMPFS_MB = 16 * 1024 * 1024

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_UUID_HEX = r"[0-9a-f]{32}"
_ROOT_TEMP = re.compile(
    rf"\.(?:prepare|ucloud-drop)-[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}-{_UUID_HEX}\Z"
)
_CHECKPOINT_TEMP = re.compile(
    rf"\.ucloud-(?:stage|unstage)-[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}-{_UUID_HEX}\Z"
)
_ATOMIC_TEMP = re.compile(
    rf"(?:\.manifest|\.\.prepare|\.\.complete|\.\.integrity)\.json\.tmp-{_UUID_HEX}\Z"
)
_STAGED_MARKER = re.compile(
    r"([0-9a-f]{64})-([A-Za-z0-9][A-Za-z0-9_.-]{0,127})\.json\Z"
)
_STAGED_MARKER_TEMP = re.compile(
    rf"\.([0-9a-f]{{64}})-([A-Za-z0-9][A-Za-z0-9_.-]{{0,127}})"
    rf"\.json\.tmp-{_UUID_HEX}\Z"
)
_APPLICATION_TRASH = re.compile(
    rf"\.ucloud-app-drop-[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}-{_UUID_HEX}\Z"
)
_RESERVED_ARTIFACT_IDS = {"application"}


class HelperError(RuntimeError):
    pass


class ArtifactNotReady(HelperError):
    pass


class ArtifactMissing(HelperError):
    pass


@dataclass(frozen=True)
class HelperConfig:
    docker_root: Path
    checkpoint_root: Path


CopyTree = Callable[[Path, Path], None]


def render_checkpoint_helper_script(*, config_path: str = DEFAULT_CONFIG_PATH) -> str:
    """Render this module as a dependency-free privileged helper."""
    if not config_path.startswith("/") or "\n" in config_path or "\r" in config_path:
        raise ValueError(
            "checkpoint helper config path must be absolute and single-line"
        )
    source = Path(__file__).read_text(encoding="utf-8")
    assignment = f"DEFAULT_CONFIG_PATH = {config_path!r}"
    lines = source.splitlines()
    indexes = [
        index
        for index, line in enumerate(lines)
        if line.startswith("DEFAULT_CONFIG_PATH = ")
    ]
    if len(indexes) != 1:
        raise RuntimeError("checkpoint helper config marker is missing or ambiguous")
    lines[indexes[0]] = assignment
    return "#!/usr/bin/python3\n" + "\n".join(lines) + "\n"


def _validate_safe_id(label: str, value: str) -> str:
    if not _SAFE_ID.fullmatch(value) or value in {".", ".."}:
        raise HelperError(f"{label} must be 1-128 safe ASCII identifier characters")
    return value


def _validate_artifact_id(value: str) -> str:
    value = _validate_safe_id("artifact id", value)
    if value in _RESERVED_ARTIFACT_IDS:
        raise HelperError(f"artifact id is reserved: {value}")
    return value


def _validate_container_id(value: str) -> str:
    if not _CONTAINER_ID.fullmatch(value):
        raise HelperError("container id must be a full lowercase 64-hex Docker id")
    return value


def _validate_image_digest(value: str) -> str:
    digest = value.removeprefix("sha256:")
    if not _DIGEST.fullmatch(digest):
        raise HelperError("source image must be a lowercase SHA-256 digest")
    return f"sha256:{digest}"


def _validate_spec_hash(value: str) -> str:
    if not _DIGEST.fullmatch(value):
        raise HelperError("source spec hash must be a lowercase 64-hex SHA-256 digest")
    return value


def _validate_source_memory_mb(value: Any) -> int:
    return _validate_size_mb(
        "source memory_mb",
        value,
        maximum=MAX_SOURCE_MEMORY_MB,
    )


def _validate_size_mb(label: str, value: Any, *, maximum: int) -> int:
    if isinstance(value, bool):
        raise HelperError(f"{label} must be a positive integer")
    if isinstance(value, int):
        size_mb = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.isdecimal()
        and (value == "0" or not value.startswith("0"))
    ):
        size_mb = int(value)
    else:
        raise HelperError(f"{label} must be a positive integer")
    if not 1 <= size_mb <= maximum:
        raise HelperError(f"{label} must be in [1, {maximum}]")
    return size_mb


def _validate_source_disk_mb(value: Any) -> int:
    return _validate_size_mb(
        "source disk_mb",
        value,
        maximum=MAX_SOURCE_DISK_MB,
    )


def _validate_tmpfs_mb(label: str, value: Any) -> int:
    return _validate_size_mb(label, value, maximum=MAX_TMPFS_MB)


def checkpoint_reservation_bytes(
    source_memory_mb: Any,
    source_disk_mb: Any,
    tmpfs_mb: Any,
    run_tmpfs_mb: Any,
) -> int:
    memory_mb = _validate_source_memory_mb(source_memory_mb)
    disk_mb = _validate_source_disk_mb(source_disk_mb)
    tmp_mb = _validate_tmpfs_mb("tmpfs_mb", tmpfs_mb)
    run_mb = _validate_tmpfs_mb("run_tmpfs_mb", run_tmpfs_mb)
    return (
        memory_mb * MIB * CHECKPOINT_MEMORY_MULTIPLIER
        + disk_mb * MIB
        + tmp_mb * MIB
        + run_mb * MIB
        + CHECKPOINT_FIXED_OVERHEAD_BYTES
    )


def _validated_absolute_path(label: str, value: Any) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        raise HelperError(f"{label} must be an absolute path")
    path = Path(value)
    if ".." in path.parts or str(path) != os.path.normpath(value):
        raise HelperError(f"{label} must be normalized and cannot contain '..'")
    return path


def _check_directory(
    path: Path,
    label: str,
    *,
    root_owned: bool,
    writable_by_owner: bool = True,
) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise HelperError(f"{label} does not exist") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise HelperError(f"{label} must be a real directory")
    if root_owned and info.st_uid != 0:
        raise HelperError(f"{label} must be owned by root")
    if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise HelperError(f"{label} cannot be group/world writable")
    if writable_by_owner and not info.st_mode & stat.S_IWUSR:
        raise HelperError(f"{label} must be owner writable")
    return info


def _check_no_symlink_components(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode):
            raise HelperError(f"path component is a symlink: {current}")


def load_config(
    path: str | Path = DEFAULT_CONFIG_PATH,
    *,
    require_root_ownership: bool = True,
) -> HelperConfig:
    config_path = Path(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(config_path, flags)
    except OSError as exc:
        raise HelperError(f"cannot open checkpoint helper config: {exc}") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise HelperError("checkpoint helper config must be a regular file")
        if require_root_ownership and info.st_uid != 0:
            raise HelperError("checkpoint helper config must be owned by root")
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise HelperError("checkpoint helper config cannot be group/world writable")
        chunks: list[bytes] = []
        remaining = MAX_JSON_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > MAX_JSON_BYTES:
            raise HelperError("checkpoint helper config is too large")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HelperError("checkpoint helper config is invalid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "version",
        "docker_root",
        "checkpoint_root",
    }:
        raise HelperError("checkpoint helper config has an invalid schema")
    if payload["version"] != HELPER_VERSION or isinstance(payload["version"], bool):
        raise HelperError("checkpoint helper config has an unsupported version")
    docker_root = _validated_absolute_path("docker root", payload["docker_root"])
    checkpoint_root = _validated_absolute_path(
        "checkpoint root", payload["checkpoint_root"]
    )
    if (
        checkpoint_root.parent != docker_root
        or checkpoint_root.name != "ucloud-checkpoints"
    ):
        raise HelperError("checkpoint root must be DockerRootDir/ucloud-checkpoints")
    _check_no_symlink_components(docker_root)
    _check_no_symlink_components(checkpoint_root)
    _check_directory(
        docker_root,
        "docker root",
        root_owned=require_root_ownership,
    )
    _check_directory(
        checkpoint_root,
        "checkpoint root",
        root_owned=require_root_ownership,
    )
    return HelperConfig(docker_root=docker_root, checkpoint_root=checkpoint_root)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _read_json(path: Path) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise HelperError(f"cannot open manifest {path.name}: {exc}") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise HelperError(f"manifest {path.name} is not a regular file")
        chunks: list[bytes] = []
        remaining = MAX_JSON_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > MAX_JSON_BYTES:
            raise HelperError(f"manifest {path.name} is too large")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HelperError(f"manifest {path.name} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HelperError(f"manifest {path.name} must be a JSON object")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _tree_manifest(root: Path) -> list[dict[str, Any]]:
    _check_directory(root, "checkpoint tree", root_owned=False)
    entries: list[dict[str, Any]] = []
    for current_raw, directories, files in os.walk(root, followlinks=False):
        current = Path(current_raw)
        for name in sorted([*directories, *files]):
            path = current / name
            try:
                info = path.lstat()
            except FileNotFoundError as exc:
                raise HelperError(
                    "checkpoint tree changed while it was inspected"
                ) from exc
            relative = path.relative_to(root)
            relative_text = PurePosixPath(*relative.parts).as_posix()
            if relative_text.startswith("/") or ".." in relative.parts:
                raise HelperError("checkpoint tree contains an unsafe path")
            if stat.S_ISDIR(info.st_mode):
                kind = "directory"
                size = 0
            elif stat.S_ISREG(info.st_mode):
                kind = "file"
                size = info.st_size
            else:
                raise HelperError(
                    f"checkpoint tree contains unsupported entry: {relative_text}"
                )
            entries.append(
                {
                    "path": relative_text,
                    "kind": kind,
                    "mode": stat.S_IMODE(info.st_mode),
                    "size": size,
                }
            )
            if len(entries) > MAX_TREE_ENTRIES:
                raise HelperError("checkpoint tree contains too many entries")
    if not entries:
        raise HelperError("checkpoint tree is empty")
    entries.sort(key=lambda item: item["path"])
    return entries


def _validate_tree_entries(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value or len(value) > MAX_TREE_ENTRIES:
        raise HelperError("manifest tree is invalid")
    result: list[dict[str, Any]] = []
    previous = ""
    for raw in value:
        if not isinstance(raw, dict) or set(raw) != {"path", "kind", "mode", "size"}:
            raise HelperError("manifest tree entry has an invalid schema")
        path = raw["path"]
        kind = raw["kind"]
        mode = raw["mode"]
        size = raw["size"]
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or ".." in PurePosixPath(path).parts
            or PurePosixPath(path).as_posix() != path
            or path <= previous
        ):
            raise HelperError("manifest tree entry has an unsafe or unsorted path")
        if kind not in {"directory", "file"}:
            raise HelperError("manifest tree entry has an invalid kind")
        if (
            not isinstance(mode, int)
            or isinstance(mode, bool)
            or not 0 <= mode <= 0o7777
        ):
            raise HelperError("manifest tree entry has an invalid mode")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise HelperError("manifest tree entry has an invalid size")
        if kind == "directory" and size != 0:
            raise HelperError("manifest directory entry has a nonzero size")
        result.append({"path": path, "kind": kind, "mode": mode, "size": size})
        previous = path
    return result


def _validate_preparation(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    expected = {
        "version",
        "state",
        "artifact_id",
        "source_container_id",
        "source_image_id",
        "source_spec_hash",
        "checkpoint_id",
        "source_memory_mb",
        "source_disk_mb",
        "tmpfs_mb",
        "run_tmpfs_mb",
        "reserved_bytes",
        "created_ns",
    }
    if set(payload) != expected:
        raise HelperError("preparation manifest has an invalid schema")
    if payload["version"] != MANIFEST_VERSION or payload["state"] != "preparing":
        raise HelperError("preparation manifest has an invalid version or state")
    if payload["artifact_id"] != artifact_id:
        raise HelperError("preparation manifest artifact id does not match")
    _validate_artifact_id(payload["artifact_id"])
    _validate_container_id(payload["source_container_id"])
    if _validate_image_digest(payload["source_image_id"]) != payload["source_image_id"]:
        raise HelperError("preparation manifest image digest is not canonical")
    _validate_spec_hash(payload["source_spec_hash"])
    _validate_safe_id("checkpoint id", payload["checkpoint_id"])
    source_memory_mb = _validate_source_memory_mb(payload["source_memory_mb"])
    source_disk_mb = _validate_source_disk_mb(payload["source_disk_mb"])
    tmpfs_mb = _validate_tmpfs_mb("tmpfs_mb", payload["tmpfs_mb"])
    run_tmpfs_mb = _validate_tmpfs_mb("run_tmpfs_mb", payload["run_tmpfs_mb"])
    if payload["reserved_bytes"] != checkpoint_reservation_bytes(
        source_memory_mb,
        source_disk_mb,
        tmpfs_mb,
        run_tmpfs_mb,
    ):
        raise HelperError("preparation manifest has an invalid storage reservation")
    created_ns = payload["created_ns"]
    if (
        not isinstance(created_ns, int)
        or isinstance(created_ns, bool)
        or created_ns <= 0
    ):
        raise HelperError("preparation manifest has an invalid timestamp")
    return payload


def _validate_completion(
    payload: dict[str, Any], preparation: dict[str, Any]
) -> dict[str, Any]:
    expected = {
        "version",
        "state",
        "artifact_id",
        "source_container_id",
        "source_image_id",
        "source_spec_hash",
        "checkpoint_id",
        "source_memory_mb",
        "source_disk_mb",
        "tmpfs_mb",
        "run_tmpfs_mb",
        "reserved_bytes",
        "created_ns",
        "completed_ns",
    }
    if set(payload) != expected:
        raise HelperError("completion marker has an invalid schema")
    if payload["version"] != MANIFEST_VERSION or payload["state"] != "complete":
        raise HelperError("completion marker has an invalid version or state")
    for field in (
        "artifact_id",
        "source_container_id",
        "source_image_id",
        "source_spec_hash",
        "checkpoint_id",
        "source_memory_mb",
        "source_disk_mb",
        "tmpfs_mb",
        "run_tmpfs_mb",
        "reserved_bytes",
        "created_ns",
    ):
        if payload[field] != preparation[field]:
            raise HelperError(f"completion marker has mismatched {field}")
    completed_ns = payload["completed_ns"]
    if (
        not isinstance(completed_ns, int)
        or isinstance(completed_ns, bool)
        or completed_ns < preparation["created_ns"]
    ):
        raise HelperError("completion marker has an invalid timestamp")
    return payload


def _validate_manifest(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    expected = {
        "artifact_id",
        "source_container_id",
        "source_image_id",
        "source_spec_hash",
        "checkpoint_id",
    }
    if set(payload) != expected:
        raise HelperError("sealed manifest has an invalid schema")
    if payload["artifact_id"] != artifact_id:
        raise HelperError("sealed manifest artifact id does not match")
    _validate_artifact_id(payload["artifact_id"])
    _validate_container_id(payload["source_container_id"])
    if _validate_image_digest(payload["source_image_id"]) != payload["source_image_id"]:
        raise HelperError("sealed manifest image digest is not canonical")
    _validate_spec_hash(payload["source_spec_hash"])
    _validate_safe_id("checkpoint id", payload["checkpoint_id"])
    return payload


def _validate_integrity(payload: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "version",
        "created_ns",
        "sealed_ns",
        "source_memory_mb",
        "source_disk_mb",
        "tmpfs_mb",
        "run_tmpfs_mb",
        "reserved_bytes",
        "file_count",
        "byte_size",
        "tree",
    }
    if set(payload) != expected:
        raise HelperError("sealed manifest has an invalid schema")
    if payload["version"] != MANIFEST_VERSION or isinstance(payload["version"], bool):
        raise HelperError("sealed integrity metadata has an invalid version")
    for field in (
        "created_ns",
        "sealed_ns",
        "source_memory_mb",
        "source_disk_mb",
        "tmpfs_mb",
        "run_tmpfs_mb",
        "reserved_bytes",
        "file_count",
        "byte_size",
    ):
        value = payload[field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise HelperError(f"sealed integrity metadata has invalid {field}")
    if payload["created_ns"] <= 0 or payload["sealed_ns"] < payload["created_ns"]:
        raise HelperError("sealed integrity metadata timestamps are invalid")
    if payload["reserved_bytes"] != checkpoint_reservation_bytes(
        payload["source_memory_mb"],
        payload["source_disk_mb"],
        payload["tmpfs_mb"],
        payload["run_tmpfs_mb"],
    ):
        raise HelperError("sealed integrity metadata has an invalid reservation")
    tree = _validate_tree_entries(payload["tree"])
    file_count = sum(entry["kind"] == "file" for entry in tree)
    byte_size = sum(entry["size"] for entry in tree if entry["kind"] == "file")
    if payload["file_count"] != file_count or payload["byte_size"] != byte_size:
        raise HelperError("sealed integrity metadata totals do not match its tree")
    if byte_size > payload["reserved_bytes"]:
        raise HelperError("sealed checkpoint exceeds its storage reservation")
    return payload


def _validate_staged_marker(payload: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "version",
        "state",
        "artifact_id",
        "target_container_id",
        "checkpoint_id",
        "created_ns",
        "updated_ns",
    }
    if set(payload) != expected:
        raise HelperError("staged checkpoint marker has an invalid schema")
    if payload["version"] != MANIFEST_VERSION or isinstance(payload["version"], bool):
        raise HelperError("staged checkpoint marker has an invalid version")
    if payload["state"] not in {"staging", "staged"}:
        raise HelperError("staged checkpoint marker has an invalid state")
    _validate_artifact_id(payload["artifact_id"])
    _validate_container_id(payload["target_container_id"])
    _validate_safe_id("checkpoint id", payload["checkpoint_id"])
    for field in ("created_ns", "updated_ns"):
        value = payload[field]
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise HelperError(f"staged checkpoint marker has invalid {field}")
    if payload["updated_ns"] < payload["created_ns"]:
        raise HelperError("staged checkpoint marker timestamps are invalid")
    return payload


def _reflink_copy(source: Path, target: Path) -> None:
    try:
        subprocess.run(
            [
                "/bin/cp",
                "--archive",
                "--reflink=always",
                "--no-target-directory",
                "--",
                str(source),
                str(target),
            ],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = (
            exc.stderr.strip()
            if isinstance(exc, subprocess.CalledProcessError)
            else str(exc)
        )
        raise HelperError(f"XFS reflink copy failed: {detail}") from exc


class CheckpointHelper:
    def __init__(
        self,
        config: HelperConfig,
        *,
        require_root_ownership: bool = True,
        copy_tree: CopyTree = _reflink_copy,
    ) -> None:
        self.config = config
        self.require_root_ownership = require_root_ownership
        self.copy_tree = copy_tree
        self._validate_roots()

    def _validate_roots(self) -> None:
        if (
            self.config.checkpoint_root.parent != self.config.docker_root
            or self.config.checkpoint_root.name != "ucloud-checkpoints"
        ):
            raise HelperError(
                "checkpoint root must be DockerRootDir/ucloud-checkpoints"
            )
        _check_no_symlink_components(self.config.docker_root)
        _check_no_symlink_components(self.config.checkpoint_root)
        _check_directory(
            self.config.docker_root,
            "docker root",
            root_owned=self.require_root_ownership,
        )
        _check_directory(
            self.config.checkpoint_root,
            "checkpoint root",
            root_owned=self.require_root_ownership,
        )

    def _lock(self) -> Any:
        lock_path = self.config.checkpoint_root / ".helper.lock"
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return os.fdopen(descriptor, "r+")

    def _artifact(self, artifact_id: str, *, must_exist: bool) -> Path:
        artifact_id = _validate_artifact_id(artifact_id)
        path = self.config.checkpoint_root / artifact_id
        if path.parent != self.config.checkpoint_root:
            raise HelperError("artifact path escaped checkpoint root")
        if must_exist:
            _check_directory(
                path,
                "artifact",
                root_owned=self.require_root_ownership,
            )
        return path

    def _application_root(self, *, create: bool) -> Path:
        path = self.config.checkpoint_root / "application"
        if create and not os.path.lexists(path):
            path.mkdir(mode=0o700)
            _fsync_directory(self.config.checkpoint_root)
        if os.path.lexists(path):
            _check_directory(
                path,
                "application checkpoint root",
                root_owned=self.require_root_ownership,
            )
        return path

    def _staged_root(self, *, create: bool) -> Path:
        path = self.config.checkpoint_root / ".staged"
        if create and not os.path.lexists(path):
            path.mkdir(mode=0o700)
            _fsync_directory(self.config.checkpoint_root)
        if os.path.lexists(path):
            _check_directory(
                path,
                "staged checkpoint registry",
                root_owned=self.require_root_ownership,
            )
        return path

    def _staged_marker_path(
        self,
        target_container_id: str,
        checkpoint_id: str,
        *,
        create_root: bool,
    ) -> Path:
        target_container_id = _validate_container_id(target_container_id)
        checkpoint_id = _validate_safe_id("checkpoint id", checkpoint_id)
        root = self._staged_root(create=create_root)
        return root / f"{target_container_id}-{checkpoint_id}.json"

    def _read_staged_marker(
        self,
        target_container_id: str,
        checkpoint_id: str,
    ) -> dict[str, Any] | None:
        path = self._staged_marker_path(
            target_container_id,
            checkpoint_id,
            create_root=False,
        )
        if not os.path.lexists(path):
            return None
        marker = _validate_staged_marker(_read_json(path))
        if (
            marker["target_container_id"] != target_container_id
            or marker["checkpoint_id"] != checkpoint_id
            or path.name
            != f"{marker['target_container_id']}-{marker['checkpoint_id']}.json"
        ):
            raise HelperError("staged checkpoint marker identity does not match")
        return marker

    def _container(
        self, container_id: str, *, allow_missing: bool = False
    ) -> Path | None:
        container_id = _validate_container_id(container_id)
        containers = self.config.docker_root / "containers"
        _check_directory(
            containers,
            "Docker containers directory",
            root_owned=self.require_root_ownership,
        )
        path = containers / container_id
        try:
            _check_directory(
                path,
                "Docker container metadata directory",
                root_owned=self.require_root_ownership,
            )
        except HelperError:
            if allow_missing and not os.path.lexists(path):
                return None
            raise
        return path

    def _remove_gc_directory(self, path: Path, label: str) -> None:
        _check_directory(
            path,
            label,
            root_owned=self.require_root_ownership,
        )
        shutil.rmtree(path)
        _fsync_directory(path.parent)

    def _gc_locked(self) -> dict[str, Any]:
        """Remove only abandoned names created by this helper.

        The global helper lock proves no live helper invocation owns these
        temporary paths.  Pending and sealed artifacts are intentionally not
        age-collected: their lifecycle is fenced by the unprivileged state
        store and they must retain distinct pending/absent replay semantics.
        """

        removed_root_temps = 0
        removed_checkpoint_temps = 0
        removed_manifest_temps = 0
        removed_application_temps = 0
        removed_staged_marker_temps = 0
        for entry in tuple(self.config.checkpoint_root.iterdir()):
            if _ROOT_TEMP.fullmatch(entry.name):
                self._remove_gc_directory(entry, "checkpoint helper temporary")
                removed_root_temps += 1

        # Atomic JSON writes can leave a uniquely named 0600 file if the
        # helper process is killed between write and replace.
        for artifact in tuple(self.config.checkpoint_root.iterdir()):
            if not _SAFE_ID.fullmatch(artifact.name) or artifact.name in {
                ".",
                "..",
                "application",
            }:
                continue
            try:
                info = artifact.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                continue
            for entry in tuple(artifact.iterdir()):
                if not _ATOMIC_TEMP.fullmatch(entry.name):
                    continue
                info = entry.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or stat.S_ISLNK(info.st_mode)
                    or (self.require_root_ownership and info.st_uid != 0)
                    or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
                ):
                    raise HelperError(
                        "checkpoint manifest temporary has unsafe ownership or type"
                    )
                entry.unlink()
                _fsync_directory(artifact)
                removed_manifest_temps += 1

        application_root = self._application_root(create=False)
        if os.path.lexists(application_root):
            for entry in tuple(application_root.iterdir()):
                if not _APPLICATION_TRASH.fullmatch(entry.name):
                    continue
                self._remove_gc_directory(entry, "application checkpoint trash")
                removed_application_temps += 1

        staged_root = self._staged_root(create=False)
        if os.path.lexists(staged_root):
            for entry in tuple(staged_root.iterdir()):
                if not _STAGED_MARKER_TEMP.fullmatch(entry.name):
                    continue
                info = entry.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or stat.S_ISLNK(info.st_mode)
                    or (self.require_root_ownership and info.st_uid != 0)
                    or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
                ):
                    raise HelperError(
                        "staged marker temporary has unsafe ownership or type"
                    )
                entry.unlink()
                _fsync_directory(staged_root)
                removed_staged_marker_temps += 1

        containers = self.config.docker_root / "containers"
        _check_directory(
            containers,
            "Docker containers directory",
            root_owned=self.require_root_ownership,
        )
        for container in tuple(containers.iterdir()):
            if not _CONTAINER_ID.fullmatch(container.name):
                continue
            _check_directory(
                container,
                "Docker container metadata directory",
                root_owned=self.require_root_ownership,
            )
            checkpoints = container / "checkpoints"
            if not os.path.lexists(checkpoints):
                continue
            _check_directory(
                checkpoints,
                "Docker checkpoint directory",
                root_owned=self.require_root_ownership,
            )
            for entry in tuple(checkpoints.iterdir()):
                if not _CHECKPOINT_TEMP.fullmatch(entry.name):
                    continue
                self._remove_gc_directory(entry, "staged checkpoint temporary")
                removed_checkpoint_temps += 1
        return {
            "state": "clean",
            "removed_root_temps": removed_root_temps,
            "removed_checkpoint_temps": removed_checkpoint_temps,
            "removed_manifest_temps": removed_manifest_temps,
            "removed_application_temps": removed_application_temps,
            "removed_staged_marker_temps": removed_staged_marker_temps,
        }

    def gc(self) -> dict[str, Any]:
        with self._lock():
            return self._gc_locked()

    def app_prepare(self, application_id: str) -> dict[str, Any]:
        """Create one empty, generation-scoped runsc application path.

        Callers must invoke this only after proving that the corresponding
        Docker container does not exist. Replacing a stale same-operation path
        makes a crash before ``docker create`` replayable without ever sharing
        application checkpoint files between sandbox generations.
        """

        application_id = _validate_safe_id("application id", application_id)
        with self._lock():
            root = self._application_root(create=True)
            path = root / application_id
            replaced = False
            if os.path.lexists(path):
                _check_directory(
                    path,
                    "application checkpoint directory",
                    root_owned=self.require_root_ownership,
                )
                trash = root / (f".ucloud-app-drop-{application_id}-{uuid.uuid4().hex}")
                os.replace(path, trash)
                _fsync_directory(root)
                shutil.rmtree(trash)
                replaced = True
            path.mkdir(mode=0o700)
            _fsync_directory(root)
            return {
                "state": "prepared",
                "application_id": application_id,
                "path": str(path),
                "replaced": replaced,
            }

    def app_drop(self, application_id: str) -> dict[str, Any]:
        application_id = _validate_safe_id("application id", application_id)
        with self._lock():
            root = self._application_root(create=False)
            path = root / application_id
            if not os.path.lexists(path):
                return {
                    "state": "absent",
                    "application_id": application_id,
                    "removed": False,
                }
            _check_directory(
                path,
                "application checkpoint directory",
                root_owned=self.require_root_ownership,
            )
            trash = root / f".ucloud-app-drop-{application_id}-{uuid.uuid4().hex}"
            os.replace(path, trash)
            _fsync_directory(root)
            shutil.rmtree(trash)
            return {
                "state": "absent",
                "application_id": application_id,
                "removed": True,
            }

    def list_state(self) -> dict[str, Any]:
        """Return validated helper-owned state without deleting named data."""

        with self._lock():
            artifacts: list[dict[str, Any]] = []
            for entry in sorted(
                self.config.checkpoint_root.iterdir(), key=lambda item: item.name
            ):
                if entry.name.startswith(".") or entry.name == "application":
                    continue
                artifact_id = _validate_artifact_id(entry.name)
                artifact = self._artifact(artifact_id, must_exist=True)
                if (artifact / "manifest.json").exists():
                    manifest = self._sealed_manifest(artifact_id)[1]
                    artifacts.append(
                        {
                            "artifact_id": artifact_id,
                            "state": "sealed",
                            "source_container_id": manifest["source_container_id"],
                            "checkpoint_id": manifest["checkpoint_id"],
                        }
                    )
                else:
                    preparation = _validate_preparation(
                        _read_json(artifact / ".prepare.json"), artifact_id
                    )
                    artifacts.append(
                        {
                            "artifact_id": artifact_id,
                            "state": "pending",
                            "source_container_id": preparation["source_container_id"],
                            "checkpoint_id": preparation["checkpoint_id"],
                        }
                    )

            applications: list[str] = []
            application_root = self._application_root(create=False)
            if os.path.lexists(application_root):
                for entry in sorted(
                    application_root.iterdir(), key=lambda item: item.name
                ):
                    if _APPLICATION_TRASH.fullmatch(entry.name):
                        continue
                    application_id = _validate_safe_id("application id", entry.name)
                    _check_directory(
                        entry,
                        "application checkpoint directory",
                        root_owned=self.require_root_ownership,
                    )
                    applications.append(application_id)

            staged: list[dict[str, Any]] = []
            staged_root = self._staged_root(create=False)
            if os.path.lexists(staged_root):
                for entry in sorted(staged_root.iterdir(), key=lambda item: item.name):
                    match = _STAGED_MARKER.fullmatch(entry.name)
                    if match is None:
                        if _STAGED_MARKER_TEMP.fullmatch(entry.name):
                            continue
                        raise HelperError(
                            "staged checkpoint registry contains an unexpected entry"
                        )
                    marker = _validate_staged_marker(_read_json(entry))
                    if entry.name != (
                        f"{marker['target_container_id']}-"
                        f"{marker['checkpoint_id']}.json"
                    ):
                        raise HelperError(
                            "staged checkpoint marker identity does not match"
                        )
                    container = self._container(
                        marker["target_container_id"], allow_missing=True
                    )
                    target = (
                        container / "checkpoints" / marker["checkpoint_id"]
                        if container is not None
                        else None
                    )
                    target_present = bool(
                        target is not None and os.path.lexists(target)
                    )
                    if target_present:
                        assert target is not None
                        _check_directory(
                            target,
                            "staged checkpoint",
                            root_owned=self.require_root_ownership,
                        )
                    content_matches: bool | None = None
                    artifact = self._artifact(marker["artifact_id"], must_exist=False)
                    if (
                        os.path.lexists(artifact)
                        and (artifact / "manifest.json").exists()
                    ):
                        _artifact, _manifest, integrity = self._sealed_manifest(
                            marker["artifact_id"]
                        )
                        content_matches = bool(
                            target_present
                            and target is not None
                            and _tree_manifest(target) == integrity["tree"]
                        )
                    staged.append(
                        {
                            **marker,
                            "target_present": target_present,
                            "content_matches": content_matches,
                        }
                    )

            return {
                "version": MANIFEST_VERSION,
                "artifacts": artifacts,
                "applications": applications,
                "staged": staged,
            }

    def _pending_reserved_bytes_locked(self) -> int:
        reserved_bytes = 0
        for artifact in tuple(self.config.checkpoint_root.iterdir()):
            if artifact.name.startswith(".") or artifact.name == "application":
                continue
            try:
                _validate_artifact_id(artifact.name)
                _check_directory(
                    artifact,
                    "checkpoint artifact",
                    root_owned=self.require_root_ownership,
                )
            except HelperError as exc:
                raise HelperError(
                    f"cannot account checkpoint root entry {artifact.name!r}: {exc}"
                ) from exc
            preparation_path = artifact / ".prepare.json"
            manifest_path = artifact / "manifest.json"
            if preparation_path.exists():
                preparation = _validate_preparation(
                    _read_json(preparation_path), artifact.name
                )
                reserved_bytes += preparation["reserved_bytes"]
            elif not manifest_path.exists():
                raise HelperError(
                    f"cannot account checkpoint artifact without a manifest: "
                    f"{artifact.name}"
                )
        return reserved_bytes

    def _require_checkpoint_capacity_locked(self, reservation_bytes: int) -> int:
        try:
            filesystem = os.statvfs(self.config.checkpoint_root)
            fragment_size = int(filesystem.f_frsize or filesystem.f_bsize)
            available_bytes = int(filesystem.f_bavail) * fragment_size
        except (OSError, TypeError, ValueError, AttributeError) as exc:
            raise HelperError(
                "cannot determine available checkpoint storage; refusing prepare"
            ) from exc
        if fragment_size <= 0 or available_bytes < 0:
            raise HelperError(
                "checkpoint filesystem reported invalid capacity; refusing prepare"
            )
        pending_bytes = self._pending_reserved_bytes_locked()
        required_bytes = reservation_bytes + pending_bytes
        if available_bytes < required_bytes:
            raise HelperError(
                "insufficient checkpoint storage: "
                f"available={available_bytes} required={required_bytes} "
                f"new_reservation={reservation_bytes} pending_reservations={pending_bytes}"
            )
        return available_bytes

    def prepare(
        self,
        artifact_id: str,
        source_container_id: str,
        source_image: str,
        source_spec_hash: str,
        checkpoint_id: str,
        source_memory_mb: Any,
        source_disk_mb: Any,
        tmpfs_mb: Any,
        run_tmpfs_mb: Any,
    ) -> dict[str, Any]:
        artifact_id = _validate_artifact_id(artifact_id)
        source_container_id = _validate_container_id(source_container_id)
        source_image = _validate_image_digest(source_image)
        source_spec_hash = _validate_spec_hash(source_spec_hash)
        checkpoint_id = _validate_safe_id("checkpoint id", checkpoint_id)
        source_memory_mb = _validate_source_memory_mb(source_memory_mb)
        source_disk_mb = _validate_source_disk_mb(source_disk_mb)
        tmpfs_mb = _validate_tmpfs_mb("tmpfs_mb", tmpfs_mb)
        run_tmpfs_mb = _validate_tmpfs_mb("run_tmpfs_mb", run_tmpfs_mb)
        reserved_bytes = checkpoint_reservation_bytes(
            source_memory_mb,
            source_disk_mb,
            tmpfs_mb,
            run_tmpfs_mb,
        )
        with self._lock():
            self._container(source_container_id)
            artifact = self._artifact(artifact_id, must_exist=False)
            if os.path.lexists(artifact):
                raise HelperError("checkpoint artifact already exists")
            available_bytes = self._require_checkpoint_capacity_locked(reserved_bytes)
            temporary = self.config.checkpoint_root / (
                f".prepare-{artifact_id}-{uuid.uuid4().hex}"
            )
            preparation = {
                "version": MANIFEST_VERSION,
                "state": "preparing",
                "artifact_id": artifact_id,
                "source_container_id": source_container_id,
                "source_image_id": source_image,
                "source_spec_hash": source_spec_hash,
                "checkpoint_id": checkpoint_id,
                "source_memory_mb": source_memory_mb,
                "source_disk_mb": source_disk_mb,
                "tmpfs_mb": tmpfs_mb,
                "run_tmpfs_mb": run_tmpfs_mb,
                "reserved_bytes": reserved_bytes,
                "created_ns": time.time_ns(),
            }
            try:
                temporary.mkdir(mode=0o700)
                (temporary / "pending").mkdir(mode=0o700)
                _atomic_write_json(temporary / ".prepare.json", preparation)
                os.replace(temporary, artifact)
                _fsync_directory(self.config.checkpoint_root)
            except BaseException:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
            return preparation | {
                "pending_path": str(artifact / "pending"),
                "available_bytes_at_prepare": available_bytes,
            }

    def _sealed_manifest(
        self, artifact_id: str
    ) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        artifact = self._artifact(artifact_id, must_exist=True)
        sealed = artifact / "sealed"
        _check_directory(
            sealed,
            "sealed checkpoint directory",
            root_owned=self.require_root_ownership,
        )
        manifest = _validate_manifest(
            _read_json(artifact / "manifest.json"), artifact_id
        )
        integrity = _validate_integrity(_read_json(artifact / ".integrity.json"))
        source = sealed / manifest["checkpoint_id"]
        actual_tree = _tree_manifest(source)
        if actual_tree != integrity["tree"]:
            raise HelperError("sealed checkpoint tree does not match its manifest")
        expected_children = {"sealed", "manifest.json", ".integrity.json"}
        actual_children = {entry.name for entry in artifact.iterdir()}
        transitional = {".prepare.json", ".complete.json"}
        if expected_children <= actual_children <= expected_children | transitional:
            for name in sorted(actual_children & transitional):
                (artifact / name).unlink()
            _fsync_directory(artifact)
            actual_children -= transitional
        if actual_children != expected_children:
            raise HelperError("sealed artifact contains unexpected entries")
        return artifact, manifest, integrity

    def complete(self, artifact_id: str) -> dict[str, Any]:
        """Record that Docker returned successfully from checkpoint creation.

        Only the unprivileged operation supervisor calls this after the Docker
        CLI has completed.  ``seal`` requires the durable marker, so replay can
        never adopt a directory that dockerd/runsc may still be writing.
        """

        artifact_id = _validate_artifact_id(artifact_id)
        with self._lock():
            artifact = self._artifact(artifact_id, must_exist=True)
            if (artifact / "manifest.json").exists():
                return self._sealed_manifest(artifact_id)[1]
            preparation = _validate_preparation(
                _read_json(artifact / ".prepare.json"), artifact_id
            )
            marker_path = artifact / ".complete.json"
            if marker_path.exists():
                return _validate_completion(_read_json(marker_path), preparation)
            pending = artifact / "pending"
            sealed = artifact / "sealed"
            if os.path.lexists(pending) == os.path.lexists(sealed):
                raise HelperError(
                    "artifact must contain exactly one pending or sealed checkpoint"
                )
            checkpoint_parent = pending if os.path.lexists(pending) else sealed
            _check_directory(
                checkpoint_parent,
                "checkpoint directory",
                root_owned=self.require_root_ownership,
            )
            if {entry.name for entry in checkpoint_parent.iterdir()} != {
                preparation["checkpoint_id"]
            }:
                raise HelperError(
                    "checkpoint directory must contain exactly its checkpoint id"
                )
            _check_directory(
                checkpoint_parent / preparation["checkpoint_id"],
                "checkpoint image",
                root_owned=self.require_root_ownership,
            )
            completion = {
                **preparation,
                "state": "complete",
                "completed_ns": max(time.time_ns(), preparation["created_ns"]),
            }
            _atomic_write_json(marker_path, completion)
            return completion

    def seal(self, artifact_id: str) -> dict[str, Any]:
        artifact_id = _validate_artifact_id(artifact_id)
        with self._lock():
            artifact = self._artifact(artifact_id, must_exist=True)
            if (artifact / "manifest.json").exists():
                return self._sealed_manifest(artifact_id)[1]
            preparation = _validate_preparation(
                _read_json(artifact / ".prepare.json"), artifact_id
            )
            _validate_completion(_read_json(artifact / ".complete.json"), preparation)
            pending = artifact / "pending"
            sealed = artifact / "sealed"
            if os.path.lexists(pending) and os.path.lexists(sealed):
                raise HelperError(
                    "artifact contains both pending and sealed checkpoints"
                )
            if os.path.lexists(pending):
                _check_directory(
                    pending,
                    "pending checkpoint directory",
                    root_owned=self.require_root_ownership,
                )
                if {entry.name for entry in pending.iterdir()} != {
                    preparation["checkpoint_id"]
                }:
                    raise HelperError(
                        "pending checkpoint must contain exactly its checkpoint id"
                    )
                tree = _tree_manifest(pending / preparation["checkpoint_id"])
                os.replace(pending, sealed)
                _fsync_directory(artifact)
            elif os.path.lexists(sealed):
                _check_directory(
                    sealed,
                    "sealed checkpoint directory",
                    root_owned=self.require_root_ownership,
                )
                tree = _tree_manifest(sealed / preparation["checkpoint_id"])
            else:
                raise HelperError("pending checkpoint is absent")
            integrity = {
                "version": MANIFEST_VERSION,
                "created_ns": preparation["created_ns"],
                "sealed_ns": max(time.time_ns(), preparation["created_ns"]),
                "source_memory_mb": preparation["source_memory_mb"],
                "source_disk_mb": preparation["source_disk_mb"],
                "tmpfs_mb": preparation["tmpfs_mb"],
                "run_tmpfs_mb": preparation["run_tmpfs_mb"],
                "reserved_bytes": preparation["reserved_bytes"],
                "file_count": sum(entry["kind"] == "file" for entry in tree),
                "byte_size": sum(
                    entry["size"] for entry in tree if entry["kind"] == "file"
                ),
                "tree": tree,
            }
            manifest = {
                "artifact_id": preparation["artifact_id"],
                "checkpoint_id": preparation["checkpoint_id"],
                "source_container_id": preparation["source_container_id"],
                "source_image_id": preparation["source_image_id"],
                "source_spec_hash": preparation["source_spec_hash"],
            }
            _validate_manifest(manifest, artifact_id)
            _validate_integrity(integrity)
            _atomic_write_json(artifact / ".integrity.json", integrity)
            _atomic_write_json(artifact / "manifest.json", manifest)
            (artifact / ".prepare.json").unlink()
            (artifact / ".complete.json").unlink()
            _fsync_directory(artifact)
            return self._sealed_manifest(artifact_id)[1]

    def status(self, artifact_id: str) -> dict[str, Any]:
        artifact_id = _validate_artifact_id(artifact_id)
        with self._lock():
            artifact = self._artifact(artifact_id, must_exist=False)
            if not os.path.lexists(artifact):
                raise ArtifactMissing("checkpoint artifact does not exist")
            if not (artifact / "manifest.json").exists():
                raise ArtifactNotReady("checkpoint artifact is not sealed")
            return self._sealed_manifest(artifact_id)[1]

    def stage(
        self, artifact_id: str, target_container_id: str, checkpoint_id: str
    ) -> dict[str, Any]:
        artifact_id = _validate_artifact_id(artifact_id)
        target_container_id = _validate_container_id(target_container_id)
        checkpoint_id = _validate_safe_id("checkpoint id", checkpoint_id)
        with self._lock():
            artifact, manifest, integrity = self._sealed_manifest(artifact_id)
            container = self._container(target_container_id)
            assert container is not None
            marker_path = self._staged_marker_path(
                target_container_id,
                checkpoint_id,
                create_root=True,
            )
            marker = self._read_staged_marker(target_container_id, checkpoint_id)
            if marker is not None and marker["artifact_id"] != artifact_id:
                raise HelperError("target checkpoint is registered to another artifact")
            created_ns = marker["created_ns"] if marker is not None else time.time_ns()
            marker = {
                "version": MANIFEST_VERSION,
                "state": "staging",
                "artifact_id": artifact_id,
                "target_container_id": target_container_id,
                "checkpoint_id": checkpoint_id,
                "created_ns": created_ns,
                "updated_ns": max(time.time_ns(), created_ns),
            }
            _atomic_write_json(marker_path, marker)
            checkpoints = container / "checkpoints"
            if not os.path.lexists(checkpoints):
                checkpoints.mkdir(mode=0o700)
                _fsync_directory(container)
            _check_directory(
                checkpoints,
                "target checkpoint directory",
                root_owned=self.require_root_ownership,
            )
            target = checkpoints / checkpoint_id
            if os.path.lexists(target):
                if _tree_manifest(target) == integrity["tree"]:
                    marker = {
                        **marker,
                        "state": "staged",
                        "updated_ns": max(time.time_ns(), marker["created_ns"]),
                    }
                    _atomic_write_json(marker_path, marker)
                    return {
                        "state": "staged",
                        "artifact_id": artifact_id,
                        "target_container_id": target_container_id,
                        "checkpoint_id": checkpoint_id,
                        "already_staged": True,
                    }
                raise HelperError(
                    "target checkpoint already exists with other contents"
                )
            temporary = (
                checkpoints / f".ucloud-stage-{checkpoint_id}-{uuid.uuid4().hex}"
            )
            source = artifact / "sealed" / manifest["checkpoint_id"]
            try:
                self.copy_tree(source, temporary)
                if _tree_manifest(temporary) != integrity["tree"]:
                    raise HelperError(
                        "staged checkpoint copy does not match its manifest"
                    )
                os.replace(temporary, target)
                _fsync_directory(checkpoints)
                marker = {
                    **marker,
                    "state": "staged",
                    "updated_ns": max(time.time_ns(), marker["created_ns"]),
                }
                _atomic_write_json(marker_path, marker)
            except BaseException:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
            return {
                "state": "staged",
                "artifact_id": artifact_id,
                "target_container_id": target_container_id,
                "checkpoint_id": checkpoint_id,
                "already_staged": False,
            }

    def unstage(self, target_container_id: str, checkpoint_id: str) -> dict[str, Any]:
        target_container_id = _validate_container_id(target_container_id)
        checkpoint_id = _validate_safe_id("checkpoint id", checkpoint_id)
        with self._lock():
            marker_path = self._staged_marker_path(
                target_container_id,
                checkpoint_id,
                create_root=False,
            )
            marker = self._read_staged_marker(target_container_id, checkpoint_id)
            container = self._container(target_container_id, allow_missing=True)
            if container is None:
                if marker is not None:
                    marker_path.unlink()
                    _fsync_directory(marker_path.parent)
                return {"state": "absent", "removed": False}
            checkpoints = container / "checkpoints"
            if not os.path.lexists(checkpoints):
                if marker is not None:
                    marker_path.unlink()
                    _fsync_directory(marker_path.parent)
                return {"state": "absent", "removed": False}
            _check_directory(
                checkpoints,
                "target checkpoint directory",
                root_owned=self.require_root_ownership,
            )
            target = checkpoints / checkpoint_id
            if not os.path.lexists(target):
                if marker is not None:
                    marker_path.unlink()
                    _fsync_directory(marker_path.parent)
                return {"state": "absent", "removed": False}
            _check_directory(
                target,
                "staged checkpoint",
                root_owned=self.require_root_ownership,
            )
            trash = checkpoints / f".ucloud-unstage-{checkpoint_id}-{uuid.uuid4().hex}"
            os.replace(target, trash)
            _fsync_directory(checkpoints)
            shutil.rmtree(trash)
            if marker is not None:
                marker_path.unlink()
                _fsync_directory(marker_path.parent)
            return {"state": "absent", "removed": True}

    def drop(self, artifact_id: str) -> dict[str, Any]:
        artifact_id = _validate_artifact_id(artifact_id)
        with self._lock():
            staged_root = self._staged_root(create=False)
            if os.path.lexists(staged_root):
                for entry in staged_root.iterdir():
                    if _STAGED_MARKER.fullmatch(entry.name) is None:
                        continue
                    marker = _validate_staged_marker(_read_json(entry))
                    if marker["artifact_id"] == artifact_id:
                        raise HelperError(
                            "checkpoint artifact still has a staged reference"
                        )
            artifact = self._artifact(artifact_id, must_exist=False)
            if not os.path.lexists(artifact):
                return {"state": "absent", "removed": False}
            _check_directory(
                artifact,
                "artifact",
                root_owned=self.require_root_ownership,
            )
            trash = self.config.checkpoint_root / (
                f".ucloud-drop-{artifact_id}-{uuid.uuid4().hex}"
            )
            os.replace(artifact, trash)
            _fsync_directory(self.config.checkpoint_root)
            shutil.rmtree(trash)
            return {"state": "absent", "removed": True}


def _usage() -> str:
    return (
        "usage: ucloud-sandbox-checkpoint prepare ARTIFACT SOURCE_CONTAINER "
        "SOURCE_IMAGE SOURCE_SPEC_HASH CHECKPOINT SOURCE_MEMORY_MB "
        "SOURCE_DISK_MB TMPFS_MB RUN_TMPFS_MB\n"
        "       ucloud-sandbox-checkpoint complete ARTIFACT\n"
        "       ucloud-sandbox-checkpoint status ARTIFACT\n"
        "       ucloud-sandbox-checkpoint seal ARTIFACT\n"
        "       ucloud-sandbox-checkpoint stage ARTIFACT TARGET_CONTAINER CHECKPOINT\n"
        "       ucloud-sandbox-checkpoint unstage TARGET_CONTAINER CHECKPOINT\n"
        "       ucloud-sandbox-checkpoint drop ARTIFACT\n"
        "       ucloud-sandbox-checkpoint app-prepare APPLICATION_ID\n"
        "       ucloud-sandbox-checkpoint app-drop APPLICATION_ID\n"
        "       ucloud-sandbox-checkpoint list\n"
        "       ucloud-sandbox-checkpoint gc"
    )


def run_action(helper: CheckpointHelper, argv: Sequence[str]) -> dict[str, Any]:
    if not argv:
        raise HelperError(_usage())
    action, *arguments = argv
    if action == "prepare" and len(arguments) == 9:
        return helper.prepare(*arguments)
    if action == "complete" and len(arguments) == 1:
        return helper.complete(arguments[0])
    if action == "status" and len(arguments) == 1:
        return helper.status(arguments[0])
    if action == "seal" and len(arguments) == 1:
        return helper.seal(arguments[0])
    if action == "stage" and len(arguments) == 3:
        return helper.stage(*arguments)
    if action == "unstage" and len(arguments) == 2:
        return helper.unstage(*arguments)
    if action == "drop" and len(arguments) == 1:
        return helper.drop(arguments[0])
    if action == "app-prepare" and len(arguments) == 1:
        return helper.app_prepare(arguments[0])
    if action == "app-drop" and len(arguments) == 1:
        return helper.app_drop(arguments[0])
    if action == "list" and not arguments:
        return helper.list_state()
    if action == "gc" and not arguments:
        return helper.gc()
    raise HelperError(_usage())


def main(
    argv: Sequence[str] | None = None,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    require_root: bool = True,
) -> int:
    try:
        if require_root and os.geteuid() != 0:
            raise HelperError("checkpoint helper must run as root")
        config = load_config(config_path, require_root_ownership=require_root)
        helper = CheckpointHelper(config, require_root_ownership=require_root)
        result = run_action(helper, list(sys.argv[1:] if argv is None else argv))
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except ArtifactNotReady as exc:
        print(f"checkpoint helper: {exc}", file=sys.stderr)
        return 3
    except ArtifactMissing as exc:
        print(f"checkpoint helper: {exc}", file=sys.stderr)
        return 4
    except HelperError as exc:
        print(f"checkpoint helper: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
