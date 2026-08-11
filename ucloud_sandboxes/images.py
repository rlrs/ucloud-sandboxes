from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from threading import BoundedSemaphore, Condition, RLock, Thread
import time
from typing import Any, Callable, Iterable
from uuid import uuid4

from .build_context_store import BuildContextBlobStore
from .managed_registry import (
    canonical_image_digest_ref,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
)
from .models import parse_iso_datetime, utc_now
from .sandbox import (
    CommandExecutor,
    CommandResult,
    SubprocessExecutor,
    _AdvisoryFileLock,
    _atomic_write_json,
)


IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
BUILD_TERMINAL_STATES = {"succeeded", "failed"}
BUILD_LOG_TAIL_CHARS = 64 * 1024
COMMAND_OUTPUT_TAIL_CHARS = 64 * 1024
COMMAND_OUTPUT_READ_CHARS = 16 * 1024
COMMAND_OUTPUT_TRUNCATION_MARKER = "[output truncated; showing retained tail]\n"
DEFAULT_TERMINAL_BUILD_HISTORY = 256
BUILD_LOG_FLUSH_CHARS = 16 * 1024
BUILD_LOG_FLUSH_INTERVAL_SECONDS = 0.25
MAX_BUILD_CONTEXT_EXTRACTED_BYTES = 2 * 1024**3
MAX_BUILD_CONTEXT_MEMBER_BYTES = 512 * 1024**2
MAX_BUILD_CONTEXT_MEMBERS = 100_000
MAX_BUILD_CONTEXT_DECOMPRESSED_ARCHIVE_BYTES = 2 * 1024**3
_IMAGE_LOCKS_GUARD = RLock()
_IMAGE_LOCKS: dict[Path, _AdvisoryFileLock] = {}


class ImageBuildCapacityError(RuntimeError):
    pass


class ImageBuildConflictError(RuntimeError):
    pass


@dataclass(frozen=True)
class ImageBuildSpec:
    id: str
    tag: str
    context_path: str
    dockerfile: str = "Dockerfile"
    build_args: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ImageBuildSpec":
        build_args = raw.get("build_args") or {}
        labels = raw.get("labels") or {}
        tag = str(raw.get("tag") or "")
        return cls(
            id=str(raw.get("id") or image_id_from_tag(tag)),
            tag=tag,
            context_path=str(raw.get("context_path") or "."),
            dockerfile=_normalize_dockerfile_path(
                str(raw.get("dockerfile") or "Dockerfile")
            ),
            build_args={str(k): str(v) for k, v in dict(build_args).items()},
            labels={str(k): str(v) for k, v in dict(labels).items()},
        )

    def validate(self) -> None:
        if not IMAGE_ID_RE.match(self.id):
            raise ValueError(
                "image id must be 1-64 characters of letters, digits, _, . or - "
                "and start with a letter or digit."
            )
        if not self.tag.strip():
            raise ValueError("image tag is required.")
        if not self.context_path.strip():
            raise ValueError("image context_path is required.")
        _normalize_dockerfile_path(self.dockerfile)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def image_build_fingerprint(
    spec: ImageBuildSpec,
    *,
    push: bool = False,
    context_identity: str | None = None,
) -> str:
    """Return the exact immutable identity used for build single-flight."""

    spec.validate()
    immutable_context_identity = context_identity
    if immutable_context_identity is None:
        immutable_context_identity = (
            f"tree:{image_build_context_digest(Path(spec.context_path))}"
        )
    payload = {
        "build_args": dict(spec.build_args),
        "context_identity": immutable_context_identity,
        "dockerfile": _normalize_dockerfile_path(spec.dockerfile),
        "image_id": spec.id,
        "labels": dict(spec.labels),
        "push": bool(push),
        "tag": spec.tag,
        "version": 1,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def image_build_context_digest(context_path: Path) -> str:
    """Hash a deterministic, mutation-checked snapshot of a local context."""

    try:
        root = context_path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"image build context is unavailable: {context_path}") from exc
    if not root.is_dir():
        raise ValueError(f"image build context must be a directory: {context_path}")
    digest = hashlib.sha256()
    digest.update(b"ucloud-image-context-v2\0")

    def visit(directory: Path, relative: Path) -> None:
        before = directory.stat(follow_symlinks=False)
        _hash_context_metadata(digest, relative, before, kind="directory")
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise ValueError(f"cannot read image build context: {directory}") from exc
        for entry in entries:
            entry_path = Path(entry.path)
            entry_relative = relative / entry.name
            try:
                entry_stat = entry.stat(follow_symlinks=False)
                if entry.is_symlink():
                    target = os.readlink(entry_path)
                    _hash_context_metadata(
                        digest,
                        entry_relative,
                        entry_stat,
                        kind="symlink",
                        value=target.encode("utf-8", errors="surrogateescape"),
                    )
                elif entry.is_dir(follow_symlinks=False):
                    visit(entry_path, entry_relative)
                elif entry.is_file(follow_symlinks=False):
                    content_digest = hashlib.sha256()
                    with entry_path.open("rb") as handle:
                        while chunk := handle.read(1024 * 1024):
                            content_digest.update(chunk)
                    after = entry.stat(follow_symlinks=False)
                    if _context_stat_identity(entry_stat) != _context_stat_identity(
                        after
                    ):
                        raise ValueError(
                            f"image build context changed while hashing: {entry_path}"
                        )
                    _hash_context_metadata(
                        digest,
                        entry_relative,
                        entry_stat,
                        kind="file",
                        value=content_digest.digest(),
                    )
                else:
                    raise ValueError(
                        f"unsupported file type in image build context: {entry_path}"
                    )
            except OSError as exc:
                raise ValueError(
                    f"cannot read image build context entry: {entry_path}"
                ) from exc
        after = directory.stat(follow_symlinks=False)
        if _context_stat_identity(before) != _context_stat_identity(after):
            raise ValueError(
                f"image build context changed while hashing: {directory}"
            )

    visit(root, Path("."))
    return digest.hexdigest()


def _context_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _hash_context_metadata(
    digest: Any,
    path: Path,
    metadata: os.stat_result,
    *,
    kind: str,
    value: bytes = b"",
) -> None:
    record = json.dumps(
        {
            "kind": kind,
            "mode": stat.S_IMODE(metadata.st_mode),
            "mtime_ns": metadata.st_mtime_ns,
            "path": path.as_posix(),
            "size": metadata.st_size if kind == "file" else 0,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8", errors="surrogateescape")
    digest.update(len(record).to_bytes(8, "big"))
    digest.update(record)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


@dataclass(frozen=True)
class ImageRecord:
    id: str
    tag: str
    source: str
    state: str
    created_at: datetime
    updated_at: datetime
    labels: dict[str, str] = field(default_factory=dict)
    pushed: bool = False
    manifest_digest: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ImageRecord":
        unsupported = sorted(
            set(raw)
            - {
                "available_to_sandboxes",
                "created_at",
                "id",
                "labels",
                "manifest_digest",
                "pushed",
                "source",
                "state",
                "tag",
                "updated_at",
            }
        )
        if unsupported:
            raise ValueError(
                "unsupported image record fields: " + ", ".join(unsupported)
            )
        created_at = parse_iso_datetime(raw.get("created_at"))
        updated_at = parse_iso_datetime(raw.get("updated_at"))
        if created_at is None or updated_at is None:
            raise ValueError("image record has invalid timestamps.")
        image_id = str(raw.get("id") or "")
        tag = str(raw.get("tag") or "")
        source = str(raw.get("source") or "")
        state = str(raw.get("state") or "")
        if not IMAGE_ID_RE.fullmatch(image_id):
            raise ValueError("image record has an invalid image id.")
        if not tag or not source or not state:
            raise ValueError("image record is missing tag, source, or state.")
        labels = raw.get("labels") or {}
        if not isinstance(labels, dict):
            raise ValueError("image record labels must be a JSON object.")
        if not isinstance(raw.get("pushed", False), bool):
            raise ValueError("image record pushed must be a boolean.")
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in labels.items()
        ):
            raise ValueError("image record labels must contain strings.")
        raw_manifest_digest = str(raw.get("manifest_digest") or "")
        manifest_digest = normalize_manifest_digest(raw_manifest_digest)
        if raw_manifest_digest and not manifest_digest:
            raise ValueError("image record has an invalid manifest digest.")
        return cls(
            id=image_id,
            tag=tag,
            source=source,
            state=state,
            created_at=created_at,
            updated_at=updated_at,
            labels=dict(labels),
            pushed=raw.get("pushed", False),
            manifest_digest=manifest_digest,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tag": self.tag,
            "source": self.source,
            "state": self.state,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "labels": dict(self.labels),
            "pushed": self.pushed,
            "manifest_digest": self.manifest_digest,
            "available_to_sandboxes": self.available_to_sandboxes,
        }

    @property
    def available_to_sandboxes(self) -> bool:
        return self.pushed or self.source == "registry"

    @property
    def digest_ref(self) -> str:
        return canonical_image_digest_ref(self.tag, self.manifest_digest)


@dataclass(frozen=True)
class ImageBuildRecord:
    build_id: str
    image_id: str
    tag: str
    status: str
    created_at: str
    updated_at: str
    context_path: str = ""
    dockerfile: str = "Dockerfile"
    push: bool = False
    command: tuple[str, ...] = ()
    push_command: tuple[str, ...] = ()
    exit_code: int | None = None
    push_exit_code: int | None = None
    error: str = ""
    log_tail: str = ""
    started_at: str = ""
    finished_at: str = ""
    image: dict[str, Any] = field(default_factory=dict)
    timings: dict[str, Any] = field(default_factory=dict)
    owner_pid: int = 0
    request_fingerprint: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ImageBuildRecord | None":
        if set(raw) - {
            "build_id",
            "command",
            "context_path",
            "created_at",
            "dockerfile",
            "error",
            "exit_code",
            "finished_at",
            "image",
            "image_id",
            "log_tail",
            "owner_pid",
            "push",
            "push_command",
            "push_exit_code",
            "request_fingerprint",
            "started_at",
            "status",
            "tag",
            "timings",
            "updated_at",
        }:
            return None
        build_id = str(raw.get("build_id") or "")
        image_id = str(raw.get("image_id") or "")
        tag = str(raw.get("tag") or "")
        status = str(raw.get("status") or "")
        created_at = str(raw.get("created_at") or "")
        updated_at = str(raw.get("updated_at") or "")
        if (
            not build_id
            or not image_id
            or not status
            or not created_at
            or not updated_at
        ):
            return None
        command = raw.get("command") or ()
        push_command = raw.get("push_command") or ()
        image = raw.get("image") if isinstance(raw.get("image"), dict) else {}
        timings = raw.get("timings") if isinstance(raw.get("timings"), dict) else {}
        exit_code = raw.get("exit_code")
        push_exit_code = raw.get("push_exit_code")
        return cls(
            build_id=build_id,
            image_id=image_id,
            tag=tag,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            context_path=str(raw.get("context_path") or ""),
            dockerfile=str(raw.get("dockerfile") or "Dockerfile"),
            push=bool(raw.get("push", False)),
            command=tuple(str(item) for item in command),
            push_command=tuple(str(item) for item in push_command),
            exit_code=_optional_int(exit_code),
            push_exit_code=_optional_int(push_exit_code),
            error=str(raw.get("error") or ""),
            log_tail=str(raw.get("log_tail") or ""),
            started_at=str(raw.get("started_at") or ""),
            finished_at=str(raw.get("finished_at") or ""),
            image={str(key): value for key, value in image.items()},
            timings={str(key): value for key, value in timings.items()},
            owner_pid=max(0, _optional_int(raw.get("owner_pid")) or 0),
            request_fingerprint=str(raw.get("request_fingerprint") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "image_id": self.image_id,
            "tag": self.tag,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "context_path": self.context_path,
            "dockerfile": self.dockerfile,
            "push": self.push,
            "command": list(self.command),
            "push_command": list(self.push_command),
            "exit_code": self.exit_code,
            "push_exit_code": self.push_exit_code,
            "error": self.error,
            "log_tail": self.log_tail,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "image": dict(self.image),
            "timings": dict(self.timings),
            "owner_pid": self.owner_pid,
            "request_fingerprint": self.request_fingerprint,
        }

    @property
    def terminal(self) -> bool:
        return self.status in BUILD_TERMINAL_STATES


class DockerImageRuntime:
    def __init__(
        self,
        *,
        executor: CommandExecutor | None = None,
        docker_binary: str = "docker",
        dry_run: bool = False,
        buildx_direct_push: bool = False,
        buildx_cache_ref: str | None = None,
    ) -> None:
        normalized_cache_ref = (buildx_cache_ref or "").strip()
        if normalized_cache_ref and not buildx_direct_push:
            raise ValueError(
                "buildx_cache_ref requires buildx_direct_push to be enabled."
            )
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.dry_run = dry_run
        self.buildx_direct_push = buildx_direct_push
        self.buildx_cache_ref = normalized_cache_ref

    def build(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool = False,
        on_output: Callable[[str, str], None] | None = None,
    ) -> CommandResult:
        return self._run(self.build_command(spec, push=push), on_output=on_output)

    def pull(self, image: str) -> CommandResult:
        if not image.strip():
            raise ValueError("image is required.")
        return self._run((self.docker_binary, "pull", image))

    def push(
        self,
        image: str,
        *,
        on_output: Callable[[str, str], None] | None = None,
    ) -> CommandResult:
        if not image.strip():
            raise ValueError("image is required.")
        return self._run(self.push_command(image), on_output=on_output)

    def tag(self, source: str, target: str) -> CommandResult:
        if not source.strip() or not target.strip():
            raise ValueError("source and target image are required.")
        return self._run((self.docker_binary, "tag", source, target))

    def build_command(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool = False,
    ) -> tuple[str, ...]:
        spec.validate()
        dockerfile = _dockerfile_path(spec.context_path, spec.dockerfile)
        direct_push = push and self.buildx_direct_push
        argv = [self.docker_binary]
        argv.extend(("buildx", "build") if direct_push else ("build",))
        argv.extend(
            [
                "-f",
                dockerfile,
                "-t",
                spec.tag,
                "--label",
                "ucloud-sandboxes.image=true",
                "--label",
                f"ucloud-sandboxes.image-id={spec.id}",
            ]
        )
        for key in sorted(spec.build_args):
            argv.extend(["--build-arg", f"{key}={spec.build_args[key]}"])
        for key in sorted(spec.labels):
            argv.extend(["--label", f"{key}={spec.labels[key]}"])
        if direct_push:
            argv.append("--push")
            if self.buildx_cache_ref:
                cache = f"type=registry,ref={self.buildx_cache_ref}"
                argv.extend(["--cache-from", cache])
                argv.extend(["--cache-to", f"{cache},mode=max"])
        argv.append(spec.context_path)
        return tuple(argv)

    def uses_direct_push(self, *, push: bool) -> bool:
        """Return whether the requested push is part of the Buildx command."""
        return push and self.buildx_direct_push

    def push_command(self, image: str) -> tuple[str, ...]:
        if not image.strip():
            raise ValueError("image is required.")
        return (self.docker_binary, "push", image)

    def _run(
        self,
        argv: tuple[str, ...],
        *,
        on_output: Callable[[str, str], None] | None = None,
    ) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        if on_output is not None and isinstance(self.executor, SubprocessExecutor):
            return self._run_streaming(argv, on_output=on_output)
        result = self.executor.run(argv)
        if on_output is not None:
            if result.stdout:
                on_output("stdout", result.stdout)
            if result.stderr:
                on_output("stderr", result.stderr)
        if result.exit_code != 0:
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
            )
        return result

    def _run_streaming(
        self,
        argv: tuple[str, ...],
        *,
        on_output: Callable[[str, str], None],
    ) -> CommandResult:
        process = subprocess.Popen(
            list(argv),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        # Continue delivering every chunk to the live callback, but retain only
        # a bounded diagnostic tail in CommandResult. Docker build output can
        # otherwise consume all node memory during a long or noisy build.
        output_tail = ""
        output_truncated = False
        assert process.stdout is not None
        try:
            while True:
                chunk = process.stdout.read(COMMAND_OUTPUT_READ_CHARS)
                if not chunk:
                    break
                if len(output_tail) + len(chunk) > COMMAND_OUTPUT_TAIL_CHARS:
                    output_truncated = True
                output_tail = _tail_text(
                    output_tail + chunk,
                    limit=COMMAND_OUTPUT_TAIL_CHARS,
                )
                on_output("combined", chunk)
        except BaseException:
            process.terminate()
            process.wait()
            raise
        finally:
            process.stdout.close()
        exit_code = process.wait()
        output = (
            COMMAND_OUTPUT_TRUNCATION_MARKER
            + output_tail[
                -(COMMAND_OUTPUT_TAIL_CHARS - len(COMMAND_OUTPUT_TRUNCATION_MARKER)) :
            ]
            if output_truncated
            else output_tail
        )
        result = CommandResult(argv=argv, exit_code=exit_code, stdout=output)
        if result.exit_code != 0:
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{output}"
            )
        return result


class ImageStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _image_lock(path)

    def load(self) -> dict[str, ImageRecord]:
        with self._lock.hold(exclusive=False):
            return self._load_unlocked()

    def save(self, records: dict[str, ImageRecord]) -> None:
        with self._lock.hold(exclusive=True):
            self._save_unlocked(records)

    def upsert(self, record: ImageRecord) -> dict[str, ImageRecord]:
        with self._lock.hold(exclusive=True):
            records = self._load_unlocked()
            records[record.id] = record
            self._save_unlocked(records)
            return records

    def delete_by_tags(self, tags: Iterable[str]) -> list[ImageRecord]:
        tag_set = {tag for tag in tags if tag}
        if not tag_set:
            return []
        with self._lock.hold(exclusive=True):
            records = self._load_unlocked()
            removed = [record for record in records.values() if record.tag in tag_set]
            if removed:
                self._save_unlocked(
                    {
                        image_id: record
                        for image_id, record in records.items()
                        if record.tag not in tag_set
                    }
                )
            return removed

    def _load_unlocked(self) -> dict[str, ImageRecord]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("image store must contain a JSON object.")
        items = raw.get("images", [])
        if not isinstance(items, list):
            raise ValueError("image store must contain an images list.")
        records: dict[str, ImageRecord] = {}
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(
                    f"image store contains an invalid record at index {index}."
                )
            record = ImageRecord.from_dict(item)
            if record.id in records:
                raise ValueError(
                    f"image store contains duplicate image id {record.id!r}."
                )
            records[record.id] = record
        return records

    def _save_unlocked(self, records: dict[str, ImageRecord]) -> None:
        _atomic_write_json(
            self.path,
            {"images": [records[image_id].to_dict() for image_id in sorted(records)]},
        )


class ImageBuildStore:
    def __init__(
        self,
        path: Path,
        *,
        max_terminal_builds: int = DEFAULT_TERMINAL_BUILD_HISTORY,
    ) -> None:
        self.path = path
        self.max_terminal_builds = max(0, max_terminal_builds)
        self._lock = _image_lock(path)

    def load(self) -> dict[str, ImageBuildRecord]:
        with self._lock.hold(exclusive=False):
            return self._load_unlocked()

    def save(self, records: dict[str, ImageBuildRecord]) -> None:
        with self._lock.hold(exclusive=True):
            self._save_unlocked(records)

    def upsert(self, record: ImageBuildRecord) -> dict[str, ImageBuildRecord]:
        with self._lock.hold(exclusive=True):
            records = self._load_unlocked()
            records[record.build_id] = record
            self._save_unlocked(records)
            return records

    def reserve_build(
        self,
        record: ImageBuildRecord,
        *,
        max_active_builds: int,
    ) -> tuple[ImageBuildRecord, bool]:
        """Atomically deduplicate, capacity-check, and persist a new build."""
        with self._lock.hold(exclusive=True):
            records = self._load_unlocked()
            matching = sorted(
                (
                    existing
                    for existing in records.values()
                    if (
                        existing.image_id == record.image_id
                        or existing.tag == record.tag
                    )
                    and not existing.terminal
                ),
                key=lambda item: (item.created_at, item.build_id),
            )
            if matching:
                exact = [
                    existing
                    for existing in matching
                    if existing.request_fingerprint
                    and existing.request_fingerprint == record.request_fingerprint
                ]
                if exact:
                    return exact[-1], False
                raise ImageBuildConflictError(
                    "an active build already owns this image id or tag with "
                    "a different build specification"
                )
            active_count = sum(
                1 for existing in records.values() if not existing.terminal
            )
            if active_count >= max_active_builds:
                raise ImageBuildCapacityError(
                    f"image build capacity reached ({max_active_builds})"
                )
            records[record.build_id] = record
            self._save_unlocked(records)
            return record, True

    def reconcile_interrupted(self) -> tuple[ImageBuildRecord, ...]:
        with self._lock.hold(exclusive=True):
            records = self._load_unlocked()
            now = utc_now().isoformat()
            interrupted: list[ImageBuildRecord] = []
            for build_id, record in records.items():
                if record.terminal or _pid_is_running(record.owner_pid):
                    continue
                error = "image build interrupted by node-agent restart"
                if record.error:
                    error = f"{record.error}; {error}"
                updated = replace(
                    record,
                    status="failed",
                    error=error,
                    updated_at=now,
                    finished_at=now,
                )
                records[build_id] = updated
                interrupted.append(updated)
            compacted = self._bounded_records(records)
            if interrupted or len(compacted) != len(records):
                self._save_unlocked(compacted)
            return tuple(interrupted)

    def get(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        records = self.load()
        exact = records.get(build_id_or_image_id)
        if exact is not None:
            return exact
        matches = [
            record
            for record in records.values()
            if record.image_id == build_id_or_image_id
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda item: (item.created_at, item.build_id))[-1]

    def active_for_image(
        self,
        image_id: str,
        *,
        tag: str | None = None,
    ) -> ImageBuildRecord | None:
        matches = [
            record
            for record in self.load().values()
            if record.image_id == image_id
            and not record.terminal
            and (tag is None or record.tag == tag)
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda item: (item.created_at, item.build_id))[-1]

    def _load_unlocked(self) -> dict[str, ImageBuildRecord]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("image build store must contain a JSON object.")
        items = raw.get("builds", [])
        if not isinstance(items, list):
            raise ValueError("image build store must contain a builds list.")
        records: dict[str, ImageBuildRecord] = {}
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(
                    f"image build store contains an invalid record at index {index}."
                )
            record = ImageBuildRecord.from_dict(item)
            if record is None:
                raise ValueError(
                    f"image build store contains an invalid record at index {index}."
                )
            if record.build_id in records:
                raise ValueError(
                    f"image build store contains duplicate build id {record.build_id!r}."
                )
            records[record.build_id] = record
        return records

    def _save_unlocked(self, records: dict[str, ImageBuildRecord]) -> None:
        records = self._bounded_records(records)
        _atomic_write_json(
            self.path,
            {
                "builds": [
                    records[build_id].to_dict()
                    for build_id in sorted(
                        records,
                        key=lambda item: (
                            records[item].created_at,
                            records[item].build_id,
                        ),
                    )
                ]
            },
        )

    def _bounded_records(
        self,
        records: dict[str, ImageBuildRecord],
    ) -> dict[str, ImageBuildRecord]:
        """Retain every active build plus the newest bounded terminal history."""
        active = {
            build_id: record
            for build_id, record in records.items()
            if not record.terminal
        }
        terminal = sorted(
            (record for record in records.values() if record.terminal),
            key=lambda record: (
                record.finished_at or record.updated_at or record.created_at,
                record.build_id,
            ),
            reverse=True,
        )[: self.max_terminal_builds]
        return {
            **active,
            **{record.build_id: record for record in terminal},
        }


class ImageManager:
    def __init__(
        self,
        store: ImageStore,
        runtime: DockerImageRuntime,
        *,
        build_store: ImageBuildStore | None = None,
        max_active_builds: int = 4,
        max_concurrent_pulls: int = 8,
        max_concurrent_context_preparations: int = 2,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.build_store = build_store or ImageBuildStore(
            default_image_build_file(store.path)
        )
        self.max_active_builds = max(1, max_active_builds)
        self.max_concurrent_pulls = max(1, max_concurrent_pulls)
        self.max_concurrent_context_preparations = max(
            1,
            max_concurrent_context_preparations,
        )
        self._build_lock = RLock()
        self._build_conditions: dict[str, Condition] = {}
        self._active_threads: dict[str, Thread] = {}
        self._active_image_operations = 0
        self._active_pulls = 0
        self._waiting_pulls = 0
        self._pull_slots = BoundedSemaphore(self.max_concurrent_pulls)
        self._context_preparation_slots = BoundedSemaphore(
            self.max_concurrent_context_preparations
        )
        self._context_preparation_condition = Condition(self._build_lock)
        self._preparing_image_ids: set[str] = set()
        self._preparing_tags: set[str] = set()
        self._active_context_preparations = 0
        self._waiting_context_preparations = 0
        self._pending_build_logs: dict[str, str] = {}
        self._build_log_last_flush: dict[str, float] = {}
        self.reconcile_interrupted_builds()

    def list(self) -> list[ImageRecord]:
        return list(self.store.load().values())

    def get_image(self, image_id: str) -> ImageRecord | None:
        return self.store.load().get(image_id)

    def list_builds(self) -> list[ImageBuildRecord]:
        return list(self.build_store.load().values())

    def active_build_count(self) -> int:
        builds = sum(
            1 for record in self.build_store.load().values() if not record.terminal
        )
        with self._build_lock:
            return builds + self._active_image_operations

    @contextmanager
    def image_operation(self):
        """Fence node drain across pull plus direct-rootfs materialization."""

        with self._build_lock:
            self._active_image_operations += 1
        try:
            yield
        finally:
            with self._build_lock:
                self._active_image_operations -= 1

    @contextmanager
    def pull_slot(self):
        """Bound distinct cold pulls without reducing warm-create concurrency."""

        queued_at = time.monotonic()
        with self._build_lock:
            self._waiting_pulls += 1
        self._pull_slots.acquire()
        admitted_at = time.monotonic()
        with self._build_lock:
            self._waiting_pulls -= 1
            self._active_pulls += 1
        try:
            yield {"queue_wait_ms": int(max(0.0, admitted_at - queued_at) * 1000)}
        finally:
            with self._build_lock:
                self._active_pulls -= 1
            self._pull_slots.release()

    def pull_operation_snapshot(self) -> dict[str, int]:
        with self._build_lock:
            return {
                "active_operations": self._active_pulls,
                "waiting_operations": self._waiting_pulls,
                "max_concurrent_operations": self.max_concurrent_pulls,
            }

    def context_preparation_snapshot(self) -> dict[str, int]:
        with self._build_lock:
            return {
                "active_operations": self._active_context_preparations,
                "waiting_operations": self._waiting_context_preparations,
                "max_concurrent_operations": (
                    self.max_concurrent_context_preparations
                ),
            }

    @contextmanager
    def _local_context_preparation(self, image_id: str, tag: str):
        acquired_slot = False
        with self._context_preparation_condition:
            self._waiting_context_preparations += 1
            while (
                image_id in self._preparing_image_ids
                or tag in self._preparing_tags
            ):
                self._context_preparation_condition.wait()
            self._preparing_image_ids.add(image_id)
            self._preparing_tags.add(tag)
        try:
            self._context_preparation_slots.acquire()
            acquired_slot = True
            with self._build_lock:
                self._waiting_context_preparations -= 1
                self._active_context_preparations += 1
            yield
        finally:
            if acquired_slot:
                with self._build_lock:
                    self._active_context_preparations -= 1
                self._context_preparation_slots.release()
            else:
                with self._build_lock:
                    self._waiting_context_preparations -= 1
            with self._context_preparation_condition:
                self._preparing_image_ids.discard(image_id)
                self._preparing_tags.discard(tag)
                self._context_preparation_condition.notify_all()

    def get_build(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        with self._build_lock:
            record = self.build_store.get(build_id_or_image_id)
            if record is not None and record.build_id in self._pending_build_logs:
                self._flush_build_log_locked(record.build_id)
                record = self.build_store.get(build_id_or_image_id)
            return record

    def reconcile_interrupted_builds(self) -> tuple[ImageBuildRecord, ...]:
        """Fail persisted non-terminal builds that have no worker after restart."""
        with self._build_lock:
            return self.build_store.reconcile_interrupted()

    def build(self, spec: ImageBuildSpec) -> tuple[ImageRecord, CommandResult]:
        spec.validate()
        result = self.runtime.build(spec)
        now = utc_now()
        record = ImageRecord(
            id=spec.id,
            tag=spec.tag,
            source=f"build:{spec.context_path}",
            state="planned" if self.runtime.dry_run else "available",
            created_at=now,
            updated_at=now,
            labels=spec.labels,
        )
        self.store.upsert(record)
        return record, result

    def start_build(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool = False,
        cleanup: Callable[[], None] | None = None,
        context_identity: str | None = None,
        materialize_context: Callable[[], MaterializedBuildContext] | None = None,
    ) -> tuple[ImageBuildRecord, bool]:
        spec.validate()
        if context_identity is None:
            with self._local_context_preparation(spec.id, spec.tag):
                return self._start_build_prepared(
                    spec,
                    push=push,
                    cleanup=cleanup,
                    context_identity=None,
                    materialize_context=materialize_context,
                )
        return self._start_build_prepared(
            spec,
            push=push,
            cleanup=cleanup,
            context_identity=context_identity,
            materialize_context=materialize_context,
        )

    def _start_build_prepared(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool,
        cleanup: Callable[[], None] | None,
        context_identity: str | None,
        materialize_context: Callable[[], MaterializedBuildContext] | None,
    ) -> tuple[ImageBuildRecord, bool]:
        spec.validate()
        spec = replace(
            spec,
            dockerfile=_normalize_dockerfile_path(spec.dockerfile),
        )
        try:
            if context_identity is None:
                local_context_path = Path(spec.context_path)
                context_identity = (
                    f"tree:{image_build_context_digest(local_context_path)}"
                )

                def materialize_local_context() -> MaterializedBuildContext:
                    return snapshot_local_build_context(local_context_path)

                materialize_context = materialize_local_context
            elif materialize_context is None:
                raise ValueError(
                    "an immutable context identity requires a context materializer"
                )
            request_fingerprint = image_build_fingerprint(
                spec,
                push=push,
                context_identity=context_identity,
            )
            direct_push = self.runtime.uses_direct_push(push=push)
            logical_command = (
                self.runtime.build_command(spec, push=True)
                if direct_push
                else self.runtime.build_command(spec)
            )
            now = utc_now().isoformat()
            record = ImageBuildRecord(
                build_id=str(uuid4()),
                image_id=spec.id,
                tag=spec.tag,
                status="running",
                created_at=now,
                updated_at=now,
                started_at=now,
                context_path=spec.context_path,
                dockerfile=spec.dockerfile,
                push=push,
                command=logical_command,
                push_command=(
                    self.runtime.push_command(spec.tag)
                    if push and not direct_push
                    else ()
                ),
                timings={"total_ms": None, "phases": {}},
                owner_pid=os.getpid(),
                request_fingerprint=request_fingerprint,
            )
            with self._build_lock:
                record, build_started = self.build_store.reserve_build(
                    record,
                    max_active_builds=self.max_active_builds,
                )
                if not build_started:
                    if cleanup is not None:
                        cleanup()
                    return record, False
                self._build_conditions[record.build_id] = Condition(self._build_lock)
                self._build_log_last_flush[record.build_id] = time.monotonic()
        except Exception:
            if cleanup is not None:
                cleanup()
            raise

        materialized: MaterializedBuildContext | None = None
        effective_cleanup = cleanup
        try:
            assert materialize_context is not None
            materialized = materialize_context()
            effective_cleanup = _combine_cleanup(materialized.cleanup, cleanup)
            if materialized.context_identity != context_identity:
                raise ImageBuildConflictError(
                    "image build context changed before its immutable snapshot"
                )
            _validate_materialized_dockerfile(
                materialized.path,
                spec.dockerfile,
            )
            effective_spec = replace(
                spec,
                context_path=str(materialized.path),
            )
            effective_command = (
                self.runtime.build_command(effective_spec, push=True)
                if direct_push
                else self.runtime.build_command(effective_spec)
            )
            record = replace(
                record,
                context_path=effective_spec.context_path,
                command=effective_command,
                updated_at=utc_now().isoformat(),
            )
            self.build_store.upsert(record)
        except Exception as exc:
            failure: Exception = exc
            if effective_cleanup is not None:
                try:
                    effective_cleanup()
                except Exception as cleanup_error:
                    failure = RuntimeError(
                        f"{exc}; build context cleanup failed: {cleanup_error}"
                    )
            with self._build_lock:
                self._fail_reserved_build_locked(record, failure)
            if failure is not exc:
                raise failure from exc
            raise

        build_id = record.build_id
        thread = Thread(
            target=self._run_tracked_build,
            args=(
                build_id,
                effective_spec,
                push,
                direct_push,
                effective_cleanup,
            ),
            daemon=True,
        )
        try:
            with self._build_lock:
                self._active_threads[build_id] = thread
                try:
                    thread.start()
                except Exception as exc:
                    self._fail_reserved_build_locked(record, exc)
                    raise
        except Exception:
            if effective_cleanup is not None:
                effective_cleanup()
            raise
        return record, True

    def _fail_reserved_build_locked(
        self,
        record: ImageBuildRecord,
        error: Exception,
    ) -> ImageBuildRecord:
        build_id = record.build_id
        self._active_threads.pop(build_id, None)
        self._pending_build_logs.pop(build_id, None)
        self._build_log_last_flush.pop(build_id, None)
        condition = self._build_conditions.pop(build_id, None)
        failed_at = utc_now().isoformat()
        failed = replace(
            record,
            status="failed",
            error=f"image build failed before worker start: {error}",
            updated_at=failed_at,
            finished_at=failed_at,
        )
        self.build_store.upsert(failed)
        if condition is not None:
            condition.notify_all()
        return failed

    def wait_for_build(
        self,
        build_id_or_image_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ImageBuildRecord | None:
        deadline = (
            None if timeout_seconds is None else utc_now().timestamp() + timeout_seconds
        )
        with self._build_lock:
            while True:
                record = self.build_store.get(build_id_or_image_id)
                if record is None:
                    return record
                condition = self._build_conditions.get(record.build_id)
                if record.terminal and condition is None:
                    return record
                if condition is None:
                    return record
                wait_seconds = 0.5
                if deadline is not None:
                    remaining = deadline - utc_now().timestamp()
                    if remaining <= 0:
                        return record
                    wait_seconds = min(wait_seconds, remaining)
                condition.wait(wait_seconds)

    def mark_pushed(
        self,
        image_id: str,
        *,
        manifest_digest: str = "",
    ) -> ImageRecord:
        records = self.store.load()
        record = records.get(image_id)
        if record is None:
            raise ValueError(f"image record not found: {image_id}")
        normalized_digest = normalize_manifest_digest(manifest_digest)
        updated = replace(
            record,
            pushed=True,
            manifest_digest=normalized_digest or record.manifest_digest,
            updated_at=utc_now(),
        )
        self.store.upsert(updated)
        return updated

    def pull(
        self, image: str, image_id: str | None = None
    ) -> tuple[ImageRecord, CommandResult]:
        result = self.runtime.pull(image)
        now = utc_now()
        record = ImageRecord(
            id=image_id or image_id_from_tag(image),
            tag=image,
            source="registry",
            state="planned" if self.runtime.dry_run else "available",
            created_at=now,
            updated_at=now,
            pushed=True,
            manifest_digest=manifest_digest_from_image_ref(image),
        )
        self.store.upsert(record)
        return record, result

    def record_snapshot(
        self,
        *,
        image_id: str,
        image: str,
        sandbox_id: str,
        dry_run: bool,
    ) -> ImageRecord:
        now = utc_now()
        record = ImageRecord(
            id=image_id,
            tag=image,
            source=f"snapshot:{sandbox_id}",
            state="planned" if dry_run else "available",
            created_at=now,
            updated_at=now,
            manifest_digest=manifest_digest_from_image_ref(image),
        )
        self.store.upsert(record)
        return record

    def _run_tracked_build(
        self,
        build_id: str,
        spec: ImageBuildSpec,
        push: bool,
        direct_push: bool,
        cleanup: Callable[[], None] | None,
    ) -> None:
        build_result: CommandResult | None = None
        push_result: CommandResult | None = None
        started = time.monotonic()
        phases: dict[str, int] = {}

        def append_output(stream: str, chunk: str) -> None:
            self._append_build_log(build_id, stream, chunk)

        try:
            phase = time.monotonic()
            try:
                build_result = self.runtime.build(
                    spec,
                    push=direct_push,
                    on_output=append_output,
                )
            finally:
                phase_name = (
                    "docker_build_and_push_ms" if direct_push else "docker_build_ms"
                )
                phases[phase_name] = _elapsed_ms(phase)
                self._update_build_timings(build_id, phases, started)
            if push and not direct_push:
                phase = time.monotonic()
                try:
                    push_result = self.runtime.push(
                        spec.tag,
                        on_output=lambda stream, chunk: self._append_build_log(
                            build_id,
                            stream,
                            chunk,
                        ),
                    )
                finally:
                    phases["docker_push_ms"] = _elapsed_ms(phase)
                    self._update_build_timings(build_id, phases, started)
            now = utc_now()
            image_record = ImageRecord(
                id=spec.id,
                tag=spec.tag,
                source=f"build:{spec.context_path}",
                state="planned" if self.runtime.dry_run else "available",
                created_at=now,
                updated_at=now,
                labels=spec.labels,
                pushed=push,
            )
            self.store.upsert(image_record)
            self._update_build(
                build_id,
                status="succeeded",
                exit_code=build_result.exit_code,
                push_exit_code=push_result.exit_code
                if push_result is not None
                else None,
                image=image_record.to_dict(),
                finished_at=now.isoformat(),
                timings=_build_timings(phases, started),
            )
        except Exception as exc:
            self._update_build(
                build_id,
                status="failed",
                error=str(exc),
                exit_code=build_result.exit_code if build_result is not None else None,
                push_exit_code=push_result.exit_code
                if push_result is not None
                else None,
                finished_at=utc_now().isoformat(),
                timings=_build_timings(phases, started),
            )
        finally:
            try:
                if cleanup is not None:
                    phase = time.monotonic()
                    try:
                        cleanup()
                    finally:
                        phases["cleanup_ms"] = _elapsed_ms(phase)
                        self._update_build_timings(build_id, phases, started)
            finally:
                with self._build_lock:
                    self._active_threads.pop(build_id, None)
                    self._pending_build_logs.pop(build_id, None)
                    self._build_log_last_flush.pop(build_id, None)
                    condition = self._build_conditions.pop(build_id, None)
                    if condition is not None:
                        condition.notify_all()

    def _append_build_log(self, build_id: str, stream: str, chunk: str) -> None:
        if not chunk:
            return
        prefix = "" if stream == "combined" else f"[{stream}] "
        with self._build_lock:
            pending = _tail_text(
                self._pending_build_logs.get(build_id, "") + prefix + chunk
            )
            self._pending_build_logs[build_id] = pending
            last_flush = self._build_log_last_flush.get(build_id, 0.0)
            if (
                len(pending) >= BUILD_LOG_FLUSH_CHARS
                or time.monotonic() - last_flush >= BUILD_LOG_FLUSH_INTERVAL_SECONDS
            ):
                self._flush_build_log_locked(build_id)

    def _update_build(self, build_id: str, **changes: Any) -> ImageBuildRecord | None:
        with self._build_lock:
            record = self.build_store.get(build_id)
            if record is None:
                return None
            pending = self._pending_build_logs.pop(build_id, "")
            if pending:
                record = replace(
                    record,
                    log_tail=_tail_text(record.log_tail + pending),
                )
                self._build_log_last_flush[build_id] = time.monotonic()
            updated = replace(record, updated_at=utc_now().isoformat(), **changes)
            self.build_store.upsert(updated)
            condition = self._build_conditions.get(build_id)
            if condition is not None:
                condition.notify_all()
            return updated

    def _flush_build_log_locked(self, build_id: str) -> ImageBuildRecord | None:
        pending = self._pending_build_logs.pop(build_id, "")
        if not pending:
            return self.build_store.get(build_id)
        record = self.build_store.get(build_id)
        if record is None:
            return None
        updated = replace(
            record,
            log_tail=_tail_text(record.log_tail + pending),
            updated_at=utc_now().isoformat(),
        )
        self.build_store.upsert(updated)
        self._build_log_last_flush[build_id] = time.monotonic()
        condition = self._build_conditions.get(build_id)
        if condition is not None:
            condition.notify_all()
        return updated

    def _update_build_timings(
        self,
        build_id: str,
        phases: dict[str, int],
        started: float,
    ) -> None:
        self._update_build(build_id, timings=_build_timings(phases, started))


def image_id_from_tag(image: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", image).strip("-.")
    if not cleaned:
        return "image"
    return cleaned[:64]


def _build_timings(phases: dict[str, int], started: float) -> dict[str, Any]:
    return {
        "total_ms": _elapsed_ms(started),
        "phases": dict(phases),
    }


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def _image_lock(path: Path) -> _AdvisoryFileLock:
    key = path.resolve()
    with _IMAGE_LOCKS_GUARD:
        lock = _IMAGE_LOCKS.get(key)
        if lock is None:
            lock = _AdvisoryFileLock(key)
            _IMAGE_LOCKS[key] = lock
        return lock


def default_image_build_file(image_file: Path) -> Path:
    return image_file.with_name(f"{image_file.stem}-builds{image_file.suffix}")


def _optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _tail_text(value: str, *, limit: int = BUILD_LOG_TAIL_CHARS) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def _dockerfile_path(context_path: str, dockerfile: str) -> str:
    return str(Path(context_path) / _normalize_dockerfile_path(dockerfile))


def _normalize_dockerfile_path(dockerfile: str) -> str:
    if not dockerfile.strip() or "\\" in dockerfile:
        raise ValueError("dockerfile must be a context-relative path")
    path = Path(dockerfile)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("dockerfile must stay within the build context")
    normalized = path.as_posix()
    if normalized in {"", "."}:
        raise ValueError("dockerfile must name a file within the build context")
    return normalized


def _validate_materialized_dockerfile(
    context_path: Path,
    dockerfile: str,
) -> None:
    root = context_path.resolve(strict=True)
    candidate = root / _normalize_dockerfile_path(dockerfile)
    if not candidate.exists() and not candidate.is_symlink():
        return
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ValueError(
            "dockerfile must stay within the immutable build context"
        ) from exc
    if not resolved.is_file():
        raise ValueError("dockerfile must name a regular file")


@contextmanager
def uploaded_build_context(
    raw: dict[str, Any],
    context_store: BuildContextBlobStore | None = None,
):
    context = materialize_uploaded_build_context(raw, context_store)
    if context is None:
        yield None
        return
    try:
        yield context.path
    finally:
        context.cleanup()


@dataclass
class MaterializedBuildContext:
    path: Path
    _temporary_directory: tempfile.TemporaryDirectory[str]
    context_identity: str

    def cleanup(self) -> None:
        self._temporary_directory.cleanup()


def uploaded_build_context_reference(
    raw: dict[str, Any],
    context_store: BuildContextBlobStore | None,
) -> tuple[str, int] | None:
    digest = raw.get("context_archive_digest")
    if digest is None:
        return None
    if not isinstance(digest, str):
        raise ValueError("context_archive_digest must be a string.")
    if raw.get("context_archive_format") != "tar.gz":
        raise ValueError("unsupported context_archive_format; expected tar.gz.")
    if raw.get("context_path") != ".":
        raise ValueError("context_path must be '.' for an uploaded build context.")
    archive_size = raw.get("context_archive_size")
    if isinstance(archive_size, bool) or not isinstance(archive_size, int):
        raise ValueError("context_archive_size must be a non-negative integer.")
    if archive_size < 0:
        raise ValueError("context_archive_size must be a non-negative integer.")
    if context_store is None:
        raise ValueError("content-addressed build contexts are not configured.")
    try:
        stored_size = context_store.size(digest)
    except (FileNotFoundError, ValueError) as exc:
        raise ValueError(f"build context {digest!r} has not been uploaded.") from exc
    if stored_size != archive_size:
        raise ValueError(
            f"build context size mismatch: expected {archive_size}, stored {stored_size}."
        )
    return digest, stored_size


def materialize_uploaded_build_context(
    raw: dict[str, Any],
    context_store: BuildContextBlobStore | None = None,
) -> MaterializedBuildContext | None:
    reference = uploaded_build_context_reference(raw, context_store)
    if reference is not None:
        digest, _ = reference
        temporary_directory: tempfile.TemporaryDirectory[str] = (
            tempfile.TemporaryDirectory(prefix="ucloud-image-context-")
        )
        context_dir = Path(temporary_directory.name)
        try:
            with context_store.open(digest) as archive_file:
                _extract_safe_tar_gz_file(archive_file, context_dir)
            context_store.touch(digest)
        except FileNotFoundError as exc:
            temporary_directory.cleanup()
            raise ValueError(
                f"build context {digest!r} has not been uploaded."
            ) from exc
        except Exception:
            temporary_directory.cleanup()
            raise
        return MaterializedBuildContext(
            context_dir,
            temporary_directory,
            f"archive:{digest}",
        )

    return None


def snapshot_local_build_context(context_path: Path) -> MaterializedBuildContext:
    try:
        source = context_path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"image build context is unavailable: {context_path}") from exc
    if not source.is_dir():
        raise ValueError(f"image build context must be a directory: {context_path}")
    temporary_directory: tempfile.TemporaryDirectory[str] = (
        tempfile.TemporaryDirectory(prefix="ucloud-image-context-")
    )
    destination = Path(temporary_directory.name)
    try:
        ignore = None
        try:
            nested_destination = destination.relative_to(source)
        except ValueError:
            pass
        else:
            nested_parent = Path(*nested_destination.parts[:-1])
            nested_name = nested_destination.parts[-1]

            def ignore_nested_destination(
                current: str,
                names: list[str],
            ) -> set[str]:
                relative = Path(current).relative_to(source)
                if relative == nested_parent and nested_name in names:
                    return {nested_name}
                return set()

            ignore = ignore_nested_destination
        shutil.copytree(
            source,
            destination,
            dirs_exist_ok=True,
            symlinks=True,
            ignore=ignore,
        )
        digest = image_build_context_digest(destination)
    except Exception:
        temporary_directory.cleanup()
        raise
    return MaterializedBuildContext(
        destination,
        temporary_directory,
        f"tree:{digest}",
    )


def _combine_cleanup(
    first: Callable[[], None],
    second: Callable[[], None] | None,
) -> Callable[[], None]:
    if second is None:
        return first

    def cleanup() -> None:
        try:
            first()
        finally:
            second()

    return cleanup


def _extract_safe_tar_gz_file(
    payload: Any,
    destination: Path,
    *,
    max_total_bytes: int = MAX_BUILD_CONTEXT_EXTRACTED_BYTES,
    max_member_bytes: int = MAX_BUILD_CONTEXT_MEMBER_BYTES,
    max_members: int = MAX_BUILD_CONTEXT_MEMBERS,
    max_archive_bytes: int = MAX_BUILD_CONTEXT_DECOMPRESSED_ARCHIVE_BYTES,
) -> None:
    if min(max_total_bytes, max_member_bytes, max_members, max_archive_bytes) < 0:
        raise ValueError("context archive limits must be non-negative.")
    try:
        with gzip.GzipFile(fileobj=payload, mode="rb") as decompressed:
            limited = _ByteLimitedReader(decompressed, max_archive_bytes)
            with tarfile.open(fileobj=limited, mode="r|") as archive:
                member_count = 0
                total_bytes = 0
                for member in archive:
                    member_count += 1
                    if member_count > max_members:
                        raise ValueError(
                            f"context archive exceeds the {max_members} member limit."
                        )
                    _validate_context_member(member)
                    if member.isfile():
                        if member.size < 0:
                            raise ValueError(
                                "invalid file size in context archive: "
                                f"{member.name!r}"
                            )
                        if member.size > max_member_bytes:
                            raise ValueError(
                                "context archive member exceeds the "
                                f"{max_member_bytes} byte limit: {member.name!r}"
                            )
                        if total_bytes + member.size > max_total_bytes:
                            raise ValueError(
                                "context archive exceeds the "
                                f"{max_total_bytes} extracted-byte limit."
                            )
                        total_bytes += member.size
                    archive.extract(member, destination)
            while limited.read(64 * 1024):
                pass
    except (gzip.BadGzipFile, tarfile.TarError, EOFError) as exc:
        raise ValueError("context archive is not a valid tar.gz file.") from exc


class _ByteLimitedReader:
    def __init__(self, stream: Any, limit: int) -> None:
        self._stream = stream
        self._remaining = limit
        self._limit = limit

    def read(self, size: int = -1) -> bytes:
        requested = self._remaining + 1 if size < 0 else min(size, self._remaining + 1)
        chunk = self._stream.read(requested)
        if len(chunk) > self._remaining:
            raise ValueError(
                "context archive exceeds the "
                f"{self._limit} decompressed-byte limit."
            )
        self._remaining -= len(chunk)
        return chunk


def _validate_context_member(member: tarfile.TarInfo) -> None:
    name = member.name
    path = Path(name)
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe path in context archive: {name!r}")
    if member.islnk() or member.issym():
        raise ValueError(f"links are not supported in context archives: {name!r}")
    if not (member.isfile() or member.isdir()):
        raise ValueError(f"unsupported file type in context archive: {name!r}")
