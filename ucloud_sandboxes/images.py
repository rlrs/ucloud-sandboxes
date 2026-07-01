from __future__ import annotations

import base64
import binascii
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
from threading import Condition, RLock, Thread
from typing import Any, Callable
from uuid import uuid4

from .models import parse_iso_datetime, utc_now
from .sandbox import CommandExecutor, CommandResult, SubprocessExecutor


IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
BUILD_TERMINAL_STATES = {"succeeded", "failed"}
BUILD_LOG_TAIL_CHARS = 64 * 1024
_IMAGE_LOCKS_GUARD = RLock()
_IMAGE_LOCKS: dict[Path, RLock] = {}


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
            dockerfile=str(raw.get("dockerfile") or "Dockerfile"),
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ImageRecord":
        created_at = parse_iso_datetime(raw.get("created_at"))
        updated_at = parse_iso_datetime(raw.get("updated_at"))
        if created_at is None or updated_at is None:
            raise ValueError("image record has invalid timestamps.")
        labels = raw.get("labels") or {}
        return cls(
            id=str(raw.get("id") or ""),
            tag=str(raw.get("tag") or ""),
            source=str(raw.get("source") or ""),
            state=str(raw.get("state") or ""),
            created_at=created_at,
            updated_at=updated_at,
            labels={str(k): str(v) for k, v in dict(labels).items()},
            pushed=bool(raw.get("pushed", False)),
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
            "available_to_sandboxes": self.available_to_sandboxes,
        }

    @property
    def available_to_sandboxes(self) -> bool:
        return self.pushed or self.source == "registry"


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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ImageBuildRecord | None":
        build_id = str(raw.get("build_id") or raw.get("buildId") or "")
        image_id = str(raw.get("image_id") or raw.get("imageId") or "")
        tag = str(raw.get("tag") or "")
        status = str(raw.get("status") or "")
        created_at = str(raw.get("created_at") or raw.get("createdAt") or "")
        updated_at = str(raw.get("updated_at") or raw.get("updatedAt") or "")
        if not build_id or not image_id or not status or not created_at or not updated_at:
            return None
        command = raw.get("command") or ()
        push_command = raw.get("push_command") or raw.get("pushCommand") or ()
        image = raw.get("image") if isinstance(raw.get("image"), dict) else {}
        return cls(
            build_id=build_id,
            image_id=image_id,
            tag=tag,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            context_path=str(raw.get("context_path") or raw.get("contextPath") or ""),
            dockerfile=str(raw.get("dockerfile") or "Dockerfile"),
            push=bool(raw.get("push", False)),
            command=tuple(str(item) for item in command),
            push_command=tuple(str(item) for item in push_command),
            exit_code=_optional_int(raw.get("exit_code") or raw.get("exitCode")),
            push_exit_code=_optional_int(raw.get("push_exit_code") or raw.get("pushExitCode")),
            error=str(raw.get("error") or ""),
            log_tail=str(raw.get("log_tail") or raw.get("logTail") or ""),
            started_at=str(raw.get("started_at") or raw.get("startedAt") or ""),
            finished_at=str(raw.get("finished_at") or raw.get("finishedAt") or ""),
            image={str(key): value for key, value in image.items()},
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
    ) -> None:
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.dry_run = dry_run

    def build(
        self,
        spec: ImageBuildSpec,
        *,
        on_output: Callable[[str, str], None] | None = None,
    ) -> CommandResult:
        return self._run(self.build_command(spec), on_output=on_output)

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

    def build_command(self, spec: ImageBuildSpec) -> tuple[str, ...]:
        spec.validate()
        dockerfile = _dockerfile_path(spec.context_path, spec.dockerfile)
        argv: list[str] = [
            self.docker_binary,
            "build",
            "-f",
            dockerfile,
            "-t",
            spec.tag,
            "--label",
            "ucloud-sandboxes.image=true",
            "--label",
            f"ucloud-sandboxes.image-id={spec.id}",
        ]
        for key in sorted(spec.build_args):
            argv.extend(["--build-arg", f"{key}={spec.build_args[key]}"])
        for key in sorted(spec.labels):
            argv.extend(["--label", f"{key}={spec.labels[key]}"])
        argv.append(spec.context_path)
        return tuple(argv)

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
        chunks: list[str] = []
        assert process.stdout is not None
        for chunk in process.stdout:
            chunks.append(chunk)
            on_output("combined", chunk)
        exit_code = process.wait()
        output = "".join(chunks)
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
        with self._lock:
            if not self.path.exists():
                return {}
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("image store must contain a JSON object.")
            items = raw.get("images", [])
            if not isinstance(items, list):
                raise ValueError("image store must contain an images list.")
            records: dict[str, ImageRecord] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                record = ImageRecord.from_dict(item)
                records[record.id] = record
            return records

    def save(self, records: dict[str, ImageRecord]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            payload = {
                "images": [records[image_id].to_dict() for image_id in sorted(records)]
            }
            tmp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)

    def upsert(self, record: ImageRecord) -> dict[str, ImageRecord]:
        with self._lock:
            records = self.load()
            records[record.id] = record
            self.save(records)
            return records


class ImageBuildStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _image_lock(path)

    def load(self) -> dict[str, ImageBuildRecord]:
        with self._lock:
            if not self.path.exists():
                return {}
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("image build store must contain a JSON object.")
            items = raw.get("builds", [])
            if not isinstance(items, list):
                raise ValueError("image build store must contain a builds list.")
            records: dict[str, ImageBuildRecord] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                record = ImageBuildRecord.from_dict(item)
                if record is not None:
                    records[record.build_id] = record
            return records

    def save(self, records: dict[str, ImageBuildRecord]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            payload = {
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
            }
            tmp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)

    def upsert(self, record: ImageBuildRecord) -> dict[str, ImageBuildRecord]:
        with self._lock:
            records = self.load()
            records[record.build_id] = record
            self.save(records)
            return records

    def get(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        records = self.load()
        exact = records.get(build_id_or_image_id)
        if exact is not None:
            return exact
        matches = [
            record for record in records.values()
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


class ImageManager:
    def __init__(
        self,
        store: ImageStore,
        runtime: DockerImageRuntime,
        *,
        build_store: ImageBuildStore | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.build_store = build_store or ImageBuildStore(default_image_build_file(store.path))
        self._build_lock = RLock()
        self._build_conditions: dict[str, Condition] = {}
        self._active_threads: dict[str, Thread] = {}

    def list(self) -> list[ImageRecord]:
        return list(self.store.load().values())

    def get_image(self, image_id: str) -> ImageRecord | None:
        return self.store.load().get(image_id)

    def list_builds(self) -> list[ImageBuildRecord]:
        return list(self.build_store.load().values())

    def active_build_count(self) -> int:
        return sum(1 for record in self.build_store.load().values() if not record.terminal)

    def get_build(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        return self.build_store.get(build_id_or_image_id)

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
    ) -> tuple[ImageBuildRecord, bool]:
        spec.validate()
        with self._build_lock:
            active = self.build_store.active_for_image(spec.id, tag=spec.tag)
            if active is not None:
                if cleanup is not None:
                    cleanup()
                return active, False
            now = utc_now().isoformat()
            build_id = str(uuid4())
            record = ImageBuildRecord(
                build_id=build_id,
                image_id=spec.id,
                tag=spec.tag,
                status="running",
                created_at=now,
                updated_at=now,
                started_at=now,
                context_path=spec.context_path,
                dockerfile=spec.dockerfile,
                push=push,
                command=self.runtime.build_command(spec),
                push_command=self.runtime.push_command(spec.tag) if push else (),
            )
            self.build_store.upsert(record)
            self._build_conditions[build_id] = Condition(self._build_lock)
            thread = Thread(
                target=self._run_tracked_build,
                args=(build_id, spec, push, cleanup),
                daemon=True,
            )
            self._active_threads[build_id] = thread
            thread.start()
            return record, True

    def wait_for_build(
        self,
        build_id_or_image_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ImageBuildRecord | None:
        deadline = None if timeout_seconds is None else utc_now().timestamp() + timeout_seconds
        with self._build_lock:
            while True:
                record = self.build_store.get(build_id_or_image_id)
                if record is None or record.terminal:
                    return record
                condition = self._build_conditions.get(record.build_id)
                if condition is None:
                    return record
                wait_seconds = 0.5
                if deadline is not None:
                    remaining = deadline - utc_now().timestamp()
                    if remaining <= 0:
                        return record
                    wait_seconds = min(wait_seconds, remaining)
                condition.wait(wait_seconds)

    def mark_pushed(self, image_id: str) -> ImageRecord:
        records = self.store.load()
        record = records.get(image_id)
        if record is None:
            raise ValueError(f"image record not found: {image_id}")
        updated = replace(record, pushed=True, updated_at=utc_now())
        self.store.upsert(updated)
        return updated

    def pull(self, image: str, image_id: str | None = None) -> tuple[ImageRecord, CommandResult]:
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
        )
        self.store.upsert(record)
        return record

    def _run_tracked_build(
        self,
        build_id: str,
        spec: ImageBuildSpec,
        push: bool,
        cleanup: Callable[[], None] | None,
    ) -> None:
        build_result: CommandResult | None = None
        push_result: CommandResult | None = None
        try:
            build_result = self.runtime.build(
                spec,
                on_output=lambda stream, chunk: self._append_build_log(
                    build_id,
                    stream,
                    chunk,
                ),
            )
            if push:
                push_result = self.runtime.push(
                    spec.tag,
                    on_output=lambda stream, chunk: self._append_build_log(
                        build_id,
                        stream,
                        chunk,
                    ),
                )
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
                push_exit_code=push_result.exit_code if push_result is not None else None,
                image=image_record.to_dict(),
                finished_at=now.isoformat(),
            )
        except Exception as exc:
            self._update_build(
                build_id,
                status="failed",
                error=str(exc),
                exit_code=build_result.exit_code if build_result is not None else None,
                push_exit_code=push_result.exit_code if push_result is not None else None,
                finished_at=utc_now().isoformat(),
            )
        finally:
            if cleanup is not None:
                cleanup()
            with self._build_lock:
                self._active_threads.pop(build_id, None)
                condition = self._build_conditions.get(build_id)
                if condition is not None:
                    condition.notify_all()

    def _append_build_log(self, build_id: str, stream: str, chunk: str) -> None:
        if not chunk:
            return
        prefix = "" if stream == "combined" else f"[{stream}] "
        with self._build_lock:
            record = self.build_store.get(build_id)
            if record is None:
                return
            updated = replace(
                record,
                log_tail=_tail_text(record.log_tail + prefix + chunk),
                updated_at=utc_now().isoformat(),
            )
            self.build_store.upsert(updated)
            condition = self._build_conditions.get(build_id)
            if condition is not None:
                condition.notify_all()

    def _update_build(self, build_id: str, **changes: Any) -> ImageBuildRecord | None:
        with self._build_lock:
            record = self.build_store.get(build_id)
            if record is None:
                return None
            updated = replace(record, updated_at=utc_now().isoformat(), **changes)
            self.build_store.upsert(updated)
            condition = self._build_conditions.get(build_id)
            if condition is not None:
                condition.notify_all()
            return updated


def image_id_from_tag(image: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", image).strip("-.")
    if not cleaned:
        return "image"
    return cleaned[:64]


def _image_lock(path: Path) -> RLock:
    key = path.resolve()
    with _IMAGE_LOCKS_GUARD:
        lock = _IMAGE_LOCKS.get(key)
        if lock is None:
            lock = RLock()
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


def _tail_text(value: str, *, limit: int = BUILD_LOG_TAIL_CHARS) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def _dockerfile_path(context_path: str, dockerfile: str) -> str:
    path = Path(dockerfile)
    if path.is_absolute():
        return dockerfile
    return str(Path(context_path) / path)


@contextmanager
def uploaded_build_context(raw: dict[str, Any]):
    context = materialize_uploaded_build_context(raw)
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

    def cleanup(self) -> None:
        self._temporary_directory.cleanup()


def materialize_uploaded_build_context(raw: dict[str, Any]) -> MaterializedBuildContext | None:
    archive = raw.get("context_archive_base64")
    if archive is None:
        return None
    if not isinstance(archive, str) or not archive:
        raise ValueError("context_archive_base64 must be a non-empty string.")
    archive_format = str(raw.get("context_archive_format") or "tar.gz")
    if archive_format != "tar.gz":
        raise ValueError("unsupported context_archive_format; expected tar.gz.")
    try:
        payload = base64.b64decode(archive.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise ValueError("context_archive_base64 is not valid base64.") from exc
    temporary_directory: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
        prefix="ucloud-image-context-"
    )
    context_dir = Path(temporary_directory.name)
    try:
        _extract_safe_tar_gz(payload, context_dir)
    except Exception:
        temporary_directory.cleanup()
        raise
    return MaterializedBuildContext(context_dir, temporary_directory)


def _extract_safe_tar_gz(payload: bytes, destination: Path) -> None:
    try:
        archive = tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz")
    except tarfile.TarError as exc:
        raise ValueError("context archive is not a valid tar.gz file.") from exc
    with archive:
        members = archive.getmembers()
        for member in members:
            _validate_context_member(member)
        archive.extractall(destination, members=members)


def _validate_context_member(member: tarfile.TarInfo) -> None:
    name = member.name
    path = Path(name)
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe path in context archive: {name!r}")
    if member.islnk() or member.issym():
        raise ValueError(f"links are not supported in context archives: {name!r}")
    if not (member.isfile() or member.isdir()):
        raise ValueError(f"unsupported file type in context archive: {name!r}")
