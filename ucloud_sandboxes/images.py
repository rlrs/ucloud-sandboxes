from __future__ import annotations

import base64
import binascii
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
import io
import json
from pathlib import Path
import re
import tarfile
import tempfile
from threading import RLock
from typing import Any

from .models import parse_iso_datetime, utc_now
from .sandbox import CommandExecutor, CommandResult, SubprocessExecutor


IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
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
        }


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

    def build(self, spec: ImageBuildSpec) -> CommandResult:
        return self._run(self.build_command(spec))

    def pull(self, image: str) -> CommandResult:
        if not image.strip():
            raise ValueError("image is required.")
        return self._run((self.docker_binary, "pull", image))

    def push(self, image: str) -> CommandResult:
        if not image.strip():
            raise ValueError("image is required.")
        return self._run((self.docker_binary, "push", image))

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

    def _run(self, argv: tuple[str, ...]) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        result = self.executor.run(argv)
        if result.exit_code != 0:
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
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


class ImageManager:
    def __init__(self, store: ImageStore, runtime: DockerImageRuntime) -> None:
        self.store = store
        self.runtime = runtime

    def list(self) -> list[ImageRecord]:
        return list(self.store.load().values())

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


def _dockerfile_path(context_path: str, dockerfile: str) -> str:
    path = Path(dockerfile)
    if path.is_absolute():
        return dockerfile
    return str(Path(context_path) / path)


@contextmanager
def uploaded_build_context(raw: dict[str, Any]):
    archive = raw.get("context_archive_base64")
    if archive is None:
        yield None
        return
    if not isinstance(archive, str) or not archive:
        raise ValueError("context_archive_base64 must be a non-empty string.")
    archive_format = str(raw.get("context_archive_format") or "tar.gz")
    if archive_format != "tar.gz":
        raise ValueError("unsupported context_archive_format; expected tar.gz.")
    try:
        payload = base64.b64decode(archive.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise ValueError("context_archive_base64 is not valid base64.") from exc
    with tempfile.TemporaryDirectory(prefix="ucloud-image-context-") as raw_dir:
        context_dir = Path(raw_dir)
        _extract_safe_tar_gz(payload, context_dir)
        yield context_dir


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
