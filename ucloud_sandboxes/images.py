from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import tarfile
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from threading import BoundedSemaphore, Condition, RLock, Thread
from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar
from uuid import uuid4

from .build_context_store import BuildContextBlobStore
from .managed_registry import (
    canonical_image_digest_ref,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
)
from .models import parse_iso_datetime, utc_now
from .sandbox import CommandExecutor, CommandResult, SubprocessExecutor

IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_UPLOADED_CONTEXT_IDENTITY_RE = re.compile(r"archive:sha256:[0-9a-f]{64}")
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

_IMAGE_STATE_ERROR = "image state database is invalid or unavailable"
_IMAGE_STATE_APPLICATION_ID = 0x55435349


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
    context_identity: str,
    push: bool = False,
) -> str:
    """Return the exact immutable identity used for build single-flight."""

    spec.validate()
    if _UPLOADED_CONTEXT_IDENTITY_RE.fullmatch(context_identity) is None:
        raise ValueError("image builds require an uploaded content-addressed context")
    payload = {
        "build_args": dict(spec.build_args),
        "context_identity": context_identity,
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
            set(raw) - set(cls.__dataclass_fields__) - {"available_to_sandboxes"}
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
        raw = asdict(self)
        raw.update(
            created_at=self.created_at.isoformat(),
            updated_at=self.updated_at.isoformat(),
            available_to_sandboxes=self.pushed or self.source == "registry",
        )
        return raw

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
        fields = set(cls.__dataclass_fields__)
        structured = set(
            "command exit_code image owner_pid push push_command push_exit_code timings".split()
        )
        if set(raw) != fields or any(
            type(raw[name]) is not str for name in fields - structured
        ):
            return None
        required = "build_id image_id status created_at updated_at".split()
        if not all(raw[name] for name in required):
            return None
        if any(
            type(raw[name]) is not list
            or any(type(item) is not str for item in raw[name])
            for name in ("command", "push_command")
        ):
            return None
        if (
            type(raw["push"]) is not bool
            or type(raw["owner_pid"]) is not int
            or raw["owner_pid"] < 0
            or any(
                value is not None and type(value) is not int
                for value in (raw["exit_code"], raw["push_exit_code"])
            )
            or any(
                type(raw[name]) is not dict
                or any(type(key) is not str for key in raw[name])
                for name in ("image", "timings")
            )
        ):
            return None
        values = raw | {
            "command": tuple(raw["command"]),
            "push_command": tuple(raw["push_command"]),
        }
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        raw.update(command=list(self.command), push_command=list(self.push_command))
        return raw

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


_ImageStateRecordT = TypeVar("_ImageStateRecordT", ImageRecord, ImageBuildRecord)


class _ImageStateStore(Generic[_ImageStateRecordT]):
    _TABLES = ("image_state_v1_images", "image_state_v1_builds")
    _COLUMNS = "record_id:TEXT:1:1 record_json:TEXT:1:0"
    _table: str
    _id_field: str
    _decode: Callable[[dict[str, Any]], _ImageStateRecordT | None]

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            application_id = int(conn.execute("PRAGMA application_id").fetchone()[0])
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if not tables and application_id == 0 and version == 0:
                for table in self._TABLES:
                    conn.execute(
                        f"CREATE TABLE {table} "
                        "(record_id TEXT PRIMARY KEY, record_json TEXT NOT NULL) STRICT"
                    )
                conn.execute(f"PRAGMA application_id = {_IMAGE_STATE_APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 1")
                tables = set(self._TABLES)
                application_id, version = _IMAGE_STATE_APPLICATION_ID, 1
            if (application_id, version, tables) != (
                _IMAGE_STATE_APPLICATION_ID,
                1,
                set(self._TABLES),
            ):
                raise sqlite3.DatabaseError("unsupported image state schema")
            for table in self._TABLES:
                columns = " ".join(
                    f"{row[1]}:{row[2]}:{row[3]}:{row[5]}"
                    for row in conn.execute(f"PRAGMA table_info({table})")
                )
                strict = conn.execute(
                    "SELECT strict FROM pragma_table_list WHERE name = ?", (table,)
                ).fetchone()
                if columns != self._COLUMNS or strict != (1,):
                    raise sqlite3.DatabaseError(f"invalid image state table: {table}")
            conn.commit()
            journal = conn.execute("PRAGMA journal_mode = DELETE").fetchone()
            if str(journal[0]).lower() != "delete":
                raise sqlite3.DatabaseError("image state requires DELETE journal mode")
        except sqlite3.Error as exc:
            raise ValueError(_IMAGE_STATE_ERROR) from exc
        finally:
            conn.close()
        os.chmod(path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        try:
            return sqlite3.connect(self.path, timeout=60, isolation_level=None)
        except sqlite3.Error as exc:
            raise ValueError(_IMAGE_STATE_ERROR) from exc

    @contextmanager
    def _transaction(self, *, write: bool) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except BaseException as exc:
            if conn.in_transaction:
                conn.rollback()
            if isinstance(exc, sqlite3.Error):
                raise ValueError(_IMAGE_STATE_ERROR) from exc
            raise
        finally:
            conn.close()

    def _load(self, conn: sqlite3.Connection) -> dict[str, _ImageStateRecordT]:
        records = {}
        for record_id, payload in conn.execute(
            f"SELECT record_id, record_json FROM {self._table} ORDER BY record_id"
        ):
            raw = json.loads(payload)
            record = self._decode(raw) if isinstance(raw, dict) else None
            if record is None or getattr(record, self._id_field) != record_id:
                raise ValueError("image state contains an invalid record")
            records[record_id] = record
        return records

    def load(self) -> dict[str, _ImageStateRecordT]:
        with self._transaction(write=False) as conn:
            return self._load(conn)

    def upsert(self, record: _ImageStateRecordT) -> None:
        with self._transaction(write=True) as conn:
            self._put(conn, record)
            self._compact(conn)

    def _put(self, conn: sqlite3.Connection, record: _ImageStateRecordT) -> None:
        conn.execute(
            f"INSERT OR REPLACE INTO {self._table} VALUES (?, ?)",
            (
                getattr(record, self._id_field),
                json.dumps(record.to_dict(), separators=(",", ":"), sort_keys=True),
            ),
        )

    def _compact(self, conn: sqlite3.Connection) -> None:
        del conn


class ImageStore(_ImageStateStore[ImageRecord]):
    _table = "image_state_v1_images"
    _id_field = "id"
    _decode = staticmethod(ImageRecord.from_dict)

    def delete_by_tags(self, tags: Iterable[str]) -> list[ImageRecord]:
        tag_set = {tag for tag in tags if tag}
        if not tag_set:
            return []
        with self._transaction(write=True) as conn:
            removed = [
                record for record in self._load(conn).values() if record.tag in tag_set
            ]
            conn.executemany(
                f"DELETE FROM {self._table} WHERE record_id = ?",
                ((record.id,) for record in removed),
            )
            return removed


class ImageBuildStore(_ImageStateStore[ImageBuildRecord]):
    _table = "image_state_v1_builds"
    _id_field = "build_id"
    _decode = staticmethod(ImageBuildRecord.from_dict)

    def __init__(
        self,
        path: Path,
        *,
        max_terminal_builds: int = DEFAULT_TERMINAL_BUILD_HISTORY,
    ) -> None:
        super().__init__(path)
        self.max_terminal_builds = max(0, max_terminal_builds)

    def reserve_build(
        self,
        record: ImageBuildRecord,
        *,
        max_active_builds: int,
    ) -> tuple[ImageBuildRecord, bool]:
        with self._transaction(write=True) as conn:
            records = self._load(conn)
            matching = sorted(
                (
                    existing
                    for existing in records.values()
                    if not existing.terminal
                    and (
                        existing.image_id == record.image_id
                        or existing.tag == record.tag
                    )
                ),
                key=lambda item: (item.created_at, item.build_id),
            )
            if matching:
                for existing in reversed(matching):
                    if (
                        existing.request_fingerprint
                        and existing.request_fingerprint == record.request_fingerprint
                    ):
                        return existing, False
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
            self._put(conn, record)
            self._compact(conn)
            return record, True

    def reconcile_interrupted(self) -> tuple[ImageBuildRecord, ...]:
        with self._transaction(write=True) as conn:
            now = utc_now().isoformat()
            interrupted: list[ImageBuildRecord] = []
            for record in self._load(conn).values():
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
                self._put(conn, updated)
                interrupted.append(updated)
            self._compact(conn)
            return tuple(interrupted)

    def get(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        records = self.load()
        if exact := records.get(build_id_or_image_id):
            return exact
        matches = [
            record
            for record in records.values()
            if record.image_id == build_id_or_image_id
        ]
        return max(
            matches, key=lambda item: (item.created_at, item.build_id), default=None
        )

    def _compact(self, conn: sqlite3.Connection) -> None:
        terminal = sorted(
            (record for record in self._load(conn).values() if record.terminal),
            key=lambda record: (
                record.finished_at or record.updated_at or record.created_at,
                record.build_id,
            ),
            reverse=True,
        )
        conn.executemany(
            f"DELETE FROM {self._table} WHERE record_id = ?",
            ((record.build_id,) for record in terminal[self.max_terminal_builds :]),
        )


class ImageManager:
    def __init__(
        self,
        store: ImageStore,
        runtime: DockerImageRuntime,
        *,
        build_store: ImageBuildStore | None = None,
        max_active_builds: int = 4,
        max_concurrent_pulls: int = 8,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.build_store = build_store or ImageBuildStore(store.path)
        self.max_active_builds = max(1, max_active_builds)
        self.max_concurrent_pulls = max(1, max_concurrent_pulls)
        self._build_lock = RLock()
        self._build_conditions: dict[str, Condition] = {}
        self._active_threads: dict[str, Thread] = {}
        self._active_image_operations = 0
        self._active_pulls = 0
        self._waiting_pulls = 0
        self._pull_slots = BoundedSemaphore(self.max_concurrent_pulls)
        self._pending_build_logs: dict[str, str] = {}
        self._build_log_last_flush: dict[str, float] = {}
        with self._build_lock:
            self.build_store.reconcile_interrupted()

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

    def get_build(self, build_id_or_image_id: str) -> ImageBuildRecord | None:
        with self._build_lock:
            record = self.build_store.get(build_id_or_image_id)
            if record is not None and record.build_id in self._pending_build_logs:
                self._flush_build_log_locked(record.build_id)
                record = self.build_store.get(build_id_or_image_id)
            return record

    def start_build(
        self,
        spec: ImageBuildSpec,
        *,
        context_identity: str,
        materialize_context: Callable[[], MaterializedBuildContext],
        push: bool = False,
        cleanup: Callable[[], None] | None = None,
    ) -> tuple[ImageBuildRecord, bool]:
        spec.validate()
        spec = replace(
            spec,
            dockerfile=_normalize_dockerfile_path(spec.dockerfile),
        )
        try:
            request_fingerprint = image_build_fingerprint(
                spec,
                context_identity=context_identity,
                push=push,
            )
            direct_push = push and self.runtime.buildx_direct_push
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


@dataclass
class MaterializedBuildContext:
    path: Path
    _temporary_directory: tempfile.TemporaryDirectory[str]
    context_identity: str

    def cleanup(self) -> None:
        self._temporary_directory.cleanup()


def uploaded_build_context_reference(
    raw: dict[str, Any],
    context_store: BuildContextBlobStore,
) -> tuple[str, int]:
    digest = raw.get("context_archive_digest")
    if digest is None:
        raise ValueError("context_archive_digest is required.")
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
    digest: str,
    context_store: BuildContextBlobStore,
) -> MaterializedBuildContext:
    temporary_directory: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
        prefix="ucloud-image-context-"
    )
    context_dir = Path(temporary_directory.name)
    try:
        with context_store.open(digest) as archive_file:
            _extract_safe_tar_gz_file(archive_file, context_dir)
        context_store.touch(digest)
    except FileNotFoundError as exc:
        temporary_directory.cleanup()
        raise ValueError(f"build context {digest!r} has not been uploaded.") from exc
    except Exception:
        temporary_directory.cleanup()
        raise
    return MaterializedBuildContext(
        context_dir,
        temporary_directory,
        f"archive:{digest}",
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
                                f"invalid file size in context archive: {member.name!r}"
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
                f"context archive exceeds the {self._limit} decompressed-byte limit."
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
