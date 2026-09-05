from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import base64
import binascii
import json
import re
from typing import Any, Mapping

from .models import parse_iso_datetime
from .guest_paths import validate_guest_path


MANAGED_PROCESS_BINARY = "/.ucloud-job-init"
MANAGED_PROCESS_PROTOCOL_VERSION = 1
DEFAULT_MAX_STDOUT_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_STDERR_BYTES = 16 * 1024 * 1024
MAX_LOG_READ_BYTES = 1024 * 1024
_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_ENV_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ManagedProcessError(RuntimeError):
    pass


@dataclass(frozen=True)
class ManagedProcessStart:
    job_id: str
    argv: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    max_stdout_bytes: int = DEFAULT_MAX_STDOUT_BYTES
    max_stderr_bytes: int = DEFAULT_MAX_STDERR_BYTES

    @classmethod
    def from_dict(cls, raw: object) -> "ManagedProcessStart":
        if not isinstance(raw, dict):
            raise ValueError("managed process payload must be a JSON object")
        unsupported = sorted(
            set(raw)
            - {
                "argv",
                "cwd",
                "env",
                "job_id",
                "max_stderr_bytes",
                "max_stdout_bytes",
            }
        )
        if unsupported:
            raise ValueError(
                "unsupported managed process fields: " + ", ".join(unsupported)
            )
        argv = raw.get("argv")
        env = raw.get("env") or {}
        if not isinstance(argv, list) or not all(
            isinstance(item, str) for item in argv
        ):
            raise ValueError("managed process argv must be a JSON string array")
        if not isinstance(env, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in env.items()
        ):
            raise ValueError("managed process env must contain strings")
        cwd = raw.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise ValueError("managed process cwd must be a string")
        job_id = raw.get("job_id")
        if not isinstance(job_id, str):
            raise ValueError("managed process job_id must be a string")
        stdout_limit = raw.get("max_stdout_bytes", DEFAULT_MAX_STDOUT_BYTES)
        stderr_limit = raw.get("max_stderr_bytes", DEFAULT_MAX_STDERR_BYTES)
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in (stdout_limit, stderr_limit)
        ):
            raise ValueError("managed process log limits must be integers")
        result = cls(
            job_id=job_id.strip(),
            argv=tuple(argv),
            env=dict(env),
            cwd=cwd,
            max_stdout_bytes=stdout_limit,
            max_stderr_bytes=stderr_limit,
        )
        result.validate()
        return result

    def validate(self) -> None:
        if not _JOB_ID.fullmatch(self.job_id):
            raise ValueError("managed process job_id is invalid")
        if not self.argv or len(self.argv) > 4096:
            raise ValueError("managed process argv must be non-empty and bounded")
        if any("\0" in item for item in self.argv):
            raise ValueError("managed process argv contains NUL")
        if self.cwd is not None:
            validate_guest_path("managed process cwd", self.cwd)
        for key, value in self.env.items():
            if not _ENV_KEY.fullmatch(key) or "\0" in value:
                raise ValueError("managed process environment is invalid")
        if self.max_stdout_bytes < 1 or self.max_stderr_bytes < 1:
            raise ValueError("managed process log limits must be positive")

    def control_payload(
        self, *, uid: int, gid: int, default_cwd: str | None = None
    ) -> dict[str, Any]:
        self.validate()
        cwd = self.cwd if self.cwd is not None else default_cwd
        validate_guest_path("resolved managed process cwd", cwd)
        return {
            "version": MANAGED_PROCESS_PROTOCOL_VERSION,
            "action": "start",
            "job_id": self.job_id,
            "argv": list(self.argv),
            "env": dict(sorted(self.env.items())),
            "cwd": cwd,
            "uid": uid,
            "gid": gid,
            "max_stdout_bytes": self.max_stdout_bytes,
            "max_stderr_bytes": self.max_stderr_bytes,
        }


@dataclass(frozen=True)
class ManagedProcessRecord:
    sandbox_id: str
    sandbox_generation: int
    job_id: str
    spec_sha256: str
    state: str
    pid: int = 0
    started_at: str = ""
    completed_at: str = ""
    exit_code: int | None = None
    signal: int = 0
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    sequence: int = 0
    updated_at: str = ""

    @classmethod
    def from_control_response(
        cls,
        raw: object,
        *,
        sandbox_id: str,
        sandbox_generation: int,
    ) -> "ManagedProcessRecord":
        if not isinstance(raw, dict) or raw.get("ok") is not True:
            error = raw.get("error") if isinstance(raw, dict) else None
            raise ManagedProcessError(str(error or "invalid managed process response"))
        job = raw.get("job")
        if not isinstance(job, dict):
            raise ManagedProcessError("managed process response omitted job state")
        exit_code = job.get("exit_code")
        record = cls(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            job_id=str(job.get("job_id") or ""),
            spec_sha256=str(job.get("spec_sha256") or ""),
            state=str(job.get("state") or ""),
            pid=max(0, int(job.get("pid") or 0)),
            started_at=str(job.get("started_at") or ""),
            completed_at=str(job.get("completed_at") or ""),
            exit_code=int(exit_code) if exit_code is not None else None,
            signal=max(0, int(job.get("signal") or 0)),
            stdout_bytes=max(0, int(job.get("stdout_bytes") or 0)),
            stderr_bytes=max(0, int(job.get("stderr_bytes") or 0)),
            stdout_truncated=bool(job.get("stdout_truncated", False)),
            stderr_truncated=bool(job.get("stderr_truncated", False)),
            sequence=max(0, int(job.get("sequence") or 0)),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        record.validate()
        return record

    @classmethod
    def from_dict(cls, raw: object) -> "ManagedProcessRecord":
        if not isinstance(raw, dict):
            raise ValueError("managed process record must be a JSON object")
        result = cls(
            sandbox_id=str(raw.get("sandbox_id") or ""),
            sandbox_generation=int(raw.get("sandbox_generation") or 0),
            job_id=str(raw.get("job_id") or ""),
            spec_sha256=str(raw.get("spec_sha256") or ""),
            state=str(raw.get("state") or ""),
            pid=max(0, int(raw.get("pid") or 0)),
            started_at=str(raw.get("started_at") or ""),
            completed_at=str(raw.get("completed_at") or ""),
            exit_code=(
                int(raw["exit_code"]) if raw.get("exit_code") is not None else None
            ),
            signal=max(0, int(raw.get("signal") or 0)),
            stdout_bytes=max(0, int(raw.get("stdout_bytes") or 0)),
            stderr_bytes=max(0, int(raw.get("stderr_bytes") or 0)),
            stdout_truncated=bool(raw.get("stdout_truncated", False)),
            stderr_truncated=bool(raw.get("stderr_truncated", False)),
            sequence=max(0, int(raw.get("sequence") or 0)),
            updated_at=str(raw.get("updated_at") or ""),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if not self.sandbox_id or self.sandbox_generation < 1:
            raise ValueError("managed process sandbox identity is invalid")
        if not _JOB_ID.fullmatch(self.job_id):
            raise ValueError("managed process job identity is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", self.spec_sha256):
            raise ValueError("managed process spec digest is invalid")
        if self.state not in {"starting", "running", "exited", "signaled", "failed"}:
            raise ValueError("managed process state is invalid")
        for value in (self.started_at, self.completed_at, self.updated_at):
            if value and parse_iso_datetime(value) is None:
                raise ValueError("managed process timestamp is invalid")
        if self.sequence < 1:
            raise ValueError("managed process sequence is invalid")

    @property
    def terminal(self) -> bool:
        return self.state in {"exited", "signaled", "failed"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "sandbox_id": self.sandbox_id,
            "sandbox_generation": self.sandbox_generation,
            "job_id": self.job_id,
            "spec_sha256": self.spec_sha256,
            "state": self.state,
            "pid": self.pid,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "exit_code": self.exit_code,
            "signal": self.signal,
            "stdout_bytes": self.stdout_bytes,
            "stderr_bytes": self.stderr_bytes,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "sequence": self.sequence,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class ManagedProcessLogChunk:
    stream: str
    offset: int
    next_offset: int
    data: bytes
    eof: bool

    @classmethod
    def from_control_response(cls, raw: object) -> "ManagedProcessLogChunk":
        if not isinstance(raw, dict) or raw.get("ok") is not True:
            error = raw.get("error") if isinstance(raw, dict) else None
            raise ManagedProcessError(
                str(error or "invalid managed process log response")
            )
        stream = str(raw.get("stream") or "")
        if stream not in {"stdout", "stderr"}:
            raise ManagedProcessError("managed process log response has invalid stream")
        encoded = raw.get("data") or ""
        if not isinstance(encoded, str):
            raise ManagedProcessError("managed process log response has invalid data")
        try:
            data = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ManagedProcessError(
                "managed process log response is not base64"
            ) from exc
        offset = int(raw.get("offset") or 0)
        next_offset = int(raw.get("next_offset") or offset)
        if offset < 0 or next_offset != offset + len(data):
            raise ManagedProcessError(
                "managed process log response has invalid offsets"
            )
        return cls(
            stream=stream,
            offset=offset,
            next_offset=next_offset,
            data=data,
            eof=bool(raw.get("eof", False)),
        )


def control_request_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def parse_control_response(payload: bytes) -> dict[str, Any]:
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManagedProcessError("managed process returned invalid JSON") from exc
    if not isinstance(raw, dict):
        raise ManagedProcessError("managed process returned a non-object response")
    return raw
