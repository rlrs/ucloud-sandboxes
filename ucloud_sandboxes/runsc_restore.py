from __future__ import annotations

from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
from typing import Any, Callable, Sequence
import uuid


DEFAULT_CONFIG_PATH = "/etc/ucloud-sandboxes/runsc-restore.json"
DEFAULT_WRAPPER_PATH = "/usr/local/libexec/ucloud-runsc-restore"
DEFAULT_RUNTIME_NAME = "runsc-restore"
RESTORE_CHECKPOINT_ANNOTATION = "dev.ucloud.sandboxes.restore.checkpoint"
WRAPPER_VERSION = 1
MAX_JSON_BYTES = 8 * 1024 * 1024

_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_RUNSC_COMMANDS = {
    "checkpoint",
    "create",
    "delete",
    "events",
    "exec",
    "kill",
    "list",
    "pause",
    "ps",
    "restore",
    "resume",
    "run",
    "spec",
    "start",
    "state",
    "wait",
}


class RestoreWrapperError(RuntimeError):
    pass


@dataclass(frozen=True)
class RestoreWrapperConfig:
    real_runsc: Path
    docker_root: Path
    checkpoint_root: Path
    state_root: Path


RunCommand = Callable[[Sequence[str]], int]


def render_runsc_restore_script(
    *, config_path: str = DEFAULT_CONFIG_PATH
) -> str:
    """Render this module as a dependency-free root-owned OCI wrapper."""
    if not config_path.startswith("/") or "\n" in config_path or "\r" in config_path:
        raise ValueError("runsc restore config path must be absolute and single-line")
    source = Path(__file__).read_text(encoding="utf-8")
    assignment = f"DEFAULT_CONFIG_PATH = {config_path!r}"
    lines = source.splitlines()
    indexes = [
        index
        for index, line in enumerate(lines)
        if line.startswith("DEFAULT_CONFIG_PATH = ")
    ]
    if len(indexes) != 1:
        raise RuntimeError("runsc restore config marker is missing or ambiguous")
    lines[indexes[0]] = assignment
    return "#!/usr/bin/python3\n" + "\n".join(lines) + "\n"


def _validated_absolute_path(label: str, value: Any) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        raise RestoreWrapperError(f"{label} must be an absolute path")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise RestoreWrapperError(f"{label} contains invalid characters")
    path = Path(value)
    if ".." in path.parts:
        raise RestoreWrapperError(f"{label} cannot contain parent traversal")
    return path


def _check_no_symlink_components(path: Path) -> None:
    current = Path(path.root)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode):
            raise RestoreWrapperError(f"path contains a symlink component: {current}")


def _check_regular_file(
    path: Path,
    label: str,
    *,
    require_root_ownership: bool,
    executable: bool = False,
) -> None:
    _check_no_symlink_components(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise RestoreWrapperError(f"cannot inspect {label}: {exc}") from exc
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise RestoreWrapperError(f"{label} must be a regular file")
    if require_root_ownership and info.st_uid != 0:
        raise RestoreWrapperError(f"{label} must be owned by root")
    if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RestoreWrapperError(f"{label} cannot be group/world writable")
    if executable and not info.st_mode & stat.S_IXUSR:
        raise RestoreWrapperError(f"{label} must be executable")


def _check_directory(
    path: Path,
    label: str,
    *,
    require_root_ownership: bool,
) -> None:
    _check_no_symlink_components(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise RestoreWrapperError(f"cannot inspect {label}: {exc}") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise RestoreWrapperError(f"{label} must be a directory")
    if require_root_ownership and info.st_uid != 0:
        raise RestoreWrapperError(f"{label} must be owned by root")
    if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RestoreWrapperError(f"{label} cannot be group/world writable")


def load_config(
    path: str | Path = DEFAULT_CONFIG_PATH,
    *,
    require_root_ownership: bool = True,
) -> RestoreWrapperConfig:
    config_path = Path(path)
    _check_regular_file(
        config_path,
        "runsc restore config",
        require_root_ownership=require_root_ownership,
    )
    try:
        raw = config_path.read_bytes()
    except OSError as exc:
        raise RestoreWrapperError(f"cannot read runsc restore config: {exc}") from exc
    if len(raw) > 64 * 1024:
        raise RestoreWrapperError("runsc restore config is too large")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RestoreWrapperError("runsc restore config is invalid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "version",
        "real_runsc",
        "docker_root",
        "checkpoint_root",
        "state_root",
    }:
        raise RestoreWrapperError("runsc restore config has an invalid schema")
    if payload["version"] != WRAPPER_VERSION:
        raise RestoreWrapperError("runsc restore config has an unsupported version")
    config = RestoreWrapperConfig(
        real_runsc=_validated_absolute_path("real runsc", payload["real_runsc"]),
        docker_root=_validated_absolute_path("docker root", payload["docker_root"]),
        checkpoint_root=_validated_absolute_path(
            "checkpoint root", payload["checkpoint_root"]
        ),
        state_root=_validated_absolute_path("state root", payload["state_root"]),
    )
    if (
        config.checkpoint_root.parent != config.docker_root
        or config.checkpoint_root.name != "ucloud-checkpoints"
    ):
        raise RestoreWrapperError(
            "checkpoint root must be DockerRootDir/ucloud-checkpoints"
        )
    _check_regular_file(
        config.real_runsc,
        "real runsc",
        require_root_ownership=require_root_ownership,
        executable=True,
    )
    _check_directory(
        config.docker_root,
        "docker root",
        require_root_ownership=require_root_ownership,
    )
    _check_directory(
        config.checkpoint_root,
        "checkpoint root",
        require_root_ownership=require_root_ownership,
    )
    _check_no_symlink_components(config.state_root)
    return config


def _command_index(argv: Sequence[str]) -> int | None:
    indexes = [index for index, item in enumerate(argv) if item in _RUNSC_COMMANDS]
    if not indexes:
        return None
    if len(indexes) != 1:
        raise RestoreWrapperError("runsc invocation contains an ambiguous command")
    return indexes[0]


def _container_id(argv: Sequence[str]) -> str:
    matches = [item for item in argv if _CONTAINER_ID.fullmatch(item)]
    if len(matches) != 1:
        raise RestoreWrapperError("runsc invocation must contain one full container id")
    return matches[0]


def _bundle_path(command_argv: Sequence[str]) -> Path:
    for index, item in enumerate(command_argv):
        if item in {"--bundle", "-b"}:
            if index + 1 >= len(command_argv):
                break
            return _validated_absolute_path("OCI bundle", command_argv[index + 1])
        for prefix in ("--bundle=", "-b="):
            if item.startswith(prefix):
                return _validated_absolute_path("OCI bundle", item[len(prefix) :])
    raise RestoreWrapperError("runsc create is missing its OCI bundle")


def _read_json_file(
    path: Path,
    label: str,
    *,
    require_root_ownership: bool,
) -> Any:
    _check_regular_file(
        path,
        label,
        require_root_ownership=require_root_ownership,
    )
    raw = path.read_bytes()
    if len(raw) > MAX_JSON_BYTES:
        raise RestoreWrapperError(f"{label} is too large")
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RestoreWrapperError(f"{label} is invalid JSON") from exc


def _restore_checkpoint_from_bundle(
    bundle: Path,
    *,
    require_root_ownership: bool,
) -> str | None:
    payload = _read_json_file(
        bundle / "config.json",
        "OCI config",
        require_root_ownership=require_root_ownership,
    )
    if not isinstance(payload, dict):
        raise RestoreWrapperError("OCI config must be an object")
    annotations = payload.get("annotations") or {}
    if not isinstance(annotations, dict):
        raise RestoreWrapperError("OCI annotations must be an object")
    raw = annotations.get(RESTORE_CHECKPOINT_ANNOTATION)
    if raw is None:
        return None
    if not isinstance(raw, str) or not _SAFE_ID.fullmatch(raw):
        raise RestoreWrapperError("restore checkpoint annotation is invalid")
    return raw


def _state_root(
    config: RestoreWrapperConfig,
    *,
    require_root_ownership: bool,
) -> Path:
    config.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _check_directory(
        config.state_root,
        "runsc restore state root",
        require_root_ownership=require_root_ownership,
    )
    return config.state_root


def _intent_path(
    config: RestoreWrapperConfig,
    container_id: str,
    *,
    require_root_ownership: bool,
) -> Path:
    if not _CONTAINER_ID.fullmatch(container_id):
        raise RestoreWrapperError("invalid container id")
    return _state_root(
        config,
        require_root_ownership=require_root_ownership,
    ) / f"{container_id}.json"


def _lock(
    config: RestoreWrapperConfig,
    container_id: str,
    *,
    require_root_ownership: bool,
) -> Any:
    path = _state_root(
        config,
        require_root_ownership=require_root_ownership,
    ) / f"{container_id}.lock"
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return os.fdopen(descriptor, "r+")


def _atomic_write_intent(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _validated_stage(
    config: RestoreWrapperConfig,
    container_id: str,
    checkpoint_id: str,
    *,
    require_root_ownership: bool,
) -> Path:
    marker = config.checkpoint_root / ".staged" / f"{container_id}-{checkpoint_id}.json"
    payload = _read_json_file(
        marker,
        "staged checkpoint marker",
        require_root_ownership=require_root_ownership,
    )
    if (
        not isinstance(payload, dict)
        or payload.get("version") != 1
        or payload.get("state") != "staged"
        or payload.get("target_container_id") != container_id
        or payload.get("checkpoint_id") != checkpoint_id
        or not isinstance(payload.get("artifact_id"), str)
        or not _SAFE_ID.fullmatch(payload["artifact_id"])
    ):
        raise RestoreWrapperError("staged checkpoint marker identity is invalid")
    checkpoint = (
        config.docker_root
        / "containers"
        / container_id
        / "checkpoints"
        / checkpoint_id
    )
    _check_directory(
        checkpoint,
        "staged checkpoint image",
        require_root_ownership=require_root_ownership,
    )
    return checkpoint


def _read_intent(
    config: RestoreWrapperConfig,
    container_id: str,
    *,
    require_root_ownership: bool,
) -> dict[str, Any] | None:
    path = _intent_path(
        config,
        container_id,
        require_root_ownership=require_root_ownership,
    )
    if not path.exists():
        return None
    payload = _read_json_file(
        path,
        "runsc restore intent",
        require_root_ownership=require_root_ownership,
    )
    if (
        not isinstance(payload, dict)
        or set(payload) != {
            "version",
            "container_id",
            "checkpoint_id",
            "checkpoint_path",
            "created_ns",
        }
        or payload["version"] != WRAPPER_VERSION
        or payload["container_id"] != container_id
        or not isinstance(payload["checkpoint_id"], str)
        or not _SAFE_ID.fullmatch(payload["checkpoint_id"])
        or not isinstance(payload["created_ns"], int)
    ):
        raise RestoreWrapperError("runsc restore intent is invalid")
    expected = _validated_stage(
        config,
        container_id,
        payload["checkpoint_id"],
        require_root_ownership=require_root_ownership,
    )
    if payload["checkpoint_path"] != str(expected):
        raise RestoreWrapperError("runsc restore intent checkpoint path changed")
    return payload


def _default_run(command: Sequence[str]) -> int:
    completed = subprocess.run(tuple(command), check=False)
    return int(completed.returncode)


def dispatch(
    config: RestoreWrapperConfig,
    argv: Sequence[str],
    *,
    run_command: RunCommand = _default_run,
    require_root_ownership: bool = True,
) -> int:
    arguments = tuple(argv)
    if arguments == ("--ucloud-wrapper-version",):
        print(f"ucloud-runsc-restore {WRAPPER_VERSION}")
        return 0
    command_index = _command_index(arguments)
    real_prefix = (str(config.real_runsc),)
    if command_index is None:
        return run_command(real_prefix + arguments)
    action = arguments[command_index]
    if action not in {"create", "start", "delete"}:
        return run_command(real_prefix + arguments)
    container_id = _container_id(arguments)
    with _lock(
        config,
        container_id,
        require_root_ownership=require_root_ownership,
    ):
        intent_path = _intent_path(
            config,
            container_id,
            require_root_ownership=require_root_ownership,
        )
        if action == "create":
            bundle = _bundle_path(arguments[command_index + 1 :])
            checkpoint_id = _restore_checkpoint_from_bundle(
                bundle,
                require_root_ownership=require_root_ownership,
            )
            if checkpoint_id is None:
                return run_command(real_prefix + arguments)
            checkpoint = _validated_stage(
                config,
                container_id,
                checkpoint_id,
                require_root_ownership=require_root_ownership,
            )
            _atomic_write_intent(
                intent_path,
                {
                    "version": WRAPPER_VERSION,
                    "container_id": container_id,
                    "checkpoint_id": checkpoint_id,
                    "checkpoint_path": str(checkpoint),
                    "created_ns": time.time_ns(),
                },
            )
            result = run_command(real_prefix + arguments)
            if result != 0:
                intent_path.unlink(missing_ok=True)
            return result
        if action == "start":
            intent = _read_intent(
                config,
                container_id,
                require_root_ownership=require_root_ownership,
            )
            if intent is None:
                return run_command(real_prefix + arguments)
            restore = (
                real_prefix
                + arguments[:command_index]
                + (
                    "restore",
                    "--detach",
                    f"--image-path={intent['checkpoint_path']}",
                )
                + arguments[command_index + 1 :]
            )
            result = run_command(restore)
            if result == 0:
                intent_path.unlink(missing_ok=True)
            return result
        result = run_command(real_prefix + arguments)
        intent_path.unlink(missing_ok=True)
        return result


def main(
    argv: Sequence[str] | None = None,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    require_root: bool = True,
) -> int:
    try:
        arguments = tuple(sys.argv[1:] if argv is None else argv)
        if arguments == ("--ucloud-wrapper-version",):
            print(f"ucloud-runsc-restore {WRAPPER_VERSION}")
            return 0
        if require_root and os.geteuid() != 0:
            raise RestoreWrapperError("runsc restore wrapper must run as root")
        config = load_config(
            config_path,
            require_root_ownership=require_root,
        )
        return dispatch(
            config,
            arguments,
            require_root_ownership=require_root,
        )
    except (OSError, RestoreWrapperError, ValueError) as exc:
        print(f"ucloud-runsc-restore: {exc}", file=sys.stderr)
        return 125


if __name__ == "__main__":
    raise SystemExit(main())
